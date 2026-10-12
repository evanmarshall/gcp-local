package compute

import (
	"net/http"
	"strings"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/providers/provider/providertest"
)

// Paths of the hammered resources.
const (
	globalPath  = "/compute/v1/projects/demo-net/global"
	hammerNet   = "hammer-net"
	hammerSub   = "hammer-sub"
	hammerFw    = "hammer-fw"
	hammerRoute = "hammer-route"
	hammerVM    = "hammer-vm"
	hammerNAT   = "hammer-router"
	hammerTest  = "hammer-test"
)

// TestConcurrentTopologyRequests races create, get, list, update, and delete
// of one resource of every mutable kind while the network they reference is
// itself created, peered, and deleted. -race fails it if a handler touches a
// record outside the scope lock.
func TestConcurrentTopologyRequests(t *testing.T) {
	h := newHarness(t)
	h.expectCode(http.MethodPost, globalPath+"/networks", map[string]any{keyName: testNetwork, keyAutoSubnets: false}, 200)
	// A stable subnetwork holds the connectivity test's addresses, so only
	// the hammered resources come and go.
	h.expectCode(http.MethodPost, regionPath+"/subnetworks", map[string]any{keyName: testSubnetOne, keyNetwork: testNetwork, "ipCidrRange": testSubnetCIDR}, 200)
	ok, created, gone := []int{200}, []int{200, 409}, []int{200, 404}
	firewall := map[string]any{
		keyName: hammerFw, keyNetwork: testNetwork,
		"allowed": []any{map[string]any{keyIPProto: "tcp", keyPorts: []any{"80"}}}, "sourceRanges": []any{testSourceCIDR},
	}
	connectivity := map[string]any{
		keySource:      map[string]any{keyIPAddress: "10.0.0.100", keyNetwork: "projects/demo-net/global/networks/" + testNetwork},
		keyDestination: map[string]any{keyIPAddress: "10.0.0.101", keyPort: 80}, keyProtocol: "TCP",
	}
	providertest.Hammer(t, h.gate, "actor", 8, 10, func(int) []providertest.Request {
		return []providertest.Request{
			{Method: http.MethodPost, Path: globalPath + "/networks", Body: map[string]any{keyName: hammerNet}, Want: created},
			{Method: http.MethodGet, Path: globalPath + "/networks/" + hammerNet, Want: gone},
			{Method: http.MethodGet, Path: globalPath + "/networks", Want: ok},
			{Method: http.MethodPost, Path: globalPath + "/networks/" + testNetwork + "/addPeering", Body: map[string]any{
				"networkPeering": map[string]any{keyName: testPeering, keyNetwork: hammerNet},
			}, Want: []int{200, 400, 409}},
			{Method: http.MethodGet, Path: globalPath + "/networks/" + testNetwork, Want: ok},
			{Method: http.MethodPost, Path: globalPath + "/networks/" + testNetwork + "/removePeering", Body: map[string]any{keyName: testPeering}, Want: []int{200, 400}},
			{Method: http.MethodDelete, Path: globalPath + "/networks/" + hammerNet, Want: gone},

			{Method: http.MethodPost, Path: regionPath + "/subnetworks", Body: map[string]any{keyName: hammerSub, keyNetwork: testNetwork, "ipCidrRange": testRangeA}, Want: created},
			{Method: http.MethodGet, Path: regionPath + "/subnetworks/" + hammerSub, Want: gone},
			{Method: http.MethodGet, Path: regionPath + "/subnetworks", Want: ok},
			{Method: http.MethodPost, Path: zonePath + "/instances", Body: instanceBody(hammerVM, hammerSub), Want: []int{200, 400, 409}},
			{Method: http.MethodGet, Path: zonePath + "/instances/" + hammerVM, Want: gone},
			{Method: http.MethodGet, Path: zonePath + "/instances", Want: ok},
			{Method: http.MethodPost, Path: regionPath + "/routers", Body: map[string]any{
				keyName: hammerNAT, keyNetwork: testNetwork,
				"nats": []any{map[string]any{keyName: "nat-1", "sourceSubnetworkIpRangesToNat": allSubnetworks}},
			}, Want: created},
			{Method: http.MethodGet, Path: regionPath + "/routers/" + hammerNAT, Want: gone},
			{Method: http.MethodGet, Path: regionPath + "/routers", Want: ok},

			{Method: http.MethodPost, Path: globalPath + "/firewalls", Body: firewall, Want: created},
			{Method: http.MethodGet, Path: globalPath + "/firewalls/" + hammerFw, Want: gone},
			{Method: http.MethodPut, Path: globalPath + "/firewalls/" + hammerFw, Body: firewall, Want: gone},
			{Method: http.MethodGet, Path: globalPath + "/firewalls", Want: ok},
			{Method: http.MethodPost, Path: globalPath + "/routes", Body: map[string]any{keyName: hammerRoute, keyNetwork: testNetwork, "destRange": "10.1.0.0/16", "nextHopGateway": "global/gateways/default-internet-gateway"}, Want: created},
			{Method: http.MethodGet, Path: globalPath + "/routes/" + hammerRoute, Want: gone},
			{Method: http.MethodGet, Path: globalPath + "/routes", Want: ok},

			{Method: http.MethodPost, Path: connectivityTests + "?testId=" + hammerTest, Body: connectivity, Want: created},
			{Method: http.MethodGet, Path: connectivityTests + "/" + hammerTest, Want: gone},
			{Method: http.MethodPost, Path: connectivityTests + "/" + hammerTest + ":rerun", Body: map[string]any{}, Want: gone},
			{Method: http.MethodGet, Path: connectivityTests, Want: ok},
			{Method: http.MethodDelete, Path: connectivityTests + "/" + hammerTest, Want: gone},

			{Method: http.MethodGet, Path: globalPath + "/operations", Want: ok},
			{Method: http.MethodDelete, Path: regionPath + "/routers/" + hammerNAT, Want: gone},
			{Method: http.MethodDelete, Path: globalPath + "/routes/" + hammerRoute, Want: gone},
			{Method: http.MethodDelete, Path: globalPath + "/firewalls/" + hammerFw, Want: gone},
			{Method: http.MethodDelete, Path: zonePath + "/instances/" + hammerVM, Want: gone},
			{Method: http.MethodDelete, Path: regionPath + "/subnetworks/" + hammerSub, Want: gone},
		}
	})
}

// TestRejectedUpdatesChangeNothing checks that a refused firewall replace or
// peering removal leaves the stored resource as it was.
func TestRejectedUpdatesChangeNothing(t *testing.T) {
	h := newHarness(t)
	h.expectCode(http.MethodPost, globalPath+"/networks", map[string]any{keyName: testNetwork}, 200)
	h.expectCode(http.MethodPost, globalPath+"/firewalls", map[string]any{
		keyName: testFirewall, keyNetwork: testNetwork, keyDescription: "before",
		"allowed": []any{map[string]any{keyIPProto: "tcp", keyPorts: []any{"80"}}},
	}, 200)
	h.expectCode(http.MethodPut, globalPath+"/firewalls/"+testFirewall, map[string]any{
		keyName: testFirewall, keyNetwork: "missing-net", keyDescription: "after",
	}, 400)
	if body := h.expectCode(http.MethodGet, globalPath+"/firewalls/"+testFirewall, nil, 200).Body.String(); !strings.Contains(body, `"description":"before"`) {
		t.Fatalf("a refused firewall replace changed the rule: %s", body)
	}
	before := h.expectCode(http.MethodGet, globalPath+"/networks/"+testNetwork, nil, 200).Body.String()
	h.expectCode(http.MethodPost, globalPath+"/networks/"+testNetwork+"/removePeering", map[string]any{keyName: "missing"}, 400)
	if after := h.expectCode(http.MethodGet, globalPath+"/networks/"+testNetwork, nil, 200).Body.String(); after != before {
		t.Fatalf("a refused peering removal changed the network:\n%s\n%s", before, after)
	}
}
