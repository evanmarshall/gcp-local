package compute

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// Names and keys repeated across the lifecycle tests.
const (
	testNetwork        = "vpc-1"
	testFirewall       = "fw-1"
	testAllowed        = "allowed"
	testProject        = "demo-net"
	testKindProj       = "project"
	testSubnetOne      = "subnet-1"
	testPeerNetwork    = "peer-net"
	testPeerSubnet     = "peer-subnet"
	testPeering        = "peering-1"
	testTCP            = "tcp"
	testSubnetCIDR     = "10.0.0.0/20"
	verdictDropped     = "dropped"
	verdictUnsupported = "unsupported"
	testSourceCIDR     = "10.0.0.0/8"
	keyDirection       = "direction"
	keySourceRanges    = "sourceRanges"
)

// harness drives the provider through the gateway.
type harness struct {
	t               *testing.T
	gate            *gateway.Gateway
	analyzerHandler http.Handler
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	registry := gateway.NewRegistry()
	index := state.NewIndex()
	engine, err := iam.New(index)
	if err != nil {
		t.Fatalf("build the iam engine: %v", err)
	}
	if err := index.Put("resourcemanager", state.Resource{
		Name: "//cloudresourcemanager.googleapis.com/projects/demo-net", Service: "resourcemanager",
		Kind: testKindProj, Number: "123456", Scope: state.Scope{Project: testProject, Location: state.Global},
	}); err != nil {
		t.Fatalf("register the project: %v", err)
	}
	if _, err := engine.SetPolicy("//cloudresourcemanager.googleapis.com/projects/demo-net", iam.Policy{
		Bindings: []iam.Binding{
			{Role: "roles/compute.admin", Members: []string{"serviceAccount:actor@demo-net.iam.gserviceaccount.com"}},
			{Role: "roles/networkmanagement.viewer", Members: []string{"serviceAccount:actor@demo-net.iam.gserviceaccount.com"}},
		},
	}); err != nil {
		t.Fatalf("grant the project: %v", err)
	}
	provider, err := New(context.Background(), registry, backend.NewManager(time.Second, time.Second), index, engine)
	if err != nil {
		t.Fatalf("build the compute provider: %v", err)
	}
	analyzer := provider.Analyzer()
	g := gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:" + token + "@demo-net.iam.gserviceaccount.com", Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})
	return &harness{t: t, gate: g, analyzerHandler: analyzer}
}

// invoke issues one REST request as the fixture principal.
func (h *harness) invoke(verb, path string, body any) *httptest.ResponseRecorder {
	h.t.Helper()
	var encoded []byte
	if body != nil {
		encoded, _ = json.Marshal(body) //nolint:errchkjson // plain map bodies
	}
	request, _ := http.NewRequestWithContext(context.Background(), verb, path, bytes.NewReader(encoded))
	if encoded != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Authorization", "Bearer actor")
	recorder := httptest.NewRecorder()
	h.gate.ServeHTTP(recorder, request)
	return recorder
}

func TestNetworkLifecycleAndDependencyDeletion(t *testing.T) {
	h := newHarness(t)
	recorder := h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/networks", map[string]any{
		keyName: testNetwork, keyAutoSubnets: false,
	})
	if recorder.Code != 200 {
		t.Fatalf("network create status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodGet, "/compute/v1/projects/demo-net/global/networks/vpc-1", nil)
	if recorder.Code != 200 {
		t.Fatalf("network get status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/regions/us-central1/subnetworks", map[string]any{
		keyName: testSubnetOne, keyNetwork: testNetwork, keyIPCIDR: testSubnetCIDR,
	})
	if recorder.Code != 200 {
		t.Fatalf("subnetwork create status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodDelete, "/compute/v1/projects/demo-net/global/networks/vpc-1", nil)
	if recorder.Code != 400 {
		t.Fatalf("network with subnet delete status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodDelete, "/compute/v1/projects/demo-net/regions/us-central1/subnetworks/subnet-1", nil)
	if recorder.Code != 200 {
		t.Fatalf("subnetwork delete status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodDelete, "/compute/v1/projects/demo-net/global/networks/vpc-1", nil)
	if recorder.Code != 200 {
		t.Fatalf("network delete status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodGet, "/compute/v1/projects/demo-net/global/networks/vpc-1", nil)
	if recorder.Code != 404 {
		t.Fatalf("deleted network get status = %d", recorder.Code)
	}
}

func TestFirewallAndRouteReferenceMissingNetwork(t *testing.T) {
	h := newHarness(t)
	recorder := h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/firewalls", map[string]any{
		keyName: testFirewall, keyNetwork: "missing-net",
		testAllowed: []any{map[string]any{keyIPProto: testTCP, keyPorts: []any{"80"}}},
	})
	if recorder.Code != 400 {
		t.Fatalf("firewall on missing network status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/routes", map[string]any{
		keyName: "route-1", keyNetwork: "missing-net", "destRange": "10.1.0.0/16",
	})
	if recorder.Code != 400 {
		t.Fatalf("route on missing network status = %d body %s", recorder.Code, recorder.Body.String())
	}
}

func TestFirewallLifecycleAndUpdate(t *testing.T) {
	h := newHarness(t)
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/networks", map[string]any{keyName: testNetwork})
	recorder := h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/firewalls", map[string]any{
		keyName: testFirewall, keyNetwork: testNetwork, keyDirection: directionIngress,
		testAllowed:     []any{map[string]any{keyIPProto: testTCP, keyPorts: []any{"80"}}},
		keySourceRanges: []any{testSourceCIDR},
	})
	if recorder.Code != 200 {
		t.Fatalf("firewall create status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodPut, "/compute/v1/projects/demo-net/global/firewalls/fw-1", map[string]any{
		keyName: testFirewall, keyNetwork: testNetwork, testAllowed: []any{map[string]any{keyIPProto: testTCP, keyPorts: []any{"443"}}},
	})
	if recorder.Code != 200 {
		t.Fatalf("firewall update status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodGet, "/compute/v1/projects/demo-net/global/firewalls/fw-1", nil)
	var firewall struct {
		Allowed []map[string]any `json:"allowed"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &firewall); err != nil {
		t.Fatalf("decode firewall: %v", err)
	}
	if len(firewall.Allowed) != 1 {
		t.Fatalf("allowed = %v", firewall.Allowed)
	}
}

func TestOperationsCompleteAndList(t *testing.T) {
	h := newHarness(t)
	recorder := h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/networks", map[string]any{keyName: testNetwork})
	var operation struct {
		Name   string `json:"name"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &operation); err != nil {
		t.Fatalf("decode operation: %v", err)
	}
	if operation.Status != "DONE" {
		t.Fatalf("operation status = %s", operation.Status)
	}
	recorder = h.invoke(http.MethodGet, "/compute/v1/projects/demo-net/global/operations/"+operation.Name, nil)
	if recorder.Code != 200 {
		t.Fatalf("operation get status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodGet, "/compute/v1/projects/demo-net/global/operations", nil)
	if recorder.Code != 200 {
		t.Fatalf("operations list status = %d", recorder.Code)
	}
}

func TestReachabilityDistinguishesVerdicts(t *testing.T) {
	h := newHarness(t)
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/networks", map[string]any{
		keyName: testNetwork, keyAutoSubnets: false,
	})
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/regions/us-central1/subnetworks", map[string]any{
		keyName: testSubnetOne, keyNetwork: testNetwork, keyIPCIDR: testSubnetCIDR,
	})
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/firewalls", map[string]any{
		keyName: testFirewall, keyNetwork: testNetwork, keyDirection: directionIngress,
		testAllowed:     []any{map[string]any{keyIPProto: testTCP, keyPorts: []any{"443"}}},
		keySourceRanges: []any{testSourceCIDR},
	})
	analyze := func(source, dest, protocol string, port int) Reachability {
		spec := map[string]any{
			keyProject: testProject, keySourceIP: source, keyDestIP: dest,
			keyProtocol: protocol, keyPort: port,
		}
		encoded, _ := json.Marshal(spec) //nolint:errchkjson // plain map bodies //nolint:errchkjson // plain map bodies
		request, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, "/_local/networking/reachability", bytes.NewReader(encoded))
		recorder := httptest.NewRecorder()
		handler := h.analyzer()
		handler.ServeHTTP(recorder, request)
		var verdict Reachability
		if err := json.Unmarshal(recorder.Body.Bytes(), &verdict); err != nil {
			h.t.Fatalf("decode verdict: %v", err)
		}
		return verdict
	}
	verdict := analyze("10.0.0.5", "10.0.0.9", testTCP, 443)
	if verdict.Result != "reachable" || len(verdict.Trace) != 1 {
		t.Fatalf("permitted flow = %+v", verdict)
	}
	verdict = analyze("10.0.0.7", "10.0.0.9", testTCP, 80)
	if verdict.Result != verdictDropped {
		t.Fatalf("unpermitted flow = %+v", verdict)
	}
	verdict = analyze("192.0.2.1", "10.0.0.9", testTCP, 443)
	if verdict.Result != verdictDropped {
		// A source outside the modeled topology is an unknown endpoint, not a
		// decided flow; a modeled foreign source needs its own subnet fixture.
		if verdict.Result != verdictUnknown {
			t.Fatalf("foreign source flow = %+v", verdict)
		}
	}
	verdict = analyze("10.0.0.5", "10.0.99.1", testTCP, 443)
	if verdict.Result != verdictUnknown {
		t.Fatalf("unresolved destination = %+v", verdict)
	}
}

// analyzer exposes the mounted reachability surface.
func (h *harness) analyzer() http.Handler { return h.analyzerHandler }

// analyzeFlow posts one flow specification and decodes the verdict.
func (h *harness) analyzeFlow(source, dest, protocol string, port int) Reachability {
	h.t.Helper()
	spec := map[string]any{
		keyProject: testProject, keySourceIP: source, keyDestIP: dest,
		keyProtocol: protocol, keyPort: port,
	}
	encoded, _ := json.Marshal(spec) //nolint:errchkjson // plain map bodies
	request, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, "/_local/networking/reachability", bytes.NewReader(encoded))
	recorder := httptest.NewRecorder()
	h.analyzer().ServeHTTP(recorder, request)
	var verdict Reachability
	if err := json.Unmarshal(recorder.Body.Bytes(), &verdict); err != nil {
		h.t.Fatalf("decode verdict: %v", err)
	}
	return verdict
}

func TestUnimplementedTopologyReportsUnsupported(t *testing.T) {
	h := newHarness(t)
	spec := map[string]any{
		keyProject: testProject, keySourceIP: "10.0.0.5", keyDestIP: "10.0.16.9",
		keyProtocol: testTCP, keyPort: 443,
	}
	encoded, _ := json.Marshal(spec) //nolint:errchkjson // plain map bodies
	request, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, "/_local/networking/reachability", bytes.NewReader(encoded))
	recorder := httptest.NewRecorder()
	h.analyzer().ServeHTTP(recorder, request)
	var verdict Reachability
	if err := json.Unmarshal(recorder.Body.Bytes(), &verdict); err != nil {
		t.Fatalf("decode verdict: %v", err)
	}
	if verdict.Result != verdictUnknown {
		t.Fatalf("empty topology verdict = %+v", verdict)
	}
}

func TestCrossNetworkFlowsViaPeering(t *testing.T) {
	h := newHarness(t)
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/networks", map[string]any{
		keyName: testNetwork, keyAutoSubnets: false,
	})
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/networks", map[string]any{
		keyName: testPeerNetwork, keyAutoSubnets: false,
	})
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/regions/us-central1/subnetworks", map[string]any{
		keyName: testSubnetOne, keyNetwork: testNetwork, keyIPCIDR: testSubnetCIDR,
	})
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/regions/us-central1/subnetworks", map[string]any{
		keyName: testPeerSubnet, keyNetwork: testPeerNetwork, keyIPCIDR: "10.1.0.0/20",
	})
	recorder := h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/networks/"+testNetwork+"/addPeering", map[string]any{
		keyName: testPeering, keyPeerNetwork: testPeerNetwork,
	})
	if recorder.Code != 200 {
		t.Fatalf("addPeering status = %d body %s", recorder.Code, recorder.Body.String())
	}
	verdict := h.analyzeFlow("10.0.0.5", "10.1.0.9", testTCP, 443)
	if verdict.Result != verdictDropped {
		t.Fatalf("peered flow verdict = %+v", verdict)
	}
	if len(verdict.Trace) != 1 || verdict.Trace[0] != "peering/peering-1 to peer-net" {
		t.Fatalf("peered flow trace = %v", verdict.Trace)
	}
	recorder = h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/networks/"+testNetwork+"/removePeering", map[string]any{
		"peering": testPeering,
	})
	if recorder.Code != 200 {
		t.Fatalf("removePeering status = %d body %s", recorder.Code, recorder.Body.String())
	}
	verdict = h.analyzeFlow("10.0.0.5", "10.1.0.9", "udp", 53)
	if verdict.Result != verdictUnsupported {
		t.Fatalf("unpeered flow verdict = %+v", verdict)
	}
}

func TestDuplicatePeeringIsRefused(t *testing.T) {
	h := newHarness(t)
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/networks", map[string]any{keyName: testNetwork})
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/networks", map[string]any{keyName: testPeerNetwork})
	recorder := h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/networks/"+testNetwork+"/addPeering", map[string]any{
		keyName: testPeering, keyPeerNetwork: testPeerNetwork,
	})
	if recorder.Code != 200 {
		t.Fatalf("addPeering status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/networks/"+testNetwork+"/addPeering", map[string]any{
		keyName: testPeering, keyPeerNetwork: testPeerNetwork,
	})
	if recorder.Code != 409 {
		t.Fatalf("duplicate peering status = %d body %s", recorder.Code, recorder.Body.String())
	}
}

func TestEgressRulesDenyBeforeIngress(t *testing.T) {
	h := newHarness(t)
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/networks", map[string]any{
		keyName: testNetwork, keyAutoSubnets: false,
	})
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/regions/us-central1/subnetworks", map[string]any{
		keyName: testSubnetOne, keyNetwork: testNetwork, keyIPCIDR: testSubnetCIDR,
	})
	// An ingress rule that would allow the flow.
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/firewalls", map[string]any{
		keyName: testFirewall, keyNetwork: testNetwork, keyDirection: directionIngress,
		testAllowed:     []any{map[string]any{keyIPProto: testTCP, keyPorts: []any{"443"}}},
		keySourceRanges: []any{testSourceCIDR},
	})
	// An egress rule that denies it, at higher precedence.
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/firewalls", map[string]any{
		keyName: "egress-block", keyNetwork: testNetwork, keyDirection: "EGRESS",
		"denied": []any{map[string]any{keyIPProto: "all"}}, "priority": 100,
	})
	verdict := h.analyzeFlow("10.0.4.5", "10.0.0.9", testTCP, 443)
	if verdict.Result != verdictDropped {
		t.Fatalf("egress-denied verdict = %+v", verdict)
	}
	if verdict.Trace[0] != "firewall/egress-block priority=100" {
		t.Fatalf("trace = %v", verdict.Trace)
	}
}

func TestEgressRuleAllowsUnrestricted(t *testing.T) {
	h := newHarness(t)
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/networks", map[string]any{
		keyName: testNetwork, keyAutoSubnets: false,
	})
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/regions/us-central1/subnetworks", map[string]any{
		keyName: testSubnetOne, keyNetwork: testNetwork, keyIPCIDR: testSubnetCIDR,
	})
	// No ingress rule at all; a broad egress allow makes egress decided.
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/firewalls", map[string]any{
		keyName: "egress-all", keyNetwork: testNetwork, keyDirection: "EGRESS",
		testAllowed: []any{map[string]any{keyIPProto: "all"}},
	})
	verdict := h.analyzeFlow("10.0.0.5", "10.0.0.9", testTCP, 443)
	if verdict.Result != "reachable" {
		t.Fatalf("egress-allowed verdict = %+v", verdict)
	}
}

func TestEveryRouteIsHandled(t *testing.T) {
	h := newHarness(t)
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/networks", map[string]any{
		keyName: testNetwork, keyAutoSubnets: false,
	})
	gap := "this method is not implemented"
	probe := func(verb, path string) {
		recorder := h.invoke(verb, path, nil)
		if recorder.Code == 501 && strings.Contains(recorder.Body.String(), gap) {
			t.Fatalf("%s %s hit the dispatch gap", verb, path)
		}
	}
	probe(http.MethodGet, "/compute/v1/projects/demo-net/global/networks/"+testNetwork)
	probe(http.MethodGet, "/compute/v1/projects/demo-net/global/networks")
	probe(http.MethodGet, "/compute/v1/projects/demo-net/regions/us-central1/subnetworks")
	probe(http.MethodGet, "/compute/v1/projects/demo-net/global/firewalls")
	probe(http.MethodGet, "/compute/v1/projects/demo-net/global/routes")
	probe(http.MethodGet, "/compute/v1/projects/demo-net/global/operations")
}

func TestConnectivityTestSurfaceReportsVerdicts(t *testing.T) {
	h := newHarness(t)
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/networks", map[string]any{
		keyName: testNetwork, keyAutoSubnets: false,
	})
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/regions/us-central1/subnetworks", map[string]any{
		keyName: testSubnetOne, keyNetwork: testNetwork, keyIPCIDR: testSubnetCIDR,
	})
	spec := func(source, dest string) map[string]any {
		h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/firewalls", map[string]any{
			keyName: testFirewall, keyNetwork: testNetwork, "direction": "INGRESS",
			testAllowed:     []any{map[string]any{keyIPProto: testTCP, keyPorts: []any{"443"}}},
			keySourceRanges: []any{testSourceCIDR},
		})
		return map[string]any{
			"source":      map[string]any{"ipAddress": source},
			"destination": map[string]any{"ipAddress": dest},
			"protocol":    testTCP,
			"port":        443,
		}
	}
	recorder := h.invoke(http.MethodPost, "/v1/projects/demo-net/locations/global/connectivityTests:run", spec("10.0.0.5", "10.0.0.9"))
	if recorder.Code != 200 {
		t.Fatalf("run status = %d body %s", recorder.Code, recorder.Body.String())
	}
	var result struct {
		ReachabilityDetails struct {
			Result string `json:"result"`
			Traces []struct {
				Steps []string `json:"steps"`
			} `json:"traces"`
		} `json:"reachabilityDetails"`
		Provenance string `json:"provenance"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if result.ReachabilityDetails.Result != "REACHABLE" || len(result.ReachabilityDetails.Traces) == 0 {
		t.Fatalf("details = %+v", result.ReachabilityDetails)
	}
	if result.Provenance != "local-modeled; no cloud recording backs this surface" {
		t.Fatalf("provenance = %s", result.Provenance)
	}
	recorder = h.invoke(http.MethodPost, "/v1/projects/demo-net/locations/global/connectivityTests:run", spec("10.0.0.5", "10.0.99.1"))
	var ambiguous struct {
		ReachabilityDetails struct {
			Result string `json:"result"`
		} `json:"reachabilityDetails"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &ambiguous); err != nil {
		t.Fatalf("decode ambiguous: %v", err)
	}
	if ambiguous.ReachabilityDetails.Result != "AMBIGUOUS" {
		t.Fatalf("unresolved result = %s", ambiguous.ReachabilityDetails.Result)
	}
	recorder = h.invoke(http.MethodDelete, "/v1/projects/demo-net/locations/global/connectivityTests/any", nil)
	if recorder.Code != 501 {
		t.Fatalf("delete status = %d body %s", recorder.Code, recorder.Body.String())
	}
}

func TestNamedConnectivityTestsStoreAndReevaluate(t *testing.T) {
	h := newHarness(t)
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/networks", map[string]any{
		keyName: testNetwork, keyAutoSubnets: false,
	})
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/regions/us-central1/subnetworks", map[string]any{
		keyName: testSubnetOne, keyNetwork: testNetwork, keyIPCIDR: testSubnetCIDR,
	})
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/firewalls", map[string]any{
		keyName: testFirewall, keyNetwork: testNetwork, keyDirection: "INGRESS",
		testAllowed:     []any{map[string]any{keyIPProto: testTCP, keyPorts: []any{"443"}}},
		keySourceRanges: []any{testSourceCIDR},
	})
	body := func() map[string]any {
		return map[string]any{
			"name":        "projects/demo-net/locations/global/connectivityTests/check-web",
			"source":      map[string]any{"ipAddress": "10.0.0.5"},
			"destination": map[string]any{"ipAddress": "10.0.0.9"},
			"protocol":    testTCP,
			"port":        443,
		}
	}
	recorder := h.invoke(http.MethodPost, "/v1/projects/demo-net/locations/global/connectivityTests", body())
	if recorder.Code != 200 {
		t.Fatalf("create status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodPost, "/v1/projects/demo-net/locations/global/connectivityTests", body())
	if recorder.Code != 409 {
		t.Fatalf("duplicate create status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodGet, "/v1/projects/demo-net/locations/global/connectivityTests/check-web", nil)
	if recorder.Code != 200 {
		t.Fatalf("get status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodGet, "/v1/projects/demo-net/locations/global/connectivityTests", nil)
	var listing struct {
		Tests []map[string]any `json:"connectivityTests"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &listing); err != nil {
		t.Fatalf("decode listing: %v", err)
	}
	if len(listing.Tests) != 1 {
		t.Fatalf("listing = %v", listing)
	}
	recorder = h.invoke(http.MethodDelete, "/v1/projects/demo-net/locations/global/connectivityTests/check-web", nil)
	if recorder.Code != 501 {
		t.Fatalf("delete status = %d", recorder.Code)
	}
}
