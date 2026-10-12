package compute

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider/providertest"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// Names and keys repeated across the lifecycle tests.
const (
	testNetwork     = "vpc-1"
	testFirewall    = "fw-1"
	testProject     = "demo-net"
	testSubnetOne   = "subnet-1"
	testPeerNetwork = "peer-net"
	testPeerSubnet  = "peer-subnet"
	testPeering     = "peering-1"
	testSubnetCIDR  = "10.0.0.0/20"
	testSourceCIDR  = "10.0.0.0/8"
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
		Kind: "project", Number: "123456", Scope: state.Scope{Project: testProject, Location: state.Global},
	}); err != nil {
		t.Fatalf("register the project: %v", err)
	}
	if _, err := engine.SetPolicy("//cloudresourcemanager.googleapis.com/projects/demo-net", iam.Policy{
		Bindings: []iam.Binding{
			{Role: "roles/compute.admin", Members: []string{"serviceAccount:actor@demo-net.iam.gserviceaccount.com"}},
			{Role: "roles/networkmanagement.admin", Members: []string{"serviceAccount:actor@demo-net.iam.gserviceaccount.com"}},
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

func TestFirewallAndRouteReferenceMissingNetwork(t *testing.T) {
	h := newHarness(t)
	recorder := h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/firewalls", map[string]any{
		keyName: testFirewall, keyNetwork: "missing-net",
		"allowed": []any{map[string]any{keyIPProto: "tcp", keyPorts: []any{"80"}}},
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
		"allowed":      []any{map[string]any{keyIPProto: "tcp", keyPorts: []any{"80"}}},
		"sourceRanges": []any{testSourceCIDR},
	})
	if recorder.Code != 200 {
		t.Fatalf("firewall create status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodPut, "/compute/v1/projects/demo-net/global/firewalls/fw-1", map[string]any{
		keyName: testFirewall, keyNetwork: testNetwork, "allowed": []any{map[string]any{keyIPProto: "tcp", keyPorts: []any{"443"}}},
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

func TestRouterLifecycleWithNAT(t *testing.T) {
	h := newHarness(t)
	recorder := h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/networks", map[string]any{
		keyName: testNetwork, keyAutoSubnets: false,
	})
	if recorder.Code != 200 {
		t.Fatalf("network create status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/regions/us-central1/subnetworks", map[string]any{
		keyName: testSubnetOne, keyNetwork: testNetwork, keyIPCIDR: testSubnetCIDR,
	})
	if recorder.Code != 200 {
		t.Fatalf("subnetwork create status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/regions/us-central1/routers", map[string]any{
		keyName: "router-1", keyNetwork: testNetwork,
		"nats": []any{map[string]any{
			keyName: "nat-1", "sourceSubnetworkIpRangesToNat": listOfSubnetworks, "subnetworks": []any{map[string]any{keyName: testSubnetOne}},
		}},
	})
	if recorder.Code != 200 {
		t.Fatalf("router create status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodGet, "/compute/v1/projects/demo-net/regions/us-central1/routers/router-1", nil)
	if recorder.Code != 200 {
		t.Fatalf("router get status = %d", recorder.Code)
	}
	// A NAT referencing a missing subnetwork is refused.
	recorder = h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/regions/us-central1/routers", map[string]any{
		keyName: "router-2", keyNetwork: testNetwork,
		"nats": []any{map[string]any{
			keyName: "nat-2", "sourceSubnetworkIpRangesToNat": listOfSubnetworks, "subnetworks": []any{map[string]any{keyName: "missing-subnet"}},
		}},
	})
	if recorder.Code != 400 {
		t.Fatalf("missing subnetwork NAT status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodDelete, "/compute/v1/projects/demo-net/regions/us-central1/routers/router-1", nil)
	if recorder.Code != 200 {
		t.Fatalf("router delete status = %d", recorder.Code)
	}
}

// decode reads one JSON response body into a generic map.
func decode(t *testing.T, recorder *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var value map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &value); err != nil {
		t.Fatalf("decode %s: %v", recorder.Body.String(), err)
	}
	return value
}

// fieldSet renders the sorted top-level field names of one document.
func fieldSet(value map[string]any) string {
	names := make([]string, 0, len(value))
	for name := range value {
		names = append(names, name)
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}

// operationErrors lists the error codes of one finished compute operation.
func operationErrors(t *testing.T, recorder *httptest.ResponseRecorder) (string, []string) {
	t.Helper()
	if recorder.Code != 200 {
		t.Fatalf("operation status = %d body %s", recorder.Code, recorder.Body.String())
	}
	var operation struct {
		Status string `json:"status"`
		Error  struct {
			Errors []struct {
				Code string `json:"code"`
			} `json:"errors"`
		} `json:"error"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &operation); err != nil {
		t.Fatalf("decode operation: %v", err)
	}
	codes := make([]string, 0, len(operation.Error.Errors))
	for _, entry := range operation.Error.Errors {
		codes = append(codes, entry.Code)
	}
	return operation.Status, codes
}

// topology creates one custom-mode network with one subnetwork.
func (h *harness) topology(network, subnet, cidr string) {
	h.t.Helper()
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/networks", map[string]any{keyName: network, keyAutoSubnets: false})
	recorder := h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/regions/us-central1/subnetworks", map[string]any{
		keyName: subnet, keyNetwork: "projects/demo-net/global/networks/" + network, keyIPCIDR: cidr,
	})
	if status, errs := operationErrors(h.t, recorder); status != "DONE" || len(errs) != 0 {
		h.t.Fatalf("subnetwork %s operation = %s %v", subnet, status, errs)
	}
}

func TestNetworkInUseDeleteFailsInOperation(t *testing.T) {
	h := newHarness(t)
	h.topology(testNetwork, testSubnetOne, testSubnetCIDR)
	status, errs := operationErrors(t, h.invoke(http.MethodDelete, "/compute/v1/projects/demo-net/global/networks/vpc-1", nil))
	if status != "DONE" || len(errs) != 1 || errs[0] != "RESOURCE_IN_USE_BY_ANOTHER_RESOURCE" {
		t.Fatalf("in-use delete operation = %s %v", status, errs)
	}
	if recorder := h.invoke(http.MethodGet, "/compute/v1/projects/demo-net/global/networks/vpc-1", nil); recorder.Code != 200 {
		t.Fatalf("network kept status = %d", recorder.Code)
	}
	if recorder := h.invoke(http.MethodDelete, "/compute/v1/projects/demo-net/regions/us-central1/subnetworks/subnet-1", nil); recorder.Code != 200 {
		t.Fatalf("subnetwork delete status = %d", recorder.Code)
	}
	if recorder := h.invoke(http.MethodDelete, "/compute/v1/projects/demo-net/regions/us-central1/subnetworks/subnet-1", nil); recorder.Code != 404 {
		t.Fatalf("missing subnetwork delete status = %d", recorder.Code)
	}
	status, errs = operationErrors(t, h.invoke(http.MethodDelete, "/compute/v1/projects/demo-net/global/networks/vpc-1", nil))
	if status != "DONE" || len(errs) != 0 {
		t.Fatalf("network delete operation = %s %v", status, errs)
	}
	if recorder := h.invoke(http.MethodGet, "/compute/v1/projects/demo-net/global/networks/vpc-1", nil); recorder.Code != 404 {
		t.Fatalf("deleted network get status = %d", recorder.Code)
	}
}

func TestOverlappingSubnetworkFailsInOperation(t *testing.T) {
	h := newHarness(t)
	h.topology(testNetwork, testSubnetOne, testRangeA)
	recorder := h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/regions/us-central1/subnetworks", map[string]any{
		keyName: "bad", keyNetwork: "projects/demo-net/global/networks/" + testNetwork, keyIPCIDR: "10.10.0.128/25",
	})
	var operation struct {
		SelfLink string `json:"selfLink"`
	}
	_ = json.Unmarshal(recorder.Body.Bytes(), &operation)
	status, errs := operationErrors(t, recorder)
	if status != "DONE" || len(errs) != 1 || errs[0] != "INVALID_USAGE" {
		t.Fatalf("overlap operation = %s %v", status, errs)
	}
	// The failed operation stays readable through regionOperations.get.
	path := strings.TrimPrefix(operation.SelfLink, "https://www.googleapis.com")
	if _, errs = operationErrors(t, h.invoke(http.MethodGet, path, nil)); len(errs) != 1 {
		t.Fatalf("operation read %s errors = %v", path, errs)
	}
	if recorder := h.invoke(http.MethodGet, "/compute/v1/projects/demo-net/regions/us-central1/subnetworks/bad", nil); recorder.Code != 404 {
		t.Fatalf("refused subnetwork get status = %d", recorder.Code)
	}
}

func TestDocumentsMatchRecordedFieldSets(t *testing.T) {
	h := newHarness(t)
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/networks", map[string]any{
		keyName: testNetwork, keyAutoSubnets: false, keyDescription: "described",
		"routingConfig": map[string]any{"routingMode": "REGIONAL"},
	})
	network := decode(t, h.invoke(http.MethodGet, "/compute/v1/projects/demo-net/global/networks/vpc-1", nil))
	if got := fieldSet(network); got != "autoCreateSubnetworks,creationTimestamp,description,id,kind,name,"+
		"networkFirewallPolicyEnforcementOrder,routingConfig,selfLink,selfLinkWithId" {
		t.Fatalf("network fields = %s", got)
	}
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/regions/us-central1/subnetworks", map[string]any{
		keyName: testSubnetOne, keyNetwork: "projects/demo-net/global/networks/vpc-1", keyIPCIDR: testRangeA, "region": "us-central1",
	})
	subnet := decode(t, h.invoke(http.MethodGet, "/compute/v1/projects/demo-net/regions/us-central1/subnetworks/subnet-1", nil))
	if got := fieldSet(subnet); got != "allowSubnetCidrRoutesOverlap,creationTimestamp,fingerprint,gatewayAddress,id,"+
		"ipCidrRange,kind,name,network,privateIpGoogleAccess,privateIpv6GoogleAccess,purpose,region,selfLink,stackType" {
		t.Fatalf("subnetwork fields = %s", got)
	}
	if subnet["region"] != "https://www.googleapis.com/compute/v1/projects/demo-net/regions/us-central1" ||
		subnet[keyNetwork] != "https://www.googleapis.com/compute/v1/projects/demo-net/global/networks/vpc-1" {
		t.Fatalf("subnetwork links = %v %v", subnet["region"], subnet[keyNetwork])
	}
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/firewalls", map[string]any{
		keyName: testFirewall, keyNetwork: "projects/demo-net/global/networks/vpc-1", keyDirection: directionIngress,
		keyPriority: 1000, "sourceRanges": []any{testRangeA},
		"allowed": []any{map[string]any{keyIPProto: "tcp", keyPorts: []any{"8080"}}},
	})
	firewall := decode(t, h.invoke(http.MethodGet, "/compute/v1/projects/demo-net/global/firewalls/fw-1", nil))
	if got := fieldSet(firewall); got != "allowed,creationTimestamp,description,direction,disabled,id,kind,"+
		"logConfig,name,network,priority,selfLink,sourceRanges" {
		t.Fatalf("firewall fields = %s", got)
	}
}

// analyzeFlow posts one flow to the local analyzer and decodes the verdict.
func (h *harness) analyzeFlow(source, dest string, port int) Reachability {
	h.t.Helper()
	spec := map[string]any{
		keyProject: testProject, keySourceIP: source, keyDestIP: dest,
		keyProtocol: "tcp", keyPort: port,
	}
	encoded, _ := json.Marshal(spec) //nolint:errchkjson // plain map bodies
	request, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, "/_local/networking/reachability", bytes.NewReader(encoded))
	recorder := httptest.NewRecorder()
	h.analyzerHandler.ServeHTTP(recorder, request)
	var verdict Reachability
	if err := json.Unmarshal(recorder.Body.Bytes(), &verdict); err != nil {
		h.t.Fatalf("decode verdict: %v", err)
	}
	return verdict
}

// Shared expectations of the analyzer tests.
const (
	dropToInternet = "DROP PRIVATE_TRAFFIC_TO_INTERNET"
)

// expectCode issues one request and fails unless it returns the status code.
func (h *harness) expectCode(verb, path string, body any, code int) *httptest.ResponseRecorder {
	h.t.Helper()
	recorder := h.invoke(verb, path, body)
	if recorder.Code != code {
		h.t.Fatalf("%s %s status = %d, want %d: %s", verb, path, recorder.Code, code, recorder.Body.String())
	}
	return recorder
}

// expectDropToInternet asserts the recorded unknown private-destination drop.
func (h *harness) expectDropToInternet(source, dest string) {
	h.t.Helper()
	verdict := h.analyzeFlow(source, dest, 443)
	if verdict.Result != verdictDropped || verdict.Trace[len(verdict.Trace)-1] != dropToInternet {
		h.t.Fatalf("%s -> %s verdict = %+v", source, dest, verdict)
	}
}

// The recorded unknown-endpoint trace: the destination has no modeled endpoint.
const unknownIPTrace = "START_FROM_PRIVATE_NETWORK|APPLY_EGRESS_FIREWALL_RULE default-allow-egress|" +
	"APPLY_ROUTE subnet-1|ABORT UNKNOWN_IP"

func TestAnalyzerReportsUnknownIPRegardlessOfIngress(t *testing.T) {
	h := newHarness(t)
	h.topology(testNetwork, testSubnetOne, testSubnetCIDR)
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/firewalls", map[string]any{
		keyName: testFirewall, keyNetwork: testNetwork, keyDirection: directionIngress,
		"allowed":      []any{map[string]any{keyIPProto: "tcp", keyPorts: []any{"443"}}},
		"sourceRanges": []any{testSourceCIDR},
	})
	for _, port := range []int{443, 80} {
		verdict := h.analyzeFlow("10.0.0.5", "10.0.0.9", port)
		if verdict.Result != verdictUnknown || strings.Join(verdict.Trace, "|") != unknownIPTrace {
			t.Fatalf("port %d verdict = %+v", port, verdict)
		}
	}
	// A private address outside every subnet leaves through the default route.
	verdict := h.analyzeFlow("10.0.0.5", "10.0.99.1", 443)
	if verdict.Result != verdictDropped || verdict.Trace[len(verdict.Trace)-1] != dropToInternet {
		t.Fatalf("unrouted private destination = %+v", verdict)
	}
	// A source outside the modeled subnets is an explicit gap, never a verdict.
	if verdict := h.analyzeFlow("192.0.2.1", "10.0.0.9", 443); verdict.Result != verdictUnsupported {
		t.Fatalf("foreign source flow = %+v", verdict)
	}
}

func TestEmptyTopologyReportsUnsupported(t *testing.T) {
	h := newHarness(t)
	if verdict := h.analyzeFlow("10.0.0.5", "10.0.16.9", 443); verdict.Result != verdictUnsupported {
		t.Fatalf("empty topology verdict = %+v", verdict)
	}
}

// peeringStates reads the peering states of one network.
func (h *harness) peeringStates(network string) []string {
	h.t.Helper()
	var document struct {
		Peerings []struct {
			State string `json:"state"`
		} `json:"peerings"`
	}
	recorder := h.invoke(http.MethodGet, "/compute/v1/projects/demo-net/global/networks/"+network, nil)
	if err := json.Unmarshal(recorder.Body.Bytes(), &document); err != nil {
		h.t.Fatalf("decode network: %v", err)
	}
	states := make([]string, 0, len(document.Peerings))
	for _, peering := range document.Peerings {
		states = append(states, peering.State)
	}
	return states
}

// addPeering peers local to remote with the recorded nested body.
func (h *harness) addPeering(local, remote, name string) *httptest.ResponseRecorder {
	return h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/networks/"+local+"/addPeering", map[string]any{
		"networkPeering": map[string]any{
			keyName: name, "exchangeSubnetRoutes": true,
			keyNetwork: "https://www.googleapis.com/compute/v1/projects/demo-net/global/networks/" + remote,
		},
	})
}

func TestPeeringStatesAndCrossNetworkDrop(t *testing.T) {
	h := newHarness(t)
	h.topology(testNetwork, testSubnetOne, testSubnetCIDR)
	h.topology(testPeerNetwork, testPeerSubnet, "10.1.0.0/20")
	h.expectDropToInternet("10.0.0.5", "10.1.0.9")
	if status, errs := operationErrors(t, h.addPeering(testNetwork, testPeerNetwork, testPeering)); status != "DONE" || len(errs) != 0 {
		t.Fatalf("addPeering operation = %s %v", status, errs)
	}
	if states := h.peeringStates(testNetwork); strings.Join(states, ",") != "INACTIVE" {
		t.Fatalf("one-sided peering states = %v", states)
	}
	h.addPeering(testPeerNetwork, testNetwork, "peering-back")
	if states := h.peeringStates(testNetwork); strings.Join(states, ",") != "ACTIVE" {
		t.Fatalf("two-sided peering states = %v", states)
	}
	// Recorded: GCP still drops an unknown peer-network IP right after peering.
	h.expectDropToInternet("10.0.0.5", "10.1.0.9")
	path := "/compute/v1/projects/demo-net/global/networks/" + testNetwork + "/removePeering"
	h.expectCode(http.MethodPost, path, map[string]any{keyName: testPeering}, 200)
	h.expectCode(http.MethodPost, path, map[string]any{keyName: testPeering}, 400)
	if states := h.peeringStates(testPeerNetwork); strings.Join(states, ",") != "INACTIVE" {
		t.Fatalf("orphaned peering states = %v", states)
	}
}

func TestDuplicatePeeringIsRefused(t *testing.T) {
	h := newHarness(t)
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/networks", map[string]any{keyName: testNetwork})
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/networks", map[string]any{keyName: testPeerNetwork})
	legacy := map[string]any{keyName: testPeering, keyPeerNetwork: testPeerNetwork}
	if recorder := h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/networks/"+testNetwork+"/addPeering", legacy); recorder.Code != 200 {
		t.Fatalf("addPeering status = %d", recorder.Code)
	}
	if recorder := h.addPeering(testNetwork, testPeerNetwork, testPeering); recorder.Code != 409 {
		t.Fatalf("duplicate peering status = %d body %s", recorder.Code, recorder.Body.String())
	}
	cross := h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/networks/"+testNetwork+"/addPeering", map[string]any{
		"networkPeering": map[string]any{keyName: "cross", keyNetwork: "projects/other/global/networks/" + testPeerNetwork},
	})
	if cross.Code != 501 {
		t.Fatalf("cross-project peering status = %d", cross.Code)
	}
}

func TestEgressDenyDropsBeforeRouting(t *testing.T) {
	h := newHarness(t)
	h.topology(testNetwork, testSubnetOne, testSubnetCIDR)
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/firewalls", map[string]any{
		keyName: "egress-allow", keyNetwork: testNetwork, keyDirection: directionEgress,
		"allowed": []any{map[string]any{keyIPProto: "all"}}, keyPriority: 100,
	})
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/firewalls", map[string]any{
		keyName: "egress-block", keyNetwork: testNetwork, keyDirection: directionEgress,
		"denied": []any{map[string]any{keyIPProto: "all"}}, keyPriority: 100,
	})
	verdict := h.analyzeFlow("10.0.4.5", "10.0.0.9", 443)
	want := "START_FROM_PRIVATE_NETWORK|APPLY_EGRESS_FIREWALL_RULE egress-block|DROP FIREWALL_RULE"
	if verdict.Result != verdictDropped || strings.Join(verdict.Trace, "|") != want {
		t.Fatalf("egress-denied verdict = %+v", verdict)
	}
}

func TestEgressAllowStillReportsUnknownIP(t *testing.T) {
	h := newHarness(t)
	h.topology(testNetwork, testSubnetOne, testSubnetCIDR)
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/firewalls", map[string]any{
		keyName: "egress-all", keyNetwork: testNetwork, keyDirection: directionEgress,
		"allowed": []any{map[string]any{keyIPProto: "all"}},
	})
	verdict := h.analyzeFlow("10.0.0.5", "10.0.0.9", 443)
	if verdict.Result != verdictUnknown || verdict.Trace[1] != "APPLY_EGRESS_FIREWALL_RULE egress-all" {
		t.Fatalf("egress-allowed verdict = %+v", verdict)
	}
}

// connectivityBody is one recorded-shape ConnectivityTest body.
func connectivityBody(source string, port int) map[string]any {
	endpoint := func(ip string) map[string]any {
		return map[string]any{keyIPAddress: ip, keyNetwork: "projects/demo-net/global/networks/" + testNetwork, "projectId": testProject}
	}
	destination := endpoint("10.0.0.9")
	destination[keyPort] = port
	return map[string]any{keySource: endpoint(source), keyDestination: destination, keyProtocol: "TCP"}
}

// testResult reads the stored result and step states of one test.
func (h *harness) testResult(id string) (string, []string) {
	h.t.Helper()
	recorder := h.invoke(http.MethodGet, "/v1/projects/demo-net/locations/global/connectivityTests/"+id, nil)
	var test struct {
		ReachabilityDetails struct {
			Result string `json:"result"`
			Traces []struct {
				Steps []struct {
					State string `json:"state"`
				} `json:"steps"`
			} `json:"traces"`
		} `json:"reachabilityDetails"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &test); err != nil {
		h.t.Fatalf("decode test: %v", err)
	}
	states := []string{}
	for _, trace := range test.ReachabilityDetails.Traces {
		for _, step := range trace.Steps {
			states = append(states, step.State)
		}
	}
	return test.ReachabilityDetails.Result, states
}

// connectivityTests is the Connectivity Tests collection path of the harness project.
const connectivityTests = "/v1/projects/demo-net/locations/global/connectivityTests"

func TestConnectivityTestsLifecycle(t *testing.T) {
	h := newHarness(t)
	h.topology(testNetwork, testSubnetOne, testSubnetCIDR)
	operation := decode(t, h.expectCode(http.MethodPost, connectivityTests+"?testId=check-web", connectivityBody("10.0.0.5", 8080), 200))
	name, _ := operation[keyName].(string)
	if operation["done"] != true || !strings.HasPrefix(name, "projects/demo-net/locations/global/operations/") {
		t.Fatalf("create operation = %v", operation)
	}
	test := decode(t, h.invoke(http.MethodGet, connectivityTests+"/check-web", nil))
	details, _ := test["reachabilityDetails"].(map[string]any)
	if got := fieldSet(test) + "|" + fieldSet(details); got != "createTime,destination,name,protocol,reachabilityDetails,source,updateTime|result,traces,verifyTime" {
		t.Fatalf("test fields = %s", got)
	}
	result, states := h.testResult("check-web")
	if result != resultUndetermined || strings.Join(states, ",") != "START_FROM_PRIVATE_NETWORK,APPLY_EGRESS_FIREWALL_RULE,APPLY_ROUTE,ABORT" {
		t.Fatalf("stored result = %s %v", result, states)
	}
	h.expectCode(http.MethodPost, connectivityTests+"?testId=check-web", connectivityBody("10.0.0.5", 8080), 409)
	h.expectCode(http.MethodPost, connectivityTests+"?testId=foreign", connectivityBody("192.0.2.1", 80), 501)
	if recorder := h.invoke(http.MethodPost, connectivityTests+":run", connectivityBody("10.0.0.5", 80)); recorder.Code == 200 {
		t.Fatal("the invented :run route still answers")
	}
	listing := decode(t, h.invoke(http.MethodGet, connectivityTests, nil))
	if items, _ := listing["resources"].([]any); len(items) != 1 {
		t.Fatalf("listing = %v", listing)
	}
	if deleted := decode(t, h.expectCode(http.MethodDelete, connectivityTests+"/check-web", nil, 200)); deleted["done"] != true {
		t.Fatalf("delete operation = %v", deleted)
	}
	h.expectCode(http.MethodGet, connectivityTests+"/check-web", nil, 404)
}

func TestConnectivityRerunAnalyzesCurrentTopology(t *testing.T) {
	h := newHarness(t)
	h.topology(testNetwork, testSubnetOne, testSubnetCIDR)
	tests := connectivityTests
	h.invoke(http.MethodPost, tests+"?testId=check-web", connectivityBody("10.0.0.5", 443))
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/firewalls", map[string]any{
		keyName: "egress-block", keyNetwork: testNetwork, keyDirection: directionEgress,
		"denied": []any{map[string]any{keyIPProto: "all"}},
	})
	// Reads return the stored analysis; only rerun re-evaluates.
	if result, _ := h.testResult("check-web"); result != resultUndetermined {
		t.Fatalf("stored result before rerun = %s", result)
	}
	if recorder := h.invoke(http.MethodPost, tests+"/check-web:rerun", nil); recorder.Code != 200 {
		t.Fatalf("rerun status = %d body %s", recorder.Code, recorder.Body.String())
	}
	if result, states := h.testResult("check-web"); result != resultUnreachable || states[len(states)-1] != "DROP" {
		t.Fatalf("rerun result = %s %v", result, states)
	}
}

func TestConnectivityTestsRequireNetworkManagementPermissions(t *testing.T) {
	h := newHarness(t)
	request, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/projects/demo-net/locations/global/connectivityTests", nil)
	request.Header.Set("Authorization", "Bearer stranger")
	recorder := httptest.NewRecorder()
	h.gate.ServeHTTP(recorder, request)
	if recorder.Code != 403 || !strings.Contains(recorder.Body.String(), "PERMISSION_DENIED") {
		t.Fatalf("stranger list = %d %s", recorder.Code, recorder.Body.String())
	}
}

func TestAnalyzerDoesNotClaimNATEgress(t *testing.T) {
	h := newHarness(t)
	h.topology(testNetwork, testSubnetOne, testSubnetCIDR)
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/regions/us-central1/routers", map[string]any{
		keyName: "router-1", keyNetwork: testNetwork,
		"nats": []any{map[string]any{keyName: "nat-1", "sourceSubnetworkIpRangesToNat": allSubnetworks}},
	})
	// External destinations and NAT are unrecorded: an explicit gap, not a pass.
	if verdict := h.analyzeFlow("10.0.1.5", "203.0.113.9", 443); verdict.Result != verdictUnsupported {
		t.Fatalf("NAT egress verdict = %+v", verdict)
	}
}

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

// listNames lists one collection and returns its item names in order.
func (h *harness) listNames(path string) []string {
	h.t.Helper()
	recorder := h.invoke(http.MethodGet, path, nil)
	var list struct {
		Items []struct {
			Name string `json:"name"`
		} `json:"items"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &list); err != nil || recorder.Code != 200 {
		h.t.Fatalf("list %s = %d %s", path, recorder.Code, recorder.Body.String())
	}
	names := make([]string, 0, len(list.Items))
	for _, item := range list.Items {
		names = append(names, item.Name)
	}
	return names
}

// Compute lists every collection in name order, whatever order the resources
// were created in (recorded: vm-firewall-service-accounts).
func TestListsAreOrderedByName(t *testing.T) {
	h := newHarness(t)
	names := []string{"n7", "n6", "n5", "n4", "n3", "n2", "n1", "n0"}
	for i, name := range names {
		h.expectCode(http.MethodPost, "/compute/v1/projects/demo-net/global/networks", map[string]any{keyName: name, keyAutoSubnets: false}, 200)
		h.expectCode(http.MethodPost, "/compute/v1/projects/demo-net/regions/us-central1/subnetworks", map[string]any{
			keyName: "s" + name, keyNetwork: name, keyIPCIDR: "10." + strconv.Itoa(i) + ".0.0/24",
		}, 200)
		h.expectCode(http.MethodPost, "/compute/v1/projects/demo-net/global/firewalls", map[string]any{
			keyName: "f" + name, keyNetwork: name, "allowed": []any{map[string]any{keyIPProto: "tcp"}},
		}, 200)
		h.expectCode(http.MethodPost, "/compute/v1/projects/demo-net/global/routes", map[string]any{
			keyName: "r" + name, keyNetwork: name, keyDestRange: "10.200." + strconv.Itoa(i) + ".0/24", "nextHopIp": "10.0.0.9",
		}, 200)
	}
	sorted := slices.Sorted(slices.Values(names))
	for prefix, path := range map[string]string{
		"":  "/compute/v1/projects/demo-net/global/networks",
		"s": "/compute/v1/projects/demo-net/regions/us-central1/subnetworks",
		"f": "/compute/v1/projects/demo-net/global/firewalls",
		"r": "/compute/v1/projects/demo-net/global/routes",
	} {
		want := make([]string, 0, len(sorted))
		for _, name := range sorted {
			want = append(want, prefix+name)
		}
		if got := h.listNames(path); !slices.Equal(got, want) {
			t.Fatalf("%s order = %v, want %v", path, got, want)
		}
	}
}

// Subnetworks and routers are regional: a name exists once per region, and
// reads, lists, and deletes see only the request's region (recorded:
// vm-firewall-service-accounts reads a us-central1 subnetwork through us-east1).
func TestSubnetworksAndRoutersAreRegional(t *testing.T) {
	h := newHarness(t)
	const central, east = "/compute/v1/projects/demo-net/regions/us-central1", "/compute/v1/projects/demo-net/regions/us-east1"
	h.expectCode(http.MethodPost, "/compute/v1/projects/demo-net/global/networks", map[string]any{keyName: testNetwork, keyAutoSubnets: false}, 200)
	h.expectCode(http.MethodPost, central+"/subnetworks", map[string]any{keyName: testSubnetOne, keyNetwork: testNetwork, keyIPCIDR: "10.1.0.0/24"}, 200)
	h.expectCode(http.MethodGet, east+"/subnetworks/"+testSubnetOne, nil, 404)
	if names := h.listNames(east + "/subnetworks"); len(names) != 0 {
		t.Fatalf("us-east1 subnetworks = %v", names)
	}
	status, errs := operationErrors(t, h.expectCode(http.MethodPost, east+"/subnetworks", map[string]any{
		keyName: testSubnetOne, keyNetwork: testNetwork, keyIPCIDR: "10.2.0.0/24",
	}, 200))
	if status != "DONE" || len(errs) != 0 {
		t.Fatalf("same name in another region = %s %v", status, errs)
	}
	body := decode(t, h.expectCode(http.MethodGet, east+"/subnetworks/"+testSubnetOne, nil, 200))
	if body[keyIPCIDR] != "10.2.0.0/24" {
		t.Fatalf("us-east1 subnetwork = %v", body)
	}
	router := func(region string) map[string]any {
		return map[string]any{keyName: "router-1", keyNetwork: testNetwork, "nats": []any{map[string]any{
			keyName: "nat-1", "sourceSubnetworkIpRangesToNat": listOfSubnetworks,
			"subnetworks": []any{map[string]any{keyName: "regions/" + region + "/subnetworks/" + testSubnetOne}},
		}}}
	}
	h.expectCode(http.MethodPost, central+"/routers", router("us-central1"), 200)
	h.expectCode(http.MethodGet, east+"/routers/router-1", nil, 404)
	h.expectCode(http.MethodPost, east+"/routers", router("us-central1"), 400) // a NAT names its own region's subnetworks
	h.expectCode(http.MethodPost, east+"/routers", router("us-east1"), 200)
	h.expectCode(http.MethodDelete, east+"/routers/router-1", nil, 200)
	h.expectCode(http.MethodGet, central+"/routers/router-1", nil, 200)
	h.expectCode(http.MethodDelete, east+"/subnetworks/"+testSubnetOne, nil, 200)
	body = decode(t, h.expectCode(http.MethodGet, central+"/subnetworks/"+testSubnetOne, nil, 200))
	if body[keyIPCIDR] != "10.1.0.0/24" {
		t.Fatalf("us-central1 subnetwork after the us-east1 delete = %v", body)
	}
}
