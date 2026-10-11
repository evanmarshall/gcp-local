package compute

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
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
	testNetwork      = "vpc-1"
	testFirewall     = "fw-1"
	testAllowed      = "allowed"
	testProject      = "demo-net"
	testKindProj     = "project"
	testSubnetOne    = "subnet-1"
	testPeerNetwork  = "peer-net"
	testPeerSubnet   = "peer-subnet"
	testPeering      = "peering-1"
	testTCP          = "tcp"
	testSubnetCIDR   = "10.0.0.0/20"
	testSourceCIDR   = "10.0.0.0/8"
	keyNats          = "nats"
	keySourceSubnets = "sourceSubnetworkIpRanges"
	keySourceRanges  = "sourceRanges"
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
	if operation.Status != opDone {
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
		keyNats: []any{map[string]any{keyName: "nat-1", keySourceSubnets: []any{testSubnetOne}}},
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
		keyNats: []any{map[string]any{keyName: "nat-2", keySourceSubnets: []any{"missing-subnet"}}},
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
	if status, errs := operationErrors(h.t, recorder); status != opDone || len(errs) != 0 {
		h.t.Fatalf("subnetwork %s operation = %s %v", subnet, status, errs)
	}
}

func TestNetworkInUseDeleteFailsInOperation(t *testing.T) {
	h := newHarness(t)
	h.topology(testNetwork, testSubnetOne, testSubnetCIDR)
	status, errs := operationErrors(t, h.invoke(http.MethodDelete, "/compute/v1/projects/demo-net/global/networks/vpc-1", nil))
	if status != opDone || len(errs) != 1 || errs[0] != "RESOURCE_IN_USE_BY_ANOTHER_RESOURCE" {
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
	if status != opDone || len(errs) != 0 {
		t.Fatalf("network delete operation = %s %v", status, errs)
	}
	if recorder := h.invoke(http.MethodGet, "/compute/v1/projects/demo-net/global/networks/vpc-1", nil); recorder.Code != 404 {
		t.Fatalf("deleted network get status = %d", recorder.Code)
	}
}

func TestOverlappingSubnetworkFailsInOperation(t *testing.T) {
	h := newHarness(t)
	h.topology(testNetwork, testSubnetOne, "10.10.0.0/24")
	recorder := h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/regions/us-central1/subnetworks", map[string]any{
		keyName: "bad", keyNetwork: "projects/demo-net/global/networks/" + testNetwork, keyIPCIDR: "10.10.0.128/25",
	})
	var operation struct {
		SelfLink string `json:"selfLink"`
	}
	_ = json.Unmarshal(recorder.Body.Bytes(), &operation)
	status, errs := operationErrors(t, recorder)
	if status != opDone || len(errs) != 1 || errs[0] != "INVALID_USAGE" {
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
		keyName: testSubnetOne, keyNetwork: "projects/demo-net/global/networks/vpc-1", keyIPCIDR: "10.10.0.0/24", "region": "us-central1",
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
		keyPriority: 1000, keySourceRanges: []any{"10.10.0.0/24"},
		testAllowed: []any{map[string]any{keyIPProto: testTCP, keyPorts: []any{"8080"}}},
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
		keyProtocol: testTCP, keyPort: port,
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
	opDone         = "DONE"
	testAll        = "all"
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
		testAllowed:     []any{map[string]any{keyIPProto: testTCP, keyPorts: []any{"443"}}},
		keySourceRanges: []any{testSourceCIDR},
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
	if status, errs := operationErrors(t, h.addPeering(testNetwork, testPeerNetwork, testPeering)); status != opDone || len(errs) != 0 {
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
		testAllowed: []any{map[string]any{keyIPProto: testAll}}, keyPriority: 100,
	})
	h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/global/firewalls", map[string]any{
		keyName: "egress-block", keyNetwork: testNetwork, keyDirection: directionEgress,
		"denied": []any{map[string]any{keyIPProto: testAll}}, keyPriority: 100,
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
		testAllowed: []any{map[string]any{keyIPProto: testAll}},
	})
	verdict := h.analyzeFlow("10.0.0.5", "10.0.0.9", 443)
	if verdict.Result != verdictUnknown || verdict.Trace[1] != "APPLY_EGRESS_FIREWALL_RULE egress-all" {
		t.Fatalf("egress-allowed verdict = %+v", verdict)
	}
}

// connectivityBody is one recorded-shape ConnectivityTest body.
func connectivityBody(source string, port int) map[string]any {
	endpoint := func(ip string) map[string]any {
		return map[string]any{"ipAddress": ip, keyNetwork: "projects/demo-net/global/networks/" + testNetwork, "projectId": testProject}
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
		"denied": []any{map[string]any{keyIPProto: testAll}},
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
		keyNats: []any{map[string]any{keyName: "nat-1", keySourceSubnets: []any{testSubnetOne}}},
	})
	// External destinations and NAT are unrecorded: an explicit gap, not a pass.
	if verdict := h.analyzeFlow("10.0.1.5", "203.0.113.9", 443); verdict.Result != verdictUnsupported {
		t.Fatalf("NAT egress verdict = %+v", verdict)
	}
}
