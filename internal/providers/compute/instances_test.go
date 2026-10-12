package compute

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// Names of the VM connectivity topology, mirroring the recorded scenario.
const (
	vmZone     = "us-central1-a"
	vmNetA     = "net-a"
	vmNetB     = "net-b"
	vmSubnetA  = "subnet-a"
	vmSubnetB  = "subnet-b"
	vmOne      = "vm1"
	vmTwo      = "vm2"
	vmThree    = "vm3"
	vmInternet = "8.8.8.8"
	zonePath   = "/compute/v1/projects/demo-net/zones/us-central1-a"
	regionPath = "/compute/v1/projects/demo-net/regions/us-central1"
	stepDrop   = "DROP"
	stepEgress = "APPLY_EGRESS_FIREWALL_RULE"
	stepRoute  = "APPLY_ROUTE"
	testRangeA = "10.10.0.0/24"
)

// Recorded step sequences of the VM connectivity scenario.
var (
	stepsDelivered    = []string{stepStartInstance, stepEgress, stepRoute, stepArriveInstance, "APPLY_INGRESS_FIREWALL_RULE", "DELIVER"}
	stepsIngressDrop  = []string{stepStartInstance, stepEgress, stepRoute, stepArriveInstance, "APPLY_INGRESS_FIREWALL_RULE", stepDrop}
	stepsRouteDrop    = []string{stepStartInstance, stepEgress, stepRoute, stepDrop}
	stepsNATDelivered = []string{stepStartInstance, stepEgress, stepRoute, "DELIVER"}
)

// instanceRef is the relative URI of one instance in the test zone.
func instanceRef(name string) string {
	return "projects/demo-net/zones/" + vmZone + "/instances/" + name
}

// vmBody is the recorded e2-micro insert body without external addresses.
func vmBody(name, subnet string) map[string]any {
	return map[string]any{
		keyName: name, "machineType": "zones/" + vmZone + "/machineTypes/e2-micro",
		"disks": []any{map[string]any{"boot": true, "autoDelete": true, "initializeParams": map[string]any{
			"sourceImage": "projects/debian-cloud/global/images/family/debian-12", "diskSizeGb": "10",
		}}},
		"networkInterfaces": []any{map[string]any{"subnetwork": "projects/demo-net/regions/us-central1/subnetworks/" + subnet}},
		"serviceAccounts":   []any{},
	}
}

// vmTopology builds the recorded topology: two networks with one subnetwork
// and one tcp:8080 ingress rule each, vm1 and vm2 in a, vm3 in b.
func (h *harness) vmTopology() {
	h.t.Helper()
	for _, net := range []struct{ network, subnet, cidr string }{
		{vmNetA, vmSubnetA, testRangeA}, {vmNetB, vmSubnetB, "10.20.0.0/24"},
	} {
		h.topology(net.network, net.subnet, net.cidr)
		h.expectCode(http.MethodPost, "/compute/v1/projects/demo-net/global/firewalls", map[string]any{
			keyName: net.network + "-allow", keyNetwork: "projects/demo-net/global/networks/" + net.network,
			keyDirection: directionIngress, keyPriority: 1000, "sourceRanges": []any{testRangeA},
			"allowed": []any{map[string]any{keyIPProto: "tcp", keyPorts: []any{"8080"}}},
		}, 200)
	}
	for _, vm := range []struct{ name, subnet string }{{vmOne, vmSubnetA}, {vmTwo, vmSubnetA}, {vmThree, vmSubnetB}} {
		if status, errs := operationErrors(h.t, h.invoke(http.MethodPost, zonePath+"/instances", vmBody(vm.name, vm.subnet))); status != "DONE" || len(errs) != 0 {
			h.t.Fatalf("instance %s operation = %s %v", vm.name, status, errs)
		}
	}
}

// verdictOf is the recorded verdict projection of one Connectivity Test.
type verdictOf struct {
	result, cause, firewall string
	steps                   []string
}

// instanceTest creates one test from vm1 and reads its verdict.
func (h *harness) instanceTest(id, destination string, port int) verdictOf {
	h.t.Helper()
	target := map[string]any{kindInstance: instanceRef(destination), keyPort: port}
	if destination == vmInternet {
		target = map[string]any{keyIPAddress: vmInternet, keyPort: port}
	}
	h.expectCode(http.MethodPost, connectivityTests+"?testId="+id, map[string]any{
		keySource:      map[string]any{kindInstance: instanceRef(vmOne)},
		keyDestination: target, keyProtocol: "TCP",
	}, 200)
	var test struct {
		ReachabilityDetails struct {
			Result string `json:"result"`
			Traces []struct {
				Steps []struct {
					State    string `json:"state"`
					Firewall struct {
						DisplayName string `json:"displayName"`
					} `json:"firewall"`
					Drop struct {
						Cause string `json:"cause"`
					} `json:"drop"`
				} `json:"steps"`
			} `json:"traces"`
		} `json:"reachabilityDetails"`
	}
	if err := json.Unmarshal(h.invoke(http.MethodGet, connectivityTests+"/"+id, nil).Body.Bytes(), &test); err != nil {
		h.t.Fatalf("decode test: %v", err)
	}
	verdict := verdictOf{result: test.ReachabilityDetails.Result}
	for _, step := range test.ReachabilityDetails.Traces[0].Steps {
		verdict.steps = append(verdict.steps, step.State)
		if verdict.firewall == "" {
			verdict.firewall = step.Firewall.DisplayName
		}
		verdict.cause = step.Drop.Cause
	}
	return verdict
}

// expectVerdict asserts one recorded verdict, including the first firewall
// step's implied default-allow-egress rule.
func (h *harness) expectVerdict(got verdictOf, result, cause string, steps []string) {
	h.t.Helper()
	if got.result != result || got.cause != cause || strings.Join(got.steps, ",") != strings.Join(steps, ",") ||
		got.firewall != "default-allow-egress" {
		h.t.Fatalf("verdict = %+v, want %s %s %v", got, result, cause, steps)
	}
}

func TestInstanceToInstanceOpenPortIsReachable(t *testing.T) {
	h := newHarness(t)
	h.vmTopology()
	h.expectVerdict(h.instanceTest("t-open", vmTwo, 8080), resultReachable, "", stepsDelivered)
}

func TestInstanceToInstanceClosedPortDropsAtIngress(t *testing.T) {
	h := newHarness(t)
	h.vmTopology()
	h.expectVerdict(h.instanceTest("t-closed", vmTwo, 9090), resultUnreachable, "FIREWALL_RULE", stepsIngressDrop)
}

func TestUnpeeredInstanceDropsAndPeeredIsReachable(t *testing.T) {
	h := newHarness(t)
	h.vmTopology()
	h.expectVerdict(h.instanceTest("t-unpeered", vmThree, 8080), resultUnreachable, "PRIVATE_TRAFFIC_TO_INTERNET", stepsRouteDrop)
	h.addPeering(vmNetA, vmNetB, "a-to-b")
	// One-sided peering is unrecorded: an explicit gap, not a verdict.
	h.expectCode(http.MethodPost, connectivityTests+"?testId=t-half", map[string]any{
		keySource:      map[string]any{kindInstance: instanceRef(vmOne)},
		keyDestination: map[string]any{kindInstance: instanceRef(vmThree), keyPort: 8080},
	}, 501)
	h.addPeering(vmNetB, vmNetA, "b-to-a")
	h.expectVerdict(h.instanceTest("t-peered", vmThree, 8080), resultReachable, "", stepsDelivered)
}

func TestInstanceInternetEgressNeedsCloudNAT(t *testing.T) {
	h := newHarness(t)
	h.vmTopology()
	h.expectVerdict(h.instanceTest("t-nonat", vmInternet, 443), resultUnreachable, "PRIVATE_GOOGLE_ACCESS_DISALLOWED", stepsRouteDrop)
	if status, errs := operationErrors(t, h.invoke(http.MethodPost, regionPath+"/routers", map[string]any{
		keyName: "router", keyNetwork: "projects/demo-net/global/networks/" + vmNetA, keyRegion: "us-central1",
		"nats": []any{map[string]any{keyName: "nat", "natIpAllocateOption": natAutoOnly, "sourceSubnetworkIpRangesToNat": allSubnetworks}},
	})); status != "DONE" || len(errs) != 0 {
		t.Fatalf("router operation = %s %v", status, errs)
	}
	h.expectVerdict(h.instanceTest("t-nat", vmInternet, 443), resultReachable, "", stepsNATDelivered)
	// Other internet destinations were not recorded.
	h.expectCode(http.MethodPost, connectivityTests+"?testId=t-other", map[string]any{
		keySource:      map[string]any{kindInstance: instanceRef(vmOne)},
		keyDestination: map[string]any{keyIPAddress: "203.0.113.9", keyPort: 443},
	}, 501)
}

func TestInstanceLifecycleAndAddresses(t *testing.T) {
	h := newHarness(t)
	h.vmTopology()
	addresses := make([]string, 0, 3)
	for _, name := range []string{vmOne, vmTwo, vmThree} {
		instance := decode(t, h.expectCode(http.MethodGet, zonePath+"/instances/"+name, nil, 200))
		nics, _ := instance["networkInterfaces"].([]any)
		nic, _ := nics[0].(map[string]any)
		addresses = append(addresses, provider.Text(nic["networkIP"]))
	}
	if got := strings.Join(addresses, ","); got != "10.10.0.2,10.10.0.3,10.20.0.2" {
		t.Fatalf("instance addresses = %s", got)
	}
	listing := decode(t, h.expectCode(http.MethodGet, zonePath+"/instances", nil, 200))
	if items, _ := listing[keyItems].([]any); len(items) != 3 {
		t.Fatalf("instance listing = %v", listing)
	}
	h.expectCode(http.MethodPost, zonePath+"/instances", vmBody(vmOne, vmSubnetA), 409)
	// The subnetwork and network an instance uses are refused in the operation.
	status, errs := operationErrors(t, h.invoke(http.MethodDelete, regionPath+"/subnetworks/"+vmSubnetB, nil))
	if status != "DONE" || len(errs) != 1 || errs[0] != "RESOURCE_IN_USE_BY_ANOTHER_RESOURCE" {
		t.Fatalf("in-use subnetwork delete = %s %v", status, errs)
	}
	deleted := decode(t, h.expectCode(http.MethodDelete, zonePath+"/instances/"+vmThree, nil, 200))
	path := strings.TrimPrefix(provider.Text(deleted[keySelfLink]), "https://www.googleapis.com")
	if operation := decode(t, h.expectCode(http.MethodGet, path, nil, 200)); operation["status"] != "DONE" {
		t.Fatalf("zone operation = %v", operation)
	}
	h.expectCode(http.MethodGet, zonePath+"/instances/"+vmThree, nil, 404)
	h.expectCode(http.MethodDelete, zonePath+"/instances/"+vmThree, nil, 404)
	if status, errs := operationErrors(t, h.invoke(http.MethodDelete, regionPath+"/subnetworks/"+vmSubnetB, nil)); status != "DONE" || len(errs) != 0 {
		t.Fatalf("free subnetwork delete = %s %v", status, errs)
	}
}

func TestInstanceInsertGapsAndValidation(t *testing.T) {
	h := newHarness(t)
	h.topology(vmNetA, vmSubnetA, testRangeA)
	external := vmBody("ext", vmSubnetA)
	nics, _ := external["networkInterfaces"].([]any)
	nic, _ := nics[0].(map[string]any)
	nic["accessConfigs"] = []any{map[string]any{"type": "ONE_TO_ONE_NAT"}}
	h.expectCode(http.MethodPost, zonePath+"/instances", external, 501)
	h.expectCode(http.MethodPost, zonePath+"/instances", vmBody("orphan", "missing"), 400)
	h.expectCode(http.MethodPost, "/compute/v1/projects/demo-net/zones/europe-west1-b/instances", vmBody("far", vmSubnetA), 400)
	taken := vmBody("fixed", vmSubnetA)
	nics, _ = taken["networkInterfaces"].([]any)
	nic, _ = nics[0].(map[string]any)
	nic["networkIP"] = "10.10.0.1"
	h.expectCode(http.MethodPost, zonePath+"/instances", taken, 400)
}

func TestInstanceEndpointsKeepAddressPathsHonest(t *testing.T) {
	h := newHarness(t)
	h.vmTopology()
	// An address that an instance owns is no longer an unknown IP: a gap.
	if verdict := h.analyzeFlow("10.10.0.9", "10.10.0.3", 8080); verdict.Result != verdictUnsupported {
		t.Fatalf("address owned by an instance = %+v", verdict)
	}
	// Unowned addresses keep the recorded IP-only verdict.
	if verdict := h.analyzeFlow("10.10.0.9", "10.10.0.50", 8080); verdict.Result != verdictUnknown {
		t.Fatalf("unowned address = %+v", verdict)
	}
	// A rule scoped to tags or service accounts that could decide an
	// IP-address flow is an explicit gap: scoped rules need instance endpoints.
	h.expectCode(http.MethodPost, "/compute/v1/projects/demo-net/global/firewalls", map[string]any{
		keyName: "tagged-egress", keyNetwork: "projects/demo-net/global/networks/" + vmNetA, keyDirection: directionEgress,
		"targetTags": []any{"web"}, "denied": []any{map[string]any{keyIPProto: "tcp", keyPorts: []any{"8080"}}},
	}, 200)
	if verdict := h.analyzeFlow("10.10.0.9", "10.10.0.50", 8080); verdict.Result != verdictUnsupported {
		t.Fatalf("address flow with a scoped egress rule = %+v", verdict)
	}
}

// Names of the service-account scenario, mirroring vm-firewall-service-accounts.
const (
	saAccount = "runner@demo-net.iam.gserviceaccount.com"
	saTag     = "gl-web"
)

// declareAccount declares a local service account and, when actAs is set,
// lets the harness principal act as it.
func (h *harness) declareAccount(email string, actAs bool) {
	h.t.Helper()
	account := iam.AccountResource(testProject, email)
	if err := h.index.Put("iam", state.Resource{
		Name: account, Service: "iam", Kind: "serviceAccount", Number: "100000000000000000001",
		Parent: "//cloudresourcemanager.googleapis.com/projects/" + testProject,
		Scope:  state.Scope{Project: testProject, Location: state.Global},
	}); err != nil {
		h.t.Fatalf("declare %s: %v", email, err)
	}
	if !actAs {
		return
	}
	if _, err := h.engine.SetPolicy(account, iam.Policy{Bindings: []iam.Binding{
		{Role: "roles/iam.serviceAccountUser", Members: []string{"serviceAccount:actor@demo-net.iam.gserviceaccount.com"}},
	}}); err != nil {
		h.t.Fatalf("grant actAs on %s: %v", email, err)
	}
}

// scopedTopology builds the recorded service-account topology: vm2 runs as
// the account, vm1 carries the tag, and one rule per port is scoped by
// account or tag (plus an EGRESS deny scoped to the account).
func (h *harness) scopedTopology() {
	h.t.Helper()
	h.declareAccount(saAccount, true)
	h.topology(vmNetA, vmSubnetA, testRangeA)
	withAccount := vmBody(vmTwo, vmSubnetA)
	withAccount["serviceAccounts"] = []any{map[string]any{"email": saAccount, "scopes": []any{"https://www.googleapis.com/auth/cloud-platform"}}}
	withTag := vmBody(vmOne, vmSubnetA)
	withTag["tags"] = map[string]any{"items": []any{saTag}}
	for _, body := range []map[string]any{withAccount, withTag} {
		if status, errs := operationErrors(h.t, h.invoke(http.MethodPost, zonePath+"/instances", body)); status != "DONE" || len(errs) != 0 {
			h.t.Fatalf("instance %v operation = %s %v", body[keyName], status, errs)
		}
	}
	ranges := []any{testRangeA}
	for _, rule := range []map[string]any{
		{keyName: "fw-target-sa", "sourceRanges": ranges, "targetServiceAccounts": []any{saAccount}, "allowed": tcp("8080")},
		{keyName: "fw-source-sa", "sourceServiceAccounts": []any{saAccount}, "allowed": tcp("9090")},
		{keyName: "fw-target-tag", "sourceRanges": ranges, "targetTags": []any{saTag}, "allowed": tcp("7070")},
		{keyName: "fw-source-tag", "sourceTags": []any{saTag}, "allowed": tcp("6060")},
		{keyName: "fw-allow-5050", "sourceRanges": ranges, "allowed": tcp("5050")},
		{keyName: "fw-egress-deny-sa", keyDirection: directionEgress, "destinationRanges": ranges, "targetServiceAccounts": []any{saAccount}, "denied": tcp("5050")},
	} {
		rule[keyNetwork] = "projects/demo-net/global/networks/" + vmNetA
		h.expectCode(http.MethodPost, "/compute/v1/projects/demo-net/global/firewalls", rule, 200)
	}
}

// tcp is one allowed or denied tcp term on a port.
func tcp(port string) []any {
	return []any{map[string]any{keyIPProto: "tcp", keyPorts: []any{port}}}
}

// scopedVerdict creates one instance-to-instance test and reads its result,
// the drop cause, and the names of the firewall rules its trace applied.
func (h *harness) scopedVerdict(id, source, destination string, port int) (string, string, []string) {
	h.t.Helper()
	h.expectCode(http.MethodPost, connectivityTests+"?testId="+id, map[string]any{
		keySource:      map[string]any{kindInstance: instanceRef(source)},
		keyDestination: map[string]any{kindInstance: instanceRef(destination), keyPort: port}, keyProtocol: "TCP",
	}, 200)
	var test struct {
		ReachabilityDetails struct {
			Result string `json:"result"`
			Traces []struct {
				Steps []struct {
					Firewall *struct {
						DisplayName string `json:"displayName"`
					} `json:"firewall"`
					Drop struct {
						Cause string `json:"cause"`
					} `json:"drop"`
				} `json:"steps"`
			} `json:"traces"`
		} `json:"reachabilityDetails"`
	}
	if err := json.Unmarshal(h.invoke(http.MethodGet, connectivityTests+"/"+id, nil).Body.Bytes(), &test); err != nil {
		h.t.Fatalf("decode test: %v", err)
	}
	var cause string
	var rules []string
	for _, step := range test.ReachabilityDetails.Traces[0].Steps {
		if step.Firewall != nil {
			rules = append(rules, step.Firewall.DisplayName)
		}
		cause = step.Drop.Cause
	}
	return test.ReachabilityDetails.Result, cause, rules
}

// Firewall rules scoped to a service account or a network tag apply only to
// the instances that run as it or carry it, as targets and as sources
// (recorded: vm-firewall-service-accounts). Before scoping was modeled, a
// rule with only targetServiceAccounts applied to every instance.
func TestScopedFirewallRulesMatchOnlyTheirInstances(t *testing.T) {
	h := newHarness(t)
	h.scopedTopology()
	for _, tc := range []struct {
		id, source, destination string
		port                    int
		result, cause           string
		rules                   []string
	}{
		{"t-target-sa", vmOne, vmTwo, 8080, resultReachable, "", []string{"default-allow-egress", "fw-target-sa"}},
		{"t-not-target-sa", vmTwo, vmOne, 8080, resultUnreachable, "FIREWALL_RULE", []string{"default-allow-egress", "default-deny-ingress"}},
		{"t-source-sa", vmTwo, vmOne, 9090, resultReachable, "", []string{"default-allow-egress", "fw-source-sa"}},
		{"t-not-source-sa", vmOne, vmTwo, 9090, resultUnreachable, "FIREWALL_RULE", []string{"default-allow-egress", "default-deny-ingress"}},
		{"t-target-tag", vmTwo, vmOne, 7070, resultReachable, "", []string{"default-allow-egress", "fw-target-tag"}},
		{"t-not-target-tag", vmOne, vmTwo, 7070, resultUnreachable, "FIREWALL_RULE", []string{"default-allow-egress", "default-deny-ingress"}},
		{"t-source-tag", vmOne, vmTwo, 6060, resultReachable, "", []string{"default-allow-egress", "fw-source-tag"}},
		{"t-not-source-tag", vmTwo, vmOne, 6060, resultUnreachable, "FIREWALL_RULE", []string{"default-allow-egress", "default-deny-ingress"}},
		{"t-egress-sa", vmTwo, vmOne, 5050, resultUnreachable, "FIREWALL_RULE", []string{"fw-egress-deny-sa"}},
		{"t-not-egress-sa", vmOne, vmTwo, 5050, resultReachable, "", []string{"default-allow-egress", "fw-allow-5050"}},
	} {
		result, cause, rules := h.scopedVerdict(tc.id, tc.source, tc.destination, tc.port)
		if result != tc.result || cause != tc.cause || !slices.Equal(rules, tc.rules) {
			t.Errorf("%s = %s %s %v, want %s %s %v", tc.id, result, cause, rules, tc.result, tc.cause, tc.rules)
		}
	}
}

// A scoped source from a peered network can't be evaluated: unsupported,
// never a verdict.
func TestScopedSourceAcrossPeeringIsUnsupported(t *testing.T) {
	h := newHarness(t)
	h.vmTopology()
	h.addPeering(vmNetA, vmNetB, "a-to-b")
	h.addPeering(vmNetB, vmNetA, "b-to-a")
	h.expectCode(http.MethodPost, "/compute/v1/projects/demo-net/global/firewalls", map[string]any{
		keyName: "from-tag", keyNetwork: "projects/demo-net/global/networks/" + vmNetB, "sourceTags": []any{saTag}, "allowed": tcp("6060"),
	}, 200)
	h.expectCode(http.MethodPost, connectivityTests+"?testId=t-peer-tag", map[string]any{
		keySource:      map[string]any{kindInstance: instanceRef(vmOne)},
		keyDestination: map[string]any{kindInstance: instanceRef(vmThree), keyPort: 6060}, keyProtocol: "TCP",
	}, 501)
}

// Running as a service account needs actAs on it. Compute accepts the insert
// and fails the operation (recorded); nothing is created.
func TestInstanceAccountNeedsActAs(t *testing.T) {
	h := newHarness(t)
	h.declareAccount(saAccount, false)
	h.topology(vmNetA, vmSubnetA, testRangeA)
	body := vmBody(vmTwo, vmSubnetA)
	body["serviceAccounts"] = []any{map[string]any{"email": saAccount}}
	recorder := h.expectCode(http.MethodPost, zonePath+"/instances", body, 200)
	var operation struct {
		Status string `json:"status"`
		Error  struct {
			Errors []struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"errors"`
		} `json:"error"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &operation); err != nil {
		t.Fatalf("decode operation: %v", err)
	}
	want := "The user does not have access to service account '" + saAccount + "'.  User: 'actor@demo-net.iam.gserviceaccount.com'. " +
		"Ask a project owner to grant you the iam.serviceAccountUser role on the service account."
	if operation.Status != "DONE" || len(operation.Error.Errors) != 1 || operation.Error.Errors[0].Code != "SERVICE_ACCOUNT_ACCESS_DENIED" ||
		operation.Error.Errors[0].Message != want {
		t.Fatalf("operation = %+v", operation)
	}
	h.expectCode(http.MethodGet, zonePath+"/instances/"+vmTwo, nil, 404)
	// An account the emulator doesn't declare is a gap, never an allow.
	body["serviceAccounts"] = []any{map[string]any{"email": "ghost@demo-net.iam.gserviceaccount.com"}}
	h.expectCode(http.MethodPost, zonePath+"/instances", body, 501)
	body["serviceAccounts"] = []any{map[string]any{"email": "default"}}
	h.expectCode(http.MethodPost, zonePath+"/instances", body, 501)
	// With actAs the instance runs as the account and renders it.
	h.declareAccount(saAccount, true)
	body["serviceAccounts"] = []any{map[string]any{"email": saAccount, "scopes": []any{"https://www.googleapis.com/auth/cloud-platform"}}}
	if status, errs := operationErrors(t, h.invoke(http.MethodPost, zonePath+"/instances", body)); status != "DONE" || len(errs) != 0 {
		t.Fatalf("instance with actAs = %s %v", status, errs)
	}
	document := decode(t, h.expectCode(http.MethodGet, zonePath+"/instances/"+vmTwo, nil, 200))
	accounts := mustJSON(t, document["serviceAccounts"])
	if accounts != `[{"email":"`+saAccount+`","scopes":["https://www.googleapis.com/auth/cloud-platform"]}]` {
		t.Fatalf("serviceAccounts = %s", accounts)
	}
}
