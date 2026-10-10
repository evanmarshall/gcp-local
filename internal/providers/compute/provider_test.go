package compute

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// Names and keys repeated across the lifecycle tests.
const (
	testNetwork   = "vpc-1"
	testFirewall  = "fw-1"
	testAllowed   = "allowed"
	testSubnetOne = "subnet-1"
	testTCP       = "tcp"
)

// harness drives the provider through the gateway.
type harness struct {
	t    *testing.T
	gate *gateway.Gateway
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
		Kind: "project", Number: "123456", Scope: state.Scope{Project: "demo-net", Location: state.Global},
	}); err != nil {
		t.Fatalf("register the project: %v", err)
	}
	if _, err := engine.SetPolicy("//cloudresourcemanager.googleapis.com/projects/demo-net", iam.Policy{
		Bindings: []iam.Binding{{Role: "roles/compute.admin", Members: []string{"serviceAccount:actor@demo-net.iam.gserviceaccount.com"}}},
	}); err != nil {
		t.Fatalf("grant the project: %v", err)
	}
	provider, err := New(context.Background(), registry, backend.NewManager(time.Second, time.Second), index, engine)
	if err != nil {
		t.Fatalf("build the compute provider: %v", err)
	}
	_ = provider
	g := gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:" + token + "@demo-net.iam.gserviceaccount.com", Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})
	return &harness{t: t, gate: g}
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
		keyName: testNetwork, "autoCreateSubnetworks": false,
	})
	if recorder.Code != 200 {
		t.Fatalf("network create status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodGet, "/compute/v1/projects/demo-net/global/networks/vpc-1", nil)
	if recorder.Code != 200 {
		t.Fatalf("network get status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodPost, "/compute/v1/projects/demo-net/regions/us-central1/subnetworks", map[string]any{
		keyName: testSubnetOne, keyNetwork: testNetwork, "ipCidrRange": "10.0.0.0/20",
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
		keyName: testFirewall, keyNetwork: testNetwork, "direction": "INGRESS",
		testAllowed:    []any{map[string]any{keyIPProto: testTCP, keyPorts: []any{"80"}}},
		"sourceRanges": []any{"10.0.0.0/8"},
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
