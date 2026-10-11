package bigtable

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
	testProject   = "demo-bigtable"
	testInstance  = "demo-bt"
	testCluster   = "demo-btc1"
	testTable     = "demo-table"
	testToken     = "actor"
	testInstances = "/v2/projects/" + testProject + "/instances"
	testActor     = "serviceAccount:actor@" + testProject + ".iam.gserviceaccount.com"
)

// harness drives the provider through the gateway.
type harness struct {
	t      *testing.T
	gate   *gateway.Gateway
	engine *iam.Engine
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
		Name: "//cloudresourcemanager.googleapis.com/projects/" + testProject, Service: "resourcemanager",
		Kind: "project", Number: "123456", Scope: state.Scope{Project: testProject, Location: state.Global},
	}); err != nil {
		t.Fatalf("register the project: %v", err)
	}
	if _, err := engine.SetPolicy("//cloudresourcemanager.googleapis.com/projects/"+testProject, iam.Policy{
		Bindings: []iam.Binding{{Role: "roles/bigtable.admin", Members: []string{testActor}}},
	}); err != nil {
		t.Fatalf("grant the project: %v", err)
	}
	_, err = New(context.Background(), registry, backend.NewManager(time.Second, time.Second), index, engine)
	if err != nil {
		t.Fatalf("build the bigtable provider: %v", err)
	}
	g := gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:" + token + "@" + testProject + ".iam.gserviceaccount.com", Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})
	return &harness{t: t, gate: g, engine: engine}
}

// invoke issues one REST request as the fixture principal.
func (h *harness) invoke(verb, path string, body any) *httptest.ResponseRecorder {
	h.t.Helper()
	var encoded []byte
	if body != nil {
		encoded, _ = json.Marshal(body)
	}
	request, _ := http.NewRequestWithContext(context.Background(), verb, path, bytes.NewReader(encoded))
	if encoded != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Authorization", "Bearer "+testToken)
	recorder := httptest.NewRecorder()
	h.gate.ServeHTTP(recorder, request)
	return recorder
}

// instance creates the fixture instance.
func (h *harness) instance() {
	h.t.Helper()
	recorder := h.invoke(http.MethodPost, testInstances, map[string]any{
		"instanceId": testInstance,
		"instance":   map[string]any{"displayName": "demo", "labels": map[string]string{"env": "dev"}},
		"clusters": []any{map[string]any{
			"clusterId": testCluster, "location": "projects/p/locations/us-central1-a",
			"serveNodes": 1, "defaultStorageType": "SSD",
		}},
	})
	if recorder.Code != 200 {
		h.t.Fatalf("instance create status = %d body %s", recorder.Code, recorder.Body.String())
	}
}

func TestInstanceAndTableLifecycle(t *testing.T) {
	h := newHarness(t)
	h.instance()

	recorder := h.invoke(http.MethodGet, testInstances+"/"+testInstance, nil)
	if recorder.Code != 200 {
		t.Fatalf("instance get status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodGet, testInstances, nil)
	if recorder.Code != 200 {
		t.Fatalf("instance list status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodGet, testInstances+"/"+testInstance+"/clusters", nil)
	if recorder.Code != 200 {
		t.Fatalf("cluster list status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodGet, testInstances+"/"+testInstance+"/clusters/"+testCluster, nil)
	if recorder.Code != 200 {
		t.Fatalf("cluster get status = %d", recorder.Code)
	}

	recorder = h.invoke(http.MethodPost, testInstances+"/"+testInstance+"/tables", map[string]any{
		"tableId": testTable,
		"table": map[string]any{
			"columnFamilies": map[string]any{
				"cf1": map[string]any{"gcRule": map[string]any{"maxAge": "2160s"}},
			},
		},
	})
	if recorder.Code != 200 {
		t.Fatalf("table create status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodGet, testInstances+"/"+testInstance+"/tables/"+testTable, nil)
	if recorder.Code != 200 {
		t.Fatalf("table get status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodGet, testInstances+"/"+testInstance+"/tables", nil)
	if recorder.Code != 200 {
		t.Fatalf("table list status = %d", recorder.Code)
	}

	// Deleting a non-empty instance is refused.
	recorder = h.invoke(http.MethodDelete, testInstances+"/"+testInstance, nil)
	if recorder.Code != 400 {
		t.Fatalf("non-empty instance delete status = %d", recorder.Code)
	}

	recorder = h.invoke(http.MethodPost, testInstances+"/"+testInstance+"/tables/"+testTable+":modifyColumnFamilies", map[string]any{
		"modifications": []any{map[string]any{"id": "cf2", "create": map[string]any{"gcRule": map[string]any{"maxVersion": "3"}}}},
	})
	if recorder.Code != 200 {
		t.Fatalf("family modify status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodPost, testInstances+"/"+testInstance+"/tables/"+testTable+":modifyColumnFamilies", map[string]any{
		"modifications": []any{map[string]any{"id": "cf1", "drop": true}},
	})
	if recorder.Code != 200 {
		t.Fatalf("family drop status = %d", recorder.Code)
	}

	recorder = h.invoke(http.MethodDelete, testInstances+"/"+testInstance+"/tables/"+testTable, nil)
	if recorder.Code != 204 {
		t.Fatalf("table delete status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodDelete, testInstances+"/"+testInstance, nil)
	if recorder.Code != 204 {
		t.Fatalf("instance delete status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodGet, testInstances+"/"+testInstance, nil)
	if recorder.Code != 404 {
		t.Fatalf("post-destroy instance get status = %d", recorder.Code)
	}
}

func TestUnknownAndDeniedPaths(t *testing.T) {
	h := newHarness(t)
	recorder := h.invoke(http.MethodGet, testInstances+"/missing", nil)
	if recorder.Code != 404 {
		t.Fatalf("unknown instance get status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodGet, testInstances+"/bad_upper", nil)
	if recorder.Code != 400 {
		t.Fatalf("invalid instance id status = %d", recorder.Code)
	}

	// A viewer may read but not create.
	if _, err := h.engine.SetPolicy("//cloudresourcemanager.googleapis.com/projects/"+testProject, iam.Policy{
		Bindings: []iam.Binding{{Role: "roles/bigtable.viewer", Members: []string{testActor}}},
	}); err != nil {
		t.Fatalf("switch the role: %v", err)
	}
	recorder = h.invoke(http.MethodPost, testInstances, map[string]any{
		"instanceId": testInstance,
		"instance":   map[string]any{"displayName": "demo"},
		"clusters": []any{map[string]any{
			"clusterId": testCluster, "location": "us-central1-a", "serveNodes": 1,
		}},
	})
	if recorder.Code != 403 {
		t.Fatalf("viewer create status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodGet, testInstances+"/"+testInstance, nil)
	if recorder.Code != 404 {
		t.Fatalf("viewer get status = %d", recorder.Code)
	}
}

func TestEveryRouteIsHandled(t *testing.T) {
	h := newHarness(t)
	h.instance()
	gap := "this method is not implemented"
	probe := func(verb, path string) {
		recorder := h.invoke(verb, path, nil)
		if recorder.Code == 501 && bytes.Contains(recorder.Body.Bytes(), []byte(gap)) {
			t.Fatalf("%s %s hit the dispatch gap", verb, path)
		}
	}
	probe(http.MethodPost, testInstances)
	probe(http.MethodGet, testInstances+"/"+testInstance)
	probe(http.MethodGet, testInstances)
	probe(http.MethodGet, testInstances+"/"+testInstance+"/clusters")
	probe(http.MethodGet, testInstances+"/"+testInstance+"/clusters/"+testCluster)
	probe(http.MethodPost, testInstances+"/"+testInstance+"/tables")
	probe(http.MethodGet, testInstances+"/"+testInstance+"/tables/"+testTable)
	probe(http.MethodGet, testInstances+"/"+testInstance+"/tables")
}
