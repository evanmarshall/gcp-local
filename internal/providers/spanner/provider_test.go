package spanner

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
	testProject   = "demo-spanner"
	testInstance  = "demo-inst"
	testDatabase  = "demo-db"
	testToken     = "actor"
	testInstances = "/v1/projects/" + testProject + "/instances"
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
		Bindings: []iam.Binding{{Role: "roles/spanner.admin", Members: []string{testActor}}},
	}); err != nil {
		t.Fatalf("grant the project: %v", err)
	}
	_, err = New(context.Background(), registry, backend.NewManager(time.Second, time.Second), index, engine)
	if err != nil {
		t.Fatalf("build the spanner provider: %v", err)
	}
	g := gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:" + token + "@" + testProject + ".iam.gserviceaccount.com", Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})
	return &harness{t: t, gate: g, engine: engine}
}

// invoke issues one REST request as the fixture principal.
func (h *harness) invoke(verb, path string, body any) *httptest.ResponseRecorder {
	h.t.Helper()
	return h.invokeAs(testToken, verb, path, body)
}

// invokeAs issues one REST request as serviceAccount:<token>@testProject.
func (h *harness) invokeAs(token, verb, path string, body any) *httptest.ResponseRecorder {
	h.t.Helper()
	var encoded []byte
	if body != nil {
		var err error
		if encoded, err = json.Marshal(body); err != nil {
			h.t.Fatalf("encode the request: %v", err)
		}
	}
	request, _ := http.NewRequestWithContext(context.Background(), verb, path, bytes.NewReader(encoded))
	if encoded != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	h.gate.ServeHTTP(recorder, request)
	return recorder
}

// instance creates the fixture instance.
func (h *harness) instance() {
	h.t.Helper()
	recorder := h.invoke(http.MethodPost, testInstances, map[string]any{
		"instanceId": testInstance,
		"instance": map[string]any{
			keyConfig: "regional-us-central1", keyDisplayName: "demo", "nodeCount": 1,
		},
	})
	if recorder.Code != 200 {
		h.t.Fatalf("instance create status = %d body %s", recorder.Code, recorder.Body.String())
	}
}

func TestInstanceAndDatabaseLifecycle(t *testing.T) {
	h := newHarness(t)
	h.instance()
	recorder := h.invoke(http.MethodGet, testInstances+"/"+testInstance, nil)
	if recorder.Code != 200 {
		t.Fatalf("instance get status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodPost, testInstances+"/"+testInstance+"/databases", map[string]any{
		keyCreateStatement: "CREATE DATABASE `" + testDatabase + "`",
	})
	if recorder.Code != 200 {
		t.Fatalf("database create status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodGet, testInstances+"/"+testInstance+"/databases/"+testDatabase, nil)
	if recorder.Code != 200 {
		t.Fatalf("database get status = %d", recorder.Code)
	}
	// Deleting a non-empty instance is refused.
	recorder = h.invoke(http.MethodDelete, testInstances+"/"+testInstance, nil)
	if recorder.Code != 400 {
		t.Fatalf("non-empty instance delete status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodDelete, testInstances+"/"+testInstance+"/databases/"+testDatabase, nil)
	if recorder.Code != 200 {
		t.Fatalf("database delete status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodDelete, testInstances+"/"+testInstance, nil)
	if recorder.Code != 200 {
		t.Fatalf("instance delete status = %d", recorder.Code)
	}
}

func TestInstanceConfigsAreModeled(t *testing.T) {
	h := newHarness(t)
	recorder := h.invoke(http.MethodGet, "/v1/projects/"+testProject+"/instanceConfigs/regional-us-central1", nil)
	if recorder.Code != 200 {
		t.Fatalf("config get status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodGet, "/v1/projects/"+testProject+"/instanceConfigs", nil)
	if recorder.Code != 200 {
		t.Fatalf("config list status = %d", recorder.Code)
	}
}

func TestUnknownConfigRefused(t *testing.T) {
	h := newHarness(t)
	recorder := h.invoke(http.MethodPost, testInstances, map[string]any{
		keyName: testInstance, keyConfig: "regional-nowhere", "nodeCount": 1,
	})
	if recorder.Code != 400 {
		t.Fatalf("unknown config status = %d", recorder.Code)
	}
}

func TestViewerCannotCreate(t *testing.T) {
	h := newHarness(t)
	if _, err := h.engineSet(iam.Policy{Version: 3, Bindings: []iam.Binding{
		{Role: "roles/spanner.viewer", Members: []string{"serviceAccount:viewer@" + testProject + ".iam.gserviceaccount.com"}},
	}}); err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, testInstances,
		bytes.NewReader([]byte(`{"name":"x","config":"regional-us-central1","nodeCount":1}`)))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer viewer")
	recorder := httptest.NewRecorder()
	h.gate.ServeHTTP(recorder, request)
	if recorder.Code != 403 {
		t.Fatalf("viewer create status = %d body %s", recorder.Code, recorder.Body.String())
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
	probe(http.MethodGet, "/v1/projects/"+testProject+"/instanceConfigs/regional-us-central1")
	probe(http.MethodGet, "/v1/projects/"+testProject+"/instanceConfigs")
	probe(http.MethodGet, testInstances+"/"+testInstance)
	probe(http.MethodGet, testInstances)
	probe(http.MethodPost, testInstances+"/"+testInstance+":getIamPolicy")
	probe(http.MethodGet, testInstances+"/"+testInstance+"/databases")
	probe(http.MethodGet, testInstances+"/"+testInstance+"/databases/"+testDatabase)
	probe(http.MethodPost, testInstances+"/"+testInstance+"/databases/"+testDatabase+":getIamPolicy")
	probe(http.MethodGet, testInstances+"/"+testInstance+"/databases/"+testDatabase+"/ddl")
}

// engineSet replaces the project policy in the test harness.
func (h *harness) engineSet(policy iam.Policy) (iam.Policy, error) {
	policy, err := h.engine.SetPolicy("//cloudresourcemanager.googleapis.com/projects/"+testProject, policy)
	if err != nil {
		return policy, fmt.Errorf("set the project policy: %w", err)
	}
	return policy, nil
}
