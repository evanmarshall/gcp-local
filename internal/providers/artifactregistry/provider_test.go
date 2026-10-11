package artifactregistry

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

const (
	testProject  = "demo-ar"
	testRepo     = "demo-repo"
	testToken    = "actor"
	testLocation = "us-central1"
	testBase     = "/v1/projects/" + testProject + "/locations/" + testLocation + "/repositories"
	testActor    = "serviceAccount:actor@" + testProject + ".iam.gserviceaccount.com"
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
		Bindings: []iam.Binding{{Role: "roles/artifactregistry.admin", Members: []string{testActor}}},
	}); err != nil {
		t.Fatalf("grant the project: %v", err)
	}
	_, err = New(context.Background(), registry, backend.NewManager(time.Second, time.Second), index, engine)
	if err != nil {
		t.Fatalf("build the provider: %v", err)
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

func TestRepositoryLifecycle(t *testing.T) {
	h := newHarness(t)
	recorder := h.invoke(http.MethodPost, testBase+"?repositoryId="+testRepo, map[string]any{
		"format": "DOCKER", "description": "demo",
	})
	if recorder.Code != 200 {
		t.Fatalf("repository create status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodGet, testBase+"/"+testRepo, nil)
	if recorder.Code != 200 {
		t.Fatalf("repository get status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodGet, testBase, nil)
	if recorder.Code != 200 {
		t.Fatalf("repository list status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodPatch, testBase+"/"+testRepo, map[string]any{
		"description": "updated",
	})
	if recorder.Code != 200 {
		t.Fatalf("repository update status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodPost, testBase+"?repositoryId="+testRepo, map[string]any{"format": "DOCKER"})
	if recorder.Code != 409 {
		t.Fatalf("duplicate create status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodDelete, testBase+"/"+testRepo, nil)
	if recorder.Code != 204 {
		t.Fatalf("repository delete status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodGet, testBase+"/"+testRepo, nil)
	if recorder.Code != 404 {
		t.Fatalf("post-destroy get status = %d", recorder.Code)
	}
}

func TestUnknownAndDeniedPaths(t *testing.T) {
	h := newHarness(t)
	recorder := h.invoke(http.MethodGet, testBase+"/missing", nil)
	if recorder.Code != 404 {
		t.Fatalf("unknown repository get status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodGet, testBase+"/Bad_Upper", nil)
	if recorder.Code != 400 {
		t.Fatalf("invalid repository id status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodPost, testBase+"?repositoryId=other", map[string]any{"format": "MISSING"})
	if recorder.Code != 400 {
		t.Fatalf("unknown format status = %d", recorder.Code)
	}

	if _, err := h.engine.SetPolicy("//cloudresourcemanager.googleapis.com/projects/"+testProject, iam.Policy{
		Bindings: []iam.Binding{{Role: "roles/artifactregistry.reader", Members: []string{testActor}}},
	}); err != nil {
		t.Fatalf("switch the role: %v", err)
	}
	recorder = h.invoke(http.MethodPost, testBase+"?repositoryId="+testRepo, map[string]any{"format": "DOCKER"})
	if recorder.Code != 403 {
		t.Fatalf("reader create status = %d", recorder.Code)
	}
}

func TestEveryRouteIsHandled(t *testing.T) {
	h := newHarness(t)
	recorder := h.invoke(http.MethodPost, testBase+"?repositoryId="+testRepo, map[string]any{"format": "DOCKER"})
	if recorder.Code != 200 {
		t.Fatalf("probe create status = %d body %s", recorder.Code, recorder.Body.String())
	}
	gap := "this method is not implemented"
	probe := func(verb, path string) {
		recorder := h.invoke(verb, path, nil)
		if recorder.Code == 501 && bytes.Contains(recorder.Body.Bytes(), []byte(gap)) {
			t.Fatalf("%s %s hit the dispatch gap", verb, path)
		}
	}
	probe(http.MethodGet, testBase)
	probe(http.MethodGet, testBase+"/"+testRepo)
	probe(http.MethodDelete, testBase+"/"+testRepo)
	probe(http.MethodGet, testBase+"/"+testRepo+":getIamPolicy")
}
