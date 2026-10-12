package resourcemanager

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
	testProject = "demo-rm"
	testToken   = "actor"
	testBase    = "/v1/projects/" + testProject
	testActor   = "serviceAccount:actor@" + testProject + ".iam.gserviceaccount.com"
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
		Bindings: []iam.Binding{{Role: "roles/resourcemanager.projectIamAdmin", Members: []string{testActor}}},
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

// post issues one REST POST as the fixture principal.
func (h *harness) post(path string, body any) *httptest.ResponseRecorder {
	h.t.Helper()
	var encoded []byte
	if body != nil {
		var err error
		if encoded, err = json.Marshal(body); err != nil {
			h.t.Fatalf("encode the request body: %v", err)
		}
	}
	request, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, path, bytes.NewReader(encoded))
	if encoded != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Authorization", "Bearer "+testToken)
	recorder := httptest.NewRecorder()
	h.gate.ServeHTTP(recorder, request)
	return recorder
}

func TestProjectPolicyRoundTrip(t *testing.T) {
	h := newHarness(t)
	recorder := h.post(testBase+":getIamPolicy", map[string]any{})
	if recorder.Code != 200 {
		t.Fatalf("getIamPolicy status = %d", recorder.Code)
	}
	recorder = h.post(testBase+":setIamPolicy", map[string]any{
		"policy": map[string]any{
			"bindings": []any{map[string]any{
				"role":    "roles/resourcemanager.projectIamAdmin",
				"members": []any{testActor},
			}},
		},
	})
	if recorder.Code != 200 {
		t.Fatalf("setIamPolicy status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.post(testBase+":getIamPolicy", map[string]any{})
	if recorder.Code != 200 {
		t.Fatalf("post-set getIamPolicy status = %d", recorder.Code)
	}
	recorder = h.post("/v1/projects/bad_upper:setIamPolicy", map[string]any{})
	if recorder.Code != 400 {
		t.Fatalf("invalid project status = %d", recorder.Code)
	}
	recorder = h.post("/v1/projects/missing:getIamPolicy", map[string]any{})
	if recorder.Code != 404 {
		t.Fatalf("missing project status = %d", recorder.Code)
	}
}
