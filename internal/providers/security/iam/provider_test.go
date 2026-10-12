package iam

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

const (
	testProject = "demo-sa"
	testAccount = "worker"
	testToken   = "actor"
	testBase    = "/v1/projects/" + testProject + "/serviceAccounts"
	testActor   = "serviceAccount:actor@" + testProject + ".iam.gserviceaccount.com"
)

// harness drives the provider through the gateway.
type harness struct {
	t        *testing.T
	gate     *gateway.Gateway
	engine   *iam.Engine
	index    *state.Index
	provider *Provider
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
		Bindings: []iam.Binding{
			{Role: "roles/iam.serviceAccountAdmin", Members: []string{testActor}},
			// Key management sits in its own role in GCP.
			{Role: "roles/iam.serviceAccountKeyAdmin", Members: []string{testActor}},
		},
	}); err != nil {
		t.Fatalf("grant the project: %v", err)
	}
	p, err := New(context.Background(), registry, backend.NewManager(time.Second, time.Second), index, engine)
	if err != nil {
		t.Fatalf("build the provider: %v", err)
	}
	g := gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:" + token + "@" + testProject + ".iam.gserviceaccount.com", Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})
	return &harness{t: t, gate: g, engine: engine, index: index, provider: p}
}

// expect issues one REST request and fails the test unless it returns want.
func (h *harness) expect(verb, path string, body any, want int) *httptest.ResponseRecorder {
	h.t.Helper()
	recorder := h.invoke(verb, path, body)
	if recorder.Code != want {
		h.t.Fatalf("%s %s status = %d, want %d: %s", verb, path, recorder.Code, want, recorder.Body.String())
	}
	return recorder
}

// invoke issues one REST request as the fixture principal.
func (h *harness) invoke(verb, path string, body any) *httptest.ResponseRecorder {
	h.t.Helper()
	return h.invokeAs(testToken, verb, path, body)
}

// invokeAs issues one REST request as the principal a token names.
func (h *harness) invokeAs(token, verb, path string, body any) *httptest.ResponseRecorder {
	h.t.Helper()
	var encoded []byte
	if body != nil {
		var err error
		if encoded, err = json.Marshal(body); err != nil {
			h.t.Fatalf("encode the request body: %v", err)
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

func TestAccountAndKeyLifecycle(t *testing.T) {
	h := newHarness(t)
	h.expect(http.MethodPost, testBase, map[string]any{
		"accountId": testAccount, "displayName": "worker",
	}, 200)
	h.expect(http.MethodGet, testBase+"/"+testAccount, nil, 200)
	h.expect(http.MethodGet, testBase, nil, 200)
	h.expect(http.MethodPatch, testBase+"/"+testAccount, map[string]any{
		"serviceAccount": map[string]any{"displayName": "renamed", "description": "worker account"},
		"updateMask":     "displayName,description",
	}, 200)
	recorder := h.expect(http.MethodPost, testBase+"/"+testAccount+"/keys", map[string]any{}, 200)
	var created struct {
		Name           string `json:"name"`
		PrivateKeyData string `json:"privateKeyData"`
	}
	if json.Unmarshal(recorder.Body.Bytes(), &created) != nil || created.Name == "" || created.PrivateKeyData == "" {
		t.Fatalf("unexpected key body %s", recorder.Body.String())
	}
	h.expect(http.MethodGet, testBase+"/"+testAccount+"/keys", nil, 200)
	h.expect(http.MethodDelete, testBase+"/"+testAccount+"/keys/"+created.Name[strings.LastIndex(created.Name, "/")+1:], nil, 200)
	h.expect(http.MethodDelete, testBase+"/"+testAccount, nil, 200)
	h.expect(http.MethodGet, testBase+"/"+testAccount, nil, 404)
}

// TestCreatedAccountIndexesItsUniqueID proves a created account is published
// with its numeric identity, which ID-token minting requires as the subject.
func TestCreatedAccountIndexesItsUniqueID(t *testing.T) {
	h := newHarness(t)
	recorder := h.invoke(http.MethodPost, testBase, map[string]any{"accountId": testAccount})
	var created struct {
		UniqueID string `json:"uniqueId"`
	}
	if recorder.Code != 200 || json.Unmarshal(recorder.Body.Bytes(), &created) != nil {
		t.Fatalf("account create status = %d body %s", recorder.Code, recorder.Body.String())
	}
	resource, ok := h.index.Get(accountResource(testProject, accountEmail(testProject, testAccount)))
	if !ok || resource.Number == "" || resource.Number != created.UniqueID {
		t.Fatalf("indexed account = %+v, unique id %q", resource, created.UniqueID)
	}
}

func TestUnknownAndDeniedPaths(t *testing.T) {
	h := newHarness(t)
	recorder := h.invoke(http.MethodGet, testBase+"/missing", nil)
	if recorder.Code != 404 {
		t.Fatalf("unknown account get status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodGet, testBase+"/Bad_Upper", nil)
	if recorder.Code != 400 {
		t.Fatalf("invalid account id status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodPost, testBase, map[string]any{"accountId": testAccount})
	if recorder.Code != 200 {
		t.Fatalf("create before role switch status = %d", recorder.Code)
	}
	if _, err := h.engine.SetPolicy("//cloudresourcemanager.googleapis.com/projects/"+testProject, iam.Policy{
		Bindings: []iam.Binding{{Role: "roles/iam.serviceAccountViewer", Members: []string{testActor}}},
	}); err != nil {
		t.Fatalf("switch the role: %v", err)
	}
	recorder = h.invoke(http.MethodPost, testBase, map[string]any{"accountId": "second"})
	if recorder.Code != 403 {
		t.Fatalf("viewer create status = %d", recorder.Code)
	}
}
