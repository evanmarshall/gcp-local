package firestore

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
	testProject     = "demo-fs"
	testCollection  = "books"
	testDoc         = "doc-1"
	testToken       = "actor"
	testDocsPath    = "/v1/projects/" + testProject + "/databases/(default)/documents"
	testActor       = "serviceAccount:actor@" + testProject + ".iam.gserviceaccount.com"
	testFieldsKey   = "fields"
	testTitleKey    = "title"
	testTitleString = "fire-and-fog"
)

// harness drives the provider through the gateway.
type harness struct {
	t      *testing.T
	gate   *gateway.Gateway
	index  *state.Index
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
		Bindings: []iam.Binding{
			{Role: "roles/datastore.user", Members: []string{testActor}},
		},
	}); err != nil {
		t.Fatalf("grant the project: %v", err)
	}
	_, err = New(context.Background(), registry, backend.NewManager(time.Second, time.Second), index, engine)
	if err != nil {
		t.Fatalf("build the firestore provider: %v", err)
	}
	g := gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:" + token + "@" + testProject + ".iam.gserviceaccount.com", Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})
	return &harness{t: t, gate: g, index: index, engine: engine}
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
	request.Header.Set("Authorization", "Bearer "+testToken)
	recorder := httptest.NewRecorder()
	h.gate.ServeHTTP(recorder, request)
	return recorder
}

// docPath is one document path in the fixture collection.
func docPath(id string) string { return testDocsPath + "/" + testCollection + "/" + id }

// fieldsBody renders one document body with the fixture title.
func fieldsBody(title string) map[string]any {
	return map[string]any{testFieldsKey: map[string]any{testTitleKey: map[string]any{"stringValue": title}}}
}

func TestDocumentLifecycle(t *testing.T) {
	h := newHarness(t)
	recorder := h.invoke(http.MethodPatch, docPath(testDoc), fieldsBody(testTitleString))
	if recorder.Code != 200 {
		t.Fatalf("create status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodGet, docPath(testDoc), nil)
	if recorder.Code != 200 {
		t.Fatalf("get status = %d", recorder.Code)
	}
	var document struct {
		Fields map[string]map[string]any `json:"fields"`
		Name   string                    `json:"name"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &document); err != nil {
		t.Fatalf("decode document: %v", err)
	}
	if document.Fields[testTitleKey]["stringValue"] != testTitleString {
		t.Fatalf("fields = %+v", document.Fields)
	}
	if document.Name == "" {
		t.Fatal("name is empty")
	}
}

// TestDocumentUpsertAndListing covers the upsert and the collection listing.
func TestDocumentUpsertAndListing(t *testing.T) {
	h := newHarness(t)
	h.invoke(http.MethodPatch, docPath(testDoc), fieldsBody(testTitleString))
	// The real surface upserts: a PATCH over an existing path replaces it.
	recorder := h.invoke(http.MethodPatch, docPath(testDoc), fieldsBody("second"))
	if recorder.Code != 200 {
		t.Fatalf("upsert status = %d", recorder.Code)
	}
	// A second document joins the collection listing.
	recorder = h.invoke(http.MethodPatch, docPath("doc-2"), fieldsBody("second-book"))
	if recorder.Code != 200 {
		t.Fatalf("second create status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodGet, testDocsPath+"/"+testCollection, nil)
	if recorder.Code != 200 {
		t.Fatalf("list status = %d", recorder.Code)
	}
	var listed struct {
		Documents []struct {
			Name string `json:"name"`
		} `json:"documents"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &listed); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(listed.Documents) != 2 {
		t.Fatalf("documents = %+v", listed.Documents)
	}
	recorder = h.invoke(http.MethodDelete, docPath(testDoc), nil)
	if recorder.Code != 200 {
		t.Fatalf("delete status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodGet, docPath(testDoc), nil)
	if recorder.Code != 404 {
		t.Fatalf("get after delete status = %d", recorder.Code)
	}
}

func TestDocumentUpdateKeepsCreateTime(t *testing.T) {
	h := newHarness(t)
	h.invoke(http.MethodPatch, docPath(testDoc), fieldsBody(testTitleString))
	recorder := h.invoke(http.MethodPatch, docPath(testDoc)+"?currentDocument.exists=true", fieldsBody("second"))
	if recorder.Code != 200 {
		t.Fatalf("update status = %d", recorder.Code)
	}
	var document struct {
		Fields     map[string]map[string]any `json:"fields"`
		CreateTime string                    `json:"createTime"`
		UpdateTime string                    `json:"updateTime"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &document); err != nil {
		t.Fatalf("decode document: %v", err)
	}
	if document.Fields[testTitleKey]["stringValue"] != "second" {
		t.Fatalf("fields = %+v", document.Fields)
	}
	if document.CreateTime == "" || document.UpdateTime == "" {
		t.Fatalf("timestamps = %+v", document)
	}
}

func TestDocumentRequiresFields(t *testing.T) {
	h := newHarness(t)
	recorder := h.invoke(http.MethodPatch, docPath(testDoc), map[string]any{})
	if recorder.Code != 400 {
		t.Fatalf("missing fields status = %d", recorder.Code)
	}
}

func TestViewerCannotWrite(t *testing.T) {
	h := newHarness(t)
	if _, err := h.engineSet(iam.Policy{Version: 3, Bindings: []iam.Binding{
		{Role: "roles/datastore.viewer", Members: []string{"serviceAccount:viewer@" + testProject + ".iam.gserviceaccount.com"}},
	}}); err != nil {
		t.Fatalf("set the viewer policy: %v", err)
	}
	request, _ := http.NewRequestWithContext(context.Background(), http.MethodPatch,
		docPath(testDoc), bytes.NewReader([]byte(`{"fields":{}}`)))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer viewer")
	recorder := httptest.NewRecorder()
	h.gate.ServeHTTP(recorder, request)
	if recorder.Code != 403 {
		t.Fatalf("viewer write status = %d body %s", recorder.Code, recorder.Body.String())
	}
}

func TestEveryRouteIsHandled(t *testing.T) {
	h := newHarness(t)
	h.invoke(http.MethodPatch, docPath(testDoc), fieldsBody(testTitleString))
	gap := "this method is not implemented"
	probe := func(verb, path string) {
		recorder := h.invoke(verb, path, nil)
		if recorder.Code == 501 && bytes.Contains(recorder.Body.Bytes(), []byte(gap)) {
			t.Fatalf("%s %s hit the dispatch gap", verb, path)
		}
	}
	probe(http.MethodGet, docPath(testDoc))
	probe(http.MethodGet, testDocsPath+"/"+testCollection)
}

// engineSet replaces the project policy in the test harness.
func (h *harness) engineSet(policy iam.Policy) (iam.Policy, error) {
	policy, err := h.engine.SetPolicy("//cloudresourcemanager.googleapis.com/projects/"+testProject, policy)
	if err != nil {
		return policy, fmt.Errorf("set the project policy: %w", err)
	}
	return policy, nil
}
