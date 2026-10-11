//go:build firestoreintegration

package firestore

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// realHarness drives the provider against the pinned official emulator.
type realHarness struct {
	t       *testing.T
	gate    *gateway.Gateway
	manager *backend.Manager
}

func newRealHarness(t *testing.T) *realHarness {
	t.Helper()
	config := Config{Java: os.Getenv("GCP_LOCAL_JAVA"), JAR: os.Getenv("GCP_LOCAL_FIRESTORE_JAR")}
	if err := config.validate(); err != nil {
		t.Fatalf("set GCP_LOCAL_JAVA and GCP_LOCAL_FIRESTORE_JAR to the pinned runtime: %v", err)
	}
	registry, index := gateway.NewRegistry(), state.NewIndex()
	engine, err := iam.New(index)
	if err != nil {
		t.Fatal(err)
	}
	for _, project := range []string{testProject, otherProject} {
		if err := index.Put("resourcemanager", state.Resource{Name: projectResource(project), Service: "resourcemanager", Kind: "project", Number: "123456", Scope: scopeOf(project)}); err != nil {
			t.Fatal(err)
		}
		if _, err := engine.SetPolicy(projectResource(project), iam.Policy{Bindings: []iam.Binding{
			{Role: "roles/datastore.user", Members: []string{principal("actor", project)}},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	manager := backend.NewManager(60*time.Second, 10*time.Second)
	if _, err := New(context.Background(), registry, manager, index, engine, config); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := manager.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})
	gate := gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		actor, project, _ := bytes.Cut([]byte(token), []byte(":"))
		return gateway.Principal{State: gateway.Authenticated, Subject: principal(string(actor), string(project)), Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})
	return &realHarness{t: t, gate: gate, manager: manager}
}

func (h *realHarness) invoke(token, verb, path string, body any) (int, map[string]any) {
	h.t.Helper()
	var encoded []byte
	if body != nil {
		encoded, _ = json.Marshal(body) //nolint:errchkjson // plain map bodies
	}
	request, _ := http.NewRequestWithContext(context.Background(), verb, path, bytes.NewReader(encoded))
	if encoded != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	h.gate.ServeHTTP(recorder, request)
	var decoded map[string]any
	_ = json.Unmarshal(recorder.Body.Bytes(), &decoded)
	return recorder.Code, decoded
}

func documentsPath(project string) string {
	return "/v1/projects/" + project + "/databases/(default)/documents/" + testCollection
}

func TestEmulatorDeniedBeforeStart(t *testing.T) {
	h := newRealHarness(t)
	if code, _ := h.invoke("actor:"+otherProject, http.MethodPatch, documentsPath(testProject)+"/"+testDoc, titleBody("no")); code != http.StatusForbidden {
		t.Fatalf("cross-project write = %d", code)
	}
	if statuses := h.manager.Status(); len(statuses) != 0 && statuses[0].Phase != backend.Stopped {
		t.Fatalf("a denied request started the emulator: %+v", statuses)
	}
}

func TestEmulatorLifecycleIsolationAndReset(t *testing.T) {
	h := newRealHarness(t)
	owner, other := "actor:"+testProject, "actor:"+otherProject
	code, created := h.invoke(owner, http.MethodPost, documentsPath(testProject)+"?documentId="+testDoc, titleBody("fog"))
	if code != http.StatusOK || created["name"] != "projects/"+testProject+"/databases/(default)/documents/"+testCollection+"/"+testDoc {
		t.Fatalf("create = %d %+v", code, created)
	}
	if code, _ := h.invoke(owner, http.MethodPost, documentsPath(testProject)+"?documentId="+testDoc, titleBody("fog")); code != http.StatusConflict {
		t.Fatalf("duplicate create = %d", code)
	}
	// The same collection and ID in another project is a different document.
	if code, _ := h.invoke(other, http.MethodGet, documentsPath(otherProject)+"/"+testDoc, nil); code != http.StatusNotFound {
		t.Fatalf("other project read = %d", code)
	}
	if code, listed := h.invoke(other, http.MethodGet, documentsPath(otherProject), nil); code != http.StatusOK || listed["documents"] != nil {
		t.Fatalf("other project listing = %d %+v", code, listed)
	}
	if code, _ := h.invoke(other, http.MethodPost, documentsPath(otherProject)+"?documentId="+testDoc, titleBody("other")); code != http.StatusOK {
		t.Fatalf("other project create = %d", code)
	}
	code, read := h.invoke(owner, http.MethodGet, documentsPath(testProject)+"/"+testDoc, nil)
	if fields, _ := read["fields"].(map[string]any); code != http.StatusOK || fields["title"].(map[string]any)["stringValue"] != "fog" {
		t.Fatalf("project document after other write = %d %+v", code, read)
	}
	if err := h.manager.Reset(context.Background(), backend.Key{Service: serviceName, Scope: scopeOf(testProject)}); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if code, _ := h.invoke(owner, http.MethodGet, documentsPath(testProject)+"/"+testDoc, nil); code != http.StatusNotFound {
		t.Fatalf("read after reset = %d", code)
	}
	if code, _ := h.invoke(other, http.MethodGet, documentsPath(otherProject)+"/"+testDoc, nil); code != http.StatusOK {
		t.Fatalf("reset reached another project: %d", code)
	}
}
