package firestore

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// Names repeated across the provider tests.
const (
	testProject    = "demo-fs"
	otherProject   = "demo-other"
	testCollection = "books"
	testDoc        = "doc-1"
	testDocsPath   = "/v1/projects/" + testProject + "/databases/(default)/documents"
	testFieldPath  = "/v1/projects/" + testProject + "/databases/(default)/collectionGroups/" + testCollection + "/fields/title"
)

// seen is one request the fake emulator received.
type seen struct {
	Method, Path, Query, Authorization, Body string
}

// fakeEmulator stands in for one scoped JVM: it records what reaches it and
// replies with a fixed status and body.
type fakeEmulator struct {
	mu       sync.Mutex
	scope    state.Scope
	server   *httptest.Server
	rpc      *fakeRPC
	starts   int
	stops    int
	requests []seen
	status   int
	reply    string
}

func (f *fakeEmulator) Start(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.starts++
	if err := f.startGRPC(ctx); err != nil {
		return err
	}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.requests = append(f.requests, seen{r.Method, r.URL.EscapedPath(), r.URL.RawQuery, r.Header.Get("Authorization"), string(body)})
		code, reply := f.status, f.reply
		f.mu.Unlock()
		w.WriteHeader(code)
		_, _ = w.Write([]byte(reply))
	}))
	return nil
}
func (f *fakeEmulator) Ready(context.Context) error { return nil }

// counts returns the lifecycle counters and received requests under the lock.
func (f *fakeEmulator) counts() (int, int, []seen) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.starts, f.stops, append([]seen(nil), f.requests...)
}

// respond sets the next replies.
func (f *fakeEmulator) respond(code int, reply string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status, f.reply = code, reply
}
func (f *fakeEmulator) Stop(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.server != nil {
		f.server.Close()
		f.server = nil
		f.stops++
	}
	f.stopGRPC()
	return nil
}
func (f *fakeEmulator) Reset(ctx context.Context) error { return f.Stop(ctx) }
func (f *fakeEmulator) baseURL() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.server.URL
}

// harness drives the provider through the gateway with a fake per-scope backend.
type harness struct {
	t         *testing.T
	gate      *gateway.Gateway
	engine    *iam.Engine
	manager   *backend.Manager
	provider  *Provider
	mu        sync.Mutex
	instances []*fakeEmulator
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	registry := gateway.NewRegistry()
	index := state.NewIndex()
	engine, err := iam.New(index)
	if err != nil {
		t.Fatalf("build the iam engine: %v", err)
	}
	for _, project := range []string{testProject, otherProject} {
		if err := index.Put("resourcemanager", state.Resource{
			Name: projectResource(project), Service: "resourcemanager",
			Kind: "project", Number: "123456", Scope: scopeOf(project),
		}); err != nil {
			t.Fatalf("register the project: %v", err)
		}
		if _, err := engine.SetPolicy(projectResource(project), iam.Policy{Bindings: []iam.Binding{
			{Role: "roles/datastore.user", Members: []string{principal("actor", project)}},
			{Role: "roles/datastore.viewer", Members: []string{principal("viewer", project)}},
			{Role: "roles/datastore.indexAdmin", Members: []string{principal("admin", project)}},
		}}); err != nil {
			t.Fatalf("grant the project: %v", err)
		}
	}
	h := &harness{t: t, engine: engine, manager: backend.NewManager(time.Second, time.Second)}
	h.provider, err = newProvider(registry, h.manager, index, engine, func(scope state.Scope) (backend.Backend, error) {
		h.mu.Lock()
		defer h.mu.Unlock()
		instance := &fakeEmulator{scope: scope, status: 200, reply: `{"name":"from-emulator"}`}
		h.instances = append(h.instances, instance)
		return instance, nil
	})
	if err != nil {
		t.Fatalf("build the firestore provider: %v", err)
	}
	t.Cleanup(func() { _ = h.manager.Shutdown(context.Background()) })
	h.gate = gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		actor, project, _ := bytes.Cut([]byte(token), []byte(":"))
		return gateway.Principal{State: gateway.Authenticated, Subject: principal(string(actor), string(project)), Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})
	return h
}

func principal(actor, project string) string {
	return "serviceAccount:" + actor + "@" + project + ".iam.gserviceaccount.com"
}

// invoke issues one REST request with a token naming the actor and its project.
func (h *harness) invoke(token, verb, path string, body any) *httptest.ResponseRecorder {
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
	return recorder
}

// invokeRaw issues one REST request with a literal body.
func (h *harness) invokeRaw(token, verb, path, body string) *httptest.ResponseRecorder {
	h.t.Helper()
	request, _ := http.NewRequestWithContext(context.Background(), verb, path, strings.NewReader(body))
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	h.gate.ServeHTTP(recorder, request)
	return recorder
}

// started returns the backend instances created so far.
func (h *harness) started() []*fakeEmulator {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*fakeEmulator(nil), h.instances...)
}

const actor = "actor:" + testProject

func docPath() string { return testDocsPath + "/" + testCollection + "/" + testDoc }

func titleBody(title string) map[string]any {
	return map[string]any{"fields": map[string]any{"title": map[string]any{"stringValue": title}}}
}

func TestDeniedRequestsNeverStartOrReachTheEmulator(t *testing.T) {
	h := newHarness(t)
	viewer := "viewer:" + testProject
	stranger := "actor:" + otherProject
	for _, attempt := range []struct{ token, verb, path string }{
		{viewer, http.MethodPatch, docPath()},
		{viewer, http.MethodPost, testDocsPath + "/" + testCollection + "?documentId=x"},
		{viewer, http.MethodDelete, docPath()},
		{stranger, http.MethodGet, docPath()},
		{stranger, http.MethodGet, testDocsPath + "/" + testCollection},
	} {
		recorder := h.invoke(attempt.token, attempt.verb, attempt.path, titleBody("no"))
		if recorder.Code != http.StatusForbidden {
			t.Fatalf("%s %s as %s = %d %s", attempt.verb, attempt.path, attempt.token, recorder.Code, recorder.Body.String())
		}
	}
	if instances := h.started(); len(instances) != 0 {
		t.Fatalf("denied requests created %d backend instances", len(instances))
	}
}

func TestAllowedRequestIsForwardedWithoutCallerCredentials(t *testing.T) {
	h := newHarness(t)
	recorder := h.invoke(actor, http.MethodPatch, docPath()+"?updateMask.fieldPaths=title&access_token=secret&alt=json", titleBody("fog"))
	if recorder.Code != http.StatusOK || recorder.Body.String() != `{"name":"from-emulator"}` {
		t.Fatalf("forwarded response = %d %s", recorder.Code, recorder.Body.String())
	}
	instances := h.started()
	if len(instances) != 1 || instances[0].scope != scopeOf(testProject) {
		t.Fatalf("instances = %d", len(instances))
	}
	starts, _, got := instances[0].counts()
	if starts != 1 {
		t.Fatalf("starts = %d", starts)
	}
	want := seen{http.MethodPatch, "/v1/projects/" + testProject + "/databases/%28default%29/documents/" + testCollection + "/" + testDoc,
		"updateMask.fieldPaths=title", ownerBearer, `{"fields":{"title":{"stringValue":"fog"}}}`}
	if len(got) != 1 {
		t.Fatalf("requests = %+v", got)
	}
	// Go may escape the database segment; both spellings name the same path.
	if got[0].Path == "/v1/projects/"+testProject+"/databases/(default)/documents/"+testCollection+"/"+testDoc {
		want.Path = got[0].Path
	}
	if got[0] != want {
		t.Fatalf("forwarded request = %+v, want %+v", got[0], want)
	}
}

func TestEmulatorErrorsPassThrough(t *testing.T) {
	h := newHarness(t)
	h.invoke(actor, http.MethodGet, docPath(), nil)
	instance := h.started()[0]
	instance.respond(http.StatusConflict, `{"error":{"code":409,"message":"exists","status":"ALREADY_EXISTS"}}`)
	recorder := h.invoke(actor, http.MethodPost, testDocsPath+"/"+testCollection+"?documentId="+testDoc, titleBody("x"))
	var wire struct {
		Error struct {
			Status string `json:"status"`
		} `json:"error"`
	}
	if recorder.Code != http.StatusConflict || json.Unmarshal(recorder.Body.Bytes(), &wire) != nil || wire.Error.Status != "ALREADY_EXISTS" {
		t.Fatalf("duplicate = %d %s", recorder.Code, recorder.Body.String())
	}
	instance.respond(http.StatusNotFound, "Not Found")
	if recorder := h.invoke(actor, http.MethodGet, docPath(), nil); recorder.Code != http.StatusNotImplemented {
		t.Fatalf("non-JSON emulator reply = %d %s", recorder.Code, recorder.Body.String())
	}
}

func TestProjectsUseSeparateEmulators(t *testing.T) {
	h := newHarness(t)
	h.invoke(actor, http.MethodGet, docPath(), nil)
	other := "/v1/projects/" + otherProject + "/databases/(default)/documents/" + testCollection + "/" + testDoc
	if recorder := h.invoke("actor:"+otherProject, http.MethodGet, other, nil); recorder.Code != http.StatusOK {
		t.Fatalf("other project = %d %s", recorder.Code, recorder.Body.String())
	}
	instances := h.started()
	if len(instances) != 2 || instances[0].scope == instances[1].scope || instances[0].baseURL() == instances[1].baseURL() {
		t.Fatalf("instances = %d", len(instances))
	}
	for _, instance := range instances {
		if _, _, requests := instance.counts(); len(requests) != 1 {
			t.Fatalf("instance %v requests = %+v", instance.scope, requests)
		}
	}
}

func TestResetStopsTheEmulatorAndClearsFieldConfiguration(t *testing.T) {
	h := newHarness(t)
	admin := "admin:" + testProject
	if recorder := h.invoke(admin, http.MethodPatch, testFieldPath, map[string]any{"ttlConfig": map[string]any{}}); recorder.Code != http.StatusOK {
		t.Fatalf("field update = %d %s", recorder.Code, recorder.Body.String())
	}
	h.invoke(actor, http.MethodGet, docPath(), nil)
	if err := h.manager.Reset(context.Background(), backend.Key{Service: serviceName, Scope: scopeOf(testProject)}); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if _, stops, _ := h.started()[0].counts(); stops != 1 {
		t.Fatal("reset left the emulator running")
	}
	recorder := h.invoke(admin, http.MethodGet, testFieldPath, nil)
	var field struct {
		TTLConfig map[string]any `json:"ttlConfig"`
	}
	if recorder.Code != http.StatusOK || json.Unmarshal(recorder.Body.Bytes(), &field) != nil || len(field.TTLConfig) != 0 {
		t.Fatalf("field after reset = %d %s", recorder.Code, recorder.Body.String())
	}
	h.invoke(actor, http.MethodGet, docPath(), nil)
	// The manager restarts the scope's driver; Start launches a fresh process.
	starts, _, requests := h.started()[0].counts()
	if len(h.started()) != 1 || starts != 2 || len(requests) != 2 {
		t.Fatalf("documents after reset did not reach a restarted emulator: starts %d requests %+v", starts, requests)
	}
}

func TestInvalidPathIsRejectedBeforeTheBackend(t *testing.T) {
	h := newHarness(t)
	if recorder := h.invoke(actor, http.MethodGet, testDocsPath+"/"+testCollection+"/a%2Fb", nil); recorder.Code < 400 {
		t.Fatalf("invalid id status = %d", recorder.Code)
	}
	if len(h.started()) != 0 {
		t.Fatal("an invalid request started the backend")
	}
}
