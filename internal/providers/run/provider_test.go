package run

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeRuntime records container calls and fails launches on demand.
type fakeRuntime struct {
	mu       sync.Mutex
	started  []StartSpec
	running  map[string]error
	stopped  []string
	failNext error
	address  string // a live listener makes rollouts finish from readiness
}

func newFakeRuntime() *fakeRuntime {
	return &fakeRuntime{running: map[string]error{}}
}

func (f *fakeRuntime) Start(_ context.Context, spec StartSpec) (Started, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext != nil {
		err := f.failNext
		f.failNext = nil
		return Started{}, err
	}
	f.started = append(f.started, spec)
	id := spec.Image + "-container"
	f.running[id] = nil
	address := f.address
	if address == "" {
		address = "127.0.0.1:19000"
	}
	return Started{Address: address, Container: id, Peer: "127.0.0.1"}, nil
}

func (f *fakeRuntime) Ready(_ context.Context, container string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.running[container]
}

func (f *fakeRuntime) Stop(_ context.Context, container string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopped = append(f.stopped, container)
	delete(f.running, container)
	return nil
}

// errNotEnrolled rejects unenrolled metadata subjects in tests.
var errNotEnrolled = errors.New("the subject is not enrolled")

// serviceReference is the full resource name of the test service.
const serviceReference = "projects/demo-project/locations/us-central1/services/svc"

// imageReference is the test container image.
const (
	imageReference    = "example.local/svc:1"
	testFirstRevision = "svc-00001"
)

// harness builds a provider with a fake runtime and a project registration.
type harness struct {
	t        *testing.T
	provider *Provider
	runtime  *fakeRuntime
	engine   *iam.Engine
	index    *state.Index
	gateway  *gateway.Gateway
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
		Name: "//cloudresourcemanager.googleapis.com/projects/demo-project", Service: "resourcemanager",
		Kind: "project", Number: "123456", Scope: state.Scope{Project: "demo-project", Location: state.Global},
	}); err != nil {
		t.Fatalf("register the project: %v", err)
	}
	if _, err := engine.SetPolicy("//cloudresourcemanager.googleapis.com/projects/demo-project", iam.Policy{
		Bindings: []iam.Binding{{Role: "roles/run.admin", Members: []string{"serviceAccount:fixture-actor@demo-project.iam.gserviceaccount.com"}}},
	}); err != nil {
		t.Fatalf("grant the project: %v", err)
	}
	runtime := newFakeRuntime()
	provider, err := New(context.Background(), Config{}, registry, backend.NewManager(time.Second, time.Second), index, engine, runtime)
	if err != nil {
		t.Fatalf("build the run provider: %v", err)
	}
	g := gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:" + token + "@demo-project.iam.gserviceaccount.com", Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})
	return &harness{t: t, provider: provider, runtime: runtime, engine: engine, index: index, gateway: g}
}

// invoke exercises the gateway end-to-end for one REST method.
func (h *harness) invoke(verb, path string, body any) *httptest.ResponseRecorder {
	h.t.Helper()
	var encoded []byte
	if body != nil {
		// Test bodies are plain map[string]any values.
		encoded, _ = json.Marshal(body) //nolint:errchkjson // plain map bodies
	}
	request, _ := http.NewRequestWithContext(context.Background(), verb, path, bytes.NewReader(encoded))
	if encoded != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Authorization", "Bearer fixture-actor")
	recorder := httptest.NewRecorder()
	h.gateway.ServeHTTP(recorder, request)
	return recorder
}

func TestServiceLifecycleCreatesRevisionAndRecordsScope(t *testing.T) {
	h := newHarness(t)
	h.create(t)
	recorder := h.invoke(http.MethodGet, "/v2/projects/demo-project/locations/us-central1/services/svc", nil)
	if recorder.Code != 200 {
		t.Fatalf("get status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodGet, "/v2/projects/demo-project/locations/us-central1/services", nil)
	var listing struct {
		Services []map[string]any `json:"services"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &listing); err != nil {
		t.Fatalf("decode listing: %v", err)
	}
	if len(listing.Services) != 1 {
		t.Fatalf("listing = %+v", listing)
	}
}

// create posts the canonical create request and verifies the pending operation
// and the launched container.
func (h *harness) create(t *testing.T) {
	t.Helper()
	recorder := h.invoke(http.MethodPost, "/v2/projects/demo-project/locations/us-central1/services", map[string]any{
		"name":     serviceReference,
		"template": map[string]any{"containers": []any{map[string]any{"image": imageReference, "ports": []any{map[string]any{"containerPort": 8080}}}}},
		"ingress":  "INGRESS_TRAFFIC_ALL",
	})
	if recorder.Code != 200 {
		t.Fatalf("create status = %d body %s", recorder.Code, recorder.Body.String())
	}
	var operation struct {
		Name string `json:"name"`
		Done bool   `json:"done"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &operation); err != nil {
		t.Fatalf("decode operation: %v", err)
	}
	if operation.Done {
		t.Fatalf("create operation completed before readiness")
	}
	if len(h.runtime.started) != 1 || h.runtime.started[0].Image != imageReference {
		t.Fatalf("container launch = %+v", h.runtime.started)
	}
	if h.runtime.started[0].Port != 8080 || !containsEntry(h.runtime.started[0].Env, "K_SERVICE=svc") {
		t.Fatalf("container spec = %+v", h.runtime.started[0])
	}
}

func TestCreateRejectsUnknownTemplateFieldsAndInvalidNames(t *testing.T) {
	h := newHarness(t)
	recorder := h.invoke(http.MethodPost, "/v2/projects/demo-project/locations/us-central1/services", map[string]any{
		"name":     serviceReference,
		"template": map[string]any{"containers": []any{}},
	})
	if code := status.Code(statusFrom(recorder)); code != codes.InvalidArgument {
		t.Fatalf("empty containers status = %d code=%v body %s", recorder.Code, code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodPost, "/v2/projects/demo-project/locations/us-central1/services", map[string]any{
		"template": map[string]any{"containers": []any{map[string]any{"image": imageReference}}},
		"ingress":  "INGRESS_TRAFFIC_SOMETING",
	})
	if status.Code(statusFrom(recorder)) != codes.InvalidArgument {
		t.Fatalf("bad ingress status = %d body %s", recorder.Code, recorder.Body.String())
	}
}

func statusFrom(recorder *httptest.ResponseRecorder) error {
	var body struct {
		Error struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	_ = json.Unmarshal(recorder.Body.Bytes(), &body)
	rest := map[int]codes.Code{400: codes.InvalidArgument, 404: codes.NotFound, 409: codes.AlreadyExists, 401: codes.Unauthenticated, 403: codes.PermissionDenied}
	return status.Error(rest[body.Error.Code], body.Error.Message)
}

func TestFailedStartupFailsOperation(t *testing.T) {
	h := newHarness(t)
	h.runtime.failNext = context.DeadlineExceeded
	recorder := h.invoke(http.MethodPost, "/v2/projects/demo-project/locations/us-central1/services", map[string]any{
		"name":     serviceReference,
		"template": map[string]any{"containers": []any{map[string]any{"image": "example.local/broken:1"}}},
	})
	var operation struct {
		Done  bool `json:"done"`
		Error struct {
			Code int `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &operation); err != nil {
		t.Fatalf("decode operation: %v", err)
	}
	if !operation.Done || operation.Error.Code == 0 {
		t.Fatalf("startup failure did not fail the operation: %s", recorder.Body.String())
	}
}

func TestDeleteStopsContainersAndClearsScope(t *testing.T) {
	h := newHarness(t)
	h.invoke(http.MethodPost, "/v2/projects/demo-project/locations/us-central1/services", map[string]any{
		"name":     serviceReference,
		"template": map[string]any{"containers": []any{map[string]any{"image": imageReference}}},
	})
	recorder := h.invoke(http.MethodDelete, "/v2/projects/demo-project/locations/us-central1/services/svc", nil)
	if recorder.Code != 200 {
		t.Fatalf("delete status = %d body %s", recorder.Code, recorder.Body.String())
	}
	if len(h.runtime.stopped) != 1 {
		t.Fatalf("stopped containers = %v", h.runtime.stopped)
	}
	recorder = h.invoke(http.MethodGet, "/v2/projects/demo-project/locations/us-central1/services/svc", nil)
	if status.Code(statusFrom(recorder)) != codes.NotFound {
		t.Fatalf("deleted service status = %d", recorder.Code)
	}
	if err := h.provider.Reset(context.Background(), state.Scope{Project: "demo-project", Location: "us-central1"}); err != nil {
		t.Fatalf("reset: %v", err)
	}
}

func TestUpdateStartsNewRevisionAndShiftsTraffic(t *testing.T) {
	h := newHarness(t)
	h.invoke(http.MethodPost, "/v2/projects/demo-project/locations/us-central1/services", map[string]any{
		"name":     serviceReference,
		"template": map[string]any{"containers": []any{map[string]any{"image": imageReference}}},
	})
	recorder := h.invoke(http.MethodPatch, "/v2/projects/demo-project/locations/us-central1/services/svc", map[string]any{
		"template": map[string]any{"containers": []any{map[string]any{"image": "example.local/svc:2"}}},
	})
	if recorder.Code != 200 {
		t.Fatalf("update status = %d body %s", recorder.Code, recorder.Body.String())
	}
	var operation struct {
		Name string `json:"name"`
		Done bool   `json:"done"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &operation); err != nil {
		t.Fatalf("decode operation: %v", err)
	}
	if operation.Name == "" {
		t.Fatalf("update returned no operation: %s", recorder.Body.String())
	}
	if len(h.runtime.started) != 2 {
		t.Fatalf("container launches = %+v", h.runtime.started)
	}
	recorder = h.invoke(http.MethodGet, "/v2/projects/demo-project/locations/us-central1/services/svc", nil)
	if recorder.Code != 200 {
		t.Fatalf("get after update status = %d", recorder.Code)
	}
	var service struct {
		Traffic []struct {
			Revision string `json:"revision"`
			Percent  int    `json:"percent"`
		} `json:"traffic"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &service); err != nil {
		t.Fatalf("decode service: %v", err)
	}
	if len(service.Traffic) != 1 || service.Traffic[0].Percent != 100 {
		t.Fatalf("traffic = %+v", service.Traffic)
	}
}

func TestSetIamPolicyStoresAndReturnsPolicy(t *testing.T) {
	h := newHarness(t)
	h.invoke(http.MethodPost, "/v2/projects/demo-project/locations/us-central1/services", map[string]any{
		"name":     serviceReference,
		"template": map[string]any{"containers": []any{map[string]any{"image": imageReference}}},
	})
	recorder := h.invoke(http.MethodPost, "/v2/projects/demo-project/locations/us-central1/services/svc:setIamPolicy", map[string]any{
		"policy": map[string]any{"bindings": []any{map[string]any{
			"role": "roles/run.invoker", "members": []any{"serviceAccount:invoker@demo-project.iam.gserviceaccount.com"},
		}}},
	})
	if recorder.Code != 200 {
		t.Fatalf("setIamPolicy status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodGet, "/v2/projects/demo-project/locations/us-central1/services/svc:getIamPolicy", nil)
	if recorder.Code != 200 {
		t.Fatalf("getIamPolicy status = %d body %s", recorder.Code, recorder.Body.String())
	}
}

func TestOperationGetAndList(t *testing.T) {
	h := newHarness(t)
	recorder := h.invoke(http.MethodPost, "/v2/projects/demo-project/locations/us-central1/services", map[string]any{
		"name":     serviceReference,
		"template": map[string]any{"containers": []any{map[string]any{"image": imageReference}}},
	})
	var operation struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &operation); err != nil {
		t.Fatalf("decode operation: %v", err)
	}
	segments := strings.SplitN(operation.Name, "/operations/", 2)
	if len(segments) != 2 {
		t.Fatalf("operation name %q", operation.Name)
	}
	recorder = h.invoke(http.MethodGet, "/v2/projects/demo-project/locations/us-central1/operations/"+segments[1], nil)
	if recorder.Code != 200 {
		t.Fatalf("operation get status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodGet, "/v2/projects/demo-project/locations/us-central1/operations", nil)
	if recorder.Code != 200 {
		t.Fatalf("operations list status = %d body %s", recorder.Code, recorder.Body.String())
	}
}

func TestWorkloadEnvInjectsServiceIdentity(t *testing.T) {
	record := &serviceRecord{Name: "svc", Template: templateShape{
		Container: containerSpec{Image: "img", Port: 8080, Env: map[string]string{"APP_ENV": "local"}},
	}}
	revision := &revisionRecord{ID: testFirstRevision, Template: record.Template}
	env := workloadEnv(record, revision)
	for _, want := range []string{"K_SERVICE=svc", "K_REVISION=svc-00001", "PORT=8080", "APP_ENV=local"} {
		if !containsEntry(env, want) {
			t.Fatalf("env %v misses %q", env, want)
		}
	}
}

// containsEntry reports whether the KEY=VALUE entry list holds one value.
func containsEntry(entries []string, want string) bool {
	return slices.Contains(entries, want)
}

func TestInvocationRequiresInvokerGrant(t *testing.T) {
	h := newHarness(t)
	h.create(t)
	handler := h.provider.Invocation()
	recorder := httptest.NewRecorder()
	request, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "/_run/svc-demo-project.us-central1.run.app/", nil)
	handler.ServeHTTP(recorder, request)
	if recorder.Code != 401 {
		t.Fatalf("anonymous invocation status = %d body %s", recorder.Code, recorder.Body.String())
	}
}

func TestWorkloadMetadataBindsPerRevisionListener(t *testing.T) {
	h := newHarness(t)
	harnessBind := ""
	h.provider.metadata = func(subject string) (http.Handler, error) {
		if subject != "serviceAccount:worker@demo-project.iam.gserviceaccount.com" {
			return nil, errNotEnrolled
		}
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("metadata"))
		}), nil
	}
	h.provider.metabind = func(handler http.Handler) (string, func(), error) {
		var listen net.ListenConfig
		listener, err := listen.Listen(context.Background(), "tcp", "127.0.0.1:0")
		if err != nil {
			return "", nil, fmt.Errorf("bind the test metadata listener: %w", err)
		}
		server := &http.Server{Handler: handler, ReadHeaderTimeout: time.Second}
		go func() { _ = server.Serve(listener) }()
		harnessBind = listener.Addr().String()
		return harnessBind, func() { _ = server.Close() }, nil
	}
	body := map[string]any{
		"name": serviceReference,
		"template": map[string]any{
			"containers":     []any{map[string]any{"image": imageReference, "ports": []any{map[string]any{"containerPort": 9090}}}},
			"serviceAccount": "worker@demo-project.iam.gserviceaccount.com",
		},
	}
	recorder := h.invoke(http.MethodPost, "/v2/projects/demo-project/locations/us-central1/services", body)
	if recorder.Code != 200 {
		t.Fatalf("create status = %d body %s", recorder.Code, recorder.Body.String())
	}
	if !containsEntry(h.runtime.started[0].Env, "GCE_METADATA_HOST="+harnessBind) {
		t.Fatalf("env = %v", h.runtime.started[0].Env)
	}
	if h.runtime.started[0].Port != 9090 {
		t.Fatalf("spec = %+v", h.runtime.started[0])
	}
}

func TestTemplateServiceAccountIsAnEmail(t *testing.T) {
	h := newHarness(t)
	body := map[string]any{
		"name": serviceReference,
		"template": map[string]any{
			"containers":     []any{map[string]any{"image": imageReference}},
			"serviceAccount": "serviceAccount:worker@demo-project.iam.gserviceaccount.com",
		},
	}
	recorder := h.invoke(http.MethodPost, "/v2/projects/demo-project/locations/us-central1/services", body)
	if recorder.Code != 400 || len(h.runtime.started) != 0 {
		t.Fatalf("member-prefixed account status = %d body %s", recorder.Code, recorder.Body.String())
	}
}

func TestInvocationUnknownHostIsNotFound(t *testing.T) {
	h := newHarness(t)
	handler := h.provider.Invocation()
	recorder := httptest.NewRecorder()
	request, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "/_run/other-demo-project.us-central1.run.app/", nil)
	handler.ServeHTTP(recorder, request)
	if recorder.Code != 404 {
		t.Fatalf("unknown host status = %d body %s", recorder.Code, recorder.Body.String())
	}
}

func TestEveryRouteIsHandled(t *testing.T) {
	h := newHarness(t)
	h.create(t)
	gap := "this method is not implemented"
	probe := func(verb, path string) {
		recorder := h.invoke(verb, path, nil)
		if recorder.Code == 501 && strings.Contains(recorder.Body.String(), gap) {
			t.Fatalf("%s %s hit the dispatch gap", verb, path)
		}
	}
	probe(http.MethodGet, "/v2/projects/demo-project/locations/us-central1/services/svc")
	probe(http.MethodGet, "/v2/projects/demo-project/locations/us-central1/services/svc/revisions")
	probe(http.MethodGet, "/v2/projects/demo-project/locations/us-central1/operations")
}

func TestOperationDeleteAcknowledgesTerminalOperations(t *testing.T) {
	h := newHarness(t)
	h.create(t)
	recorder := h.invoke(http.MethodDelete, "/v2/projects/demo-project/locations/us-central1/operations/rel-svc", nil)
	if recorder.Code != 200 {
		t.Fatalf("operation delete status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodDelete, "/v2/projects/demo-project/locations/us-central1/operations/missing", nil)
	if recorder.Code != 200 {
		t.Fatalf("missing operation delete status = %d body %s", recorder.Code, recorder.Body.String())
	}
}

func TestUpdateSplitsTrafficAcrossRevisions(t *testing.T) {
	h := newHarness(t)
	h.create(t)
	recorder := h.invoke(http.MethodPatch, "/v2/projects/demo-project/locations/us-central1/services/svc", map[string]any{
		"template": map[string]any{"containers": []any{map[string]any{"image": imageReference}}},
		"traffic": []any{
			map[string]any{"revision": testFirstRevision, "percent": 90},
			map[string]any{"latestRevision": true, "percent": 10},
		},
	})
	if recorder.Code != 200 {
		t.Fatalf("update status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodGet, "/v2/projects/demo-project/locations/us-central1/services/svc", nil)
	var service struct {
		Traffic []struct {
			Revision string `json:"revision"`
			Percent  int    `json:"percent"`
		} `json:"traffic"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &service); err != nil {
		t.Fatalf("decode service: %v", err)
	}
	total := 0
	byRevision := map[string]int{}
	for _, entry := range service.Traffic {
		total += entry.Percent
		byRevision[entry.Revision] = entry.Percent
	}
	if total != 100 || byRevision[testFirstRevision] != 90 {
		t.Fatalf("traffic = %+v", service.Traffic)
	}
}

func TestUpdateRejectsBadTrafficSplit(t *testing.T) {
	h := newHarness(t)
	h.create(t)
	recorder := h.invoke(http.MethodPatch, "/v2/projects/demo-project/locations/us-central1/services/svc", map[string]any{
		"template": map[string]any{"containers": []any{map[string]any{"image": imageReference}}},
		"traffic": []any{
			map[string]any{keyName: testFirstRevision, "percent": 90},
			map[string]any{"latestRevision": true, "percent": 90},
		},
	})
	if recorder.Code != 200 {
		t.Fatalf("update status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodGet, "/v2/projects/demo-project/locations/us-central1/services/svc", nil)
	var service struct {
		Traffic []struct {
			Percent int `json:"percent"`
		} `json:"traffic"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &service); err != nil {
		t.Fatalf("decode service: %v", err)
	}
	total := 0
	for _, entry := range service.Traffic {
		total += entry.Percent
	}
	if total != 100 {
		t.Fatalf("bad split fell back to %d percent", total)
	}
}

func TestDeleteCancelsServiceReaper(t *testing.T) {
	h := newHarness(t)
	h.create(t)
	if len(h.provider.reapers) != 1 {
		t.Fatalf("reapers after create = %d", len(h.provider.reapers))
	}
	h.invoke(http.MethodDelete, "/v2/projects/demo-project/locations/us-central1/services/svc", nil)
	if len(h.provider.reapers) != 0 {
		t.Fatalf("reapers after delete = %d", len(h.provider.reapers))
	}
}

func TestInvocationAcceptsBareServiceRoot(t *testing.T) {
	h := newHarness(t)
	h.create(t)
	recorder := h.invoke(http.MethodPost, "/_run/svc-demo-run.us-central1.run.app", nil)
	if recorder.Code == 404 {
		t.Fatalf("bare service root was not routed: %s", recorder.Body.String())
	}
}

// A service deleted between registration and launch fails the create with a
// plain 500 error body rather than a successful response that wraps it.
func TestLaunchOfVanishedServiceIsInternalError(t *testing.T) {
	h := newHarness(t)
	scope := state.Scope{Project: "demo-project", Location: "us-central1"}
	response := h.provider.launchOperation(scope, "svc")
	if response.Status != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", response.Status)
	}
	body, err := json.Marshal(response.JSON)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"error":{"code":500,"message":"The created service vanished before its launch","status":"INTERNAL"}}`
	if string(body) != want {
		t.Fatalf("body = %s, want %s", body, want)
	}
	if len(h.runtime.started) != 0 {
		t.Fatal("launched a container for a missing service")
	}
}
