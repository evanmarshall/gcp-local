package run

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/providers/provider/providertest"
	"github.com/evanmarshall/gcp-local/internal/providers/run/container"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestMain shortens the operation latency: the tests check ordering, not
// GCP's wall-clock delay, and the race detector makes every wait dearer.
func TestMain(m *testing.M) {
	operationLatency = 20 * time.Millisecond
	os.Exit(m.Run())
}

// fakeRuntime records container calls and fails launches on demand.
type fakeRuntime struct {
	mu       sync.Mutex
	started  []container.StartSpec
	running  map[string]error
	stopped  []string
	failNext error
	address  string // a live listener makes rollouts finish from readiness
}

func newFakeRuntime() *fakeRuntime {
	return &fakeRuntime{running: map[string]error{}}
}

func (f *fakeRuntime) Start(_ context.Context, spec container.StartSpec) (container.Started, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext != nil {
		err := f.failNext
		f.failNext = nil
		return container.Started{}, err
	}
	f.started = append(f.started, spec)
	id := spec.Image + "-container"
	f.running[id] = nil
	address := f.address
	if address == "" {
		address = "127.0.0.1:19000"
	}
	return container.Started{Address: address, Container: id, Peer: "127.0.0.1"}, nil
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
		Bindings: []iam.Binding{
			{Role: "roles/run.admin", Members: []string{"serviceAccount:fixture-actor@demo-project.iam.gserviceaccount.com"}},
			// Deploying needs actAs on the runtime service account.
			{Role: "roles/iam.serviceAccountUser", Members: []string{"serviceAccount:fixture-actor@demo-project.iam.gserviceaccount.com"}},
		},
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
	if recorder.Code != http.StatusOK {
		t.Fatalf("create = %d %s", recorder.Code, recorder.Body.String())
	}
	waitFor(t, func() bool { done, _ := h.rolloutState(t); return done })
	if _, code := h.rolloutState(t); code != float64(codes.Internal) {
		t.Fatalf("startup failure finished the rollout with code %v", code)
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
	template := templateShape{Container: containerSpec{Image: "img", Port: 8080, Env: map[string]string{"APP_ENV": "local"}}}
	record := &serviceRecord{Name: "svc", Spec: serviceSpec{Template: template}}
	revision := &revisionRecord{ID: testFirstRevision, Template: template}
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
	// GCP refuses an anonymous caller of a private service with 403
	// (recording run-service-lifecycle).
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("anonymous invocation status = %d body %s", recorder.Code, recorder.Body.String())
	}
}

func TestWorkloadMetadataBindsPerRevisionListener(t *testing.T) {
	h := newHarness(t)
	h.declareWorker(t)
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

func TestOperationDeleteForgetsTheOperation(t *testing.T) {
	h := newHarness(t)
	h.create(t)
	operation := h.newestOperation(t)
	if recorder := h.invoke(http.MethodDelete, "/v2/"+operation, nil); recorder.Code != http.StatusOK || recorder.Body.String() != "{}" {
		t.Fatalf("operation delete = %d %s", recorder.Code, recorder.Body.String())
	}
	for _, verb := range []string{http.MethodGet, http.MethodDelete} {
		if recorder := h.invoke(verb, "/v2/"+operation, nil); recorder.Code != http.StatusNotFound {
			t.Fatalf("%s of a deleted operation = %d %s", verb, recorder.Code, recorder.Body.String())
		}
	}
}

// Each rollout has its own operation (recording run-service-lifecycle).
func TestEveryRolloutHasItsOwnOperation(t *testing.T) {
	h := newHarness(t)
	h.create(t)
	first := h.newestOperation(t)
	update := map[string]any{"template": map[string]any{"containers": []any{map[string]any{"image": "example.local/svc:2"}}}}
	recorder := h.invoke(http.MethodPatch, "/v2/"+serviceReference, update)
	var operation struct {
		Name string `json:"name"`
	}
	if recorder.Code != http.StatusOK || json.Unmarshal(recorder.Body.Bytes(), &operation) != nil {
		t.Fatalf("update = %d %s", recorder.Code, recorder.Body.String())
	}
	if operation.Name == first || h.newestOperation(t) != operation.Name {
		t.Fatalf("update operation %q, create operation %q", operation.Name, first)
	}
	if recorder := h.invoke(http.MethodGet, "/v2/"+first, nil); recorder.Code != http.StatusOK {
		t.Fatalf("the create operation is gone after the update: %d", recorder.Code)
	}
}

// Request shapes the provider would otherwise accept and ignore are explicit
// gaps naming the field; nothing is launched.
func TestUnmodeledServiceFieldsAreGaps(t *testing.T) {
	h := newHarness(t)
	container := func(extra map[string]any) map[string]any {
		entry := map[string]any{"image": imageReference}
		maps.Copy(entry, extra)
		return entry
	}
	template := func(extra map[string]any) map[string]any {
		body := map[string]any{"containers": []any{container(nil)}}
		maps.Copy(body, extra)
		return body
	}
	cases := []struct {
		body map[string]any
		want string
	}{
		{map[string]any{"template": template(nil), "traffic": []any{
			map[string]any{"type": "TRAFFIC_TARGET_ALLOCATION_TYPE_REVISION", "revision": testFirstRevision, "percent": 90},
			map[string]any{"type": latestTraffic, "percent": 10},
		}}, "traffic other than 100% to the latest revision is not implemented"},
		{map[string]any{"template": template(nil), "traffic": []any{map[string]any{"type": latestTraffic, "percent": 100, "tag": "blue"}}},
			"traffic other than 100% to the latest revision is not implemented"},
		{map[string]any{"template": template(nil), "ingress": "INGRESS_TRAFFIC_INTERNAL_ONLY"}, "ingress INGRESS_TRAFFIC_INTERNAL_ONLY is not implemented"},
		{map[string]any{"template": template(nil), "scaling": map[string]any{"minInstanceCount": 1}}, "scaling is not modeled"},
		{map[string]any{"template": template(map[string]any{"scaling": map[string]any{"maxInstanceCount": 2}})}, "template.scaling is not modeled"},
		{map[string]any{"template": template(map[string]any{"vpcAccess": map[string]any{"connector": "c"}})}, "template.vpcAccess is not modeled"},
		{map[string]any{"template": template(map[string]any{"maxInstanceRequestConcurrency": 1})}, "template.maxInstanceRequestConcurrency is not modeled"},
		{map[string]any{"template": template(map[string]any{"encryptionKey": "k"})}, "template.encryptionKey is not modeled"},
		{map[string]any{"template": template(nil), "binaryAuthorization": map[string]any{"useDefault": true}}, "binaryAuthorization is not modeled"},
		{map[string]any{"template": map[string]any{"containers": []any{container(map[string]any{"resources": map[string]any{"limits": map[string]any{"cpu": "2"}}})}}},
			"template.containers.resources is not modeled"},
		{map[string]any{"template": map[string]any{"containers": []any{container(nil), container(nil)}}}, "more than one container (sidecars) is not implemented"},
		{map[string]any{"template": map[string]any{"containers": []any{container(map[string]any{"ports": []any{map[string]any{"name": "h2c"}}})}}},
			"port name h2c is not implemented"},
	}
	for _, c := range cases {
		recorder := h.invoke(http.MethodPost, servicesPath+"?serviceId=svc", c.body)
		if recorder.Code != http.StatusNotImplemented || !strings.Contains(recorder.Body.String(), "Cloud Run coverage gap: "+c.want) {
			t.Errorf("%v = %d %s", c.body, recorder.Code, recorder.Body.String())
		}
	}
	// GCP's own defaults are not refusals: all traffic to the latest
	// revision, all ingress, and an empty resources block.
	accepted := map[string]any{"template": map[string]any{"containers": []any{container(map[string]any{"resources": map[string]any{}})}},
		"ingress": "INGRESS_TRAFFIC_ALL", "traffic": []any{map[string]any{"type": latestTraffic, "percent": 100}}}
	if recorder := h.invoke(http.MethodPost, servicesPath+"?serviceId=svc", accepted); recorder.Code != http.StatusOK {
		t.Fatalf("default traffic and ingress = %d %s", recorder.Code, recorder.Body.String())
	}
	if len(h.runtime.started) != 1 {
		t.Fatalf("refused requests launched containers: %v", h.runtime.started)
	}
}

// Unknown names and mistyped values are GCP's transcoder 400s.
func TestServiceBodiesDecodeStrictly(t *testing.T) {
	h := newHarness(t)
	for body, want := range map[string]string{
		`{"template":{"containers":[{"image":"i"}]},"unknownField":1}`:                `Unknown name \"unknownField\" at 'service': Cannot find field.`,
		`{"template":{"containers":[{"image":"i","ports":[{"containerPort":"x"}]}]}}`: `Invalid value at 'service.template.containers[0].ports[0].container_port' (TYPE_INT32), \"x\"`,
	} {
		request, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, servicesPath+"?serviceId=svc", strings.NewReader(body))
		request.Header.Set("Authorization", "Bearer fixture-actor")
		request.Header.Set("Content-Type", "application/json")
		recorder := httptest.NewRecorder()
		h.gateway.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), want) {
			t.Errorf("%s = %d %s", body, recorder.Code, recorder.Body.String())
		}
	}
}

// Lists page in GCP's order: services by name, revisions and operations
// newest first. An unknown token is GCP's 500; a negative size is the default.
func TestListsPageInOrder(t *testing.T) {
	h := newHarness(t)
	for _, id := range []string{"svc-b", "svc-a"} {
		if recorder := h.invoke(http.MethodPost, servicesPath+"?serviceId="+id, serviceBody("")); recorder.Code != http.StatusOK {
			t.Fatalf("create %s = %d %s", id, recorder.Code, recorder.Body.String())
		}
	}
	first := page(t, h, servicesPath+"?pageSize=1", "services")
	second := page(t, h, servicesPath+"?pageSize=1&pageToken="+first.next, "services")
	if first.names[0] != testParent+"/services/svc-a" || second.names[0] != testParent+"/services/svc-b" || second.next != "" {
		t.Fatalf("pages = %+v %+v", first, second)
	}
	if all := page(t, h, servicesPath+"?pageSize=-1", "services"); len(all.names) != 2 {
		t.Fatalf("negative page size = %+v", all)
	}
	if recorder := h.invoke(http.MethodGet, servicesPath+"?pageToken=bogus", nil); recorder.Code != http.StatusInternalServerError ||
		!strings.Contains(recorder.Body.String(), "Internal error encountered.") {
		t.Fatalf("bogus page token = %d %s", recorder.Code, recorder.Body.String())
	}
}

// Revisions and operations list newest first (recording run-service-lifecycle).
func TestRevisionsAndOperationsListNewestFirst(t *testing.T) {
	h := newHarness(t)
	for _, id := range []string{"svc-b", "svc-a"} {
		if recorder := h.invoke(http.MethodPost, servicesPath+"?serviceId="+id, serviceBody("")); recorder.Code != http.StatusOK {
			t.Fatalf("create %s = %d %s", id, recorder.Code, recorder.Body.String())
		}
	}
	update := map[string]any{"template": map[string]any{"containers": []any{map[string]any{"image": "example.local/svc:2"}}}}
	if recorder := h.invoke(http.MethodPatch, "/v2/"+testParent+"/services/svc-a", update); recorder.Code != http.StatusOK {
		t.Fatalf("update = %d %s", recorder.Code, recorder.Body.String())
	}
	revisions := page(t, h, "/v2/"+testParent+"/services/svc-a/revisions", "revisions")
	if !slices.Equal(revisions.names, []string{testParent + "/services/svc-a/revisions/svc-a-00002", testParent + "/services/svc-a/revisions/svc-a-00001"}) {
		t.Fatalf("revisions = %v", revisions.names)
	}
	operations := page(t, h, "/v2/"+testParent+"/operations?pageSize=2", "operations")
	if len(operations.names) != 2 || operations.next == "" || h.newestOperation(t) != operations.names[0] {
		t.Fatalf("operations = %+v", operations)
	}
}

// listPage is one decoded list response.
type listPage struct {
	names []string
	next  string
}

// page lists one page and returns its item names and next token.
func page(t *testing.T, h *harness, path, field string) listPage {
	t.Helper()
	recorder := h.invoke(http.MethodGet, path, nil)
	var raw map[string]json.RawMessage
	if recorder.Code != http.StatusOK || json.Unmarshal(recorder.Body.Bytes(), &raw) != nil {
		t.Fatalf("list %s = %d %s", path, recorder.Code, recorder.Body.String())
	}
	var result listPage
	_ = json.Unmarshal(raw["nextPageToken"], &result.next)
	var items []struct {
		Name string `json:"name"`
	}
	_ = json.Unmarshal(raw[field], &items)
	for _, item := range items {
		result.names = append(result.names, item.Name)
	}
	return result
}

// The newest ready revision serves, whatever its name sorts as: invocations
// once went to the name-sorted last revision.
func TestTheNewestReadyRevisionServes(t *testing.T) {
	h := newHarness(t)
	revision := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer revision.Close()
	h.runtime.address = revision.Listener.Addr().String()
	named := func(name string) map[string]any {
		return map[string]any{"template": map[string]any{"revision": name, "containers": []any{map[string]any{"image": imageReference}}}}
	}
	if recorder := h.invoke(http.MethodPost, servicesPath+"?serviceId=svc", named("svc-zzz")); recorder.Code != http.StatusOK {
		t.Fatalf("create = %d %s", recorder.Code, recorder.Body.String())
	}
	waitFor(t, func() bool { done, _ := h.rolloutState(t); return done })
	if recorder := h.invoke(http.MethodPatch, "/v2/"+serviceReference, named("svc-aaa")); recorder.Code != http.StatusOK {
		t.Fatalf("update = %d %s", recorder.Code, recorder.Body.String())
	}
	waitFor(t, func() bool { done, _ := h.rolloutState(t); return done })
	serving, err := h.provider.lookupServing(hostOf(state.Scope{Project: "demo-project", Location: "us-central1"}, "svc"))
	if err != nil || serving.revision != "svc-aaa" {
		t.Fatalf("serving = %+v, %v", serving, err)
	}
}

// A service binding's condition sees request.time but no resource
// attributes, so a condition on resource.* grants nothing, as in GCP
// (recording run-service-lifecycle).
func TestServiceBindingConditionsSeeNoResourceAttributes(t *testing.T) {
	h := newHarness(t)
	if recorder := h.invoke(http.MethodPost, servicesPath+"?serviceId=svc", serviceBody("")); recorder.Code != http.StatusOK {
		t.Fatalf("create = %d %s", recorder.Code, recorder.Body.String())
	}
	for expression, want := range map[string]int{
		`request.time < timestamp("2100-01-01T00:00:00Z")`: http.StatusOK,
		`resource.type == "run.googleapis.com/Service" && resource.service == "run.googleapis.com" && resource.name == "` + serviceReference + `"`: http.StatusForbidden,
	} {
		recorder := h.invoke(http.MethodPost, "/v2/"+serviceReference+":setIamPolicy", map[string]any{"policy": map[string]any{
			"version": 3, "bindings": []any{map[string]any{"role": "roles/run.viewer", "members": []any{"serviceAccount:viewer@demo-project.iam.gserviceaccount.com"},
				"condition": map[string]any{"title": "probe", "expression": expression}}},
		}})
		if recorder.Code != http.StatusOK {
			t.Fatalf("setIamPolicy = %d %s", recorder.Code, recorder.Body.String())
		}
		request, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "/v2/"+serviceReference, nil)
		request.Header.Set("Authorization", "Bearer viewer")
		recorder = httptest.NewRecorder()
		h.gateway.ServeHTTP(recorder, request)
		if recorder.Code != want {
			t.Errorf("%s: viewer get = %d %s", expression, recorder.Code, recorder.Body.String())
		}
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

// A service deleted while its first revision launches finishes the create's
// rollout as aborted, and the launched container is stopped rather than leaked.
func TestServiceDeletedDuringLaunchDiscardsTheContainer(t *testing.T) {
	h := newHarness(t)
	blocking := &blockingRuntime{fakeRuntime: h.runtime, entered: make(chan struct{}), release: make(chan struct{})}
	h.provider.runtime = blocking
	created := make(chan string, 1)
	go func() {
		created <- h.invoke(http.MethodPost, servicesPath, serviceBody("")).Body.String()
	}()
	<-blocking.entered
	if recorder := h.invoke(http.MethodDelete, "/v2/"+serviceReference, nil); recorder.Code != http.StatusOK {
		t.Fatalf("delete during launch = %d %s", recorder.Code, recorder.Body.String())
	}
	close(blocking.release)
	var operation struct {
		Name string `json:"name"`
	}
	if json.Unmarshal([]byte(<-created), &operation) != nil || operation.Name == "" {
		t.Fatal("the create of a deleted service returned no operation")
	}
	waitFor(t, func() bool { done, _ := h.operationState(t, operation.Name); return done })
	if _, code := h.operationState(t, operation.Name); code != float64(codes.Aborted) {
		t.Fatalf("create of a deleted service finished with code %v", code)
	}
	h.runtime.mu.Lock()
	defer h.runtime.mu.Unlock()
	if !slices.Contains(h.runtime.stopped, imageReference+"-container") {
		t.Fatalf("the launched container leaked: stopped %v", h.runtime.stopped)
	}
}

// blockingRuntime holds each launch until the test releases it.
type blockingRuntime struct {
	*fakeRuntime
	entered, release chan struct{}
}

func (b *blockingRuntime) Start(ctx context.Context, spec container.StartSpec) (container.Started, error) {
	b.entered <- struct{}{}
	<-b.release
	return b.fakeRuntime.Start(ctx, spec)
}

func TestInvocationDialsOnlyLocalRevisionAddresses(t *testing.T) {
	for address, want := range map[string]bool{"127.0.0.1:8080": true, "172.30.0.5" + ":8080": true, "8.8.8.8:80": false, "metadata.google.internal:80": false} {
		if revisionAddress(address) != want {
			t.Errorf("%s: want %v", address, want)
		}
	}
}

// TestConcurrentServiceRequests races every service, revision, and operation
// method on one service while rollouts launch and readiness workers finish
// them. -race fails it if anything touches a record unlocked, and a request
// racing a delete must read 404, never an unverified-target 501.
func TestConcurrentServiceRequests(t *testing.T) {
	h := newHarness(t)
	revision := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer revision.Close()
	h.runtime.address = revision.Listener.Addr().String()
	service := "/v2/" + serviceReference
	update := map[string]any{"template": map[string]any{"containers": []any{map[string]any{"image": "example.local/svc:2"}}}}
	policy := map[string]any{"policy": map[string]any{"bindings": []any{map[string]any{
		"role": "roles/run.invoker", "members": []any{"serviceAccount:invoker@demo-project.iam.gserviceaccount.com"},
	}}}}
	providertest.Hammer(t, h.gateway, "fixture-actor", 8, 10, func(int) []providertest.Request {
		return []providertest.Request{
			{Method: http.MethodPost, Path: servicesPath, Body: serviceBody(""), Want: []int{200, 409}},
			{Method: http.MethodGet, Path: service, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: servicesPath, Want: []int{200}},
			// A concurrent rollout aborts the slower one (409).
			{Method: http.MethodPatch, Path: service, Body: update, Want: []int{200, 404, 409}},
			{Method: http.MethodPost, Path: service + ":setIamPolicy", Body: policy, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: service + ":getIamPolicy", Want: []int{200, 404}},
			{Method: http.MethodGet, Path: service + "/revisions", Want: []int{200, 404}},
			{Method: http.MethodGet, Path: service + "/revisions/svc-00001", Want: []int{200, 404}},
			{Method: http.MethodGet, Path: "/v2/projects/demo-project/locations/us-central1/operations", Want: []int{200}},
			{Method: http.MethodGet, Path: "/v2/projects/demo-project/locations/us-central1/operations/rel-svc", Want: []int{200, 404}},
			{Method: http.MethodDelete, Path: service, Want: []int{200, 404}},
		}
	})
	// The index entry and the service record change together.
	_, indexed := h.index.Get(serviceResource("demo-project", "us-central1", "svc"))
	if stored := h.invoke(http.MethodGet, service, nil).Code == http.StatusOK; stored != indexed {
		t.Fatalf("service stored = %v but indexed = %v", stored, indexed)
	}
}

// A recreated service starts with an empty policy, as in GCP: the deleted
// service's invokers cannot call its successor.
func TestRecreatedServiceStartsWithEmptyPolicy(t *testing.T) {
	h := newHarness(t)
	h.create(t)
	invoker := "serviceAccount:invoker@demo-project.iam.gserviceaccount.com"
	if recorder := h.invoke(http.MethodPost, "/v2/"+serviceReference+":setIamPolicy", map[string]any{"policy": map[string]any{
		"bindings": []any{map[string]any{"role": "roles/run.invoker", "members": []any{invoker}}},
	}}); recorder.Code != http.StatusOK {
		t.Fatalf("setIamPolicy = %d %s", recorder.Code, recorder.Body.String())
	}
	if recorder := h.invoke(http.MethodDelete, "/v2/"+serviceReference, nil); recorder.Code != http.StatusOK {
		t.Fatalf("delete = %d %s", recorder.Code, recorder.Body.String())
	}
	if recorder := h.invoke(http.MethodPost, servicesPath, serviceBody("")); recorder.Code != http.StatusOK {
		t.Fatalf("recreate = %d %s", recorder.Code, recorder.Body.String())
	}
	policy := h.invoke(http.MethodGet, "/v2/"+serviceReference+":getIamPolicy", nil)
	if policy.Code != http.StatusOK || strings.Contains(policy.Body.String(), invoker) {
		t.Fatalf("recreated service policy = %d %s", policy.Code, policy.Body.String())
	}
}

// Test subject identity for the revision metadata listener.
const (
	testProjectName   = "demo-project"
	testWorkerSubject = "serviceAccount:worker@demo-project.iam.gserviceaccount.com"
	testWorkerToken   = "gls.revision-metadata-subject-token-for-refresh-tests" //nolint:gosec // synthetic enrollment secret
)

// TestRevisionMetadataServesConfiguredIdentity proves the per-revision metadata
// listener mints tokens for the revision's configured service account only.
func TestRevisionMetadataServesConfiguredIdentity(t *testing.T) {
	subject := testWorkerSubject
	secret := testWorkerToken
	identities, err := identity.New(identity.Config{
		TokenLifetimeSeconds: 300,
		Principals:           []identity.Enrollment{{Subject: subject, Project: testProjectName, SubjectToken: secret}},
	}, nil)
	if err != nil {
		t.Fatalf("build the identity service: %v", err)
	}
	h := newHarness(t)
	h.declareWorker(t)
	h.provider.metadata = identities.Metadata
	h.provider.metabind = func(handler http.Handler) (string, func(), error) {
		var listen net.ListenConfig
		listener, err := listen.Listen(context.Background(), "tcp", "127.0.0.1:0")
		if err != nil {
			return "", nil, fmt.Errorf("bind the test metadata listener: %w", err)
		}
		server := &http.Server{Handler: handler, ReadHeaderTimeout: time.Second}
		go func() { _ = server.Serve(listener) }()
		return listener.Addr().String(), func() { _ = server.Close() }, nil
	}
	body := map[string]any{
		keyName: serviceReference,
		"template": map[string]any{
			"containers":     []any{map[string]any{"image": imageReference}},
			"serviceAccount": strings.TrimPrefix(subject, "serviceAccount:"),
		},
	}
	recorder := h.invoke(http.MethodPost, "/v2/projects/demo-project/locations/us-central1/services", body)
	if recorder.Code != 200 {
		t.Fatalf("create status = %d body %s", recorder.Code, recorder.Body.String())
	}
	metadata := boundMetadataHost(h)
	principal := fetchResolvedPrincipal(t, metadata, identities)
	if principal.State != gateway.Authenticated || principal.Subject != subject {
		t.Fatalf("resolved principal = %+v", principal)
	}
}

// boundMetadataHost extracts the revision's metadata listener address.
func boundMetadataHost(h *harness) string {
	for _, entry := range h.runtime.started[0].Env {
		if value, found := strings.CutPrefix(entry, "GCE_METADATA_HOST="); found {
			return value
		}
	}
	h.t.Fatal("no metadata listener bound for the revision")
	return ""
}

// fetchResolvedPrincipal mints and resolves one workload token.
func fetchResolvedPrincipal(t *testing.T, metadata string, identities *identity.Service) gateway.Principal {
	t.Helper()
	request, _ := http.NewRequestWithContext(context.Background(), http.MethodGet,
		"http://"+metadata+"/computeMetadata/v1/instance/service-accounts/default/token", nil)
	request.Header.Set("Metadata-Flavor", "Google")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("metadata token request: %v", err)
	}
	defer func() { _ = response.Body.Close() }()
	payload, _ := io.ReadAll(response.Body)
	var token struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(payload, &token); err != nil {
		t.Fatalf("decode token response %s: %v", payload, err)
	}
	principal, err := identities.Resolve(context.Background(), token.AccessToken)
	if err != nil {
		t.Fatalf("resolve the minted token: %v", err)
	}
	return principal
}

// exitingRuntime is the fake runtime whose containers exit only when a test
// sends their exit error.
type exitingRuntime struct {
	*fakeRuntime
	exits map[string]chan error
}

func (e *exitingRuntime) exit(container string) chan error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.exits[container] == nil {
		e.exits[container] = make(chan error, 1)
	}
	return e.exits[container]
}

func (e *exitingRuntime) Ready(ctx context.Context, container string) error {
	select {
	case err := <-e.exit(container):
		return err
	case <-ctx.Done():
		return nil
	}
}

// errExited is a container exit the tests inject.
var errExited = errors.New("the container exited")

// rolloutState reads the newest operation, the latest rollout: done, and its
// error code.
func (h *harness) rolloutState(t *testing.T) (bool, float64) {
	t.Helper()
	return h.operationState(t, h.newestOperation(t))
}

// newestOperation names the newest operation of the test location.
func (h *harness) newestOperation(t *testing.T) string {
	t.Helper()
	recorder := h.invoke(http.MethodGet, "/v2/projects/demo-project/locations/us-central1/operations", nil)
	var listing struct {
		Operations []struct {
			Name string `json:"name"`
		} `json:"operations"`
	}
	if recorder.Code != http.StatusOK || json.Unmarshal(recorder.Body.Bytes(), &listing) != nil || len(listing.Operations) == 0 {
		t.Fatalf("operations list = %d %s", recorder.Code, recorder.Body.String())
	}
	return listing.Operations[0].Name
}

// operationState reads one operation: done, and its error code.
func (h *harness) operationState(t *testing.T, name string) (bool, float64) {
	t.Helper()
	recorder := h.invoke(http.MethodGet, "/v2/"+name, nil)
	var operation struct {
		Done  bool `json:"done"`
		Error struct {
			Code float64 `json:"code"`
		} `json:"error"`
	}
	if recorder.Code != http.StatusOK || json.Unmarshal(recorder.Body.Bytes(), &operation) != nil {
		t.Fatalf("operation get = %d %s", recorder.Code, recorder.Body.String())
	}
	return operation.Done, operation.Error.Code
}

// hasReaper reports whether one service's idle reaper is registered.
func (h *harness) hasReaper(scope state.Scope, id string) bool {
	h.provider.reaperMu.Lock()
	defer h.provider.reaperMu.Unlock()
	return h.provider.reapers[reaperKey(scope, id)] != nil
}

// Service IDs repeat across locations; resetting one location must leave the
// other location's reaper running.
func TestResettingOneLocationKeepsAnotherLocationsReaper(t *testing.T) {
	h := newHarness(t)
	h.create(t)
	europe := map[string]any{"template": map[string]any{"containers": []any{map[string]any{"image": imageReference}}}}
	if recorder := h.invoke(http.MethodPost, "/v2/projects/demo-project/locations/europe-west1/services?serviceId=svc", europe); recorder.Code != http.StatusOK {
		t.Fatalf("europe create = %d %s", recorder.Code, recorder.Body.String())
	}
	central := state.Scope{Project: "demo-project", Location: "us-central1"}
	if err := h.provider.Reset(context.Background(), central); err != nil {
		t.Fatal(err)
	}
	if h.hasReaper(central, "svc") || !h.hasReaper(state.Scope{Project: "demo-project", Location: "europe-west1"}, "svc") {
		t.Fatal("resetting us-central1 cancelled the europe-west1 reaper or kept its own")
	}
}

// Deleting one service must leave its neighbors' reapers running.
func TestDeletingOneServiceKeepsAnothersReaper(t *testing.T) {
	h := newHarness(t)
	h.create(t)
	other := map[string]any{"template": map[string]any{"containers": []any{map[string]any{"image": "example.local/other:1"}}}}
	if recorder := h.invoke(http.MethodPost, servicesPath+"?serviceId=other", other); recorder.Code != http.StatusOK {
		t.Fatalf("second create = %d %s", recorder.Code, recorder.Body.String())
	}
	if recorder := h.invoke(http.MethodDelete, "/v2/"+serviceReference, nil); recorder.Code != http.StatusOK {
		t.Fatalf("delete = %d %s", recorder.Code, recorder.Body.String())
	}
	scope := state.Scope{Project: "demo-project", Location: "us-central1"}
	if h.hasReaper(scope, "svc") || !h.hasReaper(scope, "other") {
		t.Fatal("deleting svc cancelled the other service's reaper or kept its own")
	}
}

// A finished operation is final: a container that exits after its rollout
// succeeded must not turn the operation into an error.
func TestFinishedRolloutNeverFlipsToError(t *testing.T) {
	h := newHarness(t)
	revision := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer revision.Close()
	h.runtime.address = revision.Listener.Addr().String()
	runtime := &exitingRuntime{fakeRuntime: h.runtime, exits: map[string]chan error{}}
	h.provider.runtime = runtime
	h.create(t)
	waitFor(t, func() bool { done, _ := h.rolloutState(t); return done })
	if _, code := h.rolloutState(t); code != 0 {
		t.Fatalf("rollout failed with %v", code)
	}
	runtime.exit(imageReference + "-container") <- errExited
	time.Sleep(300 * time.Millisecond)
	if done, code := h.rolloutState(t); !done || code != 0 {
		t.Fatalf("succeeded rollout became done=%v code=%v", done, code)
	}
}

// Rollouts share one operation name; the first rollout's container exiting
// must not finish the second rollout's operation.
func TestStaleWatcherCannotFinishTheNextRollout(t *testing.T) {
	h := newHarness(t)
	runtime := &exitingRuntime{fakeRuntime: h.runtime, exits: map[string]chan error{}}
	h.provider.runtime = runtime
	h.create(t)
	update := map[string]any{"template": map[string]any{"containers": []any{map[string]any{"image": "example.local/svc:2"}}}}
	if recorder := h.invoke(http.MethodPatch, "/v2/"+serviceReference, update); recorder.Code != http.StatusOK {
		t.Fatalf("update = %d %s", recorder.Code, recorder.Body.String())
	}
	runtime.exit(imageReference + "-container") <- errExited
	time.Sleep(300 * time.Millisecond)
	if done, code := h.rolloutState(t); done {
		t.Fatalf("the first rollout's exit finished the second rollout with code %v", code)
	}
}

// waitFor polls a condition for up to ten seconds.
func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("the condition did not hold in time")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// An invocation of a reaped revision cold-starts its container and rearms
// the idle reaper.
func TestInvocationColdStartsAReapedRevision(t *testing.T) {
	h := newHarness(t)
	h.create(t)
	if recorder := h.invoke(http.MethodPost, "/v2/"+serviceReference+":setIamPolicy", map[string]any{"policy": map[string]any{
		"bindings": []any{map[string]any{"role": "roles/run.invoker", "members": []any{"allUsers"}}},
	}}); recorder.Code != http.StatusOK {
		t.Fatalf("setIamPolicy = %d %s", recorder.Code, recorder.Body.String())
	}
	scope := state.Scope{Project: "demo-project", Location: "us-central1"}
	h.provider.clock = func() time.Time { return time.Now().Add(2 * idleTimeout) }
	if !h.provider.reap(context.Background(), scope, "svc") {
		t.Fatal("the idle revision was not reaped")
	}
	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/_run/svc-demo-project.us-central1.run.app/", nil)
	h.provider.Invocation().ServeHTTP(httptest.NewRecorder(), request)
	h.runtime.mu.Lock()
	launches := len(h.runtime.started)
	h.runtime.mu.Unlock()
	if launches != 2 || !h.hasReaper(scope, "svc") {
		t.Fatalf("after the invocation: %d launches, reaper armed %v", launches, h.hasReaper(scope, "svc"))
	}
}

// deafRuntime is a runtime whose revision never holds its own address and
// exits shortly after start, like a revision that lost its port.
type deafRuntime struct{ *fakeRuntime }

func (deafRuntime) Listening(string) bool { return false }

func (deafRuntime) Ready(context.Context, string) error {
	time.Sleep(time.Second)
	return errRevisionExited
}

// errRevisionExited is the exit deafRuntime reports.
var errRevisionExited = errors.New("the revision process exited")

// Another process can bind the port between reserve and the revision's own
// bind. Its answers must not make the revision ready: the rollout fails when
// the revision exits, and never reports success first.
func TestRolloutIgnoresAForeignListenerOnTheRevisionPort(t *testing.T) {
	foreign := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer foreign.Close()
	h := newHarness(t)
	h.runtime.address = foreign.Listener.Addr().String()
	h.provider.runtime = deafRuntime{h.runtime}
	if recorder := h.invoke(http.MethodPost, servicesPath+"?serviceId=svc", map[string]any{
		"template": map[string]any{"containers": []any{map[string]any{"image": imageReference}}},
	}); recorder.Code != http.StatusOK {
		t.Fatalf("create = %d %s", recorder.Code, recorder.Body.String())
	}
	var code float64
	waitFor(t, func() bool { done, observed := h.rolloutState(t); code = observed; return done })
	if code == 0 {
		t.Fatal("the rollout succeeded on a foreign listener's answers")
	}
}

// helperImage maps to this test binary re-run as a revision process; the
// service's MODE env selects what it does.
const helperImage = "example.local/helper:1"

// TestRunHelperProcess is the revision process, not a test: in serve mode it
// serves PORT, exiting 1 when the port is taken.
func TestRunHelperProcess(*testing.T) {
	if os.Getenv("MODE") != "serve" {
		return
	}
	listener, err := net.Listen("tcp", "127.0.0.1:"+os.Getenv("PORT")) //nolint:noctx // the helper process has no context
	if err != nil {
		os.Exit(1)
	}
	_ = http.Serve(listener, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })) //nolint:gosec // a loopback test helper
	os.Exit(0)
}

// A revision that binds its own port becomes ready, and the runtime sees
// its listener.
func TestRolloutSucceedsWhenTheRevisionListens(t *testing.T) {
	h := newHarness(t)
	h.provider.runtime = container.NewProcessRuntime(map[string][]string{helperImage: {os.Args[0], "-test.run=^TestRunHelperProcess$"}})
	body := map[string]any{"template": map[string]any{"containers": []any{map[string]any{
		"image": helperImage, "env": []any{map[string]any{"name": "MODE", "value": "serve"}},
	}}}}
	if recorder := h.invoke(http.MethodPost, servicesPath+"?serviceId=svc", body); recorder.Code != http.StatusOK {
		t.Fatalf("create = %d %s", recorder.Code, recorder.Body.String())
	}
	t.Cleanup(func() { _ = h.invoke(http.MethodDelete, "/v2/"+serviceReference, nil) })
	waitFor(t, func() bool { done, _ := h.rolloutState(t); return done })
	if _, code := h.rolloutState(t); code != 0 {
		t.Fatalf("rollout failed with code %v", code)
	}
}
