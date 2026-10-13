package memorystore

import (
	"bytes"
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// Names repeated across the provider tests.
const (
	testProject  = "demo-redis"
	peerProject  = "peer-redis"
	testRegion   = "us-central1"
	otherRegion  = "us-east1"
	testInstance = "cache-1"
	adminToken   = "admin"
	viewerToken  = "viewer"
	peerToken    = "peer"
)

func base(project, region string) string {
	return "/v1/projects/" + project + "/locations/" + region
}

var testInstances = base(testProject, testRegion) + "/instances"

func principal(token string) string {
	project := testProject
	if token == peerToken {
		project = peerProject
	}
	return "serviceAccount:" + token + "@" + project + ".iam.gserviceaccount.com"
}

// harness drives the provider through the gateway with one fake driver per scope.
type harness struct {
	t        *testing.T
	gate     *gateway.Gateway
	engine   *iam.Engine
	registry *gateway.Registry
	index    *state.Index
	manager  *backend.Manager
	provider *Provider
	mu       sync.Mutex
	fakes    map[state.Scope]*fakeDriver
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{t: t, index: state.NewIndex(), fakes: map[state.Scope]*fakeDriver{}}
	h.engine = newEngine(t, h.index)
	h.manager = backend.NewManager(time.Second, time.Second)
	registry := gateway.NewRegistry()
	h.registry = registry
	var err error
	h.provider, err = newProvider(registry, h.manager, h.index, h.engine, 0, func(scope state.Scope) (backend.Backend, error) {
		h.mu.Lock()
		defer h.mu.Unlock()
		fake := newFakeDriver()
		h.fakes[scope] = fake
		return fake, nil
	})
	must(t, err)
	t.Cleanup(func() { must(t, h.manager.Shutdown(context.Background())) })
	h.gate = gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: principal(token),
			Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})
	return h
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// fake returns the driver for a project's test region, or nil when it never started.
func (h *harness) fake(project string) *fakeDriver {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.fakes[state.Scope{Project: project, Location: testRegion}]
}

// object narrows a decoded JSON value to an object, failing the test otherwise.
func object(t *testing.T, value any) map[string]any {
	t.Helper()
	result, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("value %v is not an object", value)
	}
	return result
}

// invoke issues one REST request; an empty token sends no credential.
func (h *harness) invoke(token, verb, path string, body any) *httptest.ResponseRecorder {
	h.t.Helper()
	var encoded []byte
	if body != nil {
		var err error
		encoded, err = json.Marshal(body)
		must(h.t, err)
	}
	request := httptest.NewRequestWithContext(context.Background(), verb, path, bytes.NewReader(encoded))
	if encoded != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	recorder := httptest.NewRecorder()
	h.gate.ServeHTTP(recorder, request)
	return recorder
}

// expect asserts one response status and decodes the body.
func (h *harness) expect(recorder *httptest.ResponseRecorder, code int) map[string]any {
	h.t.Helper()
	if recorder.Code != code {
		h.t.Fatalf("status = %d, want %d: %s", recorder.Code, code, recorder.Body.String())
	}
	var body map[string]any
	must(h.t, json.Unmarshal(recorder.Body.Bytes(), &body))
	return body
}

// basic is the smallest documented create request.
func basic(extra map[string]any) map[string]any {
	body := map[string]any{fieldTier: tierBasic, fieldMemory: 1, fieldVersion: "REDIS_7_2", fieldDisplayName: "cache",
		"labels": map[string]string{"managed-by": "gcp-local"}}
	maps.Copy(body, extra)
	return body
}

func (h *harness) create(token, project, region, id string, body map[string]any) *httptest.ResponseRecorder {
	h.t.Helper()
	return h.invoke(token, http.MethodPost, base(project, region)+"/instances?instanceId="+id, body)
}

func TestDeniedCallsStartNoBackend(t *testing.T) {
	h := newHarness(t)
	denied := h.expect(h.create(peerToken, testProject, testRegion, testInstance, basic(nil)), http.StatusForbidden)
	message, _ := denied["error"].(map[string]any)["message"].(string)
	// GCP's wording, with no details (memorystore-redis-instance-admin).
	want := "Permission 'redis.instances.create' denied on 'projects/demo-redis/locations/us-central1'"
	if message != want || object(t, denied["error"])["details"] != nil {
		t.Fatalf("denial = %v", denied)
	}
	h.expect(h.create("", testProject, testRegion, testInstance, basic(nil)), http.StatusUnauthorized)
	h.expect(h.create(viewerToken, testProject, testRegion, testInstance, basic(nil)), http.StatusForbidden)
	h.expect(h.invoke(peerToken, http.MethodGet, testInstances+"/"+testInstance+"/authString", nil), http.StatusForbidden)
	// Invalid requests from unauthorized callers are denied, not validated.
	h.expect(h.create(peerToken, testProject, testRegion, "BAD", map[string]any{"bogus": 1}), http.StatusForbidden)
	if status := h.manager.Status(); len(status) != 0 {
		t.Fatalf("a refused request started a backend: %+v", status)
	}
}

func TestInstanceLifecycle(t *testing.T) {
	h := newHarness(t)
	h.clock(time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC))
	created := h.expect(h.create(adminToken, testProject, testRegion, testInstance, basic(map[string]any{"authEnabled": true})), http.StatusOK)
	instance := checkOperation(t, created, "create", instanceType)
	name := "projects/demo-redis/locations/us-central1/instances/cache-1"
	for field, want := range map[string]any{"name": name, fieldTier: "BASIC", fieldMemory: 1.0, fieldVersion: "REDIS_7_2",
		"host": "127.0.0.1", "port": 16380.0, "state": "READY", "authEnabled": true, "transitEncryptionMode": "DISABLED",
		"connectMode": "DIRECT_PEERING", "locationId": "us-central1-a", fieldDisplayName: "cache",
		"authorizedNetwork": "projects/demo-redis/global/networks/default", "createTime": "2026-10-11T12:00:00Z"} {
		if instance[field] != want {
			t.Fatalf("instance %s = %v, want %v", field, instance[field], want)
		}
	}
	spec := h.fake(testProject).running()[testInstance]
	if !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(spec.AuthString) || spec.MemoryGB != 1 {
		t.Fatalf("launch spec = %+v", spec)
	}
	path := testInstances + "/" + testInstance
	if read := h.expect(h.invoke(viewerToken, http.MethodGet, path, nil), http.StatusOK); read["port"] != 16380.0 {
		t.Fatalf("read = %v", read)
	}
	if auth := h.expect(h.invoke(adminToken, http.MethodGet, path+"/authString", nil), http.StatusOK); auth["authString"] != spec.AuthString {
		t.Fatalf("authString = %v", auth)
	}
	h.expect(h.invoke(viewerToken, http.MethodGet, path+"/authString", nil), http.StatusForbidden)
	listed := h.expect(h.invoke(viewerToken, http.MethodGet, testInstances, nil), http.StatusOK)
	if items, _ := listed["instances"].([]any); len(items) != 1 {
		t.Fatalf("list = %v", listed)
	}
	if _, ok := h.index.Get(fullName(name)); !ok {
		t.Fatal("instance reference was not published")
	}
	h.patchAndDelete(path, name)
}

// patchAndDelete continues the lifecycle: update, operations, delete.
func (h *harness) patchAndDelete(path, name string) {
	t := h.t
	h.expect(h.invoke(viewerToken, http.MethodPatch, path+"?updateMask=displayName", map[string]any{fieldDisplayName: "x"}), http.StatusForbidden)
	patched := h.expect(h.invoke(adminToken, http.MethodPatch, path+"?updateMask=displayName,labels,memory_size_gb",
		map[string]any{fieldDisplayName: "renamed", "labels": map[string]string{"env": "dev"}, fieldMemory: 2, fieldTier: "BASIC"}), http.StatusOK)
	instance := checkOperation(t, patched, "update", instanceType)
	if instance["displayName"] != "renamed" || instance["memorySizeGb"] != 2.0 || object(t, instance["labels"])["env"] != "dev" {
		t.Fatalf("patched = %v", instance)
	}
	if size := h.fake(testProject).resized[testInstance]; size != 2 {
		t.Fatalf("resize = %d", size)
	}
	operationName, _ := patched["name"].(string)
	operationPath := "/v1/" + operationName
	if read := h.expect(h.invoke(viewerToken, http.MethodGet, operationPath, nil), http.StatusOK); read["name"] != patched["name"] {
		t.Fatalf("operation = %v", read)
	}
	operations := h.expect(h.invoke(viewerToken, http.MethodGet, base(testProject, testRegion)+"/operations", nil), http.StatusOK)
	if items, _ := operations["operations"].([]any); len(items) != 2 {
		t.Fatalf("operations = %v", operations)
	}
	h.expect(h.invoke(viewerToken, http.MethodDelete, path, nil), http.StatusForbidden)
	deleted := h.expect(h.invoke(adminToken, http.MethodDelete, path, nil), http.StatusOK)
	checkOperation(t, deleted, "delete", emptyType)
	if removed := h.fake(testProject).removed; len(removed) != 1 || removed[0] != testInstance {
		t.Fatalf("removed = %v", removed)
	}
	h.expect(h.invoke(adminToken, http.MethodGet, path, nil), http.StatusNotFound)
	h.expect(h.invoke(adminToken, http.MethodDelete, path, nil), http.StatusNotFound)
	if _, ok := h.index.Get(fullName(name)); ok {
		t.Fatal("instance reference survived deletion")
	}
}

// checkOperation asserts a completed operation and returns its response.
func checkOperation(t *testing.T, value map[string]any, verb, responseType string) map[string]any {
	t.Helper()
	metadata, _ := value["metadata"].(map[string]any)
	response, _ := value["response"].(map[string]any)
	name, _ := value["name"].(string)
	if value["done"] != true || metadata["@type"] != operationType || metadata["verb"] != verb || metadata["apiVersion"] != "v1" ||
		response["@type"] != responseType || !strings.HasPrefix(name, "projects/demo-redis/locations/us-central1/operations/operation-") {
		t.Fatalf("operation = %v", value)
	}
	return response
}

// clock fixes the provider clock before any request is served.
func (h *harness) clock(at time.Time) {
	h.provider.clock = func() time.Time { return at }
}

func TestValidationAndGaps(t *testing.T) {
	h := newHarness(t)
	cases := []struct {
		id   string
		body map[string]any
		code int
	}{
		{"BAD", basic(nil), http.StatusBadRequest},
		{"ok-1", basic(map[string]any{"bogus": true}), http.StatusBadRequest},
		{"ok-2", basic(map[string]any{fieldTier: "STANDARD_HA"}), http.StatusNotImplemented},
		{"ok-3", basic(map[string]any{fieldVersion: "REDIS_6_X"}), http.StatusNotImplemented},
		{"ok-4", basic(map[string]any{"transitEncryptionMode": "SERVER_AUTHENTICATION"}), http.StatusNotImplemented},
		{"ok-5", basic(map[string]any{"redisConfigs": map[string]string{"maxmemory-policy": "allkeys-lru"}}), http.StatusNotImplemented},
		{"ok-6", basic(map[string]any{fieldMemory: 0}), http.StatusBadRequest},
		{"ok-7", map[string]any{fieldMemory: 1}, http.StatusBadRequest},
		{"ok-8", basic(map[string]any{"locationId": "us-east1-b"}), http.StatusBadRequest},
	}
	for _, c := range cases {
		h.expect(h.create(adminToken, testProject, testRegion, c.id, c.body), c.code)
	}
	if running := h.fake(testProject); running != nil && len(running.running()) != 0 {
		t.Fatalf("invalid creates launched servers: %v", running.running())
	}
	// Empty values of unmodeled fields are accepted, as Terraform sends them.
	h.expect(h.create(adminToken, testProject, testRegion, testInstance, basic(map[string]any{"redisConfigs": map[string]string{}, "replicaCount": 0})), http.StatusOK)
	h.expect(h.create(adminToken, testProject, testRegion, testInstance, basic(nil)), http.StatusConflict)
	path := testInstances + "/" + testInstance
	h.expect(h.invoke(adminToken, http.MethodPatch, path, map[string]any{fieldDisplayName: "x"}), http.StatusBadRequest)
	h.expect(h.invoke(adminToken, http.MethodPatch, path+"?updateMask=redisConfigs", map[string]any{}), http.StatusNotImplemented)
	h.expect(h.invoke(adminToken, http.MethodPatch, path+"?updateMask=tier", map[string]any{}), http.StatusBadRequest)
	h.expect(h.invoke(adminToken, http.MethodPatch, path+"?updateMask=memorySizeGb", map[string]any{fieldMemory: 301}), http.StatusBadRequest)
	h.expect(h.invoke(adminToken, http.MethodGet, base(testProject, "global")+"/instances", nil), http.StatusBadRequest)
	h.expect(h.invoke(adminToken, http.MethodGet, base(testProject, "-")+"/instances", nil), http.StatusNotImplemented)
	h.expect(h.invoke(adminToken, http.MethodGet, base(testProject, testRegion)+"/operations?filter=done", nil), http.StatusNotImplemented)
	if auth := h.expect(h.invoke(adminToken, http.MethodGet, path+"/authString", nil), http.StatusOK); len(auth) != 0 {
		t.Fatalf("authString without AUTH = %v", auth)
	}
}

func TestScopeIsolationAndReset(t *testing.T) {
	h := newHarness(t)
	h.expect(h.create(adminToken, testProject, testRegion, testInstance, basic(nil)), http.StatusOK)
	h.expect(h.create(adminToken, testProject, otherRegion, testInstance, basic(nil)), http.StatusOK)
	h.expect(h.create(peerToken, peerProject, testRegion, testInstance, basic(nil)), http.StatusOK)
	// Another project's instance of the same ID is invisible, even to its admin elsewhere.
	h.expect(h.invoke(peerToken, http.MethodGet, testInstances+"/"+testInstance, nil), http.StatusForbidden)
	listed := h.expect(h.invoke(adminToken, http.MethodGet, base(testProject, otherRegion)+"/instances", nil), http.StatusOK)
	if items, _ := listed["instances"].([]any); len(items) != 1 || object(t, items[0])["name"] != "projects/demo-redis/locations/us-east1/instances/cache-1" {
		t.Fatalf("regional list = %v", listed)
	}
	must(t, h.manager.Reset(context.Background(), backend.Key{Service: serviceName, Scope: state.Scope{Project: testProject, Location: testRegion}}))
	if fake := h.fake(testProject); fake.stops != 1 || len(fake.running()) != 0 {
		t.Fatalf("reset did not stop the scope's servers: %+v", fake)
	}
	h.expect(h.invoke(adminToken, http.MethodGet, testInstances+"/"+testInstance, nil), http.StatusNotFound)
	if _, ok := h.index.Get(fullName(instanceName(state.Scope{Project: testProject, Location: testRegion}, testInstance))); ok {
		t.Fatal("reset kept the scope's index reference")
	}
	h.expect(h.invoke(adminToken, http.MethodGet, base(testProject, otherRegion)+"/instances/"+testInstance, nil), http.StatusOK)
	h.expect(h.invoke(peerToken, http.MethodGet, base(peerProject, testRegion)+"/instances/"+testInstance, nil), http.StatusOK)
	if fake := h.fake(peerProject); fake.stops != 0 || len(fake.running()) != 1 {
		t.Fatalf("reset reached another project: %+v", fake)
	}
	// The scope restarts lazily and accepts the same ID again.
	h.expect(h.create(adminToken, testProject, testRegion, testInstance, basic(nil)), http.StatusOK)
}

func TestFailedPublicationStopsTheNewServer(t *testing.T) {
	h := newHarness(t)
	name := fullName(instanceName(state.Scope{Project: testProject, Location: testRegion}, testInstance))
	must(t, h.index.Put("other", state.Resource{Name: name, Service: "other", Kind: "instance",
		Parent: provider.ProjectResource(testProject), Scope: state.Scope{Project: testProject, Location: testRegion}}))
	recorder := h.create(adminToken, testProject, testRegion, testInstance, basic(nil))
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d: %s", recorder.Code, recorder.Body.String())
	}
	if fake := h.fake(testProject); len(fake.running()) != 0 || len(fake.removed) != 1 {
		t.Fatalf("failed publication left a server: %+v", fake)
	}
	h.expect(h.invoke(adminToken, http.MethodGet, testInstances+"/"+testInstance, nil), http.StatusNotFound)
	operations := h.expect(h.invoke(adminToken, http.MethodGet, base(testProject, testRegion)+"/operations", nil), http.StatusOK)
	if len(operations) != 0 {
		t.Fatalf("failed create kept an operation: %v", operations)
	}
}

func TestLaunchFailureLeavesNoMetadata(t *testing.T) {
	h := newHarness(t)
	h.expect(h.create(adminToken, testProject, testRegion, "warmup", basic(nil)), http.StatusOK)
	h.fake(testProject).launchFault = errInjected
	if recorder := h.create(adminToken, testProject, testRegion, testInstance, basic(nil)); recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d", recorder.Code)
	}
	h.expect(h.invoke(adminToken, http.MethodGet, testInstances+"/"+testInstance, nil), http.StatusNotFound)
}

func TestListPagination(t *testing.T) {
	h := newHarness(t)
	for _, id := range []string{"c", "a", "b"} {
		h.expect(h.create(adminToken, testProject, testRegion, id, basic(nil)), http.StatusOK)
	}
	first := h.expect(h.invoke(adminToken, http.MethodGet, testInstances+"?pageSize=2", nil), http.StatusOK)
	token, _ := first["nextPageToken"].(string)
	if items, _ := first["instances"].([]any); len(items) != 2 || token == "" {
		t.Fatalf("first page = %v", first)
	}
	second := h.expect(h.invoke(adminToken, http.MethodGet, testInstances+"?pageSize=2&pageToken="+token, nil), http.StatusOK)
	items, _ := second["instances"].([]any)
	if len(items) != 1 || object(t, items[0])["name"] != "projects/demo-redis/locations/us-central1/instances/c" || second["nextPageToken"] != nil {
		t.Fatalf("second page = %v", second)
	}
	// GCP's refusals (memorystore-redis-instance-admin).
	for query, want := range map[string]string{"pageToken=9": "The request was invalid: invalid page token", "pageSize=-1": "Invalid argument: 'page_size'"} {
		refused := h.expect(h.invoke(adminToken, http.MethodGet, testInstances+"?"+query, nil), http.StatusBadRequest)
		if message, _ := refused["error"].(map[string]any)["message"].(string); message != want {
			t.Errorf("%s: %v", query, refused)
		}
	}
}

// TestOperationsReportProgress covers the shared operation store: a create's
// operation is not done until the latency passes, cancel of finished work
// lets it complete, and delete forgets it.
func TestOperationsReportProgress(t *testing.T) {
	h := newHarness(t)
	h.provider.operations = provider.NewOperations(validScope, 50*time.Millisecond, operationMetadata)
	created := h.expect(h.create(adminToken, testProject, testRegion, testInstance, basic(nil)), http.StatusOK)
	name, _ := created["name"].(string)
	if created["done"] != false || created["response"] != nil || object(t, created["metadata"])["endTime"] != nil {
		t.Fatalf("a fresh operation reported done: %v", created)
	}
	path := "/v1/" + name
	h.expect(h.invoke(viewerToken, http.MethodPost, path+":cancel", nil), http.StatusForbidden)
	h.expect(h.invoke(adminToken, http.MethodPost, path+":cancel", nil), http.StatusOK)
	time.Sleep(60 * time.Millisecond)
	done := h.expect(h.invoke(viewerToken, http.MethodGet, path, nil), http.StatusOK)
	checkOperation(t, done, "create", instanceType)
	if object(t, done["metadata"])["cancelRequested"] != true {
		t.Fatalf("cancel was not recorded: %v", done)
	}
	h.expect(h.invoke(viewerToken, http.MethodDelete, path, nil), http.StatusForbidden)
	h.expect(h.invoke(adminToken, http.MethodDelete, path, nil), http.StatusOK)
	h.expect(h.invoke(adminToken, http.MethodGet, path, nil), http.StatusNotFound)
	h.expect(h.invoke(adminToken, http.MethodPost, path+":cancel", nil), http.StatusNotFound)
}

// TestAuthorizedNetworkMustExist covers the recorded refusal of a network the
// Compute provider doesn't hold, and the gap when Compute isn't served.
func TestAuthorizedNetworkMustExist(t *testing.T) {
	h := newHarness(t)
	network := "projects/demo-redis/global/networks/cache-net"
	h.expect(h.create(adminToken, testProject, testRegion, "no-compute", basic(map[string]any{"authorizedNetwork": network})), http.StatusNotImplemented)
	must(t, h.registry.Register("compute.networks.get", func(gateway.Call, any) (any, error) { return &gateway.RESTResponse{}, nil }))
	refused := h.expect(h.create(adminToken, testProject, testRegion, "missing-net", basic(map[string]any{"authorizedNetwork": network})), http.StatusBadRequest)
	failure := object(t, refused["error"])
	details, _ := failure["details"].([]any)
	if failure["message"] != "com.google.apps.framework.request.StatusException: <eye3 title='INVALID_ARGUMENT'/> generic::INVALID_ARGUMENT: Invalid authorized network 'cache-net'." ||
		len(details) != 1 || object(t, details[0])["fieldViolations"] == nil {
		t.Fatalf("missing network = %v", refused)
	}
	must(t, h.index.Put("compute", state.Resource{Name: "//compute.googleapis.com/projects/demo-redis/global/network/cache-net", Service: "compute", Kind: "network",
		Parent: provider.ProjectResource(testProject), Scope: provider.ProjectScope(testProject)}))
	created := h.expect(h.create(adminToken, testProject, testRegion, testInstance, basic(map[string]any{"authorizedNetwork": network})), http.StatusOK)
	if object(t, created["response"])["authorizedNetwork"] != network {
		t.Fatalf("created = %v", created)
	}
	// An omitted network is the project's default, unchecked.
	h.expect(h.create(adminToken, testProject, testRegion, "default-net", basic(nil)), http.StatusOK)
	missing := h.expect(h.invoke(adminToken, http.MethodGet, testInstances+"/absent", nil), http.StatusNotFound)
	if details, _ := object(t, missing["error"])["details"].([]any); len(details) != 1 ||
		object(t, details[0])["resourceName"] != "projects/demo-redis/locations/us-central1/instances/absent" {
		t.Fatalf("not found = %v", missing)
	}
}

// TestConditionsSeeTheAddressedResource grants roles/redis.viewer through a
// project binding conditioned on resource.service and resource.name, which
// GCP evaluates for the addressed instance even when it doesn't exist, and for
// list on the location (probed). resource.type is "" on GCP and absent here,
// so a condition reading it grants nothing.
func TestConditionsSeeTheAddressedResource(t *testing.T) {
	h := newHarness(t)
	for _, id := range []string{testInstance, "cache-2"} {
		h.expect(h.create(adminToken, testProject, testRegion, id, basic(nil)), http.StatusOK)
	}
	location := "projects/demo-redis/locations/us-central1"
	current, err := h.engine.GetPolicy(provider.ProjectResource(testProject))
	must(t, err)
	current.Version = 3
	current.Bindings = append(current.Bindings,
		iam.Binding{Role: "roles/redis.viewer", Members: []string{principal("reader")},
			Condition: []byte(`{"title":"named","expression":"resource.service == 'redis.googleapis.com' && resource.name in ['` +
				location + `/instances/cache-1', '` + location + `/instances/absent', '` + location + `']"}`)},
		iam.Binding{Role: "roles/redis.viewer", Members: []string{principal("typed")},
			Condition: []byte(`{"title":"typed","expression":"resource.type == 'redis.googleapis.com/Instance'"}`)})
	_, err = h.engine.SetPolicy(provider.ProjectResource(testProject), current)
	must(t, err)
	h.expect(h.invoke("reader", http.MethodGet, testInstances+"/"+testInstance, nil), http.StatusOK)
	h.expect(h.invoke("reader", http.MethodGet, testInstances+"/cache-2", nil), http.StatusForbidden)
	h.expect(h.invoke("reader", http.MethodGet, testInstances+"/absent", nil), http.StatusNotFound)
	h.expect(h.invoke("reader", http.MethodGet, testInstances, nil), http.StatusOK)
	denied := h.expect(h.invoke("typed", http.MethodGet, testInstances, nil), http.StatusForbidden)
	if object(t, denied["error"])["message"] != "Permission 'redis.instances.list' denied on '"+location+"/instances'" {
		t.Fatalf("list denial = %v", denied)
	}
	h.expect(h.invoke("typed", http.MethodGet, testInstances+"/"+testInstance, nil), http.StatusForbidden)
}

// TestCancelOfAFinishedOperationIsRefused follows the recorded refusal.
func TestCancelOfAFinishedOperationIsRefused(t *testing.T) {
	h := newHarness(t)
	created := h.expect(h.create(adminToken, testProject, testRegion, testInstance, basic(nil)), http.StatusOK)
	name, _ := created["name"].(string)
	refused := h.expect(h.invoke(adminToken, http.MethodPost, "/v1/"+name+":cancel", nil), http.StatusBadRequest)
	failure := object(t, refused["error"])
	details, _ := failure["details"].([]any)
	if failure["status"] != "FAILED_PRECONDITION" || failure["message"] != "Invalid resource state '"+name+"': operation already completed" ||
		len(details) != 1 || object(t, details[0])["@type"] != "type.googleapis.com/google.rpc.PreconditionFailure" {
		t.Fatalf("cancel = %v", refused)
	}
	h.expect(h.invoke(adminToken, http.MethodDelete, "/v1/"+name, nil), http.StatusOK)
	h.expect(h.invoke(adminToken, http.MethodGet, "/v1/"+name, nil), http.StatusNotFound)
}
