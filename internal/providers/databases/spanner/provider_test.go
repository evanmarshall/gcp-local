package spanner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/providers/provider/providertest"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Names repeated across the provider tests.
const (
	testProject   = "demo-spanner"
	peerProject   = "peer-spanner"
	testInstance  = "demo-inst"
	testDatabase  = "demo_db"
	testInstances = "/v1/projects/" + testProject + "/instances"
	testPath      = testInstances + "/" + testInstance
	adminToken    = "admin"
	peerToken     = "peer"
	viewerToken   = "viewer"
)

// harness drives the provider through the gateway with one fake emulator per scope.
type harness struct {
	t       *testing.T
	gate    *gateway.Gateway
	engine  *iam.Engine
	index   *state.Index
	manager *backend.Manager
	mu      sync.Mutex
	fakes   map[string][]*fakeDriver
}

func principal(token string) string {
	project := testProject
	if token == peerToken {
		project = peerProject
	}
	return "serviceAccount:" + token + "@" + project + ".iam.gserviceaccount.com"
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{t: t, index: state.NewIndex(), fakes: map[string][]*fakeDriver{}}
	engine, err := iam.New(h.index)
	must(t, err)
	h.engine = engine
	for project, token := range map[string]string{testProject: adminToken, peerProject: peerToken} {
		must(t, h.index.Put(ownerProjects, state.Resource{Name: provider.ProjectResource(project), Service: ownerProjects,
			Kind: kindProject, Number: "123456", Scope: provider.ProjectScope(project)}))
		_, err := engine.SetPolicy(provider.ProjectResource(project), iam.Policy{Bindings: []iam.Binding{
			{Role: "roles/spanner.admin", Members: []string{principal(token)}}}})
		must(t, err)
	}
	h.manager = backend.NewManager(time.Second, time.Second)
	registry := gateway.NewRegistry()
	_, err = newProvider(registry, h.manager, h.index, engine, func(scope state.Scope) (backend.Backend, error) {
		h.mu.Lock()
		defer h.mu.Unlock()
		fake := newFakeDriver()
		h.fakes[scope.Project] = append(h.fakes[scope.Project], fake)
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

// fake returns the current fake backend for a project, or nil when none started.
func (h *harness) fake(project string) *fakeDriver {
	h.mu.Lock()
	defer h.mu.Unlock()
	fakes := h.fakes[project]
	if len(fakes) == 0 {
		return nil
	}
	return fakes[len(fakes)-1]
}

// invoke issues one REST request as serviceAccount:<token>.
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
	request.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	h.gate.ServeHTTP(recorder, request)
	return recorder
}

// expect asserts one response status.
func (h *harness) expect(recorder *httptest.ResponseRecorder, code int) map[string]any {
	h.t.Helper()
	if recorder.Code != code {
		h.t.Fatalf("status = %d, want %d: %s", recorder.Code, code, recorder.Body.String())
	}
	var body map[string]any
	must(h.t, json.Unmarshal(recorder.Body.Bytes(), &body))
	return body
}

// instanceBody is the recorded instances.create request shape plus extra instance fields.
func instanceBody(project string, extra ...string) map[string]any {
	instance := map[string]any{
		"config": "projects/" + project + "/instanceConfigs/regional-us-central1", "displayName": "demo",
		"processingUnits": 100, "labels": map[string]string{"managed-by": "gcp-local"}}
	for i := 0; i+1 < len(extra); i += 2 {
		instance[extra[i]] = extra[i+1]
	}
	return map[string]any{"instanceId": testInstance, "instance": instance}
}

// object asserts one JSON object value.
func object(t *testing.T, value any) map[string]any {
	t.Helper()
	document, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("value %v is not a JSON object", value)
	}
	return document
}

// createDatabase is the databases.create body for one database id.
func createDatabase(id string) map[string]any {
	return map[string]any{"createStatement": "CREATE DATABASE `" + id + "`"}
}

func (h *harness) createInstance() {
	h.t.Helper()
	h.expect(h.invoke(adminToken, http.MethodPost, testInstances, instanceBody(testProject)), http.StatusOK)
}

func TestDeniedAndUnsupportedCallsNeverStartOrReachBackend(t *testing.T) {
	h := newHarness(t)
	h.expect(h.invoke(peerToken, http.MethodPost, testInstances, instanceBody(testProject)), http.StatusForbidden)
	h.expect(h.invoke(peerToken, http.MethodGet, testPath, nil), http.StatusForbidden)
	h.expect(h.invoke(peerToken, http.MethodPost, testPath+"/databases", createDatabase("x_db")), http.StatusForbidden)
	h.expect(h.invoke(peerToken, http.MethodGet, testInstances+"/x/operations/op", nil), http.StatusForbidden)
	h.expect(h.invoke(adminToken, http.MethodPost, testInstances, instanceBody(testProject, "edition", "ENTERPRISE")), http.StatusNotImplemented)
	if fake := h.fake(testProject); fake != nil {
		t.Fatalf("a denied or unsupported request started the backend: %+v", fake)
	}
}

func TestLifecycleDecoratesAndPublishesReferences(t *testing.T) {
	h := newHarness(t)
	created := h.expect(h.invoke(adminToken, http.MethodPost, testInstances, instanceBody(testProject)), http.StatusOK)
	response := object(t, created["response"])
	for _, field := range []string{"edition", "instanceType", "defaultBackupScheduleType", "resourceLocation", "replicaComputeCapacity"} {
		if response[field] == nil {
			t.Fatalf("instance response lacks %s: %v", field, response)
		}
	}
	if response["resourceLocation"] != "us-central1" {
		t.Fatalf("resourceLocation = %v", response["resourceLocation"])
	}
	if _, found := h.index.Get(fullName(provider.InstanceName(testProject, testInstance))); !found {
		t.Fatal("instance reference was not published")
	}
	expectDatabaseDecorations(h)
	// Deleting the instance drops its databases and their references, as in GCP.
	h.expect(h.invoke(adminToken, http.MethodDelete, testPath, nil), http.StatusOK)
	for _, name := range []string{provider.InstanceName(testProject, testInstance), databaseName(testProject, testInstance, testDatabase)} {
		if _, found := h.index.Get(fullName(name)); found {
			t.Fatalf("reference %s survived deletion", name)
		}
	}
	h.expect(h.invoke(adminToken, http.MethodGet, testPath, nil), http.StatusNotFound)
}

// expectDatabaseDecorations checks the recorded create/read field split.
func expectDatabaseDecorations(h *harness) {
	h.t.Helper()
	created := h.expect(h.invoke(adminToken, http.MethodPost, testPath+"/databases", createDatabase(testDatabase)), http.StatusOK)
	database := object(h.t, created["response"])
	if database["resourceLocationId"] != "us-central1" || database["createTime"] == nil || database["encryptionInfo"] != nil {
		h.t.Fatalf("database create response = %v", database)
	}
	read := h.expect(h.invoke(adminToken, http.MethodGet, testPath+"/databases/"+testDatabase, nil), http.StatusOK)
	if read["versionRetentionPeriod"] != defaultVersionRetention || read["encryptionInfo"] == nil || read["resourceLocationId"] != nil {
		h.t.Fatalf("database read = %v", read)
	}
}

func TestInstanceGrantAuthorizesOnlyThatInstance(t *testing.T) {
	h := newHarness(t)
	h.createInstance()
	policy := h.expect(h.invoke(adminToken, http.MethodPost, testPath+":getIamPolicy", map[string]any{}), http.StatusOK)
	h.expect(h.invoke(adminToken, http.MethodPost, testPath+":setIamPolicy", map[string]any{"policy": map[string]any{
		"etag": policy["etag"], "bindings": []any{map[string]any{"role": "roles/spanner.viewer", "members": []string{principal(viewerToken)}}},
	}}), http.StatusOK)
	h.expect(h.invoke(viewerToken, http.MethodGet, testPath, nil), http.StatusOK)
	_, before := h.fake(testProject).counts()
	h.expect(h.invoke(viewerToken, http.MethodPost, testPath+"/databases", createDatabase("x_db")), http.StatusForbidden)
	if _, after := h.fake(testProject).counts(); after != before {
		t.Fatal("a denied database create reached the backend")
	}
	// A policy request for a resource the index does not hold is 404, never the project policy.
	h.expect(h.invoke(adminToken, http.MethodPost, testInstances+"/absent:getIamPolicy", map[string]any{}), http.StatusNotFound)
}

func TestResetStopsBackendAndClearsScope(t *testing.T) {
	h := newHarness(t)
	h.createInstance()
	h.expect(h.invoke(adminToken, http.MethodPost, testPath+"/databases", createDatabase(testDatabase)), http.StatusOK)
	first := h.fake(testProject)
	must(t, h.manager.Reset(context.Background(), backend.Key{Service: serviceName, Scope: provider.ProjectScope(testProject)}))
	if first.stops == 0 {
		t.Fatal("reset did not stop the emulator")
	}
	if _, found := h.index.Get(fullName(provider.InstanceName(testProject, testInstance))); found {
		t.Fatal("reset left the instance reference")
	}
	// The next call starts a fresh emulator and sees no state.
	h.expect(h.invoke(adminToken, http.MethodGet, testPath, nil), http.StatusNotFound)
	if starts, _ := first.counts(); starts != 2 {
		t.Fatalf("emulator starts after reset = %d, want a restart", starts)
	}
	h.createInstance()
	h.expect(h.invoke(adminToken, http.MethodPost, testPath+":getIamPolicy", map[string]any{}), http.StatusOK)
}

func TestProjectsUseIsolatedBackends(t *testing.T) {
	h := newHarness(t)
	h.createInstance()
	peerInstances := "/v1/projects/" + peerProject + "/instances"
	h.expect(h.invoke(peerToken, http.MethodGet, peerInstances+"/"+testInstance, nil), http.StatusNotFound)
	h.expect(h.invoke(peerToken, http.MethodPost, peerInstances, instanceBody(peerProject)), http.StatusOK)
	if h.fake(testProject) == h.fake(peerProject) {
		t.Fatal("projects share one emulator")
	}
	must(t, h.manager.Reset(context.Background(), backend.Key{Service: serviceName, Scope: provider.ProjectScope(peerProject)}))
	h.expect(h.invoke(adminToken, http.MethodGet, testPath, nil), http.StatusOK)
	if _, found := h.index.Get(fullName(provider.InstanceName(testProject, testInstance))); !found {
		t.Fatal("resetting one project removed another project's reference")
	}
}

func TestPathBindingOverridesBodyNames(t *testing.T) {
	h := newHarness(t)
	body := instanceBody(testProject, "name", "projects/"+peerProject+"/instances/elsewhere")
	h.expect(h.invoke(adminToken, http.MethodPost, testInstances, body), http.StatusOK)
	fake := h.fake(testProject)
	request, ok := fake.invoked[len(fake.invoked)-1].(*instancepb.CreateInstanceRequest)
	if !ok || request.GetInstance().GetName() != provider.InstanceName(testProject, testInstance) || request.GetParent() != "projects/"+testProject {
		t.Fatalf("forwarded request = %v", request)
	}
}

func TestUncertainCreateIsReconciledOrBlocksScope(t *testing.T) {
	h := newHarness(t)
	h.expect(h.invoke(adminToken, http.MethodGet, testInstances+"/absent", nil), http.StatusNotFound) // start the fake
	fake := h.fake(testProject)
	fake.fault = func(method string) error {
		if method == instanceAdmin+"CreateInstance" {
			fake.instances[provider.InstanceName(testProject, testInstance)] = &instancepb.Instance{Name: provider.InstanceName(testProject, testInstance)}
			return status.Error(codes.Unavailable, "connection reset")
		}
		return nil
	}
	h.expect(h.invoke(adminToken, http.MethodPost, testInstances, instanceBody(testProject)), http.StatusServiceUnavailable)
	if _, found := h.index.Get(fullName(provider.InstanceName(testProject, testInstance))); !found {
		t.Fatal("an instance the backend holds was not reconciled into the index")
	}
	fake.fault = func(string) error { return status.Error(codes.Unavailable, "backend lost") }
	body := instanceBody(testProject)
	body["instanceId"] = "other-inst"
	h.expect(h.invoke(adminToken, http.MethodPost, testInstances, body), http.StatusServiceUnavailable)
	h.expect(h.invoke(adminToken, http.MethodGet, testPath, nil), http.StatusBadRequest)
	fake.fault = nil
	must(t, h.manager.Reset(context.Background(), backend.Key{Service: serviceName, Scope: provider.ProjectScope(testProject)}))
	h.expect(h.invoke(adminToken, http.MethodGet, testPath, nil), http.StatusNotFound)
}

func TestEveryRouteIsHandled(t *testing.T) {
	h := newHarness(t)
	h.createInstance()
	for _, probe := range []struct{ verb, path string }{
		{http.MethodGet, "/v1/projects/" + testProject + "/instanceConfigs/regional-us-central1"},
		{http.MethodGet, "/v1/projects/" + testProject + "/instanceConfigs"},
		{http.MethodGet, testPath},
		{http.MethodGet, testInstances},
		{http.MethodGet, testPath + "/operations/_auto0"},
		{http.MethodGet, testPath + "/backups"},
		{http.MethodGet, testPath + "/databases"},
		{http.MethodGet, testPath + "/databases/" + testDatabase},
		{http.MethodGet, testPath + "/databases/" + testDatabase + "/ddl"},
		{http.MethodGet, testPath + "/databases/" + testDatabase + "/operations/_auto0"},
	} {
		recorder := h.invoke(adminToken, probe.verb, probe.path, nil)
		if recorder.Code == http.StatusNotImplemented && bytes.Contains(recorder.Body.Bytes(), []byte("this method is not implemented")) {
			t.Fatalf("%s %s hit the dispatch gap", probe.verb, probe.path)
		}
	}
	h.expect(h.invoke(adminToken, http.MethodGet, testPath+"/backups", nil), http.StatusOK)
	h.expect(h.invoke(adminToken, http.MethodGet, testInstances+"/absent/backups", nil), http.StatusNotFound)
}

func TestConfigRequiresPinnedLinuxEmulator(t *testing.T) {
	err := Config{Emulator: "relative/emulator_main", Libraries: "lib"}.validate()
	want := errBackendPaths
	if runtime.GOOS != "linux" || (runtime.GOARCH != "amd64" && runtime.GOARCH != "arm64") {
		want = errPlatform
	}
	if !errors.Is(err, want) {
		t.Fatalf("validate = %v, want %v", err, want)
	}
	if runtime.GOOS == "linux" {
		executable, err := os.Executable()
		must(t, err)
		if err := (Config{Emulator: executable, Libraries: t.TempDir()}).validate(); !errors.Is(err, errPinMismatch) {
			t.Fatalf("an unpinned binary validated: %v", err)
		}
	}
}

// TestConcurrentAdminRequests races instance and database create, get, and
// delete on one instance; -race fails it if reference publication or the
// created-database store is touched outside its lock.
func TestConcurrentAdminRequests(t *testing.T) {
	h := newHarness(t)
	database := testPath + "/databases/" + testDatabase
	providertest.Hammer(t, h.gate, adminToken, 8, 20, func(int) []providertest.Request {
		return []providertest.Request{
			{Method: http.MethodPost, Path: testInstances, Body: instanceBody(testProject), Want: []int{200, 409}},
			{Method: http.MethodGet, Path: testPath, Want: []int{200, 404}},
			{Method: http.MethodPost, Path: testPath + "/databases", Body: createDatabase(testDatabase), Want: []int{200, 404, 409}},
			{Method: http.MethodGet, Path: database, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: testPath + "/databases", Want: []int{200, 404}},
			{Method: http.MethodDelete, Path: database, Want: []int{200, 404}},
			{Method: http.MethodDelete, Path: testPath, Want: []int{200, 404}},
		}
	})
}

// TestDatabaseRequestsRacingDelete races database reads, policy reads, and
// deletes against recreation. Authorization picks the database, instance, or
// project under the scope lock that unindexing holds, so a request sees 200,
// 404, or 409, never 501 for a target unindexed mid-check (CI flake on
// DELETE .../databases/demo_db).
func TestDatabaseRequestsRacingDelete(t *testing.T) {
	h := newHarness(t)
	h.createInstance()
	database := testPath + "/databases/" + testDatabase
	providertest.Hammer(t, h.gate, adminToken, 16, 40, func(worker int) []providertest.Request {
		if worker%4 == 0 {
			return []providertest.Request{
				{Method: http.MethodPost, Path: testPath + "/databases", Body: createDatabase(testDatabase), Want: []int{200, 409}},
				{Method: http.MethodDelete, Path: database, Want: []int{200, 404}},
			}
		}
		return []providertest.Request{
			{Method: http.MethodDelete, Path: database, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: database, Want: []int{200, 404}},
			{Method: http.MethodPost, Path: database + ":getIamPolicy", Body: map[string]any{}, Want: []int{200, 404}},
		}
	})
}

// errorBody decodes a REST error.
func errorBody(t *testing.T, body map[string]any) (string, []any) {
	t.Helper()
	failure := object(t, body["error"])
	message, _ := failure["message"].(string)
	details, _ := failure["details"].([]any)
	return message, details
}

func TestDenialMatchesTheRecording(t *testing.T) {
	h := newHarness(t)
	h.createInstance()
	message, details := errorBody(t, h.expect(h.invoke(peerToken, http.MethodGet, testPath, nil), http.StatusForbidden))
	if message != "Operation denied by [IAM permission 'spanner.instances.get' on resource '//spanner.googleapis.com/projects/demo-spanner/instances/demo-inst']." {
		t.Fatalf("message = %q", message)
	}
	info := object(t, details[0])
	if info["reason"] != "IAM_PERMISSION_DENIED" || info["domain"] != "iam.googleapis.com" ||
		object(t, info["metadata"])["resource"] != "projects/demo-spanner/instances/demo-inst" {
		t.Fatalf("details = %v", details)
	}
}

func TestBodiesAreDecodedStrictly(t *testing.T) {
	h := newHarness(t)
	h.createInstance()
	body := createDatabase("x_db")
	body["unknownField"] = 1
	message, _ := errorBody(t, h.expect(h.invoke(adminToken, http.MethodPost, testPath+"/databases", body), http.StatusBadRequest))
	if message != `Invalid JSON payload received. Unknown name "unknownField": Cannot find field.` {
		t.Fatalf("unknown field: %q", message)
	}
	// The emulator accepts a CMEK key and ignores it.
	cmek := createDatabase("x_db")
	cmek["encryptionConfig"] = map[string]any{"kmsKeyName": "projects/p/locations/l/keyRings/r/cryptoKeys/k"}
	message, _ = errorBody(t, h.expect(h.invoke(adminToken, http.MethodPost, testPath+"/databases", cmek), http.StatusNotImplemented))
	if message != "Spanner coverage gap: encryptionConfig is not modeled" {
		t.Fatalf("CMEK: %q", message)
	}
	// A policy holds only version, bindings, and etag; deny rules are not Spanner's.
	h.expect(h.invoke(adminToken, http.MethodPost, testPath+":setIamPolicy", map[string]any{
		"policy": map[string]any{"denyRules": []any{}}}), http.StatusBadRequest)
	message, _ = errorBody(t, h.expect(h.invoke(adminToken, http.MethodPost, testPath+":setIamPolicy", map[string]any{
		"policy": map[string]any{}, "updateMask": "bindings"}), http.StatusNotImplemented))
	if message != "Spanner coverage gap: updateMask is not modeled" {
		t.Fatalf("update mask: %q", message)
	}
}

func TestListsPageInNameOrder(t *testing.T) {
	h := newHarness(t)
	h.createInstance()
	for _, id := range []string{"gl_db", "gl_ab", "gl_mm"} {
		h.expect(h.invoke(adminToken, http.MethodPost, testPath+"/databases", createDatabase(id)), http.StatusOK)
	}
	names := func(body map[string]any) []string {
		items := provider.MapList(body["databases"])
		out := make([]string, 0, len(items))
		for _, item := range items {
			out = append(out, provider.LastSegment(provider.Text(item["name"])))
		}
		return out
	}
	all := h.expect(h.invoke(adminToken, http.MethodGet, testPath+"/databases", nil), http.StatusOK)
	if got := strings.Join(names(all), ","); got != "gl_ab,gl_db,gl_mm" {
		t.Fatalf("order = %v", got)
	}
	first := h.expect(h.invoke(adminToken, http.MethodGet, testPath+"/databases?pageSize=2", nil), http.StatusOK)
	second := h.expect(h.invoke(adminToken, http.MethodGet, testPath+"/databases?pageSize=2&pageToken="+provider.Text(first["nextPageToken"]), nil), http.StatusOK)
	if got := len(names(first)) + 10*len(names(second)); got != 12 || first["nextPageToken"] == nil || second["nextPageToken"] != nil {
		t.Fatalf("pages = %v %v", first, second)
	}
	if got := names(h.expect(h.invoke(adminToken, http.MethodGet, testPath+"/databases?pageSize=-1", nil), http.StatusOK)); len(got) != 3 {
		t.Fatalf("a negative size is the maximum (recorded): %v", got)
	}
}

// TestListRefusals checks the recorded page-token refusal and the filter gap.
func TestListRefusals(t *testing.T) {
	h := newHarness(t)
	h.createInstance()
	message, details := errorBody(t, h.expect(h.invoke(adminToken, http.MethodGet, testPath+"/databases?pageToken=bad", nil), http.StatusBadRequest))
	if message != "Invalid ListDatabases request" || len(details) != 1 {
		t.Fatalf("token: %q %v", message, details)
	}
	message, _ = errorBody(t, h.expect(h.invoke(adminToken, http.MethodGet, testInstances+"?filter=name:x", nil), http.StatusNotImplemented))
	if message != "Spanner coverage gap: list filter is not implemented" {
		t.Fatalf("filter: %q", message)
	}
}

func TestBackupsAreAnExplicitGap(t *testing.T) {
	h := newHarness(t)
	h.createInstance()
	// Recorded: an instance without backups lists {}.
	if body := h.expect(h.invoke(adminToken, http.MethodGet, testPath+"/backups", nil), http.StatusOK); len(body) != 0 {
		t.Fatalf("backups = %v", body)
	}
	message, _ := errorBody(t, h.expect(h.invoke(adminToken, http.MethodGet, testPath+"/backups?filter=name:x", nil), http.StatusNotImplemented))
	if message != "Spanner coverage gap: list filter is not implemented" {
		t.Fatalf("backup filter: %q", message)
	}
	for _, probe := range []struct{ verb, path string }{
		{http.MethodPost, testPath + "/backups?backupId=b"},
		{http.MethodGet, testPath + "/backups/b"},
		{http.MethodDelete, testPath + "/backups/b"},
		{http.MethodPost, testPath + "/databases:restore"},
		{http.MethodGet, testPath + "/operations"},
		{http.MethodPost, testPath + "/operations/op:cancel"},
	} {
		message, _ := errorBody(t, h.expect(h.invoke(adminToken, probe.verb, probe.path, map[string]any{}), http.StatusNotImplemented))
		if !strings.HasPrefix(message, "Spanner coverage gap: ") {
			t.Fatalf("%s %s: %q", probe.verb, probe.path, message)
		}
	}
}

// TestConditionalGrantOnTheInstance replays the recorded conditional grant: a
// databaseAdmin binding on the instance conditioned on the database's
// resource.type, .name, and .service allows only that database.
func TestConditionalGrantOnTheInstance(t *testing.T) {
	h := newHarness(t)
	h.createInstance()
	for _, id := range []string{"gl_db", "gl_ab"} {
		h.expect(h.invoke(adminToken, http.MethodPost, testPath+"/databases", createDatabase(id)), http.StatusOK)
	}
	expression := `resource.type == "spanner.googleapis.com/Database" && resource.name == "projects/demo-spanner/instances/demo-inst/databases/gl_db" && resource.service == "spanner.googleapis.com"`
	h.expect(h.invoke(adminToken, http.MethodPost, testPath+":setIamPolicy", map[string]any{"policy": map[string]any{
		"version": 3, "bindings": []any{map[string]any{"role": "roles/spanner.databaseAdmin", "members": []string{principal(peerToken)},
			"condition": map[string]any{"title": "t", "expression": expression}}}}}), http.StatusOK)
	h.expect(h.invoke(peerToken, http.MethodGet, testPath+"/databases/gl_db", nil), http.StatusOK)
	h.expect(h.invoke(peerToken, http.MethodGet, testPath+"/databases/gl_ab", nil), http.StatusForbidden)
	h.expect(h.invoke(peerToken, http.MethodGet, testPath, nil), http.StatusForbidden)
}
