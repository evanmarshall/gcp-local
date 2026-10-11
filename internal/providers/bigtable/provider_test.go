package bigtable

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// Names and keys repeated across the lifecycle tests.
const (
	testProject   = "demo-bigtable"
	testInstance  = "demo-bt"
	testCluster   = "demo-btc1"
	testTable     = "demo-table"
	testZone      = "us-central1-b"
	keyTableID    = "tableId"
	keyTable      = "table"
	keyFamilies   = "columnFamilies"
	familyOne     = "cf1"
	familyTwo     = "cf2"
	keyGCRule     = "gcRule"
	keyMods       = "modifications"
	testToken     = "actor"
	testInstances = "/v2/projects/" + testProject + "/instances"
	testActor     = "serviceAccount:actor@" + testProject + ".iam.gserviceaccount.com"
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
		Bindings: []iam.Binding{{Role: "roles/bigtable.admin", Members: []string{testActor}}},
	}); err != nil {
		t.Fatalf("grant the project: %v", err)
	}
	_, err = New(context.Background(), registry, backend.NewManager(time.Second, time.Second), index, engine)
	if err != nil {
		t.Fatalf("build the bigtable provider: %v", err)
	}
	g := gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:" + token + "@" + testProject + ".iam.gserviceaccount.com", Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})
	return &harness{t: t, gate: g, engine: engine}
}

// invoke issues one REST request as the fixture principal.
func (h *harness) invoke(verb, path string, body any) *httptest.ResponseRecorder {
	h.t.Helper()
	return h.invokeAs(testToken, verb, path, body)
}

// invokeAs issues one REST request as the principal named by token.
func (h *harness) invokeAs(token, verb, path string, body any) *httptest.ResponseRecorder {
	h.t.Helper()
	var encoded []byte
	if body != nil {
		var err error
		if encoded, err = json.Marshal(body); err != nil {
			h.t.Fatalf("encode the request: %v", err)
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

// createBody is the recorded CreateInstance request shape.
func createBody() map[string]any {
	return map[string]any{
		"parent":     "projects/" + testProject,
		"instanceId": testInstance,
		"instance":   map[string]any{"displayName": "demo", "type": "PRODUCTION", "labels": map[string]string{"env": "dev"}},
		"clusters": map[string]any{testCluster: map[string]any{
			"location": "projects/" + testProject + "/locations/" + testZone, "serveNodes": 1, "defaultStorageType": "HDD",
		}},
	}
}

// instance creates the fixture instance and returns the operation.
func (h *harness) instance() map[string]any {
	h.t.Helper()
	recorder := h.invoke(http.MethodPost, testInstances, createBody())
	if recorder.Code != 200 {
		h.t.Fatalf("instance create status = %d body %s", recorder.Code, recorder.Body.String())
	}
	return decode(h.t, recorder)
}

// decode parses one JSON response body.
func decode(t *testing.T, recorder *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var value map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &value); err != nil {
		t.Fatalf("decode %q: %v", recorder.Body.String(), err)
	}
	return value
}

// fields returns the sorted top-level field names, as the recording compares them.
func fields(value map[string]any) string {
	names := make([]string, 0, len(value))
	for name := range value {
		names = append(names, name)
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}

// expectError asserts the recorded error triple.
func expectError(t *testing.T, label string, recorder *httptest.ResponseRecorder, code int, statusText string) {
	t.Helper()
	body := decode(t, recorder)
	wire, _ := body["error"].(map[string]any)
	if recorder.Code != code || wire["code"] != float64(code) || wire["status"] != statusText {
		t.Fatalf("%s = %d %s, want %d %s", label, recorder.Code, recorder.Body.String(), code, statusText)
	}
}

// expectEmpty asserts the recorded 200 {} delete body.
func expectEmpty(t *testing.T, label string, recorder *httptest.ResponseRecorder) {
	t.Helper()
	if recorder.Code != 200 || len(decode(t, recorder)) != 0 {
		t.Fatalf("%s = %d %s, want 200 {}", label, recorder.Code, recorder.Body.String())
	}
}

// expectOK asserts a 200 response.
func expectOK(t *testing.T, label string, recorder *httptest.ResponseRecorder) {
	t.Helper()
	if recorder.Code != 200 {
		t.Fatalf("%s status = %d body %s", label, recorder.Code, recorder.Body.String())
	}
}

func TestInstanceAndTableLifecycle(t *testing.T) {
	h := newHarness(t)
	h.instance()
	instance := testInstances + "/" + testInstance
	expectOK(t, "instance get", h.invoke(http.MethodGet, instance, nil))
	expectOK(t, "instance list", h.invoke(http.MethodGet, testInstances, nil))
	expectOK(t, "cluster list", h.invoke(http.MethodGet, instance+"/clusters", nil))
	expectOK(t, "cluster get", h.invoke(http.MethodGet, instance+"/clusters/"+testCluster, nil))
	expectOK(t, "table create", h.invoke(http.MethodPost, instance+"/tables", map[string]any{
		keyTableID: testTable,
		keyTable: map[string]any{keyFamilies: map[string]any{
			familyOne: map[string]any{keyGCRule: map[string]any{"maxAge": "2160s"}},
		}},
	}))
	expectOK(t, "table get", h.invoke(http.MethodGet, instance+"/tables/"+testTable, nil))
	expectOK(t, "table list", h.invoke(http.MethodGet, instance+"/tables", nil))
	modify := instance + "/tables/" + testTable + ":modifyColumnFamilies"
	expectOK(t, "family create", h.invoke(http.MethodPost, modify, map[string]any{
		keyMods: []any{map[string]any{"id": familyTwo, "create": map[string]any{keyGCRule: map[string]any{"maxVersion": "3"}}}},
	}))
	expectOK(t, "family drop", h.invoke(http.MethodPost, modify, map[string]any{
		keyMods: []any{map[string]any{"id": familyOne, "drop": true}},
	}))
	families := decode(t, h.invoke(http.MethodGet, instance+"/tables/"+testTable, nil))[keyFamilies]
	want := map[string]any{familyTwo: map[string]any{keyGCRule: map[string]any{"maxVersion": "3"}}}
	if !reflect.DeepEqual(families, want) {
		t.Fatalf("families after create/drop = %v, want %v", families, want)
	}

	// Deleting an instance removes its tables, as GCP does.
	expectEmpty(t, "instance delete", h.invoke(http.MethodDelete, testInstances+"/"+testInstance, nil))
	expectError(t, "post-destroy instance get", h.invoke(http.MethodGet, testInstances+"/"+testInstance, nil), 404, "NOT_FOUND")
	h.instance()
	expectError(t, "cascaded table get", h.invoke(http.MethodGet, testInstances+"/"+testInstance+"/tables/"+testTable, nil), 404, "NOT_FOUND")
}

// expectOperation asserts the recorded completed CreateInstance operation.
func expectOperation(t *testing.T, operation map[string]any) {
	t.Helper()
	if got := fields(operation); got != "done,metadata,name,response" || operation["done"] != true {
		t.Fatalf("operation = %v", operation)
	}
	wantName := "operations/projects/" + testProject + "/instances/" + testInstance + "/locations/" + testZone + "/operations/"
	if name, _ := operation["name"].(string); !strings.HasPrefix(name, wantName) {
		t.Fatalf("operation name = %v", operation["name"])
	}
	metadata, _ := operation["metadata"].(map[string]any)
	response, _ := operation["response"].(map[string]any)
	if metadata["@type"] != typeCreateInstanceMetadata || response["@type"] != typeInstance {
		t.Fatalf("operation types = %v / %v", metadata["@type"], response["@type"])
	}
	if got := fields(response); got != "@type,displayName,edition,labels,name,state,type" {
		t.Fatalf("operation response fields = %s", got)
	}
}

// TestRecordedInstanceShapes follows bigtable-instance-table-admin: the
// create operation, instance read, and cluster listing.
func TestRecordedInstanceShapes(t *testing.T) {
	h := newHarness(t)
	expectOperation(t, h.instance())
	instance := decode(t, h.invoke(http.MethodGet, testInstances+"/"+testInstance, nil))
	if got := fields(instance); got != "createTime,displayName,edition,knowledgeCatalogRegion,labels,name,state,type" {
		t.Fatalf("instance fields = %s", got)
	}
	if instance["state"] != "READY" || instance["type"] != "PRODUCTION" || instance["name"] != "projects/"+testProject+"/instances/"+testInstance {
		t.Fatalf("instance = %v", instance)
	}

	listing := decode(t, h.invoke(http.MethodGet, testInstances+"/"+testInstance+"/clusters", nil))
	clusters, _ := listing["clusters"].([]any)
	if len(clusters) != 1 {
		t.Fatalf("clusters = %v", listing)
	}
	cluster, _ := clusters[0].(map[string]any)
	want := map[string]any{
		"name":               "projects/" + testProject + "/instances/" + testInstance + "/clusters/" + testCluster,
		"location":           "projects/" + testProject + "/locations/" + testZone,
		"serveNodes":         float64(1),
		"defaultStorageType": "HDD",
		"state":              "READY",
	}
	if !reflect.DeepEqual(cluster, want) {
		t.Fatalf("cluster = %v, want %v", cluster, want)
	}
	expectError(t, "duplicate instance", h.invoke(http.MethodPost, testInstances, createBody()), 409, "ALREADY_EXISTS")
}

// TestRecordedInstanceIam follows the recorded denial and instance-scoped grant.
func TestRecordedInstanceIam(t *testing.T) {
	h := newHarness(t)
	h.instance()
	path := testInstances + "/" + testInstance
	expectError(t, "denied read", h.invokeAs("denied", http.MethodGet, path, nil), 403, "PERMISSION_DENIED")

	recorder := h.invoke(http.MethodPost, path+":getIamPolicy", map[string]any{})
	policy := decode(t, recorder)
	if recorder.Code != 200 || fields(policy) != "etag" {
		t.Fatalf("empty policy = %d %s", recorder.Code, recorder.Body.String())
	}
	allowed := "serviceAccount:allowed@" + testProject + ".iam.gserviceaccount.com"
	recorder = h.invoke(http.MethodPost, path+":setIamPolicy", map[string]any{"policy": map[string]any{
		"etag": policy["etag"], "bindings": []any{map[string]any{"role": "roles/bigtable.viewer", "members": []string{allowed}}},
	}})
	if recorder.Code != 200 || fields(decode(t, recorder)) != "bindings,etag,version" {
		t.Fatalf("set policy = %d %s", recorder.Code, recorder.Body.String())
	}
	if recorder = h.invokeAs("allowed", http.MethodGet, path, nil); recorder.Code != 200 {
		t.Fatalf("viewer read = %d %s", recorder.Code, recorder.Body.String())
	}
	expectError(t, "viewer table create", h.invokeAs("allowed", http.MethodPost, path+"/tables",
		map[string]any{keyTableID: "denied", keyTable: map[string]any{}}), 403, "PERMISSION_DENIED")

	// A recreated instance does not inherit the deleted instance's bindings.
	expectEmpty(t, "instance delete", h.invoke(http.MethodDelete, path, nil))
	h.instance()
	expectError(t, "recreated viewer read", h.invokeAs("allowed", http.MethodGet, path, nil), 403, "PERMISSION_DENIED")
}

// TestRecordedTableShapes follows the recorded table and column-family flow.
func TestRecordedTableShapes(t *testing.T) {
	h := newHarness(t)
	h.instance()
	tables := testInstances + "/" + testInstance + "/tables"
	name := "projects/" + testProject + "/instances/" + testInstance + "/tables/t1"
	expectTable := func(label string, recorder *httptest.ResponseRecorder, families ...string) {
		t.Helper()
		table := decode(t, recorder)
		wantFamilies := map[string]any{}
		for _, family := range families {
			wantFamilies[family] = map[string]any{}
		}
		want := map[string]any{"name": name, "granularity": "MILLIS", keyFamilies: wantFamilies}
		if recorder.Code != 200 || !reflect.DeepEqual(table, want) {
			t.Fatalf("%s = %d %v, want %v", label, recorder.Code, table, want)
		}
	}
	expectTable("table create", h.invoke(http.MethodPost, tables, map[string]any{
		keyTableID: "t1", keyTable: map[string]any{keyFamilies: map[string]any{familyOne: map[string]any{}}},
	}), familyOne)
	expectTable("family modify", h.invoke(http.MethodPost, tables+"/t1:modifyColumnFamilies", map[string]any{
		keyMods: []any{map[string]any{"id": familyTwo, "create": map[string]any{}}},
	}), familyOne, familyTwo)
	expectTable("table read", h.invoke(http.MethodGet, tables+"/t1", nil), familyOne, familyTwo)
	expectError(t, "duplicate table", h.invoke(http.MethodPost, tables, map[string]any{keyTableID: "t1", keyTable: map[string]any{}}), 409, "ALREADY_EXISTS")
	expectEmpty(t, "table delete", h.invoke(http.MethodDelete, tables+"/t1", nil))
	expectError(t, "table absent", h.invoke(http.MethodGet, tables+"/t1", nil), 404, "NOT_FOUND")
	expectError(t, "invalid table id", h.invoke(http.MethodPost, tables, map[string]any{keyTableID: "-bad", keyTable: map[string]any{}}), 400, "INVALID_ARGUMENT")
}

func TestUnknownAndDeniedPaths(t *testing.T) {
	h := newHarness(t)
	recorder := h.invoke(http.MethodGet, testInstances+"/missing", nil)
	if recorder.Code != 404 {
		t.Fatalf("unknown instance get status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodGet, testInstances+"/bad_upper", nil)
	if recorder.Code != 400 {
		t.Fatalf("invalid instance id status = %d", recorder.Code)
	}

	// A viewer may read but not create.
	if _, err := h.engine.SetPolicy("//cloudresourcemanager.googleapis.com/projects/"+testProject, iam.Policy{
		Bindings: []iam.Binding{{Role: "roles/bigtable.viewer", Members: []string{testActor}}},
	}); err != nil {
		t.Fatalf("switch the role: %v", err)
	}
	recorder = h.invoke(http.MethodPost, testInstances, createBody())
	if recorder.Code != 403 {
		t.Fatalf("viewer create status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodGet, testInstances+"/"+testInstance, nil)
	if recorder.Code != 404 {
		t.Fatalf("viewer get status = %d", recorder.Code)
	}
}

func TestEveryRouteIsHandled(t *testing.T) {
	h := newHarness(t)
	h.instance()
	gap := "this method is not implemented"
	probe := func(verb, path string) {
		recorder := h.invoke(verb, path, nil)
		if recorder.Code == 501 && bytes.Contains(recorder.Body.Bytes(), []byte(gap)) {
			t.Fatalf("%s %s hit the dispatch gap", verb, path)
		}
	}
	probe(http.MethodPost, testInstances)
	probe(http.MethodGet, testInstances+"/"+testInstance)
	probe(http.MethodGet, testInstances)
	probe(http.MethodGet, testInstances+"/"+testInstance+"/clusters")
	probe(http.MethodGet, testInstances+"/"+testInstance+"/clusters/"+testCluster)
	probe(http.MethodPost, testInstances+"/"+testInstance+"/tables")
	probe(http.MethodGet, testInstances+"/"+testInstance+"/tables/"+testTable)
	probe(http.MethodGet, testInstances+"/"+testInstance+"/tables")
	probe(http.MethodPost, testInstances+"/"+testInstance+":getIamPolicy")
	probe(http.MethodPost, testInstances+"/"+testInstance+"/tables/"+testTable+":getIamPolicy")
}
