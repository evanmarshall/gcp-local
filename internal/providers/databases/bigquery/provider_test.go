package bigquery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/providers/provider/providertest"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Names and keys repeated across the lifecycle tests.
const (
	testProject  = "demo-bq"
	testDataset  = "demo_dataset"
	testTable    = "demo_table"
	testToken    = "actor"
	testDatasets = "/bigquery/v2/projects/" + testProject + "/datasets"
	testActor    = "serviceAccount:actor@" + testProject + ".iam.gserviceaccount.com"
	// testPeerProject is a second project with the same grants, for isolation.
	testPeerProject = "peer-bq"
	// testGroup is the directory group that holds the "viewer" principal.
	testGroup = "readers@example.com"
)

// harness drives the provider through the gateway.
type harness struct {
	t       *testing.T
	gate    *gateway.Gateway
	engine  *iam.Engine
	manager *backend.Manager
	index   *state.Index
	bq      *Provider
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	registry := gateway.NewRegistry()
	index := state.NewIndex()
	engine, err := iam.New(index)
	if err != nil {
		t.Fatalf("build the iam engine: %v", err)
	}
	// The local directory: one group holding the "viewer" principal.
	if err := engine.SetDirectory(iam.Directory{Domains: []string{"example.com"}, Groups: map[string][]string{
		testGroup: {"serviceAccount:viewer@" + testProject + ".iam.gserviceaccount.com"},
	}}); err != nil {
		t.Fatalf("declare the directory: %v", err)
	}
	for number, project := range []string{testProject, testPeerProject} {
		if err := index.Put("resourcemanager", state.Resource{
			Name: "//cloudresourcemanager.googleapis.com/projects/" + project, Service: "resourcemanager",
			Kind: "project", Number: strconv.Itoa(123456 + number), Scope: state.Scope{Project: project, Location: state.Global},
		}); err != nil {
			t.Fatalf("register the project: %v", err)
		}
		if _, err := engine.SetPolicy("//cloudresourcemanager.googleapis.com/projects/"+project, iam.Policy{
			// Real dataEditor cannot delete datasets or run jobs; the actor holds
			// the recorded owner and job-user roles that do.
			Bindings: []iam.Binding{
				{Role: "roles/bigquery.dataOwner", Members: []string{testActor}},
				{Role: "roles/bigquery.jobUser", Members: []string{testActor}},
				{Role: "roles/bigquery.resourceViewer", Members: []string{testActor}},
			},
		}); err != nil {
			t.Fatalf("grant the project: %v", err)
		}
	}
	manager := backend.NewManager(10*time.Second, 10*time.Second)
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
	bq, err := New(context.Background(), registry, manager, index, engine)
	if err != nil {
		t.Fatalf("build the bigquery provider: %v", err)
	}
	g := gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:" + token + "@" + testProject + ".iam.gserviceaccount.com", Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})
	return &harness{t: t, gate: g, engine: engine, manager: manager, index: index, bq: bq}
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

// dataset creates the fixture dataset.
func (h *harness) dataset() {
	h.t.Helper()
	recorder := h.invoke(http.MethodPost, testDatasets, map[string]any{"datasetReference": map[string]any{"datasetId": testDataset}})
	if recorder.Code != 200 {
		h.t.Fatalf("dataset create status = %d body %s", recorder.Code, recorder.Body.String())
	}
}

// table creates the fixture table inside the fixture dataset.
func (h *harness) table() {
	h.t.Helper()
	h.dataset()
	recorder := h.invoke(http.MethodPost, testDatasets+"/"+testDataset+"/tables", map[string]any{
		"tableReference": map[string]any{"datasetId": testDataset, "tableId": testTable},
		"schema":         map[string]any{"fields": []any{map[string]any{"name": "title", "type": "STRING"}}},
	})
	if recorder.Code != 200 {
		h.t.Fatalf("table create status = %d body %s", recorder.Code, recorder.Body.String())
	}
}

func TestDatasetAndTableLifecycle(t *testing.T) {
	h := newHarness(t)
	h.table()
	recorder := h.invoke(http.MethodGet, testDatasets+"/"+testDataset, nil)
	if recorder.Code != 200 {
		t.Fatalf("dataset get status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodGet, testDatasets+"/"+testDataset+"/tables/"+testTable, nil)
	if recorder.Code != 200 {
		t.Fatalf("table get status = %d", recorder.Code)
	}
	// Duplicate dataset is refused.
	recorder = h.invoke(http.MethodPost, testDatasets, map[string]any{"datasetReference": map[string]any{"datasetId": testDataset}})
	if recorder.Code != 409 {
		t.Fatalf("duplicate dataset status = %d", recorder.Code)
	}
	// Deleting a non-empty dataset is refused.
	recorder = h.invoke(http.MethodDelete, testDatasets+"/"+testDataset, nil)
	if recorder.Code != 400 {
		t.Fatalf("non-empty dataset delete status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodDelete, testDatasets+"/"+testDataset+"/tables/"+testTable, nil)
	if recorder.Code != 204 {
		t.Fatalf("table delete status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodDelete, testDatasets+"/"+testDataset, nil)
	if recorder.Code != 204 {
		t.Fatalf("dataset delete status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodGet, testDatasets+"/"+testDataset, nil)
	if recorder.Code != 404 {
		t.Fatalf("get after delete status = %d", recorder.Code)
	}
}

func TestRowsInsertAndList(t *testing.T) {
	h := newHarness(t)
	h.table()
	recorder := h.invoke(http.MethodPost, testDatasets+"/"+testDataset+"/tables/"+testTable+"/insertAll", map[string]any{
		"rows": []any{map[string]any{"json": map[string]any{"title": "fire-and-fog"}}},
	})
	if recorder.Code != 200 {
		t.Fatalf("insertAll status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodGet, testDatasets+"/"+testDataset+"/tables/"+testTable+"/data", nil)
	if recorder.Code != 200 {
		t.Fatalf("tabledata list status = %d", recorder.Code)
	}
	var data struct {
		TotalRows string `json:"totalRows"`
		Rows      []struct {
			F []map[string]any `json:"f"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &data); err != nil {
		t.Fatalf("decode rows: %v", err)
	}
	if data.TotalRows != "1" {
		t.Fatalf("totalRows = %s", data.TotalRows)
	}
}

func TestTableRequiresSchema(t *testing.T) {
	h := newHarness(t)
	h.dataset()
	recorder := h.invoke(http.MethodPost, testDatasets+"/"+testDataset+"/tables", map[string]any{
		"tableReference": map[string]any{"datasetId": testDataset, "tableId": "no-schema"},
	})
	if recorder.Code != 400 {
		t.Fatalf("missing schema status = %d", recorder.Code)
	}
}

func TestViewerCannotInsert(t *testing.T) {
	h := newHarness(t)
	h.table()
	if err := h.engineSet(iam.Policy{Version: 3, Bindings: []iam.Binding{
		{Role: "roles/bigquery.dataViewer", Members: []string{"serviceAccount:viewer@" + testProject + ".iam.gserviceaccount.com"}},
	}}); err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequestWithContext(context.Background(), http.MethodPost,
		testDatasets+"/"+testDataset+"/tables/"+testTable+"/insertAll",
		bytes.NewReader([]byte(`{"rows":[{"json":{"title":"x"}}]}`)))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer viewer")
	recorder := httptest.NewRecorder()
	h.gate.ServeHTTP(recorder, request)
	if recorder.Code != 403 {
		t.Fatalf("viewer insert status = %d body %s", recorder.Code, recorder.Body.String())
	}
}

func TestEveryRouteIsHandled(t *testing.T) {
	h := newHarness(t)
	h.table()
	gap := "this method is not implemented"
	probe := func(verb, path string) {
		recorder := h.invoke(verb, path, nil)
		if recorder.Code == 501 && bytes.Contains(recorder.Body.Bytes(), []byte(gap)) {
			t.Fatalf("%s %s hit the dispatch gap", verb, path)
		}
	}
	probe(http.MethodGet, testDatasets)
	probe(http.MethodGet, testDatasets+"/"+testDataset)
	probe(http.MethodGet, testDatasets+"/"+testDataset+"/tables")
	probe(http.MethodGet, testDatasets+"/"+testDataset+"/tables/"+testTable)
	probe(http.MethodGet, testDatasets+"/"+testDataset+"/tables/"+testTable+"/data")
}

// engineSet replaces the project policy in the test harness.
func (h *harness) engineSet(policy iam.Policy) error {
	if _, err := h.engine.SetPolicy("//cloudresourcemanager.googleapis.com/projects/"+testProject, policy); err != nil {
		return fmt.Errorf("set the project policy: %w", err)
	}
	return nil
}

func TestJobsQueryAndResults(t *testing.T) {
	h := newHarness(t)
	h.expect(http.MethodPost, testDatasets, map[string]any{"datasetReference": map[string]any{"datasetId": testDataset}}, 200)
	h.expect(http.MethodPost, testDatasets+"/"+testDataset+"/tables", map[string]any{
		"tableReference": map[string]any{"datasetId": testDataset, "tableId": testTable},
		"schema":         map[string]any{"fields": []any{map[string]any{"name": "title", "type": "STRING"}, map[string]any{"name": "rank", "type": "INTEGER"}}},
	}, 200)
	h.expect(http.MethodPost, testDatasets+"/"+testDataset+"/tables/"+testTable+"/insertAll", map[string]any{
		"rows": []any{
			map[string]any{"json": map[string]any{"title": "fire-and-fog", "rank": 1}},
			map[string]any{"json": map[string]any{"title": "still-wind", "rank": 2}},
		},
	}, 200)

	recorder := h.expect(http.MethodPost, "/bigquery/v2/projects/"+testProject+"/queries", map[string]any{
		"query": "SELECT title FROM " + testDataset + "." + testTable + " WHERE rank = 2",
	}, 200)
	var result struct {
		JobReference struct {
			JobID string `json:"jobId"`
		} `json:"jobReference"`
		Rows []struct {
			F []struct {
				V string `json:"v"`
			} `json:"f"`
		} `json:"rows"`
		TotalRows any `json:"totalRows"`
	}
	if json.Unmarshal(recorder.Body.Bytes(), &result) != nil {
		t.Fatalf("decode the query result: %v", recorder.Body.String())
	}
	if result.JobReference.JobID == "" || len(result.Rows) != 1 || result.Rows[0].F[0].V != "still-wind" {
		t.Fatalf("unexpected query result %+v", result)
	}
	h.expect(http.MethodGet, "/bigquery/v2/projects/"+testProject+"/queries/"+result.JobReference.JobID, nil, 200)

	want(t, "invalid aggregate", h.invoke(http.MethodPost, "/bigquery/v2/projects/"+testProject+"/queries", map[string]any{
		"query": "SELECT SUM(title) FROM " + testDataset + "." + testTable,
	}), 400, "No matching signature for aggregate function SUM")
	h.expect(http.MethodPost, "/bigquery/v2/projects/"+testProject+"/queries", map[string]any{
		"query": "SELECT title FROM " + testDataset + ".missing",
	}, 404)
}

// TestMain runs the tests as the binaries run: in UTC (RequireUTC).
func TestMain(m *testing.M) {
	RequireUTC()
	os.Exit(m.Run())
}

// An unsupported-policy refusal on a live resource fails closed: the project
// anchor fallback is only for targets absent from the index.
func TestUnsupportedPolicyFailsClosed(t *testing.T) {
	h := newHarness(t)
	h.dataset()
	if err := h.engine.MarkUnsupported(datasetResource(testProject, testDataset), []string{"test-only feature"}); err != nil {
		t.Fatal(err)
	}
	want(t, "unsupported dataset read", h.invokeAs(testToken, http.MethodGet, testDatasets+"/"+testDataset, nil), 501, "Unsupported policy configuration")
	want(t, "deleted dataset reads through the project", h.invokeAs(testToken, http.MethodGet, testDatasets+"/absent", nil), 404, "absent")
}

// A scope the state store refuses fails closed instead of skipping the check.
func TestRefusedScopeFailsClosed(t *testing.T) {
	h := newHarness(t)
	call := gateway.Call{Context: context.Background(), Method: methodDatasetsGet,
		Principal: gateway.Principal{State: gateway.Authenticated, Subject: testActor}}
	if _, err := h.bq.authorize(call, call.Method, "", datasetResource("", testDataset)); err == nil || !strings.Contains(err.Error(), `authorize in scope /global`) {
		t.Fatalf("authorize in a refused scope = %v, want a refusal", err)
	}
}

// A table policy is the table's own: it leaves the dataset's grants alone and
// gives the new member only that table.
func TestTablePolicyIsItsOwn(t *testing.T) {
	h := newHarness(t)
	h.table()
	tables := testDatasets + "/" + testDataset + "/tables/"
	h.expect(http.MethodPost, testDatasets+"/"+testDataset+"/tables", map[string]any{
		"tableReference": map[string]any{"datasetId": testDataset, "tableId": "other"},
		"schema":         map[string]any{"fields": []any{map[string]any{"name": "title", "type": "STRING"}}},
	}, 200)
	dataset := datasetResource(testProject, testDataset)
	if _, err := h.engine.SetPolicy(dataset, iam.Policy{Bindings: []iam.Binding{
		{Role: "roles/bigquery.dataViewer", Members: []string{account("keeper")}},
	}}); err != nil {
		t.Fatal(err)
	}
	h.expect(http.MethodPost, tables+testTable+":setIamPolicy", map[string]any{"policy": map[string]any{
		"bindings": []any{map[string]any{"role": "roles/bigquery.dataViewer", "members": []any{account("reader")}}},
	}}, 200)
	policy, err := h.engine.GetPolicy(dataset)
	if err != nil || len(policy.Bindings) != 1 || policy.Bindings[0].Members[0] != account("keeper") {
		t.Fatalf("dataset policy after a table setIamPolicy = %+v, %v", policy, err)
	}
	want(t, "dataset grant still reads", h.invokeAs("keeper", http.MethodGet, tables+"other/data", nil), 200, "tableDataList")
	want(t, "table grant reads its table", h.invokeAs("reader", http.MethodGet, tables+testTable+"/data", nil), 200, "tableDataList")
	want(t, "table grant reads no other table", h.invokeAs("reader", http.MethodGet, tables+"other/data", nil), 403, "bigquery.tables.getData")
	want(t, "missing table policy", h.invoke(http.MethodPost, tables+"absent:getIamPolicy", map[string]any{}), 404, "absent")

	// A recreated table starts without the deleted table's grants.
	h.expect(http.MethodDelete, tables+testTable, nil, 204)
	h.expect(http.MethodPost, testDatasets+"/"+testDataset+"/tables", map[string]any{
		"tableReference": map[string]any{"datasetId": testDataset, "tableId": testTable},
		"schema":         map[string]any{"fields": []any{map[string]any{"name": "title", "type": "STRING"}}},
	}, 200)
	want(t, "recreated table drops the old grant", h.invokeAs("reader", http.MethodGet, tables+testTable+"/data", nil), 403, "bigquery.tables.getData")
}

// TestTablePolicyIsAuthorizedOnTheCurrentTable covers a table setIamPolicy
// authorized by a grant on the table, which is deleted and recreated before
// the policy write. The recreated table has a fresh policy, so the write must
// be refused, not land on a table the caller holds no grant on.
func TestTablePolicyIsAuthorizedOnTheCurrentTable(t *testing.T) {
	h := newHarness(t)
	h.table()
	tables := testDatasets + "/" + testDataset + "/tables"
	resource := tableResource(testProject, testDataset, testTable)
	if _, err := h.engine.SetPolicy(resource, iam.Policy{Bindings: []iam.Binding{
		{Role: "roles/bigquery.dataOwner", Members: []string{account("owner")}},
	}}); err != nil {
		t.Fatal(err)
	}
	h.expect(http.MethodDelete, tables+"/"+testTable, nil, 204)
	h.expect(http.MethodPost, tables, map[string]any{
		"tableReference": map[string]any{"datasetId": testDataset, "tableId": testTable},
		"schema":         map[string]any{"fields": []any{map[string]any{"name": "title", "type": "STRING"}}},
	}, 200)
	call := gateway.Call{Context: context.Background(), Method: methodTablesSetIam,
		Principal: gateway.Principal{State: gateway.Authenticated, Subject: account("owner"), Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}}
	request := &gateway.RESTRequest{Path: map[string]string{"project": testProject, keyDatasetID: testDataset, keyTableID: testTable},
		JSON: []byte(`{"policy":{"bindings":[{"role":"roles/bigquery.dataOwner","members":["` + account("owner") + `"]}]}}`)}
	if _, err := h.bq.execute(call, request, provider.ProjectScope(testProject)); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("policy write on the recreated table = %v", err)
	}
	if policy, _ := h.engine.GetPolicy(resource); len(policy.Bindings) != 0 {
		t.Fatalf("the recreated table's policy changed: %+v", policy)
	}
}

// A recreated dataset and table start with empty policies, as in GCP. The
// index removal on delete drops the old policy for every provider, BigQuery
// included. (Table policies are stored on their dataset here.)
func TestRecreatedDatasetStartsWithEmptyPolicy(t *testing.T) {
	h := newHarness(t)
	h.table()
	table := testDatasets + "/" + testDataset + "/tables/" + testTable
	member := "serviceAccount:reader@" + testProject + ".iam.gserviceaccount.com"
	h.expect(http.MethodPost, table+":setIamPolicy", map[string]any{"policy": map[string]any{
		"bindings": []any{map[string]any{"role": "roles/bigquery.dataViewer", "members": []any{member}}},
	}}, 200)
	h.expect(http.MethodDelete, testDatasets+"/"+testDataset+"?deleteContents=true", nil, 204)
	h.table()
	policy := h.expect(http.MethodPost, table+":getIamPolicy", map[string]any{}, 200)
	if strings.Contains(policy.Body.String(), member) {
		t.Fatalf("recreated table kept the deleted grants: %s", policy.Body.String())
	}
}

// wireOf decodes a v2 error body.
func wireOf(t *testing.T, recorder *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body struct {
		Error map[string]any `json:"error"`
	}
	decode(t, recorder, &body)
	return body.Error
}

// expectRefusal asserts a recorded v2 refusal: its code, message, and the
// one errors[] entry ({} for none).
func expectRefusal(t *testing.T, step string, recorder *httptest.ResponseRecorder, code int, message string, entry map[string]any) {
	t.Helper()
	wire := wireOf(t, recorder)
	entries, _ := wire["errors"].([]any)
	got := map[string]any{}
	if len(entries) == 1 {
		got, _ = entries[0].(map[string]any)
	}
	if recorder.Code != code || wire["message"] != message || len(entries) > 1 || fmt.Sprint(got) != fmt.Sprint(entry) {
		t.Errorf("%s = %d %s, want %d %q %v", step, recorder.Code, recorder.Body.String(), code, message, entry)
	}
}

// global is an errors[] entry in the global domain.
func global(reason, message string) map[string]any {
	return map[string]any{"domain": "global", "message": message, "reason": reason}
}

// TestRecordedErrorBodies follows bigquery-api-surface: each refusal's
// message and errors[] entry.
func TestRecordedErrorBodies(t *testing.T) {
	h := newHarness(t)
	h.table()
	dataset, table := testDatasets+"/"+testDataset, testDatasets+"/"+testDataset+"/tables"
	name := testProject + ":" + testDataset
	create := map[string]any{"datasetReference": map[string]any{"datasetId": testDataset}}
	message := "Already Exists: Dataset " + name
	expectRefusal(t, "duplicate dataset", h.invoke(http.MethodPost, testDatasets, create), 409, message, global("duplicate", message))
	message = "Not found: Dataset " + name + "_absent"
	expectRefusal(t, "missing dataset", h.invoke(http.MethodGet, dataset+"_absent", nil), 404, message, global("notFound", message))
	message = "Access Denied: Dataset " + name + ": Permission bigquery.datasets.get denied on dataset " + name + " (or it may not exist)."
	expectRefusal(t, "denied dataset", h.invokeAs("denied", http.MethodGet, dataset, nil), 403, message, global("accessDenied", message))
	message = "Access Denied: Table " + name + ".absent: Permission bigquery.tables.get denied on table " + name + ".absent (or it may not exist)."
	expectRefusal(t, "denied missing table", h.invokeAs("denied", http.MethodGet, table+"/absent", nil), 403, message, global("accessDenied", message))
	message = "Not found: Table " + name + ".absent"
	expectRefusal(t, "missing table", h.invoke(http.MethodGet, table+"/absent", nil), 404, message, global("notFound", message))
	message = `Invalid table ID "bad id!".`
	expectRefusal(t, "bad table id", h.invoke(http.MethodPost, table, map[string]any{"tableReference": map[string]any{"tableId": "bad id!"},
		"schema": map[string]any{"fields": []any{map[string]any{"name": "a", "type": "STRING"}}}}), 400, message, global("invalid", message))
	message = "Dataset " + name + " is still in use"
	expectRefusal(t, "dataset in use", h.invoke(http.MethodDelete, dataset, nil), 400, message, global("resourceInUse", message))
	message = "Access Denied: Project " + testProject + ": User does not have bigquery.jobs.create permission in project " + testProject + "."
	expectRefusal(t, "denied query", h.queryAs("denied", "SELECT 1", nil), 403, message, global("accessDenied", message))
	message = "Not found: Table " + name + ".absent was not found in location US"
	expectRefusal(t, "query missing table", h.queryAs(testToken, "SELECT 1 FROM "+testDataset+".absent", nil), 404, message, global("notFound", message))
	message = `Syntax error: Unexpected identifier "SELEC" at [1:1]`
	expectRefusal(t, "syntax", h.queryAs(testToken, "SELEC 1", nil), 400, message,
		map[string]any{"domain": "global", "location": "q", "locationType": "parameter", "message": message, "reason": "invalidQuery"})
	message = "Invalid value at 'max_results.value' (TYPE_UINT32), \"-1\""
	recorder := h.invoke(http.MethodGet, testDatasets+"?maxResults=-1", nil)
	expectRefusal(t, "negative maxResults", recorder, 400, message, map[string]any{"message": message, "reason": "invalid"})
	if details, _ := wireOf(t, recorder)["details"].([]any); len(details) != 1 {
		t.Errorf("maxResults details = %s", recorder.Body.String())
	}
	expectRefusal(t, "policy version", h.invoke(http.MethodPost, table+"/"+testTable+":setIamPolicy", map[string]any{"policy": map[string]any{"version": 3}}),
		400, "Unsupported policy version 3; max supported version is 1.", map[string]any{})

	request, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, dataset, nil)
	recorder = httptest.NewRecorder()
	h.gate.ServeHTTP(recorder, request)
	expectRefusal(t, "anonymous", recorder, 401, "Request is missing required authentication credential. Expected OAuth 2 access token,"+
		" login cookie or other valid authentication credential. See https://developers.google.com/identity/sign-in/web/devconsole-project.",
		map[string]any{"domain": "global", "location": "Authorization", "locationType": "header", "message": "Login Required.", "reason": "required"})
	if details := fmt.Sprint(wireOf(t, recorder)["details"]); !strings.Contains(details, "google.cloud.bigquery.v2.DatasetService.GetDataset") {
		t.Errorf("anonymous details = %s", details)
	}
}

// descriptors is the slice of a dataset or table document the patch tests read.
type descriptors struct {
	Labels       map[string]string `json:"labels"`
	FriendlyName string            `json:"friendlyName"`
	Description  string            `json:"description"`
	TimeTravel   string            `json:"maxTimeTravelHours"`
}

// patched sends one patch and decodes the result.
func (h *harness) patched(path string, body any) descriptors {
	h.t.Helper()
	var document descriptors
	decode(h.t, h.expect(http.MethodPatch, path, body, 200), &document)
	return document
}

// TestDatasetPatchMergesLabelsAndKeepsOmittedFields follows the recorded
// patches: labels merge, omitted descriptors stay, and a patched dataset
// reports its time-travel window. A null label removes it.
func TestDatasetPatchMergesLabelsAndKeepsOmittedFields(t *testing.T) {
	h := newHarness(t)
	h.expect(http.MethodPost, testDatasets, map[string]any{"datasetReference": map[string]any{"datasetId": testDataset},
		"friendlyName": "one", "description": "first", "labels": map[string]string{"a": "1", "b": "2"}}, 200)
	dataset := testDatasets + "/" + testDataset
	if got := h.patched(dataset, map[string]any{"labels": map[string]any{"a": "1"}}); fmt.Sprint(got) != "{map[a:1 b:2] one first 168}" {
		t.Fatalf("labels patch = %+v", got)
	}
	if got := h.patched(dataset, map[string]any{"friendlyName": "renamed", "labels": map[string]any{"b": nil}}); fmt.Sprint(got) != "{map[a:1] renamed first 168}" {
		t.Fatalf("name patch = %+v", got)
	}
	// A read-modify-write sends the read document back; unchanged values pass.
	h.expect(http.MethodPatch, dataset, json.RawMessage(h.expect(http.MethodGet, dataset, nil, 200).Body.Bytes()), 200)
}

// TestTablePatchMergesLabelsAndKeepsOmittedFields follows the recorded table
// patches, and refuses a schema change instead of dropping it.
func TestTablePatchMergesLabelsAndKeepsOmittedFields(t *testing.T) {
	h := newHarness(t)
	h.dataset()
	table := testDatasets + "/" + testDataset + "/tables/" + testTable
	h.expect(http.MethodPost, testDatasets+"/"+testDataset+"/tables", map[string]any{"tableReference": map[string]any{"tableId": testTable}, "friendlyName": "t",
		"labels": map[string]string{"a": "1", "b": "2"}, "schema": map[string]any{"fields": []any{map[string]any{"name": "title", "type": "STRING"}}}}, 200)
	if got := h.patched(table, map[string]any{"labels": map[string]any{"a": "9"}, "description": "about"}); fmt.Sprint(got) != "{map[a:9 b:2] t about }" {
		t.Fatalf("table patch = %+v", got)
	}
	h.expect(http.MethodPatch, table, json.RawMessage(h.expect(http.MethodGet, table, nil, 200).Body.Bytes()), 200)
	want(t, "schema change", h.invoke(http.MethodPatch, table, map[string]any{"schema": map[string]any{"fields": []any{
		map[string]any{"name": "other", "type": "STRING"}}}}), 501, "BigQuery coverage gap: schema changes through tables.patch")
}

// TestUnmodeledFieldsAreGaps refuses each unmodeled dataset and table field by
// name instead of dropping it, and ignores names the schema lacks, as GCP does.
func TestUnmodeledFieldsAreGaps(t *testing.T) {
	h := newHarness(t)
	h.dataset()
	dataset := map[string]any{"datasetReference": map[string]any{"datasetId": "second"}}
	with := func(base map[string]any, key string, value any) map[string]any {
		copied := maps.Clone(base)
		copied[key] = value
		return copied
	}
	for field, value := range map[string]any{
		"defaultEncryptionConfiguration": map[string]any{"kmsKeyName": "k"}, "storageBillingModel": "PHYSICAL",
	} {
		want(t, field, h.invoke(http.MethodPost, testDatasets, with(dataset, field, value)), 501, "BigQuery coverage gap: "+field+" is not modeled")
	}
	want(t, "routine access", h.invoke(http.MethodPost, testDatasets, with(dataset, "access", []any{map[string]any{
		"routine": map[string]any{"projectId": testProject, "datasetId": testDataset, "routineId": "f"}}})),
		501, "BigQuery coverage gap: routine access entries (routines are not modeled)")
	want(t, "access condition", h.invoke(http.MethodPost, testDatasets, with(dataset, "access", []any{map[string]any{
		"role": "READER", "userByEmail": "a@b.c", "condition": map[string]any{"expression": "true"}}})),
		501, "BigQuery coverage gap: dataset access entry conditions")
	want(t, "time travel", h.invoke(http.MethodPost, testDatasets, with(dataset, "maxTimeTravelHours", "48")), 501, "BigQuery coverage gap: maxTimeTravelHours other than 168")
	want(t, "dataset view", h.invoke(http.MethodGet, testDatasets+"/"+testDataset+"?datasetView=ACL", nil), 501, "BigQuery coverage gap: query parameter datasetView")
	want(t, "list filter", h.invoke(http.MethodGet, testDatasets+"?filter=labels.a:1", nil), 501, "BigQuery coverage gap: query parameter filter")
	want(t, "unknown name", h.invoke(http.MethodPost, testDatasets, with(dataset, "bogus", 1)), 200, `"datasetId":"second"`)
	table := map[string]any{"tableReference": map[string]any{"tableId": "t"}, "schema": map[string]any{"fields": []any{map[string]any{"name": "a", "type": "STRING"}}}}
	for field, value := range map[string]any{
		"encryptionConfiguration": map[string]any{"kmsKeyName": "k"}, "externalDataConfiguration": map[string]any{"sourceUris": []string{"gs://b/o"}},
		"materializedView": map[string]any{"query": "SELECT 1"},
	} {
		want(t, field, h.invoke(http.MethodPost, testDatasets+"/"+testDataset+"/tables", with(table, field, value)), 501, "BigQuery coverage gap: "+field+" is not modeled")
	}
	nested := with(table, "schema", map[string]any{"fields": []any{map[string]any{"name": "r", "type": "RECORD", "fields": []any{
		map[string]any{"name": "s", "type": "STRING", "maxLength": "10"}}}}})
	want(t, "nested column option", h.invoke(http.MethodPost, testDatasets+"/"+testDataset+"/tables", nested), 501,
		"BigQuery coverage gap: schema.fields.fields.maxLength is not modeled")
	want(t, "table view", h.invoke(http.MethodGet, testDatasets+"/"+testDataset+"/tables/t?view=BASIC", nil), 501, "BigQuery coverage gap: query parameter view")
	tagged := func(names []string) map[string]any {
		return with(table, "schema", map[string]any{"fields": []any{map[string]any{"name": "a", "type": "STRING", "policyTags": map[string]any{"names": names}}}})
	}
	want(t, "policy tags", h.invoke(http.MethodPost, testDatasets+"/"+testDataset+"/tables", tagged([]string{"projects/p/locations/us/taxonomies/1/policyTags/2"})),
		501, "BigQuery coverage gap: column policyTags (column a)")
	want(t, "empty policy tags", h.invoke(http.MethodPost, testDatasets+"/"+testDataset+"/tables", tagged([]string{})), 200, `"tableId":"t"`)
}

// pick follows keys and indexes into decoded JSON; a missing step is nil.
func pick(value any, steps ...any) any {
	for _, step := range steps {
		if index, ok := step.(int); ok {
			if list, _ := value.([]any); index < len(list) {
				value = list[index]
				continue
			}
			return nil
		}
		object, _ := value.(map[string]any)
		value = object[fmt.Sprint(step)]
	}
	return value
}

// listed reads one list page: the IDs under field's references, and the body.
func (h *harness) listed(path, field, id string) ([]string, map[string]any) {
	h.t.Helper()
	var body map[string]any
	decode(h.t, h.expect(http.MethodGet, path, nil, 200), &body)
	items, _ := body[field].([]any)
	names := make([]string, 0, len(items))
	for index := range items {
		names = append(names, fmt.Sprint(pick(body, field, index, strings.TrimSuffix(field, "s")+"Reference", id)))
	}
	return names, body
}

// TestDatasetsListIsOrderedAndPaged lists datasets by ID in pages, hides
// "_" datasets without all=true, and lists nothing for a foreign token
// (recorded).
func TestDatasetsListIsOrderedAndPaged(t *testing.T) {
	h := newHarness(t)
	for _, id := range []string{"zeta", "alpha", "_hidden", "mid"} {
		h.expect(http.MethodPost, testDatasets, map[string]any{"datasetReference": map[string]any{"datasetId": id}}, 200)
	}
	if names, body := h.listed(testDatasets, "datasets", "datasetId"); fmt.Sprint(names) != "[alpha mid zeta]" || body["kind"] != "bigquery#datasetList" {
		t.Fatalf("datasets = %v %v", names, body)
	}
	if names, _ := h.listed(testDatasets+"?all=true", "datasets", "datasetId"); fmt.Sprint(names) != "[_hidden alpha mid zeta]" {
		t.Fatalf("all datasets = %v", names)
	}
	names, body := h.listed(testDatasets+"?maxResults=2", "datasets", "datasetId")
	rest, _ := h.listed(testDatasets+"?maxResults=2&pageToken="+fmt.Sprint(body["nextPageToken"]), "datasets", "datasetId")
	if fmt.Sprint(names) != "[alpha mid]" || fmt.Sprint(rest) != "[zeta]" {
		t.Fatalf("pages = %v %v", names, rest)
	}
	if names, body := h.listed(testDatasets+"?pageToken=not-a-page-token", "datasets", "datasetId"); len(names) != 0 || len(body) != 2 {
		t.Fatalf("foreign token = %v", body)
	}
	want(t, "mistyped maxResults", h.invoke(http.MethodGet, testDatasets+"?maxResults=many", nil), 400, `Invalid value at 'max_results.value' (TYPE_UINT32), \"many\"`)
}

// TestTablesListIsOrderedAndPaged lists tables by ID with totalItems and
// reads the first page for a foreign token (recorded).
func TestTablesListIsOrderedAndPaged(t *testing.T) {
	h := newHarness(t)
	h.dataset()
	tables := testDatasets + "/" + testDataset + "/tables"
	for _, id := range []string{"t1", "t0"} {
		h.expect(http.MethodPost, tables, map[string]any{"tableReference": map[string]any{"tableId": id}, "labels": map[string]string{"k": "v"},
			"schema": map[string]any{"fields": []any{map[string]any{"name": "a", "type": "STRING"}}}}, 200)
	}
	names, body := h.listed(tables+"?maxResults=1", "tables", "tableId")
	item, _ := pick(body, "tables", 0).(map[string]any)
	if fmt.Sprint(names) != "[t0]" || body["totalItems"] != float64(2) || body["nextPageToken"] == nil || len(item) != 6 || item["labels"] == nil {
		t.Fatalf("tables page = %v", body)
	}
	if names, _ := h.listed(tables+"?pageToken=not-a-page-token", "tables", "tableId"); fmt.Sprint(names) != "[t0 t1]" {
		t.Fatalf("foreign table token = %v", names)
	}
	want(t, "missing dataset tables", h.invoke(http.MethodGet, testDatasets+"/absent/tables", nil), 404, "Not found: Dataset "+testProject+":absent")
}

// jobListing is the slice of a jobs.list page the tests read.
type jobListing struct {
	Kind string           `json:"kind"`
	Jobs []map[string]any `json:"jobs"`
	Next string           `json:"nextPageToken"`
}

// jobsListed reads one jobs.list page.
func (h *harness) jobsListed(path string) jobListing {
	h.t.Helper()
	var listing jobListing
	decode(h.t, h.expect(http.MethodGet, path, nil, 200), &listing)
	return listing
}

// fiveJobs runs two jobs.insert jobs, two jobs.query jobs, and one job by
// another user, a second apart; it returns the second job's creation time.
func (h *harness) fiveJobs() int64 {
	h.t.Helper()
	h.rows()
	h.grantJobUsers()
	clock := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h.bq.clock = func() time.Time { clock = clock.Add(time.Second); return clock }
	for _, id := range []string{"first", "second"} {
		h.expect(http.MethodPost, testJobs, queryJob(id, testSQL), 200)
	}
	h.expect(http.MethodPost, testQueries, map[string]any{"query": "SELECT 1", "useLegacySql": false, "requestId": "r"}, 200)
	h.expect(http.MethodPost, testQueries, map[string]any{"query": "SELECT 1", "useLegacySql": false}, 200)
	want(h.t, "runner job", h.invokeAs("runner", http.MethodPost, testQueries, map[string]any{"query": "SELECT 1", "useLegacySql": false}), 200, "jobComplete")
	created, _ := strconv.ParseInt(fmt.Sprint(pick(h.jobsListed(testJobs + "?maxResults=3").Jobs[2], "statistics", "creationTime")), 10, 64)
	return created
}

// TestJobsListNewestFirst lists the caller's jobs newest first in the
// recorded minimal and full shapes, and refuses a foreign token as GCP does.
func TestJobsListNewestFirst(t *testing.T) {
	h := newHarness(t)
	h.fiveJobs()
	listing := h.jobsListed(testJobs + "?maxResults=3")
	keys := func(item map[string]any) string { return fmt.Sprint(slices.Sorted(maps.Keys(item))) }
	if listing.Kind != "bigquery#jobList" || len(listing.Jobs) != 3 || listing.Next == "" ||
		keys(listing.Jobs[0]) != "[id jobReference kind state statistics status]" || pick(listing.Jobs[2], "jobReference", "jobId") != "second" {
		t.Fatalf("jobs = %+v", listing)
	}
	full := h.jobsListed(testJobs + "?projection=full&maxResults=3&pageToken=" + listing.Next)
	if len(full.Jobs) != 1 || pick(full.Jobs[0], "jobReference", "jobId") != "first" ||
		keys(full.Jobs[0]) != "[configuration id jobReference kind principal_subject state statistics status user_email]" {
		t.Fatalf("full page = %+v", full)
	}
	expectRefusal(t, "bad token", h.invoke(http.MethodGet, testJobs+"?pageToken=not-a-page-token", nil), 400, "Invalid pagination token not-a-page-token",
		global("invalid", "Invalid pagination token not-a-page-token"))
	want(t, "parent job", h.invoke(http.MethodGet, testJobs+"?parentJobId=x", nil), 501, "BigQuery coverage gap: query parameter parentJobId")
}

// TestJobsListFiltersByUserStateAndTime lists only the caller's jobs unless
// allUsers, which needs bigquery.jobs.listAll, and applies stateFilter and
// the creation-time bounds.
func TestJobsListFiltersByUserStateAndTime(t *testing.T) {
	h := newHarness(t)
	second := h.fiveJobs()
	counts := map[string]int{
		testJobs: 4, testJobs + "?allUsers=true": 5, testJobs + "?stateFilter=running": 0, testJobs + "?stateFilter=done": 4,
		testJobs + "?minCreationTime=" + strconv.FormatInt(second, 10): 3, testJobs + "?maxCreationTime=" + strconv.FormatInt(second-1, 10): 1,
	}
	for path, count := range counts {
		if got := len(h.jobsListed(path).Jobs); got != count {
			t.Errorf("%s lists %d jobs, want %d", path, got, count)
		}
	}
	want(t, "runner list", h.invokeAs("runner", http.MethodGet, testJobs, nil), 403, "bigquery.jobs.list permission")
}

// TestRequestIDDeduplicatesRetries answers a jobs.query retry with the same
// requestId from the first run instead of running it again.
func TestRequestIDDeduplicatesRetries(t *testing.T) {
	h := newHarness(t)
	h.rows()
	insert := map[string]any{"query": "INSERT INTO " + testDataset + "." + testTable + " (title) VALUES ('again')", "useLegacySql": false, "requestId": "retry-1"}
	var first, second struct {
		JobReference struct {
			JobID string `json:"jobId"`
		} `json:"jobReference"`
	}
	decode(t, h.expect(http.MethodPost, testQueries, insert, 200), &first)
	decode(t, h.expect(http.MethodPost, testQueries, insert, 200), &second)
	if first.JobReference.JobID == "" || first.JobReference.JobID != second.JobReference.JobID {
		t.Fatalf("retry ran a new job: %s then %s", first.JobReference.JobID, second.JobReference.JobID)
	}
	want(t, "one insert", h.queryAs(testToken, "SELECT COUNT(*) AS n FROM "+testDataset+"."+testTable+" WHERE title = 'again'", nil), 200, `"v":"1"`)
}

// TestProjectConditionSeesTheAddressedResource grants dataViewer on the
// project under a condition naming one dataset, with BigQuery's documented
// resource.type, resource.service, and resource.name. GCP refuses
// conditions on table policies (recorded), so a project grant is where they
// apply; this evaluation is modeled, not recorded.
func TestProjectConditionSeesTheAddressedResource(t *testing.T) {
	h := newHarness(t)
	h.table()
	h.expect(http.MethodPost, testDatasets, map[string]any{"datasetReference": map[string]any{"datasetId": "other"}}, 200)
	project := provider.ProjectResource(testProject)
	policy, err := h.engine.GetPolicy(project)
	if err != nil {
		t.Fatal(err)
	}
	expression := `resource.type == "bigquery.googleapis.com/Dataset" && resource.service == "bigquery.googleapis.com"` +
		` && resource.name == "projects/` + testProject + `/datasets/` + testDataset + `"`
	policy.Version = 3
	policy.Bindings = append(policy.Bindings, iam.Binding{Role: "roles/bigquery.dataViewer", Members: []string{account("reader")},
		Condition: json.RawMessage(`{"title":"one-dataset","expression":` + strconv.Quote(expression) + `}`)})
	if _, err := h.engine.SetPolicy(project, policy); err != nil {
		t.Fatal(err)
	}
	h.expectAs("reader", http.MethodGet, testDatasets+"/"+testDataset, 200)
	h.expectAs("reader", http.MethodGet, testDatasets+"/other", 403)
	h.expectAs("reader", http.MethodGet, testDatasets+"/"+testDataset+"/tables/"+testTable, 403)
}

// expectAs issues one request as token and requires the status.
func (h *harness) expectAs(token, verb, path string, code int) {
	h.t.Helper()
	if recorder := h.invokeAs(token, verb, path, nil); recorder.Code != code {
		h.t.Errorf("%s %s as %s = %d %s, want %d", verb, path, token, recorder.Code, recorder.Body.String(), code)
	}
}

// TestAccessPolicyVersionIsAccepted serves accessPolicyVersion on dataset
// inserts, reads, and patches, as the google Terraform provider sends it:
// any int32 is accepted and anything else refused, as recorded.
func TestAccessPolicyVersionIsAccepted(t *testing.T) {
	h := newHarness(t)
	h.expect(http.MethodPost, testDatasets+"?accessPolicyVersion=3", map[string]any{"datasetReference": map[string]any{"datasetId": testDataset}}, 200)
	dataset := testDatasets + "/" + testDataset
	for _, version := range []string{"1", "2", "3"} {
		h.expect(http.MethodGet, dataset+"?accessPolicyVersion="+version, nil, 200)
	}
	h.expect(http.MethodPatch, dataset+"?accessPolicyVersion=3", map[string]any{}, 200)
	message := `Invalid value at 'access_policy_version' (TYPE_INT32), "many"`
	expectRefusal(t, "mistyped", h.invoke(http.MethodGet, dataset+"?accessPolicyVersion=many", nil), 400, message,
		map[string]any{"message": message, "reason": "invalid"})
}

// TestPatchIsAtomic rejects a dataset or table update with one bad field
// without changing anything.
func TestPatchIsAtomic(t *testing.T) {
	h := newHarness(t)
	h.table()
	dataset := testDatasets + "/" + testDataset
	table := dataset + "/tables/" + testTable
	for _, path := range []string{dataset, table} {
		h.expect(http.MethodPatch, path, map[string]any{"friendlyName": "before"}, 200)
		want(t, "bad update "+path, h.invoke(http.MethodPatch, path, map[string]any{"friendlyName": "after", "labels": "bogus"}), 400, "Invalid")
		if body := h.invoke(http.MethodGet, path, nil).Body.String(); !strings.Contains(body, `"friendlyName":"before"`) || strings.Contains(body, "after") {
			t.Errorf("a rejected update changed %s: %s", path, body)
		}
	}
}

// TestConcurrentRequests races dataset and table lifecycles, streaming
// inserts, DDL and DML queries, reads, and deletes on one dataset; -race
// fails it if a handler touches a record outside the scope lock.
func TestConcurrentRequests(t *testing.T) {
	h := newHarness(t)
	dataset := testDatasets + "/" + testDataset
	table := dataset + "/tables/" + testTable
	schema := map[string]any{"fields": []any{map[string]any{"name": "title", "type": "STRING"}}}
	query := func(sql string, want ...int) providertest.Request {
		return providertest.Request{Method: http.MethodPost, Path: testQueries, Body: map[string]any{"query": sql}, Want: want}
	}
	path := testDataset + "." + testTable
	providertest.Hammer(t, h.gate, testToken, 8, 6, func(int) []providertest.Request {
		return []providertest.Request{
			{Method: http.MethodPost, Path: testDatasets, Body: map[string]any{"datasetReference": map[string]any{"datasetId": testDataset}}, Want: []int{200, 409}},
			{Method: http.MethodPatch, Path: dataset, Body: map[string]any{"labels": map[string]string{"k": "v"}}, Want: []int{200, 404}},
			{Method: http.MethodPatch, Path: dataset, Body: map[string]any{"defaultTableExpirationMs": "3600000", "access": []any{
				map[string]any{"role": "OWNER", "userByEmail": "actor@" + testProject + ".iam.gserviceaccount.com"},
				map[string]any{"role": "READER", "groupByEmail": testGroup},
			}}, Want: []int{200, 404}},
			{Method: http.MethodPost, Path: dataset + "/tables", Body: map[string]any{"tableReference": map[string]any{"tableId": "parted"}, "schema": map[string]any{
				"fields": layoutFields}, "timePartitioning": map[string]any{"type": "DAY", "expirationMs": "3600000"}, "clustering": map[string]any{"fields": []string{"n"}}},
				Want: []int{200, 404, 409}},
			query("INSERT INTO "+testDataset+".parted (name) VALUES ('p')", 200, 404),
			query("SELECT * FROM "+testDataset+".parted WHERE _PARTITIONTIME IS NOT NULL", 200, 404), //nolint:unqueryvet // star over a pseudo-column
			{Method: http.MethodPatch, Path: dataset + "/tables/parted", Body: map[string]any{"clustering": nil, "expirationTime": nil}, Want: []int{200, 404}},
			{Method: http.MethodPost, Path: dataset + "/tables", Body: map[string]any{"tableReference": map[string]any{"tableId": testTable}, "schema": schema}, Want: []int{200, 404, 409}},
			{Method: http.MethodPost, Path: table + "/insertAll", Body: map[string]any{"rows": []any{map[string]any{"json": map[string]any{"title": "x"}}}}, Want: []int{200, 404}},
			{Method: http.MethodPatch, Path: table, Body: map[string]any{"friendlyName": "w"}, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: table + "/data", Want: []int{200, 404}},
			query("SELECT COUNT(*) FROM "+path, 200, 404),
			query("INSERT INTO "+path+" (title) VALUES ('y')", 200, 404),
			query("CREATE TABLE IF NOT EXISTS "+testDataset+".made (x INT64)", 200, 404),
			{Method: http.MethodGet, Path: dataset + "/tables?maxResults=1", Want: []int{200, 404}},
			{Method: http.MethodGet, Path: "/bigquery/v2/projects/" + testProject + "/jobs?projection=full", Want: []int{200}},
			{Method: http.MethodGet, Path: testDatasets, Want: []int{200}},
			query("DROP TABLE IF EXISTS "+testDataset+".made", 200, 404),
			{Method: http.MethodDelete, Path: table, Want: []int{204, 404}},
			{Method: http.MethodDelete, Path: dataset + "?deleteContents=true", Want: []int{204, 404}},
		}
	})
	// Whatever the interleaving, the metadata and the engine agree at the end.
	h.expect(http.MethodPost, testDatasets, map[string]any{"datasetReference": map[string]any{"datasetId": "after"}}, 200)
	h.expect(http.MethodPost, testDatasets+"/after/tables", map[string]any{"tableReference": map[string]any{"tableId": "t"}, "schema": schema}, 200)
	h.rowsAre("engine after the race", h.queryAs(testToken, "SELECT COUNT(*) FROM after.t", nil), "0")
}

// TestFailedCreateLeavesNoEngineTable drops an engine table whose metadata
// could not be stored, so a later create of the same name succeeds.
func TestFailedCreateLeavesNoEngineTable(t *testing.T) {
	h := newHarness(t)
	h.dataset()
	scope := provider.ProjectScope(testProject)
	schema := map[string]any{"fields": []any{map[string]any{"name": "title", "type": "STRING"}}}
	create := map[string]any{"tableReference": map[string]any{"tableId": testTable}, "schema": schema}
	ddl := "CREATE TABLE " + testDataset + ".made (x INT64)"
	for _, table := range []string{testTable, "made"} {
		// Another owner's index entry makes indexing the new table fail
		// after the engine created it.
		blocker := state.Resource{Name: tableResource(testProject, testDataset, table), Service: "blocker", Kind: kindTable,
			Parent: datasetResource(testProject, testDataset), Scope: scope}
		if err := h.index.Put("blocker", blocker); err != nil {
			t.Fatal(err)
		}
		if table == testTable {
			want(t, "blocked tables.insert", h.invoke(http.MethodPost, testDatasets+"/"+testDataset+"/tables", create), 500, "Internal provider error")
		} else {
			want(t, "blocked CREATE TABLE", h.queryAs(testToken, ddl, nil), 500, "Internal provider error")
		}
		if err := h.index.Remove("blocker", scope, blocker.Name); err != nil {
			t.Fatal(err)
		}
	}
	h.expect(http.MethodPost, testDatasets+"/"+testDataset+"/tables", create, 200)
	h.expect(http.MethodPost, testQueries, map[string]any{"query": ddl}, 200)
}

// TestQueryOnADeletedDataset refuses a plan whose dataset was deleted after
// planning, before the engine creates a table no metadata would name; and a
// table create that reaches the engine gate after its dataset went.
func TestQueryOnADeletedDataset(t *testing.T) {
	h := newHarness(t)
	h.dataset()
	scope := provider.ProjectScope(testProject)
	ddl := "CREATE TABLE " + testDataset + ".made (x INT64)"
	plan, err := planQuery(h.bq.catalogOf(scope), ddl, "", "", nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	h.expect(http.MethodDelete, testDatasets+"/"+testDataset, nil, 204)
	err = h.bq.runPlan(context.Background(), scope, plan, &jobRecord{})
	if status.Code(err) != codes.NotFound || !strings.Contains(err.Error(), "Not found: Dataset "+testProject+":"+testDataset) {
		t.Fatalf("plan on a deleted dataset = %v, want NotFound", err)
	}
	err = h.bq.admitTable(scope, testDataset, testTable)
	if status.Code(err) != codes.NotFound || !strings.Contains(err.Error(), testDataset) {
		t.Fatalf("admit into a deleted dataset = %v, want NotFound", err)
	}
	h.dataset()
	h.expect(http.MethodPost, testQueries, map[string]any{"query": ddl}, 200)
}

// documentOf decodes a successful response's document.
func documentOf(t *testing.T, recorder *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	if recorder.Code != 200 {
		t.Fatalf("status %d: %s", recorder.Code, recorder.Body.String())
	}
	var document map[string]any
	decode(t, recorder, &document)
	return document
}

// layoutFields is the schema the partitioning tests share.
var layoutFields = []any{
	map[string]any{"name": "ts", "type": "TIMESTAMP"}, map[string]any{"name": "d", "type": "DATE"},
	map[string]any{"name": "name", "type": "STRING"}, map[string]any{"name": "n", "type": "INTEGER"},
}

// createLayout creates one table of layoutFields in the fixture dataset
// with the given layout fields.
func (h *harness) createLayout(table string, fields map[string]any) *httptest.ResponseRecorder {
	h.t.Helper()
	body := map[string]any{"tableReference": map[string]any{"tableId": table}, "schema": map[string]any{"fields": layoutFields}}
	maps.Copy(body, fields)
	return h.invoke(http.MethodPost, testDatasets+"/"+testDataset+"/tables", body)
}

// sameAs fails a step unless got prints as want.
func sameAs(t *testing.T, step string, got any, want string) {
	t.Helper()
	if text := fmt.Sprint(got); text != want {
		t.Errorf("%s = %s, want %s", step, text, want)
	}
}

// fieldsOf selects a document's fields, in order.
func fieldsOf(document map[string]any, keys ...string) []any {
	values := make([]any, 0, len(keys))
	for _, key := range keys {
		values = append(values, document[key])
	}
	return values
}

// TestPartitioningAndClustering stores and returns each partitioning kind and
// clustering as GCP does (recorded: bigquery-partitioning-expiration).
func TestPartitioningAndClustering(t *testing.T) {
	h := newHarness(t)
	h.dataset()
	tables := testDatasets + "/" + testDataset + "/tables"
	created := documentOf(t, h.createLayout("events", map[string]any{
		"timePartitioning":       map[string]any{"type": "DAY", "field": "ts", "expirationMs": "86400000"},
		"requirePartitionFilter": true, "clustering": map[string]any{"fields": []string{"name", "n"}},
	}))
	sameAs(t, "created", fieldsOf(created, "timePartitioning", "clustering", "requirePartitionFilter", "partitionDefinition"),
		"[map[expirationMs:86400000 field:ts type:DAY] map[fields:[name n]] true map[partitionedColumn:[map[field:ts]]]]")
	read := documentOf(t, h.expect(http.MethodGet, tables+"/events", nil, 200))
	sameAs(t, "read", read["timePartitioning"], "map[expirationMs:86400000 field:ts requirePartitionFilter:true type:DAY]")
	ingest := documentOf(t, h.createLayout("ingest", map[string]any{"timePartitioning": map[string]any{"type": "HOUR"}}))
	sameAs(t, "ingestion-time", fieldsOf(ingest, "timePartitioning", "partitionDefinition"), "[map[type:HOUR] <nil>]")
	ranged := documentOf(t, h.createLayout("ranged", map[string]any{
		"rangePartitioning": map[string]any{"field": "n", "range": map[string]any{"start": 0, "end": "100", "interval": "10"}},
		"timePartitioning":  map[string]any{"type": "DAY", "field": "ts"},
	}))
	sameAs(t, "both partitionings keep the range", fieldsOf(ranged, "rangePartitioning", "timePartitioning"),
		"[map[field:n range:map[end:100 interval:10 start:0]] <nil>]")
	_, listing := h.listed(tables, "tables", "tableId")
	item, _ := pick(listing, "tables", 0).(map[string]any)
	sameAs(t, "list item", fieldsOf(item, "timePartitioning", "requirePartitionFilter"), "[map[expirationMs:86400000 field:ts type:DAY] true]")

	patched := documentOf(t, h.expect(http.MethodPatch, tables+"/events", map[string]any{
		"clustering": map[string]any{"fields": []string{"n"}}, "requirePartitionFilter": false,
		"timePartitioning": map[string]any{"type": "DAY", "field": "ts", "expirationMs": "172800000"},
	}, 200))
	sameAs(t, "patched", fieldsOf(patched, "clustering", "requirePartitionFilter", "timePartitioning"),
		"[map[fields:[n]] false map[expirationMs:172800000 field:ts type:DAY]]")
	for path, body := range map[string]map[string]any{
		"/events": {"timePartitioning": map[string]any{"type": "MONTH", "field": "ts"}},
		"/ranged": {"rangePartitioning": map[string]any{"field": "n", "range": map[string]any{"start": "0", "end": "50", "interval": "10"}}},
		"/ingest": {"timePartitioning": map[string]any{"type": "DAY"}},
	} {
		expectRefusal(t, "spec change "+path, h.invoke(http.MethodPatch, tables+path, body), 400, specFrozen, global(reasonInvalid, specFrozen))
	}
	cleared := documentOf(t, h.expect(http.MethodPatch, tables+"/events", map[string]any{"clustering": nil}, 200))
	sameAs(t, "cleared clustering", cleared["clustering"], "<nil>")
}

// TestPartitioningRefusals refuses invalid partitioning and clustering with
// GCP's messages (recorded: bigquery-partitioning-expiration).
func TestPartitioningRefusals(t *testing.T) {
	h := newHarness(t)
	h.dataset()
	invalid := func(step string, fields map[string]any, message string) {
		t.Helper()
		expectRefusal(t, step, h.createLayout("refused", fields), 400, message, global(reasonInvalid, message))
	}
	invalid("type", map[string]any{"timePartitioning": map[string]any{"type": "WEEK"}}, "Invalid value for timePartitioning.type: WEEK is not a valid value")
	invalid("missing field", map[string]any{"timePartitioning": map[string]any{"field": "absent"}}, "The field specified for partitioning cannot be found in the schema.")
	invalid("field type", map[string]any{"timePartitioning": map[string]any{"field": "name"}},
		"The field specified for time partitioning can only be of type TIMESTAMP, DATE or DATETIME. The type found is: STRING.")
	invalid("range type", map[string]any{"rangePartitioning": map[string]any{"field": "ts", "range": map[string]any{"end": "1", "interval": "1"}}},
		"The field specified for integer range partitioning can only be of type INT64. The type found is: TIMESTAMP.")
	invalid("range interval", map[string]any{"rangePartitioning": map[string]any{"field": "n", "range": map[string]any{"end": "1", "interval": "0"}}},
		"Interval must be larger than 0 for range partition.")
	invalid("range bounds", map[string]any{"rangePartitioning": map[string]any{"field": "n", "range": map[string]any{"end": "0", "interval": "1"}}},
		"End must be larger than start for range partition.")
	invalid("repeated clustering", map[string]any{"clustering": map[string]any{"fields": []string{"n", "N"}}},
		"Field can only appear once in clustering fields. Invalid field: N.")
	invalid("missing clustering", map[string]any{"clustering": map[string]any{"fields": []string{"absent"}}},
		"The field specified for clustering cannot be found in the schema. Invalid field: absent.")
	invalid("partition expiration", map[string]any{"timePartitioning": map[string]any{"expirationMs": "-1"}}, "Partition expiration must be positive")
	five := map[string]any{"tableReference": map[string]any{"tableId": "five"}, "clustering": map[string]any{"fields": []string{"ts", "d", "name", "n", "e"}},
		"schema": map[string]any{"fields": append(slices.Clone(layoutFields), map[string]any{"name": "e", "type": "STRING"})}}
	expectRefusal(t, "five clustering fields", h.invoke(http.MethodPost, testDatasets+"/"+testDataset+"/tables", five), 400,
		"5 clustering fields specified, exceeding the limit of 4.", global(reasonInvalid, "5 clustering fields specified, exceeding the limit of 4."))
	want(t, "mistyped expirationMs", h.createLayout("refused", map[string]any{"timePartitioning": map[string]any{"expirationMs": "soon"}}), 400, "invalid int64 value")
}

// TestTableExpiration expires a table on the provider's clock: it reads as
// gone at once and a new table may take its name (recorded:
// bigquery-partitioning-expiration for the expirationTime round trip).
func TestTableExpiration(t *testing.T) {
	h := newHarness(t)
	now := time.Now()
	h.bq.clock = func() time.Time { return now }
	h.dataset()
	table := testDatasets + "/" + testDataset + "/tables/short"
	in := func(d time.Duration) string { return strconv.FormatInt(now.Add(d).UnixMilli(), 10) }
	expired := "Expiration time must be at least 1 seconds in the future"
	expectRefusal(t, "past expiration", h.createLayout("past", map[string]any{"expirationTime": in(-time.Hour)}), 400, expired, global(reasonInvalid, expired))
	sameAs(t, "expirationTime", documentOf(t, h.createLayout("short", map[string]any{"expirationTime": in(time.Hour)}))["expirationTime"], in(time.Hour))
	h.expect(http.MethodPatch, table, map[string]any{"expirationTime": in(2 * time.Hour)}, 200)
	sameAs(t, "cleared", documentOf(t, h.expect(http.MethodPatch, table, map[string]any{"expirationTime": nil}, 200))["expirationTime"], "<nil>")
	expectRefusal(t, "past patch", h.invoke(http.MethodPatch, table, map[string]any{"expirationTime": in(0)}), 400, expired, global(reasonInvalid, expired))
	h.expect(http.MethodPatch, table, map[string]any{"expirationTime": in(time.Hour)}, 200)

	now = now.Add(time.Hour)
	h.expect(http.MethodGet, table, nil, 404)
	ids, _ := h.listed(testDatasets+"/"+testDataset+"/tables", "tables", "tableId")
	sameAs(t, "listed", ids, "[]")
	want(t, "query an expired table", h.queryAs(testToken, "SELECT 1 FROM "+testDataset+".short", nil), 404, "Not found: Table")
	h.expect(http.MethodDelete, testDatasets+"/"+testDataset, nil, 204)
}

// TestDatasetDefaultExpiration applies a dataset's default expirations to
// new tables: a partitioned table under a partition default gets no table
// expiration (recorded: bigquery-partitioning-expiration).
func TestDatasetDefaultExpiration(t *testing.T) {
	h := newHarness(t)
	now := time.Now()
	h.bq.clock = func() time.Time { return now }
	h.dataset()
	dataset := testDatasets + "/" + testDataset
	day := strconv.FormatInt(now.Add(24*time.Hour).UnixMilli(), 10)
	short := "Table expiration is less than 3600000ms"
	expectRefusal(t, "short default", h.invoke(http.MethodPatch, dataset, map[string]any{"defaultTableExpirationMs": "1000"}), 400, short, global(reasonInvalid, short))
	defaults := documentOf(t, h.expect(http.MethodPatch, dataset, map[string]any{"defaultTableExpirationMs": "86400000", "defaultPartitionExpirationMs": "172800000"}, 200))
	sameAs(t, "defaults", fieldsOf(defaults, "defaultTableExpirationMs", "defaultPartitionExpirationMs"), "[86400000 172800000]")
	partitioned := documentOf(t, h.createLayout("partitioned", map[string]any{"timePartitioning": map[string]any{"field": "ts"}}))
	sameAs(t, "partitioned", fieldsOf(partitioned, "expirationTime", "timePartitioning"), "[<nil> map[expirationMs:172800000 field:ts type:DAY]]")
	sameAs(t, "plain", documentOf(t, h.createLayout("plain", nil))["expirationTime"], day)
	h.expect(http.MethodPost, testQueries, map[string]any{"query": "CREATE TABLE " + testDataset + ".ddl (x INT64)"}, 200)
	sameAs(t, "DDL table", documentOf(t, h.expect(http.MethodGet, dataset+"/tables/ddl", nil, 200))["expirationTime"], day)
	cleared := documentOf(t, h.expect(http.MethodPatch, dataset, map[string]any{"defaultTableExpirationMs": nil, "defaultPartitionExpirationMs": nil}, 200))
	sameAs(t, "cleared", fieldsOf(cleared, "defaultTableExpirationMs", "defaultPartitionExpirationMs"), "[<nil> <nil>]")
	sameAs(t, "after clearing", documentOf(t, h.createLayout("later", nil))["expirationTime"], "<nil>")
}

// TestPartitionExpiration drops an expired partition's rows before a read:
// partitions expire their expirationMs after their start. The engine stamps
// rows with the real time, so the clock moves from there.
func TestPartitionExpiration(t *testing.T) {
	h := newHarness(t)
	now := time.Now()
	h.bq.clock = func() time.Time { return now }
	h.dataset()
	h.createLayout("days", map[string]any{"timePartitioning": map[string]any{"field": "ts", "expirationMs": "172800000"}})
	h.createLayout("months", map[string]any{"timePartitioning": map[string]any{"type": "MONTH", "field": "d", "expirationMs": "3456000000"}})
	h.createLayout("ingest", map[string]any{"timePartitioning": map[string]any{"type": "DAY", "expirationMs": "172800000"}})
	ds := testDatasets + "/" + testDataset + "/tables/"
	h.expect(http.MethodPost, testQueries, map[string]any{"query": "INSERT INTO " + testDataset + ".days (ts, name) VALUES " +
		"(CURRENT_TIMESTAMP(), 'new'), (TIMESTAMP '2001-01-01', 'old'), (NULL, 'unpartitioned'); " +
		"INSERT INTO " + testDataset + ".months (d, name) VALUES (CURRENT_DATE(), 'new'), (DATE '2001-01-01', 'old'); " +
		"INSERT INTO " + testDataset + ".ingest (name) VALUES ('new')"}, 200)
	h.rowsAre("expired partition unread", h.queryAs(testToken, "SELECT name FROM "+testDataset+".days ORDER BY name", nil), "new", "unpartitioned")
	h.rowsAre("expired month unread", h.queryAs(testToken, "SELECT name FROM "+testDataset+".months", nil), "new")
	now = now.Add(3 * 24 * time.Hour)
	h.rowsAre("every dated partition expired", h.invoke(http.MethodGet, ds+"days/data", nil), "NULL,NULL,unpartitioned,NULL")
	h.rowsAre("ingestion-time partition expired", h.queryAs(testToken, "SELECT name FROM "+testDataset+".ingest", nil))
}
