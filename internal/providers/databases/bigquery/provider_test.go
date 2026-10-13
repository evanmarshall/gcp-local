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
	"github.com/evanmarshall/gcp-local/internal/state"
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
		"defaultTableExpirationMs": "3600000", "defaultPartitionExpirationMs": "3600000",
		"defaultEncryptionConfiguration": map[string]any{"kmsKeyName": "k"}, "storageBillingModel": "PHYSICAL",
	} {
		want(t, field, h.invoke(http.MethodPost, testDatasets, with(dataset, field, value)), 501, "BigQuery coverage gap: "+field+" is not modeled")
	}
	want(t, "access", h.invoke(http.MethodPost, testDatasets, with(dataset, "access", []any{map[string]any{"role": "READER", "userByEmail": "a@b.c"}})),
		501, "BigQuery coverage gap: dataset access entries other than the defaults")
	want(t, "time travel", h.invoke(http.MethodPost, testDatasets, with(dataset, "maxTimeTravelHours", "48")), 501, "BigQuery coverage gap: maxTimeTravelHours other than 168")
	want(t, "dataset view", h.invoke(http.MethodGet, testDatasets+"/"+testDataset+"?datasetView=ACL", nil), 501, "BigQuery coverage gap: query parameter datasetView")
	want(t, "list filter", h.invoke(http.MethodGet, testDatasets+"?filter=labels.a:1", nil), 501, "BigQuery coverage gap: query parameter filter")
	want(t, "unknown name", h.invoke(http.MethodPost, testDatasets, with(dataset, "bogus", 1)), 200, `"datasetId":"second"`)
	table := map[string]any{"tableReference": map[string]any{"tableId": "t"}, "schema": map[string]any{"fields": []any{map[string]any{"name": "a", "type": "STRING"}}}}
	for field, value := range map[string]any{
		"timePartitioning": map[string]any{"type": "DAY"}, "rangePartitioning": map[string]any{"field": "a"},
		"clustering": map[string]any{"fields": []string{"a"}}, "expirationTime": "1800000000000",
		"encryptionConfiguration": map[string]any{"kmsKeyName": "k"}, "externalDataConfiguration": map[string]any{"sourceUris": []string{"gs://b/o"}},
		"materializedView": map[string]any{"query": "SELECT 1"}, "requirePartitionFilter": true,
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
