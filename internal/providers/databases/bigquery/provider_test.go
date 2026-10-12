package bigquery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
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
	_, err = New(context.Background(), registry, manager, index, engine)
	if err != nil {
		t.Fatalf("build the bigquery provider: %v", err)
	}
	g := gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:" + token + "@" + testProject + ".iam.gserviceaccount.com", Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})
	return &harness{t: t, gate: g, engine: engine, manager: manager}
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
