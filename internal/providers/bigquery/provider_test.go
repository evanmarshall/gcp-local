package bigquery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
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
	_, err = New(context.Background(), registry, backend.NewManager(time.Second, time.Second), index, engine)
	if err != nil {
		t.Fatalf("build the bigquery provider: %v", err)
	}
	g := gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:" + token + "@" + testProject + ".iam.gserviceaccount.com", Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})
	return &harness{t: t, gate: g, engine: engine}
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
	if _, err := h.engineSet(iam.Policy{Version: 3, Bindings: []iam.Binding{
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
func (h *harness) engineSet(policy iam.Policy) (iam.Policy, error) {
	policy, err := h.engine.SetPolicy("//cloudresourcemanager.googleapis.com/projects/"+testProject, policy)
	if err != nil {
		return policy, fmt.Errorf("set the project policy: %w", err)
	}
	return policy, nil
}

func TestJobsQueryAndResults(t *testing.T) {
	h := newHarness(t)
	recorder := h.invoke(http.MethodPost, testDatasets, map[string]any{"datasetReference": map[string]any{"datasetId": testDataset}})
	if recorder.Code != 200 {
		t.Fatalf("dataset create status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodPost, testDatasets+"/"+testDataset+"/tables", map[string]any{
		"tableReference": map[string]any{"datasetId": testDataset, "tableId": testTable},
		"schema":         map[string]any{"fields": []any{map[string]any{"name": "title", "type": "STRING"}, map[string]any{"name": "rank", "type": "INTEGER"}}},
	})
	if recorder.Code != 200 {
		t.Fatalf("table create status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodPost, testDatasets+"/"+testDataset+"/tables/"+testTable+"/insertAll", map[string]any{
		"rows": []any{
			map[string]any{"json": map[string]any{"title": "fire-and-fog", "rank": 1}},
			map[string]any{"json": map[string]any{"title": "still-wind", "rank": 2}},
		},
	})
	if recorder.Code != 200 {
		t.Fatalf("insertAll status = %d", recorder.Code)
	}

	recorder = h.invoke(http.MethodPost, "/bigquery/v2/projects/"+testProject+"/queries", map[string]any{
		"query": "SELECT title FROM " + testDataset + "." + testTable + " WHERE rank = 2",
	})
	if recorder.Code != 200 {
		t.Fatalf("jobs.query status = %d body %s", recorder.Code, recorder.Body.String())
	}
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
	recorder = h.invoke(http.MethodGet, "/bigquery/v2/projects/"+testProject+"/queries/"+result.JobReference.JobID, nil)
	if recorder.Code != 200 {
		t.Fatalf("getQueryResults status = %d", recorder.Code)
	}

	recorder = h.invoke(http.MethodPost, "/bigquery/v2/projects/"+testProject+"/queries", map[string]any{
		"query": "SELECT SUM(title) FROM " + testDataset + "." + testTable,
	})
	if recorder.Code != 501 {
		t.Fatalf("unsupported function status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodPost, "/bigquery/v2/projects/"+testProject+"/queries", map[string]any{
		"query": "SELECT title FROM " + testDataset + ".missing",
	})
	if recorder.Code != 404 {
		t.Fatalf("missing table status = %d", recorder.Code)
	}
}
