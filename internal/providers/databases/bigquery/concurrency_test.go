package bigquery

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/providers/provider/providertest"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
			{Method: http.MethodPost, Path: dataset + "/tables", Body: map[string]any{"tableReference": map[string]any{"tableId": testTable}, "schema": schema}, Want: []int{200, 404, 409}},
			{Method: http.MethodPost, Path: table + "/insertAll", Body: map[string]any{"rows": []any{map[string]any{"json": map[string]any{"title": "x"}}}}, Want: []int{200, 404}},
			{Method: http.MethodPatch, Path: table, Body: map[string]any{"friendlyName": "w"}, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: table + "/data", Want: []int{200, 404}},
			query("SELECT COUNT(*) FROM "+path, 200, 404),
			query("INSERT INTO "+path+" (title) VALUES ('y')", 200, 404),
			query("CREATE TABLE IF NOT EXISTS "+testDataset+".made (x INT64)", 200, 404),
			{Method: http.MethodGet, Path: dataset + "/tables", Want: []int{200}},
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
	plan, err := planQuery(h.bq.catalogOf(scope), ddl, "", "", nil)
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
