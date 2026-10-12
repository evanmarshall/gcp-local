package bigquery

import (
	"net/http"
	"strings"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/providers/provider/providertest"
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
