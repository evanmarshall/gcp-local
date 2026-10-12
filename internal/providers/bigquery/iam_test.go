package bigquery

import (
	"net/http"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/iam"
)

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
