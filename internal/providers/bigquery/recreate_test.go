package bigquery

import (
	"net/http"
	"strings"
	"testing"
)

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
