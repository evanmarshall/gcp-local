package artifactregistry

import (
	"net/http"
	"strings"
	"testing"
)

// A recreated repository starts with an empty policy, as in GCP; the deleted
// repository's grants are gone with it.
func TestRecreatedRepositoryStartsWithEmptyPolicy(t *testing.T) {
	h := newHarness(t)
	create := func() {
		if recorder := h.invoke(http.MethodPost, testBase+"?repositoryId="+testRepo, map[string]any{"format": "DOCKER"}); recorder.Code != 200 {
			t.Fatalf("create status = %d body %s", recorder.Code, recorder.Body.String())
		}
	}
	create()
	member := "serviceAccount:reader@" + testProject + ".iam.gserviceaccount.com"
	if recorder := h.invoke(http.MethodPost, testBase+"/"+testRepo+":setIamPolicy", map[string]any{"policy": map[string]any{
		"bindings": []any{map[string]any{"role": "roles/artifactregistry.reader", "members": []any{member}}},
	}}); recorder.Code != 200 {
		t.Fatalf("setIamPolicy status = %d body %s", recorder.Code, recorder.Body.String())
	}
	if recorder := h.invoke(http.MethodDelete, testBase+"/"+testRepo, nil); recorder.Code != 200 {
		t.Fatalf("delete status = %d body %s", recorder.Code, recorder.Body.String())
	}
	create()
	policy := h.invoke(http.MethodGet, testBase+"/"+testRepo+":getIamPolicy", nil)
	if policy.Code != 200 || strings.Contains(policy.Body.String(), member) {
		t.Fatalf("recreated repository policy = %d %s", policy.Code, policy.Body.String())
	}
}
