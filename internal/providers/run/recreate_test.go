package run

import (
	"net/http"
	"strings"
	"testing"
)

// A recreated service starts with an empty policy, as in GCP: the deleted
// service's invokers cannot call its successor.
func TestRecreatedServiceStartsWithEmptyPolicy(t *testing.T) {
	h := newHarness(t)
	h.create(t)
	invoker := "serviceAccount:invoker@demo-project.iam.gserviceaccount.com"
	if recorder := h.invoke(http.MethodPost, "/v2/"+serviceReference+":setIamPolicy", map[string]any{"policy": map[string]any{
		"bindings": []any{map[string]any{"role": "roles/run.invoker", "members": []any{invoker}}},
	}}); recorder.Code != http.StatusOK {
		t.Fatalf("setIamPolicy = %d %s", recorder.Code, recorder.Body.String())
	}
	if recorder := h.invoke(http.MethodDelete, "/v2/"+serviceReference, nil); recorder.Code != http.StatusOK {
		t.Fatalf("delete = %d %s", recorder.Code, recorder.Body.String())
	}
	if recorder := h.invoke(http.MethodPost, servicesPath, serviceBody("")); recorder.Code != http.StatusOK {
		t.Fatalf("recreate = %d %s", recorder.Code, recorder.Body.String())
	}
	policy := h.invoke(http.MethodGet, "/v2/"+serviceReference+":getIamPolicy", nil)
	if policy.Code != http.StatusOK || strings.Contains(policy.Body.String(), invoker) {
		t.Fatalf("recreated service policy = %d %s", policy.Code, policy.Body.String())
	}
}
