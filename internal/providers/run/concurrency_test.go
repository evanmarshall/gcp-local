package run

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/providers/provider/providertest"
)

// TestConcurrentServiceRequests races every service, revision, and operation
// method on one service while rollouts launch and readiness workers finish
// them. -race fails it if anything touches a record unlocked, and a request
// racing a delete must read 404, never an unverified-target 501.
func TestConcurrentServiceRequests(t *testing.T) {
	h := newHarness(t)
	revision := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer revision.Close()
	h.runtime.address = revision.Listener.Addr().String()
	service := "/v2/" + serviceReference
	update := map[string]any{"template": map[string]any{"containers": []any{map[string]any{"image": "example.local/svc:2"}}}}
	policy := map[string]any{"policy": map[string]any{"bindings": []any{map[string]any{
		"role": "roles/run.invoker", "members": []any{"serviceAccount:invoker@demo-project.iam.gserviceaccount.com"},
	}}}}
	providertest.Hammer(t, h.gateway, "fixture-actor", 8, 10, func(int) []providertest.Request {
		return []providertest.Request{
			{Method: http.MethodPost, Path: servicesPath, Body: serviceBody(""), Want: []int{200, 409}},
			{Method: http.MethodGet, Path: service, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: servicesPath, Want: []int{200}},
			// A concurrent rollout aborts the slower one (409).
			{Method: http.MethodPatch, Path: service, Body: update, Want: []int{200, 404, 409}},
			{Method: http.MethodPost, Path: service + ":setIamPolicy", Body: policy, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: service + ":getIamPolicy", Want: []int{200, 404}},
			{Method: http.MethodGet, Path: service + "/revisions", Want: []int{200, 404}},
			{Method: http.MethodGet, Path: service + "/revisions/svc-00001", Want: []int{200, 404}},
			{Method: http.MethodGet, Path: "/v2/projects/demo-project/locations/us-central1/operations", Want: []int{200}},
			{Method: http.MethodGet, Path: "/v2/projects/demo-project/locations/us-central1/operations/rel-svc", Want: []int{200, 404}},
			{Method: http.MethodDelete, Path: service, Want: []int{200, 404}},
		}
	})
	// The index entry and the service record change together.
	_, indexed := h.index.Get(serviceResource("demo-project", "us-central1", "svc"))
	if stored := h.invoke(http.MethodGet, service, nil).Code == http.StatusOK; stored != indexed {
		t.Fatalf("service stored = %v but indexed = %v", stored, indexed)
	}
}
