package bigtable

import (
	"net/http"
	"strings"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/providers/provider/providertest"
)

// TestPatchIsAtomic rejects an invalid instance update without changing the
// instance.
func TestPatchIsAtomic(t *testing.T) {
	h := newHarness(t)
	h.instance()
	instance := testInstances + "/" + testInstance
	if recorder := h.invoke(http.MethodPatch, instance, map[string]any{"displayName": "after", "labels": "bogus"}); recorder.Code != 400 {
		t.Fatalf("invalid update status = %d", recorder.Code)
	}
	if body := h.invoke(http.MethodGet, instance, nil).Body.String(); !strings.Contains(body, `"displayName":"demo"`) {
		t.Fatalf("a rejected update changed the instance: %s", body)
	}
}

// TestConcurrentInstanceRequests races instance create, reads, updates, table
// methods, and deletes on one instance; -race fails it if a handler touches a
// record outside the scope lock.
func TestConcurrentInstanceRequests(t *testing.T) {
	h := newHarness(t)
	instance := testInstances + "/" + testInstance
	table := instance + "/tables/" + testTable
	policy := map[string]any{"policy": map[string]any{"bindings": []any{map[string]any{
		"role": "roles/bigtable.user", "members": []any{"serviceAccount:reader@" + testProject + ".iam.gserviceaccount.com"},
	}}}}
	providertest.Hammer(t, h.gate, testToken, 8, 10, func(int) []providertest.Request {
		return []providertest.Request{
			{Method: http.MethodPost, Path: testInstances, Body: createBody(), Want: []int{200, 409}},
			{Method: http.MethodGet, Path: instance, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: testInstances, Want: []int{200}},
			{Method: http.MethodPatch, Path: instance, Body: map[string]any{"displayName": "worker", "labels": map[string]string{"worker": "w"}}, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: instance + "/clusters", Want: []int{200, 404}},
			{Method: http.MethodGet, Path: instance + "/clusters/" + testCluster, Want: []int{200, 404}},
			{Method: http.MethodPost, Path: instance + "/tables", Body: map[string]any{"tableId": testTable}, Want: []int{200, 404, 409}},
			{Method: http.MethodGet, Path: table, Want: []int{200, 404}},
			{Method: http.MethodPost, Path: instance + ":setIamPolicy", Body: policy, Want: []int{200, 404}},
			{Method: http.MethodPost, Path: instance + ":getIamPolicy", Body: map[string]any{}, Want: []int{200, 404}},
			{Method: http.MethodPost, Path: table + ":setIamPolicy", Body: policy, Want: []int{200, 404}},
			{Method: http.MethodPost, Path: table + ":getIamPolicy", Body: map[string]any{}, Want: []int{200, 404}},
			{Method: http.MethodDelete, Path: table, Want: []int{200, 404}},
			{Method: http.MethodDelete, Path: instance, Want: []int{200, 404}},
		}
	})
}
