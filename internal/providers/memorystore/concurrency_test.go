package memorystore

import (
	"net/http"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/providers/provider/providertest"
)

// TestPatchIsAtomic rejects a mask with one bad path, or a valid mask with
// one bad value, without applying the good fields.
func TestPatchIsAtomic(t *testing.T) {
	h := newHarness(t)
	h.expect(h.create(adminToken, testProject, testRegion, testInstance, basic(nil)), http.StatusOK)
	path := testInstances + "/" + testInstance
	h.expect(h.invoke(adminToken, http.MethodPatch, path+"?updateMask=displayName,bogus", map[string]any{fieldDisplayName: "after"}), http.StatusBadRequest)
	h.expect(h.invoke(adminToken, http.MethodPatch, path+"?updateMask=displayName,memory_size_gb",
		map[string]any{fieldDisplayName: "after", fieldMemory: -1}), http.StatusBadRequest)
	if got := h.expect(h.invoke(adminToken, http.MethodGet, path, nil), http.StatusOK); got[fieldDisplayName] != "cache" {
		t.Fatalf("a rejected patch changed the instance: %v", got)
	}
}

// TestConcurrentInstanceRequests races create, get, list, patch, and delete
// on one instance; -race fails it if a handler touches the catalog unlocked.
func TestConcurrentInstanceRequests(t *testing.T) {
	h := newHarness(t)
	path := testInstances + "/" + testInstance
	providertest.Hammer(t, h.gate, adminToken, 8, 20, func(int) []providertest.Request {
		return []providertest.Request{
			{Method: http.MethodPost, Path: testInstances + "?instanceId=" + testInstance, Body: basic(nil), Want: []int{200, 409}},
			{Method: http.MethodGet, Path: path, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: testInstances, Want: []int{200}},
			{Method: http.MethodPatch, Path: path + "?updateMask=displayName,labels", Body: map[string]any{fieldDisplayName: "worker", "labels": map[string]string{"worker": "w"}}, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: base(testProject, testRegion) + "/operations", Want: []int{200}},
			{Method: http.MethodDelete, Path: path, Want: []int{200, 404}},
		}
	})
}
