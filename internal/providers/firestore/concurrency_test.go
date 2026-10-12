package firestore

import (
	"net/http"
	"strings"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/providers/provider/providertest"
)

const fieldAdmin = "admin:" + testProject

// TestFieldPatchIsAtomic rejects a malformed field patch without changing the
// stored configuration.
func TestFieldPatchIsAtomic(t *testing.T) {
	h := newHarness(t)
	ttl := map[string]any{"ttlConfig": map[string]any{"expirationPolicy": map[string]any{"expiration": "60s"}}}
	if recorder := h.invoke(fieldAdmin, http.MethodPatch, testFieldPath, ttl); recorder.Code != http.StatusOK {
		t.Fatalf("field patch status = %d", recorder.Code)
	}
	if recorder := h.invokeRaw(fieldAdmin, http.MethodPatch, testFieldPath, `{"ttlConfig":"bad","indexConfig":{"ancestorField":"X"}}`); recorder.Code != http.StatusBadRequest {
		t.Fatalf("malformed patch status = %d", recorder.Code)
	}
	body := h.invoke(fieldAdmin, http.MethodGet, testFieldPath, nil).Body.String()
	if !strings.Contains(body, `"expiration":"60s"`) || strings.Contains(body, `"ancestorField":"X"`) {
		t.Fatalf("a rejected patch changed the field: %s", body)
	}
}

// TestConcurrentFieldRequests races field patches and reads with forwarded
// document requests on one project; -race fails it if a handler touches a
// field record outside the scope lock.
func TestConcurrentFieldRequests(t *testing.T) {
	h := newHarness(t)
	if _, err := h.engine.SetPolicy(provider.ProjectResource(testProject), iam.Policy{Bindings: []iam.Binding{
		{Role: "roles/datastore.user", Members: []string{principal("admin", testProject)}},
		{Role: "roles/datastore.indexAdmin", Members: []string{principal("admin", testProject)}},
	}}); err != nil {
		t.Fatalf("grant the field admin document access: %v", err)
	}
	providertest.Hammer(t, h.gate, fieldAdmin, 8, 20, func(worker int) []providertest.Request {
		patch := map[string]any{"ttlConfig": map[string]any{}}
		if worker%2 == 1 {
			patch = map[string]any{"indexConfig": map[string]any{"ancestorField": "REQUEST_TIME"}}
		}
		return []providertest.Request{
			{Method: http.MethodPatch, Path: testFieldPath, Body: patch, Want: []int{200}},
			{Method: http.MethodGet, Path: testFieldPath, Want: []int{200}},
			{Method: http.MethodPost, Path: testDocsPath + "/" + testCollection + "?documentId=" + testDoc, Body: titleBody("x"), Want: []int{200}},
			{Method: http.MethodGet, Path: docPath(), Want: []int{200}},
		}
	})
}
