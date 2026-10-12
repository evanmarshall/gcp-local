package secretmanager

import (
	"net/http"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/providers/provider/providertest"
)

// TestConcurrentSecretRequests races create, get, list, update, delete, and
// every version method on one secret; -race fails it if a handler touches a
// record unlocked.
func TestConcurrentSecretRequests(t *testing.T) {
	h := newHarness(t)
	base := "/v1/projects/" + testProject + "/secrets"
	secret := base + "/" + testSecret
	version := secret + "/versions/1"
	providertest.Hammer(t, h.gate, "actor", 8, 20, func(int) []providertest.Request {
		return []providertest.Request{
			{Method: http.MethodPost, Path: base + "?secretId=" + testSecret, Body: map[string]any{"replication": replicationAuto()}, Want: []int{200, 409}},
			{Method: http.MethodGet, Path: secret, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: base, Want: []int{200}},
			{Method: http.MethodPatch, Path: secret + "?updateMask=labels", Body: map[string]any{"labels": map[string]any{"worker": "w"}}, Want: []int{200, 404}},
			{Method: http.MethodPost, Path: secret + ":addVersion", Body: map[string]any{"payload": map[string]any{"data": base64Of("value")}}, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: secret + "/versions", Want: []int{200, 404}},
			{Method: http.MethodGet, Path: version, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: secret + "/versions/latest:access", Want: []int{200, 400, 404}},
			{Method: http.MethodPost, Path: version + ":disable", Body: map[string]any{}, Want: []int{200, 400, 404}},
			{Method: http.MethodPost, Path: version + ":enable", Body: map[string]any{}, Want: []int{200, 400, 404}},
			{Method: http.MethodPost, Path: version + ":destroy", Body: map[string]any{}, Want: []int{200, 400, 404}},
			{Method: http.MethodGet, Path: secret + ":getIamPolicy", Want: []int{200, 404}},
			{Method: http.MethodDelete, Path: secret, Want: []int{200, 404}},
		}
	})
}
