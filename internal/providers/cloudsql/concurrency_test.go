package cloudsql

import (
	"net/http"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/providers/provider/providertest"
)

// TestPatchIsAtomic rejects a patch with one unmodeled field without applying
// the modeled one.
func TestPatchIsAtomic(t *testing.T) {
	h := newHarness(t)
	h.createInstance()
	h.expect(h.invoke(adminToken, http.MethodPatch, testPath, settingsBodyOf(map[string]any{"tier": smallTier, "databaseFlags": []any{}})), http.StatusNotImplemented)
	settings := object(t, h.expect(h.invoke(adminToken, http.MethodGet, testPath, nil), http.StatusOK)[keySettings])
	if settings["tier"] != "db-f1-micro" || settings[keySettingsVersion] != "1" {
		t.Fatalf("a rejected patch changed the instance: %v", settings)
	}
}

// TestConcurrentInstanceRequests races insert, get, list, patch, database
// create, and delete on one instance; -race fails it if a handler touches the
// catalog unlocked.
func TestConcurrentInstanceRequests(t *testing.T) {
	h := newHarness(t)
	providertest.Hammer(t, h.gate, adminToken, 8, 20, func(int) []providertest.Request {
		return []providertest.Request{
			{Method: http.MethodPost, Path: testInstances, Body: insertBody(), Want: []int{200, 409}},
			{Method: http.MethodGet, Path: testPath, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: testInstances, Want: []int{200}},
			{Method: http.MethodPatch, Path: testPath, Body: settingsBodyOf(map[string]any{"tier": smallTier, "userLabels": map[string]string{"worker": "w"}}), Want: []int{200, 404}},
			{Method: http.MethodPost, Path: testPath + "/databases", Body: map[string]any{keyName: testDatabase}, Want: []int{200, 404, 409}},
			{Method: http.MethodGet, Path: testOperations, Want: []int{200}},
			{Method: http.MethodDelete, Path: testPath, Want: []int{200, 404}},
		}
	})
}

// TestConcurrentMySQLRequests races the MySQL paths on one instance: insert,
// get, database and user create, list, and delete, and instance delete, so
// every engine branch reads the catalog under its lock and a request that
// loses to a delete gets 404.
func TestConcurrentMySQLRequests(t *testing.T) {
	h := newHarness(t)
	providertest.Hammer(t, h.gate, adminToken, 8, 20, func(int) []providertest.Request {
		return []providertest.Request{
			{Method: http.MethodPost, Path: testInstances, Body: mysqlInsertBody(), Want: []int{200, 409}},
			{Method: http.MethodGet, Path: testMySQLPath, Want: []int{200, 404}},
			{Method: http.MethodPost, Path: testMySQLPath + "/databases", Body: map[string]any{keyName: testDatabase, "charset": "utf8mb4"}, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: testMySQLPath + "/databases", Want: []int{200, 404}},
			{Method: http.MethodPost, Path: testMySQLPath + "/users", Body: map[string]any{keyName: "app", "password": "pw"}, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: testMySQLPath + "/users", Want: []int{200, 404}},
			{Method: http.MethodDelete, Path: testMySQLPath + "/users?name=app", Want: []int{200, 404}},
			{Method: http.MethodDelete, Path: testMySQLPath + "/databases/" + testDatabase, Want: []int{200, 404}},
			{Method: http.MethodDelete, Path: testMySQLPath, Want: []int{200, 404}},
		}
	})
}
