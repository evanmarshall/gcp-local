package spanner

import (
	"net/http"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/providers/provider/providertest"
)

// TestConcurrentAdminRequests races instance and database create, get, and
// delete on one instance; -race fails it if reference publication or the
// created-database store is touched outside its lock.
func TestConcurrentAdminRequests(t *testing.T) {
	h := newHarness(t)
	database := testPath + "/databases/" + testDatabase
	providertest.Hammer(t, h.gate, adminToken, 8, 20, func(int) []providertest.Request {
		return []providertest.Request{
			{Method: http.MethodPost, Path: testInstances, Body: instanceBody(testProject), Want: []int{200, 409}},
			{Method: http.MethodGet, Path: testPath, Want: []int{200, 404}},
			{Method: http.MethodPost, Path: testPath + "/databases", Body: createDatabase(testDatabase), Want: []int{200, 404, 409}},
			{Method: http.MethodGet, Path: database, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: testPath + "/databases", Want: []int{200, 404}},
			{Method: http.MethodDelete, Path: database, Want: []int{200, 404}},
			{Method: http.MethodDelete, Path: testPath, Want: []int{200, 404}},
		}
	})
}
