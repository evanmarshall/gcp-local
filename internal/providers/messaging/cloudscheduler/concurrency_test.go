package cloudscheduler

import (
	"net/http"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/providers/provider/providertest"
)

// TestConcurrentJobRequests races create, get, list, patch, pause, and
// delete of one job. A get racing the delete reads 404, never the engine's
// unverified-target refusal, because authorize checks existence and IAM under
// one scope lock; -race fails it if a handler touches the jobs unlocked.
func TestConcurrentJobRequests(t *testing.T) {
	h := newHarness(t, nil)
	path := basePath + "/raced"
	job := map[string]any{"name": "projects/" + testProject + "/locations/" + testLocation + "/jobs/raced", "schedule": "0 0 1 1 *",
		"pubsubTarget": map[string]any{"topicName": "projects/" + testProject + "/topics/t", "data": "eA=="}}
	providertest.Hammer(t, h.gate, "actor", 8, 20, func(int) []providertest.Request {
		return []providertest.Request{
			{Method: http.MethodPost, Path: basePath, Body: job, Want: []int{200, 409}},
			{Method: http.MethodGet, Path: path, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: basePath + "?pageSize=1", Want: []int{200}},
			{Method: http.MethodPatch, Path: path + "?updateMask=description", Body: map[string]any{"description": "raced"}, Want: []int{200, 404}},
			{Method: http.MethodPost, Path: path + ":pause", Body: map[string]any{}, Want: []int{200, 404}},
			{Method: http.MethodDelete, Path: path, Want: []int{200, 404}},
		}
	})
}
