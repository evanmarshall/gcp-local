package cloudtasks

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/providers/provider/providertest"
)

// TestQueuePatchIsAtomic rejects a patch with one bad retry field without
// applying the valid rate limit sent beside it.
func TestQueuePatchIsAtomic(t *testing.T) {
	h := newHarness(t)
	queue := basePrefix + "/queues/" + testQueue
	if recorder := h.invoke(http.MethodPost, basePrefix+"/queues", map[string]any{"name": testQueue}); recorder.Code != 200 {
		t.Fatalf("create status = %d", recorder.Code)
	}
	before := h.invoke(http.MethodGet, queue, nil).Body.String()
	recorder := h.invoke(http.MethodPatch, queue, map[string]any{
		"rateLimits":  map[string]any{"maxDispatchesPerSecond": 7},
		"retryConfig": map[string]any{"maxAttempts": -5},
	})
	if recorder.Code != 400 {
		t.Fatalf("bad patch status = %d", recorder.Code)
	}
	if after := h.invoke(http.MethodGet, queue, nil).Body.String(); after != before {
		t.Fatalf("a rejected patch changed the queue:\nbefore %s\nafter  %s", before, after)
	}
}

// TestConcurrentQueueAndTaskRequests races queue and task methods on one
// queue while dispatch workers deliver and retry its tasks; -race fails it if
// a handler or worker touches a record unlocked.
func TestConcurrentQueueAndTaskRequests(t *testing.T) {
	h := newHarness(t)
	var hits atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1)%2 == 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	queues := basePrefix + "/queues"
	queue := queues + "/" + testQueue
	retry := map[string]any{"minBackoff": "0.001s", "maxBackoff": "0.002s", "maxAttempts": 3}
	providertest.Hammer(t, h.gate, "actor", 8, 15, func(worker int) []providertest.Request {
		task := queue + "/tasks/t" + strconv.Itoa(worker%2)
		return []providertest.Request{
			{Method: http.MethodPost, Path: queues, Body: map[string]any{"name": testQueue, "retryConfig": retry}, Want: []int{200, 409}},
			{Method: http.MethodPost, Path: queue + "/tasks", Body: map[string]any{"task": map[string]any{
				"name": strings.TrimPrefix(task, "/v2beta3/"), "httpRequest": map[string]any{
					"url": target.URL, "headers": map[string]any{"X-Worker": "w"}, "body": "aGk=",
				},
			}}, Want: []int{200, 404, 409}},
			{Method: http.MethodGet, Path: task + "?responseView=FULL", Want: []int{200, 404}},
			{Method: http.MethodGet, Path: queue + "/tasks", Want: []int{200, 404}},
			{Method: http.MethodPost, Path: task + ":run", Want: []int{200, 404}},
			{Method: http.MethodGet, Path: queue, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: queues, Want: []int{200}},
			{Method: http.MethodPatch, Path: queue, Body: map[string]any{"rateLimits": map[string]any{"maxDispatchesPerSecond": worker + 1}, "retryConfig": retry}, Want: []int{200, 404}},
			{Method: http.MethodPost, Path: queue + ":pause", Want: []int{200, 404}},
			{Method: http.MethodPost, Path: queue + ":resume", Want: []int{200, 404}},
			{Method: http.MethodDelete, Path: task, Want: []int{200, 404}},
			{Method: http.MethodPost, Path: queue + ":purge", Want: []int{200, 404}},
			{Method: http.MethodDelete, Path: queue, Want: []int{200, 404}},
		}
	})
	// The index entry and the queue record change together.
	_, indexed := h.provider.index.Get(queueResource(testProject, testLocation, testQueue))
	if stored := h.invoke(http.MethodGet, queue, nil).Code == 200; stored != indexed {
		t.Fatalf("queue stored = %v but indexed = %v", stored, indexed)
	}
}
