package logging

import (
	"net/http"
	"strconv"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/providers/provider/providertest"
)

// TestConcurrentEntryRequests races entry writes, entry and log listing, and
// log deletion on one project; -race fails it if a handler reads or compacts
// the entry slice outside the lock.
func TestConcurrentEntryRequests(t *testing.T) {
	h := newHarness(t)
	providertest.Hammer(t, h.gate, actorAdmin, 8, 25, func(worker int) []providertest.Request {
		log := projectAName + "/logs/hammer" + strconv.Itoa(worker%2)
		return []providertest.Request{
			{Method: http.MethodPost, Path: "/v2/entries:write", Body: map[string]any{
				"logName": log, "resource": global(), "labels": map[string]any{labelEnv: "w"},
				"entries": []any{map[string]any{"textPayload": "a"}, map[string]any{"textPayload": "b"}},
			}, Want: []int{200}},
			{Method: http.MethodPost, Path: "/v2/entries:list", Body: map[string]any{
				"resourceNames": []string{projectAName}, "filter": `labels.env="w"`, "orderBy": orderDesc,
			}, Want: []int{200}},
			{Method: http.MethodGet, Path: "/v2/" + projectAName + "/logs", Want: []int{200}},
			{Method: http.MethodDelete, Path: "/v2/" + log, Want: []int{200, 404}},
		}
	})
}
