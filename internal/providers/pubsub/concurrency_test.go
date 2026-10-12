package pubsub

import (
	"net/http"
	"strings"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/providers/provider/providertest"
)

// TestConcurrentTopicAndSubscriptionRequests races topic and subscription
// create, get, list, and delete on the in-memory driver; -race fails it if
// provider-owned labels, delivery, or index state is touched unlocked.
func TestConcurrentTopicAndSubscriptionRequests(t *testing.T) {
	_, registry, _ := memoryFixture(t)
	c := newRESTClient(t, registry)
	providertest.Hammer(t, c.g, strings.TrimPrefix(owner, "serviceAccount:"), 8, 10, func(int) []providertest.Request {
		return []providertest.Request{
			{Method: http.MethodPut, Path: restTopicPath, Body: map[string]any{"labels": map[string]string{"env": "test"}}, Want: []int{200, 409}},
			{Method: http.MethodGet, Path: restTopicPath, Want: []int{200, 404}},
			{Method: http.MethodPut, Path: restSubPath, Body: map[string]any{"topic": "rest-topic", "labels": map[string]string{"team": "a"}}, Want: []int{200, 404, 409}},
			{Method: http.MethodGet, Path: restSubPath, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: "/v1/projects/project-a/subscriptions", Want: []int{200}},
			{Method: http.MethodDelete, Path: restSubPath, Want: []int{200, 404}},
			{Method: http.MethodDelete, Path: restTopicPath, Want: []int{200, 404}},
		}
	})
}
