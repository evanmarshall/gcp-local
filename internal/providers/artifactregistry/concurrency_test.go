package artifactregistry

import (
	"net/http"
	"strings"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/providers/provider/providertest"
)

// TestPatchIsAtomic rejects a mask with one bad field without applying the
// good one.
func TestPatchIsAtomic(t *testing.T) {
	h := newHarness(t)
	repository := testBase + "/" + testRepo
	if recorder := h.invoke(http.MethodPost, testBase+"?repositoryId="+testRepo, map[string]any{"format": "DOCKER", "description": "before"}); recorder.Code != 200 {
		t.Fatalf("create status = %d", recorder.Code)
	}
	if recorder := h.invoke(http.MethodPatch, repository+"?updateMask=description,bogus", map[string]any{"description": "after"}); recorder.Code != 400 {
		t.Fatalf("bad-mask patch status = %d", recorder.Code)
	}
	if body := h.invoke(http.MethodGet, repository, nil).Body.String(); !strings.Contains(body, `"description":"before"`) {
		t.Fatalf("a rejected patch changed the repository: %s", body)
	}
}

// TestConcurrentRepositoryRequests races create, get, list, patch, and delete
// on one repository; -race fails it if a handler touches a record unlocked.
func TestConcurrentRepositoryRequests(t *testing.T) {
	h := newHarness(t)
	repository := testBase + "/" + testRepo
	providertest.Hammer(t, h.gate, testToken, 8, 20, func(int) []providertest.Request {
		return []providertest.Request{
			{Method: http.MethodPost, Path: testBase + "?repositoryId=" + testRepo, Body: map[string]any{"format": "DOCKER"}, Want: []int{200, 409}},
			{Method: http.MethodGet, Path: repository, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: testBase, Want: []int{200}},
			{Method: http.MethodPatch, Path: repository, Body: map[string]any{"description": "worker", "labels": map[string]string{"worker": "w"}}, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: repository + ":getIamPolicy", Want: []int{200, 404}},
			{Method: http.MethodDelete, Path: repository, Want: []int{200, 404}},
		}
	})
}
