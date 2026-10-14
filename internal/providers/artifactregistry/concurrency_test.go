package artifactregistry

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/providers/provider/providertest"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestPatchIsAtomic refuses a mask with one unmodeled path without applying
// the modeled one.
func TestPatchIsAtomic(t *testing.T) {
	h := newHarness(t)
	repository := testBase + "/" + testRepo
	if recorder := h.invoke(http.MethodPost, testBase+"?repositoryId="+testRepo, map[string]any{"format": "DOCKER", "description": "before"}); recorder.Code != 200 {
		t.Fatalf("create status = %d", recorder.Code)
	}
	if recorder := h.invoke(http.MethodPatch, repository+"?updateMask=description,cleanupPolicies", map[string]any{"description": "after"}); recorder.Code != 501 {
		t.Fatalf("unmodeled-mask patch status = %d", recorder.Code)
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
			{Method: http.MethodPatch, Path: repository + "?updateMask=description,labels", Body: map[string]any{"description": "worker", "labels": map[string]string{"worker": "w"}}, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: "/v1/projects/" + testProject + "/locations/us-east1/repositories", Want: []int{200}},
			{Method: http.MethodGet, Path: repository + ":getIamPolicy", Want: []int{200, 404}},
			{Method: http.MethodDelete, Path: repository, Want: []int{200, 404}},
		}
	})
}

// TestPolicyNeverWritesTheProjectPolicy covers a repository policy call
// authorized at the project, because the repository was missing when Nearest
// picked the target, that finds it created by the time it takes the lock. It
// must read NOT_FOUND and leave the project's policy alone.
func TestPolicyNeverWritesTheProjectPolicy(t *testing.T) {
	h := newHarness(t)
	h.create(testBase, testRepo)
	scope := state.Scope{Project: testProject, Location: testLocation}
	project := provider.ProjectResource(testProject)
	before, err := h.engine.GetPolicy(project)
	if err != nil {
		t.Fatal(err)
	}
	for _, method := range []string{methodReposSetIam, methodReposGetIam} {
		call := gateway.Call{Context: context.Background(), Method: method}
		request := &gateway.RESTRequest{Path: map[string]string{pathProject: testProject, pathLocation: testLocation, pathRepository: testRepo}, JSON: []byte(`{"policy":{}}`)}
		_, err = h.provider.execute(call, request, scope, iam.Target{Name: project})
		if status.Code(err) != codes.NotFound || status.Convert(err).Message() != "Requested entity was not found." {
			t.Fatalf("%s at the project = %v", method, err)
		}
	}
	if after, _ := h.engine.GetPolicy(project); len(after.Bindings) != len(before.Bindings) {
		t.Fatalf("the project policy changed: %+v", after)
	}
}
