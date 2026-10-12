package iam

import (
	"net/http"
	"strings"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider/providertest"
)

// TestPatchIsAtomic rejects a mask with one bad field without applying the
// good one.
func TestPatchIsAtomic(t *testing.T) {
	h := newHarness(t)
	account := testBase + "/" + testAccount
	h.expect(http.MethodPost, testBase, map[string]any{"accountId": testAccount, "displayName": "before"}, 200)
	h.expect(http.MethodPatch, account, map[string]any{
		"serviceAccount": map[string]any{"displayName": "after"},
		"updateMask":     "displayName,bogus",
	}, 400)
	if body := h.expect(http.MethodGet, account, nil, 200).Body.String(); !strings.Contains(body, `"displayName":"before"`) {
		t.Fatalf("a rejected patch changed the account: %s", body)
	}
}

// TestConcurrentAccountRequests races create, get, list, patch, delete, key
// create/list/get/delete, and signBlob on one account; -race fails it if a
// handler touches a record unlocked.
func TestConcurrentAccountRequests(t *testing.T) {
	h := newHarness(t)
	account := testBase + "/" + testAccount
	if _, err := h.engine.SetPolicy("//cloudresourcemanager.googleapis.com/projects/"+testProject, iam.Policy{Bindings: []iam.Binding{
		{Role: "roles/iam.serviceAccountAdmin", Members: []string{testActor}},
		{Role: "roles/iam.serviceAccountKeyAdmin", Members: []string{testActor}},
		{Role: "roles/iam.serviceAccountTokenCreator", Members: []string{testActor}}}}); err != nil {
		t.Fatal(err)
	}
	providertest.Hammer(t, h.gate, testToken, 8, 5, func(int) []providertest.Request {
		return []providertest.Request{
			{Method: http.MethodPost, Path: testBase, Body: map[string]any{"accountId": testAccount}, Want: []int{200, 409}},
			{Method: http.MethodGet, Path: account, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: testBase, Want: []int{200}},
			{Method: http.MethodPatch, Path: account, Body: map[string]any{
				"serviceAccount": map[string]any{"displayName": "worker", "description": "racing"},
				"updateMask":     "displayName,description",
			}, Want: []int{200, 404}},
			{Method: http.MethodPost, Path: account + "/keys", Body: map[string]any{}, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: account + "/keys", Want: []int{200, 404}},
			{Method: http.MethodGet, Path: account + "/keys/0000000000000000000000000000000000000000", Want: []int{404}},
			{Method: http.MethodDelete, Path: account + "/keys/0000000000000000000000000000000000000000", Want: []int{404}},
			{Method: http.MethodPost, Path: account + ":signBlob", Body: map[string]any{"payload": "YQ=="}, Want: []int{200, 404}},
			{Method: http.MethodPost, Path: account + ":getIamPolicy", Body: map[string]any{}, Want: []int{200, 404}},
			{Method: http.MethodDelete, Path: account, Want: []int{200, 404}},
		}
	})
}
