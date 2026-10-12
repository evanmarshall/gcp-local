package iam

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/iam"
)

// A service account recreated with a deleted account's email is a separate
// identity with a new unique ID: it starts with an empty policy of its own and
// inherits no grant made to the deleted account, which reads back as
// deleted:serviceAccount:EMAIL?uid=ID
// (https://cloud.google.com/iam/docs/service-accounts-delete-undelete).
func TestRecreatedAccountInheritsNoGrants(t *testing.T) {
	h := newHarness(t)
	email := accountEmail(testProject, testAccount)
	member := "serviceAccount:" + email
	project := "//cloudresourcemanager.googleapis.com/projects/" + testProject
	first := h.expect(http.MethodPost, testBase, map[string]any{"accountId": testAccount}, 200)
	var created struct {
		UniqueID string `json:"uniqueId"`
	}
	if json.Unmarshal(first.Body.Bytes(), &created) != nil || created.UniqueID == "" {
		t.Fatalf("create body %s", first.Body.String())
	}
	h.expect(http.MethodPost, testBase+"/"+testAccount+":setIamPolicy", map[string]any{"policy": map[string]any{
		"bindings": []any{map[string]any{"role": "roles/iam.serviceAccountUser", "members": []any{"serviceAccount:deployer@" + testProject + ".iam.gserviceaccount.com"}}},
	}}, 200)
	h.grantOnProject(iam.Binding{Role: "roles/iam.serviceAccountViewer", Members: []string{member}})
	h.expect(http.MethodDelete, testBase+"/"+testAccount, nil, 200)
	h.expect(http.MethodPost, testBase, map[string]any{"accountId": testAccount}, 200)
	own := h.expect(http.MethodPost, testBase+"/"+testAccount+":getIamPolicy", map[string]any{}, 200)
	if strings.Contains(own.Body.String(), "deployer") {
		t.Fatalf("recreated account kept the deleted account's policy: %s", own.Body.String())
	}
	inherited, err := h.engine.GetPolicy(project)
	if err != nil {
		t.Fatal(err)
	}
	if members := roleMembers(inherited, "roles/iam.serviceAccountViewer"); !slices.Equal(members, []string{"deleted:" + member + "?uid=" + created.UniqueID}) {
		t.Fatalf("project grant to the deleted account = %v", members)
	}
	// The retired member reads back and writes back unchanged.
	if _, err := h.engine.SetPolicy(project, inherited); err != nil {
		t.Fatalf("read-modify-write with a deleted member: %v", err)
	}
}

// grantOnProject adds one binding to the project policy.
func (h *harness) grantOnProject(binding iam.Binding) {
	h.t.Helper()
	project := "//cloudresourcemanager.googleapis.com/projects/" + testProject
	current, err := h.engine.GetPolicy(project)
	if err != nil {
		h.t.Fatal(err)
	}
	current.Bindings = append(current.Bindings, binding)
	if _, err := h.engine.SetPolicy(project, current); err != nil {
		h.t.Fatal(err)
	}
}

// roleMembers returns the members of one role's binding.
func roleMembers(policy iam.Policy, role string) []string {
	for _, binding := range policy.Bindings {
		if binding.Role == role {
			return binding.Members
		}
	}
	return nil
}
