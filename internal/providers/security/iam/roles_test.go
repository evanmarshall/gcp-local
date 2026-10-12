package iam

import (
	"encoding/json"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
)

const (
	testRoles    = "/v1/projects/" + testProject + "/roles"
	testRoleID   = "appReader"
	testRole     = testRoles + "/" + testRoleID
	testRoleName = "projects/" + testProject + "/roles/" + testRoleID
	testReader   = "serviceAccount:reader@" + testProject + ".iam.gserviceaccount.com"
	testOrg      = "//cloudresourcemanager.googleapis.com/organizations/4242"
	accountGet   = "iam.serviceAccounts.get"
	accountList  = "iam.serviceAccounts.list"
)

// roleJSON is a Role response.
type roleJSON struct {
	Name                string   `json:"name"`
	Title               string   `json:"title"`
	Description         string   `json:"description"`
	IncludedPermissions []string `json:"includedPermissions"`
	Stage               string   `json:"stage"`
	ETag                string   `json:"etag"`
	Deleted             bool     `json:"deleted"`
}

// roleListJSON is a ListRoles response.
type roleListJSON struct {
	Roles         []roleJSON `json:"roles"`
	NextPageToken string     `json:"nextPageToken"`
}

// roleHarness is a harness whose actor administers roles and accounts, with
// a fixed clock it can advance.
func roleHarness(t *testing.T) (*harness, *time.Time) {
	t.Helper()
	h := newHarness(t)
	now := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	h.provider.clock = func() time.Time { return now }
	h.grantOnProject(iam.Binding{Role: "roles/iam.roleAdmin", Members: []string{testActor}})
	return h, &now
}

// decodeInto reads a JSON response body into a value of type T.
func decodeInto[T any](t *testing.T, body []byte) T {
	t.Helper()
	var value T
	if err := json.Unmarshal(body, &value); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	return value
}

// role issues one request that must succeed and decodes the role.
func (h *harness) role(verb, path string, body any) roleJSON {
	h.t.Helper()
	return decodeInto[roleJSON](h.t, h.expect(verb, path, body, 200).Body.Bytes())
}

// roleList lists roles and decodes the page.
func (h *harness) roleList(path string) roleListJSON {
	h.t.Helper()
	return decodeInto[roleListJSON](h.t, h.expect(http.MethodGet, path, nil, 200).Body.Bytes())
}

// errorJSON is a GCP error body.
type errorJSON struct {
	Error struct {
		Message string `json:"message"`
	} `json:"error"`
}

// wantError requires one HTTP status and GCP error message.
func (h *harness) wantError(verb, path string, body any, code int, message string) {
	h.t.Helper()
	got := decodeInto[errorJSON](h.t, h.expect(verb, path, body, code).Body.Bytes())
	if got.Error.Message != message {
		h.t.Fatalf("%s %s message = %q, want %q", verb, path, got.Error.Message, message)
	}
}

// expectAs issues a GET as another principal, checks its status, and returns
// the body.
func (h *harness) expectAs(token, path string, want int) []byte {
	h.t.Helper()
	recorder := h.invokeAs(token, http.MethodGet, path, nil)
	if recorder.Code != want {
		h.t.Fatalf("%s GET %s status = %d, want %d: %s", token, path, recorder.Code, want, recorder.Body.String())
	}
	return recorder.Body.Bytes()
}

// setPolicy is a setIamPolicy body with one binding.
func setPolicy(role string, members ...string) map[string]any {
	return map[string]any{"policy": map[string]any{"bindings": []map[string]any{{"role": role, "members": members}}}}
}

func createRole(permissions ...string) map[string]any {
	return map[string]any{"roleId": testRoleID, "role": map[string]any{
		"title": "App reader", "description": "Reads accounts", "includedPermissions": permissions, "stage": "GA",
	}}
}

func TestCustomRoleCreateReadList(t *testing.T) {
	h, _ := roleHarness(t)
	created := h.role(http.MethodPost, testRoles, createRole(accountGet, accountList, accountGet))
	if created.Name != testRoleName || created.Stage != "GA" || created.Deleted ||
		!slices.Equal(created.IncludedPermissions, []string{accountGet, accountList}) {
		t.Fatalf("created role = %+v", created)
	}
	h.wantError(http.MethodPost, testRoles, createRole(accountGet), 409, "A role named appReader in projects/demo-sa already exists.")
	if basic := h.roleList(testRoles); len(basic.Roles) != 1 || basic.Roles[0].IncludedPermissions != nil {
		t.Fatalf("basic list = %+v", basic)
	}
	if full := h.roleList(testRoles + "?view=FULL"); len(full.Roles) != 1 || len(full.Roles[0].IncludedPermissions) != 2 {
		t.Fatalf("full list = %+v", full)
	}
	h.wantError(http.MethodGet, testRoles+"/missing", nil, 404, "The role named projects/demo-sa/roles/missing was not found.")
}

func TestCustomRolePatch(t *testing.T) {
	h, _ := roleHarness(t)
	created := h.role(http.MethodPost, testRoles, createRole(accountGet))
	h.wantError(http.MethodPatch, testRole+"?updateMask=title", map[string]any{"title": "x", "etag": "BwZdrMztIT0="}, 409,
		"There were concurrent policy changes. Please retry the whole read-modify-write with exponential backoff.")
	patched := h.role(http.MethodPatch, testRole+"?updateMask=includedPermissions", map[string]any{
		"includedPermissions": []string{accountList}, "etag": created.ETag})
	if patched.Title != "App reader" || patched.ETag == created.ETag || !slices.Equal(patched.IncludedPermissions, []string{accountList}) {
		t.Fatalf("masked patch = %+v", patched)
	}
	// Without a mask every field is replaced, as GCP does.
	if replaced := h.role(http.MethodPatch, testRole, map[string]any{"title": "t2"}); replaced.Description != "" ||
		replaced.IncludedPermissions != nil || replaced.Stage != "" {
		t.Fatalf("unmasked patch = %+v", replaced)
	}
}

func TestCustomRoleDeleteUndelete(t *testing.T) {
	h, _ := roleHarness(t)
	h.expect(http.MethodPost, testRoles, createRole(accountGet), 200)
	h.wantError(http.MethodPost, testRole+":undelete", map[string]any{}, 400, "A role that is not deleted cannot be undeleted.")
	if deleted := h.role(http.MethodDelete, testRole, nil); !deleted.Deleted {
		t.Fatalf("deleted role = %+v", deleted)
	}
	h.wantError(http.MethodDelete, testRole, nil, 400, "You can't delete role_id (projects/demo-sa/roles/appReader) as it is already deleted.")
	h.wantError(http.MethodPatch, testRole+"?updateMask=title", map[string]any{"title": "x"}, 400,
		"You can't update a role (projects/demo-sa/roles/appReader) which is in a deleted state.")
	h.wantError(http.MethodPost, testRoles, createRole(accountGet), 400,
		"You can't create a role with role_id (appReader) where there is an existing role with that role_id in a deleted state.")
	if read := h.role(http.MethodGet, testRole, nil); !read.Deleted {
		t.Fatalf("deleted role read = %+v", read)
	}
	if listed := h.roleList(testRoles); len(listed.Roles) != 0 {
		t.Fatalf("list without showDeleted = %+v", listed)
	}
	if listed := h.roleList(testRoles + "?showDeleted=true"); len(listed.Roles) != 1 || !listed.Roles[0].Deleted {
		t.Fatalf("list with showDeleted = %+v", listed)
	}
	if restored := h.role(http.MethodPost, testRole+":undelete", map[string]any{}); restored.Deleted {
		t.Fatalf("undeleted role = %+v", restored)
	}
}

// TestCustomRoleValidation covers GCP's recorded refusals of role IDs,
// permissions, stages, and query parameters.
func TestCustomRoleValidation(t *testing.T) {
	h, _ := roleHarness(t)
	bad := createRole(accountGet)
	bad["roleId"] = "a"
	h.wantError(http.MethodPost, testRoles, bad, 400, `The role_id "a" is invalid. It doesn't match pattern "[a-zA-Z0-9_\.]{3,64}". `+
		"The role_id must be 3 to 64 characters long and can be a mix of uppercase and lowercase English letters, digits, underscores and periods.")
	h.wantError(http.MethodPost, testRoles, createRole("storage.bogus.get"), 400, "Permission storage.bogus.get is not valid.")
	h.wantError(http.MethodPost, testRoles, createRole("stackdriver.projects.edit"), 400, "Permission stackdriver.projects.edit is not supported in custom roles.")
	// Testable only above a project, so not valid in a project role.
	h.wantError(http.MethodPost, testRoles, createRole("resourcemanager.projects.list"), 400, "Permission resourcemanager.projects.list is not valid.")
	staged := createRole(accountGet)
	staged["role"] = map[string]any{"title": "x", "stage": "WEIRD"}
	h.wantError(http.MethodPost, testRoles, staged, 400, `Invalid value at 'role.stage' (type.googleapis.com/google.iam.admin.v1.Role.RoleLaunchStage), "WEIRD"`)
	// A permission outside the pinned roles but testable on a project is valid.
	h.expect(http.MethodPost, testRoles, createRole("accessapproval.requests.approve"), 200)
	h.wantError(http.MethodGet, testRole+"?view=BASIC", nil, 400,
		`Invalid JSON payload received. Unknown name "view": Cannot bind query parameter. Field 'view' could not be found in request message.`)
	h.wantError(http.MethodGet, testRoles+"?view=ALL", nil, 400, `Invalid value at 'view' (type.googleapis.com/google.iam.admin.v1.RoleView), "ALL"`)
	h.wantError(http.MethodPatch, testRole+"?updateMask=name", map[string]any{}, 400, "Invalid update_mask path: name")
}

// TestCustomRoleGrants binds a custom role on an account and follows its
// grant through delete, undelete, and a permission change.
func TestCustomRoleGrants(t *testing.T) {
	h, _ := roleHarness(t)
	h.expect(http.MethodPost, testBase, map[string]any{"accountId": testAccount}, 200)
	account := testBase + "/" + testAccount
	h.expect(http.MethodPost, testRoles, createRole(accountGet), 200)
	h.expect(http.MethodPost, account+":setIamPolicy", setPolicy(testRoleName, testReader), 200)
	h.expectAs("reader", account, 200)
	h.expect(http.MethodDelete, testRole, nil, 200)
	h.expectAs("reader", account, 403)
	// The binding stays and writes back unchanged; a new member is refused.
	policy := decodeInto[map[string]any](t, h.expect(http.MethodPost, account+":getIamPolicy", map[string]any{}, 200).Body.Bytes())
	h.expect(http.MethodPost, account+":setIamPolicy", map[string]any{"policy": policy}, 200)
	h.wantError(http.MethodPost, account+":setIamPolicy", setPolicy(testRoleName, testReader, testActor), 400,
		"Cannot add new grants to deleted role (projects/demo-sa/roles/appReader).")
	h.expect(http.MethodPost, testRole+":undelete", map[string]any{}, 200)
	h.expectAs("reader", account, 200)
	h.expect(http.MethodPatch, testRole+"?updateMask=includedPermissions", map[string]any{"includedPermissions": []string{accountList}}, 200)
	h.expectAs("reader", account, 403)
	h.wantError(http.MethodPost, account+":setIamPolicy", setPolicy("projects/demo-sa/roles/missing", testReader), 400,
		"Role (projects/demo-sa/roles/missing) does not exist in the resource's hierarchy.")
	h.wantError(http.MethodPost, account+":setIamPolicy", setPolicy("projects/demo-sa/roles/a", testReader), 400,
		`The role name must be in the form "roles/{role}", "organizations/{organization_id}/roles/{role}", or "projects/{project_id}/roles/{role}".`)
	// A role in a project that doesn't exist reads as a denial, as in GCP.
	h.wantError(http.MethodPost, account+":setIamPolicy", setPolicy("projects/elsewhere/roles/missing", testReader), 403,
		"Permission denied on resource (or it may not exist).")
}

// TestCustomRoleDeletionTimeline follows GCP's documented timeline: undelete
// for 7 days, then the role is gone but its ID stays reserved for up to 44
// days, after which a new role with the ID starts without the old bindings.
func TestCustomRoleDeletionTimeline(t *testing.T) {
	h, now := roleHarness(t)
	h.expect(http.MethodPost, testRoles, createRole(accountGet), 200)
	h.grantOnProject(iam.Binding{Role: testRoleName, Members: []string{testReader}})
	h.expect(http.MethodDelete, testRole, nil, 200)
	*now = now.Add(8 * 24 * time.Hour)
	h.wantError(http.MethodGet, testRole, nil, 404, "The role named projects/demo-sa/roles/appReader was not found.")
	h.expect(http.MethodPost, testRole+":undelete", map[string]any{}, 404)
	h.expect(http.MethodPost, testRoles, createRole(accountGet), 400)
	if listed := h.roleList(testRoles + "?showDeleted=true"); len(listed.Roles) != 0 {
		t.Fatalf("purged role listed: %+v", listed)
	}
	*now = now.Add(40 * 24 * time.Hour)
	h.expect(http.MethodPost, testRoles, createRole(accountGet), 200)
	policy, err := h.engine.GetPolicy("//cloudresourcemanager.googleapis.com/projects/" + testProject)
	if err != nil || roleMembers(policy, testRoleName) != nil {
		t.Fatalf("recreated role inherited %v (%v)", roleMembers(policy, testRoleName), err)
	}
	h.expectAs("reader", testBase, 403)
}

// TestOrganizationRoles models organization roles locally: they need
// iam.roles.* on the organization, grant on descendants, and refuse
// permissions the project catalog can't vouch for.
func TestOrganizationRoles(t *testing.T) {
	h, _ := roleHarness(t)
	child := "//cloudresourcemanager.googleapis.com/projects/org-child"
	for _, resource := range []state.Resource{
		{Name: testOrg, Service: "resourcemanager", Kind: "organization", Scope: state.Scope{Location: state.Global}},
		{Name: child, Service: "resourcemanager", Kind: "project", Parent: testOrg, Scope: state.Scope{Project: "org-child", Location: state.Global}},
	} {
		if err := h.index.Put("resourcemanager", resource); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := h.engine.SetPolicy(child, iam.Policy{Bindings: []iam.Binding{{Role: "roles/iam.serviceAccountAdmin", Members: []string{testActor}}}}); err != nil {
		t.Fatal(err)
	}
	orgRoles := "/v1/organizations/4242/roles"
	h.expect(http.MethodPost, orgRoles, createRole(accountGet), 403)
	if _, err := h.engine.SetPolicy(testOrg, iam.Policy{Bindings: []iam.Binding{{Role: "roles/iam.roleAdmin", Members: []string{testActor}}}}); err != nil {
		t.Fatal(err)
	}
	h.wantError(http.MethodPost, orgRoles, createRole("resourcemanager.projects.list"), 501,
		"Permission resourcemanager.projects.list is not testable on a project; organization-level custom role support is not recorded")
	h.expect(http.MethodPost, orgRoles, createRole(accountGet), 200)
	h.expect(http.MethodGet, "/v1/roles?parent=organizations/4242", nil, 200)
	accounts := "/v1/projects/org-child/serviceAccounts"
	h.expect(http.MethodPost, accounts, map[string]any{"accountId": testAccount}, 200)
	account := accounts + "/" + testAccount + "@org-child.iam.gserviceaccount.com"
	h.expect(http.MethodPost, account+":setIamPolicy", setPolicy("organizations/4242/roles/"+testRoleID, testReader), 200)
	h.expectAs("reader", account, 200)
	h.expect(http.MethodDelete, orgRoles+"/"+testRoleID, nil, 200)
	h.expectAs("reader", account, 403)
	// A project role can't be granted outside its project.
	h.expect(http.MethodPost, testRoles, createRole(accountGet), 200)
	h.wantError(http.MethodPost, account+":setIamPolicy", setPolicy(testRoleName, testReader), 400,
		"Role (projects/demo-sa/roles/appReader) does not exist in the resource's hierarchy.")
}

// TestPredefinedRoles reads a pinned snapshot without any permission.
func TestPredefinedRoles(t *testing.T) {
	h := newHarness(t)
	role := decodeInto[roleJSON](t, h.expectAs("nobody", "/v1/roles/iam.roleViewer", 200))
	if role.Name != "roles/iam.roleViewer" || role.ETag != "AA==" || role.Stage != "GA" || len(role.IncludedPermissions) == 0 {
		t.Fatalf("predefined role = %+v", role)
	}
	h.wantError(http.MethodGet, "/v1/roles/owner", nil, 501, "Predefined role roles/owner has no pinned snapshot")
}

// TestPredefinedRoleList pages through the pinned snapshots in name order.
func TestPredefinedRoleList(t *testing.T) {
	h := newHarness(t)
	first := decodeInto[roleListJSON](t, h.expectAs("nobody", "/v1/roles?pageSize=2", 200))
	if len(first.Roles) != 2 || first.Roles[0].IncludedPermissions != nil || first.NextPageToken == "" {
		t.Fatalf("first page = %+v", first)
	}
	second := decodeInto[roleListJSON](t, h.expectAs("nobody", "/v1/roles?view=FULL&pageSize=2&pageToken="+first.NextPageToken, 200))
	if len(second.Roles) != 2 || second.Roles[0].IncludedPermissions == nil || second.Roles[0].Name <= first.Roles[1].Name {
		t.Fatalf("second page = %+v", second)
	}
}
