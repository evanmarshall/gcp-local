package resourcemanager

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
)

const (
	testProject  = "demo-rm"
	otherProject = "other-rm"
	testToken    = "actor"
	adminToken   = "admin"
	readerToken  = "reader"
	testBase     = "/v1/projects/" + testProject
	testActor    = "serviceAccount:actor@" + testProject + ".iam.gserviceaccount.com"
	testAdmin    = "serviceAccount:admin@" + testProject + ".iam.gserviceaccount.com"
	testReader   = "serviceAccount:reader@" + testProject + ".iam.gserviceaccount.com"
	testOrg      = "organizations/100"
	seededFolder = "folders/200"
)

// harness drives the provider through the gateway. The hierarchy is one
// organization holding a seeded folder and two projects; admin holds the
// folder, mover, and organization admin roles on the organization, actor
// project IAM admin on demo-rm, and reader nothing.
type harness struct {
	t      *testing.T
	gate   *gateway.Gateway
	engine *iam.Engine
	index  *state.Index
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	registry := gateway.NewRegistry()
	index := state.NewIndex()
	engine, err := iam.New(index)
	if err != nil {
		t.Fatalf("build the iam engine: %v", err)
	}
	global := state.Scope{Location: state.Global}
	for _, resource := range []state.Resource{
		{Name: namePrefix + testOrg, Service: service, Kind: kindOrganization, Scope: global},
		{Name: namePrefix + seededFolder, Service: service, Kind: kindFolder, Parent: namePrefix + testOrg, Scope: global},
		{Name: namePrefix + "projects/" + testProject, Service: service, Kind: kindProject, Number: "123456",
			Parent: namePrefix + testOrg, Scope: state.Scope{Project: testProject, Location: state.Global}},
		{Name: namePrefix + "projects/" + otherProject, Service: service, Kind: kindProject, Number: "654321",
			Parent: namePrefix + testOrg, Scope: state.Scope{Project: otherProject, Location: state.Global}},
	} {
		if err := index.Put(service, resource); err != nil {
			t.Fatalf("seed %s: %v", resource.Name, err)
		}
	}
	h := &harness{t: t, engine: engine, index: index}
	h.grant(namePrefix+"projects/"+testProject, "roles/resourcemanager.projectIamAdmin", testActor)
	h.grant(namePrefix+testOrg, "roles/resourcemanager.folderAdmin", testAdmin)
	h.grant(namePrefix+testOrg, "roles/resourcemanager.projectMover", testAdmin)
	h.grant(namePrefix+testOrg, "roles/resourcemanager.organizationAdmin", testAdmin)
	if _, err := New(context.Background(), registry, backend.NewManager(time.Second, time.Second), index, engine); err != nil {
		t.Fatalf("build the provider: %v", err)
	}
	h.gate = gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		if token == "anonymous" {
			return gateway.Principal{State: gateway.Anonymous}, nil
		}
		return gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:" + token + "@" + testProject + ".iam.gserviceaccount.com", Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})
	return h
}

// grant adds one binding to a node's policy.
func (h *harness) grant(name, role, member string) {
	h.t.Helper()
	policy, err := h.engine.GetPolicy(name)
	if err != nil {
		h.t.Fatal(err)
	}
	policy.Bindings = append(policy.Bindings, iam.Binding{Role: role, Members: []string{member}})
	if _, err := h.engine.SetPolicy(name, policy); err != nil {
		h.t.Fatalf("grant %s on %s: %v", role, name, err)
	}
}

// do issues one REST request as token.
func (h *harness) do(token, method, path string, body any) *httptest.ResponseRecorder {
	h.t.Helper()
	var encoded []byte
	if body != nil {
		var err error
		if encoded, err = json.Marshal(body); err != nil {
			h.t.Fatalf("encode the request body: %v", err)
		}
	}
	request, _ := http.NewRequestWithContext(context.Background(), method, path, bytes.NewReader(encoded))
	if encoded != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	h.gate.ServeHTTP(recorder, request)
	return recorder
}

// expect issues a request, requires the status, and decodes the body.
func (h *harness) expect(token, method, path string, body any, want int) map[string]any {
	h.t.Helper()
	recorder := h.do(token, method, path, body)
	if recorder.Code != want {
		h.t.Fatalf("%s %s as %s = %d, want %d: %s", method, path, token, recorder.Code, want, recorder.Body.String())
	}
	decoded := map[string]any{}
	if recorder.Body.Len() > 0 {
		if err := json.Unmarshal(recorder.Body.Bytes(), &decoded); err != nil {
			h.t.Fatalf("decode %s: %v", recorder.Body.String(), err)
		}
	}
	return decoded
}

// post issues one REST POST as the project IAM admin.
func (h *harness) post(path string, body any) *httptest.ResponseRecorder {
	h.t.Helper()
	return h.do(testToken, http.MethodPost, path, body)
}

// wait polls an operation as admin until it is done and returns it.
func (h *harness) wait(operation map[string]any) map[string]any {
	h.t.Helper()
	name, _ := operation["name"].(string)
	if !strings.HasPrefix(name, "operations/") {
		h.t.Fatalf("operation name = %v", operation)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		current := h.expect(adminToken, http.MethodGet, "/v3/"+name, nil, 200)
		if current["done"] == true {
			return current
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("operation %s never finished: %v", name, current)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// response returns a finished operation's response, failing on an error.
func (h *harness) response(operation map[string]any) map[string]any {
	h.t.Helper()
	done := h.wait(operation)
	response, ok := done["response"].(map[string]any)
	if !ok {
		h.t.Fatalf("operation failed: %v", done)
	}
	return response
}

// createFolder creates a folder as admin and returns it.
func (h *harness) createFolder(parent, displayName string) map[string]any {
	h.t.Helper()
	operation := h.expect(adminToken, http.MethodPost, "/v3/folders", map[string]any{"parent": parent, "displayName": displayName}, 200)
	if operation["done"] != false {
		h.t.Fatalf("create returned a done operation: %v", operation)
	}
	return h.response(operation)
}

func TestProjectPolicyRoundTrip(t *testing.T) {
	h := newHarness(t)
	recorder := h.post(testBase+":getIamPolicy", map[string]any{})
	if recorder.Code != 200 {
		t.Fatalf("getIamPolicy status = %d", recorder.Code)
	}
	recorder = h.post(testBase+":setIamPolicy", map[string]any{
		"policy": map[string]any{
			"bindings": []any{map[string]any{
				"role":    "roles/resourcemanager.projectIamAdmin",
				"members": []any{testActor},
			}},
		},
		"updateMask": "bindings,etag,auditConfigs",
	})
	if recorder.Code != 200 {
		t.Fatalf("setIamPolicy status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.post(testBase+":getIamPolicy", map[string]any{})
	if recorder.Code != 200 {
		t.Fatalf("post-set getIamPolicy status = %d", recorder.Code)
	}
	recorder = h.post("/v1/projects/bad_upper:setIamPolicy", map[string]any{})
	if recorder.Code != 400 {
		t.Fatalf("invalid project status = %d", recorder.Code)
	}
	recorder = h.post("/v1/projects/missing:getIamPolicy", map[string]any{})
	if recorder.Code != 404 {
		t.Fatalf("missing project status = %d", recorder.Code)
	}
	recorder = h.post(testBase+":setIamPolicy", map[string]any{"policy": map[string]any{"bogus": 1}})
	if recorder.Code != 400 || !strings.Contains(recorder.Body.String(), `Unknown name \"bogus\" at 'policy'`) {
		t.Fatalf("unknown policy field = %d %s", recorder.Code, recorder.Body.String())
	}
}

// policyClient reads and writes the test project's v1 policy as the test actor.
type policyClient struct{ h *harness }

func (c policyClient) get(options any) map[string]any {
	body := map[string]any{}
	if options != nil {
		body["options"] = options
	}
	return c.h.expect(testToken, http.MethodPost, testBase+":getIamPolicy", body, 200)
}

// set sends policy with the current etag (unless it names one) and mask.
func (c policyClient) set(policy map[string]any, mask string, code int) map[string]any {
	if _, named := policy["etag"]; !named {
		policy["etag"] = c.get(nil)["etag"]
	}
	body := map[string]any{"policy": policy}
	if mask != "" {
		body["updateMask"] = mask
	}
	return c.h.expect(testToken, http.MethodPost, testBase+":setIamPolicy", body, code)
}

// Bindings the policy tests write.
var (
	adminBinding  = map[string]any{"role": "roles/resourcemanager.projectIamAdmin", "members": []any{testActor}}
	viewerBinding = map[string]any{"role": "roles/iam.serviceAccountViewer", "members": []any{testReader},
		"condition": map[string]any{"title": "t", "expression": `request.time < timestamp("2100-01-01T00:00:00Z")`}}
	schedulerAudit = []any{map[string]any{"service": "cloudscheduler.googleapis.com", "auditLogConfigs": []any{map[string]any{"logType": "DATA_READ"}}}}
)

// Project policy versions and conditional bindings (recorded: project-iam-policy-versions).
func TestProjectPolicyVersions(t *testing.T) {
	c := policyClient{newHarness(t)}
	c.h.expect(testToken, http.MethodPost, testBase+":getIamPolicy", map[string]any{"options": map[string]any{"requestedPolicyVersion": 7}}, 400)
	if version := c.get(map[string]any{"requestedPolicyVersion": 2})["version"]; version != float64(1) {
		t.Fatalf("version 2 read = %v", version)
	}
	if written := c.set(map[string]any{"version": 3, "bindings": []any{adminBinding, viewerBinding}}, "", 200); written["version"] != float64(3) {
		t.Fatalf("conditional set = %v", written)
	}
	if role := at(t, c.get(map[string]any{"requestedPolicyVersion": 3}), "bindings", 1, "role"); role != "roles/iam.serviceAccountViewer" {
		t.Fatalf("v3 role = %v", role)
	}
	for _, options := range []any{nil, map[string]any{"requestedPolicyVersion": 1}} {
		read := c.get(options)
		role, _ := at(t, read, "bindings", 1, "role").(string)
		prefix := "roles/iam.serviceAccountViewer_withcond_"
		if read["version"] != float64(1) || !strings.HasPrefix(role, prefix) || len(role) != len(prefix)+20 {
			t.Fatalf("v1 read = %v", read)
		}
	}
	c.set(map[string]any{"version": 1, "bindings": []any{adminBinding}}, "", 400)
	c.set(map[string]any{"auditConfigs": schedulerAudit}, "auditConfigs", 400)
	c.set(map[string]any{"version": 3, "bindings": []any{adminBinding}}, "", 200)
}

// Project policy update masks and audit configs (recorded: project-iam-policy-versions).
func TestProjectPolicyMasksAndAuditConfigs(t *testing.T) {
	c := policyClient{newHarness(t)}
	stale := c.set(map[string]any{"version": 1, "bindings": []any{adminBinding}}, "", 200)["etag"]
	added := c.set(map[string]any{"auditConfigs": schedulerAudit}, "auditConfigs", 200)
	if bindings, ok := added["bindings"].([]any); !ok || len(bindings) != 1 || added["auditConfigs"] == nil {
		t.Fatalf("audit mask = %v", added)
	}
	for mask, policy := range map[string]map[string]any{
		"":         {"version": 1, "bindings": []any{adminBinding}},
		"bindings": {"version": 1, "bindings": []any{adminBinding}, "auditConfigs": []any{}},
	} {
		if kept := c.set(policy, mask, 200); kept["auditConfigs"] == nil {
			t.Fatalf("audit configs dropped by mask %q: %v", mask, kept)
		}
	}
	if kept := c.set(map[string]any{"version": 1, "bindings": []any{adminBinding}, "auditConfigs": []any{}}, "", 200); kept["auditConfigs"] == nil {
		t.Fatalf("default mask applied audit configs: %v", kept)
	}
	c.set(map[string]any{"etag": stale, "auditConfigs": []any{}}, "auditConfigs", 409)
	c.set(map[string]any{"version": 1}, "version", 200)
	c.set(map[string]any{}, "bogus", 400)
	if cleared := c.set(map[string]any{"auditConfigs": []any{}}, "auditConfigs", 200); cleared["auditConfigs"] != nil {
		t.Fatalf("audit configs kept: %v", cleared)
	}
	if emptied, _ := c.set(map[string]any{}, "bindings", 200)["bindings"].([]any); len(emptied) != 0 {
		t.Fatalf("bindings mask without bindings kept %v", emptied)
	}
}

// at follows a path through decoded JSON: a string indexes an object and an
// int an array. Any other shape fails the test.
func at(t *testing.T, value any, path ...any) any {
	t.Helper()
	for _, step := range path {
		switch key := step.(type) {
		case string:
			members, ok := value.(map[string]any)
			if !ok {
				t.Fatalf("%v has no field %q", value, key)
			}
			value = members[key]
		case int:
			array, ok := value.([]any)
			if !ok || key >= len(array) {
				t.Fatalf("%v has no item %d", value, key)
			}
			value = array[key]
		}
	}
	return value
}

// text is the string at path.
func text(t *testing.T, value any, path ...any) string {
	t.Helper()
	found, ok := at(t, value, path...).(string)
	if !ok {
		t.Fatalf("%v at %v is not a string", value, path)
	}
	return found
}

// object is the JSON object at path.
func object(t *testing.T, value any, path ...any) map[string]any {
	t.Helper()
	found, ok := at(t, value, path...).(map[string]any)
	if !ok {
		t.Fatalf("%v at %v is not an object", value, path)
	}
	return found
}

// count is the length of the array at path; an absent field counts 0.
func count(t *testing.T, value any, path ...any) int {
	t.Helper()
	found := at(t, value, path...)
	if found == nil {
		return 0
	}
	array, ok := found.([]any)
	if !ok {
		t.Fatalf("%v at %v is not an array", value, path)
	}
	return len(array)
}

// fields requires each listed field of body to have its value; nil means
// absent.
func fields(t *testing.T, label string, body map[string]any, want map[string]any) {
	t.Helper()
	for key, value := range want {
		if body[key] != value {
			t.Fatalf("%s: %s = %v, want %v in %v", label, key, body[key], value, body)
		}
	}
}

// TestFolderLifecycle creates, reads, lists, searches, and renames a
// folder, each mutation through an operation that is not done when returned.
func TestFolderLifecycle(t *testing.T) {
	h := newHarness(t)
	created := h.createFolder(testOrg, "Team A")
	name := text(t, created, "name")
	fields(t, "create", created, map[string]any{"parent": testOrg, "state": stateActive, "@type": typePrefix + "Folder"})
	if !strings.HasPrefix(name, "folders/") || created["etag"] == nil {
		t.Fatalf("created = %v", created)
	}
	fields(t, "get", h.expect(adminToken, http.MethodGet, "/v3/"+name, nil, 200), map[string]any{"displayName": "Team A"})
	if listed := count(t, h.expect(adminToken, http.MethodGet, "/v3/folders?parent="+testOrg, nil, 200), "folders"); listed != 2 {
		t.Fatalf("listed %d folders", listed)
	}
	search := h.expect(adminToken, http.MethodGet, "/v3/folders:search?query=displayName%3D%22Team%20A%22", nil, 200)
	if found := count(t, search, "folders"); found != 1 {
		t.Fatalf("search found %d folders", found)
	}
	renamed := h.response(h.expect(adminToken, http.MethodPatch, "/v3/"+name+"?updateMask=display_name",
		map[string]any{"displayName": "Team B"}, 200))
	fields(t, "rename", renamed, map[string]any{"displayName": "Team B"})
	fields(t, "seeded", h.expect(adminToken, http.MethodGet, "/v3/"+seededFolder, nil, 200), map[string]any{"displayName": "200", "parent": testOrg})
}

// TestFolderMoveDeleteUndelete moves a folder, soft-deletes it, hides it
// from lists unless showDeleted, refuses changes to it, and undeletes it.
func TestFolderMoveDeleteUndelete(t *testing.T) {
	h := newHarness(t)
	name := text(t, h.createFolder(testOrg, "Team A"), "name")
	moved := h.response(h.expect(adminToken, http.MethodPost, "/v3/"+name+":move", map[string]any{"destinationParent": seededFolder}, 200))
	fields(t, "move", moved, map[string]any{"parent": seededFolder})
	deleted := h.response(h.expect(adminToken, http.MethodDelete, "/v3/"+name, nil, 200))
	fields(t, "delete", deleted, map[string]any{"state": stateDeleteRequested})
	if deleted["deleteTime"] == nil {
		t.Fatalf("delete = %v", deleted)
	}
	if listed := count(t, h.expect(adminToken, http.MethodGet, "/v3/folders?parent="+seededFolder, nil, 200), "folders"); listed != 0 {
		t.Fatalf("a deleted folder is listed")
	}
	if listed := count(t, h.expect(adminToken, http.MethodGet, "/v3/folders?parent="+seededFolder+"&showDeleted=true", nil, 200), "folders"); listed != 1 {
		t.Fatalf("showDeleted lists %d folders", listed)
	}
	h.expect(adminToken, http.MethodPatch, "/v3/"+name, map[string]any{"displayName": "Team C"}, 400)
	restored := h.response(h.expect(adminToken, http.MethodPost, "/v3/"+name+":undelete", map[string]any{}, 200))
	fields(t, "undelete", restored, map[string]any{"state": stateActive, "deleteTime": nil})
}

// TestFolderRules refuses duplicate sibling names, depth beyond ten levels,
// deleting a non-empty folder, and moving a folder under itself; strict
// decoding refuses unknown fields and unmodeled tags.
func TestFolderRules(t *testing.T) {
	h := newHarness(t)
	h.createFolder(testOrg, "dup")
	duplicate := h.wait(h.expect(adminToken, http.MethodPost, "/v3/folders", map[string]any{"parent": testOrg, "displayName": "dup"}, 200))
	fields(t, "duplicate", object(t, duplicate, "error"), map[string]any{"code": float64(9), "message": "Requested entity already exists"})
	top := text(t, h.createFolder(testOrg, "level a"), "name")
	parent := top
	for level := 1; level < maxFolderDepth; level++ {
		parent = text(t, h.createFolder(parent, "level "+string(rune('a'+level))), "name")
	}
	tooDeep := h.wait(h.expect(adminToken, http.MethodPost, "/v3/folders", map[string]any{"parent": parent, "displayName": "eleven"}, 200))
	if message := text(t, tooDeep, "error", "message"); message != "Folder reached maximum depth." {
		t.Fatalf("depth = %v", tooDeep)
	}
	notEmpty := h.wait(h.expect(adminToken, http.MethodDelete, "/v3/"+top, nil, 200))
	if code := at(t, notEmpty, "error", "code"); code != float64(9) {
		t.Fatalf("non-empty delete = %v", notEmpty)
	}
	h.expect(adminToken, http.MethodPost, "/v3/"+top+":move", map[string]any{"destinationParent": parent}, 400)
	bad := h.expect(adminToken, http.MethodPost, "/v3/folders", map[string]any{"parent": testOrg, "displayName": "x", "bogus": 1}, 400)
	if message := text(t, bad, "error", "message"); message != `Invalid JSON payload received. Unknown name "bogus" at 'folder': Cannot find field.` {
		t.Fatalf("unknown field = %v", bad)
	}
	tags := h.expect(adminToken, http.MethodPost, "/v3/folders", map[string]any{"parent": testOrg, "displayName": "x", "tags": map[string]any{"tagKeys/1": "tagValues/2"}}, 501)
	if message := text(t, tags, "error", "message"); message != "Resource Manager coverage gap: tags is not modeled" {
		t.Fatalf("tags = %v", tags)
	}
	h.expect(adminToken, http.MethodPost, "/v3/folders", map[string]any{"parent": testOrg, "displayName": "-bad"}, 400)
	h.expect(adminToken, http.MethodPatch, "/v3/"+top+"?updateMask=parent", map[string]any{"displayName": "x"}, 400)
}

// TestErrorInfoDenials returns the recorded ErrorInfo 403s naming the
// permission and the addressed node, the same for a node that doesn't exist.
func TestErrorInfoDenials(t *testing.T) {
	h := newHarness(t)
	denied := h.expect(readerToken, http.MethodGet, "/v3/"+testOrg, nil, 403)
	if message := text(t, denied, "error", "message"); message != "Permission 'resourcemanager.organizations.get' denied on resource '//cloudresourcemanager.googleapis.com/organizations/100' (or it may not exist)." {
		t.Fatalf("org denial = %v", denied)
	}
	detail := object(t, denied, "error", "details", 0)
	fields(t, "details", detail, map[string]any{"reason": "IAM_PERMISSION_DENIED", "domain": "cloudresourcemanager.googleapis.com"})
	fields(t, "metadata", object(t, detail, "metadata"), map[string]any{"permission": "resourcemanager.organizations.get", "resource": testOrg})
	missing := h.expect(adminToken, http.MethodGet, "/v3/folders/1", nil, 403)
	if !strings.HasPrefix(text(t, missing, "error", "message"), "Permission 'resourcemanager.folders.get' denied on resource '//cloudresourcemanager.googleapis.com/folders/1'") {
		t.Fatalf("missing folder = %v", missing)
	}
	for _, request := range []struct {
		method, path string
		body         any
	}{
		{http.MethodGet, "/v3/folders?parent=" + testOrg, nil},
		{http.MethodPost, "/v3/folders", map[string]any{"parent": testOrg, "displayName": "x"}},
		{http.MethodPost, "/v3/" + testOrg + ":getIamPolicy", map[string]any{}},
		{http.MethodGet, "/v3/projects/" + testProject, nil},
		{http.MethodGet, "/v3/projects/no-such-project", nil},
	} {
		if count(t, h.expect(readerToken, request.method, request.path, request.body, 403), "error", "details") != 1 {
			t.Fatalf("%s %s denial has no details", request.method, request.path)
		}
	}
}

// TestPlainRefusals returns the recorded 403 without details for list, move,
// and a parentless create, 401 for an anonymous caller, and GCP's 400s.
func TestPlainRefusals(t *testing.T) {
	h := newHarness(t)
	for _, request := range []struct {
		method, path string
		body         any
	}{
		{http.MethodPost, "/v3/folders", map[string]any{"displayName": "x"}},
		{http.MethodGet, "/v3/folders", nil},
		{http.MethodGet, "/v3/projects?parent=" + testOrg, nil},
		{http.MethodPost, "/v3/projects/" + testProject + ":move", map[string]any{"destinationParent": seededFolder}},
		{http.MethodPost, "/v3/folders/1:move", map[string]any{"destinationParent": testOrg}},
	} {
		body := object(t, h.expect(readerToken, request.method, request.path, request.body, 403), "error")
		fields(t, request.path, body, map[string]any{"message": "The caller does not have permission", "details": nil})
	}
	h.expect("anonymous", http.MethodGet, "/v3/"+testOrg, nil, 401)
	h.expect("anonymous", http.MethodGet, "/v3/projects:search", nil, 401)
	h.expect(readerToken, http.MethodGet, "/v3/projects", nil, 400)
	h.expect(readerToken, http.MethodGet, "/v3/projects?parent=projects/"+testProject, nil, 400)
	h.expect(adminToken, http.MethodPost, "/v3/folders/1:move", map[string]any{"destinationParent": "organizations/{ORG}"}, 400)
	h.expect(adminToken, http.MethodGet, "/v3/projects:search?pageToken=bogus", nil, 400)
	negative := h.expect(adminToken, http.MethodGet, "/v3/projects:search?pageSize=-1", nil, 400)
	if message := text(t, negative, "error", "message"); message != "field [pageSize] has issue [invalid page size]" {
		t.Fatalf("negative page size = %v", negative)
	}
	query := h.expect(adminToken, http.MethodGet, "/v3/organizations:search?query=domain:example.com", nil, 501)
	if message := text(t, query, "error", "message"); message != `Resource Manager coverage gap: search query term "domain:example.com"` {
		t.Fatalf("org query = %v", query)
	}
	h.expect(adminToken, http.MethodGet, "/v3/operations/operation-1", nil, 400)
}

// TestVisibility filters searches by get permission.
func TestVisibility(t *testing.T) {
	h := newHarness(t)
	if body := h.expect(readerToken, http.MethodGet, "/v3/organizations:search", nil, 200); len(body) != 0 {
		t.Fatalf("reader sees organizations: %v", body)
	}
	if found := count(t, h.expect(adminToken, http.MethodGet, "/v3/organizations:search", nil, 200), "organizations"); found != 1 {
		t.Fatalf("admin sees %d organizations", found)
	}
	projects := h.expect(testToken, http.MethodGet, "/v3/projects:search", nil, 200)
	fields(t, "actor projects", object(t, projects, "projects", 0), map[string]any{"parent": testOrg, "name": "projects/123456", "updateTime": nil})
	if found := count(t, projects, "projects"); found != 1 {
		t.Fatalf("actor sees %d projects", found)
	}
	if found := count(t, h.expect(adminToken, http.MethodGet, "/v3/projects:search?query=id:"+otherProject, nil, 200), "projects"); found != 1 {
		t.Fatalf("query by id found %d", found)
	}
	page := h.expect(adminToken, http.MethodGet, "/v3/projects?parent="+testOrg+"&pageSize=1", nil, 200)
	if count(t, page, "projects") != 1 || page["nextPageToken"] == nil {
		t.Fatalf("first page = %v", page)
	}
	v1 := h.expect(testToken, http.MethodGet, "/v1/projects/"+testProject, nil, 200)
	fields(t, "v1", v1, map[string]any{"projectNumber": "123456"})
	fields(t, "v1 parent", object(t, v1, "parent"), map[string]any{"type": kindOrganization, "id": "100"})
}

// TestPermissions answers testIamPermissions without requiring a
// permission: held permissions in order without repeats, none on a node
// that doesn't exist, and a wildcard refused.
func TestPermissions(t *testing.T) {
	h := newHarness(t)
	held := h.expect(testToken, http.MethodPost, "/v3/projects/"+testProject+":testIamPermissions", map[string]any{
		"permissions": []string{"resourcemanager.projects.get", "resourcemanager.projects.move", "resourcemanager.projects.get"}}, 200)
	if count(t, held, "permissions") != 1 || text(t, held, "permissions", 0) != "resourcemanager.projects.get" {
		t.Fatalf("held = %v", held)
	}
	if body := h.expect(readerToken, http.MethodPost, "/v3/"+testOrg+":testIamPermissions", map[string]any{"permissions": []string{"resourcemanager.folders.create"}}, 200); len(body) != 0 {
		t.Fatalf("reader holds = %v", body)
	}
	if body := h.expect(readerToken, http.MethodPost, "/v3/folders/1:testIamPermissions", map[string]any{"permissions": []string{"resourcemanager.folders.get"}}, 200); len(body) != 0 {
		t.Fatalf("missing folder holds = %v", body)
	}
	h.expect(readerToken, http.MethodPost, "/v3/"+testOrg+":testIamPermissions", map[string]any{"permissions": []string{"resourcemanager.*"}}, 400)
}

// TestInheritance grants through folder and organization policies: a
// project moved under a folder gains the folder's grants at once and loses
// them when moved away, and an operation is readable only by its starter.
func TestInheritance(t *testing.T) {
	h := newHarness(t)
	folder := text(t, h.createFolder(testOrg, "inherit"), "name")
	policy := h.expect(adminToken, http.MethodPost, "/v3/"+folder+":getIamPolicy", map[string]any{"options": map[string]any{"requestedPolicyVersion": 3}}, 200)
	h.expect(adminToken, http.MethodPost, "/v3/"+folder+":setIamPolicy", map[string]any{
		"policy": map[string]any{"etag": policy["etag"], "bindings": []any{map[string]any{
			"role": "roles/resourcemanager.folderViewer", "members": []any{testReader}}}},
		"updateMask": "bindings,etag,auditConfigs",
	}, 200)
	h.expect(readerToken, http.MethodGet, "/v3/"+folder, nil, 200)
	h.expect(readerToken, http.MethodGet, "/v3/projects/"+otherProject, nil, 403)
	operation := h.expect(adminToken, http.MethodPost, "/v3/projects/"+otherProject+":move", map[string]any{"destinationParent": folder}, 200)
	h.expect(readerToken, http.MethodGet, "/v3/"+text(t, operation, "name"), nil, 400)
	fields(t, "moved", h.response(operation), map[string]any{"parent": folder, "@type": typePrefix + "Project"})
	h.expect(readerToken, http.MethodGet, "/v3/projects/"+otherProject, nil, 200)
	h.expect(readerToken, http.MethodGet, "/v3/projects/654321", nil, 200)
	h.response(h.expect(adminToken, http.MethodPost, "/v3/projects/"+otherProject+":move", map[string]any{"destinationParent": testOrg}, 200))
	h.expect(readerToken, http.MethodGet, "/v3/projects/"+otherProject, nil, 403)
	h.grant(namePrefix+testOrg, "roles/resourcemanager.organizationViewer", testReader)
	h.expect(readerToken, http.MethodGet, "/v3/"+testOrg, nil, 200)
	if bindings := count(t, h.expect(adminToken, http.MethodPost, "/v3/"+testOrg+":getIamPolicy", map[string]any{}, 200), "bindings"); bindings != 4 {
		t.Fatalf("org policy has %d bindings", bindings)
	}
	h.expect(adminToken, http.MethodPost, "/v3/"+testOrg+":setIamPolicy", map[string]any{"policy": map[string]any{"etag": "stale", "bindings": []any{}}}, 409)
}
