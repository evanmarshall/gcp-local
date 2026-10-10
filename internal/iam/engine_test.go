package iam_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/storage"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// mustMarshal encodes a fixture value or fails the test.
func mustMarshal(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// getWithContext issues a context-aware GET against a local test server.
func getWithContext(url string) (*http.Response, error) {
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", url, err)
	}
	return response, nil
}

// mustWriteFile writes a fixture file or fails the test.
func mustWriteFile(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.WriteFile(path, content, 0600); err != nil {
		t.Fatal(err)
	}
}

const testProject = "project-a"
const projectA = "//cloudresourcemanager.googleapis.com/projects/project-a"
const projectB = "//cloudresourcemanager.googleapis.com/projects/project-b"
const org = "//cloudresourcemanager.googleapis.com/organizations/local-org"
const folder = "//cloudresourcemanager.googleapis.com/folders/local-folder"
const allowed = "serviceAccount:allowed@project-a.iam.gserviceaccount.com"
const fixture = "serviceAccount:fixture@project-a.iam.gserviceaccount.com"
const peer = "serviceAccount:peer@project-b.iam.gserviceaccount.com"

const (
	resourceManagerService = "resourcemanager"
	kindProject            = "project"
	projectBName           = "project-b"
	conditionObjectExists  = "object_exists"
	changedMarker          = "changed"
)

func setup(t *testing.T) (*iam.Engine, *state.Index) {
	t.Helper()
	index := state.NewIndex()
	resources := []state.Resource{
		{Name: org, Service: resourceManagerService, Kind: "organization", Scope: state.Scope{Location: state.Global}},
		{Name: folder, Service: resourceManagerService, Kind: "folder", Parent: org, Scope: state.Scope{Location: state.Global}},
		{Name: projectA, Service: resourceManagerService, Kind: kindProject, Parent: folder, Scope: state.Scope{Project: testProject, Location: state.Global}},
		{Name: projectB, Service: resourceManagerService, Kind: kindProject, Parent: org, Scope: state.Scope{Project: projectBName, Location: state.Global}},
	}
	for _, resource := range resources {
		if err := index.Put(resourceManagerService, resource); err != nil {
			t.Fatal(err)
		}
	}
	catalog := storage.NewCatalog(index)
	if err := catalog.PublishBucket(state.Scope{Project: testProject, Location: state.Global}, storage.Bucket{Name: "bucket-a", Location: "us-central1"}); err != nil {
		t.Fatal(err)
	}
	engine, err := iam.New(index)
	if err != nil {
		t.Fatal(err)
	}
	if err := storage.RegisterPermissions(engine); err != nil {
		t.Fatal(err)
	}
	set(t, engine, projectA, "roles/storage.admin", fixture)
	set(t, engine, projectB, "roles/storage.admin", peer)
	set(t, engine, storage.BucketResource("bucket-a"), "roles/storage.objectUser", allowed)
	return engine, index
}
func set(t *testing.T, e *iam.Engine, resource, role string, members ...string) iam.Policy {
	t.Helper()
	policy, err := e.SetPolicy(resource, iam.Policy{Bindings: []iam.Binding{{Role: role, Members: members}}})
	if err != nil {
		t.Fatal(err)
	}
	return policy
}
func call(method, subject, project string) gateway.Call {
	return gateway.Call{Context: context.Background(), Method: method, Project: testProject, Location: state.Global, TraceID: "same-client-trace", Principal: gateway.Principal{State: gateway.Authenticated, Subject: subject, Project: project, Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}}
}
func object() iam.Target {
	return iam.Target{Name: storage.BucketResource("bucket-a") + "/objects/object.txt", Parent: storage.BucketResource("bucket-a")}
}
func TestInheritanceRevocationAndCrossProjectGrants(t *testing.T) {
	engine, _ := setup(t)
	read := call("storage.objects.get", allowed, testProject)
	decision, err := engine.Authorize(read, object(), nil)
	if err != nil || decision.Outcome != "allow" || decision.Checks[0].GrantPolicy != object().Parent {
		t.Fatalf("%+v %v", decision, err)
	}
	if _, err := engine.SetPolicy(object().Parent, iam.Policy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Authorize(read, object(), nil); status.Code(err) != codes.PermissionDenied {
		t.Fatal(err)
	}
	inherited, err := engine.Authorize(call("storage.objects.get", fixture, testProject), object(), nil)
	if err != nil || inherited.Checks[0].GrantPolicy != projectA {
		t.Fatalf("%+v %v", inherited, err)
	}
	foreign := call("storage.objects.get", peer, projectBName)
	if _, err := engine.Authorize(foreign, object(), nil); status.Code(err) != codes.PermissionDenied {
		t.Fatal("caller-project role leaked into resource project:", err)
	}
	set(t, engine, object().Parent, "roles/storage.objectViewer", peer)
	if result, err := engine.Authorize(foreign, object(), nil); err != nil || result.Principal.Project != projectBName || result.Checks[0].GrantPolicy != object().Parent {
		t.Fatalf("%+v %v", result, err)
	}
	_, _ = engine.SetPolicy(object().Parent, iam.Policy{})
	if _, err := engine.Authorize(foreign, object(), nil); status.Code(err) != codes.PermissionDenied {
		t.Fatal("revocation did not deny:", err)
	}
	set(t, engine, folder, "roles/storage.objectViewer", allowed)
	if result, err := engine.Authorize(read, object(), nil); err != nil || result.Checks[0].GrantPolicy != folder {
		t.Fatalf("folder inheritance: %+v %v", result, err)
	}
	_, _ = engine.SetPolicy(folder, iam.Policy{})
	set(t, engine, org, "roles/storage.objectViewer", allowed)
	if result, err := engine.Authorize(read, object(), nil); err != nil || result.Checks[0].GrantPolicy != org {
		t.Fatalf("org inheritance: %+v %v", result, err)
	}
}
func TestReplacementCannotOmitBaselineOrUnresolvedState(t *testing.T) {
	engine, _ := setup(t)
	set(t, engine, object().Parent, "roles/storage.objectCreator", allowed)
	create := call("storage.objects.insert", allowed, testProject)
	preflight, err := engine.Baseline(create, object())
	if err != nil || preflight.Complete || preflight.Outcome != "preflight" {
		t.Fatalf("%+v %v", preflight, err)
	}
	var writes atomic.Int32
	perform := func(features map[string]bool) (iam.Decision, error) {
		decision, err := engine.Authorize(create, object(), features)
		if err == nil {
			writes.Add(1)
			return decision, nil
		}
		return decision, fmt.Errorf("authorize: %w", err)
	}
	if result, err := perform(map[string]bool{conditionObjectExists: false}); err != nil || !result.Complete || len(result.Checks) != 1 {
		t.Fatalf("%+v %v", result, err)
	}
	denied, err := perform(map[string]bool{conditionObjectExists: true})
	if status.Code(err) != codes.PermissionDenied || len(denied.Checks) != 2 || !denied.Checks[0].Allowed || denied.Checks[1].Allowed || denied.Checks[1].Permission != "storage.objects.delete" {
		t.Fatalf("%+v %v", denied, err)
	}
	if _, err := perform(nil); status.Code(err) != codes.Unimplemented {
		t.Fatal("unresolved state was treated as absent:", err)
	}
	if _, err := perform(map[string]bool{"unknown": true}); status.Code(err) != codes.Unimplemented {
		t.Fatal(err)
	}
	if writes.Load() != 1 {
		t.Fatal("denied or incomplete plan reached mutation")
	}
	set(t, engine, object().Parent, "roles/storage.objectViewer", allowed)
	denied, err = perform(map[string]bool{conditionObjectExists: true})
	if status.Code(err) != codes.PermissionDenied || denied.Checks[0].Allowed {
		t.Fatal("additional check displaced baseline create permission")
	}
}
func TestUnsupportedPoliciesAndMethodsCannotBecomeAllows(t *testing.T) {
	engine, _ := setup(t)
	read := call("storage.objects.get", fixture, testProject)
	if err := engine.MarkUnsupported(org, []string{"deny", "principal-access-boundary"}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Authorize(read, object(), nil); status.Code(err) != codes.Unimplemented {
		t.Fatal("known grant bypassed unknown ancestor policy:", err)
	}
	_ = engine.MarkUnsupported(org, nil)
	for _, policy := range []iam.Policy{
		{Version: 3}, {Bindings: []iam.Binding{{Role: "roles/storage.objectViewer", Members: []string{allowed}, Condition: json.RawMessage(`{"expression":"true"}`)}}},
		{Bindings: []iam.Binding{{Role: "roles/unknown", Members: []string{allowed}}}},
		{Bindings: []iam.Binding{{Role: "roles/storage.objectViewer", Members: []string{"group:unknown@local"}}}},
	} {
		if _, err := engine.SetPolicy(object().Parent, policy); status.Code(err) != codes.Unimplemented {
			t.Fatal(err)
		}
	}
	read.Method = "storage.objects.unknown"
	if _, err := engine.Authorize(read, object(), nil); status.Code(err) != codes.Unimplemented {
		t.Fatal(err)
	}
	read.Method = "storage.objects.get"
	read.Principal.Scopes = []string{"https://www.googleapis.com/auth/devstorage.read_only"}
	if _, err := engine.Authorize(read, object(), nil); status.Code(err) != codes.Unimplemented {
		t.Fatal("claimed unverified narrow scopes")
	}
}
func TestAnonymousPublicBindingsAndInvalidCredentials(t *testing.T) {
	engine, _ := setup(t)
	read := call("storage.objects.get", "forged-admin", testProject)
	read.Principal.State = gateway.Anonymous
	if result, err := engine.Authorize(read, object(), nil); status.Code(err) != codes.Unauthenticated || result.Principal.Subject != "" {
		t.Fatal("anonymous caller became administrator")
	}
	set(t, engine, object().Parent, "roles/storage.objectViewer", "allUsers")
	if _, err := engine.Authorize(read, object(), nil); err != nil {
		t.Fatal("explicit public read failed:", err)
	}
	for _, state := range []gateway.PrincipalState{gateway.Unknown, gateway.Expired} {
		read.Principal.State = state
		if _, err := engine.Authorize(read, object(), nil); status.Code(err) != codes.Unauthenticated {
			t.Fatal("invalid credential authenticated")
		}
	}
	read.Principal.State = gateway.Authenticated
	read.Principal.Subject = ""
	if _, err := engine.Authorize(read, object(), nil); status.Code(err) != codes.Unauthenticated {
		t.Fatal("unnamed authenticated caller accepted")
	}
}
func TestHierarchyGapsAndOwnershipMismatchFailClosed(t *testing.T) {
	engine, index := setup(t)
	read := call("storage.objects.get", fixture, testProject)
	target := object()
	target.Parent = projectB
	if _, err := engine.Authorize(read, target, nil); status.Code(err) != codes.Unimplemented {
		t.Fatal("object accepted unrelated anchor")
	}
	read.Project = projectBName
	if _, err := engine.Authorize(read, object(), nil); status.Code(err) != codes.Unimplemented {
		t.Fatal("caller resource project mismatch ignored")
	}
	read.Project = testProject
	_ = index.Remove(resourceManagerService, state.Scope{Location: state.Global}, folder)
	if _, err := engine.Authorize(read, object(), nil); status.Code(err) != codes.Unimplemented {
		t.Fatal("missing hierarchy silently ended inheritance")
	}
}
func TestPolicyETagsCopiesAndScopedCleanup(t *testing.T) {
	engine, index := setup(t)
	policy, err := engine.GetPolicy(object().Parent)
	if err != nil {
		t.Fatal(err)
	}
	original := policy.ETag
	policy.Bindings[0].Members[0] = peer
	stored, _ := engine.GetPolicy(object().Parent)
	if stored.Bindings[0].Members[0] != allowed {
		t.Fatal("caller mutated stored policy")
	}
	updated, err := engine.SetPolicy(object().Parent, policy)
	if err != nil || updated.ETag == original {
		t.Fatal(err)
	}
	if _, err := engine.SetPolicy(object().Parent, policy); status.Code(err) != codes.Aborted {
		t.Fatal("stale etag overwrote policy")
	}
	_ = engine.MarkUnsupported(object().Parent, []string{"vpc-service-controls"})
	index.RemoveScope("storage", state.Scope{Project: testProject, Location: state.Global})
	engine.ClearScope("storage", state.Scope{Project: testProject, Location: state.Global})
	catalog := storage.NewCatalog(index)
	_ = catalog.PublishBucket(state.Scope{Project: testProject, Location: state.Global}, storage.Bucket{Name: "bucket-a", Location: "us-central1"})
	fresh, _ := engine.GetPolicy(object().Parent)
	if len(fresh.Bindings) != 0 {
		t.Fatal("reset retained resource policy")
	}
	if _, err := engine.Authorize(call("storage.objects.get", fixture, testProject), object(), nil); err != nil {
		t.Fatal("reset retained unsupported marker or erased project policy:", err)
	}
}
func TestDecisionDiagnosticsAreImmutableAndDoNotUseTraceAsID(t *testing.T) {
	engine, _ := setup(t)
	read := call("storage.objects.get", peer, projectBName)
	first, _ := engine.Authorize(read, object(), nil)
	second, _ := engine.Authorize(read, object(), nil)
	if first.ID == second.ID || first.ID == read.TraceID {
		t.Fatal("client trace overwrote decision ID")
	}
	server := httptest.NewServer(engine)
	defer server.Close()
	response, err := getWithContext(server.URL + "/_local/iam/decisions/" + first.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result iam.Decision
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.Outcome != "deny" || result.Checks[0].Permission != "storage.objects.get" || len(result.Checks[0].Policies) != 4 || result.Principal.Subject != peer {
		t.Fatalf("%+v", result)
	}
	snapshot, _ := engine.Decision(first.ID)
	snapshot.Checks[0].Policies[0] = changedMarker
	snapshot.Principal.Scopes[0] = changedMarker
	again, _ := engine.Decision(first.ID)
	if again.Checks[0].Policies[0] == changedMarker || again.Principal.Scopes[0] == changedMarker {
		t.Fatal("diagnostics mutated audit record")
	}
}
func TestConcurrentPoliciesAndBoundedDecisionRetention(t *testing.T) {
	engine, _ := setup(t)
	first, _ := engine.Authorize(call("storage.objects.get", allowed, testProject), object(), nil)
	var group sync.WaitGroup
	for range 20 {
		group.Go(func() {
			for range 60 {
				_, _ = engine.Authorize(call("storage.objects.get", allowed, testProject), object(), nil)
			}
		})
	}
	group.Wait()
	if _, found := engine.Decision(first.ID); found {
		t.Fatal("decision cache grew beyond its limit")
	}
}
func TestBootstrapIsExplicitAndRejectsIncompleteHierarchy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "iam.json")
	config := iam.Config{Resources: []state.Resource{{Name: projectA, Kind: kindProject, Service: resourceManagerService, Scope: state.Scope{Project: testProject, Location: state.Global}}}, Policies: map[string]iam.Policy{projectA: {Bindings: []iam.Binding{{Role: "roles/storage.admin", Members: []string{fixture}}}}}}
	raw := mustMarshal(t, config)
	mustWriteFile(t, path, raw)
	engine, index, err := iam.Load(path)
	if err != nil || engine == nil || index == nil {
		t.Fatal(err)
	}
	config.Resources[0].Parent = folder
	raw = mustMarshal(t, config)
	mustWriteFile(t, path, raw)
	if _, _, err := iam.Load(path); err == nil || !strings.Contains(err.Error(), "incomplete") {
		t.Fatal("accepted missing hierarchy:", err)
	}
}
