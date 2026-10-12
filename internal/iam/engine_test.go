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
	"google.golang.org/genproto/googleapis/rpc/errdetails"
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

const testCloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"

// testObjectGet is the read permission exercised by the engine tests.
const testObjectGet = "storage.objects.get"

// testViewerRole is the object-viewer role used by the condition tests.
const testViewerRole = "roles/storage.objectViewer"

// testBadCondition is a syntactically invalid CEL expression.
const testBadCondition = `{"expression":"== bad syntax ("}`

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
	return gateway.Call{Context: context.Background(), Method: method, Project: testProject, Location: state.Global, TraceID: "same-client-trace", Principal: gateway.Principal{State: gateway.Authenticated, Subject: subject, Project: project, Scopes: []string{testCloudPlatformScope}}}
}
func object() iam.Target {
	return iam.Target{Name: storage.BucketResource("bucket-a") + "/objects/object.txt", Parent: storage.BucketResource("bucket-a")}
}

// grantedBy authorizes c on object() and requires the grant to come from policy.
func grantedBy(t *testing.T, engine *iam.Engine, c gateway.Call, policy, label string) iam.Decision {
	t.Helper()
	decision, err := engine.Authorize(c, object(), nil)
	if err != nil || decision.Checks[0].GrantPolicy != policy {
		t.Fatalf("%s: %+v %v", label, decision, err)
	}
	return decision
}

// denied requires c on object() to fail with PermissionDenied.
func denied(t *testing.T, engine *iam.Engine, c gateway.Call, label string) {
	t.Helper()
	if _, err := engine.Authorize(c, object(), nil); status.Code(err) != codes.PermissionDenied {
		t.Fatal(label, err)
	}
}

func TestInheritanceRevocationAndCrossProjectGrants(t *testing.T) {
	engine, _ := setup(t)
	read := call(testObjectGet, allowed, testProject)
	if decision := grantedBy(t, engine, read, object().Parent, "bucket grant"); decision.Outcome != "allow" {
		t.Fatalf("%+v", decision)
	}
	if _, err := engine.SetPolicy(object().Parent, iam.Policy{}); err != nil {
		t.Fatal(err)
	}
	denied(t, engine, read, "bucket revocation:")
	grantedBy(t, engine, call(testObjectGet, fixture, testProject), projectA, "project inheritance")
	foreign := call(testObjectGet, peer, projectBName)
	denied(t, engine, foreign, "caller-project role leaked into resource project:")
	set(t, engine, object().Parent, testViewerRole, peer)
	if result := grantedBy(t, engine, foreign, object().Parent, "cross-project grant"); result.Principal.Project != projectBName {
		t.Fatalf("%+v", result)
	}
	_, _ = engine.SetPolicy(object().Parent, iam.Policy{})
	denied(t, engine, foreign, "revocation did not deny:")
	set(t, engine, folder, testViewerRole, allowed)
	grantedBy(t, engine, read, folder, "folder inheritance")
	_, _ = engine.SetPolicy(folder, iam.Policy{})
	set(t, engine, org, testViewerRole, allowed)
	grantedBy(t, engine, read, org, "org inheritance")
}

// mutationCounter authorizes one create call and counts the plans that would
// have reached mutation.
type mutationCounter struct {
	engine *iam.Engine
	create gateway.Call
	writes atomic.Int32
}

func (m *mutationCounter) perform(features map[string]bool) (iam.Decision, error) {
	decision, err := m.engine.Authorize(m.create, object(), features)
	if err != nil {
		return decision, fmt.Errorf("authorize: %w", err)
	}
	m.writes.Add(1)
	return decision, nil
}

// requireOnlyDeleteDenied requires an overwrite plan whose create check passed
// and whose additional delete check denied the request.
func requireOnlyDeleteDenied(t *testing.T, decision iam.Decision, err error) {
	t.Helper()
	if status.Code(err) != codes.PermissionDenied || len(decision.Checks) != 2 {
		t.Fatalf("%+v %v", decision, err)
	}
	if !decision.Checks[0].Allowed || decision.Checks[1].Allowed || decision.Checks[1].Permission != "storage.objects.delete" {
		t.Fatalf("%+v", decision.Checks)
	}
}

// requirePreflight requires an incomplete baseline plan for c on object().
func requirePreflight(t *testing.T, engine *iam.Engine, c gateway.Call) {
	t.Helper()
	preflight, err := engine.Baseline(c, object())
	if err != nil || preflight.Complete || preflight.Outcome != "preflight" {
		t.Fatalf("%+v %v", preflight, err)
	}
}

func TestReplacementCannotOmitBaselineOrUnresolvedState(t *testing.T) {
	engine, _ := setup(t)
	set(t, engine, object().Parent, "roles/storage.objectCreator", allowed)
	create := call("storage.objects.insert", allowed, testProject)
	requirePreflight(t, engine, create)
	counter := &mutationCounter{engine: engine, create: create}
	if result, err := counter.perform(map[string]bool{conditionObjectExists: false}); err != nil || !result.Complete || len(result.Checks) != 1 {
		t.Fatalf("%+v %v", result, err)
	}
	refused, err := counter.perform(map[string]bool{conditionObjectExists: true})
	requireOnlyDeleteDenied(t, refused, err)
	for _, features := range []map[string]bool{nil, {"unknown": true}} {
		if _, err := counter.perform(features); status.Code(err) != codes.Unimplemented {
			t.Fatal("unresolved state was treated as absent:", features, err)
		}
	}
	if counter.writes.Load() != 1 {
		t.Fatal("denied or incomplete plan reached mutation")
	}
	set(t, engine, object().Parent, testViewerRole, allowed)
	refused, err = counter.perform(map[string]bool{conditionObjectExists: true})
	if status.Code(err) != codes.PermissionDenied || refused.Checks[0].Allowed {
		t.Fatal("additional check displaced baseline create permission")
	}
}
func TestUnsupportedPoliciesAndMethodsCannotBecomeAllows(t *testing.T) {
	engine, _ := setup(t)
	read := call(testObjectGet, fixture, testProject)
	if err := engine.MarkUnsupported(org, []string{"deny", "principal-access-boundary"}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Authorize(read, object(), nil); status.Code(err) != codes.Unimplemented {
		t.Fatal("known grant bypassed unknown ancestor policy:", err)
	}
	_ = engine.MarkUnsupported(org, nil)
	// Recorded (storage-iam-conditions): a conditional binding below version 3
	// and an uncompilable expression are 400 INVALID_ARGUMENT in GCP.
	for _, refused := range []struct {
		policy iam.Policy
		code   codes.Code
	}{
		{iam.Policy{Version: 2}, codes.Unimplemented},
		{iam.Policy{Bindings: []iam.Binding{{Role: testViewerRole, Members: []string{allowed}, Condition: json.RawMessage(testBadCondition)}}}, codes.InvalidArgument},
		{iam.Policy{Version: 3, Bindings: []iam.Binding{{Role: testViewerRole, Members: []string{allowed}, Condition: json.RawMessage(testBadCondition)}}}, codes.InvalidArgument},
		{iam.Policy{Bindings: []iam.Binding{{Role: "roles/unknown", Members: []string{allowed}}}}, codes.Unimplemented},
		{iam.Policy{Bindings: []iam.Binding{{Role: testViewerRole, Members: []string{"group:unknown@local"}}}}, codes.Unimplemented},
	} {
		if _, err := engine.SetPolicy(object().Parent, refused.policy); status.Code(err) != refused.code {
			t.Fatal(err)
		}
	}
	read.Method = "storage.objects.unknown"
	if _, err := engine.Authorize(read, object(), nil); status.Code(err) != codes.Unimplemented {
		t.Fatal(err)
	}
}

// A token's scopes limit the methods it can call before IAM is consulted, as
// in GCP: a scope the method does not accept is 403
// ACCESS_TOKEN_SCOPE_INSUFFICIENT even when IAM would allow the call.
func TestOAuthScopesLimitPlansBeforeIAM(t *testing.T) {
	engine, _ := setup(t)
	const auth = "https://www.googleapis.com/auth/"
	insert := map[string]bool{"object_exists": false}
	for _, tc := range []struct {
		scopes   []string
		method   string
		features map[string]bool
		allowed  bool
	}{
		{[]string{auth + "devstorage.read_only"}, testObjectGet, nil, true},
		{[]string{auth + "devstorage.read_only"}, "storage.objects.insert", insert, false},
		{[]string{auth + "devstorage.read_write"}, "storage.objects.insert", insert, true},
		// storage v1 discovery: objects.patch accepts full_control, not read_write.
		{[]string{auth + "devstorage.read_write"}, "storage.objects.patch", nil, false},
		{[]string{auth + "devstorage.full_control"}, "storage.objects.patch", nil, true},
		{[]string{auth + "cloud-platform.read-only"}, testObjectGet, nil, true},
		{[]string{auth + "cloud-platform.read-only"}, "storage.objects.insert", insert, false},
		{[]string{auth + "cloudkms", auth + "pubsub"}, testObjectGet, nil, false},
		{[]string{"openid", auth + "userinfo.email"}, testObjectGet, nil, false},
		{[]string{auth + "not-a-scope"}, testObjectGet, nil, false},
		{nil, testObjectGet, nil, false},
		{[]string{auth + "cloudkms", testCloudPlatformScope}, "storage.objects.insert", insert, true},
	} {
		request := call(tc.method, allowed, testProject)
		request.Principal.Scopes = tc.scopes
		decision, err := engine.Authorize(request, object(), tc.features)
		if tc.allowed {
			if err != nil {
				t.Fatalf("%v %s: %v", tc.scopes, tc.method, err)
			}
			continue
		}
		requireScopeInsufficient(t, decision, err)
	}
}

// requireScopeInsufficient requires GCP's insufficient-scope refusal, recorded
// as a denial before any policy check.
func requireScopeInsufficient(t *testing.T, decision iam.Decision, err error) {
	t.Helper()
	s := status.Convert(err)
	if s.Code() != codes.PermissionDenied || s.Message() != "Request had insufficient authentication scopes." || decision.Outcome != "deny" || len(decision.Checks) != 0 {
		t.Fatalf("%v %+v", err, decision)
	}
	if info, ok := s.Details()[0].(*errdetails.ErrorInfo); !ok || info.GetReason() != "ACCESS_TOKEN_SCOPE_INSUFFICIENT" || info.GetDomain() != "googleapis.com" {
		t.Fatalf("details %v", s.Details())
	}
}
func TestAnonymousPublicBindingsAndInvalidCredentials(t *testing.T) {
	engine, _ := setup(t)
	read := call(testObjectGet, "forged-admin", testProject)
	read.Principal.State = gateway.Anonymous
	if result, err := engine.Authorize(read, object(), nil); status.Code(err) != codes.Unauthenticated || result.Principal.Subject != "" {
		t.Fatal("anonymous caller became administrator")
	}
	set(t, engine, object().Parent, testViewerRole, "allUsers")
	if _, err := engine.Authorize(read, object(), nil); err != nil {
		t.Fatal("explicit public read failed:", err)
	}
	// A target that prevents public access ignores the same binding.
	prevented := object()
	prevented.PublicPrevented = true
	if _, err := engine.Authorize(read, prevented, nil); status.Code(err) != codes.Unauthenticated {
		t.Fatal("public binding granted access on a public-prevented target:", err)
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
	read := call(testObjectGet, fixture, testProject)
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
	if _, err := engine.Authorize(call(testObjectGet, fixture, testProject), object(), nil); err != nil {
		t.Fatal("reset retained unsupported marker or erased project policy:", err)
	}
}

// fetchDecision reads one decision through the diagnostics HTTP endpoint.
func fetchDecision(t *testing.T, engine *iam.Engine, id string) iam.Decision {
	t.Helper()
	server := httptest.NewServer(engine)
	defer server.Close()
	response, err := getWithContext(server.URL + "/_local/iam/decisions/" + id)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result iam.Decision
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestDecisionDiagnosticsAreImmutableAndDoNotUseTraceAsID(t *testing.T) {
	engine, _ := setup(t)
	read := call(testObjectGet, peer, projectBName)
	first, _ := engine.Authorize(read, object(), nil)
	second, _ := engine.Authorize(read, object(), nil)
	if first.ID == second.ID || first.ID == read.TraceID {
		t.Fatal("client trace overwrote decision ID")
	}
	result := fetchDecision(t, engine, first.ID)
	if result.Outcome != "deny" || result.Checks[0].Permission != testObjectGet || len(result.Checks[0].Policies) != 4 || result.Principal.Subject != peer {
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
	first, _ := engine.Authorize(call(testObjectGet, allowed, testProject), object(), nil)
	var group sync.WaitGroup
	for range 20 {
		group.Go(func() {
			for range 60 {
				_, _ = engine.Authorize(call(testObjectGet, allowed, testProject), object(), nil)
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

func TestConditionalBindingsFailClosedAndGrant(t *testing.T) {
	engine, _ := setup(t)
	read := call(testObjectGet, fixture, testProject)
	// A true expression grants; a false expression fails closed with a trace.
	conditional := func(expression string) iam.Policy {
		condition, _ := json.Marshal(map[string]string{"title": "test", "expression": expression}) //nolint:errchkjson // plain string map
		return iam.Policy{Version: 3, Bindings: []iam.Binding{{
			Role: testViewerRole, Members: []string{fixture},
			Condition: condition,
		}}}
	}
	// Remove the unconditional ancestor grant so only the conditional
	// binding decides.
	if _, err := engine.SetPolicy(projectA, iam.Policy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.SetPolicy(object().Parent, conditional(`"storage.objects.get" == "storage.objects.get"`)); err != nil {
		t.Fatal(err)
	}
	decision, err := engine.Authorize(read, object(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !decision.Checks[0].Allowed {
		t.Fatalf("matching condition denied: %+v", decision.Checks[0])
	}
	if len(decision.Checks[0].Conditions) == 0 {
		t.Fatalf("no condition trace: %+v", decision.Checks[0])
	}
	if _, err := engine.SetPolicy(object().Parent, conditional("1 == 2")); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Authorize(read, object(), nil); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("unmatched condition did not deny: %v", err)
	}
}

// TestConditionAttributesAndTimeMatchRecording mirrors storage-iam-conditions:
// a resource.name prefix scopes a grant, the conditional binding need not be
// first in the policy, and request.time compares as a timestamp.
// A condition that compiles but errors at evaluation time grants nothing.
func TestConditionEvaluationErrorFailsClosed(t *testing.T) {
	engine, _ := setup(t)
	if _, err := engine.SetPolicy(projectA, iam.Policy{}); err != nil {
		t.Fatal(err)
	}
	condition, _ := json.Marshal(map[string]string{"title": "bad", "expression": `int(resource.name) == 1`}) //nolint:errchkjson // plain string map
	policy := iam.Policy{Version: 3, Bindings: []iam.Binding{{Role: testViewerRole, Members: []string{fixture}, Condition: condition}}}
	if _, err := engine.SetPolicy(object().Parent, policy); err != nil {
		t.Fatal(err)
	}
	decision, err := engine.Authorize(call(testObjectGet, fixture, testProject), object(), nil)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("erroring condition did not deny: %v", err)
	}
	if len(decision.Checks) == 0 || len(decision.Checks[0].Conditions) == 0 || !strings.Contains(decision.Checks[0].Conditions[0], "could not be evaluated") {
		t.Fatalf("no evaluation-error trace: %+v", decision.Checks)
	}
}

func TestConditionAttributesAndTimeMatchRecording(t *testing.T) {
	engine, _ := setup(t)
	read := call(testObjectGet, fixture, testProject)
	if _, err := engine.SetPolicy(projectA, iam.Policy{}); err != nil {
		t.Fatal(err)
	}
	grant := func(expression string) {
		t.Helper()
		condition, _ := json.Marshal(map[string]string{"title": "t", "expression": expression}) //nolint:errchkjson // plain string map
		if _, err := engine.SetPolicy(object().Parent, iam.Policy{Version: 3, Bindings: []iam.Binding{
			{Role: testViewerRole, Members: []string{allowed}},
			{Role: testViewerRole, Members: []string{fixture}, Condition: condition},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	named := func(name string) iam.Target {
		target := object()
		target.Attributes = iam.ResourceAttributes{Name: "projects/_/buckets/bucket-a/objects/" + name,
			Type: "storage.googleapis.com/Object", Service: "storage.googleapis.com"}
		return target
	}
	grant(`resource.name.startsWith("projects/_/buckets/bucket-a/objects/public-")`)
	if _, err := engine.Authorize(read, named("public-a.txt"), nil); err != nil {
		t.Fatalf("prefix grant denied: %v", err)
	}
	if _, err := engine.Authorize(read, named("private-b.txt"), nil); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("outside the prefix was allowed: %v", err)
	}
	grant(`request.time < timestamp("2000-01-01T00:00:00Z")`)
	if _, err := engine.Authorize(read, named("public-a.txt"), nil); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expired condition was allowed: %v", err)
	}
}

func TestDenyRulesOverrideAllowBindings(t *testing.T) {
	engine, _ := setup(t)
	read := call(testObjectGet, fixture, testProject)
	deny := iam.Policy{Version: 3, DenyRules: []iam.DenyRule{{
		DeniedPrincipals:  []string{fixture},
		DeniedPermissions: []string{testObjectGet},
	}}}
	if _, err := engine.SetPolicy(object().Parent, deny); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Authorize(read, object(), nil); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("deny did not override the allow: %v", err)
	}
	// An exception principal for the same rule restores the allow.
	deny.DenyRules[0].ExceptionPrincipals = []string{fixture}
	if _, err := engine.SetPolicy(object().Parent, deny); err != nil {
		t.Fatal(err)
	}
	decision, err := engine.Authorize(read, object(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !decision.Checks[0].Allowed || len(decision.Checks[0].Conditions) == 0 {
		t.Fatalf("exception did not restore the allow: %+v", decision.Checks[0])
	}
}

func TestOrgPolicyAndVPCSCSignalDistinctFromIAMDenial(t *testing.T) {
	engine, _ := setup(t)
	// A granted user read plus a dry-run org-policy violation and a perimeter.
	if err := engine.MarkOrgPolicy(projectA, "constraints/storage.publicAccessPrevention", true); err != nil {
		t.Fatal(err)
	}
	if err := engine.MarkPerimeter("local-perimeter", []string{projectA, object().Parent}); err != nil {
		t.Fatal(err)
	}
	read := call("storage.objects.get", fixture, testProject)
	decision, err := engine.Authorize(read, object(), nil)
	if err != nil {
		t.Fatal(err)
	}
	// The perimeter covers both the bucket and the project, so the bucket
	// contributes one vpc-sc signal and the project contributes the org-policy
	// violation plus a second perimeter signal.
	if len(decision.Signals) != 3 {
		t.Fatalf("signals = %+v", decision.Signals)
	}
	byFamily := map[string]iam.Signal{}
	for _, signal := range decision.Signals {
		byFamily[signal.Family] = signal
	}
	if byFamily["org-policy"].Verdict != "dry-run violation" {
		t.Fatalf("org-policy signal = %+v", byFamily["org-policy"])
	}
	if byFamily["vpc-sc"].Detail != "local-perimeter" {
		t.Fatalf("vpc-sc signal = %+v", byFamily["vpc-sc"])
	}
	// The IAM check itself stays an allow: signals never replace IAM verdicts.
	if decision.Checks[0].Allowed == false {
		t.Fatalf("IAM check = %+v", decision.Checks[0])
	}
	// An unregistered constraint resource is refused.
	if err := engine.MarkOrgPolicy("missing", "constraints/x", true); status.Code(err) != codes.NotFound {
		t.Fatalf("missing resource = %v", err)
	}
}

// accessPermission is both the method and the permission in the plan export test.
const accessPermission = "secretmanager.versions.access"

// TestPlansExportsCopiesForOnePrefix keeps the drift export read-only.
func TestPlansExportsCopiesForOnePrefix(t *testing.T) {
	engine, err := iam.New(state.NewIndex())
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Register(accessPermission, iam.Rule{Baseline: []string{accessPermission}}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Register("storage.objects.get", iam.Rule{Baseline: []string{"storage.objects.get"}}); err != nil {
		t.Fatal(err)
	}
	plans := engine.Plans("secretmanager.")
	if len(plans) != 1 || plans[accessPermission].Baseline[0] != accessPermission {
		t.Fatalf("unexpected plans: %v", plans)
	}
	plans[accessPermission].Baseline[0] = "changed"
	if engine.Plans("secretmanager.")[accessPermission].Baseline[0] != accessPermission {
		t.Fatal("Plans returned shared rule storage")
	}
}
