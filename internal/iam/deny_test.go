package iam_test

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	iamv2 "cloud.google.com/go/iam/apiv2/iampb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/genproto/googleapis/type/expr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Deny-policy identifiers of the test principals.
const (
	publicPrincipals = "principalSet://goog/public:all"
	fixturePrincipal = "principal://iam.googleapis.com/projects/-/serviceAccounts/fixture@project-a.iam.gserviceaccount.com"
	v2ObjectGet      = "storage.googleapis.com/objects.get"
)

// condition encodes one titled CEL expression as a policy condition.
func condition(t *testing.T, expression string) json.RawMessage {
	t.Helper()
	return mustMarshal(t, map[string]string{"title": "t", "expression": expression})
}

// namedObject is object() with Storage's resource attributes for one object.
func namedObject(name string) iam.Target {
	target := object()
	target.Attributes = iam.ResourceAttributes{Name: "projects/_/buckets/bucket-a/objects/" + name,
		Type: "storage.googleapis.com/Object", Service: "storage.googleapis.com"}
	return target
}

func mustSet(t *testing.T, engine *iam.Engine, resource string, policy iam.Policy) {
	t.Helper()
	if _, err := engine.SetPolicy(resource, policy); err != nil {
		t.Fatal(err)
	}
}

// rule is one deny rule.
func rule(principals []string, permissions ...string) *iamv2.DenyRule {
	return &iamv2.DenyRule{DeniedPrincipals: principals, DeniedPermissions: permissions}
}

// putDeny stores node's deny policy id with rules.
func putDeny(t *testing.T, engine *iam.Engine, index *state.Index, node, id string, rules ...*iamv2.DenyRule) (*iamv2.Policy, error) {
	t.Helper()
	resource, ok := index.Get(node)
	if !ok {
		resource = state.Resource{Name: node}
	}
	policy := &iamv2.Policy{}
	for _, deny := range rules {
		policy.Rules = append(policy.Rules, &iamv2.PolicyRule{Kind: &iamv2.PolicyRule_DenyRule{DenyRule: deny}})
	}
	return engine.PutDenyPolicy(resource, id, policy, time.Now()) //nolint:wrapcheck // test helper
}

// mustDeny is putDeny that must succeed.
func mustDeny(t *testing.T, engine *iam.Engine, index *state.Index, node, id string, rules ...*iamv2.DenyRule) *iamv2.Policy {
	t.Helper()
	policy, err := putDeny(t, engine, index, node, id, rules...)
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

// A deny policy on an ancestor overrides an allow grant; the trace names the
// rule and policy, and deleting the policy restores the grant.
func TestDenyPolicyOverridesAllow(t *testing.T) {
	engine, index := setup(t)
	read := call(testObjectGet, fixture, testProject)
	created := mustDeny(t, engine, index, projectA, "block", rule([]string{fixturePrincipal}, v2ObjectGet))
	if created.GetName() != "policies/cloudresourcemanager.googleapis.com%2Fprojects%2Fproject-a/denypolicies/block" || created.GetKind() != "DenyPolicy" || created.GetUid() == "" {
		t.Fatalf("created: %v", created)
	}
	decision, err := engine.Authorize(read, object(), nil)
	if status.Code(err) != codes.PermissionDenied || decision.Checks[0].DeniedBy != "rule 0 of "+created.GetName() || len(decision.Checks[0].DenyPolicies) != 1 {
		t.Fatalf("deny did not override the allow: %+v %v", decision.Checks, err)
	}
	if _, ok := engine.DeleteDenyPolicy(projectA, "block"); !ok {
		t.Fatal("delete found no policy")
	}
	grantedBy(t, engine, read, projectA, "after delete")
	if got := engine.DenyPolicies(projectA); len(got) != 0 {
		t.Fatalf("listed after delete: %v", got)
	}
}

// An update keeps the uid and create time, changes the etag, and replaces
// the rules; a resource wildcard covers the resource's permissions.
func TestDenyPolicyUpdate(t *testing.T) {
	engine, index := setup(t)
	read := call(testObjectGet, fixture, testProject)
	created := mustDeny(t, engine, index, projectA, "block", rule([]string{fixturePrincipal}, v2ObjectGet))
	updated := mustDeny(t, engine, index, projectA, "block", rule([]string{fixturePrincipal}, "storage.googleapis.com/objects.list"))
	if updated.GetUid() != created.GetUid() || updated.GetEtag() == created.GetEtag() || !updated.GetCreateTime().AsTime().Equal(created.GetCreateTime().AsTime()) {
		t.Fatalf("update: %v then %v", created, updated)
	}
	grantedBy(t, engine, read, projectA, "a rule on another permission")
	mustDeny(t, engine, index, projectA, "block", rule([]string{fixturePrincipal}, "storage.googleapis.com/objects.*"))
	denied(t, engine, read, "a resource wildcard did not deny:")
}

// An exception exempts the principal from its own rule only: a second rule in
// the same policy, or a policy higher in the hierarchy, still denies.
func TestDenyExceptionAppliesOnlyToItsRule(t *testing.T) {
	read := call(testObjectGet, fixture, testProject)
	excepted := rule([]string{fixturePrincipal}, v2ObjectGet)
	excepted.ExceptionPrincipals = []string{fixturePrincipal}
	plain := rule([]string{fixturePrincipal}, v2ObjectGet)
	t.Run("same policy", func(t *testing.T) {
		engine, index := setup(t)
		mustDeny(t, engine, index, projectA, "p", excepted, plain)
		denied(t, engine, read, "a later rule in the same policy was skipped:")
	})
	t.Run("higher in the hierarchy", func(t *testing.T) {
		engine, index := setup(t)
		mustDeny(t, engine, index, projectA, "p", excepted)
		decision := grantedBy(t, engine, read, projectA, "the excepted rule alone")
		if !strings.Contains(strings.Join(decision.Checks[0].Conditions, ";"), "deny rule excepts the principal") {
			t.Fatalf("exception not traced: %+v", decision.Checks[0])
		}
		mustDeny(t, engine, index, org, "p", plain)
		denied(t, engine, read, "an organization rule was skipped after a project exception:")
	})
}

// principalSet://goog/public:all covers anonymous and authenticated callers.
func TestPublicDenyCoversAnonymousCallers(t *testing.T) {
	engine, index := setup(t)
	set(t, engine, object().Parent, testViewerRole, "allUsers")
	anonymous := call(testObjectGet, "", testProject)
	anonymous.Principal = gateway.Principal{State: gateway.Anonymous}
	if _, err := engine.Authorize(anonymous, object(), nil); err != nil {
		t.Fatal("public grant:", err)
	}
	mustDeny(t, engine, index, projectA, "public", rule([]string{publicPrincipals}, v2ObjectGet))
	decision, err := engine.Authorize(anonymous, object(), nil)
	if status.Code(err) != codes.Unauthenticated || decision.Outcome != "deny" {
		t.Fatalf("public deny skipped an anonymous caller: %+v %v", decision, err)
	}
	denied(t, engine, call(testObjectGet, fixture, testProject), "public deny skipped an authenticated caller:")
}

// Denial conditions take only tag functions (recorded). No resource carries
// a tag, so matchTag is false and its negation applies the rule.
func TestDenyConditionsSeeNoTags(t *testing.T) {
	engine, index := setup(t)
	read := call(testObjectGet, fixture, testProject)
	tagged := rule([]string{fixturePrincipal}, v2ObjectGet)
	tagged.DenialCondition = &expr.Expr{Expression: "resource.matchTag('1/env', 'prod')"}
	mustDeny(t, engine, index, projectA, "p", tagged)
	decision := grantedBy(t, engine, read, projectA, "a tag condition matched an untagged resource")
	if !strings.Contains(strings.Join(decision.Checks[0].Conditions, ";"), "did not match") {
		t.Fatalf("condition not traced: %+v", decision.Checks[0])
	}
	tagged.DenialCondition.Expression = "!resource.matchTag('1/env', 'prod')"
	mustDeny(t, engine, index, projectA, "p", tagged)
	denied(t, engine, read, "a negated tag condition did not deny:")
	tagged.DenialCondition.Expression = `resource.name.startsWith("projects/")`
	_, err := putDeny(t, engine, index, projectA, "p", tagged)
	if info := errorInfo(t, err); status.Code(err) != codes.InvalidArgument || info.GetReason() != "INVALID_CONDITION" ||
		!strings.HasPrefix(status.Convert(err).Message(), "Condition expression compilation failed. Debug message: ") {
		t.Fatalf("non-tag condition: %v", err)
	}
}

// errorInfo returns err's ErrorInfo detail.
func errorInfo(t *testing.T, err error) *errdetails.ErrorInfo {
	t.Helper()
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			return info
		}
	}
	return nil
}

// Write-time refusals with GCP's errors (recorded: iam-deny-policy-lifecycle).
func TestDenyPolicyRefusals(t *testing.T) {
	engine, index := setupWith(t, testDirectory())
	refusals := map[string]struct {
		rule    *iamv2.DenyRule
		message string
		reason  string
	}{
		"v1 permission": {rule([]string{fixturePrincipal}, testObjectGet), "The following permissions are not valid on this resource type: storage.objects.get. " +
			"Note that permissions should be specified using Fully Qualified Domain Names (e.g., iam.googleapis.com/denyPolicies.get). " +
			"For more information about supported permissions, visit https://cloud.google.com/iam/help/deny/supported-permissions", "INVALID_PERMISSIONS"},
		"unknown permission":  {rule([]string{fixturePrincipal}, "storage.googleapis.com/objects.gett"), "", "INVALID_PERMISSIONS"},
		"service wildcard":    {rule([]string{fixturePrincipal}, "storage.googleapis.com/*"), "", "INVALID_PERMISSIONS"},
		"v1 service account":  {rule([]string{fixture}, v2ObjectGet), "The principal specified by (" + fixture + ") is in an incorrect format.", "INVALID_PRINCIPAL"},
		"domain":              {rule([]string{"domain:example.com"}, v2ObjectGet), "The principal specified by (domain:example.com) is in an incorrect format.", "INVALID_PRINCIPAL"},
		"unknown form":        {rule([]string{"principal://goog/bogus/x"}, v2ObjectGet), "The provided deny policy contains a rule with an invalid principal (principal://goog/bogus/x) as part of the denied principals list.", "INVALID_PRINCIPAL"},
		"missing group":       {rule([]string{"principalSet://goog/group/missing@example.com"}, v2ObjectGet), "The principal specified by member (principalSet://goog/group/missing@example.com) was not found.", "INVALID_PRINCIPAL"},
		"exempted public set": {&iamv2.DenyRule{DeniedPrincipals: []string{publicPrincipals}, ExceptionPrincipals: []string{publicPrincipals}, DeniedPermissions: []string{v2ObjectGet}}, "The provided deny policy contains a rule with an invalid principal (principalSet://goog/public:all) as part of the exempted principals list.", "INVALID_PRINCIPAL"},
	}
	for name, refusal := range refusals {
		_, err := putDeny(t, engine, index, projectA, "p", refusal.rule)
		info := errorInfo(t, err)
		matches := refusal.message == "" || status.Convert(err).Message() == refusal.message
		if status.Code(err) != codes.InvalidArgument || info.GetReason() != refusal.reason || info.GetDomain() != "iam.googleapis.com" || !matches {
			t.Fatalf("%s: %v %v", name, err, info)
		}
	}
	if got := engine.DenyPolicies(projectA); len(got) != 0 {
		t.Fatalf("a refused policy was stored: %v", got)
	}
}

// Identifier and wildcard forms GCP accepts that the engine can't match are
// UNIMPLEMENTED; deny policies attach only to hierarchy nodes; and the empty
// shapes GCP accepts deny nothing.
func TestDenyPolicyGapsAndEmptyShapes(t *testing.T) {
	engine, index := setup(t)
	for name, unmodeled := range map[string]*iamv2.DenyRule{
		"customer set":      rule([]string{"principalSet://goog/cloudIdentityCustomerId/C01"}, v2ObjectGet),
		"verb wildcard":     rule([]string{fixturePrincipal}, "storage.googleapis.com/*.get"),
		"workload identity": rule([]string{"principalSet://iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/*"}, v2ObjectGet),
	} {
		if _, err := putDeny(t, engine, index, projectA, "p", unmodeled); status.Code(err) != codes.Unimplemented {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if _, err := putDeny(t, engine, index, object().Parent, "p", rule([]string{fixturePrincipal}, v2ObjectGet)); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("a bucket deny policy was accepted: %v", err)
	}
	if _, err := engine.PutDenyPolicy(state.Resource{Name: projectA, Kind: kindProject}, "p", &iamv2.Policy{Rules: []*iamv2.PolicyRule{{}}}, time.Now()); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("a rule without a deny rule was accepted: %v", err)
	}
	mustDeny(t, engine, index, projectA, "empty")
	mustDeny(t, engine, index, projectA, "no-principals", rule(nil, v2ObjectGet), rule([]string{fixturePrincipal}))
	grantedBy(t, engine, call(testObjectGet, fixture, testProject), projectA, "empty rules")
}

// The resource limits count every rule and distinct principals across the
// node's policies, with GCP's RESOURCE_EXHAUSTED (recorded for 501 rules).
func TestDenyPolicyLimits(t *testing.T) {
	engine, index := setup(t)
	rules := make([]*iamv2.DenyRule, 501)
	for i := range rules {
		rules[i] = rule([]string{fixturePrincipal}, v2ObjectGet)
	}
	_, err := putDeny(t, engine, index, projectA, "p", rules...)
	want := "Applying this policy change would exceed one or more limits for the DenyPolicy policies attached to cloudresourcemanager.googleapis.com%2Fprojects%2Fproject-a. " +
		"Maximum rules: 500. Rules after proposed change: 501. Maximum groups: 500. Groups after proposed change: 0. Maximum principals: 2500. Principals after proposed change: 1. " +
		"For details on IAM policy limits, see https://cloud.google.com/iam/quotas."
	if info := errorInfo(t, err); status.Code(err) != codes.ResourceExhausted || status.Convert(err).Message() != want ||
		info.GetReason() != "QUOTA_EXCEEDED" || info.GetMetadata()["denyPolicyRuleUsage"] != "501" || info.GetMetadata()["resource"] != "projects/project-a" {
		t.Fatalf("501 rules: %v %v", err, info)
	}
	mustDeny(t, engine, index, projectA, "a", rules[:300]...)
	// The one principal counts once per policy (recorded).
	_, err = putDeny(t, engine, index, projectA, "b", rules[:201]...)
	if info := errorInfo(t, err); status.Code(err) != codes.ResourceExhausted || info.GetMetadata()["denyPolicyMemberUsage"] != "2" {
		t.Fatalf("rules across policies: %v %v", err, info)
	}
	mustDeny(t, engine, index, projectA, "a", rules[:299]...)
	mustDeny(t, engine, index, projectA, "b", rules[:201]...)
}

// A deny rule naming a group denies its nested members over an allow; a group
// exception exempts its members from that rule only.
func TestDirectoryGroupDeny(t *testing.T) {
	engine, index := setupWith(t, testDirectory())
	set(t, engine, object().Parent, testViewerRole, nestedAccount, bob)
	deny := rule([]string{"principalSet://goog/group/team@example.com"}, v2ObjectGet)
	mustDeny(t, engine, index, projectA, "groups", deny)
	denied(t, engine, call(testObjectGet, nestedAccount, testProject), "a nested group member escaped the deny:")
	denied(t, engine, call(testObjectGet, bob, testProject), "a user group member escaped the deny:")
	deny.ExceptionPrincipals = []string{"principalSet://goog/group/leads@example.com"}
	mustDeny(t, engine, index, projectA, "groups", deny)
	grantedBy(t, engine, call(testObjectGet, nestedAccount, testProject), object().Parent, "group exception")
	denied(t, engine, call(testObjectGet, carol, testProject), "a non-member was granted:")
}

// A node that leaves the index takes its deny policies with it.
func TestForgetDropsDenyPolicies(t *testing.T) {
	engine, index := setup(t)
	mustDeny(t, engine, index, projectB, "p", rule([]string{publicPrincipals}, v2ObjectGet))
	if err := index.Remove(resourceManagerService, state.Scope{Project: projectBName, Location: state.Global}, projectB); err != nil {
		t.Fatal(err)
	}
	if got := engine.DenyPolicies(projectB); len(got) != 0 {
		t.Fatalf("deny policies outlived their node: %v", got)
	}
}

// A service account is enrolled in the principal sets of its project and the
// project's ancestors; its requests must stay inside the union of the bound
// resources (and their descendants).
func TestPrincipalAccessBoundaryBoundsEnrolledServiceAccounts(t *testing.T) {
	read := call(testObjectGet, fixture, testProject)
	bound := func(resources ...string) iam.PABPolicy {
		return iam.PABPolicy{Rules: []iam.PABRule{{BoundCloudResources: resources}}}
	}
	declare := func(t *testing.T, engine *iam.Engine, node string, boundary iam.PABPolicy) {
		t.Helper()
		if err := engine.DeclareBoundary(node, boundary); err != nil {
			t.Fatal(err)
		}
	}
	engine, _ := setup(t)
	declare(t, engine, projectA, bound(projectB))
	decision, err := engine.Authorize(read, object(), nil)
	if status.Code(err) != codes.PermissionDenied || decision.Outcome != "deny" {
		t.Fatalf("an enrolled service account left its boundary: %+v %v", decision, err)
	}
	// Policies are additive: a folder boundary covering project-a admits the request.
	declare(t, engine, folder, bound(projectA))
	grantedBy(t, engine, read, projectA, "union of boundaries")
	// A bound organization covers its descendants.
	engine, _ = setup(t)
	declare(t, engine, projectA, bound(org))
	grantedBy(t, engine, read, projectA, "descendant of a bound organization")
	// A project-b service account is not enrolled in project-a's principal set.
	declare(t, engine, projectA, bound(projectB))
	set(t, engine, object().Parent, testViewerRole, peer)
	grantedBy(t, engine, call(testObjectGet, peer, projectBName), object().Parent, "unenrolled principal")
	// Boundaries bind to resource-manager principal sets only.
	if err := engine.DeclareBoundary(object().Parent, bound(projectA)); err == nil {
		t.Fatal("a bucket boundary was accepted")
	}
}

// bootstrapResources are two projects for the configuration tests.
var bootstrapResources = []state.Resource{
	{Name: projectA, Kind: kindProject, Service: resourceManagerService, Scope: state.Scope{Project: testProject, Location: state.Global}},
	{Name: projectB, Kind: kindProject, Service: resourceManagerService, Number: "7", Scope: state.Scope{Project: projectBName, Location: state.Global}},
}

// legacyDeny is a retired allow-policy deny rule on the fixture.
var legacyDeny = []iam.LegacyDenyRule{{DeniedPrincipals: []string{fixture}, DeniedPermissions: []string{testObjectGet}}}

// loadConfig writes config and loads it.
func loadConfig(t *testing.T, config iam.Config) (*iam.Engine, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "iam.json")
	mustWriteFile(t, path, mustMarshal(t, config))
	engine, _, err := iam.Load(path)
	return engine, err //nolint:wrapcheck // test helper
}

// The bootstrap configuration declares deny policies in the API's form, and
// loads the retired allow-policy denyRules as the deny policy
// bootstrap-deny-rules on the same node, in v2 identifiers and names.
func TestBootstrapDenyPolicies(t *testing.T) {
	legacy := iam.ConfigPolicy{Policy: iam.Policy{Bindings: []iam.Binding{{Role: "roles/storage.admin", Members: []string{fixture}}}},
		DenyRules: legacyDeny, PAB: &iam.PABPolicy{Rules: []iam.PABRule{{BoundCloudResources: []string{projectA}}}}}
	declared := json.RawMessage(`{"name": "policies/cloudresourcemanager.googleapis.com%2Fprojects%2Fproject-b/denypolicies/declared",
		"rules": [{"denyRule": {"deniedPrincipals": ["principalSet://goog/public:all"], "deniedPermissions": ["storage.googleapis.com/objects.get"]}}]}`)
	engine, err := loadConfig(t, iam.Config{Resources: bootstrapResources, Policies: map[string]iam.ConfigPolicy{projectA: legacy},
		DenyPolicies: []json.RawMessage{declared}})
	if err != nil {
		t.Fatal(err)
	}
	translated := engine.DenyPolicies(projectA)
	if len(translated) != 1 || translated[0].GetName() != "policies/cloudresourcemanager.googleapis.com%2Fprojects%2Fproject-a/denypolicies/bootstrap-deny-rules" {
		t.Fatalf("translated = %v", translated)
	}
	if rule := translated[0].GetRules()[0].GetDenyRule(); rule.GetDeniedPrincipals()[0] != fixturePrincipal || rule.GetDeniedPermissions()[0] != v2ObjectGet {
		t.Fatalf("translated rule = %v", rule)
	}
	if got := engine.DenyPolicies(projectB); len(got) != 1 || got[0].GetName() != "policies/cloudresourcemanager.googleapis.com%2Fprojects%2F7/denypolicies/declared" {
		t.Fatalf("declared = %v", got)
	}
	requireDenied(t, engine, projectA)
}

// requireDenied requires a deny rule to refuse the fixture storage.objects.get
// on node.
func requireDenied(t *testing.T, engine *iam.Engine, node string) {
	t.Helper()
	held, decision, err := engine.Permitted(call(testObjectGet, fixture, testProject), iam.Target{Name: node}, []string{testObjectGet})
	if err != nil || len(held) != 0 || decision.Checks[0].DeniedBy == "" {
		t.Fatalf("no deny rule refused the fixture: %v %+v %v", held, decision, err)
	}
}

// Legacy deny rules elsewhere than a project, folder, or organization fail
// the load.
func TestBootstrapRefusesLegacyDenyOffTheHierarchy(t *testing.T) {
	email := "app-account@" + testProject + ".iam.gserviceaccount.com"
	_, err := loadConfig(t, iam.Config{Resources: bootstrapResources,
		ServiceAccounts: []iam.ServiceAccount{{Project: testProject, Email: email, UniqueID: "123456789012345678901"}},
		Policies:        map[string]iam.ConfigPolicy{iam.AccountResource(testProject, email): {DenyRules: legacyDeny}}})
	if err == nil || !strings.Contains(err.Error(), "deny policies attach only to projects, folders, and organizations") {
		t.Fatalf("legacy deny rules on a service account: %v", err)
	}
}
