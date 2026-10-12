package iam_test

import (
	"encoding/json"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// publicPrincipals is GCP's deny-policy identifier for every caller.
const publicPrincipals = "principalSet://goog/public:all"

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

// projectAdmin re-grants the fixture project admin alongside extra policy parts.
func projectAdmin(policy iam.Policy) iam.Policy {
	policy.Version = 3
	policy.Bindings = append(policy.Bindings, iam.Binding{Role: "roles/storage.admin", Members: []string{fixture}})
	return policy
}

func mustSet(t *testing.T, engine *iam.Engine, resource string, policy iam.Policy) {
	t.Helper()
	if _, err := engine.SetPolicy(resource, policy); err != nil {
		t.Fatal(err)
	}
}

// An exception exempts the principal from its own rule only: a second rule in
// the same policy, or a rule higher in the hierarchy, still denies.
func TestDenyExceptionAppliesOnlyToItsRule(t *testing.T) {
	read := call(testObjectGet, fixture, testProject)
	excepted := iam.DenyRule{DeniedPrincipals: []string{fixture}, DeniedPermissions: []string{testObjectGet}, ExceptionPrincipals: []string{fixture}}
	plain := iam.DenyRule{DeniedPrincipals: []string{fixture}, DeniedPermissions: []string{testObjectGet}}
	t.Run("same policy", func(t *testing.T) {
		engine, _ := setup(t)
		mustSet(t, engine, object().Parent, iam.Policy{Version: 3, DenyRules: []iam.DenyRule{excepted, plain}})
		denied(t, engine, read, "a later rule in the same policy was skipped:")
	})
	t.Run("higher in the hierarchy", func(t *testing.T) {
		engine, _ := setup(t)
		mustSet(t, engine, object().Parent, iam.Policy{Version: 3, DenyRules: []iam.DenyRule{excepted}})
		grantedBy(t, engine, read, projectA, "the excepted rule alone")
		mustSet(t, engine, projectA, projectAdmin(iam.Policy{DenyRules: []iam.DenyRule{plain}}))
		denied(t, engine, read, "a project rule was skipped after a bucket exception:")
	})
}

// principalSet://goog/public:all covers anonymous and authenticated callers.
func TestPublicDenyCoversAnonymousCallers(t *testing.T) {
	engine, _ := setup(t)
	set(t, engine, object().Parent, testViewerRole, "allUsers")
	anonymous := call(testObjectGet, "", testProject)
	anonymous.Principal = gateway.Principal{State: gateway.Anonymous}
	if _, err := engine.Authorize(anonymous, object(), nil); err != nil {
		t.Fatal("public grant:", err)
	}
	mustSet(t, engine, projectA, projectAdmin(iam.Policy{DenyRules: []iam.DenyRule{{DeniedPrincipals: []string{publicPrincipals}, DeniedPermissions: []string{testObjectGet}}}}))
	decision, err := engine.Authorize(anonymous, object(), nil)
	if status.Code(err) != codes.Unauthenticated || decision.Outcome != "deny" {
		t.Fatalf("public deny skipped an anonymous caller: %+v %v", decision, err)
	}
	denied(t, engine, call(testObjectGet, fixture, testProject), "public deny skipped an authenticated caller:")
}

// Deny conditions are evaluated: false lifts the rule, true or an error applies it.
func TestDenyConditionsAreEvaluatedAndFailClosed(t *testing.T) {
	engine, _ := setup(t)
	read := call(testObjectGet, fixture, testProject)
	rule := iam.DenyRule{DeniedPrincipals: []string{fixture}, DeniedPermissions: []string{testObjectGet},
		DenialCondition: condition(t, `resource.name.startsWith("projects/_/buckets/bucket-a/objects/secret-")`)}
	mustSet(t, engine, object().Parent, iam.Policy{Version: 3, DenyRules: []iam.DenyRule{rule}})
	if _, err := engine.Authorize(read, namedObject("public.txt"), nil); err != nil {
		t.Fatal("a false deny condition still denied:", err)
	}
	if _, err := engine.Authorize(read, namedObject("secret-a.txt"), nil); status.Code(err) != codes.PermissionDenied {
		t.Fatal("a true deny condition did not deny:", err)
	}
	// object() carries no resource attributes, so the condition cannot be evaluated.
	decision, err := engine.Authorize(read, object(), nil)
	if status.Code(err) != codes.PermissionDenied || decision.Checks[0].Allowed {
		t.Fatalf("an unevaluable deny condition did not fail closed: %+v %v", decision, err)
	}
}

// Deny principals use GCP's v2 identifiers; unsupported identifiers are refused
// at write time rather than silently never matching.
func TestDenyPrincipalIdentifiers(t *testing.T) {
	engine, _ := setup(t)
	read := call(testObjectGet, fixture, testProject)
	v2 := "principal://iam.googleapis.com/projects/-/serviceAccounts/fixture@project-a.iam.gserviceaccount.com"
	mustSet(t, engine, object().Parent, iam.Policy{Version: 3, DenyRules: []iam.DenyRule{{DeniedPrincipals: []string{v2}, DeniedPermissions: []string{"storage.googleapis.com/objects.get"}}}})
	denied(t, engine, read, "a v2 service-account deny did not match:")
	for _, unsupported := range []string{"principalSet://all", "domain:example.com", "principalSet://goog/cloudIdentityCustomerId/C01"} {
		_, err := engine.SetPolicy(object().Parent, iam.Policy{Version: 3, DenyRules: []iam.DenyRule{{DeniedPrincipals: []string{unsupported}, DeniedPermissions: []string{testObjectGet}}}})
		if status.Code(err) != codes.Unimplemented {
			t.Fatalf("%s was accepted: %v", unsupported, err)
		}
	}
	_, err := engine.SetPolicy(object().Parent, iam.Policy{Version: 3, DenyRules: []iam.DenyRule{{DeniedPrincipals: []string{fixture}, DeniedPermissions: []string{"storage.objects.gett"}}}})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("an unknown denied permission was accepted: %v", err)
	}
}

// A service account is enrolled in the principal sets of its project and the
// project's ancestors; its requests must stay inside the union of the bound
// resources (and their descendants).
func TestPrincipalAccessBoundaryBoundsEnrolledServiceAccounts(t *testing.T) {
	engine, _ := setup(t)
	read := call(testObjectGet, fixture, testProject)
	bound := func(resources ...string) *iam.PABPolicy {
		return &iam.PABPolicy{Rules: []iam.PABRule{{BoundCloudResources: resources}}}
	}
	mustSet(t, engine, projectA, projectAdmin(iam.Policy{PAB: bound(projectB)}))
	decision, err := engine.Authorize(read, object(), nil)
	if status.Code(err) != codes.PermissionDenied || decision.Outcome != "deny" {
		t.Fatalf("an enrolled service account left its boundary: %+v %v", decision, err)
	}
	// Policies are additive: a folder boundary covering project-a admits the request.
	mustSet(t, engine, folder, iam.Policy{Version: 3, PAB: bound(projectA)})
	grantedBy(t, engine, read, projectA, "union of boundaries")
	// A bound organization covers its descendants.
	mustSet(t, engine, folder, iam.Policy{})
	mustSet(t, engine, projectA, projectAdmin(iam.Policy{PAB: bound(org)}))
	grantedBy(t, engine, read, projectA, "descendant of a bound organization")
	// A project-b service account is not enrolled in project-a's principal set.
	mustSet(t, engine, projectA, projectAdmin(iam.Policy{PAB: bound(projectB)}))
	set(t, engine, object().Parent, testViewerRole, peer)
	grantedBy(t, engine, call(testObjectGet, peer, projectBName), object().Parent, "unenrolled principal")
	// Boundaries bind to resource-manager principal sets only.
	if _, err := engine.SetPolicy(object().Parent, iam.Policy{Version: 3, PAB: bound(projectA)}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("a bucket boundary was accepted: %v", err)
	}
}

// A missing resource attribute makes the condition fail, so a negated
// expression grants nothing where the provider supplies no attributes.
func TestMissingConditionAttributeGrantsNothing(t *testing.T) {
	engine, _ := setup(t)
	mustSet(t, engine, projectA, iam.Policy{})
	read := call(testObjectGet, fixture, testProject)
	for _, expression := range []string{`!resource.name.startsWith("projects/_/buckets/bucket-a/objects/secret-")`, `resource.service != "sqladmin.googleapis.com"`, `resource.type == ""`} {
		mustSet(t, engine, object().Parent, iam.Policy{Version: 3, Bindings: []iam.Binding{{Role: testViewerRole, Members: []string{fixture}, Condition: condition(t, expression)}}})
		if _, err := engine.Authorize(read, object(), nil); status.Code(err) != codes.PermissionDenied {
			t.Fatalf("%s granted without the attribute: %v", expression, err)
		}
		if expression[0] == '!' {
			if _, err := engine.Authorize(read, namedObject("public.txt"), nil); err != nil {
				t.Fatalf("%s denied with the attribute: %v", expression, err)
			}
		}
	}
}
