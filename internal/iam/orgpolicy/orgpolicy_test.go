package orgpolicy_test

import (
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/orgpolicy/apiv2/orgpolicypb"
	"github.com/evanmarshall/gcp-local/internal/iam/orgpolicy"
	"google.golang.org/genproto/googleapis/type/expr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

// testTime is a fixed write time.
var testTime = time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)

// constraint looks up a catalog constraint or fails the test.
func constraint(t *testing.T, name string) orgpolicy.Constraint {
	t.Helper()
	found, ok := orgpolicy.Lookup(name)
	if !ok {
		t.Fatalf("constraint %s is not in the catalog", name)
	}
	return found
}

// spec decodes a PolicySpec from its JSON form.
func spec(t *testing.T, text string) *orgpolicypb.PolicySpec {
	t.Helper()
	value := &orgpolicypb.PolicySpec{}
	if err := protojson.Unmarshal([]byte(text), value); err != nil {
		t.Fatal(err)
	}
	return value
}

// rendered is the JSON getEffectivePolicy would carry for a chain.
func rendered(t *testing.T, c orgpolicy.Constraint, chain ...*orgpolicypb.PolicySpec) string {
	t.Helper()
	rules, err := orgpolicy.Effective(c, chain)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := protojson.Marshal(orgpolicy.Render(c, rules))
	if err != nil {
		t.Fatal(err)
	}
	return strings.ReplaceAll(string(encoded), " ", "")
}

func TestCatalogKeepsRecordedOrderAndTypes(t *testing.T) {
	names := make([]string, 0, len(orgpolicy.Catalog()))
	for _, c := range orgpolicy.Catalog() {
		names = append(names, c.Name)
	}
	want := "gcp.resourceLocations iam.allowedPolicyMemberDomains iam.disableServiceAccountKeyCreation storage.uniformBucketLevelAccess storage.publicAccessPrevention iam.managed.disableServiceAccountKeyCreation"
	if strings.Join(names, " ") != want {
		t.Fatalf("catalog = %v", names)
	}
	if !constraint(t, orgpolicy.ResourceLocations).List || constraint(t, orgpolicy.UniformBucketAccess).List {
		t.Fatal("list and boolean constraint types are wrong")
	}
	wire := constraint(t, orgpolicy.ResourceLocations).Wire("projects/p")
	if wire["name"] != "projects/p/constraints/gcp.resourceLocations" || wire["constraintDefault"] != "ALLOW" {
		t.Fatalf("wire = %v", wire)
	}
	if _, ok := orgpolicy.Lookup("compute.vmExternalIpAccess"); ok {
		t.Fatal("an unenforced constraint is in the catalog")
	}
}

func TestEffectiveMergesTheHierarchy(t *testing.T) {
	locations := constraint(t, orgpolicy.ResourceLocations)
	org := spec(t, `{"rules":[{"values":{"allowedValues":["in:us-locations"],"deniedValues":["us-west1"]}}]}`)
	inherit := spec(t, `{"inheritFromParent":true,"rules":[{"values":{"allowedValues":["europe-west1"]}}]}`)
	replace := spec(t, `{"rules":[{"values":{"allowedValues":["asia-east1"]}}]}`)
	reset := spec(t, `{"reset":true}`)
	cases := map[string]struct {
		chain []*orgpolicypb.PolicySpec
		want  string
	}{
		"unset renders the default": {nil, `{"rules":[{"allowAll":true}]}`},
		"a node without a spec inherits": {[]*orgpolicypb.PolicySpec{org, nil},
			`{"rules":[{"values":{"allowedValues":["in:us-locations"],"deniedValues":["us-west1"]}}]}`},
		"inheritFromParent merges": {[]*orgpolicypb.PolicySpec{org, inherit},
			`{"rules":[{"values":{"allowedValues":["in:us-locations","europe-west1"],"deniedValues":["us-west1"]}}]}`},
		"a spec replaces":            {[]*orgpolicypb.PolicySpec{org, replace}, `{"rules":[{"values":{"allowedValues":["asia-east1"]}}]}`},
		"reset restores the default": {[]*orgpolicypb.PolicySpec{org, reset}, `{"rules":[{"allowAll":true}]}`},
		"denyAll wins":               {[]*orgpolicypb.PolicySpec{spec(t, `{"rules":[{"denyAll":true}]}`), inherit}, `{"rules":[{"denyAll":true}]}`},
	}
	for name, test := range cases {
		if got := rendered(t, locations, test.chain...); got != test.want {
			t.Errorf("%s: %s, want %s", name, got, test.want)
		}
	}
	boolean := constraint(t, orgpolicy.UniformBucketAccess)
	if got := rendered(t, boolean); got != `{"rules":[{"enforce":false}]}` {
		t.Fatalf("unset boolean = %s", got)
	}
	if got := rendered(t, boolean, spec(t, `{"rules":[{"enforce":true}]}`), nil); got != `{"rules":[{"enforce":true}]}` {
		t.Fatalf("inherited boolean = %s", got)
	}
}

func TestConditionsSeeNoTags(t *testing.T) {
	boolean := constraint(t, orgpolicy.UniformBucketAccess)
	// Tags aren't modeled: matchTag is false, so only the negated rule holds,
	// and a holding conditional rule overrides the unconditional one.
	holds := spec(t, `{"rules":[{"enforce":true},{"enforce":false,"condition":{"expression":"!resource.matchTag('123/env', 'prod')"}}]}`)
	if got := rendered(t, boolean, holds); got != `{"rules":[{"enforce":false}]}` {
		t.Fatalf("negated tag condition = %s", got)
	}
	fails := spec(t, `{"rules":[{"enforce":true},{"enforce":false,"condition":{"expression":"resource.hasTagKey('123/env')"}}]}`)
	if got := rendered(t, boolean, fails); got != `{"rules":[{"enforce":true}]}` {
		t.Fatalf("tag condition = %s", got)
	}
}

func TestAllowsAndLocationGroups(t *testing.T) {
	rules := []*orgpolicypb.PolicySpec_PolicyRule{
		{Kind: &orgpolicypb.PolicySpec_PolicyRule_Values{Values: &orgpolicypb.PolicySpec_PolicyRule_StringValues{
			AllowedValues: []string{"in:us-locations", "in:europe-west1-locations"}, DeniedValues: []string{"us-west1"},
		}}},
	}
	for location, want := range map[string]bool{
		"US-CENTRAL1": true, "us": true, "us-east1-b": true, "europe-west1": true, "europe-west1-c": true,
		"us-west1": false, "europe-west2": false, "asia-east1": false, "eu": false,
	} {
		allowed := orgpolicy.Allows(rules, func(value string) bool { return orgpolicy.LocationMatches(value, location) })
		if allowed != want {
			t.Errorf("%s: allowed %v, want %v", location, allowed, want)
		}
	}
	if !orgpolicy.Allows(nil, func(string) bool { return false }) {
		t.Fatal("no rules must apply the ALLOW default")
	}
	if orgpolicy.LocationMatches("us", "us-east1") {
		t.Fatal("a multi-region value must not cover its regions")
	}
}

func TestValidateRefusesWhatGCPRefusesAndWhatIsNotModeled(t *testing.T) {
	locations, domains, boolean := constraint(t, orgpolicy.ResourceLocations), constraint(t, orgpolicy.MemberDomains), constraint(t, orgpolicy.UniformBucketAccess)
	policy := func(text string) *orgpolicypb.Policy {
		value := &orgpolicypb.Policy{}
		if err := protojson.Unmarshal([]byte(text), value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	cases := []struct {
		c    orgpolicy.Constraint
		body string
		code codes.Code
		text string
	}{
		{boolean, `{}`, codes.InvalidArgument, "spec or dryRunSpec"},
		{boolean, `{"spec":{"reset":true,"rules":[{"enforce":true}]}}`, codes.InvalidArgument, "reset requires empty rules"},
		{boolean, `{"spec":{"inheritFromParent":true}}`, codes.InvalidArgument, "only for list constraints"},
		{boolean, `{"spec":{"rules":[{"allowAll":true}]}}`, codes.InvalidArgument, "takes only enforce"},
		{boolean, `{"spec":{"rules":[{"enforce":true},{"enforce":false}]}}`, codes.InvalidArgument, "at most one unconditional"},
		{boolean, `{"dryRunSpec":{"rules":[]}}`, codes.InvalidArgument, "dryRunSpec: a policy that neither resets nor inherits"},
		{locations, `{"spec":{"rules":[{"enforce":true}]}}`, codes.InvalidArgument, "enforce applies only to boolean"},
		{locations, `{"spec":{"rules":[{"values":{"allowedValues":["in:northamerica-locations"]}}]}}`, codes.Unimplemented, "is not modeled"},
		{locations, `{"spec":{"rules":[{"values":{"allowedValues":["Not A Place"]}}]}}`, codes.InvalidArgument, "Invalid location"},
		{domains, `{"spec":{"rules":[{"values":{"deniedValues":["C0abcdef"]}}]}}`, codes.InvalidArgument, "supports only allowed values"},
		{domains, `{"spec":{"rules":[{"values":{"allowedValues":["is:principalSet://iam.googleapis.com/organizations/1"]}}]}}`, codes.Unimplemented, "principal-set values"},
		{boolean, `{"spec":{"rules":[{"enforce":true,"condition":{"expression":"resource.name == 'x'"}}]}}`, codes.InvalidArgument, "condition"},
		{boolean, `{"alternate":{"launch":"x"},"spec":{"rules":[{"enforce":true}]}}`, codes.Unimplemented, "alternate"},
	}
	for _, test := range cases {
		err := orgpolicy.Validate(test.c, policy(test.body))
		if status.Code(err) != test.code || !strings.Contains(status.Convert(err).Message(), test.text) {
			t.Errorf("%s: %v, want %v containing %q", test.body, err, test.code, test.text)
		}
	}
	withParameters := policy(`{"spec":{"rules":[{"enforce":true}]}}`)
	withParameters.Spec.Rules[0].Parameters = &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewBoolValue(true)}}
	if status.Code(orgpolicy.Validate(boolean, withParameters)) != codes.Unimplemented {
		t.Fatal("rule parameters must be refused as unmodeled")
	}
	conditional := policy(`{"spec":{"rules":[{"enforce":true}]}}`)
	conditional.Spec.Rules = append(conditional.Spec.Rules, &orgpolicypb.PolicySpec_PolicyRule{
		Kind: &orgpolicypb.PolicySpec_PolicyRule_Enforce{Enforce: false}, Condition: &expr.Expr{Expression: "resource.matchTagId('tagKeys/1', 'tagValues/2')"}})
	if err := orgpolicy.Validate(boolean, conditional); err != nil {
		t.Fatalf("a tag condition is valid: %v", err)
	}
	for _, body := range []string{
		`{"spec":{"rules":[{"values":{"allowedValues":["in:us-locations","is:europe-west1","us-east1-b"]}}]},"dryRunSpec":{"reset":true}}`,
		`{"spec":{"inheritFromParent":true}}`,
	} {
		if err := orgpolicy.Validate(locations, policy(body)); err != nil {
			t.Errorf("%s: %v", body, err)
		}
	}
}

func TestParseNameAndSeal(t *testing.T) {
	for name, want := range map[string]bool{
		"projects/p/policies/gcp.resourceLocations": true, "folders/1/policies/x": true, "organizations/1/policies/x": true,
		"projects/p/policies/": false, "buckets/b/policies/x": false, "projects/p/x/policies/y": false, "projects/p": false,
	} {
		if _, _, ok := orgpolicy.ParseName(name); ok != want {
			t.Errorf("%s: %v", name, ok)
		}
	}
	first := &orgpolicypb.Policy{Name: "projects/1/policies/x", Spec: spec(t, `{"rules":[{"enforce":true}]}`)}
	second := &orgpolicypb.Policy{Name: "projects/1/policies/x", Spec: spec(t, `{"rules":[{"enforce":false}]}`)}
	orgpolicy.Seal(first, testTime)
	orgpolicy.Seal(second, testTime)
	if first.GetEtag() == "" || first.GetEtag() == second.GetEtag() || first.GetSpec().GetEtag() == "" || first.GetSpec().GetUpdateTime() == nil {
		t.Fatalf("sealed: %v / %v", first, second)
	}
}
