package orgpolicy

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"cloud.google.com/go/orgpolicy/apiv2/orgpolicypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Validate checks a policy written for constraint c, by the API or the
// bootstrap configuration, against the rules the v2 reference states for
// PolicySpec and PolicyRule. It returns INVALID_ARGUMENT for a policy GCP
// refuses and UNIMPLEMENTED for one the emulator can't evaluate.
func Validate(c Constraint, policy *orgpolicypb.Policy) error {
	if message := policy.ProtoReflect(); message.Has(message.Descriptor().Fields().ByName("alternate")) {
		return unimplemented("alternate (launch) policy specs are not modeled")
	}
	if policy.GetSpec() == nil && policy.GetDryRunSpec() == nil {
		return invalid("A policy requires spec or dryRunSpec")
	}
	if spec := policy.GetSpec(); spec != nil {
		if err := validateSpec(c, spec, "spec"); err != nil {
			return err
		}
	}
	if spec := policy.GetDryRunSpec(); spec != nil {
		return validateSpec(c, spec, "dryRunSpec")
	}
	return nil
}

// validateSpec checks one spec. reset needs no rules and no inheritance;
// inheritFromParent is for list constraints only (PolicySpec reference).
func validateSpec(c Constraint, spec *orgpolicypb.PolicySpec, label string) error {
	if err := validateShape(c, spec, label); err != nil {
		return err
	}
	unconditional := 0
	for index, rule := range spec.GetRules() {
		if err := validateRule(c, rule, fmt.Sprintf("%s.rules[%d]", label, index)); err != nil {
			return err
		}
		if rule.GetCondition() == nil {
			unconditional++
		}
	}
	if !c.List && unconditional > 1 {
		return invalid(label + ": a boolean constraint takes at most one unconditional rule")
	}
	return nil
}

// validateShape checks a spec's reset, inheritance, and rule presence.
func validateShape(c Constraint, spec *orgpolicypb.PolicySpec, label string) error {
	switch {
	case spec.GetReset_() && (len(spec.GetRules()) > 0 || spec.GetInheritFromParent()):
		return invalid(label + ": reset requires empty rules and inheritFromParent false")
	case spec.GetInheritFromParent() && !c.List:
		return invalid(label + ": inheritFromParent can be set only for list constraints")
	case !spec.GetReset_() && !spec.GetInheritFromParent() && len(spec.GetRules()) == 0:
		return invalid(label + ": a policy that neither resets nor inherits needs rules")
	}
	return nil
}

// validateRule checks one rule: its kind fits the constraint type, its
// values fit the constraint, and its condition compiles.
func validateRule(c Constraint, rule *orgpolicypb.PolicySpec_PolicyRule, where string) error {
	if len(rule.GetParameters().GetFields()) > 0 {
		return unimplemented(where + ": rule parameters are not modeled")
	}
	switch rule.GetKind().(type) {
	case nil:
		return invalid(where + ": a rule needs values, allowAll, denyAll, or enforce")
	case *orgpolicypb.PolicySpec_PolicyRule_Enforce:
		if c.List {
			return invalid(where + ": enforce applies only to boolean constraints")
		}
	default:
		if !c.List {
			return invalid(where + ": a boolean constraint takes only enforce")
		}
	}
	if err := validateValues(c, rule.GetValues(), where); err != nil {
		return err
	}
	if condition := rule.GetCondition(); condition != nil {
		if _, err := compile(condition.GetExpression()); err != nil {
			return invalid(where + ".condition: " + err.Error())
		}
	}
	return nil
}

// validateValues checks list values against what the constraint accepts.
func validateValues(c Constraint, values *orgpolicypb.PolicySpec_PolicyRule_StringValues, where string) error {
	if values == nil {
		return nil
	}
	if len(values.GetAllowedValues()) == 0 && len(values.GetDeniedValues()) == 0 {
		return invalid(where + ": values needs allowedValues or deniedValues")
	}
	check := map[string]func(string) error{MemberDomains: customerValue, ResourceLocations: locationValue}[c.Name]
	if check == nil {
		return nil
	}
	// "Only allowed values can be defined in this constraint, denied values
	// are not supported" (iam.allowedPolicyMemberDomains' description).
	if c.Name == MemberDomains && len(values.GetDeniedValues()) > 0 {
		return invalid(where + ": " + c.Reference() + " supports only allowed values")
	}
	for _, value := range slices.Concat(values.GetAllowedValues(), values.GetDeniedValues()) {
		if err := check(value); err != nil {
			return err
		}
	}
	return nil
}

// Constraint IDs the emulator names in code.
const (
	MemberDomains          = "iam.allowedPolicyMemberDomains"
	ResourceLocations      = "gcp.resourceLocations"
	KeyCreation            = "iam.disableServiceAccountKeyCreation"
	ManagedKeyCreation     = "iam.managed.disableServiceAccountKeyCreation"
	UniformBucketAccess    = "storage.uniformBucketLevelAccess"
	PublicAccessPrevention = "storage.publicAccessPrevention"
)

// customerPattern is a Google Workspace or Cloud Identity customer ID.
var customerPattern = regexp.MustCompile(`^C[0-9A-Za-z]{5,}$`)

// customerValue accepts a customer ID, optionally prefixed "is:".
// Principal-set values (is:principalSet://iam.googleapis.com/organizations/N)
// need organization membership of every principal, which isn't modeled.
func customerValue(value string) error {
	bare := strings.TrimPrefix(value, "is:")
	switch {
	case strings.HasPrefix(bare, "principalSet://"):
		return unimplemented("principal-set values of " + prefix + MemberDomains + " are not modeled")
	case !customerPattern.MatchString(bare):
		return invalid("Invalid customer ID " + value + " for " + prefix + MemberDomains)
	}
	return nil
}

// locationValue accepts a location ("us-east1", "us"), optionally prefixed
// "is:", or one of the value groups LocationMatches expands.
func locationValue(value string) error {
	if group, ok := strings.CutPrefix(value, "in:"); ok {
		if _, known := groupMembers(group); !known {
			return unimplemented("value group in:" + group + " of " + prefix + ResourceLocations + " is not modeled")
		}
		return nil
	}
	if !locationPattern.MatchString(strings.TrimPrefix(value, "is:")) {
		return invalid("Invalid location " + value + " for " + prefix + ResourceLocations)
	}
	return nil
}

// locationPattern is a lowercase location: a multi-region, region, or zone.
var locationPattern = regexp.MustCompile(`^[a-z][a-z0-9]*(-[a-z0-9]+)*$`)

func invalid(message string) error { return status.Error(codes.InvalidArgument, message) }

func unimplemented(message string) error { return status.Error(codes.Unimplemented, message) }
