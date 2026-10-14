package orgpolicy

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"cloud.google.com/go/orgpolicy/apiv2/orgpolicypb"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Effective merges the specs stored along a resource's ancestry, root first
// (nil where a node stores none), into the rules that apply at the resource,
// with conditions evaluated: a conditional rule whose condition is false is
// dropped and the condition of one that holds is removed. GCP's hierarchy
// evaluation: a node without a spec inherits its parent's result; reset
// restores the constraint default; inheritFromParent (list constraints)
// merges the node's rules with its parent's; any other spec replaces them.
func Effective(c Constraint, chain []*orgpolicypb.PolicySpec) ([]*orgpolicypb.PolicySpec_PolicyRule, error) {
	applied, err := applyConditions(merge(chain))
	if err != nil {
		return nil, err
	}
	if !c.List && len(applied) > 1 {
		applied = applied[:1]
	}
	return applied, nil
}

// merge folds the chain's specs, root first, into the rules that reach its
// last node.
func merge(chain []*orgpolicypb.PolicySpec) []*orgpolicypb.PolicySpec_PolicyRule {
	var rules []*orgpolicypb.PolicySpec_PolicyRule
	for _, spec := range chain {
		switch {
		case spec == nil:
		case spec.GetReset_():
			rules = nil
		case spec.GetInheritFromParent():
			rules = append(rules, spec.GetRules()...)
		default:
			rules = append([]*orgpolicypb.PolicySpec_PolicyRule(nil), spec.GetRules()...)
		}
	}
	return rules
}

// applyConditions drops the rules whose conditions don't hold and strips the
// conditions of the others; a boolean constraint's holding conditional rule
// goes first, since it overrides the unconditional one.
func applyConditions(rules []*orgpolicypb.PolicySpec_PolicyRule) ([]*orgpolicypb.PolicySpec_PolicyRule, error) {
	applied := make([]*orgpolicypb.PolicySpec_PolicyRule, 0, len(rules))
	conditional := 0
	for _, rule := range rules {
		holds, err := conditionHolds(rule)
		if err != nil {
			return nil, err
		}
		if !holds {
			continue
		}
		bare := &orgpolicypb.PolicySpec_PolicyRule{Kind: rule.GetKind()}
		if rule.GetCondition() != nil {
			applied = slices.Insert(applied, conditional, bare)
			conditional++
			continue
		}
		applied = append(applied, bare)
	}
	return applied, nil
}

// Render is the spec getEffectivePolicy returns for effective rules:
// enforce for a boolean constraint, and for a list constraint one rule that
// unions the values, or allowAll / denyAll. Unset constraints render their
// default (recorded: orgpolicy-effective-visibility).
func Render(c Constraint, rules []*orgpolicypb.PolicySpec_PolicyRule) *orgpolicypb.PolicySpec {
	if !c.List {
		return &orgpolicypb.PolicySpec{Rules: []*orgpolicypb.PolicySpec_PolicyRule{
			{Kind: &orgpolicypb.PolicySpec_PolicyRule_Enforce{Enforce: Enforced(rules)}},
		}}
	}
	allowAll, allowed, denied := false, []string{}, []string{}
	for _, rule := range rules {
		if rule.GetDenyAll() {
			return &orgpolicypb.PolicySpec{Rules: []*orgpolicypb.PolicySpec_PolicyRule{{Kind: &orgpolicypb.PolicySpec_PolicyRule_DenyAll{DenyAll: true}}}}
		}
		allowAll = allowAll || rule.GetAllowAll()
		allowed = appendNew(allowed, rule.GetValues().GetAllowedValues())
		denied = appendNew(denied, rule.GetValues().GetDeniedValues())
	}
	if len(denied) == 0 && (allowAll || len(allowed) == 0) {
		return &orgpolicypb.PolicySpec{Rules: []*orgpolicypb.PolicySpec_PolicyRule{{Kind: &orgpolicypb.PolicySpec_PolicyRule_AllowAll{AllowAll: true}}}}
	}
	if allowAll {
		allowed = nil
	}
	return &orgpolicypb.PolicySpec{Rules: []*orgpolicypb.PolicySpec_PolicyRule{{Kind: &orgpolicypb.PolicySpec_PolicyRule_Values{
		Values: &orgpolicypb.PolicySpec_PolicyRule_StringValues{AllowedValues: allowed, DeniedValues: denied},
	}}}}
}

// appendNew appends the values not already present, keeping order.
func appendNew(values, more []string) []string {
	for _, value := range more {
		if !slices.Contains(values, value) {
			values = append(values, value)
		}
	}
	return values
}

// Enforced reports whether a boolean constraint's effective rules enforce
// it. Every enforced boolean constraint defaults to not enforced (ALLOW).
func Enforced(rules []*orgpolicypb.PolicySpec_PolicyRule) bool {
	return len(rules) > 0 && rules[0].GetEnforce()
}

// Allows reports whether a list constraint's effective rules allow a
// requested value; matches reports whether one policy value covers it. A
// denied value or denyAll refuses; allowAll or an allowed value admits;
// allowed values that don't match refuse; with none, the constraint default
// (ALLOW for every enforced list constraint) applies.
func Allows(rules []*orgpolicypb.PolicySpec_PolicyRule, matches func(string) bool) bool {
	for _, rule := range rules {
		if rule.GetDenyAll() || slices.ContainsFunc(rule.GetValues().GetDeniedValues(), matches) {
			return false
		}
	}
	restricted := false
	for _, rule := range rules {
		if rule.GetAllowAll() || slices.ContainsFunc(rule.GetValues().GetAllowedValues(), matches) {
			return true
		}
		restricted = restricted || len(rule.GetValues().GetAllowedValues()) > 0
	}
	return !restricted
}

// LocationMatches reports whether a gcp.resourceLocations policy value
// covers a resource location, compared without case (Storage reports
// "US-CENTRAL1"). "in:GROUP" values expand the value groups groupMembers
// models; "is:X" and plain values match exactly: denying the us multi-region
// leaves us-east1 allowed (the constraint's description).
func LocationMatches(value, location string) bool {
	location = strings.ToLower(location)
	if group, ok := strings.CutPrefix(value, "in:"); ok {
		member, known := groupMembers(group)
		return known && member(location)
	}
	return strings.ToLower(strings.TrimPrefix(value, "is:")) == location
}

// regionPattern is one GCP region ("us-central1", "europe-west4").
var regionPattern = regexp.MustCompile(`^[a-z]+-[a-z]+[0-9]+$`)

// groupMembers returns the membership test of one modeled value group:
// in:us-locations, in:eu-locations, and in:asia-locations cover their
// multi-region and every region (and zone) with the matching prefix ("us-",
// "europe-", "asia-"); in:REGION-locations covers the region and its zones.
// Other groups (dual-regions, in:northamerica-locations) aren't modeled.
func groupMembers(group string) (func(string) bool, bool) {
	name, ok := strings.CutSuffix(group, "-locations")
	if !ok {
		return nil, false
	}
	regional := map[string]string{"us": "us-", "eu": "europe-", "asia": "asia-"}
	if regionPrefix, multi := regional[name]; multi {
		return func(location string) bool { return location == name || strings.HasPrefix(location, regionPrefix) }, true
	}
	if regionPattern.MatchString(name) {
		return func(location string) bool {
			zone, inRegion := strings.CutPrefix(location, name+"-")
			return location == name || (inRegion && len(zone) == 1)
		}, true
	}
	return nil, false
}

// conditionHolds evaluates a rule's condition; a rule without one holds.
func conditionHolds(rule *orgpolicypb.PolicySpec_PolicyRule) (bool, error) {
	if rule.GetCondition() == nil {
		return true, nil
	}
	compiled, err := compile(rule.GetCondition().GetExpression())
	if err != nil {
		return false, err
	}
	return compiled.evaluate()
}

// TagCondition compiles and evaluates a tag-function condition, as IAM deny
// rules' denial conditions take them. Tags aren't modeled, so the value is
// fixed. It returns the value, whether the condition could be evaluated,
// and an error for a condition that doesn't compile.
func TagCondition(expression string) (bool, bool, error) {
	compiled, err := compile(expression)
	if err != nil {
		return false, false, err
	}
	value, err := compiled.evaluate()
	return value, err == nil, nil
}

// evaluate runs a compiled condition against an untagged resource.
func (c *condition) evaluate() (bool, error) {
	out, _, err := c.program.Eval(map[string]any{"resource": &emptypb.Empty{}})
	if err != nil {
		return false, fmt.Errorf("evaluate the rule condition: %w", err)
	}
	truth, ok := out.Value().(bool)
	if !ok {
		return false, errConditionValue
	}
	return truth, nil
}

// Condition refusals.
var (
	errConditionValue      = errors.New("the rule condition did not produce a boolean")
	errConditionExpression = errors.New("a condition requires an expression")
)

// condition is one compiled rule condition.
type condition struct{ program cel.Program }

// tagFunctions are the functions an organization policy condition may call,
// all on resource: matchTag(key, value), matchTagId(keyId, valueId),
// hasTagKey(key), and hasTagKeyId(keyId) ("Setting an organization policy
// with tags"). Resource Manager tags aren't modeled, so no resource carries a
// tag and each returns false.
var tagFunctions = map[string]int{"matchTag": 2, "matchTagId": 2, "hasTagKey": 1, "hasTagKeyId": 1}

// compile checks a condition expression in the tag-function environment.
func compile(expression string) (*condition, error) {
	if strings.TrimSpace(expression) == "" {
		return nil, errConditionExpression
	}
	// resource is an empty message: only the tag functions apply to it, so
	// a condition reading resource.name fails to compile.
	resource := cel.ObjectType("google.protobuf.Empty")
	options := []cel.EnvOption{cel.Types(&emptypb.Empty{}), cel.Variable("resource", resource)}
	for name, arity := range tagFunctions {
		arguments := []*cel.Type{resource}
		for range arity {
			arguments = append(arguments, cel.StringType)
		}
		options = append(options, cel.Function(name, cel.MemberOverload("resource_"+name, arguments, cel.BoolType,
			cel.FunctionBinding(func(...ref.Val) ref.Val { return types.False }))))
	}
	env, err := cel.NewEnv(options...)
	if err != nil {
		return nil, fmt.Errorf("build the condition environment: %w", err)
	}
	checked, issues := env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("compile the condition: %w", issues.Err())
	}
	if checked.OutputType() != cel.BoolType {
		return nil, errConditionValue
	}
	program, err := env.Program(checked)
	if err != nil {
		return nil, fmt.Errorf("compile the condition: %w", err)
	}
	return &condition{program: program}, nil
}
