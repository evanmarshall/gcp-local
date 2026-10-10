package iam

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Rule keeps baseline checks irremovable. Every declared feature must be
// supplied explicitly, including false, before a complete authorization.
type Rule struct {
	Baseline []string
	When     map[string][]string
}
type Target struct{ Name, Parent string }
type Check struct {
	Permission  string   `json:"permission"`
	Target      string   `json:"target"`
	Allowed     bool     `json:"allowed"`
	Policies    []string `json:"policies_considered"`
	GrantPolicy string   `json:"grant_policy,omitempty"`
	Role        string   `json:"role,omitempty"`
	Member      string   `json:"member,omitempty"`
	Conditions  []string `json:"conditions_evaluated,omitempty"`
}
type Decision struct {
	ID        string            `json:"id"`
	TraceID   string            `json:"trace_id"`
	Method    string            `json:"method"`
	Principal gateway.Principal `json:"principal"`
	Outcome   string            `json:"outcome"`
	Reason    string            `json:"reason"`
	Complete  bool              `json:"complete"`
	Checks    []Check           `json:"checks"`
}
type Engine struct {
	mu          sync.Mutex
	index       *state.Index
	roles       map[string]map[string]bool
	rules       map[string]Rule
	policies    map[string]storedPolicy
	unsupported map[string]unsupportedPolicy
	revision    uint64
	decisions   map[string]Decision
	order       []string
}

// errPlanExists rejects a second permission plan for one method.
var errPlanExists = errors.New("method already has a permission plan")

// Decision outcomes recorded on every Check.
const (
	outcomeAllow           = "allow"
	outcomeDeny            = "deny"
	outcomePreflight       = "preflight"
	outcomeUnsupported     = "unsupported"
	outcomeUnauthenticated = "unauthenticated"
	outcomeCancelled       = "cancelled"
)

// Engine sentinels for registration and hierarchy validation.
var (
	errIndexRequired    = errors.New("resource index is required")
	errRuleIncomplete   = errors.New("method and baseline permissions are required")
	errBaselineUnknown  = errors.New("baseline permission is outside the pinned roles")
	errFeatureRequired  = errors.New("feature and additional permissions are required")
	errFeatureUnknown   = errors.New("feature permission is outside the pinned roles")
	errTargetParent     = errors.New("target parent disagrees with the resource index")
	errTargetShape      = errors.New("resource ownership or target shape is unverified")
	errHierarchyCycle   = errors.New("resource hierarchy contains a cycle")
	errHierarchyProject = errors.New("resource hierarchy crosses project ownership")
	errHierarchyRoot    = errors.New("resource hierarchy has no known root")
	errHierarchyMissing = errors.New("resource hierarchy is incomplete")
	errHierarchyDepth   = errors.New("resource hierarchy exceeds the supported depth")
)

func New(index *state.Index) (*Engine, error) {
	if index == nil {
		return nil, errIndexRequired
	}
	roles, err := loadRoles()
	if err != nil {
		return nil, err
	}
	return &Engine{index: index, roles: roles, rules: map[string]Rule{}, policies: map[string]storedPolicy{}, unsupported: map[string]unsupportedPolicy{}, decisions: map[string]Decision{}}, nil
}
func (e *Engine) Register(method string, plan Rule) error {
	rule, err := e.validateRule(plan)
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.rules[method]; ok {
		return errPlanExists
	}
	e.rules[method] = rule
	return nil
}

// Baseline is a preflight for state resolution only. It never authorizes a
// mutation; providers must call Authorize with all required state features.
func (e *Engine) Baseline(call gateway.Call, target Target) (Decision, error) {
	return e.evaluate(call, target, nil, false)
}
func (e *Engine) Authorize(call gateway.Call, target Target, features map[string]bool) (Decision, error) {
	return e.evaluate(call, target, features, true)
}
func (e *Engine) evaluate(call gateway.Call, target Target, features map[string]bool, complete bool) (Decision, error) {
	decision, err := newDecision(call, complete)
	if err != nil {
		return Decision{}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	finish := func(outcome, reason string, code codes.Code) (Decision, error) {
		decision.Outcome = outcome
		decision.Reason = reason
		e.remember(decision)
		if code == codes.OK {
			return decision, nil
		}
		return decision, status.Error(code, reason)
	}
	if err := call.Context.Err(); err != nil {
		return finish(outcomeCancelled, "Request context ended", status.FromContextError(err).Code())
	}
	rule, ok := e.rules[call.Method]
	if !ok {
		return finish(outcomeUnsupported, "No permission plan is registered for this method", codes.Unimplemented)
	}
	if outcome, reason, code, refused := principalRefusal(decision.Principal, rule); refused {
		return finish(outcome, reason, code)
	}
	permissions, outcome, reason, code, refused := e.plannedPermissions(rule, features, complete)
	if refused {
		return finish(outcome, reason, code)
	}
	ancestors, refusal, refused := e.ancestorGate(call, target)
	if refused {
		return finish(outcomeUnsupported, refusal, codes.Unimplemented)
	}
	denied := e.collectChecks(&decision, permissions, ancestors, decision.Principal)
	return finalize(finish, decision, denied, complete)
}

// finalize records the outcome of a fully evaluated request. A denied permission
// is a permission error for an authenticated caller and unauthenticated for an
// anonymous one; an incomplete evaluation is only a preflight.
func finalize(finish func(string, string, codes.Code) (Decision, error), decision Decision, denied string, complete bool) (Decision, error) {
	if denied != "" {
		if decision.Principal.State == gateway.Anonymous {
			return finish(outcomeDeny, "Missing permission: "+denied, codes.Unauthenticated)
		}
		return finish(outcomeDeny, "Missing permission: "+denied, codes.PermissionDenied)
	}
	if !complete {
		return finish(outcomePreflight, "Baseline permissions satisfied; state-dependent authorization is still required", codes.OK)
	}
	return finish(outcomeAllow, "All supported permission checks passed", codes.OK)
}

// newDecision seeds a decision record. Evaluation then records every outcome
// through one finish closure so retention and error shape stay consistent.
func newDecision(call gateway.Call, complete bool) (Decision, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return Decision{}, status.Error(codes.Internal, "Unable to create decision ID")
	}
	principal := call.Principal
	principal.Scopes = append([]string(nil), principal.Scopes...)
	if principal.State != gateway.Authenticated {
		principal.Subject = ""
		principal.Project = ""
		principal.Scopes = nil
	}
	return Decision{ID: hex.EncodeToString(raw), TraceID: call.TraceID, Method: call.Method, Principal: principal, Complete: complete, Checks: []Check{}}, nil
}

// principalRefusal applies the credential-state gate that precedes any policy
// read, including the unverified OAuth scope rule.
func principalRefusal(principal gateway.Principal, rule Rule) (string, string, codes.Code, bool) {
	state := principal.State
	if state == gateway.Unknown || state == gateway.Expired {
		return outcomeUnauthenticated, "Unknown or expired credential", codes.Unauthenticated, true
	}
	if state != gateway.Authenticated && state != gateway.Anonymous {
		return outcomeUnsupported, "Unknown principal state", codes.Unimplemented, true
	}
	if state != gateway.Authenticated {
		return "", "", codes.OK, false
	}
	if principal.Subject == "" {
		return outcomeUnauthenticated, "Authenticated caller has no named principal", codes.Unauthenticated, true
	}
	if !scopeSupports(principal.Scopes, rule) {
		return outcomeUnsupported, "OAuth scope enforcement is unverified for this scope set", codes.Unimplemented, true
	}
	return "", "", codes.OK, false
}

// scopeSupports accepts cloud-platform, or a service scope whose every planned
// permission belongs to that service.
func scopeSupports(scopes []string, rule Rule) bool {
	for _, scope := range scopes {
		if scope == "https://www.googleapis.com/auth/cloud-platform" {
			return true
		}
		service := ""
		switch scope {
		case "https://www.googleapis.com/auth/devstorage.full_control":
			service = "storage."
		case "https://www.googleapis.com/auth/pubsub":
			service = "pubsub."
		}
		if service != "" && ruleServes(rule, service) {
			return true
		}
	}
	return false
}

// ruleServes reports whether every permission the plan can grant belongs to one service prefix.
func ruleServes(rule Rule, service string) bool {
	for _, permission := range rule.Baseline {
		if !strings.HasPrefix(permission, service) {
			return false
		}
	}
	for _, permissions := range rule.When {
		for _, permission := range permissions {
			if !strings.HasPrefix(permission, service) {
				return false
			}
		}
	}
	return true
}

// plannedPermissions resolves the baseline plus the state-dependent additions.
// An unresolved feature is a coverage gap, never an allow.
func (e *Engine) plannedPermissions(rule Rule, features map[string]bool, complete bool) ([]string, string, string, codes.Code, bool) {
	permissions := append([]string(nil), rule.Baseline...)
	if !complete {
		return permissions, "", "", codes.OK, false
	}
	for feature := range features {
		if _, ok := rule.When[feature]; !ok {
			return nil, "unsupported", "Unknown request/state feature", codes.Unimplemented, true
		}
	}
	names := make([]string, 0, len(rule.When))
	for name := range rule.When {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		enabled, present := features[name]
		if !present {
			return nil, "unsupported", "Required request/state feature was not resolved: " + name, codes.Unimplemented, true
		}
		if enabled {
			permissions = append(permissions, rule.When[name]...)
		}
	}
	return permissions, "", "", codes.OK, false
}

// anchor resolves the resource whose policies govern a target. An unregistered
// object is anchored by its bucket: object data stays in the backend.
func (e *Engine) anchor(target Target) (state.Resource, error) {
	resource, found := e.index.Get(target.Name)
	if found {
		if target.Parent != "" && target.Parent != resource.Parent {
			return state.Resource{}, errTargetParent
		}
		return resource, nil
	}
	parent, ok := e.index.Get(target.Parent)
	if !ok || parent.Service != "storage" || parent.Kind != "bucket" || !strings.HasPrefix(target.Name, parent.Name+"/objects/") || target.Name == parent.Name+"/objects/" {
		return state.Resource{}, errTargetShape
	}
	return parent, nil
}

// validateRule rejects a permission plan that is incomplete, or that names a
// permission outside the pinned predefined roles.
func (e *Engine) validateRule(plan Rule) (Rule, error) {
	if len(plan.Baseline) == 0 {
		return Rule{}, errRuleIncomplete
	}
	rule := Rule{Baseline: append([]string(nil), plan.Baseline...), When: map[string][]string{}}
	for _, permission := range plan.Baseline {
		if !e.roleGrants(permission) {
			return Rule{}, fmt.Errorf("%w: %s", errBaselineUnknown, permission)
		}
	}
	for feature, permissions := range plan.When {
		if feature == "" || len(permissions) == 0 {
			return Rule{}, errFeatureRequired
		}
		for _, permission := range permissions {
			if !e.roleGrants(permission) {
				return Rule{}, fmt.Errorf("%w: %s", errFeatureUnknown, permission)
			}
		}
		rule.When[feature] = append([]string(nil), permissions...)
	}
	return rule, nil
}

// roleGrants reports whether any pinned predefined role contains a permission.
func (e *Engine) roleGrants(permission string) bool {
	for _, role := range e.roles {
		if role[permission] {
			return true
		}
	}
	return false
}

// ancestorGate resolves the ownership chain and refuses when the hierarchy is
// incomplete, owned by another project, or carries unsupported configuration.
func (e *Engine) ancestorGate(call gateway.Call, target Target) ([]state.Resource, string, bool) {
	ancestors, err := e.ancestry(target)
	if err != nil {
		return nil, err.Error(), true
	}
	if call.Project != "" && ancestors[0].Scope.Project != call.Project {
		return nil, "Request project disagrees with resource ownership", true
	}
	for _, ancestor := range ancestors {
		if len(e.unsupported[ancestor.Name].features) > 0 {
			return nil, "Unsupported policy configuration at " + ancestor.Name, true
		}
	}
	return ancestors, "", false
}

// collectChecks evaluates every planned permission against the ancestor chain.
// It records each check on the decision and returns the first denied permission.
func (e *Engine) collectChecks(decision *Decision, permissions []string, ancestors []state.Resource, principal gateway.Principal) string {
	seen := map[string]bool{}
	denied := ""
	for _, permission := range permissions {
		if seen[permission] {
			continue
		}
		seen[permission] = true
		check := e.evaluatePermission(decision, ancestors, permission, principal)
		decision.Checks = append(decision.Checks, check)
		if !check.Allowed && denied == "" {
			denied = permission
		}
	}
	return denied
}

// evaluatePermission runs the deny phase and, unless denied, the allow
// bindings for one permission, returning the recorded check.
func (e *Engine) evaluatePermission(decision *Decision, ancestors []state.Resource, permission string, principal gateway.Principal) Check {
	check := Check{Permission: permission, Target: decision.Method, Policies: []string{}}
	denyMatch, excepted := e.denialPhase(ancestors, permission, principal, &check)
	switch {
	case denyMatch != "" && !excepted:
		check.Conditions = append(check.Conditions, "denied by "+denyMatch)
	case excepted:
		check.Conditions = append(check.Conditions, "denial excepted for "+principal.Subject)
	}
	if denyMatch == "" || excepted {
		for _, ancestor := range ancestors {
			e.matchBindings(&check, ancestor.Name, permission, principal)
		}
	}
	return check
}

// matchBindings records the first binding that grants one permission at one
// policy. A binding whose condition fails closed never grants.
func (e *Engine) matchBindings(check *Check, policyName, permission string, principal gateway.Principal) {
	stored := e.policies[policyName]
	for index, binding := range stored.policy.Bindings {
		member := firstMember(binding, principal)
		if member == "" || !e.roles[binding.Role][permission] {
			continue
		}
		if len(binding.Condition) > 0 {
			verdict := stored.evaluateBinding(index)
			check.Conditions = append(check.Conditions, verdict.Reason)
			if !verdict.Allowed {
				continue
			}
		}
		if !check.Allowed {
			check.Allowed = true
			check.GrantPolicy = policyName
			check.Role = binding.Role
			check.Member = member
		}
	}
}

// firstMember returns the effective member of a binding for the principal.
func firstMember(binding Binding, principal gateway.Principal) string {
	for _, member := range binding.Members {
		if member == "allUsers" || (principal.State == gateway.Authenticated && (member == principal.Subject || member == "allAuthenticatedUsers")) {
			return member
		}
	}
	return ""
}

func (e *Engine) ancestry(target Target) ([]state.Resource, error) {
	resource, err := e.anchor(target)
	if err != nil {
		return nil, err
	}
	project := resource.Scope.Project
	seen := map[string]bool{}
	var result []state.Resource
	for len(result) < 128 {
		if seen[resource.Name] {
			return nil, errHierarchyCycle
		}
		seen[resource.Name] = true
		if resource.Scope.Project != "" && resource.Scope.Project != project {
			return nil, errHierarchyProject
		}
		result = append(result, resource)
		if resource.Parent == "" {
			if resource.Kind != "project" && resource.Kind != "organization" {
				return nil, errHierarchyRoot
			}
			return result, nil
		}
		parent, found := e.index.Get(resource.Parent)
		if !found {
			return nil, errHierarchyMissing
		}
		resource = parent
	}
	return nil, errHierarchyDepth
}
func cloneDecision(value Decision) Decision {
	value.Principal.Scopes = append([]string(nil), value.Principal.Scopes...)
	value.Checks = append([]Check(nil), value.Checks...)
	for i := range value.Checks {
		value.Checks[i].Policies = append([]string(nil), value.Checks[i].Policies...)
	}
	return value
}
func (e *Engine) remember(decision Decision) {
	const limit = 1000
	if len(e.order) == limit {
		delete(e.decisions, e.order[0])
		e.order = e.order[1:]
	}
	e.order = append(e.order, decision.ID)
	e.decisions[decision.ID] = cloneDecision(decision)
}
func (e *Engine) Decision(id string) (Decision, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	decision, ok := e.decisions[id]
	return cloneDecision(decision), ok
}

// matchDenies returns the first deny rule that matches one permission check,
// honoring exceptions; deny policies override allow bindings.
func (e *Engine) matchDenies(policyName, permission string, principal gateway.Principal) (string, bool) {
	stored := e.policies[policyName]
	for _, deny := range stored.denies {
		if permissionDeniedBy(deny.rule, permission, principal) {
			return "deny rule on " + policyName, false
		}
		if principalExcepted(deny.rule, principal) {
			return "deny rule on " + policyName, true
		}
	}
	return "", false
}

// principalExcepted reports whether the principal is exempted from a rule.
func principalExcepted(rule DenyRule, principal gateway.Principal) bool {
	return slices.Contains(rule.ExceptionPrincipals, principal.Subject)
}

// permissionDeniedBy reports whether one deny rule denies the check.
func permissionDeniedBy(rule DenyRule, permission string, principal gateway.Principal) bool {
	if principal.State != gateway.Authenticated || principalExcepted(rule, principal) {
		return false
	}
	return permissionDenied(permission, rule) && principalDenied(rule, principal)
}

// permissionDenied reports whether the rule covers one permission.
func permissionDenied(permission string, rule DenyRule) bool {
	if slices.Contains(rule.ExceptionPermissions, permission) {
		return false
	}
	return slices.Contains(rule.DeniedPermissions, permission) ||
		slices.ContainsFunc(rule.DeniedPermissions, func(denied string) bool {
			return strings.HasSuffix(denied, "*") && strings.HasPrefix(permission, strings.TrimSuffix(denied, "*"))
		})
}

// principalDenied reports whether the rule covers one authenticated principal.
func principalDenied(rule DenyRule, principal gateway.Principal) bool {
	return slices.Contains(rule.DeniedPrincipals, principal.Subject) ||
		slices.Contains(rule.DeniedPrincipals, "principalSet://all")
}

// denialPhase scans the ancestor chain for a matching deny rule and records
// the considered policies on the check.
func (e *Engine) denialPhase(ancestors []state.Resource, permission string, principal gateway.Principal, check *Check) (string, bool) {
	denyMatch, excepted := "", false
	for _, ancestor := range ancestors {
		check.Policies = append(check.Policies, ancestor.Name)
		if denyMatch == "" {
			denyMatch, excepted = e.matchDenies(ancestor.Name, permission, principal)
		}
	}
	return denyMatch, excepted
}
