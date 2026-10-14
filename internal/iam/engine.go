package iam

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/orgpolicy/apiv2/orgpolicypb"
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

// Target names the resource a call acts on. Attributes are the CEL
// resource.* values GCP exposes to IAM conditions for that resource.
type Target struct {
	Name, Parent string
	Attributes   ResourceAttributes
	// PublicPrevented excludes allUsers and allAuthenticatedUsers bindings
	// at every level, as Storage's enforced public access prevention does:
	// existing public grants stay in place but grant nothing.
	PublicPrevented bool
}

// ResourceAttributes are the documented IAM condition resource attributes.
type ResourceAttributes struct{ Name, Type, Service string }
type Check struct {
	Permission  string   `json:"permission"`
	Target      string   `json:"target"`
	Allowed     bool     `json:"allowed"`
	Policies    []string `json:"policies_considered"`
	GrantPolicy string   `json:"grant_policy,omitempty"`
	Role        string   `json:"role,omitempty"`
	Member      string   `json:"member,omitempty"`
	Conditions  []string `json:"conditions_evaluated,omitempty"`
	// DenyPolicies names every deny policy evaluated on the ancestry;
	// DeniedBy the deny rule that refused the permission, as "rule N of
	// policies/.../denypolicies/ID".
	DenyPolicies []string `json:"deny_policies_considered,omitempty"`
	DeniedBy     string   `json:"denied_by,omitempty"`
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
	// Signals carry the non-IAM policy families evaluated for the request:
	// VPC-SC perimeter membership at authorization, and every organization
	// policy check a provider makes at the mutation after authorization
	// (CheckOrgPolicy, SetPolicyFor). They stay distinct from the IAM checks,
	// so a trace tells an organization policy refusal from an IAM denial.
	Signals []Signal `json:"signals,omitempty"`
	// conditions holds the request's CEL attributes; never serialized.
	conditions conditionContext
	// publicPrevented is the target's PublicPrevented; never serialized.
	publicPrevented bool
	// memberships are the directory members naming the principal; never
	// serialized.
	memberships map[string]bool
}

// Signal records one non-IAM policy family's observation on a decision.
// Organization policy verdicts are "allow", "violation" (the mutation was
// refused), "dry-run allow", and "dry-run violation" (traced, never
// enforced); a VPC-SC signal is "inside-perimeter".
type Signal struct {
	Family   string `json:"family"`
	Verdict  string `json:"verdict"`
	Detail   string `json:"detail"`
	Resource string `json:"resource"`
}
type Engine struct {
	mu          sync.Mutex
	index       *state.Index
	roles       map[string]map[string]bool
	definitions map[string]json.RawMessage
	testable    map[string]string
	customRoles CustomRoleLookup
	rules       map[string]Rule
	policies    map[string]storedPolicy
	// denyPolicies are each hierarchy node's IAM v2 deny policies, in
	// creation order; boundaries its declared principal access boundary.
	denyPolicies map[string][]denyPolicy
	boundaries   map[string]*PABPolicy
	unsupported  map[string]unsupportedPolicy
	orgPolicies  map[string]map[string]*orgpolicypb.Policy
	perimeters   map[string][]string
	revision     uint64
	decisions    map[string]Decision
	order        []string
	signingKeys  map[string]map[string]signingKey
	directory    directory
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
	catalog, err := loadRoles()
	if err != nil {
		return nil, err
	}
	engine := &Engine{index: index, roles: catalog.permissions, definitions: catalog.definitions, testable: catalog.testable, rules: map[string]Rule{}, policies: map[string]storedPolicy{}, denyPolicies: map[string][]denyPolicy{}, boundaries: map[string]*PABPolicy{}, unsupported: map[string]unsupportedPolicy{}, orgPolicies: map[string]map[string]*orgpolicypb.Policy{}, perimeters: map[string][]string{}, decisions: map[string]Decision{}, signingKeys: map[string]map[string]signingKey{}}
	// A resource that leaves the index takes its policy with it, whichever
	// provider deleted it: GCP gives a recreated resource an empty policy.
	// A delete racing a same-name recreate can drop the new policy, never
	// keep the old one.
	index.OnRemove(engine.Forget)
	return engine, nil
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

// Plans copies the registered permission plans whose method starts with
// prefix. It is a read-only export for drift reports; it never changes rules.
func (e *Engine) Plans(prefix string) map[string]Rule {
	e.mu.Lock()
	defer e.mu.Unlock()
	plans := map[string]Rule{}
	for method, rule := range e.rules {
		if !strings.HasPrefix(method, prefix) {
			continue
		}
		copied := Rule{Baseline: append([]string(nil), rule.Baseline...), When: map[string][]string{}}
		for feature, permissions := range rule.When {
			copied.When[feature] = append([]string(nil), permissions...)
		}
		plans[method] = copied
	}
	return plans
}

// Baseline is a preflight for state resolution only. It never authorizes a
// mutation; providers must call Authorize with all required state features.
func (e *Engine) Baseline(call gateway.Call, target Target) (Decision, error) {
	return e.evaluate(call, target, nil, false)
}
func (e *Engine) Authorize(call gateway.Call, target Target, features map[string]bool) (Decision, error) {
	return e.evaluate(call, target, features, true)
}

// Permitted answers testIamPermissions: the subset of permissions, in request
// order without repeats, that the caller holds on target through the same
// ancestry, boundaries, deny rules, and bindings Authorize evaluates. GCP
// requires no permission for the call itself, so a caller holding none gets
// an empty list, not a denial. A permission the token's OAuth scopes don't
// cover is not held. Credential refusals and an unverified hierarchy fail as
// in Authorize.
func (e *Engine) Permitted(call gateway.Call, target Target, permissions []string) ([]string, Decision, error) {
	decision, err := newDecision(call, true)
	if err != nil {
		return nil, Decision{}, err
	}
	decision.conditions = conditionContext{ResourceName: target.Attributes.Name, ResourceType: target.Attributes.Type,
		Service: target.Attributes.Service, Now: time.Now()}
	e.mu.Lock()
	defer e.mu.Unlock()
	decision.memberships = e.directory.memberships(decision.Principal)
	finish := func(outcome, reason string, code codes.Code) ([]string, Decision, error) {
		decision.Outcome, decision.Reason = outcome, reason
		e.remember(decision)
		return nil, decision, refusal(code, reason)
	}
	if err := call.Context.Err(); err != nil {
		return finish(outcomeCancelled, "Request context ended", status.FromContextError(err).Code())
	}
	if outcome, reason, code, refused := principalRefusal(decision.Principal); refused {
		return finish(outcome, reason, code)
	}
	ancestors, reason, refused := e.ancestorGate(call, target)
	if refused {
		return finish(outcomeUnsupported, reason, codes.Unimplemented)
	}
	held := []string{}
	if e.pabViolation(ancestors, decision.Principal) == "" {
		held = e.heldPermissions(&decision, ancestors, permissions)
	}
	decision.Outcome, decision.Reason = outcomeAllow, "Tested permissions evaluated"
	e.remember(decision)
	return held, decision, nil
}

// heldPermissions records one check per distinct permission and returns
// those allowed. A permission the token's scopes don't cover is not held.
func (e *Engine) heldPermissions(decision *Decision, ancestors []state.Resource, permissions []string) []string {
	held, seen := []string{}, map[string]bool{}
	for _, permission := range permissions {
		if seen[permission] {
			continue
		}
		seen[permission] = true
		check := e.evaluatePermission(decision, ancestors, permission, decision.Principal)
		if decision.Principal.State == gateway.Authenticated && !scopesCover(decision.Principal.Scopes, []string{permission}) {
			check.Allowed = false
			check.Conditions = append(check.Conditions, scopeInsufficientMessage)
		}
		decision.Checks = append(decision.Checks, check)
		if check.Allowed {
			held = append(held, permission)
		}
	}
	return held
}

func (e *Engine) evaluate(call gateway.Call, target Target, features map[string]bool, complete bool) (Decision, error) {
	decision, err := newDecision(call, complete)
	if err != nil {
		return Decision{}, err
	}
	decision.conditions = conditionContext{ResourceName: target.Attributes.Name, ResourceType: target.Attributes.Type,
		Service: target.Attributes.Service, Now: time.Now()}
	decision.publicPrevented = target.PublicPrevented
	e.mu.Lock()
	defer e.mu.Unlock()
	decision.memberships = e.directory.memberships(decision.Principal)
	finish := func(outcome, reason string, code codes.Code) (Decision, error) {
		decision.Outcome = outcome
		decision.Reason = reason
		e.remember(decision)
		return decision, refusal(code, reason)
	}
	if err := call.Context.Err(); err != nil {
		return finish(outcomeCancelled, "Request context ended", status.FromContextError(err).Code())
	}
	rule, ok := e.rules[call.Method]
	if !ok {
		return finish(outcomeUnsupported, "No permission plan is registered for this method", codes.Unimplemented)
	}
	permissions, outcome, reason, code, refused := e.callerPermissions(decision.Principal, rule, features, complete)
	if refused {
		return finish(outcome, reason, code)
	}
	ancestors, refusal, refused := e.ancestorGate(call, target)
	if refused {
		return finish(outcomeUnsupported, refusal, codes.Unimplemented)
	}
	if violation := e.pabViolation(ancestors, decision.Principal); violation != "" {
		return finish(outcomeDeny, violation, codes.PermissionDenied)
	}
	denied := e.collectChecks(&decision, permissions, ancestors, decision.Principal)
	e.collectSignals(ancestors, &decision)
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

// callerPermissions gates the caller's credential, resolves the plan's
// permissions, and checks the token's OAuth scopes against them. GCP refuses
// a token without an accepted scope before consulting IAM.
func (e *Engine) callerPermissions(principal gateway.Principal, rule Rule, features map[string]bool, complete bool) ([]string, string, string, codes.Code, bool) {
	if outcome, reason, code, refused := principalRefusal(principal); refused {
		return nil, outcome, reason, code, true
	}
	permissions, outcome, reason, code, refused := e.plannedPermissions(rule, features, complete)
	if refused {
		return nil, outcome, reason, code, true
	}
	if principal.State == gateway.Authenticated && !scopesCover(principal.Scopes, permissions) {
		return nil, outcomeDeny, scopeInsufficientMessage, codes.PermissionDenied, true
	}
	return permissions, "", "", codes.OK, false
}

// refusal is the wire error for a refused decision; nil when code is OK.
func refusal(code codes.Code, reason string) error {
	switch {
	case code == codes.OK:
		return nil
	case reason == scopeInsufficientMessage:
		return scopeInsufficient()
	}
	return status.Error(code, reason)
}

// principalRefusal applies the credential-state gate that precedes any policy
// read. OAuth scopes are checked once the planned permissions are resolved.
func principalRefusal(principal gateway.Principal) (string, string, codes.Code, bool) {
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
	return "", "", codes.OK, false
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
			return nil, outcomeUnsupported, "Unknown request/state feature", codes.Unimplemented, true
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
			return nil, outcomeUnsupported, "Required request/state feature was not resolved: " + name, codes.Unimplemented, true
		}
		if enabled {
			permissions = append(permissions, rule.When[name]...)
		}
	}
	return permissions, "", "", codes.OK, false
}

// anchor resolves the resource whose policies govern a target. An unregistered
// object or folder is anchored by its bucket: its data stays with the
// Storage provider.
func (e *Engine) anchor(target Target) (state.Resource, error) {
	resource, found := e.index.Get(target.Name)
	if found {
		if target.Parent != "" && target.Parent != resource.Parent {
			return state.Resource{}, errTargetParent
		}
		return resource, nil
	}
	parent, ok := e.index.Get(target.Parent)
	if !ok || parent.Service != "storage" || parent.Kind != "bucket" || !bucketChild(parent.Name, target.Name) {
		return state.Resource{}, errTargetShape
	}
	return parent, nil
}

// bucketChild reports whether name is an object or folder of bucket.
func bucketChild(bucket, name string) bool {
	for _, collection := range []string{"/objects/", "/folders/"} {
		if child, ok := strings.CutPrefix(name, bucket+collection); ok && child != "" {
			return true
		}
	}
	return false
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
	if denyMatch := e.denialPhase(ancestors, permission, principal, decision, &check); denyMatch != "" {
		check.DeniedBy = denyMatch
		return check
	}
	for _, ancestor := range ancestors {
		e.matchBindings(&check, ancestor.Name, permission, principal, decision)
	}
	return check
}

// matchBindings records the first binding that grants one permission at one
// policy. A binding whose condition fails closed never grants.
func (e *Engine) matchBindings(check *Check, policyName, permission string, principal gateway.Principal, decision *Decision) {
	stored := e.policies[policyName]
	for index, binding := range stored.policy.Bindings {
		member := firstMember(binding, principal, decision)
		if member == "" || !e.rolePermission(binding.Role, permission) {
			continue
		}
		if len(binding.Condition) > 0 {
			matched, reason := stored.evaluateBinding(index, decision.conditions)
			check.Conditions = append(check.Conditions, reason)
			if !matched {
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

// firstMember returns the effective member of a binding for the principal:
// its own member, a public principal, or a directory group or domain that
// contains it. With public principals prevented, the public ones don't count.
func firstMember(binding Binding, principal gateway.Principal, decision *Decision) string {
	for _, member := range binding.Members {
		if decision.publicPrevented && isPublicMember(member) {
			continue
		}
		if member == memberAllUsers || (principal.State == gateway.Authenticated && (member == principal.Subject || member == memberAllAuthenticated)) ||
			decision.memberships[strings.ToLower(member)] {
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
			if resource.Kind != kindProject && resource.Kind != "organization" {
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
	value.Signals = append([]Signal(nil), value.Signals...)
	value.Checks = append([]Check(nil), value.Checks...)
	for i := range value.Checks {
		value.Checks[i].Policies = append([]string(nil), value.Checks[i].Policies...)
		value.Checks[i].DenyPolicies = append([]string(nil), value.Checks[i].DenyPolicies...)
		value.Checks[i].Conditions = append([]string(nil), value.Checks[i].Conditions...)
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

// pabViolation enforces principal access boundaries. A service account
// NAME@PROJECT.iam.gserviceaccount.com is enrolled in the principal sets of its
// project and that project's folders and organization; a boundary stored on
// one of those nodes binds it. An enrolled principal may only reach resources
// inside the union of its boundaries' bound resources and their descendants.
func (e *Engine) pabViolation(ancestors []state.Resource, principal gateway.Principal) string {
	sets, err := e.principalSets(principal)
	if err != nil {
		return "Principal access boundary enrollment cannot be resolved: " + err.Error()
	}
	enrolled := false
	for _, set := range sets {
		pab := e.boundaries[set.Name]
		if pab == nil {
			continue
		}
		enrolled = true
		if pabBounds(pab, ancestors) {
			return ""
		}
	}
	if enrolled {
		return "The request is outside every principal access boundary the principal is subject to"
	}
	return ""
}

// principalSets resolves the resource-manager nodes whose principal sets
// contain the caller. Only service accounts of an indexed project are enrolled;
// users, Google-managed service agents, and anonymous callers are not.
func (e *Engine) principalSets(principal gateway.Principal) ([]state.Resource, error) {
	email, ok := strings.CutPrefix(principal.Subject, "serviceAccount:")
	if principal.State != gateway.Authenticated || !ok {
		return nil, nil
	}
	_, domain, _ := strings.Cut(email, "@")
	project, ok := strings.CutSuffix(domain, ".iam.gserviceaccount.com")
	if !ok {
		return nil, nil
	}
	name := projectNodePrefix + project
	if _, found := e.index.Get(name); !found {
		return nil, nil
	}
	return e.ancestry(Target{Name: name})
}

// pabBounds reports whether any boundary rule lists the target or one of its
// ancestors, by resource name or, for a project, by project number.
func pabBounds(pab *PABPolicy, ancestors []state.Resource) bool {
	for _, ancestor := range ancestors {
		for _, rule := range pab.Rules {
			for _, bound := range rule.BoundCloudResources {
				if bound == ancestor.Name || (ancestor.Kind == kindProject && ancestor.Number != "" && bound == projectNodePrefix+ancestor.Number) {
					return true
				}
			}
		}
	}
	return false
}

// MarkPerimeter records one VPC Service Controls perimeter over resources.
// Membership is reported in traces; nothing enforces it.
func (e *Engine) MarkPerimeter(name string, resources []string) error {
	if name == "" || len(resources) == 0 {
		return status.Error(codes.InvalidArgument, "A perimeter requires a name and resources")
	}
	for _, resource := range resources {
		if _, ok := e.index.Get(resource); !ok {
			return status.Error(codes.NotFound, "Perimeter resource is not registered: "+resource)
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, resource := range resources {
		e.perimeters[resource] = append(e.perimeters[resource], name)
	}
	return nil
}

// collectSignals reports the VPC-SC perimeters along the ancestor chain.
// Membership is a trace signal only: no request is refused for crossing a
// perimeter, and it never replaces an IAM verdict.
func (e *Engine) collectSignals(ancestors []state.Resource, decision *Decision) {
	for _, ancestor := range ancestors {
		if perimeters := e.perimeters[ancestor.Name]; len(perimeters) > 0 {
			decision.Signals = append(decision.Signals, Signal{
				Family: "vpc-sc", Verdict: "inside-perimeter",
				Detail: strings.Join(perimeters, ","), Resource: ancestor.Name,
			})
		}
	}
}

// ServeHTTP serves the decision lookup, GET /_local/iam/decisions/ID.
func (e *Engine) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	const prefix = "/_local/iam/decisions/"
	if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, prefix) {
		http.NotFound(w, r)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, prefix)
	decoded, err := hex.DecodeString(id)
	if err != nil || len(decoded) != 16 {
		http.NotFound(w, r)
		return
	}
	decision, ok := e.Decision(id)
	if !ok {
		http.NotFound(w, r)
		return
	}
	encoded, err := json.Marshal(decision)
	if err != nil {
		http.Error(w, `{"error":"response encoding failed"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(encoded)
}
