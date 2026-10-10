package iam

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
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
	if method == "" || len(plan.Baseline) == 0 {
		return errRuleIncomplete
	}
	known := func(permission string) bool {
		for _, role := range e.roles {
			if role[permission] {
				return true
			}
		}
		return false
	}
	rule := Rule{Baseline: append([]string(nil), plan.Baseline...), When: map[string][]string{}}
	for _, permission := range plan.Baseline {
		if !known(permission) {
			return fmt.Errorf("%w: %s", errBaselineUnknown, permission)
		}
	}
	for feature, permissions := range plan.When {
		if feature == "" || len(permissions) == 0 {
			return errFeatureRequired
		}
		for _, permission := range permissions {
			if !known(permission) {
				return fmt.Errorf("%w: %s", errFeatureUnknown, permission)
			}
		}
		rule.When[feature] = append([]string(nil), permissions...)
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
	decision := Decision{ID: hex.EncodeToString(raw), TraceID: call.TraceID, Method: call.Method, Principal: principal, Complete: complete, Checks: []Check{}}
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
		return finish("cancelled", "Request context ended", status.FromContextError(err).Code())
	}
	rule, ok := e.rules[call.Method]
	if !ok {
		return finish("unsupported", "No permission plan is registered for this method", codes.Unimplemented)
	}
	if principal.State == gateway.Unknown || principal.State == gateway.Expired {
		return finish("unauthenticated", "Unknown or expired credential", codes.Unauthenticated)
	}
	if principal.State != gateway.Authenticated && principal.State != gateway.Anonymous {
		return finish("unsupported", "Unknown principal state", codes.Unimplemented)
	}
	if principal.State == gateway.Authenticated {
		if principal.Subject == "" {
			return finish("unauthenticated", "Authenticated caller has no named principal", codes.Unauthenticated)
		}
		supported := false
		for _, scope := range principal.Scopes {
			if scope == "https://www.googleapis.com/auth/cloud-platform" {
				supported = true
			}
			service := ""
			if scope == "https://www.googleapis.com/auth/devstorage.full_control" {
				service = "storage."
			}
			if scope == "https://www.googleapis.com/auth/pubsub" {
				service = "pubsub."
			}
			if service != "" {
				matches := true
				for _, permission := range rule.Baseline {
					matches = matches && strings.HasPrefix(permission, service)
				}
				for _, permissions := range rule.When {
					for _, permission := range permissions {
						matches = matches && strings.HasPrefix(permission, service)
					}
				}
				supported = supported || matches
			}
		}
		if !supported {
			return finish("unsupported", "OAuth scope enforcement is unverified for this scope set", codes.Unimplemented)
		}
	}
	permissions := append([]string(nil), rule.Baseline...)
	if complete {
		for feature := range features {
			if _, ok := rule.When[feature]; !ok {
				return finish("unsupported", "Unknown request/state feature", codes.Unimplemented)
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
				return finish("unsupported", "Required request/state feature was not resolved: "+name, codes.Unimplemented)
			}
			if enabled {
				permissions = append(permissions, rule.When[name]...)
			}
		}
	}
	ancestors, err := e.ancestry(target)
	if err != nil {
		return finish("unsupported", err.Error(), codes.Unimplemented)
	}
	if call.Project != "" && ancestors[0].Scope.Project != call.Project {
		return finish("unsupported", "Request project disagrees with resource ownership", codes.Unimplemented)
	}
	for _, ancestor := range ancestors {
		if len(e.unsupported[ancestor.Name].features) > 0 {
			return finish("unsupported", "Unsupported policy configuration at "+ancestor.Name, codes.Unimplemented)
		}
	}
	seen := map[string]bool{}
	denied := ""
	for _, permission := range permissions {
		if seen[permission] {
			continue
		}
		seen[permission] = true
		check := Check{Permission: permission, Target: target.Name, Policies: []string{}}
		for _, ancestor := range ancestors {
			check.Policies = append(check.Policies, ancestor.Name)
			policy := e.policies[ancestor.Name].policy
			for _, binding := range policy.Bindings {
				for _, member := range binding.Members {
					matches := (member == "allUsers") || (principal.State == gateway.Authenticated && (member == principal.Subject || member == "allAuthenticatedUsers"))
					if matches && e.roles[binding.Role][permission] && !check.Allowed {
						check.Allowed = true
						check.GrantPolicy = ancestor.Name
						check.Role = binding.Role
						check.Member = member
					}
				}
			}
		}
		decision.Checks = append(decision.Checks, check)
		if !check.Allowed && denied == "" {
			denied = permission
		}
	}
	if denied != "" {
		code := codes.PermissionDenied
		if principal.State == gateway.Anonymous {
			code = codes.Unauthenticated
		}
		return finish("deny", "Missing permission: "+denied, code)
	}
	if !complete {
		return finish("preflight", "Baseline permissions satisfied; state-dependent authorization is still required", codes.OK)
	}
	return finish("allow", "All supported permission checks passed", codes.OK)
}
func (e *Engine) ancestry(target Target) ([]state.Resource, error) {
	resource, found := e.index.Get(target.Name)
	if found {
		if target.Parent != "" && target.Parent != resource.Parent {
			return nil, errTargetParent
		}
	} else {
		parent, ok := e.index.Get(target.Parent)
		if !ok || parent.Service != "storage" || parent.Kind != "bucket" || !strings.HasPrefix(target.Name, parent.Name+"/objects/") || target.Name == parent.Name+"/objects/" {
			return nil, errTargetShape
		}
		resource = parent // Object data stays in the backend; bucket anchors its IAM.
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
		resource, found = e.index.Get(resource.Parent)
		if !found {
			return nil, errHierarchyMissing
		}
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
