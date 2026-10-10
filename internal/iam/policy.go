package iam

import (
	"encoding/base64"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Binding struct {
	Role      string          `json:"role"`
	Members   []string        `json:"members"`
	Condition json.RawMessage `json:"condition,omitempty"`
}
type Policy struct {
	Version   int        `json:"version"`
	Bindings  []Binding  `json:"bindings"`
	DenyRules []DenyRule `json:"denyRules,omitempty"`
	// PAB is the principal access boundary policy attached to this resource.
	PAB  *PABPolicy `json:"principalAccessBoundary,omitempty"`
	ETag string     `json:"etag"`
}

// PABPolicy bounds the cloud resources service agents of this hierarchy may
// touch; a service-agent request outside every bound fails closed.
type PABPolicy struct {
	Rules []PABRule `json:"rules"`
}

// PABRule is one boundary rule: a resource list its service agents stay in.
type PABRule struct {
	BoundCloudResources []string `json:"boundCloudResources"`
}

// DenyRule is one v3 deny rule: a principal set that must not use a
// permission set, with optional exceptions and a denial condition.
type DenyRule struct {
	DeniedPrincipals     []string        `json:"deniedPrincipals"`
	ExceptionPrincipals  []string        `json:"exceptionPrincipals,omitempty"`
	DeniedPermissions    []string        `json:"deniedPermissions"`
	ExceptionPermissions []string        `json:"exceptionPermissions,omitempty"`
	DenialCondition      json.RawMessage `json:"denialCondition,omitempty"`
}

// storedDeny is a compiled deny rule.
type storedDeny struct {
	rule      DenyRule
	condition *evaluated
}
type unsupportedPolicy struct {
	features []string
	owner    string
	scope    state.Scope
}

type storedPolicy struct {
	policy     Policy
	owner      string
	scope      state.Scope
	conditions []conditionGuard
	denies     []storedDeny
	pab        *PABPolicy
}

func clonePolicy(p Policy) Policy {
	result := p
	result.Bindings = make([]Binding, len(p.Bindings))
	for i, binding := range p.Bindings {
		result.Bindings[i] = binding
		result.Bindings[i].Members = append([]string(nil), binding.Members...)
		result.Bindings[i].Condition = append(json.RawMessage(nil), binding.Condition...)
	}
	return result
}
func emptyPolicy() Policy { return Policy{Version: 1, Bindings: []Binding{}, ETag: "MA=="} }
func (e *Engine) GetPolicy(resource string) (Policy, error) {
	if _, ok := e.index.Get(resource); !ok {
		return Policy{}, status.Error(codes.NotFound, "Policy resource is not registered")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if stored, ok := e.policies[resource]; ok {
		return clonePolicy(stored.policy), nil
	}
	return emptyPolicy(), nil
}
func (e *Engine) SetPolicy(resource string, policy Policy) (Policy, error) {
	owner, ok := e.index.Get(resource)
	if !ok {
		return Policy{}, status.Error(codes.NotFound, "Policy resource is not registered")
	}
	if policy.Version != 0 && policy.Version != 1 && policy.Version != 3 {
		return Policy{}, status.Error(codes.Unimplemented, "Unsupported policy version")
	}
	if policy.Version != 3 {
		for _, binding := range policy.Bindings {
			if len(binding.Condition) > 0 {
				return Policy{}, status.Error(codes.Unimplemented, "Bindings with conditions require policy version 3")
			}
		}
		if len(policy.DenyRules) > 0 {
			return Policy{}, status.Error(codes.Unimplemented, "Deny rules require policy version 3")
		}
	}
	for _, rule := range policy.DenyRules {
		if len(rule.DeniedPrincipals) == 0 || len(rule.DeniedPermissions) == 0 {
			return Policy{}, status.Error(codes.InvalidArgument, "A deny rule requires denied principals and permissions")
		}
	}
	if len(policy.Bindings) > 1000 {
		return Policy{}, status.Error(codes.InvalidArgument, "Policy has too many bindings")
	}
	var compiledConditions []conditionGuard
	var compiledDenies []storedDeny
	for index, rule := range policy.DenyRules {
		compiled := storedDeny{rule: rule}
		if len(rule.DenialCondition) > 0 {
			condition, compileErr := compileCondition(rule.DenialCondition)
			if compileErr != nil {
				return Policy{}, status.Error(codes.Unimplemented, compileErr.Error())
			}
			compiled.condition = condition
		}
		_ = index
		compiledDenies = append(compiledDenies, compiled)
	}
	for _, binding := range policy.Bindings {
		if len(binding.Condition) > 0 {
			compiled, compileErr := compileCondition(binding.Condition)
			if compileErr != nil {
				return Policy{}, status.Error(codes.Unimplemented, compileErr.Error())
			}
			compiledConditions = append(compiledConditions, conditionGuard{
				bindingIndex: len(compiledConditions), condition: compiled,
			})
		}
		if _, ok := e.roles[binding.Role]; !ok {
			return Policy{}, status.Error(codes.Unimplemented, "Role is outside the pinned supported role set")
		}
		if len(binding.Members) == 0 || len(binding.Members) > 1000 {
			return Policy{}, status.Error(codes.InvalidArgument, "Supply one to one thousand members per binding")
		}
		for _, member := range binding.Members {
			if member == "allUsers" || member == "allAuthenticatedUsers" {
				continue
			}
			if (!strings.HasPrefix(member, "user:") && !strings.HasPrefix(member, "serviceAccount:")) || !strings.Contains(member, "@") || strings.ContainsAny(member, " \r\n\t") {
				return Policy{}, status.Error(codes.Unimplemented, "IAM member type is unsupported")
			}
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	previous := emptyPolicy()
	if stored, ok := e.policies[resource]; ok {
		previous = stored.policy
	}
	if policy.ETag != "" && policy.ETag != previous.ETag {
		return Policy{}, status.Error(codes.Aborted, "Policy etag does not match")
	}
	e.revision++
	policy = clonePolicy(policy)
	if policy.Version == 0 {
		policy.Version = 1
	}
	policy.ETag = base64.StdEncoding.EncodeToString([]byte(strconv.FormatUint(e.revision, 10)))
	e.policies[resource] = storedPolicy{
		policy: policy, owner: owner.Service, scope: owner.Scope,
		conditions: compiledConditions, denies: compiledDenies, pab: policy.PAB,
	}
	return clonePolicy(policy), nil
}

// MarkUnsupported retains unsupported configuration as a fail-closed coverage
// marker; recording its presence never pretends to evaluate its semantics.
func (e *Engine) MarkUnsupported(resource string, features []string) error {
	owner, ok := e.index.Get(resource)
	if !ok {
		return status.Error(codes.NotFound, "Unsupported-policy resource is not registered")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.unsupported[resource] = unsupportedPolicy{features: append([]string(nil), features...), owner: owner.Service, scope: owner.Scope}
	return nil
}
func (e *Engine) Forget(resource string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.policies, resource)
	delete(e.unsupported, resource)
}
func (e *Engine) ClearScope(owner string, scope state.Scope) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for name, stored := range e.policies {
		if stored.owner == owner && stored.scope == scope {
			delete(e.policies, name)
			delete(e.unsupported, name)
		}
	}
	for name, marker := range e.unsupported {
		if marker.owner == owner && marker.scope == scope {
			delete(e.unsupported, name)
		}
	}
}
