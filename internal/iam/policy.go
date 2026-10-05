package iam

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
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
	Version  int       `json:"version"`
	Bindings []Binding `json:"bindings"`
	ETag     string    `json:"etag"`
}
type unsupportedPolicy struct {
	features []string
	owner    string
	scope    state.Scope
}

type storedPolicy struct {
	policy Policy
	owner  string
	scope  state.Scope
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
	if policy.Version != 0 && policy.Version != 1 {
		return Policy{}, status.Error(codes.Unimplemented, "Only unconditional version-1 allow policies are supported")
	}
	if len(policy.Bindings) > 1000 {
		return Policy{}, status.Error(codes.InvalidArgument, "Policy has too many bindings")
	}
	for _, binding := range policy.Bindings {
		if len(binding.Condition) > 0 {
			return Policy{}, status.Error(codes.Unimplemented, "IAM Conditions are unsupported")
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
			if !(strings.HasPrefix(member, "user:") || strings.HasPrefix(member, "serviceAccount:")) || !strings.Contains(member, "@") || strings.ContainsAny(member, " \r\n\t") {
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
	policy.Version = 1
	policy.ETag = base64.StdEncoding.EncodeToString([]byte(fmt.Sprint(e.revision)))
	e.policies[resource] = storedPolicy{policy: policy, owner: owner.Service, scope: owner.Scope}
	return clonePolicy(policy), nil
}

// Unsupported configuration is retained as a fail-closed coverage marker;
// recording its presence never pretends to evaluate its policy semantics.
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
