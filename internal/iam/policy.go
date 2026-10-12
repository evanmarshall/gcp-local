package iam

import (
	"encoding/base64"
	"encoding/json"
	"regexp"
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

// PABPolicy bounds the resources the principal set of the resource-manager
// node it is stored on may reach; see pabViolation.
type PABPolicy struct {
	Rules []PABRule `json:"rules"`
}

// PABRule is one boundary rule: the resources its enrolled principals stay in.
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

// orgPolicyRecord is one stored constraint; dryRun keeps it advisory.
type orgPolicyRecord struct {
	constraint string
	dryRun     bool
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

// SetPolicy validates, compiles, and stores an allow policy. Validation runs in
// the order GCP reports failures, and nothing is stored unless every check passes.
func (e *Engine) SetPolicy(resource string, policy Policy) (Policy, error) {
	owner, ok := e.index.Get(resource)
	if !ok {
		return Policy{}, status.Error(codes.NotFound, "Policy resource is not registered")
	}
	if err := validatePolicyShape(policy); err != nil {
		return Policy{}, err
	}
	if policy.PAB != nil && (owner.Service != "resourcemanager" || owner.Kind == "") {
		return Policy{}, status.Error(codes.InvalidArgument, "A principal access boundary binds only to a project, folder, or organization principal set")
	}
	e.mu.Lock()
	directory, lookup := e.directory, e.customRoles
	previous := emptyPolicy()
	if stored, ok := e.policies[resource]; ok {
		previous = clonePolicy(stored.policy)
	}
	e.mu.Unlock()
	denies, err := e.compileDenies(policy.DenyRules, directory)
	if err != nil {
		return Policy{}, err
	}
	conditions, err := e.compileBindings(policy.Bindings, directory, roleContext{resource: resource, previous: previous, lookup: lookup})
	if err != nil {
		return Policy{}, err
	}
	return e.storePolicy(resource, owner, policy, conditions, denies)
}

// validatePolicyShape checks the version, version-gated features, deny rule
// shape, and binding count before any binding is compiled.
func validatePolicyShape(policy Policy) error {
	if err := validateVersion(policy); err != nil {
		return err
	}
	for _, rule := range policy.DenyRules {
		if len(rule.DeniedPrincipals) == 0 || len(rule.DeniedPermissions) == 0 {
			return status.Error(codes.InvalidArgument, "A deny rule requires denied principals and permissions")
		}
	}
	if len(policy.Bindings) > 1000 {
		return status.Error(codes.InvalidArgument, "Policy has too many bindings")
	}
	return nil
}

// validateVersion rejects unknown versions and v3-only features in older ones.
func validateVersion(policy Policy) error {
	if policy.Version != 0 && policy.Version != 1 && policy.Version != 3 {
		return status.Error(codes.Unimplemented, "Unsupported policy version")
	}
	if policy.Version == 3 {
		return nil
	}
	for _, binding := range policy.Bindings {
		if len(binding.Condition) > 0 {
			return status.Errorf(codes.InvalidArgument, "Specified policy version (%d) must be at least 3 based on the policy's contents. "+
				"For more information, please refer to https://cloud.google.com/iam/help/allow-policies/versions.", max(policy.Version, 1))
		}
	}
	if len(policy.DenyRules) > 0 {
		return status.Error(codes.Unimplemented, "Deny rules require policy version 3")
	}
	return nil
}

// Deny-policy identifier prefixes, the project node prefix, and its kind.
const (
	userIdentifierPrefix           = "principal://goog/subject/"
	serviceAccountIdentifierPrefix = "principal://iam.googleapis.com/projects/-/serviceAccounts/"
	projectNodePrefix              = "//cloudresourcemanager.googleapis.com/projects/"
	kindProject                    = "project"
)

// compileDenies validates each deny rule's principals and permissions and
// compiles its optional denial condition. An identifier or permission the
// engine cannot match is refused, so a rule never silently fails to deny; a
// group must be in the directory, which can't change once policies exist.
func (e *Engine) compileDenies(rules []DenyRule, directory directory) ([]storedDeny, error) {
	var compiled []storedDeny
	for _, rule := range rules {
		for _, principal := range append(append([]string(nil), rule.DeniedPrincipals...), rule.ExceptionPrincipals...) {
			if err := checkDenyPrincipal(principal, directory); err != nil {
				return nil, err
			}
		}
		denied, err := e.denyPermissions(rule.DeniedPermissions)
		if err != nil {
			return nil, err
		}
		excepted, err := e.denyPermissions(rule.ExceptionPermissions)
		if err != nil {
			return nil, err
		}
		deny := storedDeny{rule: DenyRule{DeniedPrincipals: append([]string(nil), rule.DeniedPrincipals...),
			ExceptionPrincipals: append([]string(nil), rule.ExceptionPrincipals...), DeniedPermissions: denied, ExceptionPermissions: excepted}}
		if len(rule.DenialCondition) > 0 {
			condition, err := compileCondition(rule.DenialCondition)
			if err != nil {
				return nil, status.Error(codes.Unimplemented, err.Error())
			}
			deny.condition = condition
		}
		compiled = append(compiled, deny)
	}
	return compiled, nil
}

// checkDenyPrincipal refuses a deny principal the engine can't match: a
// group outside the directory, or an unsupported identifier.
func checkDenyPrincipal(principal string, directory directory) error {
	if strings.HasPrefix(principal, "group:") || strings.HasPrefix(principal, groupSetPrefix) {
		_, err := directory.checkMember(principal)
		return err
	}
	if !denyPrincipalSupported(principal) {
		return status.Error(codes.Unimplemented, "Deny principal identifier is unsupported: "+principal)
	}
	return nil
}

// denyPrincipalSupported accepts the public set, GCP's v2 user and service
// account identifiers, and the allow-policy user:/serviceAccount: members.
func denyPrincipalSupported(principal string) bool {
	if principal == publicPrincipalSet {
		return true
	}
	for _, prefix := range []string{userIdentifierPrefix, serviceAccountIdentifierPrefix, "user:", "serviceAccount:"} {
		if email, ok := strings.CutPrefix(principal, prefix); ok && strings.Contains(email, "@") {
			return true
		}
	}
	return false
}

// denyPermissions converts v2 names (storage.googleapis.com/objects.get) to
// the v1 names checks use, and refuses a name no pinned role contains. A
// trailing * matches every permission with that prefix.
func (e *Engine) denyPermissions(permissions []string) ([]string, error) {
	converted := make([]string, 0, len(permissions))
	for _, permission := range permissions {
		if service, rest, ok := strings.Cut(permission, ".googleapis.com/"); ok {
			permission = service + "." + rest
		}
		prefix, wildcard := strings.CutSuffix(permission, "*")
		if (wildcard && !strings.Contains(prefix, ".")) || (!wildcard && !e.roleGrants(permission)) {
			return nil, status.Error(codes.Unimplemented, "Deny permission is outside the pinned roles: "+permission)
		}
		converted = append(converted, permission)
	}
	return converted, nil
}

// roleContext is what role validation reads: the policy's resource, its
// stored policy, and the custom-role lookup.
type roleContext struct {
	resource string
	previous Policy
	lookup   CustomRoleLookup
}

// compileBindings validates each binding in order and compiles its condition.
// Guards are keyed by the binding's own position in the policy.
func (e *Engine) compileBindings(bindings []Binding, directory directory, roles roleContext) ([]conditionGuard, error) {
	var guards []conditionGuard
	for index, binding := range bindings {
		if len(binding.Condition) > 0 {
			condition, err := compileCondition(binding.Condition)
			if err != nil {
				return nil, status.Error(codes.InvalidArgument, "Condition expression compilation failed: "+err.Error())
			}
			guards = append(guards, conditionGuard{bindingIndex: index, condition: condition})
		}
		if err := e.checkRole(binding, roles); err != nil {
			return nil, err
		}
		if err := validateMembers(binding.Members, directory); err != nil {
			return nil, err
		}
	}
	return guards, nil
}

// The public principals: everyone, and every authenticated caller.
const (
	memberAllUsers         = "allUsers"
	memberAllAuthenticated = "allAuthenticatedUsers"
)

// isPublicMember reports whether a member is a public principal.
func isPublicMember(member string) bool {
	return member == memberAllUsers || member == memberAllAuthenticated
}

// deletedMember is GCP's form of a deleted user or service account binding.
var deletedMember = regexp.MustCompile(`^deleted:(user|serviceAccount):[^\s?]+@[^\s?]+\?uid=[0-9]+$`)

// publicOrDeleted reports the members that name no live identity: the public
// principals, and a deleted principal, which reads back in its deleted form.
// Accepting the deleted form keeps read-modify-write working; it never
// matches a caller.
func publicOrDeleted(member string) bool {
	return isPublicMember(member) || deletedMember.MatchString(member)
}

// validateMembers accepts the public principals, user/service-account
// emails, and the directory's groups and domains.
func validateMembers(members []string, directory directory) error {
	if len(members) == 0 || len(members) > 1000 {
		return status.Error(codes.InvalidArgument, "Supply one to one thousand members per binding")
	}
	for _, member := range members {
		if err := checkMember(member, directory); err != nil {
			return err
		}
	}
	return nil
}

// checkMember validates one allow-policy member.
func checkMember(member string, directory directory) error {
	if publicOrDeleted(member) {
		return nil
	}
	if handled, err := directory.checkMember(member); handled {
		return err
	}
	if (!strings.HasPrefix(member, "user:") && !strings.HasPrefix(member, "serviceAccount:")) || !strings.Contains(member, "@") || strings.ContainsAny(member, " \r\n\t") {
		return status.Error(codes.Unimplemented, "IAM member type is unsupported")
	}
	return nil
}

// storePolicy commits a compiled policy under the etag check and assigns the
// next revision as its etag.
func (e *Engine) storePolicy(resource string, owner state.Resource, policy Policy, conditions []conditionGuard, denies []storedDeny) (Policy, error) {
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
		conditions: conditions, denies: denies, pab: policy.PAB,
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

// Forget drops everything attached to one resource: its allow policy,
// unsupported markers, org-policy constraints, and perimeter membership. The
// index calls it whenever a resource is removed.
func (e *Engine) Forget(resource string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.policies, resource)
	delete(e.unsupported, resource)
	delete(e.orgPolicies, resource)
	delete(e.perimeters, resource)
}

// RetireMember rewrites every binding of a deleted principal to the form GCP
// shows, deleted:serviceAccount:EMAIL?uid=UNIQUE_ID. A recreated account has
// a new unique ID and the same email, so it inherits none of the grants
// (https://cloud.google.com/iam/docs/service-accounts-delete-undelete).
func (e *Engine) RetireMember(member, uniqueID string) {
	retired := "deleted:" + member + "?uid=" + uniqueID
	e.mu.Lock()
	defer e.mu.Unlock()
	for name, stored := range e.policies {
		policy, changed := clonePolicy(stored.policy), false
		for i := range policy.Bindings {
			for j, existing := range policy.Bindings[i].Members {
				if existing == member {
					policy.Bindings[i].Members[j], changed = retired, true
				}
			}
		}
		if changed {
			e.revision++
			policy.ETag = base64.StdEncoding.EncodeToString([]byte(strconv.FormatUint(e.revision, 10)))
			stored.policy = policy
			e.policies[name] = stored
		}
	}
}

// RetireRole removes every binding of a permanently deleted custom role, as
// GCP does once the role's deletion is final, so a role later created with
// the same ID inherits none of them. Condition guards follow their bindings.
func (e *Engine) RetireRole(role string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for name, stored := range e.policies {
		kept := make([]Binding, 0, len(stored.policy.Bindings))
		var guards []conditionGuard
		for index, binding := range stored.policy.Bindings {
			if binding.Role == role {
				continue
			}
			for _, guard := range stored.conditions {
				if guard.bindingIndex == index {
					guards = append(guards, conditionGuard{bindingIndex: len(kept), condition: guard.condition})
				}
			}
			kept = append(kept, binding)
		}
		if len(kept) == len(stored.policy.Bindings) {
			continue
		}
		e.revision++
		stored.policy = clonePolicy(stored.policy)
		stored.policy.Bindings = kept
		stored.policy.ETag = base64.StdEncoding.EncodeToString([]byte(strconv.FormatUint(e.revision, 10)))
		stored.conditions = guards
		e.policies[name] = stored
	}
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
	e.clearSigningKeys(owner, scope)
}

// Wire renders a policy as GCP returns it: a policy without bindings, deny
// rules, or a boundary reads back as only its etag.
func (p Policy) Wire() any {
	if len(p.Bindings) == 0 && len(p.DenyRules) == 0 && p.PAB == nil {
		return map[string]any{"etag": p.ETag}
	}
	return p
}
