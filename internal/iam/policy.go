package iam

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"cloud.google.com/go/orgpolicy/apiv2/orgpolicypb"
	"github.com/evanmarshall/gcp-local/internal/iam/orgpolicy"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type Binding struct {
	Role      string          `json:"role"`
	Members   []string        `json:"members"`
	Condition json.RawMessage `json:"condition,omitempty"`
}

// Policy is a google.iam.v1 allow policy.
type Policy struct {
	Version  int       `json:"version"`
	Bindings []Binding `json:"bindings"`
	// AuditConfigs are stored and returned on projects, folders, and
	// organizations. They aren't enforced: no audit logs are written.
	AuditConfigs []AuditConfig `json:"auditConfigs,omitempty"`
	ETag         string        `json:"etag"`
}

// AuditConfig is one service's audit logging configuration in an allow policy.
type AuditConfig struct {
	Service         string           `json:"service"`
	AuditLogConfigs []AuditLogConfig `json:"auditLogConfigs,omitempty"`
}

// AuditLogConfig enables one log type, minus its exempted members.
type AuditLogConfig struct {
	LogType         string   `json:"logType"`
	ExemptedMembers []string `json:"exemptedMembers,omitempty"`
}

// PABPolicy bounds the resources the principal set of the resource-manager
// node it is declared on may reach; see pabViolation. Only the bootstrap
// configuration declares one.
type PABPolicy struct {
	Rules []PABRule `json:"rules"`
}

// PABRule is one boundary rule: the resources its enrolled principals stay in.
type PABRule struct {
	BoundCloudResources []string `json:"boundCloudResources"`
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
}

func clonePolicy(p Policy) Policy {
	result := p
	result.Bindings = make([]Binding, len(p.Bindings))
	for i, binding := range p.Bindings {
		result.Bindings[i] = binding
		result.Bindings[i].Members = append([]string(nil), binding.Members...)
		result.Bindings[i].Condition = append(json.RawMessage(nil), binding.Condition...)
	}
	if p.AuditConfigs != nil {
		result.AuditConfigs = make([]AuditConfig, len(p.AuditConfigs))
		for i, config := range p.AuditConfigs {
			result.AuditConfigs[i] = AuditConfig{Service: config.Service, AuditLogConfigs: make([]AuditLogConfig, len(config.AuditLogConfigs))}
			for j, log := range config.AuditLogConfigs {
				result.AuditConfigs[i].AuditLogConfigs[j] = AuditLogConfig{LogType: log.LogType, ExemptedMembers: append([]string(nil), log.ExemptedMembers...)}
			}
		}
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
// the order GCP reports failures, and nothing is stored unless every check
// passes. Members the policy adds must pass iam.allowedPolicyMemberDomains
// (an *OrgPolicyError otherwise); SetPolicyFor also traces that check.
func (e *Engine) SetPolicy(resource string, policy Policy) (Policy, error) {
	return e.setPolicy("", resource, policy)
}

// SetPolicyFor is SetPolicy after the decision decisionID authorized the
// write: the organization policy verdicts are traced on that decision.
func (e *Engine) SetPolicyFor(decisionID, resource string, policy Policy) (Policy, error) {
	return e.setPolicy(decisionID, resource, policy)
}

func (e *Engine) setPolicy(decisionID, resource string, policy Policy) (Policy, error) {
	owner, ok := e.index.Get(resource)
	if !ok {
		return Policy{}, status.Error(codes.NotFound, "Policy resource is not registered")
	}
	if err := validatePolicyShape(policy); err != nil {
		return Policy{}, err
	}
	if err := checkPlacement(owner, policy); err != nil {
		return Policy{}, err
	}
	e.mu.Lock()
	directory, lookup := e.directory, e.customRoles
	previous := emptyPolicy()
	if stored, ok := e.policies[resource]; ok {
		previous = clonePolicy(stored.policy)
	}
	e.mu.Unlock()
	if err := checkConditionalWrite(previous, policy); err != nil {
		return Policy{}, err
	}
	conditions, err := e.compileBindings(policy.Bindings, directory, roleContext{resource: resource, previous: previous, lookup: lookup})
	if err != nil {
		return Policy{}, err
	}
	if err := checkAuditConfigs(policy.AuditConfigs, directory); err != nil {
		return Policy{}, err
	}
	if err := e.checkMemberDomains(decisionID, resource, previous, policy); err != nil {
		return Policy{}, err
	}
	return e.storePolicy(resource, owner, policy, conditions)
}

// resourceManager is the service that owns organizations, folders, and
// projects in the index.
const resourceManager = "resourcemanager"

// checkPlacement refuses audit configs outside resource-manager nodes, where
// they are stored and not modeled elsewhere.
func checkPlacement(owner state.Resource, policy Policy) error {
	if len(policy.AuditConfigs) > 0 && owner.Service != resourceManager {
		return status.Error(codes.Unimplemented, "Audit configs on a "+owner.Service+" resource policy are not modeled")
	}
	return nil
}

// checkConditionalWrite refuses a write below version 3 to a policy with a
// conditional binding; only a write without an etag may replace it blindly
// (Policy.version reference; recorded: project-iam-policy-versions).
func checkConditionalWrite(previous, policy Policy) error {
	if policy.ETag != "" && policy.Version < 3 && previous.View(3).Version == 3 {
		return status.Errorf(codes.InvalidArgument, "Specified policy version (%d) must be at least 3 based on the policy's contents. "+
			"For more information, please refer to https://cloud.google.com/iam/help/allow-policies/versions.", max(policy.Version, 1))
	}
	return nil
}

// validatePolicyShape checks the version, version-gated features, and
// binding count before any binding is compiled.
func validatePolicyShape(policy Policy) error {
	if err := validateVersion(policy); err != nil {
		return err
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
	return nil
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
func (e *Engine) storePolicy(resource string, owner state.Resource, policy Policy, conditions []conditionGuard) (Policy, error) {
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
		policy: policy, owner: owner.Service, scope: owner.Scope, conditions: conditions,
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

// Forget drops everything attached to one resource: its allow policy, deny
// policies, boundary, unsupported markers, organization policies, and
// perimeter membership. The index calls it whenever a resource is removed.
func (e *Engine) Forget(resource string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.policies, resource)
	delete(e.denyPolicies, resource)
	delete(e.boundaries, resource)
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

// Wire renders a policy as GCP returns it: a policy without bindings or
// audit configs reads back as only its etag.
func (p Policy) Wire() any {
	if len(p.Bindings) == 0 && len(p.AuditConfigs) == 0 {
		return map[string]any{"etag": p.ETag}
	}
	return p
}

// OrgPolicyError is a mutation an enforced organization policy refuses.
// Providers render it as their service reports the refusal. GRPCStatus is
// the form IAM returns (recorded: orgpolicy-constraint-enforcement, a
// service account policy naming allUsers): FAILED_PRECONDITION with a
// PreconditionFailure naming the constraint and the refused value.
type OrgPolicyError struct {
	// Constraint is the constraint ID ("gcp.resourceLocations").
	Constraint string
	// Value is the refused list value (a member, a location); "" for a
	// boolean constraint.
	Value string
	// Resource is the index name of the resource the mutation acts on.
	Resource string
}

// MemberDomainsMessage is GCP's refusal of a policy member outside the
// permitted customers (recorded for Storage and IAM).
const MemberDomainsMessage = "One or more users named in the policy do not belong to a permitted customer."

func (v *OrgPolicyError) Error() string { return v.GRPCStatus().Message() }

// GRPCStatus renders the violation for a provider that passes it through.
func (v *OrgPolicyError) GRPCStatus() *status.Status {
	message := "Request violates constraint 'constraints/" + v.Constraint + "'"
	description := message
	if v.Constraint == orgpolicy.MemberDomains {
		message, description = MemberDomainsMessage, "User "+v.Value+" is not in permitted organization."
	}
	subject := "orgpolicy:" + relativeName(v.Resource)
	if v.Value != "" {
		subject += "?configvalue=" + v.Value
	}
	base := status.New(codes.FailedPrecondition, message)
	detailed, err := base.WithDetails(&errdetails.PreconditionFailure{Violations: []*errdetails.PreconditionFailure_Violation{
		{Type: "constraints/" + v.Constraint, Subject: subject, Description: description},
	}})
	if err != nil {
		return base
	}
	return detailed
}

// relativeName strips the service host from a full resource name.
func relativeName(name string) string {
	if rest, ok := strings.CutPrefix(name, "//"); ok {
		if _, path, found := strings.Cut(rest, "/"); found {
			return path
		}
	}
	return name
}

// OrgPolicy returns a copy of the organization policy node stores for a
// constraint. node is a hierarchy node's index name.
func (e *Engine) OrgPolicy(node, constraint string) (*orgpolicypb.Policy, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	policy, ok := e.orgPolicies[node][constraint]
	if !ok {
		return nil, false
	}
	return proto.CloneOf(policy), true
}

// OrgPolicies returns copies of a node's organization policies in catalog
// order.
func (e *Engine) OrgPolicies(node string) []*orgpolicypb.Policy {
	e.mu.Lock()
	defer e.mu.Unlock()
	var policies []*orgpolicypb.Policy
	for _, constraint := range orgpolicy.Catalog() {
		if policy, ok := e.orgPolicies[node][constraint.Name]; ok {
			policies = append(policies, proto.CloneOf(policy))
		}
	}
	return policies
}

// SetOrgPolicy validates and stores an organization policy on a hierarchy
// node, replacing the node's policy for the same constraint. Bootstrap and
// the Organization Policy API both write through it. A constraint the
// emulator doesn't enforce is refused: storing it would claim an
// enforcement that never happens. The caller sets the name and Seals it.
func (e *Engine) SetOrgPolicy(node, constraint string, policy *orgpolicypb.Policy) error {
	if _, ok := e.index.Get(node); !ok || !strings.HasPrefix(node, hierarchyPrefix) {
		return status.Error(codes.NotFound, "Organization policy resource is not registered")
	}
	definition, ok := orgpolicy.Lookup(constraint)
	if !ok {
		return status.Errorf(codes.Unimplemented, "Constraint constraints/%s is not enforced locally", constraint)
	}
	if err := orgpolicy.Validate(definition, policy); err != nil {
		return fmt.Errorf("validate the organization policy: %w", err)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.orgPolicies[node] == nil {
		e.orgPolicies[node] = map[string]*orgpolicypb.Policy{}
	}
	e.orgPolicies[node][constraint] = proto.CloneOf(policy)
	return nil
}

// DeleteOrgPolicy removes a node's policy for a constraint and reports
// whether one was stored.
func (e *Engine) DeleteOrgPolicy(node, constraint string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.orgPolicies[node][constraint]
	delete(e.orgPolicies[node], constraint)
	if len(e.orgPolicies[node]) == 0 {
		delete(e.orgPolicies, node)
	}
	return ok
}

// EffectiveOrgPolicy is the spec getEffectivePolicy reports for a constraint
// at a node: the hierarchy's specs merged with conditions evaluated.
func (e *Engine) EffectiveOrgPolicy(node, constraint string) (*orgpolicypb.PolicySpec, error) {
	definition, ok := orgpolicy.Lookup(constraint)
	if !ok {
		return nil, status.Errorf(codes.Unimplemented, "Constraint constraints/%s is not enforced locally", constraint)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	rules, _, err := e.effectiveRules(node, definition, false)
	if err != nil {
		return nil, err
	}
	return orgpolicy.Render(definition, rules), nil
}

// OrgPolicyEnforced reports whether a boolean constraint's spec applies at
// resource, without tracing: Storage reads public access prevention with it.
func (e *Engine) OrgPolicyEnforced(resource, constraint string) (bool, error) {
	definition, ok := orgpolicy.Lookup(constraint)
	if !ok || definition.List {
		return false, status.Errorf(codes.Internal, "Constraint constraints/%s is not an enforced boolean constraint", constraint)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.orgPolicies) == 0 {
		return false, nil
	}
	rules, _, err := e.effectiveRules(resource, definition, false)
	return orgpolicy.Enforced(rules), err
}

// CheckOrgPolicy evaluates one enforced constraint for a mutation of
// resource (its index name, or its project's for a create) after the
// decision decisionID authorized it, and traces the verdict on it. A boolean
// constraint takes no values; a list constraint checks each value. The
// first value the spec refuses is an *OrgPolicyError; a dryRunSpec
// verdict is only traced. Call it at the mutation, after Authorize, so an
// IAM denial comes first (recorded for service account key creation).
func (e *Engine) CheckOrgPolicy(decisionID, resource, constraint string, values ...string) error {
	definition, ok := orgpolicy.Lookup(constraint)
	if !ok {
		return status.Errorf(codes.Internal, "Constraint constraints/%s is not enforced", constraint)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.checkOrgPolicy(decisionID, resource, definition, values)
}

// checkMemberDomains applies iam.allowedPolicyMemberDomains to the members a
// policy write adds. Members already present stay: GCP's constraint is not
// retroactive, and a read-modify-write must keep working.
func (e *Engine) checkMemberDomains(decisionID, resource string, previous, policy Policy) error {
	present := map[string]bool{}
	for _, binding := range previous.Bindings {
		for _, member := range binding.Members {
			present[member] = true
		}
	}
	var added []string
	for _, binding := range policy.Bindings {
		for _, member := range binding.Members {
			if !present[member] && !slices.Contains(added, member) {
				added = append(added, member)
			}
		}
	}
	if len(added) == 0 {
		return nil
	}
	definition, _ := orgpolicy.Lookup(orgpolicy.MemberDomains)
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.checkOrgPolicy(decisionID, resource, definition, added)
}

// checkOrgPolicy evaluates and traces one constraint. Without any stored
// organization policy there is nothing to enforce. The caller holds mu.
func (e *Engine) checkOrgPolicy(decisionID, resource string, definition orgpolicy.Constraint, values []string) error {
	if len(e.orgPolicies) == 0 {
		return nil
	}
	rules, dryRun, err := e.effectiveRules(resource, definition, false)
	if err != nil {
		return err
	}
	value, refused := e.refusedValue(definition, rules, values)
	e.traceOrgPolicy(decisionID, resource, definition, "", values, value, refused)
	if dryRun {
		dryRules, _, err := e.effectiveRules(resource, definition, true)
		if err != nil {
			return err
		}
		dryValue, dryRefused := e.refusedValue(definition, dryRules, values)
		e.traceOrgPolicy(decisionID, resource, definition, "dry-run ", values, dryValue, dryRefused)
	}
	if refused {
		return &OrgPolicyError{Constraint: definition.Name, Value: value, Resource: resource}
	}
	return nil
}

// effectiveRules merges the constraint's specs along resource's ancestry,
// root first. The dry-run chain takes each node's dryRunSpec, or its spec
// where it has none. hasDryRun reports whether any node has a dryRunSpec.
// An unresolvable hierarchy fails closed. The caller holds mu.
func (e *Engine) effectiveRules(resource string, definition orgpolicy.Constraint, dryRun bool) ([]*orgpolicypb.PolicySpec_PolicyRule, bool, error) {
	ancestors, err := e.ancestry(Target{Name: resource})
	if err != nil {
		return nil, false, status.Error(codes.Unimplemented, "Organization policy hierarchy cannot be resolved: "+err.Error())
	}
	chain := make([]*orgpolicypb.PolicySpec, 0, len(ancestors))
	hasDryRun := false
	for _, ancestor := range slices.Backward(ancestors) {
		policy := e.orgPolicies[ancestor.Name][definition.Name]
		spec := policy.GetSpec()
		if policy.GetDryRunSpec() != nil {
			hasDryRun = true
			if dryRun {
				spec = policy.GetDryRunSpec()
			}
		}
		chain = append(chain, spec)
	}
	rules, err := orgpolicy.Effective(definition, chain)
	if err != nil {
		return nil, false, status.Error(codes.Internal, err.Error())
	}
	return rules, hasDryRun, nil
}

// refusedValue returns the first value the effective rules refuse; a
// boolean constraint refuses when enforced. The caller holds mu.
func (e *Engine) refusedValue(definition orgpolicy.Constraint, rules []*orgpolicypb.PolicySpec_PolicyRule, values []string) (string, bool) {
	if !definition.List {
		return "", orgpolicy.Enforced(rules)
	}
	for _, value := range values {
		matches := func(entry string) bool { return orgpolicy.LocationMatches(entry, value) }
		if definition.Name == orgpolicy.MemberDomains {
			customer := e.memberCustomer(value)
			matches = func(entry string) bool { return customer != "" && strings.TrimPrefix(entry, "is:") == customer }
		}
		if !orgpolicy.Allows(rules, matches) {
			return value, true
		}
	}
	return "", false
}

// traceOrgPolicy appends one verdict to a remembered decision. The caller
// holds mu.
func (e *Engine) traceOrgPolicy(decisionID, resource string, definition orgpolicy.Constraint, mode string, values []string, refused string, violation bool) {
	decision, ok := e.decisions[decisionID]
	if !ok {
		return
	}
	verdict, detail := mode+"allow", definition.Reference()
	if violation {
		verdict = mode + "violation"
		if refused != "" {
			detail += ": " + refused
		}
	} else if len(values) > 0 {
		detail += ": " + strings.Join(values, ", ")
	}
	decision.Signals = append(decision.Signals, Signal{Family: "org-policy", Verdict: verdict, Detail: detail, Resource: resource})
	e.decisions[decisionID] = decision
}

// memberCustomer is the directory customer an IAM member belongs to, or ""
// for none: users of a declared domain, declared groups, declared domains,
// and service accounts of indexed projects (Google-managed ones by project
// number). allUsers, allAuthenticatedUsers, and everyone else belong to no
// customer. The caller holds mu.
func (e *Engine) memberCustomer(member string) string {
	group := func(email string) bool { return e.directory.groups[email] }
	forms := []struct {
		prefix  string
		belongs func(string) bool
	}{
		{"user:", func(email string) bool {
			_, domain, _ := strings.Cut(email, "@")
			return e.directory.domains[domain]
		}},
		{"group:", group},
		{strings.ToLower(groupSetPrefix), group},
		{"domain:", func(domain string) bool { return e.directory.domains[domain] }},
		{"serviceaccount:", e.localAccount},
	}
	lower := strings.ToLower(member)
	for _, form := range forms {
		if rest, ok := strings.CutPrefix(lower, form.prefix); ok && form.belongs(rest) {
			return e.directory.customer
		}
	}
	return ""
}

// hierarchyPrefix starts the index name of every organization, folder, and
// project.
const hierarchyPrefix = "//cloudresourcemanager.googleapis.com/"

// localAccount reports a service account of an indexed project: a
// Google-managed account named by the project's number (service-NUMBER@...,
// as the Storage and gcp-sa-* service agents are, NUMBER-compute@developer,
// NUMBER@cloudservices), or NAME@PROJECT.iam.gserviceaccount.com.
func (e *Engine) localAccount(email string) bool {
	local, domain, _ := strings.Cut(email, "@")
	if !serviceAccountEmail(email) {
		return false
	}
	if number := managedAccountNumber.FindStringSubmatch(local); number != nil {
		_, found := e.index.ProjectByNumber(number[1])
		return found
	}
	project, ok := strings.CutSuffix(domain, ".iam.gserviceaccount.com")
	_, found := e.index.Get(projectNodePrefix + project)
	return ok && found
}

// managedAccountNumber reads the project number from a Google-managed
// account's local part.
var managedAccountNumber = regexp.MustCompile(`^(?:service-)?([0-9]+)(?:-compute)?$`)

// auditLogTypes are the AuditLogConfig log types a policy may enable.
var auditLogTypes = map[string]bool{"ADMIN_READ": true, "DATA_WRITE": true, "DATA_READ": true}

// checkAuditConfigs validates each audit config's service, log types, and
// exempted members, as GCP refuses a malformed one.
func checkAuditConfigs(configs []AuditConfig, directory directory) error {
	for _, config := range configs {
		if config.Service == "" {
			return status.Error(codes.InvalidArgument, "An audit config requires a service")
		}
		for _, log := range config.AuditLogConfigs {
			if !auditLogTypes[log.LogType] {
				return status.Errorf(codes.InvalidArgument, "Invalid audit log type %q", log.LogType)
			}
			for _, member := range log.ExemptedMembers {
				if err := checkMember(member, directory); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// View renders a stored policy as getIamPolicy returns it for a requested
// version; anything below 3 reads as 1 (https://cloud.google.com/iam/docs/policies#versions,
// recorded: project-iam-policy-versions).
// The version is 3 only for a policy with a conditional binding read at
// version 3. Read at a lower version, each conditional binding loses its
// condition and its role gains "_withcond_" and an opaque hash of the
// condition, so a version-1 client never mistakes it for an unconditional
// grant.
func (p Policy) View(requested int) Policy {
	view := clonePolicy(p)
	view.Version = 1
	for i, binding := range view.Bindings {
		if len(binding.Condition) == 0 {
			continue
		}
		if requested >= 3 {
			view.Version = 3
			continue
		}
		digest := sha256.Sum256(binding.Condition)
		view.Bindings[i].Role += "_withcond_" + hex.EncodeToString(digest[:])[:withcondLength]
		view.Bindings[i].Condition = nil
	}
	return view
}

// withcondLength is the length of the hash a version-1 read appends to a
// conditional binding's role.
const withcondLength = 20
