package iam

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	iamv2 "cloud.google.com/go/iam/apiv2/iampb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam/orgpolicy"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Deny policy limits per attachment point (https://cloud.google.com/iam/quotas#limits).
const (
	maxDenyPolicies   = 500
	maxDenyRules      = 500 // across a resource's policies, so per policy too
	maxDenyPrincipals = 2500
	maxDenyGroups     = 500
)

// Deny-policy identifier prefixes, the project node prefix, and its kind.
const (
	userIdentifierPrefix           = "principal://goog/subject/"
	serviceAccountIdentifierPrefix = "principal://iam.googleapis.com/projects/-/serviceAccounts/"
	projectNodePrefix              = "//cloudresourcemanager.googleapis.com/projects/"
	kindProject                    = "project"
	// publicPrincipalSet is the deny-policy identifier for every caller,
	// including anonymous ones.
	publicPrincipalSet = "principalSet://goog/public:all"
	// denyKind is the only IAM v2 policy kind, as its collection and its
	// kind field name it.
	denyKind, denyCollection = "DenyPolicy", "denypolicies"
	// iamDomain is the ErrorInfo domain of the IAM v2 API's errors.
	iamDomain = "iam.googleapis.com"
)

// denyPolicy is one stored deny policy: its wire message and compiled rules.
type denyPolicy struct {
	id      string
	message *iamv2.Policy
	rules   []storedDeny
}

// storedDeny is a compiled deny rule. Principals are v2 identifiers as
// written; permissions are v1 names (storage.objects.get), as checks use.
type storedDeny struct {
	principals, exceptionPrincipals   []string
	permissions, exceptionPermissions []string
	// condition is the denial condition's expression, "" for none. Tags
	// aren't modeled, so its verdict is fixed when it is compiled: applies
	// reports whether it is true or can't be evaluated.
	condition string
	applies   bool
}

// DenyPolicyName is the name GCP gives a deny policy on node:
// policies/cloudresourcemanager.googleapis.com%2Fprojects%2FNUMBER/denypolicies/ID.
func DenyPolicyName(node state.Resource, id string) string {
	return DenyParent(node) + "/" + id
}

// DenyParent is the deny-policy collection of node, with a project named by
// number, as GCP returns it.
func DenyParent(node state.Resource) string {
	return "policies/" + url.PathEscape("cloudresourcemanager.googleapis.com/"+orgpolicy.Parent(node)) + "/" + denyCollection
}

// DenyNode resolves an attachment point,
// "cloudresourcemanager.googleapis.com/projects/P" (by ID or number), a
// folder, or an organization, to its hierarchy node.
func DenyNode(index *state.Index, attachment string) (state.Resource, bool) {
	parent, ok := strings.CutPrefix(attachment, "cloudresourcemanager.googleapis.com/")
	if !ok {
		return state.Resource{}, false
	}
	return orgpolicy.Node(index, parent)
}

// ParseDenyPolicyName splits "policies/ATTACHMENT/denypolicies/ID", with
// the attachment point URL-encoded, into its attachment point and ID.
func ParseDenyPolicyName(name string) (string, string, bool) {
	parts := strings.Split(name, "/")
	if len(parts) != 4 || parts[0] != "policies" || parts[2] != denyCollection || parts[3] == "" {
		return "", "", false
	}
	attachment, err := url.PathUnescape(parts[1])
	return attachment, parts[3], err == nil
}

// DenyPolicies returns copies of node's deny policies in creation order.
func (e *Engine) DenyPolicies(node string) []*iamv2.Policy {
	e.mu.Lock()
	defer e.mu.Unlock()
	policies := make([]*iamv2.Policy, 0, len(e.denyPolicies[node]))
	for _, stored := range e.denyPolicies[node] {
		policies = append(policies, proto.CloneOf(stored.message))
	}
	return policies
}

// DenyPolicy returns a copy of node's deny policy id.
func (e *Engine) DenyPolicy(node, id string) (*iamv2.Policy, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, stored := range e.denyPolicies[node] {
		if stored.id == id {
			return proto.CloneOf(stored.message), true
		}
	}
	return nil, false
}

// PutDenyPolicy validates policy's rules and stores it as node's deny policy
// id, creating it or replacing its rules, display name, and annotations. The
// server-owned fields (name, uid, kind, etag, times) are set here; an
// existing policy keeps its uid and create time. The caller checks
// existence and etags. It returns the stored policy.
func (e *Engine) PutDenyPolicy(node state.Resource, id string, policy *iamv2.Policy, now time.Time) (*iamv2.Policy, error) {
	if !strings.HasPrefix(node.Name, hierarchyPrefix) || node.Kind == "" {
		return nil, status.Error(codes.InvalidArgument, "Deny policies attach only to projects, folders, and organizations")
	}
	e.mu.Lock()
	directory := e.directory
	e.mu.Unlock()
	rules, err := e.compileDenyRules(policy.GetRules(), directory)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	stored := denyPolicy{id: id, message: &iamv2.Policy{
		Name: DenyPolicyName(node, id), Kind: denyKind, DisplayName: policy.GetDisplayName(),
		Annotations: policy.GetAnnotations(), Rules: policy.GetRules(), UpdateTime: timestamppb.New(now),
	}, rules: rules}
	current := slices.IndexFunc(e.denyPolicies[node.Name], func(p denyPolicy) bool { return p.id == id })
	if current >= 0 {
		previous := e.denyPolicies[node.Name][current].message
		stored.message.Uid, stored.message.CreateTime = previous.GetUid(), previous.GetCreateTime()
	} else {
		stored.message.Uid, stored.message.CreateTime = newUID(), timestamppb.New(now)
	}
	if err := checkDenyLimits(node, e.denyPolicies[node.Name], current, stored); err != nil {
		return nil, err
	}
	e.revision++
	stored.message.Etag = base64.StdEncoding.EncodeToString([]byte(strconv.FormatUint(e.revision, 10)))
	stored.message = proto.CloneOf(stored.message)
	if current >= 0 {
		e.denyPolicies[node.Name][current] = stored
	} else {
		e.denyPolicies[node.Name] = append(e.denyPolicies[node.Name], stored)
	}
	return proto.CloneOf(stored.message), nil
}

// DeleteDenyPolicy removes node's deny policy id and returns it.
func (e *Engine) DeleteDenyPolicy(node, id string) (*iamv2.Policy, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	policies := e.denyPolicies[node]
	index := slices.IndexFunc(policies, func(p denyPolicy) bool { return p.id == id })
	if index < 0 {
		return nil, false
	}
	removed := policies[index].message
	e.denyPolicies[node] = slices.Delete(policies, index, index+1)
	if len(e.denyPolicies[node]) == 0 {
		delete(e.denyPolicies, node)
	}
	return proto.CloneOf(removed), true
}

// newUID returns a random UUID-shaped policy uid, as GCP's.
func newUID() string {
	raw := make([]byte, 16)
	_, _ = rand.Read(raw)
	text := hex.EncodeToString(raw)
	return text[:8] + "-" + text[8:12] + "-" + text[12:16] + "-" + text[16:20] + "-" + text[20:]
}

// checkDenyLimits applies the per-resource limits to node's policies with
// stored written at index current (-1 for a new policy).
func checkDenyLimits(node state.Resource, policies []denyPolicy, current int, stored denyPolicy) error {
	if current < 0 && len(policies) >= maxDenyPolicies {
		return status.Errorf(codes.ResourceExhausted, "A resource can have at most %d deny policies.", maxDenyPolicies)
	}
	var rules, principals, groups int
	for index, policy := range append(slices.Clone(policies), stored) {
		if index != current {
			policyPrincipals, policyGroups := policy.usage()
			rules, principals, groups = rules+len(policy.rules), principals+policyPrincipals, groups+policyGroups
		}
	}
	if rules <= maxDenyRules && principals <= maxDenyPrincipals && groups <= maxDenyGroups {
		return nil
	}
	return errorInfo(codes.ResourceExhausted, fmt.Sprintf("Applying this policy change would exceed one or more limits for the DenyPolicy policies attached to %s. "+
		"Maximum rules: %d. Rules after proposed change: %d. Maximum groups: %d. Groups after proposed change: %d. "+
		"Maximum principals: %d. Principals after proposed change: %d. For details on IAM policy limits, see https://cloud.google.com/iam/quotas.",
		strings.TrimPrefix(strings.TrimSuffix(DenyParent(node), "/"+denyCollection), "policies/"), maxDenyRules, rules, maxDenyGroups, groups, maxDenyPrincipals, principals),
		"QUOTA_EXCEEDED", map[string]string{
			"resource": NodeReference(node), "denyPolicyRuleLimit": strconv.Itoa(maxDenyRules), "denyPolicyRuleUsage": strconv.Itoa(rules),
			"denyPolicyGroupLimit": strconv.Itoa(maxDenyGroups), "denyPolicyGroupUsage": strconv.Itoa(groups),
			"denyPolicyMemberLimit": strconv.Itoa(maxDenyPrincipals), "denyPolicyMemberUsage": strconv.Itoa(principals),
		})
}

// usage counts the policy's distinct principals and groups. IAM sums them
// over a resource's policies (recorded: 501 rules naming one principal count
// 1, and the same principal in two policies counts 2).
func (p denyPolicy) usage() (int, int) {
	distinct := map[string]bool{}
	groups := 0
	for _, rule := range p.rules {
		for _, principal := range slices.Concat(rule.principals, rule.exceptionPrincipals) {
			if !distinct[principal] && strings.HasPrefix(principal, groupSetPrefix) {
				groups++
			}
			distinct[principal] = true
		}
	}
	return len(distinct), groups
}

// NodeReference is a hierarchy node's relative name ("projects/p",
// "folders/1"), a project by ID.
func NodeReference(node state.Resource) string {
	return strings.TrimPrefix(node.Name, hierarchyPrefix)
}

// compileDenyRules validates each rule's principals, permissions, and
// condition. An identifier or permission the engine can't match is refused,
// so a rule never silently fails to deny; a group must be in the directory,
// which can't change once policies exist.
func (e *Engine) compileDenyRules(rules []*iamv2.PolicyRule, directory directory) ([]storedDeny, error) {
	compiled := make([]storedDeny, 0, len(rules))
	for _, policyRule := range rules {
		rule := policyRule.GetDenyRule()
		if rule == nil {
			return nil, status.Error(codes.InvalidArgument, "Every rule of a deny policy requires a deny rule")
		}
		deny, err := e.compileDenyRule(rule, directory)
		if err != nil {
			return nil, err
		}
		compiled = append(compiled, deny)
	}
	return compiled, nil
}

// compileDenyRule validates and compiles one deny rule. GCP accepts a rule
// without denied principals or permissions; it denies nothing (recorded).
func (e *Engine) compileDenyRule(rule *iamv2.DenyRule, directory directory) (storedDeny, error) {
	if err := checkDenyPrincipals(rule.GetDeniedPrincipals(), "denied principals", directory); err != nil {
		return storedDeny{}, err
	}
	if err := checkDenyPrincipals(rule.GetExceptionPrincipals(), "exempted principals", directory); err != nil {
		return storedDeny{}, err
	}
	denied, err := e.denyPermissions(rule.GetDeniedPermissions())
	if err != nil {
		return storedDeny{}, err
	}
	excepted, err := e.denyPermissions(rule.GetExceptionPermissions())
	if err != nil {
		return storedDeny{}, err
	}
	deny := storedDeny{principals: slices.Clone(rule.GetDeniedPrincipals()), exceptionPrincipals: slices.Clone(rule.GetExceptionPrincipals()),
		permissions: denied, exceptionPermissions: excepted, applies: true}
	if rule.GetDenialCondition() == nil {
		return deny, nil
	}
	expression := rule.GetDenialCondition().GetExpression()
	value, evaluated, err := orgpolicy.TagCondition(expression)
	if err != nil {
		return storedDeny{}, errorInfo(codes.InvalidArgument, "Condition expression compilation failed. Debug message: "+err.Error(),
			"INVALID_CONDITION", map[string]string{"condition": expression})
	}
	deny.condition, deny.applies = expression, value || !evaluated
	return deny, nil
}

// checkDenyPrincipals checks one principal list of a rule.
func checkDenyPrincipals(principals []string, list string, directory directory) error {
	for _, principal := range principals {
		if err := checkDenyPrincipal(principal, list, directory); err != nil {
			return err
		}
	}
	return nil
}

// checkDenyPrincipal refuses a deny principal the engine can't match: a
// group outside the directory, an identifier form GCP accepts that isn't
// modeled (UNIMPLEMENTED), and the forms GCP refuses (recorded:
// iam-deny-policy-lifecycle). list names the rule's list for GCP's message.
// GCP also refuses an identifier naming no account ("was not found",
// recorded for a user); account existence isn't checked locally.
func checkDenyPrincipal(principal, list string, directory directory) error {
	invalid := "The provided deny policy contains a rule with an invalid principal (" + principal + ") as part of the " + list + " list."
	switch {
	case strings.HasPrefix(principal, groupSetPrefix):
		if group := strings.TrimPrefix(principal, groupSetPrefix); !emailShaped(group) || serviceAccountEmail(group) || !directory.groups[strings.ToLower(group)] {
			return principalError(principal, "The principal specified by member ("+principal+") was not found.")
		}
		return nil
	case principal == publicPrincipalSet:
		// Everyone can be denied, but not exempted (recorded).
		if list != "denied principals" {
			return principalError(principal, invalid)
		}
		return nil
	case accountIdentifier(principal):
		return nil
	case unmodeledIdentifier(principal):
		return status.Error(codes.Unimplemented, "Deny principal identifier is unsupported: "+principal)
	case !strings.Contains(principal, "://"):
		return principalError(principal, "The principal specified by ("+principal+") is in an incorrect format.")
	}
	return principalError(principal, invalid)
}

// accountIdentifier reports a user or service account identifier.
func accountIdentifier(principal string) bool {
	for _, prefix := range []string{userIdentifierPrefix, serviceAccountIdentifierPrefix} {
		if email, ok := strings.CutPrefix(principal, prefix); ok && emailShaped(email) {
			return true
		}
	}
	return false
}

// unmodeledIdentifier reports the identifier forms GCP accepts that the
// engine can't match: customer sets, workforce and workload identity pools,
// and deleted principals.
func unmodeledIdentifier(principal string) bool {
	for _, prefix := range []string{"principalSet://goog/cloudIdentityCustomerId/", "principal://iam.googleapis.com/locations/",
		"principalSet://iam.googleapis.com/locations/", "principal://iam.googleapis.com/projects/", "principalSet://iam.googleapis.com/projects/", "deleted:"} {
		if strings.HasPrefix(principal, prefix) {
			return true
		}
	}
	return false
}

// principalError is GCP's INVALID_PRINCIPAL refusal.
func principalError(principal, message string) error {
	return errorInfo(codes.InvalidArgument, message, "INVALID_PRINCIPAL", map[string]string{"principal": principal})
}

// errorInfo is a status carrying an IAM google.rpc.ErrorInfo.
func errorInfo(code codes.Code, message, reason string, metadata map[string]string) error {
	base := status.New(code, message)
	detailed, err := base.WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: iamDomain, Metadata: metadata})
	if err != nil {
		return base.Err()
	}
	return detailed.Err()
}

// denyPermissions converts the v2 names deny rules use
// (storage.googleapis.com/objects.get) to v1 names. A name that isn't
// SERVICE.googleapis.com/RESOURCE.VERB or SERVICE.googleapis.com/RESOURCE.*,
// or that no testable permission or pinned role contains, is GCP's 400 for
// permissions not valid on the resource type (recorded:
// iam-deny-policy-lifecycle). Other wildcard forms aren't modeled. Which
// permissions GCP supports in deny policies isn't pinned: a testable
// permission it doesn't support is accepted.
func (e *Engine) denyPermissions(permissions []string) ([]string, error) {
	converted := make([]string, 0, len(permissions))
	var invalid []string
	for _, permission := range permissions {
		name, valid, err := e.denyPermission(permission)
		if err != nil {
			return nil, err
		}
		if !valid {
			invalid = append(invalid, permission)
		}
		converted = append(converted, name)
	}
	if len(invalid) > 0 {
		return nil, invalidPermissions(invalid)
	}
	return converted, nil
}

// denyPermission converts one v2 name and reports whether it is valid; an
// unmodeled wildcard is an error.
func (e *Engine) denyPermission(permission string) (string, bool, error) {
	service, rest, ok := strings.Cut(permission, ".googleapis.com/")
	name := service + "." + rest
	if resource, wildcard := strings.CutSuffix(rest, ".*"); ok && wildcard && resource != "" && !strings.ContainsAny(resource, "*. ") {
		return name, e.permissionWithPrefix(service + "." + resource + "."), nil
	}
	if strings.Contains(rest, "*") && rest != "*" {
		return "", false, status.Error(codes.Unimplemented, "Deny permission wildcard is not modeled: "+permission)
	}
	return name, ok && !strings.ContainsAny(permission, "* ") && (e.testable[name] != "" || e.roleGrants(name)), nil
}

// permissionWithPrefix reports whether a testable permission or pinned role
// permission starts with prefix.
func (e *Engine) permissionWithPrefix(prefix string) bool {
	for permission := range e.testable {
		if strings.HasPrefix(permission, prefix) {
			return true
		}
	}
	for _, role := range e.roles {
		for permission := range role {
			if strings.HasPrefix(permission, prefix) {
				return true
			}
		}
	}
	return false
}

// invalidPermissions is GCP's refusal of deny permissions (recorded:
// iam-deny-policy-lifecycle).
func invalidPermissions(permissions []string) error {
	listed := strings.Join(permissions, ", ")
	return errorInfo(codes.InvalidArgument, "The following permissions are not valid on this resource type: "+listed+
		". Note that permissions should be specified using Fully Qualified Domain Names (e.g., iam.googleapis.com/denyPolicies.get). "+
		"For more information about supported permissions, visit https://cloud.google.com/iam/help/deny/supported-permissions",
		"INVALID_PERMISSIONS", map[string]string{"permissions": "[" + listed + "]"})
}

// denialPhase evaluates every deny rule of every deny policy at every
// ancestor independently and returns the first that applies, as "rule N of
// NAME". An exception exempts the principal from its own rule only.
func (e *Engine) denialPhase(ancestors []state.Resource, permission string, principal gateway.Principal, decision *Decision, check *Check) string {
	match := ""
	for _, ancestor := range ancestors {
		check.Policies = append(check.Policies, ancestor.Name)
		for _, policy := range e.denyPolicies[ancestor.Name] {
			check.DenyPolicies = append(check.DenyPolicies, policy.message.GetName())
			for index, deny := range policy.rules {
				if match == "" && deny.matches(permission, principal, decision, check) {
					match = fmt.Sprintf("rule %d of %s", index, policy.message.GetName())
				}
			}
		}
	}
	return match
}

// matches reports whether one deny rule denies the check. GCP applies a rule
// whose denial condition is true or cannot be evaluated.
func (d storedDeny) matches(permission string, principal gateway.Principal, decision *Decision, check *Check) bool {
	identities := denyIdentities(principal, decision.memberships)
	if !d.covers(permission) || !slices.ContainsFunc(d.principals, identities) {
		return false
	}
	if slices.ContainsFunc(d.exceptionPrincipals, identities) {
		check.Conditions = append(check.Conditions, "deny rule excepts the principal")
		return false
	}
	if d.condition == "" {
		return true
	}
	if d.applies {
		check.Conditions = append(check.Conditions, "denial condition "+d.condition+" matched")
		return true
	}
	check.Conditions = append(check.Conditions, "denial condition "+d.condition+" did not match (resources carry no tags)")
	return false
}

// covers reports whether the rule covers one v1 permission; a name ending
// in .* covers every permission of its resource.
func (d storedDeny) covers(permission string) bool {
	names := func(name string) bool {
		prefix, wildcard := strings.CutSuffix(name, "*")
		return name == permission || (wildcard && strings.HasPrefix(permission, prefix))
	}
	return slices.ContainsFunc(d.permissions, names) && !slices.ContainsFunc(d.exceptionPermissions, names)
}

// denyIdentities returns a matcher for the deny-policy identifiers that name
// the principal: the public set, GCP's v2 identifiers, and the groups
// containing it (memberships).
func denyIdentities(principal gateway.Principal, memberships map[string]bool) func(string) bool {
	// Emails compare without case, so every name is lower-cased.
	names := map[string]bool{strings.ToLower(publicPrincipalSet): true}
	if principal.State == gateway.Authenticated {
		if email, ok := strings.CutPrefix(principal.Subject, "user:"); ok {
			names[strings.ToLower(userIdentifierPrefix+email)] = true
		}
		if email, ok := strings.CutPrefix(principal.Subject, "serviceAccount:"); ok {
			names[strings.ToLower(serviceAccountIdentifierPrefix+email)] = true
		}
	}
	return func(identifier string) bool {
		lower := strings.ToLower(identifier)
		return names[lower] || memberships[lower]
	}
}
