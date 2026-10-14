package iam

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	iamv2 "cloud.google.com/go/iam/apiv2/iampb"
	"cloud.google.com/go/orgpolicy/apiv2/orgpolicypb"
	"github.com/evanmarshall/gcp-local/internal/iam/orgpolicy"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/genproto/googleapis/type/expr"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Configuration sentinels; Load reports the first violated bootstrap rule.
var (
	errConfigMode      = errors.New("iam configuration must be a regular owner-only file")
	errConfigOpen      = errors.New("unable to open iam configuration")
	errConfigInvalid   = errors.New("invalid iam configuration")
	errConfigTrailing  = errors.New("iam configuration contains trailing data")
	errConfigHierarchy = errors.New("bootstrap only distinct resource-manager hierarchy nodes")
	errConfigProject   = errors.New("project name and scope disagree")
	errConfigRoot      = errors.New("organization must be a canonical root")
	errConfigFolder    = errors.New("folder must have a canonical name and parent")
	errConfigKind      = errors.New("unsupported hierarchy node kind")
	errConfigDuplicate = errors.New("duplicate local service account name or unique id")
)

type Config struct {
	Resources []state.Resource        `json:"resources"`
	Policies  map[string]ConfigPolicy `json:"policies"`
	// DenyPolicies declares IAM v2 deny policies in the API's JSON form, each
	// named policies/ATTACHMENT/denypolicies/ID on a declared organization,
	// folder, or project. They are the records the IAM v2 API serves.
	DenyPolicies []json.RawMessage `json:"deny_policies,omitempty"`
	// Boundaries declares principal access boundaries by hierarchy node.
	// They are local-only: the IAM v3 boundary API isn't implemented.
	Boundaries      map[string]PABPolicy `json:"principal_access_boundaries,omitempty"`
	Unsupported     map[string][]string  `json:"unsupported,omitempty"`
	ServiceAccounts []ServiceAccount     `json:"service_accounts,omitempty"`
	// OrgPolicies declares Organization Policy v2 policies in the API's JSON
	// form, each named PARENT/policies/CONSTRAINT on a declared organization,
	// folder, or project. They are the same records the Organization Policy
	// API reads and writes.
	OrgPolicies []json.RawMessage `json:"org_policies,omitempty"`
	// Perimeters declares VPC-SC perimeters by name over listed resources.
	Perimeters map[string][]string `json:"vpcsc_perimeters,omitempty"`
	// Directory declares the local groups and domains policies may name.
	Directory Directory `json:"directory,omitzero"`
}

// Load creates a fresh index/engine so a rejected bootstrap cannot publish a
// partial hierarchy to a running provider. Hierarchy roots and explicitly
// declared local service accounts are bootstrapped. Other service resources are
// published by their owning providers after backend success.
func Load(path string) (*Engine, *state.Index, error) {
	config, err := readConfig(path)
	if err != nil {
		return nil, nil, err
	}
	index := state.NewIndex()
	engine, err := New(index)
	if err != nil {
		return nil, nil, err
	}
	if err := engine.SetDirectory(config.Directory); err != nil {
		return nil, nil, err
	}
	seen := map[string]bool{}
	if err := publishHierarchy(index, engine, config.Resources, seen); err != nil {
		return nil, nil, err
	}
	if err := publishAccounts(index, engine, config.ServiceAccounts, seen); err != nil {
		return nil, nil, err
	}
	if err := engine.publishPolicies(config); err != nil {
		return nil, nil, err
	}
	if err := engine.publishSignalFamilies(config); err != nil {
		return nil, nil, err
	}
	return engine, index, nil
}

// readConfig opens an operator-private regular file and decodes it.
func readConfig(path string) (Config, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return Config{}, errConfigMode
	}
	file, err := os.Open(path) //nolint:gosec // the operator selects the config path explicitly
	if err != nil {
		return Config{}, errConfigOpen
	}
	defer file.Close()
	return decodeConfig(file)
}

// ConfigPolicy is an allow policy as the configuration declares it. The
// legacy denyRules and principalAccessBoundary fields predate the IAM v2
// deny policies and the boundary key; they load as those.
type ConfigPolicy struct {
	Policy
	DenyRules []LegacyDenyRule `json:"denyRules,omitempty"`
	PAB       *PABPolicy       `json:"principalAccessBoundary,omitempty"`
}

// LegacyDenyRule is a deny rule in the retired allow-policy form, which
// took allow-policy members and v1 permission names.
type LegacyDenyRule struct {
	DeniedPrincipals     []string        `json:"deniedPrincipals"`
	ExceptionPrincipals  []string        `json:"exceptionPrincipals,omitempty"`
	DeniedPermissions    []string        `json:"deniedPermissions"`
	ExceptionPermissions []string        `json:"exceptionPermissions,omitempty"`
	DenialCondition      json.RawMessage `json:"denialCondition,omitempty"`
}

// LegacyDenyPolicyID is the ID a policy's legacy denyRules load under.
const LegacyDenyPolicyID = "bootstrap-deny-rules"

// publishPolicies loads the declared allow policies, deny policies,
// boundaries, and unsupported features.
func (e *Engine) publishPolicies(config Config) error {
	for resource, policy := range config.Policies {
		if err := e.publishPolicy(resource, policy); err != nil {
			return err
		}
	}
	for _, raw := range config.DenyPolicies {
		policy := &iamv2.Policy{}
		if err := protojson.Unmarshal(raw, policy); err != nil {
			return fmt.Errorf("%w: deny_policies: %w", errConfigInvalid, err)
		}
		if err := e.declareDenyPolicy(policy); err != nil {
			return fmt.Errorf("deny policy %s: %w", policy.GetName(), err)
		}
	}
	for node, boundary := range config.Boundaries {
		if err := e.DeclareBoundary(node, boundary); err != nil {
			return err
		}
	}
	for resource, features := range config.Unsupported {
		if err := e.MarkUnsupported(resource, features); err != nil {
			return err
		}
	}
	return nil
}

// publishSignalFamilies loads the organization policies and perimeters.
// Organization policies load after allow policies: like GCP's constraints,
// iam.allowedPolicyMemberDomains is not retroactive.
func (e *Engine) publishSignalFamilies(config Config) error {
	for _, raw := range config.OrgPolicies {
		policy := &orgpolicypb.Policy{}
		if err := protojson.Unmarshal(raw, policy); err != nil {
			return fmt.Errorf("%w: org_policies: %w", errConfigInvalid, err)
		}
		if err := e.DeclareOrgPolicy(policy, time.Now()); err != nil {
			return fmt.Errorf("org policy %s: %w", policy.GetName(), err)
		}
	}
	for name, resources := range config.Perimeters {
		if err := e.MarkPerimeter(name, resources); err != nil {
			return err
		}
	}
	return nil
}

// publishPolicy loads one declared allow policy and its legacy fields.
func (e *Engine) publishPolicy(resource string, policy ConfigPolicy) error {
	if _, err := e.SetPolicy(resource, policy.Policy); err != nil {
		return err
	}
	if len(policy.DenyRules) > 0 {
		if err := e.declareLegacyDeny(resource, policy.DenyRules); err != nil {
			return err
		}
	}
	if policy.PAB != nil {
		return e.DeclareBoundary(resource, *policy.PAB)
	}
	return nil
}

// declareDenyPolicy stores a deny policy named by its attachment point.
func (e *Engine) declareDenyPolicy(policy *iamv2.Policy) error {
	attachment, id, ok := ParseDenyPolicyName(policy.GetName())
	if !ok {
		return fmt.Errorf("%w: deny policy name %q", errConfigInvalid, policy.GetName())
	}
	node, found := DenyNode(e.index, attachment)
	if !found {
		return fmt.Errorf("%w: deny policy attachment point %s is not declared", errConfigInvalid, attachment)
	}
	_, err := e.PutDenyPolicy(node, id, policy, time.Now())
	return err
}

// declareLegacyDeny loads a policy's legacy denyRules as the deny policy
// LegacyDenyPolicyID on the same node, which must be a project, folder, or
// organization. Members become v2 identifiers (user:E as
// principal://goog/subject/E, serviceAccount:E as
// principal://iam.googleapis.com/projects/-/serviceAccounts/E, group:E as
// principalSet://goog/group/E) and v1 permissions v2 names
// (storage.objects.get as storage.googleapis.com/objects.get); the result is
// validated as the API validates it.
func (e *Engine) declareLegacyDeny(resource string, legacy []LegacyDenyRule) error {
	node, found := e.index.Get(resource)
	if !found || !strings.HasPrefix(resource, hierarchyPrefix) {
		return fmt.Errorf("%w: denyRules on %s: deny policies attach only to projects, folders, and organizations", errConfigInvalid, resource)
	}
	rules := make([]*iamv2.PolicyRule, 0, len(legacy))
	for _, rule := range legacy {
		deny := &iamv2.DenyRule{DeniedPrincipals: v2Principals(rule.DeniedPrincipals), ExceptionPrincipals: v2Principals(rule.ExceptionPrincipals),
			DeniedPermissions: v2Permissions(rule.DeniedPermissions), ExceptionPermissions: v2Permissions(rule.ExceptionPermissions)}
		if len(rule.DenialCondition) > 0 {
			condition := &expr.Expr{}
			if err := protojson.Unmarshal(rule.DenialCondition, condition); err != nil {
				return fmt.Errorf("%w: denyRules on %s: %w", errConfigInvalid, resource, err)
			}
			deny.DenialCondition = condition
		}
		rules = append(rules, &iamv2.PolicyRule{Kind: &iamv2.PolicyRule_DenyRule{DenyRule: deny}})
	}
	if _, err := e.PutDenyPolicy(node, LegacyDenyPolicyID, &iamv2.Policy{Rules: rules}, time.Now()); err != nil {
		return fmt.Errorf("denyRules on %s: %w", resource, err)
	}
	return nil
}

// v2Principals rewrites allow-policy members as deny-policy identifiers.
func v2Principals(members []string) []string {
	converted := make([]string, 0, len(members))
	for _, member := range members {
		converted = append(converted, DenyPrincipal(member))
	}
	return converted
}

// DenyPrincipal is the deny-policy identifier of an allow-policy member:
// user:E is principal://goog/subject/E, serviceAccount:E is
// principal://iam.googleapis.com/projects/-/serviceAccounts/E, and group:E
// is principalSet://goog/group/E. Other members are returned unchanged.
func DenyPrincipal(member string) string {
	forms := []struct{ member, identifier string }{
		{"user:", userIdentifierPrefix}, {"serviceAccount:", serviceAccountIdentifierPrefix}, {"group:", groupSetPrefix},
	}
	for _, form := range forms {
		if email, ok := strings.CutPrefix(member, form.member); ok {
			return form.identifier + email
		}
	}
	return member
}

// v2Permissions rewrites v1 permission names (storage.objects.get) as
// deny-policy names (storage.googleapis.com/objects.get).
func v2Permissions(permissions []string) []string {
	converted := make([]string, 0, len(permissions))
	for _, permission := range permissions {
		if service, rest, ok := strings.Cut(permission, "."); ok && !strings.Contains(permission, ".googleapis.com/") {
			permission = service + ".googleapis.com/" + rest
		}
		converted = append(converted, permission)
	}
	return converted
}

// DeclareBoundary stores a principal access boundary for the principal set
// of a project, folder, or organization (see pabViolation).
func (e *Engine) DeclareBoundary(node string, boundary PABPolicy) error {
	if resource, ok := e.index.Get(node); !ok || resource.Service != resourceManager || resource.Kind == "" {
		return fmt.Errorf("%w: a principal access boundary binds only to a declared project, folder, or organization: %s", errConfigInvalid, node)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.boundaries[node] = &PABPolicy{Rules: slices.Clone(boundary.Rules)}
	return nil
}

// DeclareOrgPolicy stores a policy named PARENT/policies/CONSTRAINT, as the
// bootstrap configuration declares one. The name takes GCP's response form
// (a project by number) and the server-owned fields are set.
func (e *Engine) DeclareOrgPolicy(policy *orgpolicypb.Policy, now time.Time) error {
	parent, constraint, ok := orgpolicy.ParseName(policy.GetName())
	if !ok {
		return fmt.Errorf("%w: organization policy name %q", errConfigInvalid, policy.GetName())
	}
	node, found := orgpolicy.Node(e.index, parent)
	if !found {
		return fmt.Errorf("%w: organization policy parent %s is not declared", errConfigInvalid, parent)
	}
	policy = proto.CloneOf(policy)
	policy.Name = orgpolicy.Parent(node) + "/policies/" + constraint
	orgpolicy.Seal(policy, now)
	return e.SetOrgPolicy(node.Name, constraint, policy)
}

// decodeConfig reads one JSON object, rejecting unknown fields and trailing data.
func decodeConfig(file *os.File) (Config, error) {
	var config Config
	decoder := json.NewDecoder(io.LimitReader(file, 2<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return Config{}, errConfigInvalid
	}
	var trailing map[string]any
	if decoder.Decode(&trailing) != io.EOF {
		return Config{}, errConfigTrailing
	}
	return config, nil
}

// publishHierarchy publishes the declared resourcemanager resources and rejects
// duplicates, non-global locations, and shapes the hierarchy cannot anchor.
func publishHierarchy(index *state.Index, engine *Engine, resources []state.Resource, seen map[string]bool) error {
	for _, resource := range resources {
		if resource.Service != resourceManager || resource.Scope.Location != state.Global || seen[resource.Name] {
			return errConfigHierarchy
		}
		if err := validateHierarchyResource(resource); err != nil {
			return err
		}
		seen[resource.Name] = true
		if err := index.Put(resourceManager, resource); err != nil {
			return fmt.Errorf("publish hierarchy resource %s: %w", resource.Name, err)
		}
	}
	for _, resource := range resources {
		if _, err := engine.ancestry(Target{Name: resource.Name}); err != nil {
			return err
		}
	}
	return nil
}

// validateHierarchyResource checks the resource name against its declared kind.
func validateHierarchyResource(resource state.Resource) error {
	prefix := "//cloudresourcemanager.googleapis.com/"
	switch resource.Kind {
	case kindProject:
		if resource.Name != prefix+"projects/"+resource.Scope.Project {
			return errConfigProject
		}
	case "organization":
		if !strings.HasPrefix(resource.Name, prefix+"organizations/") || resource.Parent != "" {
			return errConfigRoot
		}
	case "folder":
		if !strings.HasPrefix(resource.Name, prefix+"folders/") || resource.Parent == "" {
			return errConfigFolder
		}
	default:
		return errConfigKind
	}
	return nil
}

// publishAccounts publishes the declared service accounts and verifies each
// resource is reachable through the hierarchy.
func publishAccounts(index *state.Index, engine *Engine, accounts []ServiceAccount, seen map[string]bool) error {
	accountIDs := map[string]bool{}
	for _, account := range accounts {
		resource, err := account.resource()
		if err != nil {
			return err
		}
		if seen[resource.Name] || accountIDs[account.UniqueID] {
			return errConfigDuplicate
		}
		seen[resource.Name], accountIDs[account.UniqueID] = true, true
		if err := index.Put("iam", resource); err != nil {
			return fmt.Errorf("publish service account %s: %w", resource.Name, err)
		}
		if _, err := engine.ancestry(Target{Name: resource.Name}); err != nil {
			return err
		}
	}
	return nil
}
