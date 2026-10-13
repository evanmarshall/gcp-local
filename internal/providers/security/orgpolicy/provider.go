package orgpolicy

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/orgpolicy/apiv2/orgpolicypb"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/iam/orgpolicy"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Provider serves the Organization Policy API. The IAM engine owns the
// policies; mu serializes this API's read-check-write sequences (create's
// existence check, update's and delete's etag check) so two writers can't
// both pass a check.
type Provider struct {
	index      *state.Index
	engine     *iam.Engine
	parentGate provider.Gate
	policyGate provider.Gate
	clock      func() time.Time

	mu sync.Mutex
}

// New registers the Organization Policy provider. Its policies are global
// hierarchy configuration, so it has no backend; manager is unused.
func New(_ context.Context, registry *gateway.Registry, _ *backend.Manager, index *state.Index, engine *iam.Engine) (*Provider, error) {
	if registry == nil || index == nil || engine == nil {
		return nil, errProviderShape
	}
	plans := map[string]string{}
	for collection := range collections {
		for verb, permission := range permissions {
			plans["orgpolicy."+collection+"."+verb] = permission
		}
	}
	if err := provider.RegisterPermissions(engine, plans); err != nil {
		return nil, fmt.Errorf("orgpolicy permissions: %w", err)
	}
	// Recorded: orgpolicy-effective-visibility. Methods on a parent deny on
	// the Resource Manager node, methods on a policy on the policy.
	p := &Provider{index: index, engine: engine, clock: time.Now,
		parentGate: provider.Gate{Engine: engine, Index: index, Domain: parentDomain,
			Message: "Permission '%s' denied on resource '//" + parentDomain + "/%s' (or it may not exist)."},
		policyGate: provider.Gate{Engine: engine, Index: index, Domain: policyDomain,
			Message: "Permission '%s' denied on resource '//" + policyDomain + "/%s' (or it may not exist)."}}
	for method := range plans {
		if err := registry.Register(method, p.handle); err != nil {
			return nil, fmt.Errorf("register %s: %w", method, err)
		}
	}
	return p, nil
}

// handlers maps each verb to its implementation. parent is the request's
// parent reference ("projects/p"), as the path names it.
var handlers = map[string]func(*Provider, gateway.Call, *gateway.RESTRequest, string) (*gateway.RESTResponse, error){
	verbConstraintsList: (*Provider).listConstraints, verbPoliciesList: (*Provider).listPolicies,
	verbPoliciesCreate: (*Provider).createPolicy, verbPoliciesGet: (*Provider).getPolicy,
	verbPoliciesGetEff: (*Provider).getEffectivePolicy, verbPoliciesPatch: (*Provider).patchPolicy,
	verbPoliciesDelete: (*Provider).deletePolicy,
}

// handle is the gateway entry point.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid orgpolicy request")
	}
	collection, verb := split(call.Method)
	p.mu.Lock()
	defer p.mu.Unlock()
	return handlers[verb](p, call, request, collection+"/"+request.Path[collections[collection]])
}

// authorize resolves parent to its hierarchy node and authorizes call on it.
// resource is the name the denial reports. A parent that doesn't exist gets
// the same denial: GCP answers 403 "(or it may not exist)" for an unknown
// project (recorded).
func (p *Provider) authorize(call gateway.Call, gate provider.Gate, parent, resource string) (state.Resource, iam.Decision, error) {
	node, found := orgpolicy.Node(p.index, parent)
	if !found {
		_, verb := split(call.Method)
		decision := iam.Decision{Checks: []iam.Check{{Permission: permissions[verb]}}}
		return state.Resource{}, decision, gate.Denial(call, decision, resource)
	}
	decision, err := gate.Check(call, iam.Target{Name: node.Name}, resource)
	return node, decision, err
}

// policyTarget authorizes a method on one policy and resolves its
// constraint, which must be one the emulator enforces.
func (p *Provider) policyTarget(call gateway.Call, request *gateway.RESTRequest, parent string) (state.Resource, string, iam.Decision, error) {
	constraint := request.Path["policy"]
	node, decision, err := p.authorize(call, p.policyGate, parent, parent+"/policies/"+constraint)
	if err != nil {
		return node, "", decision, err
	}
	if _, enforced := orgpolicy.Lookup(constraint); !enforced {
		return node, "", decision, gap("constraint constraints/" + constraint + " is not enforced locally")
	}
	return node, constraint, decision, nil
}

// listConstraints serves constraints.list: the enforced constraints, named
// under the parent as the request names it. GCP returns every constraint in
// one response and ignores pageSize and pageToken (recorded).
func (p *Provider) listConstraints(call gateway.Call, _ *gateway.RESTRequest, parent string) (*gateway.RESTResponse, error) {
	_, decision, err := p.authorize(call, p.parentGate, parent, parent)
	if err != nil {
		return nil, err
	}
	items := []map[string]any{}
	for _, constraint := range orgpolicy.Catalog() {
		items = append(items, constraint.Wire(parent))
	}
	return provider.WithDecision(&gateway.RESTResponse{JSON: provider.ListBody("constraints", items, "")}, decision), nil
}

// listPolicies serves policies.list: the policies set on the node itself,
// in catalog order, without paging (GCP's empty list ignores page options,
// recorded).
func (p *Provider) listPolicies(call gateway.Call, _ *gateway.RESTRequest, parent string) (*gateway.RESTResponse, error) {
	node, decision, err := p.authorize(call, p.parentGate, parent, parent)
	if err != nil {
		return nil, err
	}
	items := []map[string]any{}
	for _, policy := range p.engine.OrgPolicies(node.Name) {
		items = append(items, wire(policy))
	}
	return provider.WithDecision(&gateway.RESTResponse{JSON: provider.ListBody("policies", items, "")}, decision), nil
}

// getPolicy serves policies.get: the policy set on the node itself.
func (p *Provider) getPolicy(call gateway.Call, request *gateway.RESTRequest, parent string) (*gateway.RESTResponse, error) {
	node, constraint, decision, err := p.policyTarget(call, request, parent)
	if err != nil {
		return nil, err
	}
	policy, found := p.engine.OrgPolicy(node.Name, constraint)
	if !found {
		return nil, notFound(constraint, node)
	}
	return provider.WithDecision(&gateway.RESTResponse{JSON: wire(policy)}, decision), nil
}

// getEffectivePolicy serves policies.getEffectivePolicy: the merged
// hierarchy, with no etag or condition (v2 reference), named by number.
func (p *Provider) getEffectivePolicy(call gateway.Call, request *gateway.RESTRequest, parent string) (*gateway.RESTResponse, error) {
	node, constraint, decision, err := p.policyTarget(call, request, parent)
	if err != nil {
		return nil, err
	}
	spec, err := p.engine.EffectiveOrgPolicy(node.Name, constraint)
	if err != nil {
		return nil, err //nolint:wrapcheck // the engine's fail-closed hierarchy status
	}
	return provider.WithDecision(&gateway.RESTResponse{JSON: wire(&orgpolicypb.Policy{Name: policyName(node, constraint), Spec: spec})}, decision), nil
}

// createPolicy serves policies.create. The body's name must name a policy
// on the request's parent (by project ID or number).
func (p *Provider) createPolicy(call gateway.Call, request *gateway.RESTRequest, parent string) (*gateway.RESTResponse, error) {
	node, decision, err := p.authorize(call, p.parentGate, parent, parent)
	if err != nil {
		return nil, err
	}
	policy, err := decodePolicy(request.JSON)
	if err != nil {
		return nil, err
	}
	named, constraint, ok := orgpolicy.ParseName(policy.GetName())
	if target, found := orgpolicy.Node(p.index, named); !ok || !found || target.Name != node.Name {
		return nil, provider.FieldViolation("policy.name", "The policy name must name a policy on the parent "+parent)
	}
	if _, enforced := orgpolicy.Lookup(constraint); !enforced {
		return nil, gap("constraint constraints/" + constraint + " is not enforced locally")
	}
	if _, exists := p.engine.OrgPolicy(node.Name, constraint); exists {
		return nil, status.Errorf(codes.AlreadyExists, "A Policy of constraint constraints/%s on resource %s already exists.", constraint, orgpolicy.Parent(node))
	}
	return p.store(node, constraint, policy, decision)
}

// patchPolicy serves policies.patch. updateMask names the fields to
// replace, "spec" and "dryRunSpec" (optionally prefixed "policy."); without
// one both are replaced. A
// non-empty etag must match the stored policy's (ABORTED otherwise, per the
// v2 reference).
func (p *Provider) patchPolicy(call gateway.Call, request *gateway.RESTRequest, parent string) (*gateway.RESTResponse, error) {
	node, constraint, decision, err := p.policyTarget(call, request, parent)
	if err != nil {
		return nil, err
	}
	update, err := p.decodeUpdate(request.JSON, node, constraint)
	if err != nil {
		return nil, err
	}
	stored, found := p.engine.OrgPolicy(node.Name, constraint)
	if !found {
		return nil, notFound(constraint, node)
	}
	if update.GetEtag() != "" && update.GetEtag() != stored.GetEtag() {
		return nil, errEtag
	}
	if err := applyMask(stored, update, request.Query.Get("updateMask")); err != nil {
		return nil, err
	}
	return p.store(node, constraint, stored, decision)
}

// decodeUpdate decodes a patch body, whose name, when given, must be the
// policy the path names.
func (p *Provider) decodeUpdate(body []byte, node state.Resource, constraint string) (*orgpolicypb.Policy, error) {
	update, err := decodePolicy(body)
	if err != nil || update.GetName() == "" {
		return update, err
	}
	named, bodyConstraint, ok := orgpolicy.ParseName(update.GetName())
	if target, found := orgpolicy.Node(p.index, named); !ok || !found || target.Name != node.Name || bodyConstraint != constraint {
		return nil, provider.FieldViolation("policy.name", "The policy name must match the request path")
	}
	return update, nil
}

// applyMask copies the fields updateMask names from update into stored.
func applyMask(stored, update *orgpolicypb.Policy, mask string) error {
	replaceSpec, replaceDryRun, err := updateFields(mask)
	if err != nil {
		return err
	}
	if replaceSpec {
		stored.Spec = update.GetSpec()
	}
	if replaceDryRun {
		stored.DryRunSpec = update.GetDryRunSpec()
	}
	return nil
}

// deletePolicy serves policies.delete with the optional etag check.
func (p *Provider) deletePolicy(call gateway.Call, request *gateway.RESTRequest, parent string) (*gateway.RESTResponse, error) {
	node, constraint, decision, err := p.policyTarget(call, request, parent)
	if err != nil {
		return nil, err
	}
	stored, found := p.engine.OrgPolicy(node.Name, constraint)
	if !found {
		return nil, notFound(constraint, node)
	}
	if etag := request.Query.Get("etag"); etag != "" && etag != stored.GetEtag() {
		return nil, errEtag
	}
	p.engine.DeleteOrgPolicy(node.Name, constraint)
	return provider.WithDecision(&gateway.RESTResponse{JSON: map[string]any{}}, decision), nil
}

// store names, seals, validates, and writes one policy, and renders it.
func (p *Provider) store(node state.Resource, constraint string, policy *orgpolicypb.Policy, decision iam.Decision) (*gateway.RESTResponse, error) {
	policy = proto.CloneOf(policy)
	policy.Name = policyName(node, constraint)
	orgpolicy.Seal(policy, p.clock())
	if err := p.engine.SetOrgPolicy(node.Name, constraint, policy); err != nil {
		return nil, err //nolint:wrapcheck // the engine's INVALID_ARGUMENT or UNIMPLEMENTED validation status
	}
	return provider.WithDecision(&gateway.RESTResponse{JSON: wire(policy)}, decision), nil
}

// updateFields reads updateMask: the policy fields it replaces.
func updateFields(mask string) (bool, bool, error) {
	if mask == "" {
		return true, true, nil
	}
	spec, dryRun := false, false
	for field := range strings.SplitSeq(mask, ",") {
		// The reference calls mask paths relative to the policy; Terraform
		// 8.5.0 sends them relative to the request ("policy.spec").
		switch strings.TrimPrefix(field, "policy.") {
		case "spec":
			spec = true
		case "dryRunSpec", "dry_run_spec":
			dryRun = true
		default:
			return false, false, gap("updateMask field " + field)
		}
	}
	return spec, dryRun, nil
}

// decodePolicy strictly decodes a policy body. Output-only fields decode and
// are replaced when the policy is sealed.
func decodePolicy(body []byte) (*orgpolicypb.Policy, error) {
	policy := &orgpolicypb.Policy{}
	if err := provider.DecodeProto(body, "policy", policy, gap); err != nil {
		return nil, err
	}
	return policy, nil
}

// policyName is a policy's name as GCP returns it: the project by number.
func policyName(node state.Resource, constraint string) string {
	return orgpolicy.Parent(node) + "/policies/" + constraint
}

// wire renders a policy as its REST JSON.
func wire(policy *orgpolicypb.Policy) map[string]any {
	encoded, err := protojson.Marshal(policy)
	if err != nil {
		panic(err) // generated messages always encode
	}
	body := map[string]any{}
	_ = json.Unmarshal(encoded, &body)
	return body
}

// notFound is GCP's answer for a constraint with no policy on the node
// (recorded), which names the node by number.
func notFound(constraint string, node state.Resource) error {
	return provider.ErrorInfo(codes.NotFound, fmt.Sprintf("A Policy of constraint constraints/%s on resource %s does not exist.", constraint, orgpolicy.Parent(node)),
		"POLICY_NOT_FOUND", policyDomain, nil)
}

// errEtag is the v2 reference's refusal of a stale etag.
var errEtag = status.Error(codes.Aborted, "The etag in the request does not match the policy's current etag.")
