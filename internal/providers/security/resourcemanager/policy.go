package resourcemanager

import (
	"fmt"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// setPolicyBody is a SetIamPolicyRequest body.
type setPolicyBody struct {
	Policy     iam.Policy `json:"policy"`
	UpdateMask string     `json:"updateMask"`
}

// policyRequest is one decoded get or set policy request: a get's requested
// version, or a set's policy and the fields its update mask replaces.
type policyRequest struct {
	set     bool
	version int
	policy  iam.Policy
	mask    map[string]bool
}

// decodePolicyRequest strictly decodes a get or set policy body.
func decodePolicyRequest(method string, request *gateway.RESTRequest) (policyRequest, error) {
	if !strings.HasSuffix(method, "setIamPolicy") {
		version, err := provider.RequestedPolicyVersion(request, "options.requestedPolicyVersion", gap)
		return policyRequest{version: version}, err //nolint:wrapcheck // wire statuses
	}
	var set setPolicyBody
	if err := provider.DecodeJSON(request.JSON, "", &set, gap, provider.Fields{}); err != nil {
		return policyRequest{}, err
	}
	mask, err := updateMask(set.UpdateMask)
	return policyRequest{set: true, policy: set.Policy, mask: mask}, err
}

// updateMask parses a setIamPolicy update mask. Its paths are the policy
// fields the request replaces; GCP's default is "bindings,etag"
// (https://cloud.google.com/resource-manager/reference/rest/v1/projects/setIamPolicy).
func updateMask(mask string) (map[string]bool, error) {
	if mask == "" {
		mask = "bindings,etag"
	}
	paths := map[string]bool{}
	for path := range strings.SplitSeq(mask, ",") {
		switch path = strings.TrimSpace(path); path {
		case "bindings", "etag", "auditConfigs", "version":
			paths[path] = true
		case "audit_configs":
			paths["auditConfigs"] = true
		default:
			return nil, provider.Invalid(fmt.Sprintf("Invalid update mask path %q", path))
		}
	}
	return paths, nil
}

// masked returns the policy a set request stores: the stored policy with
// the masked fields replaced (recorded: project-iam-policy-versions). A
// masked field the request omits is cleared: "bindings" without bindings
// removes every binding. bindings brings the request's version; the etag is
// checked whenever the request has one, masked or not. A policy with a
// conditional binding takes only a version-3 request, whatever the mask.
func (r policyRequest) masked(stored iam.Policy) (iam.Policy, error) {
	if stored.View(3).Version == 3 && r.policy.Version < 3 {
		return iam.Policy{}, provider.Invalid(fmt.Sprintf("Specified policy version (%d) must be at least 3 based on the policy's contents. "+
			"For more information, please refer to https://cloud.google.com/iam/help/allow-policies/versions.", max(r.policy.Version, 1)))
	}
	result := stored
	if r.mask["bindings"] {
		result.Bindings = r.policy.Bindings
	}
	if r.mask["bindings"] || r.mask["version"] {
		result.Version = r.policy.Version
	}
	if r.mask["auditConfigs"] {
		result.AuditConfigs = r.policy.AuditConfigs
	}
	result.ETag = r.policy.ETag
	return result, nil
}

// references names each v3 policy and permission method's path variable.
var references = map[string]string{collectionOrganizations: "organization", collectionFolders: "folder", "projects": "project"}

// reference is the relative name a v3 policy or permission request
// addresses ("folders/123"), from its method and path.
func reference(method string, path map[string]string) string {
	collection := strings.Split(method, ".")[1]
	return collection + "/" + path[references[collection]]
}

// policy serves get and set IAM policy on v3 organizations, folders, and
// projects. Bindings on an organization or folder are inherited by
// everything under it through the IAM engine's ancestry walk.
func (p *Provider) policy(call gateway.Call, request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	decoded, err := decodePolicyRequest(call.Method, request)
	if err != nil {
		return nil, err
	}
	addressed := reference(call.Method, request.Path)
	p.mu.Lock()
	defer p.mu.Unlock()
	node, found := p.lookupRef(addressed)
	if !found {
		return nil, p.absent(call, addressed)
	}
	decision, err := p.gate.Check(call, iam.Target{Name: node.Name}, addressed)
	if err != nil {
		return nil, err
	}
	stored, err := p.readOrWrite(node.Name, decoded)
	if err != nil {
		return nil, err
	}
	return respond(stored.Wire(), decision), nil
}

// readOrWrite reads a node's stored policy at the requested version, or
// applies a set request's masked fields to it. The caller holds p.mu, so the
// read and the write see the same stored policy.
func (p *Provider) readOrWrite(name string, request policyRequest) (iam.Policy, error) {
	stored, err := p.engine.GetPolicy(name)
	if err != nil {
		return iam.Policy{}, policyError(err)
	}
	if !request.set {
		return stored.View(request.version), nil
	}
	update, err := request.masked(stored)
	if err != nil {
		return iam.Policy{}, err
	}
	if stored, err = p.engine.SetPolicy(name, update); err != nil {
		return iam.Policy{}, policyError(err)
	}
	return stored.View(3), nil
}

// policyError passes the engine's statuses (a stale etag is ABORTED) through.
func policyError(err error) error {
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Errorf(codes.Internal, "store the policy: %v", err)
}

// projectPolicyV1 serves the v1 project policy methods with their original
// shapes: a malformed project ID is 400, an unknown project 404, and a
// denial "The caller does not have ..." (project-iam-policy-binding records
// only their codes).
func (p *Provider) projectPolicyV1(call gateway.Call, request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	project := request.Path["project"]
	if !projectIDPattern.MatchString(project) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid project id %q", project)
	}
	decoded, err := decodePolicyRequest(call.Method, request)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	target := iam.Target{Name: provider.ProjectResource(project)}
	if _, found := p.index.Get(target.Name); !found {
		return nil, notFound(project)
	}
	decision, err := p.engine.Authorize(call, target, nil)
	if err != nil {
		if code := status.Code(err); code != codes.PermissionDenied && code != codes.Unauthenticated {
			return nil, fmt.Errorf("authorize %s: %w", call.Method, err)
		}
		return provider.Denied(decision, target.Name), nil
	}
	stored, err := p.readOrWrite(target.Name, decoded)
	if err != nil {
		return nil, err
	}
	return respond(stored, decision), nil
}

// testPermissions serves testIamPermissions: the requested permissions the
// caller holds on the node. It needs no permission of its own, and a node
// that doesn't exist holds none (recorded for folders/1).
func (p *Provider) testPermissions(call gateway.Call, request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	var body struct {
		Permissions []string `json:"permissions"`
	}
	if err := provider.DecodeJSON(request.JSON, "", &body, gap, provider.Fields{}); err != nil {
		return nil, err
	}
	for _, permission := range body.Permissions {
		if permission == "" || strings.Contains(permission, "*") {
			return nil, provider.Invalid("Permissions with wildcards (such as '*' or 'storage.*') are not allowed in a call to testIamPermissions.")
		}
	}
	if err := authenticated(call); err != nil {
		return nil, err
	}
	addressed := "projects/" + request.Path["project"]
	if call.Method != methodProjectsTestIam {
		addressed = reference(call.Method, request.Path)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	node, found := p.lookupRef(addressed)
	if !found {
		return &gateway.RESTResponse{JSON: map[string]any{}}, nil
	}
	held, decision, err := p.engine.Permitted(call, iam.Target{Name: node.Name}, body.Permissions)
	if err != nil {
		return nil, fmt.Errorf("test permissions on %s: %w", addressed, err)
	}
	response := map[string]any{}
	if len(held) > 0 {
		response["permissions"] = held
	}
	return respond(response, decision), nil
}
