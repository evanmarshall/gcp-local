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

// getPolicyBody is a GetIamPolicyRequest body; any requested version reads
// the stored policy.
type getPolicyBody struct {
	Options *struct {
		RequestedPolicyVersion int `json:"requestedPolicyVersion"`
	} `json:"options"`
}

// setPolicyBody is a SetIamPolicyRequest body. Audit configs aren't modeled.
type setPolicyBody struct {
	Policy     iam.Policy `json:"policy"`
	UpdateMask string     `json:"updateMask"`
}

var setPolicyFields = provider.Fields{Unmodeled: []string{"policy.auditConfigs"}}

// decodePolicyRequest strictly decodes a get or set policy body. set reports
// a set, whose policy it returns.
func decodePolicyRequest(method string, body []byte) (iam.Policy, bool, error) {
	if !strings.HasSuffix(method, "setIamPolicy") {
		return iam.Policy{}, false, decodeGetPolicy(body)
	}
	var set setPolicyBody
	if err := provider.DecodeJSON(body, "", &set, gap, setPolicyFields); err != nil {
		return iam.Policy{}, true, err
	}
	return set.Policy, true, checkUpdateMask(set.UpdateMask)
}

// decodeGetPolicy checks a get policy body; any requested version from 0 to
// 3 reads the stored policy.
func decodeGetPolicy(body []byte) error {
	var get getPolicyBody
	if err := provider.DecodeJSON(body, "", &get, gap, provider.Fields{}); err != nil {
		return err
	}
	if get.Options != nil && (get.Options.RequestedPolicyVersion < 0 || get.Options.RequestedPolicyVersion > 3) {
		return provider.Invalid("Invalid requested policy version")
	}
	return nil
}

// checkUpdateMask accepts a mask that includes bindings; it may also name
// etag, version, and auditConfigs, as Terraform's "bindings,etag,auditConfigs"
// does. A mask without bindings is a gap.
func checkUpdateMask(mask string) error {
	if mask == "" {
		return nil
	}
	bindings := false
	for path := range strings.SplitSeq(mask, ",") {
		switch path = strings.TrimSpace(path); path {
		case "bindings":
			bindings = true
		case "etag", "version", "auditConfigs", "audit_configs":
		default:
			return provider.Invalid(fmt.Sprintf("Invalid update mask path %q", path))
		}
	}
	if !bindings {
		return gap("setIamPolicy updateMask without bindings")
	}
	return nil
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
	policy, set, err := decodePolicyRequest(call.Method, request.JSON)
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
	stored, err := p.readOrWrite(node.Name, policy, set)
	if err != nil {
		return nil, err
	}
	return respond(stored.Wire(), decision), nil
}

// readOrWrite reads a node's stored policy, or replaces it when set.
func (p *Provider) readOrWrite(name string, policy iam.Policy, set bool) (iam.Policy, error) {
	var stored iam.Policy
	var err error
	if set {
		stored, err = p.engine.SetPolicy(name, policy)
	} else {
		stored, err = p.engine.GetPolicy(name)
	}
	if err != nil {
		return iam.Policy{}, policyError(err)
	}
	return stored, nil
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
	policy, set, err := decodePolicyRequest(call.Method, request.JSON)
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
	stored, err := p.readOrWrite(target.Name, policy, set)
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
