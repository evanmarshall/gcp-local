// Package denypolicy implements the IAM v2 Policies API for deny policies
// on organizations, folders, and projects. The IAM engine owns the policies,
// which bootstrap declarations and this API share, and evaluates them on
// every authorization after the allow bindings' ancestry is resolved.
package denypolicy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	iamv2 "cloud.google.com/go/iam/apiv2/iampb"
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
	"google.golang.org/protobuf/types/known/timestamppb"
)

// REST method IDs, the iam v2 discovery IDs.
const (
	methodList      = "iam.policies.listPolicies"
	methodCreate    = "iam.policies.createPolicy"
	methodGet       = "iam.policies.get"
	methodUpdate    = "iam.policies.update"
	methodDelete    = "iam.policies.delete"
	methodOperation = "iam.policies.operations.get"
)

// permissions is each method's permission (iam v2 reference). Reading an
// operation needs the policy read permission.
var permissions = map[string]string{
	methodList: "iam.denypolicies.list", methodCreate: "iam.denypolicies.create",
	methodGet: "iam.denypolicies.get", methodUpdate: "iam.denypolicies.update",
	methodDelete: "iam.denypolicies.delete", methodOperation: "iam.denypolicies.get",
}

// Provider serves the deny policies the IAM engine stores. mu serializes
// this API's read-check-write sequences (create's existence check, update's
// and delete's etag checks).
type Provider struct {
	index      *state.Index
	engine     *iam.Engine
	operations *provider.Operations
	clock      func() time.Time

	mu sync.Mutex
}

// errProviderShape rejects an incomplete provider construction.
var errProviderShape = errors.New("denypolicy requires registry, index, and iam")

// gap refuses an unmodeled request shape with UNIMPLEMENTED.
var gap = provider.CoverageGap("IAM deny policies")

// New registers the IAM v2 Policies provider. Deny policies are global
// hierarchy configuration, so it has no backend; manager is unused.
func New(_ context.Context, registry *gateway.Registry, _ *backend.Manager, index *state.Index, engine *iam.Engine) (*Provider, error) {
	if registry == nil || index == nil || engine == nil {
		return nil, errProviderShape
	}
	if err := provider.RegisterPermissions(engine, permissions); err != nil {
		return nil, fmt.Errorf("denypolicy permissions: %w", err)
	}
	p := &Provider{index: index, engine: engine, clock: time.Now}
	// GCP finishes a policy write before it answers (recorded); the
	// operation names the policy it wrote.
	p.operations = provider.NewOperations(state.GlobalScope, 0, func(op provider.Operation) map[string]any {
		return map[string]any{"@type": "type.googleapis.com/google.iam.v2.PolicyOperationMetadata", "createTime": provider.Stamp(op.Created)}
	}).Named(func(_ state.Scope, target, id string) string { return target + "/operations/" + id })
	for method := range permissions {
		if err := registry.Register(method, p.handle); err != nil {
			return nil, fmt.Errorf("register %s: %w", method, err)
		}
	}
	return p, nil
}

// handlers maps each method to its implementation.
var handlers = map[string]func(*Provider, gateway.Call, *gateway.RESTRequest, address) (*gateway.RESTResponse, error){
	methodList: (*Provider).list, methodCreate: (*Provider).create, methodGet: (*Provider).get,
	methodUpdate: (*Provider).update, methodDelete: (*Provider).remove, methodOperation: (*Provider).operation,
}

// address is a request's attachment point and policy ID, as addressed.
type address struct {
	attachment string // cloudresourcemanager.googleapis.com/projects/p
	id         string
}

// handle is the gateway entry point.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid IAM policies request")
	}
	attachment := request.Path["attachmentPoint"]
	// A gRPC name carries the attachment point URL-encoded inside one
	// segment, so it arrives still encoded once.
	if decoded, err := url.PathUnescape(attachment); err == nil && strings.Contains(attachment, "%") {
		attachment = decoded
	}
	if kind := request.Path["kind"]; kind != "denypolicies" {
		return nil, provider.Invalid("Policy kind (" + kind + ") is not supported")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return handlers[call.Method](p, call, request, address{attachment: attachment, id: request.Path["policy"]})
}

// authorize resolves the attachment point to its hierarchy node and
// authorizes call on it. An unknown attachment point gets the same denial,
// naming the addressed resource.
func (p *Provider) authorize(call gateway.Call, at address) (state.Resource, iam.Decision, error) {
	node, found := iam.DenyNode(p.index, at.attachment)
	if !found {
		return node, iam.Decision{}, p.denial(call, at, node, iam.Decision{Checks: []iam.Check{{Permission: permissions[call.Method]}}})
	}
	decision, err := p.engine.Authorize(call, iam.Target{Name: node.Name}, nil)
	if code := status.Code(err); code == codes.PermissionDenied || code == codes.Unauthenticated {
		return node, decision, p.denial(call, at, node, decision)
	}
	if err != nil {
		return node, decision, fmt.Errorf("authorize %s: %w", call.Method, err)
	}
	return node, decision, nil
}

// denial is IAM's 403 (recorded: iam-deny-policy-lifecycle): the v2
// permission name and the attachment point as addressed, with the node by
// number in the ErrorInfo.
func (p *Provider) denial(call gateway.Call, at address, node state.Resource, decision iam.Decision) error {
	if call.Principal.State != gateway.Authenticated {
		return status.Error(codes.Unauthenticated, "Request had invalid authentication credentials.")
	}
	service, rest, _ := strings.Cut(provider.DeniedPermission(decision), ".")
	permission := service + ".googleapis.com/" + rest
	resource := strings.TrimPrefix(at.attachment, "cloudresourcemanager.googleapis.com/")
	if node.Name != "" {
		resource = orgpolicy.Parent(node)
	}
	return provider.ErrorInfo(codes.PermissionDenied, "Permission "+permission+" denied on resource "+at.attachment,
		"IAM_PERMISSION_DENIED", iamDomain, map[string]string{"permission": permission, "resource": resource})
}

// iamDomain is the ErrorInfo domain of the IAM v2 API.
const iamDomain = "iam.googleapis.com"

// policyError is a refusal naming one policy as addressed (recorded: 404
// POLICY_NOT_FOUND, 409 POLICY_ALREADY_EXISTS, 409 INVALID_ETAG).
func policyError(code codes.Code, message, reason string, at address) error {
	resource := strings.TrimPrefix(at.attachment, "cloudresourcemanager.googleapis.com/")
	return provider.ErrorInfo(code, message, reason, iamDomain, map[string]string{"resource": resource, "policyId": at.id})
}

// addressedName is a policy's name as the request addressed it.
func addressedName(at address) string {
	return "policies/" + url.PathEscape(at.attachment) + "/denypolicies/" + at.id
}

// notFound is GCP's 404 for a policy that doesn't exist.
func notFound(at address) error {
	return policyError(codes.NotFound, "Policy ("+addressedName(at)+") does not exist", "POLICY_NOT_FOUND", at)
}

// errEtag is GCP's refusal of a stale etag.
func etagMismatch(at address) error {
	return policyError(codes.Aborted, "The provided etag does not match the most recent version of the policy.", "INVALID_ETAG", at)
}

// list serves listPolicies: the node's policies in creation order, without
// rules or etags (recorded). IAM ignores pageSize
// and returns up to 1,000 policies, more than a resource can hold, so there
// is never a next page.
func (p *Provider) list(call gateway.Call, request *gateway.RESTRequest, at address) (*gateway.RESTResponse, error) {
	node, decision, err := p.authorize(call, at)
	if err != nil {
		return nil, err
	}
	query := url.Values{"pageToken": request.Query["pageToken"]}
	policies := p.engine.DenyPolicies(node.Name)
	page, _, err := provider.Page(policies, query, provider.Paging{Default: 1000, Max: 1000}, gap)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(page))
	for _, policy := range page {
		policy.Rules, policy.Etag = nil, ""
		items = append(items, wire(policy))
	}
	body := map[string]any{}
	if len(items) > 0 {
		body["policies"] = items
	}
	return provider.WithDecision(&gateway.RESTResponse{JSON: body}, decision), nil
}

// get serves get.
func (p *Provider) get(call gateway.Call, _ *gateway.RESTRequest, at address) (*gateway.RESTResponse, error) {
	node, decision, err := p.authorize(call, at)
	if err != nil {
		return nil, err
	}
	policy, found := p.engine.DenyPolicy(node.Name, at.id)
	if !found {
		return nil, notFound(at)
	}
	return provider.WithDecision(&gateway.RESTResponse{JSON: wire(policy)}, decision), nil
}

// policyID is the policyId shape GCP's refusal names (recorded).
var policyID = regexp.MustCompile(`^[a-z][a-z0-9.-]{2,62}$`)

// create serves createPolicy with a caller-chosen policyId.
func (p *Provider) create(call gateway.Call, request *gateway.RESTRequest, at address) (*gateway.RESTResponse, error) {
	at.id = request.Query.Get("policyId")
	node, decision, err := p.authorize(call, at)
	if err != nil {
		return nil, err
	}
	policy, err := decode(request.JSON)
	if err != nil {
		return nil, err
	}
	if at.id == "" {
		return nil, provider.Invalid("Request does not have required field `policy_id`")
	}
	if !policyID.MatchString(at.id) {
		return nil, provider.Invalid("Policy id (" + at.id + ") does not follow the pattern [a-z][a-z0-9-.]+, or is not between 3 and 63 characters.")
	}
	if _, exists := p.engine.DenyPolicy(node.Name, at.id); exists {
		return nil, policyError(codes.AlreadyExists, "Policy ("+addressedName(at)+") already exists", "POLICY_ALREADY_EXISTS", at)
	}
	return p.write(node, at, policy, decision)
}

// update serves update. A non-empty etag must match the stored policy's;
// the body's name, when given, must be the addressed policy.
func (p *Provider) update(call gateway.Call, request *gateway.RESTRequest, at address) (*gateway.RESTResponse, error) {
	node, decision, err := p.authorize(call, at)
	if err != nil {
		return nil, err
	}
	policy, err := decode(request.JSON)
	if err != nil {
		return nil, err
	}
	if name := policy.GetName(); name != "" && name != addressedName(at) && name != iam.DenyPolicyName(node, at.id) {
		return nil, provider.FieldViolation("policy.name", "The policy name must match the request path")
	}
	stored, found := p.engine.DenyPolicy(node.Name, at.id)
	if !found {
		return nil, notFound(at)
	}
	if policy.GetEtag() != "" && policy.GetEtag() != stored.GetEtag() {
		return nil, etagMismatch(at)
	}
	return p.write(node, at, policy, decision)
}

// write stores a policy and answers with its operation.
func (p *Provider) write(node state.Resource, at address, policy *iamv2.Policy, decision iam.Decision) (*gateway.RESTResponse, error) {
	stored, err := p.engine.PutDenyPolicy(node, at.id, policy, p.clock())
	if err != nil {
		return nil, err //nolint:wrapcheck // the engine's validation status
	}
	return p.finish(node, stored, decision)
}

// remove serves delete, with the optional etag check. The operation's
// response is the deleted policy, without its uid and with its delete time
// (recorded).
func (p *Provider) remove(call gateway.Call, request *gateway.RESTRequest, at address) (*gateway.RESTResponse, error) {
	node, decision, err := p.authorize(call, at)
	if err != nil {
		return nil, err
	}
	stored, found := p.engine.DenyPolicy(node.Name, at.id)
	if !found {
		return nil, notFound(at)
	}
	if etag := request.Query.Get("etag"); etag != "" && etag != stored.GetEtag() {
		return nil, etagMismatch(at)
	}
	removed, _ := p.engine.DeleteDenyPolicy(node.Name, at.id)
	now := timestamppb.New(p.clock())
	removed.Uid, removed.UpdateTime, removed.DeleteTime = "", now, now
	return p.finish(node, removed, decision)
}

// finish records a finished operation for a written policy and renders it
// as GCP answers the write: name, metadata, and response, with done unset
// (recorded).
func (p *Provider) finish(node state.Resource, policy *iamv2.Policy, decision iam.Decision) (*gateway.RESTResponse, error) {
	response := wire(policy)
	response["@type"] = "type.googleapis.com/google.iam.v2.Policy"
	id, err := p.operations.Run(operationScope(node), policy.GetName(), "write", func() (map[string]any, error) { return response, nil })
	if err != nil {
		return nil, err //nolint:wrapcheck // the store's scope refusal
	}
	body, err := p.operations.Get(operationScope(node), id)
	if err != nil {
		return nil, err
	}
	delete(body, "done")
	return provider.WithDecision(&gateway.RESTResponse{JSON: body}, decision), nil
}

// operation serves operations.get: GCP's answer has no metadata
// (recorded).
func (p *Provider) operation(call gateway.Call, request *gateway.RESTRequest, at address) (*gateway.RESTResponse, error) {
	node, decision, err := p.authorize(call, at)
	if err != nil {
		return nil, err
	}
	body, err := p.operations.Get(operationScope(node), request.Path["operation"])
	if err != nil {
		return nil, err
	}
	delete(body, "metadata")
	return provider.WithDecision(&gateway.RESTResponse{JSON: body}, decision), nil
}

// operationScope keeps each node's operations apart.
func operationScope(node state.Resource) state.Scope {
	sum := sha256.Sum256([]byte(node.Name))
	return state.Scope{Project: "p" + hex.EncodeToString(sum[:16]), Location: state.Global}
}

// decode strictly decodes a policy body. Output-only fields decode and are
// replaced when the policy is stored; managingAuthority isn't modeled.
func decode(body []byte) (*iamv2.Policy, error) {
	policy := &iamv2.Policy{}
	if err := provider.DecodeProto(body, "policy", policy, gap, "managingAuthority"); err != nil {
		return nil, err
	}
	return policy, nil
}

// wire renders a policy as its REST JSON.
func wire(policy *iamv2.Policy) map[string]any {
	encoded, err := protojson.Marshal(proto.Message(policy))
	if err != nil {
		panic(err) // generated messages always encode
	}
	body := map[string]any{}
	_ = json.Unmarshal(encoded, &body)
	return body
}
