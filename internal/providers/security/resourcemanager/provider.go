package resourcemanager

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// operationLatency is the minimum time from a request to its operation
// reporting done, so clients poll as they do against GCP.
const operationLatency = 250 * time.Millisecond

// Provider owns the resource hierarchy: the index entries of organizations,
// folders, and projects (published here or by the bootstrap config) and the
// folder metadata the index doesn't hold. The hierarchy is global, not
// project-scoped, so one lock serializes it: every handler resolves,
// authorizes, changes, and renders under mu, and the index changes only
// under mu. The IAM engine never calls back into the provider.
type Provider struct {
	index      *state.Index
	engine     *iam.Engine
	gate       provider.Gate
	operations *provider.Operations
	clock      func() time.Time
	started    time.Time

	mu    sync.Mutex
	nodes map[string]*node // by index name; absent until a node changes
}

// New registers the resourcemanager provider.
func New(_ context.Context, registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine) (*Provider, error) {
	if registry == nil || manager == nil || index == nil || engine == nil {
		return nil, errProviderShape
	}
	if err := provider.RegisterPermissions(engine, permissions); err != nil {
		return nil, fmt.Errorf("resourcemanager permissions: %w", err)
	}
	now := time.Now().UTC()
	p := &Provider{index: index, engine: engine, clock: time.Now, started: now, nodes: map[string]*node{},
		// Recorded: resourcemanager-hierarchy-visibility.
		gate: provider.Gate{Engine: engine, Index: index, Domain: "cloudresourcemanager.googleapis.com",
			Message: "Permission '%s' denied on resource '//cloudresourcemanager.googleapis.com/%s' (or it may not exist)."},
		operations: provider.NewOperations(state.GlobalScope, operationLatency, operationMetadata)}
	methods := append([]string(nil), unplanned...)
	for method := range permissions {
		methods = append(methods, method)
	}
	for _, method := range methods {
		if err := registry.Register(method, p.handle); err != nil {
			return nil, fmt.Errorf("register %s: %w", method, err)
		}
	}
	return p, nil
}

// handlers maps each served method to its implementation.
var handlers = map[string]func(*Provider, gateway.Call, *gateway.RESTRequest) (*gateway.RESTResponse, error){
	methodProjectsGetIam: (*Provider).projectPolicyV1, methodProjectsSetIam: (*Provider).projectPolicyV1,
	methodProjectsTestIam: (*Provider).testPermissions, methodProjectsGetV1: (*Provider).getProject,
	methodOperationsGetV1: (*Provider).getOperation, methodOperationsGet: (*Provider).getOperation,
	methodOrganizationsGet: (*Provider).getOrganization, methodOrganizationsFind: (*Provider).searchOrganizations,
	methodOrganizationsGetP: (*Provider).policy, methodOrganizationsSetP: (*Provider).policy,
	methodOrganizationsTestP: (*Provider).testPermissions,
	methodFoldersCreate:      (*Provider).createFolder, methodFoldersList: (*Provider).listFolders,
	methodFoldersFind: (*Provider).searchFolders, methodFoldersGet: (*Provider).getFolder,
	methodFoldersPatch: (*Provider).patchFolder, methodFoldersDelete: (*Provider).deleteFolder,
	methodFoldersMove: (*Provider).moveFolder, methodFoldersUndelete: (*Provider).undeleteFolder,
	methodFoldersGetP: (*Provider).policy, methodFoldersSetP: (*Provider).policy, methodFoldersTestP: (*Provider).testPermissions,
	methodProjectsGet: (*Provider).getProject, methodProjectsList: (*Provider).listProjects,
	methodProjectsFind: (*Provider).searchProjects, methodProjectsMove: (*Provider).moveProject,
	methodProjectsGetP: (*Provider).policy, methodProjectsSetP: (*Provider).policy, methodProjectsTestP: (*Provider).testPermissions,
}

// handle is the gateway entry point.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid resourcemanager request")
	}
	return handlers[call.Method](p, call, request)
}

// check authorizes call on an indexed node. A denial names the node's
// relative name, as recorded.
func (p *Provider) check(call gateway.Call, name string) (iam.Decision, error) {
	return p.gate.Check(call, iam.Target{Name: name}, relative(name))
}

// absent is the denial for a node that doesn't exist: GCP answers 403
// "(or it may not exist)" rather than 404 (recorded for folders/1 and an
// unknown project).
func (p *Provider) absent(call gateway.Call, name string) error {
	decision := iam.Decision{Checks: []iam.Check{{Permission: permissions[call.Method]}}}
	return p.gate.Denial(call, decision, name)
}

// plain turns a denial into the 403 without details that project list and
// move return (recorded); 401 and other refusals pass through.
func plain(err error) error {
	if status.Code(err) == codes.PermissionDenied {
		return errNoPermission
	}
	return err
}

// authenticated refuses a caller without valid credentials, as every method
// does before reading the hierarchy.
func authenticated(call gateway.Call) error {
	if call.Principal.State != gateway.Authenticated {
		return status.Error(codes.Unauthenticated, "Request had invalid authentication credentials.")
	}
	return nil
}

// respond renders a JSON response with the decision header.
func respond(body any, decision iam.Decision) *gateway.RESTResponse {
	return provider.WithDecision(&gateway.RESTResponse{JSON: body}, decision)
}

// Operation metadata types (google.cloud.resourcemanager.v3).
const (
	metadataCreateFolder   = "CreateFolderMetadata"
	metadataUpdateFolder   = "UpdateFolderMetadata"
	metadataMoveFolder     = "MoveFolderMetadata"
	metadataDeleteFolder   = "DeleteFolderMetadata"
	metadataUndeleteFolder = "UndeleteFolderMetadata"
	metadataMoveProject    = "MoveProjectMetadata"
	typePrefix             = "type.googleapis.com/google.cloud.resourcemanager.v3."
)

// operationMetadata renders an operation's metadata: the type its verb
// names, whose fields are all optional and not modeled.
func operationMetadata(op provider.Operation) map[string]any {
	return map[string]any{"@type": typePrefix + op.Verb}
}

// operationScope keeps each principal's operations apart: only the caller
// that started an operation can read it.
func operationScope(call gateway.Call) state.Scope {
	sum := sha256.Sum256([]byte(call.Principal.Subject))
	return state.Scope{Project: "p" + hex.EncodeToString(sum[:16]), Location: state.Global}
}

// run records a mutation as an operation: work runs at once, under mu, and
// its result (or business-rule refusal) becomes the operation's outcome,
// which reports done once operationLatency passes. typ is the response's
// type ("Folder", "Project").
func (p *Provider) run(call gateway.Call, target, verb, typ string, decision iam.Decision, work func() (map[string]any, error)) (*gateway.RESTResponse, error) {
	scope := operationScope(call)
	id, err := p.operations.Start(scope, provider.Launch{Target: target, Verb: verb})
	if err != nil {
		return nil, fmt.Errorf("start the operation: %w", err)
	}
	result, err := work()
	if result != nil {
		result["@type"] = typePrefix + typ
	}
	p.operations.Finish(scope, id, result, err)
	body, err := p.operations.Get(scope, id)
	if err != nil {
		return nil, fmt.Errorf("read the operation: %w", err)
	}
	body[fieldName] = "operations/" + id
	return respond(body, decision), nil
}

// getOperation serves operations.get on v1 and v3. An operation another
// principal started, or one that doesn't exist, is GCP's bare 400
// (recorded for an unknown name).
func (p *Provider) getOperation(call gateway.Call, request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	if err := authenticated(call); err != nil {
		return nil, err
	}
	id := request.Path["operation"]
	body, err := p.operations.Get(operationScope(call), id)
	if err != nil {
		return nil, errInvalidArgument
	}
	body[fieldName] = "operations/" + id
	return &gateway.RESTResponse{JSON: body}, nil
}
