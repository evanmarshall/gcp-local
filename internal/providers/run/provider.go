package run

import (
	"context"
	"errors"
	"fmt"
	"github.com/evanmarshall/gcp-local/internal/providers/run/container"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Provider owns the Cloud Run v2 control plane and its container lifecycle.
// Service records live in the scope store and are read and changed only
// inside its callbacks; container launches and stops run outside it.
type Provider struct {
	index    *state.Index
	metadata func(subject string) (http.Handler, error)
	metabind func(http.Handler) (string, func(), error)
	reapers  map[string]context.CancelFunc
	reaperMu sync.Mutex
	rollouts atomic.Uint64
	tokens   *identity.IDTokens
	baseURL  string
	hosts    map[string]hostEntry
	hostMu   sync.RWMutex
	engine   *iam.Engine
	runtime  container.Runtime
	scopes   *state.Store[scopeState]
	clock    func() time.Time
}

// errProviderShape rejects an incomplete provider construction.
var errProviderShape = errors.New("run requires registry, manager, index, iam, and runtime")

// New registers the Cloud Run v2 provider. Operation completion happens from
// container readiness, evaluated by the per-service worker in methods.go. Reset
// cleanup runs under an uncancellable child of the provider context so it can
// outlive a shutdown that cancels the serving context.
func New(ctx context.Context, config Config, registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine, runtime container.Runtime) (*Provider, error) {
	if registry == nil || manager == nil || index == nil || engine == nil || runtime == nil {
		return nil, errProviderShape
	}
	if err := provider.RegisterPermissions(engine, permissions); err != nil {
		return nil, fmt.Errorf("run permissions: %w", err)
	}
	p := &Provider{
		index: index, engine: engine, runtime: runtime, tokens: config.Tokens, baseURL: config.BaseURL,
		metadata: config.Metadata, metabind: config.MetadataBind,
		scopes: state.NewStore(newScopeState, state.Scope.Validate),
		hosts:  map[string]hostEntry{}, reapers: map[string]context.CancelFunc{},
		clock: time.Now,
	}
	if err := manager.Register(serviceName, backend.Definition{
		Validate: state.Scope.Validate,
		// The control plane is in memory and container work is provider-owned.
		New: provider.NoBackend,
		AfterReset: func(scope state.Scope) error {
			// The manager's reset hooks carry no request context by design, so the
			// cleanup runs under an uncancellable child of the provider context.
			if resetErr := p.Reset(context.WithoutCancel(ctx), scope); resetErr != nil {
				return resetErr
			}
			engine.ClearScope(serviceName, scope)
			index.RemoveScope(serviceName, scope)
			return nil
		},
	}); err != nil {
		return nil, fmt.Errorf("register the run backend: %w", err)
	}
	if err := registerMethods(registry, p); err != nil {
		return nil, err
	}
	return p, nil
}

// registerMethods binds every provider method to the shared REST handler. The
// gateway transcodes the gRPC methods onto these routes.
func registerMethods(registry *gateway.Registry, p *Provider) error {
	for _, method := range methodNames() {
		if err := registry.Register(method, p.handle); err != nil {
			return fmt.Errorf("register %s: %w", method, err)
		}
	}
	return nil
}

// reaperKey addresses one service's idle reaper. Service IDs repeat across
// projects and locations, so the key carries both.
func reaperKey(scope state.Scope, id string) string {
	return scope.Project + "\x00" + scope.Location + "\x00" + id
}

// stopReapers cancels the idle reapers of one project and location. Reset
// calls this before clearing state so no reaper outlives the scope it watches.
func (p *Provider) stopReapers(scope state.Scope) {
	p.reaperMu.Lock()
	for key, cancel := range p.reapers {
		if strings.HasPrefix(key, reaperKey(scope, "")) {
			cancel()
			delete(p.reapers, key)
		}
	}
	p.reaperMu.Unlock()
}

// stopReaper cancels one service's idle reaper.
func (p *Provider) stopReaper(scope state.Scope, id string) {
	p.reaperMu.Lock()
	if cancel := p.reapers[reaperKey(scope, id)]; cancel != nil {
		cancel()
		delete(p.reapers, reaperKey(scope, id))
	}
	p.reaperMu.Unlock()
}

// Reset stops every container in the scope, then clears the provider state.
func (p *Provider) Reset(ctx context.Context, scope state.Scope) error {
	p.stopReapers(scope)
	var ids []string
	if err := p.scopes.With(scope, func(s *scopeState) error {
		for id, service := range s.services {
			ids = append(ids, id)
			for _, revision := range service.Revisions {
				_ = p.runtime.Stop(context.WithoutCancel(ctx), revision.Container)
				releaseMetadata(revision)
			}
		}
		s.reset()
		return nil
	}); err != nil {
		return fmt.Errorf("clear the run scope: %w", err)
	}
	for _, id := range ids {
		p.forgetHost(scope, id)
	}
	return nil
}

// errNoService reports a service that does not exist (or no longer does).
func errNoService(id string) error {
	return status.Errorf(codes.NotFound, "service %q is not found", id)
}

// withService runs use on an existing service under its scope lock. The
// callback may read and change the record and must render anything it
// returns: the record is never used outside the lock.
func (p *Provider) withService(scope state.Scope, id string, use func(*scopeState, *serviceRecord) error) error {
	return p.scopes.With(scope, func(s *scopeState) error { //nolint:wrapcheck // status errors carry the wire status
		record := s.services[id]
		if record == nil {
			return errNoService(id)
		}
		return use(s, record)
	})
}

// gap refuses a request shape the provider does not model. Clients do not
// retry UNIMPLEMENTED.
var gap = provider.CoverageGap("Cloud Run")

// errUnimplemented is the default dispatch gap for unregistered methods.
var errUnimplemented = gap("this method is not implemented")

// coverageGap refuses one documented request feature the provider does not
// model.
func coverageGap(feature string) error { return gap(feature + " is not implemented") }

// refusedOptions are the Admin API v2 query options (services.create, list,
// patch, delete; revisions.list, delete; operations.list) the provider does
// not model. A request that sets one to a non-default value is refused rather
// than silently answered as if it were unset: validateOnly would mutate,
// pageSize would be exceeded, and etag would not be checked.
var refusedOptions = []string{"validateOnly", "allowMissing", "showDeleted", "updateMask", "etag", "pageSize", "pageToken", "filter"}

// refuseOptions returns the gap for the first non-default refused option.
func refuseOptions(query url.Values) error {
	for _, name := range refusedOptions {
		if value := query.Get(name); value != "" && value != "false" && value != "0" {
			return coverageGap("the " + name + " option")
		}
	}
	return nil
}

// denied renders the Cloud Run denial shape with the failed permission.
func denied(call gateway.Call, target iam.Target, decision iam.Decision) *gateway.RESTResponse {
	permission := provider.DeniedPermission(decision)
	actor := call.Principal.Subject
	code := 403
	if call.Principal.State != gateway.Authenticated {
		actor, code = "Anonymous caller", 401
	}
	return provider.WireError(code, "PERMISSION_DENIED", fmt.Sprintf("%s does not have %s access to %s.", actor, permission, target.Name))
}

// handle is the gateway entry point for every Cloud Run v2 method.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Cloud Run request")
	}
	target, scope, id, err := resolveCall(call, request)
	if err != nil {
		return nil, err
	}
	decision, refusal, err := p.authorize(call, target, scope, id)
	if err != nil {
		return nil, err
	}
	if refusal != nil {
		return refusal, nil
	}
	if err := p.preflight(call, request, scope); err != nil {
		return nil, err
	}
	call.Resource = target.Name
	call.Location = scope.Location
	response, err := p.execute(call, request, scope, id)
	if err != nil {
		return nil, err
	}
	if response.Headers == nil {
		response.Headers = http.Header{}
	}
	response.Headers.Set("X-Gcp-Local-Decision-Id", decision.ID)
	return response, nil
}

// authorize evaluates the method's permission plan and returns the denial
// response of a refused caller. A service-addressed call checks existence and
// authorizes under one scope lock, and delete removes the index entry under
// that lock, so a call racing a delete reads NOT_FOUND rather than an
// unverified target.
func (p *Provider) authorize(call gateway.Call, target iam.Target, scope state.Scope, id string) (iam.Decision, *gateway.RESTResponse, error) {
	var decision iam.Decision
	var authorizeErr error
	evaluate := func(*scopeState, *serviceRecord) error {
		decision, authorizeErr = p.engine.Authorize(call, target, nil)
		return nil
	}
	if id == "" {
		_ = evaluate(nil, nil)
	} else if err := p.withService(scope, id, evaluate); err != nil {
		return decision, nil, err
	}
	switch status.Code(authorizeErr) {
	case codes.OK:
		return decision, nil, nil
	case codes.PermissionDenied, codes.Unauthenticated:
		return decision, denied(call, target, decision), nil
	default:
		return decision, nil, fmt.Errorf("authorize %s: %w", call.Method, authorizeErr)
	}
}

// preflight refuses unmodeled request options and, for a deploy, a caller
// that cannot act as the revision's service account.
func (p *Provider) preflight(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) error {
	if refused := refuseOptions(request.Query); refused != nil {
		return refused
	}
	if call.Method == methodServicesCreate || call.Method == methodServicesUpdate {
		return p.authorizeRuntimeAccount(call, request, scope)
	}
	return nil
}

// resolveCall derives the IAM target, scope, and addressed service ID ("" for
// project-scoped methods) of one request.
func resolveCall(call gateway.Call, request *gateway.RESTRequest) (iam.Target, state.Scope, string, error) {
	parts := request.Path
	scope := state.Scope{Project: parts["project"], Location: parts["location"]}
	switch call.Method {
	case methodServicesCreate, methodServicesList, methodOperationsList, methodOperationsGet, methodOperationsDelete:
		// Project-scoped methods and operations authorize against the project
		// ancestor; the location namespace may not exist yet on a first create.
		return iam.Target{Name: provider.ProjectResource(scope.Project)}, scope, "", nil
	}
	// Revisions are not indexed IAM resources; they authorize on their
	// service, which inherits the project's policy.
	id := parts["service"]
	if !uidPattern.MatchString(id) || scope.Location == "" {
		return iam.Target{}, state.Scope{}, "", status.Error(codes.InvalidArgument, "Invalid Cloud Run resource name")
	}
	return iam.Target{Name: serviceResource(scope.Project, scope.Location, id)}, scope, id, nil
}

// locationTarget names the parent collection for project-scoped methods.
func locationTarget(scope state.Scope) string {
	return fmt.Sprintf("//run.googleapis.com/projects/%s/locations/%s", scope.Project, scope.Location)
}
