package run

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Provider owns the Cloud Run v2 control plane and its container lifecycle.
type Provider struct {
	index    *state.Index
	metadata func(subject string) (http.Handler, error)
	metabind func(http.Handler) (string, func(), error)
	reapers  map[string]context.CancelFunc
	reaperMu sync.Mutex
	recordMu sync.RWMutex
	tokens   *identity.IDTokens
	baseURL  string
	hosts    map[string]hostEntry
	hostMu   sync.RWMutex
	engine   *iam.Engine
	runtime  Runtime
	scopes   *state.Store[scopeState]
	clock    func() time.Time
}

// errProviderShape rejects an incomplete provider construction.
var errProviderShape = errors.New("run requires registry, manager, index, iam, and runtime")

// New registers the Cloud Run v2 provider. Operation completion happens from
// container readiness, evaluated by the per-service worker in methods.go. Reset
// cleanup runs under an uncancellable child of the provider context so it can
// outlive a shutdown that cancels the serving context.
func New(ctx context.Context, config Config, registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine, runtime Runtime) (*Provider, error) {
	if registry == nil || manager == nil || index == nil || engine == nil || runtime == nil {
		return nil, errProviderShape
	}
	if err := registerPermissions(engine); err != nil {
		return nil, err
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
		New:      func(state.Scope) (backend.Backend, error) { return emptyBackend{}, nil },
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

// registerMethods binds every provider method to the shared REST handler.
func registerMethods(registry *gateway.Registry, p *Provider) error {
	for _, method := range methodNames() {
		if err := registry.Register(method, p.handle); err != nil {
			return fmt.Errorf("register %s: %w", method, err)
		}
	}
	return nil
}

// emptyBackend satisfies the backend contract for a provider without a child
// process: the control plane is in-memory and container work is provider-owned.
type emptyBackend struct{}

func (emptyBackend) Start(context.Context) error    { return nil }
func (emptyBackend) Ready(context.Context) error    { return nil }
func (emptyBackend) Stop(context.Context) error     { return nil }
func (emptyBackend) Reset(context.Context) error    { return nil }
func (emptyBackend) Snapshot(context.Context) error { return backend.ErrPersistenceUnsupported }
func (emptyBackend) Restore(context.Context) error  { return backend.ErrPersistenceUnsupported }

// stopReapers cancels the idle reapers of one scope. Reset calls this before
// clearing state so no reaper outlives the scope it watches.
func (p *Provider) stopReapers(scope state.Scope) {
	p.reaperMu.Lock()
	for key, cancel := range p.reapers {
		if strings.HasPrefix(key, scope.Project+"/") {
			cancel()
			delete(p.reapers, key)
		}
	}
	p.reaperMu.Unlock()
}

// Reset stops every container in the scope, then clears the provider state.
func (p *Provider) Reset(ctx context.Context, scope state.Scope) error {
	p.stopReapers(scope)
	if err := p.scopes.With(scope, func(s *scopeState) error {
		for _, service := range s.services {
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
	return nil
}

// resolve builds the IAM target and scope for one request and, when the method
// addresses an existing service, its live record.
func (p *Provider) resolve(_ gateway.Call, parts map[string]string, kind, id string) (iam.Target, state.Scope, *serviceRecord, error) {
	location := parts["location"]
	if !uidPattern.MatchString(id) || location == "" {
		return iam.Target{}, state.Scope{}, nil, status.Error(codes.InvalidArgument, "Invalid Cloud Run resource name")
	}
	scope := ScopeOf(parts["project"], location)
	target := iam.Target{Name: resource(scope.Project, scope.Location, kind, id)}
	var record *serviceRecord
	if err := p.scopes.With(scope, func(s *scopeState) error {
		if kind == kindService {
			record = s.services[id]
		}
		return nil
	}); err != nil {
		return iam.Target{}, state.Scope{}, nil, fmt.Errorf("open the run scope: %w", err)
	}
	if kind == kindService && record == nil {
		return iam.Target{}, state.Scope{}, nil, status.Errorf(codes.NotFound, "service %q is not found", id)
	}
	return target, scope, record, nil
}

// authorize evaluates the method's permission plan for one call.
func (p *Provider) authorize(call gateway.Call, target iam.Target, features map[string]bool) (iam.Decision, error) {
	decision, authorizeErr := p.engine.Authorize(call, target, features)
	if authorizeErr != nil {
		return decision, fmt.Errorf("authorize %s: %w", call.Method, authorizeErr)
	}
	return decision, nil
}

// errUnimplemented is the default dispatch gap for unregistered methods.
var errUnimplemented = status.Error(codes.Unimplemented, "Cloud Run coverage gap: this method is not implemented")

// wireError builds the Cloud Run REST error body.
func wireError(code int, statusText, message string) *gateway.RESTResponse {
	return &gateway.RESTResponse{Status: code, JSON: map[string]any{
		keyError: map[string]any{keyCode: code, keyStatus: statusText, keyMessage: message},
	}}
}

// denied renders the Cloud Run denial shape with the failed permission.
func denied(call gateway.Call, target iam.Target, decision iam.Decision) *gateway.RESTResponse {
	permission := "unspecified"
	for _, check := range decision.Checks {
		if !check.Allowed {
			permission = check.Permission
			break
		}
	}
	actor := call.Principal.Subject
	code := 403
	if call.Principal.State != gateway.Authenticated {
		actor, code = "Anonymous caller", 401
	}
	return wireError(code, "PERMISSION_DENIED", fmt.Sprintf("%s does not have %s access to %s.", actor, permission, target.Name))
}

// handle is the gateway entry point for every Cloud Run v2 method.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Cloud Run request")
	}
	target, scope, record, err := p.resolveCall(call, request)
	if err != nil {
		return nil, err
	}
	decision, authorizeErr := p.authorize(call, target, nil)
	if authorizeErr != nil {
		code := status.Code(authorizeErr)
		if code != codes.PermissionDenied && code != codes.Unauthenticated {
			return nil, fmt.Errorf("authorize %s: %w", call.Method, authorizeErr)
		}
		return denied(call, target, decision), nil
	}
	call.Resource = target.Name
	call.Location = scope.Location
	response, err := p.execute(call, request, scope, record)
	if err != nil {
		return nil, err
	}
	if response.Headers == nil {
		response.Headers = http.Header{}
	}
	response.Headers.Set("X-Gcp-Local-Decision-Id", decision.ID)
	return response, nil
}

// resolveCall derives the target and record for the method's resource shape.
func (p *Provider) resolveCall(call gateway.Call, request *gateway.RESTRequest) (iam.Target, state.Scope, *serviceRecord, error) {
	parts := request.Path
	switch call.Method {
	case methodServicesCreate, methodServicesList, methodOperationsList:
		// Project-scoped methods authorize against the project ancestor; the
		// location namespace may not exist yet on a first create.
		scope := ScopeOf(parts["project"], parts["location"])
		return iam.Target{Name: projectResource(scope.Project)}, scope, nil, nil
	case methodOperationsGet, methodOperationsDelete:
		// Operations authorize on the project ancestor like collection methods.
		scope := ScopeOf(parts["project"], parts["location"])
		return iam.Target{Name: projectResource(scope.Project)}, scope, nil, nil
	case methodRevisionsList:
		return p.resolve(call, parts, kindService, parts["service"])
	case methodRevisionsGet, methodRevisionsDelete:
		return p.resolve(call, parts, kindRevision, parts["revision"])
	default:
		return p.resolve(call, parts, kindService, parts["service"])
	}
}

// locationTarget names the parent collection for project-scoped methods.
func locationTarget(scope state.Scope) string {
	return fmt.Sprintf("//run.googleapis.com/projects/%s/locations/%s", scope.Project, scope.Location)
}

// serviceIDs returns the sorted service identifiers of one scope.
func (p *Provider) serviceIDs(scope state.Scope) []string {
	p.recordMu.RLock()
	defer p.recordMu.RUnlock()
	var names []string
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for id := range s.services {
			names = append(names, id)
		}
		return nil
	})
	sort.Strings(names)
	return names
}

// serviceRecordLookUp returns the live record or a not-found response. Callers
// must hold the provider record lock while reading or mutating the record.
func (p *Provider) serviceRecordLookUp(scope state.Scope, id string) (*serviceRecord, *gateway.RESTResponse, error) {
	p.recordMu.RLock()
	defer p.recordMu.RUnlock()
	var record *serviceRecord
	if err := p.scopes.With(scope, func(s *scopeState) error {
		record = s.services[id]
		return nil
	}); err != nil {
		return nil, nil, fmt.Errorf("open the run scope: %w", err)
	}
	if record == nil {
		return nil, wireError(404, "NOT_FOUND", "service "+id+" is not found"), nil
	}
	return record, nil, nil
}
