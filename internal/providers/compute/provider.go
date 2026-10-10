package compute

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Provider owns the first networking control-plane resources.
type Provider struct {
	index   *state.Index
	engine  *iam.Engine
	scopes  *state.Store[scopeState]
	clock   func() time.Time
	records map[string]*operationRecord
	mu      sync.RWMutex
}

// errProviderShape rejects an incomplete provider construction.
var errProviderShape = errors.New("compute requires registry, manager, index, and iam")

// New registers the compute provider.
func New(_ context.Context, registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine) (*Provider, error) {
	if registry == nil || manager == nil || index == nil || engine == nil {
		return nil, errProviderShape
	}
	if err := registerPermissions(engine); err != nil {
		return nil, err
	}
	p := &Provider{
		index: index, engine: engine,
		scopes:  state.NewStore(newScopeState, state.GlobalScope),
		records: map[string]*operationRecord{},
		clock:   time.Now,
	}
	if err := manager.Register(serviceName, backend.Definition{
		Validate: state.GlobalScope,
		New:      func(state.Scope) (backend.Backend, error) { return emptyBackend{}, nil },
		AfterReset: func(scope state.Scope) error {
			if err := p.Reset(scope); err != nil {
				return err
			}
			engine.ClearScope(serviceName, scope)
			index.RemoveScope(serviceName, scope)
			return nil
		},
	}); err != nil {
		return nil, fmt.Errorf("register the compute backend: %w", err)
	}
	for _, method := range methodNames() {
		if err := registry.Register(method, p.handle); err != nil {
			return nil, fmt.Errorf("register %s: %w", method, err)
		}
	}
	return p, nil
}

// emptyBackend satisfies the backend contract for an in-memory provider.
type emptyBackend struct{}

func (emptyBackend) Start(context.Context) error    { return nil }
func (emptyBackend) Ready(context.Context) error    { return nil }
func (emptyBackend) Stop(context.Context) error     { return nil }
func (emptyBackend) Reset(context.Context) error    { return nil }
func (emptyBackend) Snapshot(context.Context) error { return backend.ErrPersistenceUnsupported }
func (emptyBackend) Restore(context.Context) error  { return backend.ErrPersistenceUnsupported }

// Reset clears the scope's topology state and its operation records.
func (p *Provider) Reset(scope state.Scope) error {
	if err := p.scopes.With(scope, func(s *scopeState) error {
		s.reset()
		return nil
	}); err != nil {
		return fmt.Errorf("clear the compute scope: %w", err)
	}
	p.mu.Lock()
	for name := range p.records {
		if strings.HasPrefix(name, scope.Project+"/") {
			delete(p.records, name)
		}
	}
	p.mu.Unlock()
	return nil
}

// authorize evaluates the method's permission plan for one call.
func (p *Provider) authorize(call gateway.Call, target iam.Target) (iam.Decision, error) {
	decision, authorizeErr := p.engine.Authorize(call, target, nil)
	if authorizeErr != nil {
		return decision, fmt.Errorf("authorize %s: %w", call.Method, authorizeErr)
	}
	return decision, nil
}

// handle is the gateway entry point for every compute method.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid compute request")
	}
	scope, target := p.resolveTarget(call, request)
	decision, authorizeErr := p.authorize(call, target)
	if authorizeErr != nil {
		code := status.Code(authorizeErr)
		if code != codes.PermissionDenied && code != codes.Unauthenticated {
			return nil, authorizeErr
		}
		return p.denied(call, decision, target), nil
	}
	call.Resource = target.Name
	call.Location = scope.Location
	response, err := p.execute(call, request, scope)
	if err != nil {
		return nil, err
	}
	if response.Headers == nil {
		response.Headers = http.Header{}
	}
	response.Headers.Set("X-Gcp-Local-Decision-Id", decision.ID)
	return response, nil
}

// resolveTarget derives the scope and IAM target for the request's path shape.
// Regions are placement attributes, not namespaces (the storage precedent):
// every compute resource lives in the project's global scope so network
// references resolve across regions.
func (p *Provider) resolveTarget(_ gateway.Call, request *gateway.RESTRequest) (state.Scope, iam.Target) {
	project := request.Path["project"]
	return scopeOf(project, state.Global), iam.Target{Name: projectResource(project)}
}

// projectResource names the resource-manager project ancestor.
func projectResource(project string) string {
	return "//cloudresourcemanager.googleapis.com/projects/" + project
}

// denied renders the compute denial shape.
func (p *Provider) denied(call gateway.Call, decision iam.Decision, target iam.Target) *gateway.RESTResponse {
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
	return wireError(code, "forbidden", fmt.Sprintf("%s does not have %s access to %s.", actor, permission, target.Name))
}
