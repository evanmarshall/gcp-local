package artifactregistry

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Provider owns the Artifact Registry control plane and repository state.
type Provider struct {
	index  *state.Index
	engine *iam.Engine
	scopes *state.Store[scopeState]
	clock  func() time.Time
}

// New registers the Artifact Registry provider.
func New(_ context.Context, registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine) (*Provider, error) {
	if registry == nil || manager == nil || index == nil || engine == nil {
		return nil, errProviderShape
	}
	if err := registerPermissions(engine); err != nil {
		return nil, err
	}
	p := &Provider{
		index: index, engine: engine,
		scopes: state.NewStore(newScopeState, state.GlobalScope),
		clock:  time.Now,
	}
	if err := p.registerBackend(manager, engine, index); err != nil {
		return nil, err
	}
	for method := range permissions {
		if err := registry.Register(method, p.handle); err != nil {
			return nil, fmt.Errorf("register %s: %w", method, err)
		}
	}
	return p, nil
}

// registerBackend attaches the in-memory backend and its reset hook.
func (p *Provider) registerBackend(manager *backend.Manager, engine *iam.Engine, index *state.Index) error {
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
		return fmt.Errorf("register the Artifact Registry backend: %w", err)
	}
	return nil
}

// emptyBackend satisfies the backend contract for an in-memory provider.
type emptyBackend struct{}

func (emptyBackend) Start(context.Context) error    { return nil }
func (emptyBackend) Ready(context.Context) error    { return nil }
func (emptyBackend) Stop(context.Context) error     { return nil }
func (emptyBackend) Reset(context.Context) error    { return nil }
func (emptyBackend) Snapshot(context.Context) error { return backend.ErrPersistenceUnsupported }
func (emptyBackend) Restore(context.Context) error  { return backend.ErrPersistenceUnsupported }

// Reset clears the scope's repositories.
func (p *Provider) Reset(scope state.Scope) error {
	if err := p.scopes.With(scope, func(s *scopeState) error {
		s.reset()
		return nil
	}); err != nil {
		return fmt.Errorf("clear the artifactregistry scope: %w", err)
	}
	return nil
}

// registerPermissions binds every method to its permission.
func registerPermissions(engine *iam.Engine) error {
	for method, permission := range permissions {
		if err := engine.Register(method, iam.Rule{Baseline: []string{permission}}); err != nil {
			return fmt.Errorf("register %s: %w", method, err)
		}
	}
	return nil
}

// handle is the gateway entry point for every Artifact Registry method.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Artifact Registry request")
	}
	scope, target, err := p.resolve(call, request)
	if err != nil {
		return nil, err
	}
	decision, authorizeErr := p.engine.Authorize(call, target, nil)
	if authorizeErr != nil {
		code := status.Code(authorizeErr)
		if code != codes.PermissionDenied && code != codes.Unauthenticated {
			return nil, fmt.Errorf("authorize %s: %w", call.Method, authorizeErr)
		}
		return p.denied(call, decision, target), nil
	}
	call.Resource = target.Name
	call.Location = scope.Location
	response, err := p.execute(call, request, scope, target)
	if err != nil {
		return nil, err
	}
	if response.Headers == nil {
		response.Headers = http.Header{}
	}
	response.Headers.Set("X-Gcp-Local-Decision-Id", decision.ID)
	return response, nil
}

// denied renders the denial shape.
func (p *Provider) denied(_ gateway.Call, decision iam.Decision, target iam.Target) *gateway.RESTResponse {
	permission := "unspecified"
	for _, check := range decision.Checks {
		if !check.Allowed {
			permission = check.Permission
			break
		}
	}
	return wireError(403, "PERMISSION_DENIED", fmt.Sprintf("%s does not have %s access to %s.", "The caller", permission, target.Name))
}

// resolve derives the scope and IAM target for one request.
func (p *Provider) resolve(call gateway.Call, request *gateway.RESTRequest) (state.Scope, iam.Target, error) {
	project := request.Path["project"]
	scope := scopeOf(project)
	target := iam.Target{Name: projectResource(project)}
	repository := request.Path["repository"]
	switch call.Method {
	case methodReposCreate, methodReposList:
		return scope, target, nil
	case methodReposGet, methodReposUpdate, methodReposDelete, methodReposGetIam, methodReposSetIam:
		if !uid.MatchString(repository) {
			return scope, target, status.Errorf(codes.InvalidArgument, "Invalid repository id %q", repository)
		}
		if p.repositoryLocked(scope, repository) == nil {
			return scope, target, notFound(kindRepository, repository)
		}
		target.Name = repositoryResource(project, repository)
		return scope, target, nil
	default:
		return scope, target, status.Errorf(codes.Internal, "Unknown Artifact Registry method %s", call.Method)
	}
}
