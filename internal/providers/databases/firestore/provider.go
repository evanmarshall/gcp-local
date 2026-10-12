package firestore

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Provider is the Firestore authentication and authorization gate. The
// official emulator owns documents; the provider owns collection-group field
// configuration, which the emulator does not implement.
type Provider struct {
	index   *state.Index
	engine  *iam.Engine
	manager *backend.Manager
	scopes  *state.Store[scopeState]
	clock   func() time.Time
}

// New registers the Firestore provider backed by the official emulator.
func New(_ context.Context, registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine, config Config) (*Provider, error) {
	return newProvider(registry, manager, index, engine, func(scope state.Scope) (backend.Backend, error) {
		return &emulator{config: config, scope: scope}, nil
	})
}

func newProvider(registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine, factory func(state.Scope) (backend.Backend, error)) (*Provider, error) {
	if registry == nil || manager == nil || index == nil || engine == nil || factory == nil {
		return nil, errProviderShape
	}
	if err := registerPermissions(engine); err != nil {
		return nil, err
	}
	p := &Provider{
		index: index, engine: engine, manager: manager,
		scopes: state.NewStore(newScopeState, state.GlobalScope),
		clock:  time.Now,
	}
	if err := p.registerBackend(factory); err != nil {
		return nil, err
	}
	if err := p.registerMethods(registry); err != nil {
		return nil, err
	}
	return p, nil
}

// registerMethods binds the served REST routes and gRPC methods.
func (p *Provider) registerMethods(registry *gateway.Registry) error {
	for _, method := range restMethods {
		if err := registry.Register(method, p.handle); err != nil {
			return fmt.Errorf("register %s: %w", method, err)
		}
	}
	return p.registerGRPC(registry)
}

// registerBackend attaches the per-project emulator and its reset hook.
func (p *Provider) registerBackend(factory func(state.Scope) (backend.Backend, error)) error {
	if err := p.manager.Register(serviceName, backend.Definition{
		Validate: state.GlobalScope,
		New:      factory,
		AfterReset: func(scope state.Scope) error {
			if err := p.Reset(scope); err != nil {
				return err
			}
			p.engine.ClearScope(serviceName, scope)
			p.index.RemoveScope(serviceName, scope)
			return nil
		},
	}); err != nil {
		return fmt.Errorf("register the Firestore backend: %w", err)
	}
	return nil
}

// Reset clears the scope's provider-owned field configuration. The backend
// manager has already stopped the emulator, which discards its documents.
func (p *Provider) Reset(scope state.Scope) error {
	if err := p.scopes.With(scope, func(s *scopeState) error {
		s.reset()
		return nil
	}); err != nil {
		return fmt.Errorf("clear the firestore scope: %w", err)
	}
	return nil
}

// registerPermissions registers every permission plan.
func registerPermissions(engine *iam.Engine) error {
	for plan, required := range plans {
		if err := engine.Register(plan, iam.Rule{Baseline: required}); err != nil {
			return fmt.Errorf("register %s: %w", plan, err)
		}
	}
	return nil
}

// authorize evaluates each plan against the project's (default) database.
// Every plan must allow; the first refusal is returned with its decision.
func (p *Provider) authorize(call gateway.Call, project string, planNames []string) (iam.Decision, error) {
	if len(planNames) == 0 {
		return iam.Decision{}, status.Error(codes.Internal, "Firestore request has no permission plan")
	}
	target := iam.Target{Name: databaseResource(project)}
	var decision iam.Decision
	for _, plan := range planNames {
		planned := call
		planned.Method = plan
		var err error
		if decision, err = p.engine.Authorize(planned, target, nil); err != nil {
			return decision, err //nolint:wrapcheck // the IAM status is the wire error
		}
	}
	return decision, nil
}

// projectScope validates a REST project and registers its default database.
func (p *Provider) projectScope(project string) (state.Scope, error) {
	scope := provider.ProjectScope(project)
	if err := scope.Validate(); err != nil {
		return scope, status.Errorf(codes.InvalidArgument, "Invalid project %q", project)
	}
	return scope, p.ensureDatabase(scope)
}

// handle is the gateway entry point for every Firestore REST method.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Firestore request")
	}
	project := request.Path["project"]
	scope, err := p.projectScope(project)
	if err != nil {
		return nil, err
	}
	operation, err := restOperationFor(call.Method, request)
	if err != nil {
		return nil, err
	}
	decision, authorizeErr := p.authorize(call, project, operation.plans)
	if authorizeErr != nil {
		code := status.Code(authorizeErr)
		if code != codes.PermissionDenied && code != codes.Unauthenticated {
			return nil, fmt.Errorf("authorize %s: %w", call.Method, authorizeErr)
		}
		return streamed(call.Method, provider.Denied(decision, databaseResource(project))), nil
	}
	call.Resource = databaseResource(project)
	call.Location = scope.Location
	var response *gateway.RESTResponse
	err = p.manager.Use(call.Context, backend.Key{Service: serviceName, Scope: scope}, func(ctx context.Context, active backend.Backend) error {
		b, ok := active.(driver)
		if !ok {
			return status.Error(codes.Internal, "Invalid Firestore backend")
		}
		var executeErr error
		response, executeErr = p.execute(ctx, b, call, request, scope, operation)
		return executeErr
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // keep the emulator or manager status unchanged on the wire
	}
	if response.Headers == nil {
		response.Headers = http.Header{}
	}
	response.Headers.Set("X-Gcp-Local-Decision-Id", decision.ID)
	return response, nil
}

// ensureDatabase registers the project's default database resource once, so
// document authorization anchors on it. The call is idempotent per project.
func (p *Provider) ensureDatabase(scope state.Scope) error {
	name := databaseResource(scope.Project)
	if _, ok := p.index.Get(name); ok {
		return nil
	}
	if err := p.index.Put(serviceName, state.Resource{
		Name: name, Service: serviceName,
		Kind: "database", Parent: provider.ProjectResource(scope.Project), Scope: scope,
	}); err != nil {
		return fmt.Errorf("index the default database: %w", err)
	}
	return nil
}

// streamed wraps a response to a streamed REST method in a JSON array, as
// Firestore does for errors too.
func streamed(method string, response *gateway.RESTResponse) *gateway.RESTResponse {
	if streamedREST[method] {
		response.JSON = []any{response.JSON}
	}
	return response
}
