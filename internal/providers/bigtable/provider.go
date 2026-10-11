package bigtable

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

// Provider owns the Bigtable admin control plane and instance/table state.
type Provider struct {
	index  *state.Index
	engine *iam.Engine
	scopes *state.Store[scopeState]
	clock  func() time.Time
}

// New registers the Bigtable provider.
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

// registerBackend attaches the in-memory Bigtable backend and its reset hook.
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
		return fmt.Errorf("register the Bigtable backend: %w", err)
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

// Reset clears the scope's instances and tables.
func (p *Provider) Reset(scope state.Scope) error {
	if err := p.scopes.With(scope, func(s *scopeState) error {
		s.reset()
		return nil
	}); err != nil {
		return fmt.Errorf("clear the bigtable scope: %w", err)
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

// handle is the gateway entry point for every Bigtable method.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Bigtable request")
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

// denied renders the Bigtable denial shape.
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

// resolve derives the scope and IAM target for one request. Instance methods
// authorize against the instance; table methods inherit via the instance;
// list and create methods authorize against the project.
func (p *Provider) resolve(call gateway.Call, request *gateway.RESTRequest) (state.Scope, iam.Target, error) {
	project := request.Path["project"]
	scope := scopeOf(project)
	target := iam.Target{Name: projectResource(project)}
	instance, table := request.Path["instance"], request.Path["table"]
	switch call.Method {
	case methodInstancesCreate, methodInstancesList:
		return scope, target, nil
	case methodInstancesGet, methodInstancesUpdate, methodInstancesDelete, methodInstancesGetIam, methodInstancesSetIam:
		if !uid.MatchString(instance) {
			return scope, target, status.Errorf(codes.InvalidArgument, "Invalid instance id %q", instance)
		}
		if p.instanceLocked(scope, instance) == nil {
			return scope, target, notFound(kindInstance, instance)
		}
		target.Name = instanceResource(project, instance)
		return scope, target, nil
	case methodClustersList, methodClustersGet, methodTablesCreate, methodTablesList:
		if !uid.MatchString(instance) {
			return scope, target, status.Errorf(codes.InvalidArgument, "Invalid instance id %q", instance)
		}
		if p.instanceLocked(scope, instance) == nil {
			return scope, target, notFound(kindInstance, instance)
		}
		target.Name = instanceResource(project, instance)
		return scope, target, nil
	default:
		if !uid.MatchString(instance) || !uid.MatchString(table) {
			return scope, target, status.Errorf(codes.InvalidArgument, "Invalid table id %q", table)
		}
		if p.instanceLocked(scope, instance) == nil {
			return scope, target, notFound(kindInstance, instance)
		}
		if p.tableLocked(scope, instance, table) == nil {
			return scope, target, notFound(kindTable, table)
		}
		target.Name = tableResource(project, instance, table)
		return scope, target, nil
	}
}
