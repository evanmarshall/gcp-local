package spanner

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

// Provider owns the Spanner admin control plane and instance state.
type Provider struct {
	index  *state.Index
	engine *iam.Engine
	scopes *state.Store[scopeState]
	clock  func() time.Time
}

// New registers the Spanner provider.
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

// registerBackend attaches the in-memory Spanner backend and its reset hook.
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
		return fmt.Errorf("register the Spanner backend: %w", err)
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

// Reset clears the scope's instances and databases.
func (p *Provider) Reset(scope state.Scope) error {
	if err := p.scopes.With(scope, func(s *scopeState) error {
		s.reset()
		return nil
	}); err != nil {
		return fmt.Errorf("clear the spanner scope: %w", err)
	}
	return nil
}

// locked runs use under the scope lock. Scope failures are wrapped; use's
// own gRPC status errors are returned unchanged so the gateway keeps their code.
func (p *Provider) locked(scope state.Scope, use func(*scopeState) error) error {
	var failure error
	if err := p.scopes.With(scope, func(s *scopeState) error {
		failure = use(s)
		return nil
	}); err != nil {
		return fmt.Errorf("open the spanner scope: %w", err)
	}
	return failure
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

// handle is the gateway entry point for every Spanner method.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Spanner request")
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

// denied renders the Spanner denial shape.
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

// resolve derives the scope and IAM target for one request. Instance and
// instance-child methods authorize against the instance; database methods
// authorize against the database, inheriting via the instance; config and
// project-list methods authorize against the project.
func (p *Provider) resolve(call gateway.Call, request *gateway.RESTRequest) (state.Scope, iam.Target, error) {
	project := request.Path["project"]
	scope := scopeOf(project)
	target := iam.Target{Name: projectResource(project)}
	instance, database := request.Path["instance"], request.Path["database"]
	var err error
	switch call.Method {
	case methodConfigsGet, methodConfigsList, methodInstancesCreate, methodInstancesList:
		return scope, target, nil
	case methodBackupsList:
		err = validInstance(instance)
	case methodInstancesGet, methodInstancesUpdate, methodInstancesDelete, methodInstancesGetIam,
		methodInstancesSetIam, methodDatabasesList, methodDatabasesCreate:
		err = p.existingInstance(scope, instance)
	default:
		if err = p.existingDatabase(scope, instance, database); err == nil {
			target.Name = databaseResource(project, instance, database)
		}
		return scope, target, err
	}
	if err == nil {
		target.Name = instanceResource(project, instance)
	}
	return scope, target, err
}

// validInstance checks the instance id syntax.
func validInstance(instance string) error {
	if !uid.MatchString(instance) {
		return status.Errorf(codes.InvalidArgument, "Invalid instance id %q", instance)
	}
	return nil
}

// existingInstance checks that the instance id is valid and stored.
func (p *Provider) existingInstance(scope state.Scope, instance string) error {
	if err := validInstance(instance); err != nil {
		return err
	}
	if p.instanceLocked(scope, instance) == nil {
		return notFound(kindInstance, instance)
	}
	return nil
}

// existingDatabase checks that the database and its instance are stored.
func (p *Provider) existingDatabase(scope state.Scope, instance, database string) error {
	if !databaseID.MatchString(database) {
		return status.Errorf(codes.InvalidArgument, "Invalid database id %q", database)
	}
	if err := p.existingInstance(scope, instance); err != nil {
		return err
	}
	if p.databaseLocked(scope, instance, database) == nil {
		return notFound(kindDatabase, database)
	}
	return nil
}
