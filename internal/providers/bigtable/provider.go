package bigtable

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Provider owns the Bigtable admin control plane and instance state; tables
// live in one in-process emulator per project.
type Provider struct {
	manager *backend.Manager
	index   *state.Index
	engine  *iam.Engine
	scopes  *state.Store[scopeState]
	clock   func() time.Time
	// operations numbers instance and table operation names.
	operations atomic.Uint64
	// serverOptions are passed to each emulator; tests use them to observe calls.
	serverOptions []grpc.ServerOption
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
		manager: manager, index: index, engine: engine,
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

// registerBackend attaches one in-process Bigtable emulator per project scope.
// Reset stops the emulator, discarding its tables, before clearing instances,
// policies, and index entries for that project.
func (p *Provider) registerBackend(manager *backend.Manager, engine *iam.Engine, index *state.Index) error {
	if err := manager.Register(serviceName, backend.Definition{
		Validate: state.GlobalScope,
		New: func(state.Scope) (backend.Backend, error) {
			return &emulatorBackend{options: p.serverOptions}, nil
		},
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

// Reset clears the scope's provider-owned instances.
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

// resolve derives the scope and IAM target for one request from provider
// state and the shared index only; it never calls the backend. Instance methods
// authorize against the instance; list and create methods against the project.
// Table methods authorize against a published table, or against its instance
// when the table is not published, so absence is reported only to callers that
// hold the permission.
func (p *Provider) resolve(call gateway.Call, request *gateway.RESTRequest) (state.Scope, iam.Target, error) {
	project := request.Path["project"]
	scope := scopeOf(project)
	target := iam.Target{Name: projectResource(project)}
	instance, table := request.Path["instance"], request.Path["table"]
	switch call.Method {
	case methodInstancesCreate, methodInstancesList:
		return scope, target, nil
	case methodInstancesGet, methodInstancesUpdate, methodInstancesDelete, methodInstancesGetIam, methodInstancesSetIam,
		methodClustersList, methodClustersGet, methodTablesCreate, methodTablesList:
		if err := p.existingInstance(scope, instance); err != nil {
			return scope, target, err
		}
		target.Name = instanceResource(project, instance)
		return scope, target, nil
	default:
		if !tableID.MatchString(table) {
			return scope, target, status.Errorf(codes.InvalidArgument, "Invalid table id %q", table)
		}
		if err := p.existingInstance(scope, instance); err != nil {
			return scope, target, err
		}
		target.Name = instanceResource(project, instance)
		if resource, found := p.index.Get(tableResource(project, instance, table)); found && resource.Scope == scope {
			target.Name = resource.Name
		}
		return scope, target, nil
	}
}

// existingInstance validates an instance id and requires the instance.
func (p *Provider) existingInstance(scope state.Scope, instance string) error {
	if !uid.MatchString(instance) {
		return status.Errorf(codes.InvalidArgument, "Invalid instance id %q", instance)
	}
	if p.instanceLocked(scope, instance) == nil {
		return notFound(kindInstance, instance)
	}
	return nil
}
