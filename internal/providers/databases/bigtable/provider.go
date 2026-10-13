package bigtable

import (
	"context"
	"fmt"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
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
	gate    provider.Gate
	scopes  *state.Store[scopeState]
	clock   func() time.Time
	// operations holds instance and table operations, named after their
	// resource and zone.
	operations *provider.Operations
	// serverOptions are passed to each emulator; tests use them to observe calls.
	serverOptions []grpc.ServerOption
}

// New registers the Bigtable provider.
func New(_ context.Context, registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine) (*Provider, error) {
	if registry == nil || manager == nil || index == nil || engine == nil {
		return nil, errProviderShape
	}
	if err := provider.RegisterPermissions(engine, permissions); err != nil {
		return nil, fmt.Errorf("bigtable permissions: %w", err)
	}
	p := &Provider{
		manager: manager, index: index, engine: engine,
		gate:   provider.Gate{Engine: engine, Index: index, Message: provider.MissingPermission, Attributes: attributes},
		scopes: state.NewStore(newScopeState, state.GlobalScope),
		clock:  time.Now,
	}
	p.operations = provider.NewOperations(state.GlobalScope, createLatency, operationMetadata).Named(operationName)
	if err := p.registerBackend(manager, engine, index); err != nil {
		return nil, err
	}
	for method := range permissions {
		if err := registry.Register(method, p.handle); err != nil {
			return nil, fmt.Errorf("register %s: %w", method, err)
		}
	}
	if err := p.registerData(registry, engine); err != nil {
		return nil, err
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

// Reset clears the scope's provider-owned instances and operations.
func (p *Provider) Reset(scope state.Scope) error {
	if err := p.scopes.With(scope, func(s *scopeState) error {
		s.reset()
		return nil
	}); err != nil {
		return fmt.Errorf("clear the bigtable scope: %w", err)
	}
	if err := p.operations.Reset(scope); err != nil {
		return fmt.Errorf("clear the bigtable operations: %w", err)
	}
	return nil
}

// handle is the gateway entry point for every Bigtable method.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Bigtable request")
	}
	scope, candidates, err := p.resolve(call, request)
	if err != nil {
		return nil, err
	}
	// Table and instance deletes unindex under the scope lock Nearest holds,
	// so a request racing one reads 404, never an unverified target.
	decision, target, err := provider.Nearest(p.gate, p.scopes, scope, call, "", candidates...)
	if err != nil {
		return nil, credentials(call, err, adminHost, rpcNames[call.Method])
	}
	call.Resource = target.Name
	call.Location = scope.Location
	response, err := p.execute(call, request, scope, target)
	if err != nil {
		return nil, err
	}
	return provider.WithDecision(response, decision), nil
}

// credentials replaces the refusal of a call without credentials with GCP's
// CREDENTIALS_MISSING 401 for rpc on host; other refusals pass unchanged.
func credentials(call gateway.Call, err error, host, rpc string) error {
	if status.Code(err) == codes.Unauthenticated && call.Principal.State == gateway.Anonymous {
		return provider.CredentialsMissing(host, rpc) //nolint:wrapcheck // a wire status
	}
	return err
}

// resolve derives the scope and the IAM target candidates, most specific
// first, for one request; it never calls the backend. Instance methods
// authorize against the instance; list and create methods against the project.
// Table methods authorize against a published table, or against its instance
// when the table is not published, so absence is reported only to callers that
// hold the permission.
func (p *Provider) resolve(call gateway.Call, request *gateway.RESTRequest) (state.Scope, []string, error) {
	project := request.Path["project"]
	scope := provider.ProjectScope(project)
	candidates := []string{provider.ProjectResource(project)}
	instance, table := request.Path["instance"], request.Path["table"]
	switch call.Method {
	case methodInstancesCreate, methodInstancesList, methodOperationsList:
		return scope, candidates, nil
	case methodOperationGet, methodOperationDelete, methodOperationCancel:
		// An operation outlives its instance, so the instance need not exist.
		return scope, append([]string{instanceResource(project, instance)}, candidates...), nil
	case methodTableOpGet, methodTableOpDelete, methodTableOpCancel:
		return scope, append([]string{tableResource(project, instance, table), instanceResource(project, instance)}, candidates...), nil
	case methodInstancesGet, methodInstancesUpdate, methodInstancesDelete, methodInstancesGetIam, methodInstancesSetIam,
		methodClustersList, methodClustersGet, methodTablesCreate, methodTablesList:
		if err := p.existingInstance(scope, instance); err != nil {
			return scope, nil, err
		}
		return scope, append([]string{instanceResource(project, instance)}, candidates...), nil
	default:
		if !tableID.MatchString(table) {
			return scope, nil, status.Errorf(codes.InvalidArgument, "Invalid table id %q", table)
		}
		if err := p.existingInstance(scope, instance); err != nil {
			return scope, nil, err
		}
		return scope, append([]string{tableResource(project, instance, table), instanceResource(project, instance)}, candidates...), nil
	}
}

// existingInstance validates an instance id and requires the instance.
func (p *Provider) existingInstance(scope state.Scope, instance string) error {
	if !uid.MatchString(instance) {
		return status.Errorf(codes.InvalidArgument, "Invalid instance id %q", instance)
	}
	return p.withInstance(scope, instance, func(*scopeState, *instanceRecord) error { return nil })
}
