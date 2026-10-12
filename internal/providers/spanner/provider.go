package spanner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
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

// Provider is the authorization gate in front of the official emulator. It
// owns IAM policies and index references; the emulator owns resource state.
type Provider struct {
	manager  *backend.Manager
	index    *state.Index
	engine   *iam.Engine
	created  *state.Store[createdDatabases]
	clock    func() time.Time
	poisoned sync.Map // scopes with an unresolved mutation outcome, cleared only on reset
}

// New registers the Spanner provider backed by the pinned official emulator.
func New(_ context.Context, registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine, config Config) (*Provider, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	return newProvider(registry, manager, index, engine, func(scope state.Scope) (backend.Backend, error) {
		return &emulator{config: config, scope: scope}, nil
	})
}

// newProvider wires the provider with an arbitrary backend factory (tests use a fake driver).
func newProvider(registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine, factory func(state.Scope) (backend.Backend, error)) (*Provider, error) {
	if registry == nil || manager == nil || index == nil || engine == nil {
		return nil, errProviderShape
	}
	if err := provider.RegisterPermissions(engine, permissions); err != nil {
		return nil, fmt.Errorf("spanner permissions: %w", err)
	}
	if err := registerDataPermissions(engine); err != nil {
		return nil, err
	}
	p := &Provider{manager: manager, index: index, engine: engine, clock: time.Now,
		created: state.NewStore(func() createdDatabases { return createdDatabases{} }, state.GlobalScope)}
	if err := manager.Register(serviceName, backend.Definition{
		Validate: state.GlobalScope,
		New:      factory,
		// The manager stops the emulator (discarding its in-memory state)
		// before this hook clears the provider-owned policies and references.
		AfterReset: func(scope state.Scope) error {
			if err := p.created.Reset(scope); err != nil {
				return fmt.Errorf("clear the Spanner scope: %w", err)
			}
			engine.ClearScope(serviceName, scope)
			index.RemoveScope(serviceName, scope)
			p.poisoned.Delete(scope)
			return nil
		},
	}); err != nil {
		return nil, fmt.Errorf("register the Spanner backend: %w", err)
	}
	if err := p.registerHandlers(registry); err != nil {
		return nil, err
	}
	return p, nil
}

// registerHandlers binds the admin REST handlers and the data-plane handlers.
func (p *Provider) registerHandlers(registry *gateway.Registry) error {
	for method := range permissions {
		if err := registry.Register(method, p.handle); err != nil {
			return fmt.Errorf("register %s: %w", method, err)
		}
	}
	return p.registerData(registry)
}

// handle is the gateway entry point for every Spanner method. Authorization
// completes before any backend is started or called.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Spanner request")
	}
	scope := provider.ProjectScope(request.Path[pathProject])
	if _, failed := p.poisoned.Load(scope); failed {
		return nil, status.Error(codes.FailedPrecondition, "A Spanner mutation outcome is unresolved; reset this scope")
	}
	target := p.target(request.Path)
	decision, authorizeErr := p.engine.Authorize(call, target, nil)
	if authorizeErr != nil {
		code := status.Code(authorizeErr)
		if code != codes.PermissionDenied && code != codes.Unauthenticated {
			return nil, fmt.Errorf("authorize %s: %w", call.Method, authorizeErr)
		}
		return provider.Denied(decision, target.Name), nil
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

// target selects the IAM resource for a request: the addressed database or
// instance when the index knows it, else its nearest indexed ancestor, so a
// missing resource is authorized against its parent and then reported by the
// emulator, as GCP does.
func (p *Provider) target(path map[string]string) iam.Target {
	project := path[pathProject]
	names := []string{provider.ProjectResource(project)}
	if instance := path[pathInstance]; instance != "" {
		names = append(names, fullName(instanceName(project, instance)))
		if database := path[pathDatabase]; database != "" {
			names = append(names, fullName(databaseName(project, instance, database)))
		}
	}
	for i := len(names) - 1; i > 0; i-- {
		if resource, ok := p.index.Get(names[i]); ok && resource.Service == serviceName && resource.Scope == provider.ProjectScope(project) {
			return iam.Target{Name: names[i]}
		}
	}
	return iam.Target{Name: names[0]}
}

// addressed names the instance or database an IAM-policy request addresses.
func addressed(path map[string]string) string {
	name := instanceName(path[pathProject], path[pathInstance])
	if database := path[pathDatabase]; database != "" {
		name += "/databases/" + database
	}
	return fullName(name)
}

// execute serves IAM policies from the engine and forwards everything else.
func (p *Provider) execute(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, target iam.Target) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodInstancesGetIam, methodDatabasesGetIam, methodInstancesSetIam, methodDatabasesSetIam:
		if target.Name != addressed(request.Path) {
			return nil, status.Errorf(codes.NotFound, "Resource not found: %s", addressed(request.Path))
		}
		if call.Method == methodInstancesGetIam || call.Method == methodDatabasesGetIam {
			return p.getIam(target)
		}
		return p.setIam(request, target)
	default:
		return p.forward(call, request, scope)
	}
}

// getIam returns the stored policy of an instance or database.
func (p *Provider) getIam(target iam.Target) (*gateway.RESTResponse, error) {
	policy, err := p.engine.GetPolicy(target.Name)
	if err != nil {
		return nil, fmt.Errorf("read the Spanner policy: %w", err)
	}
	return &gateway.RESTResponse{JSON: policy.Wire()}, nil
}

// setIam stores the policy of an instance or database.
func (p *Provider) setIam(request *gateway.RESTRequest, target iam.Target) (*gateway.RESTResponse, error) {
	var envelope struct {
		Policy iam.Policy `json:"policy"`
	}
	if json.Unmarshal(request.JSON, &envelope) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid IAM policy")
	}
	policy, err := p.engine.SetPolicy(target.Name, envelope.Policy)
	if err != nil {
		return nil, fmt.Errorf("write the Spanner policy: %w", err)
	}
	return &gateway.RESTResponse{JSON: policy}, nil
}

// errBackendType reports a backend that is not a Spanner driver.
var errBackendType = errors.New("spanner backend is not an emulator driver")

// driverOf narrows the manager's backend to the forwarding contract.
func driverOf(active backend.Backend) (driver, error) {
	value, ok := active.(driver)
	if !ok {
		return nil, errBackendType
	}
	return value, nil
}
