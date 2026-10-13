package memorystore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Provider is the authorization gate and metadata owner in front of the Redis servers.
type Provider struct {
	registry   *gateway.Registry
	manager    *backend.Manager
	index      *state.Index
	gate       provider.Gate
	catalog    *state.Store[catalog]
	operations *provider.Operations
	clock      func() time.Time
}

// New registers the Memorystore provider backed by the pinned redis-server.
func New(ctx context.Context, registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine, config Config) (*Provider, error) {
	if err := config.validate(ctx); err != nil {
		return nil, err
	}
	return newProvider(registry, manager, index, engine, operationLatency, func(state.Scope) (backend.Backend, error) {
		return newFleet(config), nil
	})
}

// newProvider wires the provider with an operation latency and an arbitrary
// backend factory (tests use a fake driver).
func newProvider(registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine, latency time.Duration, factory func(state.Scope) (backend.Backend, error)) (*Provider, error) {
	if registry == nil || manager == nil || index == nil || engine == nil || factory == nil {
		return nil, errProviderShape
	}
	if err := provider.RegisterPermissions(engine, permissions); err != nil {
		return nil, err //nolint:wrapcheck // names the method
	}
	p := &Provider{registry: registry, manager: manager, index: index, clock: time.Now,
		gate:    provider.Gate{Engine: engine, Index: index, Message: provider.PermissionDeniedOn},
		catalog: state.NewStore(newCatalog, validScope), operations: provider.NewOperations(validScope, latency, operationMetadata)}
	if err := p.registerBackend(factory); err != nil {
		return nil, err
	}
	for method := range permissions {
		if err := registry.Register(method, p.handle); err != nil {
			return nil, fmt.Errorf("register %s: %w", method, err)
		}
	}
	return p, nil
}

// registerBackend declares the per-project/region fleet and its reset hook.
func (p *Provider) registerBackend(factory func(state.Scope) (backend.Backend, error)) error {
	if err := p.manager.Register(serviceName, backend.Definition{
		Validate: validScope,
		New:      factory,
		// The manager stops every Redis server in the scope (discarding their
		// keyspaces) before this hook clears instance metadata and references.
		// References go first, under the scope lock authorization holds.
		AfterReset: func(scope state.Scope) error {
			unindex := p.catalog.With(scope, func(*catalog) error {
				p.index.RemoveScope(serviceName, scope)
				return nil
			})
			if err := errors.Join(unindex, p.catalog.Reset(scope), p.operations.Reset(scope)); err != nil {
				return fmt.Errorf("clear the Memorystore scope: %w", err)
			}
			return nil
		},
	}); err != nil {
		return fmt.Errorf("register the Memorystore backend: %w", err)
	}
	return nil
}

// handle is the gateway entry point. Authorization completes before any
// backend is started, probed, or changed.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Memorystore request")
	}
	project, location := request.Path[pathProject], request.Path[pathLocation]
	scope := state.Scope{Project: project, Location: location}
	decision, err := p.authorize(call, request.Path, scope)
	if err != nil {
		return nil, err
	}
	if location == "-" {
		return nil, gap("listing across all locations (\"-\")")
	}
	if validScope(scope) != nil {
		return nil, invalid("Location %q is not a Memorystore region", location)
	}
	call.Location = scope.Location
	response, err := p.execute(call, request, scope)
	if err != nil {
		return nil, err
	}
	return provider.WithDecision(response, decision), nil
}

// authorize checks the call's permission on the project: Memorystore has no
// instance-level policies. IAM Conditions see the addressed resource's name
// (the instance, or for create and list its location, even when the
// instance doesn't exist) and resource.service "redis.googleapis.com", as
// GCP evaluates them (probed). GCP reports resource.type as "", which the
// engine treats as absent. A denial names the addressed resource (recorded).
func (p *Provider) authorize(call gateway.Call, path map[string]string, scope state.Scope) (iam.Decision, error) {
	target := iam.Target{Name: provider.ProjectResource(scope.Project)}
	switch call.Method {
	case methodCreate, methodList:
		target.Attributes = iam.ResourceAttributes{Name: locationName(scope), Service: redisService}
	case methodOperationGet, methodOperationList, methodOperationStop, methodOperationDrop:
	default:
		target.Attributes = iam.ResourceAttributes{Name: instanceName(scope, path[pathInstance]), Service: redisService}
	}
	return p.gate.Check(call, target, addressed(call.Method, path))
}

// addressed names the resource a request acts on, which a denial reports.
func addressed(method string, path map[string]string) string {
	scope := state.Scope{Project: path[pathProject], Location: path[pathLocation]}
	switch method {
	case methodCreate, methodOperationList:
		return locationName(scope)
	case methodList:
		return locationName(scope) + "/instances" // recorded
	case methodOperationGet, methodOperationStop, methodOperationDrop:
		return provider.OperationName(scope, path[pathOperation])
	default:
		return instanceName(scope, path[pathInstance])
	}
}

// execute dispatches one authorized call.
func (p *Provider) execute(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	ctx := call.Context
	switch call.Method {
	case methodCreate:
		return p.create(ctx, request, scope)
	case methodGet:
		return p.get(ctx, request.Path[pathInstance], scope)
	case methodList:
		return p.list(ctx, request, scope)
	case methodPatch:
		return p.patch(ctx, request, scope)
	case methodDelete:
		return p.remove(ctx, request.Path[pathInstance], scope)
	case methodAuthString:
		return p.authString(ctx, request.Path[pathInstance], scope)
	default:
		return p.executeOperation(call.Method, request, scope)
	}
}

// executeOperation dispatches one authorized operations method.
func (p *Provider) executeOperation(method string, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	id := request.Path[pathOperation]
	switch method {
	case methodOperationGet:
		return p.operation(scope, id)
	case methodOperationList:
		return p.listOperations(request, scope)
	case methodOperationStop:
		return p.cancel(scope, id)
	case methodOperationDrop:
		return empty(p.operations.Delete(scope, id))
	default:
		return nil, gap("method " + method)
	}
}

// errBackendType reports a backend that is not a Redis fleet driver.
var errBackendType = errors.New("memorystore backend is not a redis driver")

// use runs one operation on the scope's backend inside Manager.Use, which
// starts the scope lazily, probes every running server, and serializes the
// call with reset and shutdown.
func (p *Provider) use(ctx context.Context, scope state.Scope, operation func(context.Context, driver) error) error {
	err := p.manager.Use(ctx, backend.Key{Service: serviceName, Scope: scope}, func(ctx context.Context, active backend.Backend) error {
		current, ok := active.(driver)
		if !ok {
			return errBackendType
		}
		return operation(ctx, current)
	})
	if errors.Is(err, backend.ErrFailed) {
		return status.Error(codes.Unavailable, "The Memorystore backend for this project and region failed; reset the scope")
	}
	if err != nil {
		if wire, isStatus := status.FromError(err); isStatus {
			return wire.Err()
		}
		return fmt.Errorf("memorystore backend: %w", err)
	}
	return nil
}
