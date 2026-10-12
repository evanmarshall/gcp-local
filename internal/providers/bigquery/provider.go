package bigquery

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

// Provider owns the BigQuery control plane and row storage.
type Provider struct {
	index  *state.Index
	engine *iam.Engine
	scopes *state.Store[scopeState]
	clock  func() time.Time
}

// New registers the BigQuery provider.
func New(_ context.Context, registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine) (*Provider, error) {
	if registry == nil || manager == nil || index == nil || engine == nil {
		return nil, errProviderShape
	}
	if err := provider.RegisterPermissions(engine, permissions); err != nil {
		return nil, fmt.Errorf("bigquery permissions: %w", err)
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

// registerBackend attaches the in-memory BigQuery backend and its reset hook.
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
		return fmt.Errorf("register the BigQuery backend: %w", err)
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

// Reset clears the scope's datasets, tables, and rows.
func (p *Provider) Reset(scope state.Scope) error {
	if err := p.scopes.With(scope, func(s *scopeState) error {
		s.reset()
		return nil
	}); err != nil {
		return fmt.Errorf("clear the bigquery scope: %w", err)
	}
	return nil
}

// handle is the gateway entry point for every BigQuery method.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid BigQuery request")
	}
	scope, target, err := p.resolve(call, request)
	if err != nil {
		return nil, err
	}
	decision, authorizeErr := p.engine.Authorize(call, target, nil)
	if authorizeErr != nil && status.Code(authorizeErr) == codes.Unimplemented {
		// A deleted target still reads 404 through the project anchor: the
		// project's policies govern resources that no longer exist.
		target = iam.Target{Name: provider.ProjectResource(scope.Project)}
		decision, authorizeErr = p.engine.Authorize(call, target, nil)
	}
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

// resolve derives the scope and IAM target for one request. Dataset methods
// authorize against the dataset resource; table and row methods authorize
// against the parent dataset, matching the real surface's hierarchy.
func (p *Provider) resolve(call gateway.Call, request *gateway.RESTRequest) (state.Scope, iam.Target, error) {
	project := request.Path["project"]
	scope := provider.ProjectScope(project)
	target := iam.Target{Name: provider.ProjectResource(project)}
	dataset, table := request.Path["datasetId"], request.Path["tableId"]
	switch call.Method {
	case methodDatasetsCreate:
		// The collection-level create takes the dataset id from the body.
		return scope, target, nil
	case methodDatasetsList:
		return scope, target, nil
	case methodJobsQuery, methodJobsResults:
		// Query jobs authorize at the project, like the documented rows.
		return scope, target, nil
	}
	if !uid.MatchString(dataset) {
		return scope, target, status.Errorf(codes.InvalidArgument, "Invalid dataset id %q", dataset)
	}
	switch call.Method {
	case methodDatasetsGet, methodDatasetsDelete, methodDatasetsUpdate, methodDatasetsGetIam, methodDatasetsSetIam:
		target.Name = datasetResource(project, dataset)
		return scope, target, nil
	case methodTablesCreate, methodTablesList:
		target.Name = datasetResource(project, dataset)
		return scope, target, nil
	case methodRowsInsert:
		if !uid.MatchString(table) {
			return scope, target, status.Errorf(codes.InvalidArgument, "Invalid table id %q", table)
		}
		target.Name = tableResource(project, dataset, table)
		return scope, target, nil
	default:
		if !uid.MatchString(table) {
			return scope, target, status.Errorf(codes.InvalidArgument, "Invalid table id %q", table)
		}
		target.Name = datasetResource(project, dataset)
		return scope, target, nil
	}
}
