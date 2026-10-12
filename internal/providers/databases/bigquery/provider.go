package bigquery

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Provider owns datasets, table metadata, and jobs; each project's engine
// owns its tables' columns and rows.
type Provider struct {
	manager *backend.Manager
	index   *state.Index
	engine  *iam.Engine
	scopes  *state.Store[scopeState]
	clock   func() time.Time
}

// New registers the BigQuery provider.
func New(_ context.Context, registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine) (*Provider, error) {
	if registry == nil || manager == nil || index == nil || engine == nil {
		return nil, errProviderShape
	}
	if err := provider.RegisterPermissions(engine, permissions); err != nil {
		return nil, fmt.Errorf("bigquery permissions: %w", err)
	}
	if err := provider.RegisterPermissions(engine, relatedPermissions); err != nil {
		return nil, fmt.Errorf("bigquery related permissions: %w", err)
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

// registerBackend attaches one GoogleSQL engine per project. Reset stops the
// engine, discarding its tables, before clearing datasets, jobs, policies,
// and index entries for that project.
func (p *Provider) registerBackend(manager *backend.Manager, engine *iam.Engine, index *state.Index) error {
	if err := manager.Register(serviceName, backend.Definition{
		Validate: state.GlobalScope,
		New: func(scope state.Scope) (backend.Backend, error) {
			return &engineBackend{project: scope.Project}, nil
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
		return fmt.Errorf("register the BigQuery backend: %w", err)
	}
	return nil
}

// Reset clears the scope's datasets, table metadata, and jobs.
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
	scope, target, err := resolveTarget(call, request)
	if err != nil {
		return nil, err
	}
	authorized := call
	authorized.Method = p.ownJobCheck(call, request, scope)
	decision, resource, authorizeErr := p.authorizeAt(authorized, target.Name, scope.Project)
	if authorizeErr != nil {
		if provider.Unverified(authorizeErr) {
			return nil, fmt.Errorf("authorize %s: %w", call.Method, authorizeErr)
		}
		return provider.Denied(decision, resource), nil
	}
	call.Resource = resource
	call.Location = scope.Location
	response, err := p.execute(call, request, scope)
	if err != nil {
		return nil, err
	}
	stampDecision(response, decision.ID)
	return response, nil
}

// authorizeAt authorizes one call against a resource, or, when the index
// lacks it, against its nearest indexed ancestor (table, then dataset, then
// project), so absence reads 404 through the policies that govern it. The
// target is chosen and authorized under the scope lock, which every index
// change for this provider's datasets and tables also holds, so a concurrent
// create or delete cannot unindex the target mid-check. An indexed resource
// never falls back: an engine refusal such as an unsupported policy stays
// closed, and so does a scope the store refuses.
func (p *Provider) authorizeAt(call gateway.Call, resource, project string) (iam.Decision, string, error) {
	var decision iam.Decision
	var authorizeErr error
	if err := p.scopes.With(provider.ProjectScope(project), func(*scopeState) error {
		for {
			parent := parentResource(resource, project)
			if _, indexed := p.index.Get(resource); indexed || parent == resource {
				break
			}
			resource = parent
		}
		decision, authorizeErr = p.engine.Authorize(call, iam.Target{Name: resource}, nil)
		return nil
	}); err != nil {
		return decision, resource, fmt.Errorf("authorize in project %q: %w", project, err)
	}
	return decision, resource, authorizeErr //nolint:wrapcheck // callers wrap or render the refusal
}

// parentResource names a table's dataset, or a dataset's project.
func parentResource(resource, project string) string {
	if dataset, _, isTable := strings.Cut(resource, "/tables/"); isTable {
		return dataset
	}
	return provider.ProjectResource(project)
}

// stampDecision names the call's IAM decision on its response, unless a
// related check that denied the call already named its own.
func stampDecision(response *gateway.RESTResponse, id string) {
	if response.Headers == nil {
		response.Headers = http.Header{}
	}
	if response.Headers.Get(headerDecision) == "" {
		response.Headers.Set(headerDecision, id)
	}
}

// headerDecision names the IAM decision that answered the request.
const headerDecision = "X-Gcp-Local-Decision-Id"

// resolveTarget derives the scope and IAM target for one request. Dataset methods
// authorize against the dataset resource. Table, row, and table IAM methods
// authorize against the table, whose policy inherits the dataset's; a table
// that does not exist authorizes against its dataset (authorizeAt), so only
// callers with dataset access learn it is absent.
func resolveTarget(call gateway.Call, request *gateway.RESTRequest) (state.Scope, iam.Target, error) {
	project := request.Path["project"]
	scope := provider.ProjectScope(project)
	target := iam.Target{Name: provider.ProjectResource(project)}
	dataset, table := request.Path["datasetId"], request.Path["tableId"]
	switch call.Method {
	case methodDatasetsCreate, methodDatasetsList, methodJobsQuery, methodJobsResults, methodJobsInsert, methodJobsGet:
		// The collection-level create takes the dataset id from the body, and
		// query jobs authorize at the project, like the documented rows.
		return scope, target, nil
	}
	if !uid.MatchString(dataset) {
		return scope, target, status.Errorf(codes.InvalidArgument, "Invalid dataset id %q", dataset)
	}
	switch call.Method {
	case methodDatasetsGet, methodDatasetsDelete, methodDatasetsUpdate, methodDatasetsGetIam, methodDatasetsSetIam,
		methodTablesCreate, methodTablesList:
		target.Name = datasetResource(project, dataset)
		return scope, target, nil
	default:
		if !uid.MatchString(table) {
			return scope, target, status.Errorf(codes.InvalidArgument, "Invalid table id %q", table)
		}
		target.Name = tableResource(project, dataset, table)
		return scope, target, nil
	}
}
