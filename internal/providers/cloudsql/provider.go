package cloudsql

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/dataplane"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Provider is the authorization gate in front of the PostgreSQL fleet. It
// owns instance metadata, operations, and index references; the servers own
// databases, roles, and data.
type Provider struct {
	manager  *backend.Manager
	index    *state.Index
	engine   *iam.Engine
	catalog  *state.Store[catalog]
	releases map[string]string
	pool     *dataplane.Pool
	publicMu sync.Mutex
	public   map[string]*dataplane.Listener
	clock    func() time.Time
}

// handler serves one authorized method for one project.
type handler func(call gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error)

// New registers the Cloud SQL provider backed by the pinned PostgreSQL binaries.
func New(_ context.Context, registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine, config Config) (*Provider, error) {
	versions, err := config.validate()
	if err != nil {
		return nil, err
	}
	releases := map[string]string{}
	for version, dist := range versions {
		releases[version] = dist.release
	}
	provider, err := newProvider(registry, manager, index, engine, releases, func(state.Scope) (backend.Backend, error) {
		return newFleet(versions, config.RunAs), nil
	})
	if err != nil {
		return nil, err
	}
	provider.pool = config.Publish
	return provider, nil
}

// newProvider wires the provider with an arbitrary backend factory (tests use a fake fleet).
func newProvider(registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine, releases map[string]string, factory func(state.Scope) (backend.Backend, error)) (*Provider, error) {
	if registry == nil || manager == nil || index == nil || engine == nil {
		return nil, errProviderShape
	}
	if err := registerPlans(engine); err != nil {
		return nil, err
	}
	p := &Provider{manager: manager, index: index, engine: engine, releases: releases, clock: time.Now,
		catalog: state.NewStore(newCatalog, state.GlobalScope), public: map[string]*dataplane.Listener{}}
	if err := manager.Register(serviceName, backend.Definition{
		Validate: state.GlobalScope,
		New:      factory,
		// The manager stops every server of the project (discarding its data
		// directories) before this hook closes its published ports and clears
		// metadata and references.
		AfterReset: func(scope state.Scope) error {
			p.closeProjectPublic(scope.Project)
			if err := p.catalog.Reset(scope); err != nil {
				return fmt.Errorf("clear the Cloud SQL scope: %w", err)
			}
			index.RemoveScope(serviceName, scope)
			return nil
		},
	}); err != nil {
		return nil, fmt.Errorf("register the Cloud SQL backend: %w", err)
	}
	return p, p.registerRoutes(registry)
}

// registerPlans registers every permission plan after confirming on a
// scratch engine that the pinned roles grant them all.
func registerPlans(engine *iam.Engine) error {
	if err := rolesRecorded(); err != nil {
		return err
	}
	for method, permission := range permissions {
		if err := engine.Register(method, iam.Rule{Baseline: []string{permission}}); err != nil {
			return fmt.Errorf("register %s: %w", method, err)
		}
	}
	return nil
}

// registerRoutes binds every served method to the provider.
func (p *Provider) registerRoutes(registry *gateway.Registry) error {
	for method := range p.handlers() {
		if err := registry.Register(method, p.handle); err != nil {
			return fmt.Errorf("register %s: %w", method, err)
		}
	}
	return nil
}

// rolesRecorded checks on a scratch engine that the pinned predefined roles
// grant every planned permission, so a missing snapshot refuses registration
// before any plan reaches the shared engine.
func rolesRecorded() error {
	scratch, err := iam.New(state.NewIndex())
	if err != nil {
		return fmt.Errorf("load the pinned roles: %w", err)
	}
	for method, permission := range permissions {
		if scratch.Register(method, iam.Rule{Baseline: []string{permission}}) != nil {
			return fmt.Errorf("%w: no pinned role grants %s", ErrRolesUnrecorded, permission)
		}
	}
	return nil
}

// handlers maps each served method to its implementation.
func (p *Provider) handlers() map[string]handler {
	return map[string]handler{
		methodInstancesInsert: p.insertInstance, methodInstancesGet: p.getInstance, methodInstancesList: p.listInstances,
		methodInstancesPatch: p.patchInstance, methodInstancesDelete: p.deleteInstance,
		methodDatabasesInsert: p.insertDatabase, methodDatabasesGet: p.getDatabase, methodDatabasesList: p.listDatabases,
		methodDatabasesDelete: p.deleteDatabase,
		methodUsersInsert:     p.insertUser, methodUsersList: p.listUsers, methodUsersDelete: p.deleteUser,
		methodOperationsGet: p.getOperation, methodOperationsList: p.listOperations,
	}
}

// handle is the gateway entry point. Authorization completes before any
// backend is started or called.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Cloud SQL request")
	}
	project := request.Path[pathProject]
	if provider.ProjectScope(project).Validate() != nil {
		return nil, provider.Invalid("Invalid project.")
	}
	target := p.target(project, p.addressed(call.Method, request, project))
	decision, authorizeErr := p.engine.Authorize(call, target, nil)
	if provider.Unverified(authorizeErr) {
		// A concurrent delete can unindex the instance after target chose it.
		target = p.target(project, p.addressed(call.Method, request, project))
		decision, authorizeErr = p.engine.Authorize(call, target, nil)
	}
	if authorizeErr != nil {
		return refusal(call.Method, authorizeErr)
	}
	call.Resource = target.Name
	response, err := p.handlers()[call.Method](call, request, project)
	if err != nil {
		return nil, err
	}
	if response.Headers == nil {
		response.Headers = http.Header{}
	}
	response.Headers.Set("X-Gcp-Local-Decision-Id", decision.ID)
	return response, nil
}

// refusal renders a denied decision in the Cloud SQL shape; coverage gaps
// and other failures stay errors.
func refusal(method string, err error) (any, error) {
	switch status.Code(err) {
	case codes.PermissionDenied:
		return apiError(http.StatusForbidden, "PERMISSION_DENIED", "notAuthorized", "The client is not authorized to make this request."), nil
	case codes.Unauthenticated:
		return apiError(http.StatusUnauthorized, "UNAUTHENTICATED", "required", "Login Required."), nil
	default:
		return nil, fmt.Errorf("authorize %s: %w", method, err)
	}
}

// addressed names the instance a request acts on, or "" for project-level calls.
func (p *Provider) addressed(method string, request *gateway.RESTRequest, project string) string {
	switch method {
	case methodInstancesInsert, methodInstancesList:
		return ""
	case methodOperationsList:
		return request.Query.Get("instance")
	case methodOperationsGet:
		var instance string
		_ = p.catalog.With(provider.ProjectScope(project), func(c *catalog) error {
			for _, operation := range c.operations {
				if operation.Name == request.Path[pathOperation] {
					instance = operation.Instance
				}
			}
			return nil
		})
		return instance
	default:
		return request.Path[pathInstance]
	}
}

// target selects the IAM resource: the instance when indexed, else its
// project, so a missing instance is authorized at the project and then
// reported as missing. Cloud SQL has no instance-level IAM policies; the
// instance node only carries its project ancestry and condition attributes.
func (p *Provider) target(project, instance string) iam.Target {
	if instance != "" {
		name := instanceResource(project, instance)
		if resource, ok := p.index.Get(name); ok && resource.Service == serviceName && resource.Scope == provider.ProjectScope(project) {
			return iam.Target{Name: name, Attributes: iam.ResourceAttributes{
				Name: "projects/" + project + "/instances/" + instance, Type: "sqladmin.googleapis.com/Instance", Service: "sqladmin.googleapis.com"}}
		}
	}
	return iam.Target{Name: provider.ProjectResource(project)}
}

// use runs one backend operation for a project after authorization.
func (p *Provider) use(ctx context.Context, project string, operation func(context.Context, driver) error) error {
	err := p.manager.Use(ctx, backend.Key{Service: serviceName, Scope: provider.ProjectScope(project)}, func(ctx context.Context, active backend.Backend) error {
		fleet, err := driverOf(active)
		if err != nil {
			return err
		}
		return operation(ctx, fleet)
	})
	if err != nil {
		return fmt.Errorf("cloud SQL backend: %w", err)
	}
	return nil
}

// lookup returns a copy of one instance's metadata.
func (p *Provider) lookup(project, instance string) (instanceRecord, bool) {
	var record instanceRecord
	var found bool
	_ = p.catalog.With(provider.ProjectScope(project), func(c *catalog) error {
		var stored instanceRecord
		stored, found = c.instances[instance]
		record = stored.clone()
		return nil
	})
	return record, found
}

// complete records a finished operation and returns its wire form.
func (p *Provider) complete(call gateway.Call, project, kind, instance string, started time.Time) (*gateway.RESTResponse, error) {
	operation := newOperation(kind, instance, subject(call.Principal), started, p.clock())
	if err := p.catalog.With(provider.ProjectScope(project), func(c *catalog) error { c.record(operation); return nil }); err != nil {
		return nil, fmt.Errorf("record the operation: %w", err)
	}
	return &gateway.RESTResponse{JSON: operation.wire(project)}, nil
}

// subject renders the operation user as GCP does: the caller's email.
func subject(principal gateway.Principal) string {
	if principal.State != gateway.Authenticated {
		return ""
	}
	for _, prefix := range []string{"serviceAccount:", "user:"} {
		if len(principal.Subject) > len(prefix) && principal.Subject[:len(prefix)] == prefix {
			return principal.Subject[len(prefix):]
		}
	}
	return principal.Subject
}

// backendFailure maps PostgreSQL errors onto API errors; other failures stay internal.
func backendFailure(err error) (*gateway.RESTResponse, error) {
	var pg *pgError
	if !errors.As(err, &pg) {
		return nil, err
	}
	switch pg.Code {
	case "42P04", "42710": // duplicate_database, duplicate_object
		return apiError(http.StatusConflict, "ALREADY_EXISTS", "", pg.Message), nil
	case "3D000", "42704": // invalid_catalog_name, undefined_object
		return apiError(http.StatusNotFound, "NOT_FOUND", "", pg.Message), nil
	default:
		return apiError(http.StatusBadRequest, "FAILED_PRECONDITION", "", pg.Message), nil
	}
}
