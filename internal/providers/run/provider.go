package run

import (
	"context"
	"errors"
	"fmt"
	"github.com/evanmarshall/gcp-local/internal/providers/run/container"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Provider owns the Cloud Run v2 control plane and its container lifecycle.
// Service records live in the scope store and are read and changed only
// inside its callbacks; container launches and stops run outside it.
type Provider struct {
	index    *state.Index
	metadata func(subject string) (http.Handler, error)
	metabind func(http.Handler) (string, func(), error)
	reapers  map[string]context.CancelFunc
	reaperMu sync.Mutex
	tokens   *identity.IDTokens
	baseURL  string
	hosts    map[string]hostEntry
	hostMu   sync.RWMutex
	engine   *iam.Engine
	gate     provider.Gate
	runtime  container.Runtime
	scopes   *state.Store[scopeState]
	// operations holds every rollout and delete operation, one name each.
	operations *provider.Operations
	clock      func() time.Time
}

// operationLatency is the least time from an operation's start to done, so
// a delete's operation reads done=false first and a rollout is still running
// a second in, as GCP's are (recording run-service-lifecycle).
// Tests shorten it (TestMain); nothing else changes it.
var operationLatency = 2 * time.Second

// errProviderShape rejects an incomplete provider construction.
var errProviderShape = errors.New("run requires registry, manager, index, iam, and runtime")

// New registers the Cloud Run v2 provider. Operation completion happens from
// container readiness, evaluated by the per-service worker in methods.go. Reset
// cleanup runs under an uncancellable child of the provider context so it can
// outlive a shutdown that cancels the serving context.
func New(ctx context.Context, config Config, registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine, runtime container.Runtime) (*Provider, error) {
	if registry == nil || manager == nil || index == nil || engine == nil || runtime == nil {
		return nil, errProviderShape
	}
	if err := provider.RegisterPermissions(engine, permissions); err != nil {
		return nil, fmt.Errorf("run permissions: %w", err)
	}
	p := &Provider{
		index: index, engine: engine, runtime: runtime, tokens: config.Tokens, baseURL: config.BaseURL,
		metadata: config.Metadata, metabind: config.MetadataBind,
		// Cloud Run's denials carry no details, and its service policies'
		// conditions see no resource attributes (recording run-service-lifecycle).
		gate:       provider.Gate{Engine: engine, Index: index, Message: provider.PermissionDeniedResourceMayNotExist},
		scopes:     state.NewStore(newScopeState, state.Scope.Validate),
		operations: provider.NewOperations(state.Scope.Validate, operationLatency, operationMetadata).NewestFirst(),
		hosts:      map[string]hostEntry{}, reapers: map[string]context.CancelFunc{},
		clock: time.Now,
	}
	if err := manager.Register(serviceName, backend.Definition{
		Validate: state.Scope.Validate,
		// The control plane is in memory and container work is provider-owned.
		New: provider.NoBackend,
		AfterReset: func(scope state.Scope) error {
			// The manager's reset hooks carry no request context by design, so the
			// cleanup runs under an uncancellable child of the provider context.
			if resetErr := p.Reset(context.WithoutCancel(ctx), scope); resetErr != nil {
				return resetErr
			}
			engine.ClearScope(serviceName, scope)
			index.RemoveScope(serviceName, scope)
			return nil
		},
	}); err != nil {
		return nil, fmt.Errorf("register the run backend: %w", err)
	}
	if err := registerMethods(registry, p); err != nil {
		return nil, err
	}
	return p, nil
}

// registerMethods binds every provider method to the shared REST handler. The
// gateway transcodes the gRPC methods onto these routes.
func registerMethods(registry *gateway.Registry, p *Provider) error {
	for _, method := range methodNames() {
		if err := registry.Register(method, p.handle); err != nil {
			return fmt.Errorf("register %s: %w", method, err)
		}
	}
	return nil
}

// reaperKey addresses one service's idle reaper. Service IDs repeat across
// projects and locations, so the key carries both.
func reaperKey(scope state.Scope, id string) string {
	return scope.Project + "\x00" + scope.Location + "\x00" + id
}

// stopReapers cancels the idle reapers of one project and location. Reset
// calls this before clearing state so no reaper outlives the scope it watches.
func (p *Provider) stopReapers(scope state.Scope) {
	p.reaperMu.Lock()
	for key, cancel := range p.reapers {
		if strings.HasPrefix(key, reaperKey(scope, "")) {
			cancel()
			delete(p.reapers, key)
		}
	}
	p.reaperMu.Unlock()
}

// stopReaper cancels one service's idle reaper.
func (p *Provider) stopReaper(scope state.Scope, id string) {
	p.reaperMu.Lock()
	if cancel := p.reapers[reaperKey(scope, id)]; cancel != nil {
		cancel()
		delete(p.reapers, reaperKey(scope, id))
	}
	p.reaperMu.Unlock()
}

// Reset stops every container in the scope, then clears the provider state
// and its operations.
func (p *Provider) Reset(ctx context.Context, scope state.Scope) error {
	p.stopReapers(scope)
	var ids []string
	if err := p.scopes.With(scope, func(s *scopeState) error {
		for id, service := range s.services {
			ids = append(ids, id)
			for _, revision := range service.Revisions {
				_ = p.runtime.Stop(context.WithoutCancel(ctx), revision.Container)
				releaseMetadata(revision)
			}
		}
		s.reset()
		return nil
	}); err != nil {
		return fmt.Errorf("clear the run scope: %w", err)
	}
	for _, id := range ids {
		p.forgetHost(scope, id)
	}
	if err := p.operations.Reset(scope); err != nil {
		return fmt.Errorf("clear the run operations: %w", err)
	}
	return nil
}

// errNoService reports a service that does not exist (or no longer does), in
// GCP's wording.
func errNoService(scope state.Scope, id string) error {
	return status.Errorf(codes.NotFound, "Resource '%s' of kind 'SERVICE' in region '%s' in project '%s' does not exist.", id, scope.Location, scope.Project)
}

// withService runs use on an existing service under its scope lock. The
// callback may read and change the record and must render anything it
// returns: the record is never used outside the lock.
func (p *Provider) withService(scope state.Scope, id string, use func(*scopeState, *serviceRecord) error) error {
	return p.scopes.With(scope, func(s *scopeState) error { //nolint:wrapcheck // status errors carry the wire status
		record := s.services[id]
		if record == nil {
			return errNoService(scope, id)
		}
		return use(s, record)
	})
}

// gap refuses a request shape the provider does not model. Clients do not
// retry UNIMPLEMENTED.
var gap = provider.CoverageGap("Cloud Run")

// errUnimplemented is the default dispatch gap for unregistered methods.
var errUnimplemented = gap("this method is not implemented")

// coverageGap refuses one documented request feature the provider does not
// model.
func coverageGap(feature string) error { return gap(feature + " is not implemented") }

// refusedOptions are the Admin API v2 query options (services.create,
// patch, delete; revisions.delete; services and revisions list) the provider
// does not model. A request that sets one to a non-default value is refused
// rather than silently answered as if it were unset: validateOnly would
// mutate, allowMissing would not create, showDeleted would hide deleted
// resources, and etag would not be checked.
var refusedOptions = []string{"validateOnly", "allowMissing", "showDeleted", "updateMask", "etag"}

// refuseOptions returns the gap for the first non-default refused option.
func refuseOptions(query url.Values) error {
	for _, name := range refusedOptions {
		if value := query.Get(name); value != "" && value != "false" && value != "0" {
			return coverageGap("the " + name + " option")
		}
	}
	return nil
}

// handle is the gateway entry point for every Cloud Run v2 method.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Cloud Run request")
	}
	scope, id, reported, err := resolveCall(call, request)
	if err != nil {
		return nil, err
	}
	candidates := []string{provider.ProjectResource(scope.Project)}
	if id != "" {
		candidates = []string{serviceResource(scope.Project, scope.Location, id), candidates[0]}
	}
	// The service and its index entry change together under the scope lock
	// Nearest holds, so a request racing a delete authorizes at the project
	// and reads 404.
	decision, target, err := provider.Nearest(p.gate, p.scopes, scope, call, reported, candidates...)
	if err != nil {
		return nil, err
	}
	if err := p.preflight(call, request, scope); err != nil {
		return nil, err
	}
	call.Resource = target.Name
	call.Location = scope.Location
	response, err := p.execute(call, request, scope, id)
	if err != nil {
		return nil, err
	}
	return provider.WithDecision(response, decision), nil
}

// preflight refuses unmodeled request options and, for a deploy, a caller
// that cannot act as the revision's service account.
func (p *Provider) preflight(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) error {
	if refused := refuseOptions(request.Query); refused != nil {
		return refused
	}
	if call.Method == methodServicesCreate || call.Method == methodServicesUpdate {
		return p.authorizeRuntimeAccount(call, request, scope)
	}
	return nil
}

// resolveCall derives the scope, the addressed service ID ("" for
// collection and operation methods), and the resource name a denial reports
// (recording run-service-lifecycle): the service, or for create the service
// being created, the collection for a list, or the operation.
func resolveCall(call gateway.Call, request *gateway.RESTRequest) (state.Scope, string, string, error) {
	parts := request.Path
	scope := state.Scope{Project: parts["project"], Location: parts["location"]}
	switch call.Method {
	case methodServicesCreate:
		return scope, "", wireName(scope, "services", request.Query.Get("serviceId")), nil
	case methodServicesList:
		return scope, "", wireName(scope, "services"), nil
	case methodOperationsList:
		return scope, "", wireName(scope, "operations"), nil
	case methodOperationsGet, methodOperationsDelete, methodOperationsWait:
		return scope, "", wireName(scope, "operations", parts["operation"]), nil
	}
	// Revisions are not indexed IAM resources; they authorize on their
	// service, which inherits the project's policy.
	id := parts["service"]
	if !uidPattern.MatchString(id) || scope.Location == "" {
		return state.Scope{}, "", "", status.Error(codes.InvalidArgument, "Invalid Cloud Run resource name")
	}
	reported := wireName(scope, "services", id)
	if revision := parts["revision"]; revision != "" {
		reported += "/revisions/" + revision
	}
	return scope, id, reported, nil
}

// locationTarget names the parent collection for project-scoped methods.
func locationTarget(scope state.Scope) string {
	return fmt.Sprintf("//run.googleapis.com/projects/%s/locations/%s", scope.Project, scope.Location)
}

// methodServicesActAs is the engine-only plan for the deployer's act on a
// revision's runtime service account; it has no route. Cloud Tasks OIDC and
// Pub/Sub push check the same permission the same way.
const methodServicesActAs = "run.services.actAs"

// authorizeRuntimeAccount requires iam.serviceAccounts.actAs on the service
// account a create or update deploys as, before anything is stored or
// launched. Every rollout is a deploy, so the check runs on each one. A
// template without an account deploys as the project's default compute
// account, NUMBER-compute@developer.gserviceaccount.com
// (https://cloud.google.com/run/docs/configuring/services/service-identity).
// GCP creates that account with the project; an environment that declares it
// checks its own policy, and otherwise it has none, so the project's decides.
func (p *Provider) authorizeRuntimeAccount(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) error {
	service, err := decodeService(request.JSON)
	if err != nil {
		return err
	}
	template, err := resolveTemplate(service.GetTemplate())
	if err != nil {
		return err
	}
	email, target, declared := p.runtimeAccount(template.ServiceAccount, scope)
	if !declared {
		return actAsDenied(email)
	}
	call.Method, call.Resource = methodServicesActAs, target
	_, err = p.engine.Authorize(call, iam.Target{Name: target}, nil)
	switch status.Code(err) {
	case codes.OK:
		return nil
	case codes.PermissionDenied, codes.Unauthenticated:
		return actAsDenied(email)
	case codes.Unimplemented:
		return coverageGap("a runtime service account outside the service's project")
	default:
		return fmt.Errorf("authorize the runtime service account: %w", err)
	}
}

// runtimeAccount names the account a template deploys as and the resource
// whose policy decides actAs on it, and reports whether that account exists
// locally. The default compute account always exists.
func (p *Provider) runtimeAccount(email string, scope state.Scope) (string, string, bool) {
	project := provider.ProjectResource(scope.Project)
	if email != "" {
		account := iam.AccountResource(accountProject(email), email)
		return email, account, p.indexed(account)
	}
	resource, ok := p.index.Get(project)
	if !ok || resource.Number == "" {
		return "the default compute service account", project, true
	}
	email = resource.Number + "-compute@developer.gserviceaccount.com"
	if account := iam.AccountResource(scope.Project, email); p.indexed(account) {
		return email, account, true
	}
	return email, project, true
}

// indexed reports whether a resource is in the index.
func (p *Provider) indexed(name string) bool {
	_, ok := p.index.Get(name)
	return ok
}

// actAsDenied is the refusal Cloud Run returns for a deployer without actAs.
// GCP words a missing account the same way.
func actAsDenied(email string) error {
	return status.Errorf(codes.PermissionDenied,
		"Permission 'iam.serviceaccounts.actAs' denied on service account %s (or it may not exist).", email)
}

// accountProject is the project that owns a user-managed service account,
// named in its email domain.
func accountProject(email string) string {
	_, domain, _ := strings.Cut(email, "@")
	return strings.TrimSuffix(domain, ".iam.gserviceaccount.com")
}
