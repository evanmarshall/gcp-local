// Package cloudscheduler models Cloud Scheduler v1 jobs in process. The
// provider owns the jobs, their cron schedules, and one worker per job that
// delivers to HTTP and Pub/Sub targets as the Cloud Scheduler service agent.
package cloudscheduler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
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

// serviceName is the provider, backend, and -services name.
const serviceName = "cloudscheduler"

// Method names; the generated route table uses the discovery method IDs.
const (
	methodCreate = "cloudscheduler.projects.locations.jobs.create"
	methodList   = "cloudscheduler.projects.locations.jobs.list"
	methodGet    = "cloudscheduler.projects.locations.jobs.get"
	methodPatch  = "cloudscheduler.projects.locations.jobs.patch"
	methodDelete = "cloudscheduler.projects.locations.jobs.delete"
	methodPause  = "cloudscheduler.projects.locations.jobs.pause"
	methodResume = "cloudscheduler.projects.locations.jobs.resume"
	methodRun    = "cloudscheduler.projects.locations.jobs.run"
)

// permissions follow the Cloud Scheduler access-control reference; resume is
// cloudscheduler.jobs.enable (roles/cloudscheduler.admin snapshot).
var permissions = map[string]string{
	methodCreate: "cloudscheduler.jobs.create",
	methodList:   "cloudscheduler.jobs.list",
	methodGet:    "cloudscheduler.jobs.get",
	methodPatch:  "cloudscheduler.jobs.update",
	methodDelete: "cloudscheduler.jobs.delete",
	methodPause:  "cloudscheduler.jobs.pause",
	methodResume: "cloudscheduler.jobs.enable",
	methodRun:    "cloudscheduler.jobs.run",
}

// Engine-only plans with no route: the caller's actAs on a job's token
// account, and the service agent's token minting and Pub/Sub publishing.
const (
	methodActAs       = "cloudscheduler.jobs.actAs"
	methodImpersonate = "cloudscheduler.delivery.impersonate"
)

// statePermissions bind the engine-only plans to their permissions.
var statePermissions = map[string]string{
	methodActAs:       "iam.serviceAccounts.actAs",
	methodImpersonate: "iam.serviceAccounts.getAccessToken",
}

// Clock is the time source of schedules, retries, and attempt times. Tests
// substitute a manual clock to advance time deterministically.
type Clock interface {
	Now() time.Time
	// After returns a channel that receives once d has elapsed.
	After(d time.Duration) <-chan time.Time
}

// systemClock is wall time.
type systemClock struct{}

func (systemClock) Now() time.Time                         { return time.Now() }
func (systemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Config carries the delivery dependencies. A nil Clock is wall time; a nil
// token source leaves jobs that need it failing with a coverage gap.
type Config struct {
	IDTokens *identity.IDTokens // OIDC tokens for HTTP targets
	Identity *identity.Service  // OAuth access tokens for HTTP targets
	Clock    Clock
}

// Provider owns Cloud Scheduler jobs and their workers.
type Provider struct {
	index    *state.Index
	engine   *iam.Engine
	registry *gateway.Registry
	scopes   *state.Store[scopeState]
	config   Config
	clock    Clock
	client   *http.Client
	mu       sync.Mutex
	workers  map[string]context.CancelFunc
}

// errProviderShape rejects an incomplete provider construction.
var errProviderShape = errors.New("cloudscheduler requires registry, manager, index, and iam")

// New registers the Cloud Scheduler provider.
func New(_ context.Context, config Config, registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine) (*Provider, error) {
	if registry == nil || manager == nil || index == nil || engine == nil {
		return nil, errProviderShape
	}
	if err := errors.Join(provider.RegisterPermissions(engine, permissions), provider.RegisterPermissions(engine, statePermissions)); err != nil {
		return nil, fmt.Errorf("cloudscheduler permissions: %w", err)
	}
	p := &Provider{
		index: index, engine: engine, registry: registry, config: config, clock: config.Clock,
		scopes: state.NewStore(newScopeState, state.GlobalScope), client: &http.Client{},
		workers: map[string]context.CancelFunc{},
	}
	if p.clock == nil {
		p.clock = systemClock{}
	}
	if err := p.register(registry, manager); err != nil {
		return nil, err
	}
	return p, nil
}

// register binds the reset hook and every method handler.
func (p *Provider) register(registry *gateway.Registry, manager *backend.Manager) error {
	engine, index := p.engine, p.index
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
		return fmt.Errorf("register the cloudscheduler backend: %w", err)
	}
	for method := range permissions {
		if err := registry.Register(method, p.handle); err != nil {
			return fmt.Errorf("register %s: %w", method, err)
		}
	}
	return nil
}

// emptyBackend satisfies the backend contract for an in-memory provider.
type emptyBackend struct{}

func (emptyBackend) Start(context.Context) error { return nil }
func (emptyBackend) Ready(context.Context) error { return nil }
func (emptyBackend) Stop(context.Context) error  { return nil }
func (emptyBackend) Reset(context.Context) error { return nil }

// Reset stops every worker of the scope, then clears its jobs.
func (p *Provider) Reset(scope state.Scope) error {
	p.mu.Lock()
	for key, cancel := range p.workers {
		if strings.HasPrefix(key, scope.Project+"\x00") {
			cancel()
			delete(p.workers, key)
		}
	}
	p.mu.Unlock()
	if err := p.scopes.With(scope, func(s *scopeState) error {
		s.jobs = map[string]*jobRecord{}
		return nil
	}); err != nil {
		return fmt.Errorf("clear the cloudscheduler scope: %w", err)
	}
	return nil
}

// workerKey addresses one job's worker.
func workerKey(scope state.Scope, key string) string { return scope.Project + "\x00" + key }

// stopWorker cancels one job's worker.
func (p *Provider) stopWorker(scope state.Scope, key string) {
	p.mu.Lock()
	if cancel := p.workers[workerKey(scope, key)]; cancel != nil {
		cancel()
		delete(p.workers, workerKey(scope, key))
	}
	p.mu.Unlock()
}

// scopeState is one project's jobs, keyed by location and job ID.
type scopeState struct {
	jobs map[string]*jobRecord
}

func newScopeState() scopeState { return scopeState{jobs: map[string]*jobRecord{}} }

// jobKey addresses one job; job IDs are unique within a location.
func jobKey(location, id string) string { return location + "/" + id }

// handle is the gateway entry point for every Cloud Scheduler method.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Cloud Scheduler request")
	}
	decision, target, err := p.authorize(call, request)
	if err != nil {
		code := status.Code(err)
		if code != codes.PermissionDenied && code != codes.Unauthenticated {
			return nil, fmt.Errorf("authorize %s: %w", call.Method, err)
		}
		if call.Principal.State != gateway.Authenticated {
			return provider.WireError(401, "UNAUTHENTICATED", "Request had invalid authentication credentials."), nil
		}
		return provider.Denied(decision, target.Name), nil
	}
	call.Resource = target.Name
	response, err := p.execute(call, request)
	if err != nil {
		return nil, err
	}
	response.Headers = http.Header{}
	response.Headers.Set("X-Gcp-Local-Decision-Id", decision.ID)
	return response, nil
}

// authorize checks the method on its job, whose ancestry is the project; a
// job that is not indexed authorizes on the project and then reads 404.
func (p *Provider) authorize(call gateway.Call, request *gateway.RESTRequest) (iam.Decision, iam.Target, error) {
	project := iam.Target{Name: provider.ProjectResource(request.Path["project"])}
	target := project
	if job := request.Path["job"]; job != "" {
		target = iam.Target{Name: jobResource(request.Path["project"], request.Path["location"], job)}
	}
	decision, err := p.engine.Authorize(call, target, nil)
	if target != project && status.Code(err) == codes.Unimplemented {
		target = project
		decision, err = p.engine.Authorize(call, target, nil)
	}
	return decision, target, err //nolint:wrapcheck // callers read the IAM status code
}

// authorizeActAs requires iam.serviceAccounts.actAs on a token account before
// a job that mints its token is stored, as GCP does at create and update.
func (p *Provider) authorizeActAs(call gateway.Call, project, email string) error {
	account := iam.AccountResource(project, email)
	call.Method, call.Resource = methodActAs, account
	_, err := p.engine.Authorize(call, iam.Target{Name: account}, nil)
	switch status.Code(err) {
	case codes.OK:
		return nil
	case codes.PermissionDenied, codes.Unauthenticated:
		return status.Error(codes.PermissionDenied, "The caller lacks iam.serviceAccounts.actAs on "+email)
	case codes.Unimplemented:
		return gap("token service accounts must be declared same-project local service accounts")
	default:
		return fmt.Errorf("authorize the token service account: %w", err)
	}
}

// gap reports an unimplemented slice boundary.
func gap(message string) error {
	return status.Error(codes.Unimplemented, "Cloud Scheduler coverage gap: "+message)
}
