package cloudtasks

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Method name constants; the generated route table uses the same identifiers.
const (
	methodQueuesCreate  = "cloudtasks.queues.create"
	methodQueuesGet     = "cloudtasks.queues.get"
	methodQueuesList    = "cloudtasks.queues.list"
	methodQueuesDelete  = "cloudtasks.queues.delete"
	methodQueuesPause   = "cloudtasks.queues.pause"
	methodQueuesResume  = "cloudtasks.queues.resume"
	methodQueuesPurge   = "cloudtasks.queues.purge"
	methodTasksCreate   = "cloudtasks.tasks.create"
	methodTasksGet      = "cloudtasks.tasks.get"
	methodTasksList     = "cloudtasks.tasks.list"
	methodTasksDelete   = "cloudtasks.tasks.delete"
	methodTasksRun      = "cloudtasks.tasks.run"
	methodLocationsList = "cloudtasks.locations.list"
)

// permissions follow the documented Cloud Tasks access-control rows.
var permissions = map[string]string{
	methodQueuesCreate:  "cloudtasks.queues.create",
	methodQueuesGet:     "cloudtasks.queues.get",
	methodQueuesList:    "cloudtasks.queues.list",
	methodQueuesDelete:  "cloudtasks.queues.delete",
	methodQueuesPause:   "cloudtasks.queues.pause",
	methodQueuesResume:  "cloudtasks.queues.resume",
	methodQueuesPurge:   "cloudtasks.queues.purge",
	methodTasksCreate:   "cloudtasks.tasks.create",
	methodTasksGet:      "cloudtasks.tasks.get",
	methodTasksList:     "cloudtasks.tasks.list",
	methodTasksDelete:   "cloudtasks.tasks.delete",
	methodTasksRun:      "cloudtasks.tasks.run",
	methodLocationsList: "cloudtasks.locations.list",
}

// DeliveryConfig carries the dispatch dependencies of the provider.
type DeliveryConfig struct {
	Tokens *identity.IDTokens
}

// Provider owns the Cloud Tasks queues, tasks, and dispatch workers.
type Provider struct {
	index   *state.Index
	engine  *iam.Engine
	scopes  *state.Store[scopeState]
	tokens  *identity.IDTokens
	clock   func() time.Time
	client  *http.Client
	mu      sync.Mutex
	workers map[string]context.CancelFunc
}

// errProviderShape rejects an incomplete provider construction.
var errProviderShape = errors.New("cloudtasks requires registry, manager, index, and iam")

// New registers the Cloud Tasks provider.
func New(_ context.Context, config DeliveryConfig, registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine) (*Provider, error) {
	if registry == nil || manager == nil || index == nil || engine == nil {
		return nil, errProviderShape
	}
	if err := registerPermissions(engine); err != nil {
		return nil, err
	}
	p := &Provider{
		index: index, engine: engine, tokens: config.Tokens,
		scopes:  state.NewStore(newScopeState, state.GlobalScope),
		client:  &http.Client{Timeout: 10 * time.Second},
		clock:   time.Now,
		workers: map[string]context.CancelFunc{},
	}
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
		return nil, fmt.Errorf("register the cloudtasks backend: %w", err)
	}
	for method := range permissions {
		if err := registry.Register(method, p.handle); err != nil {
			return nil, fmt.Errorf("register %s: %w", method, err)
		}
	}
	return p, nil
}

// emptyBackend satisfies the backend contract for an in-memory provider.
type emptyBackend struct{}

func (emptyBackend) Start(context.Context) error    { return nil }
func (emptyBackend) Ready(context.Context) error    { return nil }
func (emptyBackend) Stop(context.Context) error     { return nil }
func (emptyBackend) Reset(context.Context) error    { return nil }
func (emptyBackend) Snapshot(context.Context) error { return backend.ErrPersistenceUnsupported }
func (emptyBackend) Restore(context.Context) error  { return backend.ErrPersistenceUnsupported }

// Reset stops every queue worker, then clears the scope's state.
func (p *Provider) Reset(scope state.Scope) error {
	p.stopWorkers(scope)
	if err := p.scopes.With(scope, func(s *scopeState) error {
		s.reset()
		return nil
	}); err != nil {
		return fmt.Errorf("clear the cloudtasks scope: %w", err)
	}
	return nil
}

// stopWorkers cancels the dispatch workers of one scope.
func (p *Provider) stopWorkers(scope state.Scope) {
	p.mu.Lock()
	for key, cancel := range p.workers {
		if key == scope.Project {
			cancel()
			delete(p.workers, key)
		}
	}
	p.mu.Unlock()
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

// recordState is one stored queue or task.
type queueRecord struct {
	Name    string
	Rate    map[string]any
	State   string // RUNNING, PAUSED
	Created time.Time
}

// taskRecord is one stored task.
type taskRecord struct {
	Name           string
	Queue          string
	URL            string
	Method         string
	Body           []byte
	Headers        map[string]string
	ServiceAccount string
	Audience       string
	Attempts       int
	State          string // PENDING, SUCCEEDED, FAILED
	Detail         string
	Created        time.Time
}

// scopeState is one project slice of provider state.
type scopeState struct {
	queues map[string]*queueRecord
	tasks  map[string]*taskRecord
}

func newScopeState() scopeState {
	return scopeState{queues: map[string]*queueRecord{}, tasks: map[string]*taskRecord{}}
}

func (s *scopeState) reset() {
	s.queues = map[string]*queueRecord{}
	s.tasks = map[string]*taskRecord{}
}

// handle is the gateway entry point for every Cloud Tasks method.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Cloud Tasks request")
	}
	scope, target := p.resolve(call, request)
	decision, authorizeErr := p.engine.Authorize(call, target, nil)
	if authorizeErr != nil {
		code := status.Code(authorizeErr)
		if code != codes.PermissionDenied && code != codes.Unauthenticated {
			return nil, fmt.Errorf("authorize %s: %w", call.Method, authorizeErr)
		}
		return p.denied(call, decision, target), nil
	}
	call.Resource = target.Name
	response, err := p.execute(call, request, scope)
	if err != nil {
		return nil, err
	}
	if response.Headers == nil {
		response.Headers = http.Header{}
	}
	response.Headers.Set("X-Gcp-Local-Decision-Id", decision.ID)
	return response, nil
}

// resolve derives the scope and IAM target for one request.
func (p *Provider) resolve(_ gateway.Call, request *gateway.RESTRequest) (state.Scope, iam.Target) {
	project := request.Path["project"]
	scope := scopeOf(project)
	target := iam.Target{Name: projectResource(project)}
	return scope, target
}

// projectResource names the resource-manager project ancestor.
func projectResource(project string) string {
	return "//cloudresourcemanager.googleapis.com/projects/" + project
}

// denied renders the Cloud Tasks denial shape.
func (p *Provider) denied(call gateway.Call, decision iam.Decision, target iam.Target) *gateway.RESTResponse {
	permission := "unspecified"
	for _, check := range decision.Checks {
		if !check.Allowed {
			permission = check.Permission
			break
		}
	}
	actor := call.Principal.Subject
	code := 403
	if call.Principal.State != gateway.Authenticated {
		actor, code = "Anonymous caller", 401
	}
	return wireError(code, "PERMISSION_DENIED", fmt.Sprintf("%s does not have %s access to %s.", actor, permission, target.Name))
}

// wireError builds the Cloud Tasks error body.
func wireError(code int, statusText, message string) *gateway.RESTResponse {
	return &gateway.RESTResponse{Status: code, JSON: map[string]any{
		"error": map[string]any{"code": code, "status": statusText, "message": message},
	}}
}
