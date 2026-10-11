package cloudtasks

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
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
	methodQueuesUpdate  = "cloudtasks.queues.update"
	methodQueuesPause   = "cloudtasks.queues.pause"
	methodQueuesResume  = "cloudtasks.queues.resume"
	methodQueuesPurge   = "cloudtasks.queues.purge"
	methodTasksCreate   = "cloudtasks.tasks.create"
	methodTasksGet      = "cloudtasks.tasks.get"
	methodTasksList     = "cloudtasks.tasks.list"
	methodTasksDelete   = "cloudtasks.tasks.delete"
	methodTasksRun      = "cloudtasks.tasks.run"
	methodQueuesGetIam  = "cloudtasks.queues.getIamPolicy"
	methodQueuesSetIam  = "cloudtasks.queues.setIamPolicy"
	methodLocationsList = "cloudtasks.locations.list"
)

// Engine-only plans for state-dependent task checks; they have no route.
const (
	methodTasksActAs    = "cloudtasks.tasks.actAs"
	methodTasksFullView = "cloudtasks.tasks.fullView"
)

// statePermissions bind the engine-only plans to their permissions.
var statePermissions = map[string]string{
	methodTasksActAs:    "iam.serviceAccounts.actAs",
	methodTasksFullView: "cloudtasks.tasks.fullView",
}

// permissions follow the documented Cloud Tasks access-control rows.
var permissions = map[string]string{
	methodQueuesCreate:  "cloudtasks.queues.create",
	methodQueuesGet:     "cloudtasks.queues.get",
	methodQueuesList:    "cloudtasks.queues.list",
	methodQueuesDelete:  "cloudtasks.queues.delete",
	methodQueuesUpdate:  "cloudtasks.queues.update",
	methodQueuesPause:   "cloudtasks.queues.pause",
	methodQueuesResume:  "cloudtasks.queues.resume",
	methodQueuesPurge:   "cloudtasks.queues.purge",
	methodTasksCreate:   "cloudtasks.tasks.create",
	methodTasksGet:      "cloudtasks.tasks.get",
	methodTasksList:     "cloudtasks.tasks.list",
	methodTasksDelete:   "cloudtasks.tasks.delete",
	methodTasksRun:      "cloudtasks.tasks.run",
	methodQueuesGetIam:  "cloudtasks.queues.getIamPolicy",
	methodQueuesSetIam:  "cloudtasks.queues.setIamPolicy",
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
	workers map[string]*worker
	// retention keeps an exhausted task readable after its last attempt.
	retention time.Duration
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
		scopes:    state.NewStore(newScopeState, state.GlobalScope),
		client:    dispatchClient(),
		clock:     time.Now,
		workers:   map[string]*worker{},
		retention: exhaustedRetention,
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

// dispatchClient bounds each attempt; a target that never answers fails as a
// transport error well inside the queue's retry schedule.
func dispatchClient() *http.Client {
	client := &http.Client{Timeout: 10 * time.Second}
	if base, ok := http.DefaultTransport.(*http.Transport); ok {
		transport := base.Clone()
		transport.DialContext = (&net.Dialer{Timeout: 5 * time.Second}).DialContext
		client.Transport = transport
	}
	return client
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

// workerKey addresses one task's dispatch worker.
func workerKey(scope state.Scope, key string) string { return scope.Project + "\x00" + key }

// stopWorkers cancels the dispatch workers of one scope.
func (p *Provider) stopWorkers(scope state.Scope) {
	p.mu.Lock()
	for key, active := range p.workers {
		if strings.HasPrefix(key, scope.Project+"\x00") {
			active.cancel()
			delete(p.workers, key)
		}
	}
	p.mu.Unlock()
}

// stopWorker cancels one task's dispatch worker.
func (p *Provider) stopWorker(scope state.Scope, key string) {
	p.mu.Lock()
	if active := p.workers[workerKey(scope, key)]; active != nil {
		active.cancel()
		delete(p.workers, workerKey(scope, key))
	}
	p.mu.Unlock()
}

// registerPermissions binds every method to its permission.
func registerPermissions(engine *iam.Engine) error {
	for _, table := range []map[string]string{permissions, statePermissions} {
		for method, permission := range table {
			if err := engine.Register(method, iam.Rule{Baseline: []string{permission}}); err != nil {
				return fmt.Errorf("register %s: %w", method, err)
			}
		}
	}
	return nil
}

// queueRecord is one stored queue.
type queueRecord struct {
	Name     string
	Location string
	Rate     map[string]any
	Retry    retryPolicy
	State    string // RUNNING, PAUSED
}

// attemptRecord is one dispatch attempt. Code is the RPC status of the
// response; a transport failure is UNKNOWN, as GCP reports it.
type attemptRecord struct {
	Schedule, Dispatch, Response time.Time
	Code                         codes.Code
	Message                      string
}

// taskRecord is one stored task; GCP removes a task once it succeeds or
// exhausts its queue's attempts.
type taskRecord struct {
	ID             string
	Queue          string
	URL            string
	Method         string
	Body           []byte
	Headers        map[string]string
	ServiceAccount string
	Audience       string
	Created        time.Time
	ScheduleAt     time.Time
	Dispatches     int
	Responses      int
	First, Last    *attemptRecord
	wake           chan struct{}
}

// taskKey addresses one task; task IDs are unique only within a queue.
func taskKey(queue, id string) string { return queue + "/" + id }

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
	scope := scopeOf(request.Path["project"])
	decision, target, authorizeErr := p.authorize(call, request)
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

// authorize checks the method against its queue, so queue policies grant
// queue and task access. A queue that is not indexed authorizes through the
// project anchor and then reads 404.
func (p *Provider) authorize(call gateway.Call, request *gateway.RESTRequest) (iam.Decision, iam.Target, error) {
	project := iam.Target{Name: projectResource(request.Path["project"])}
	target := project
	if queue := request.Path["queue"]; queue != "" {
		target = iam.Target{Name: queueResource(request.Path["project"], request.Path["location"], queue)}
	}
	decision, err := p.engine.Authorize(call, target, nil)
	if target != project && status.Code(err) == codes.Unimplemented {
		target = project
		decision, err = p.engine.Authorize(call, target, nil)
	}
	return decision, target, err //nolint:wrapcheck // callers read the IAM status code
}

// authorizeActAs requires iam.serviceAccounts.actAs on the OIDC service
// account before a task that mints its token is stored.
func (p *Provider) authorizeActAs(call gateway.Call, project, email string) error {
	account := iam.AccountResource(project, email)
	call.Method, call.Resource = methodTasksActAs, account
	_, err := p.engine.Authorize(call, iam.Target{Name: account}, nil)
	switch status.Code(err) {
	case codes.OK:
		return nil
	case codes.PermissionDenied, codes.Unauthenticated:
		return status.Error(codes.PermissionDenied, "The caller lacks iam.serviceAccounts.actAs on "+email)
	case codes.Unimplemented:
		return gap("OIDC tokens require a declared same-project local service account")
	default:
		return fmt.Errorf("authorize the OIDC service account: %w", err)
	}
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
	if call.Principal.State != gateway.Authenticated {
		return wireError(401, "UNAUTHENTICATED", "Request had invalid authentication credentials.")
	}
	return wireError(403, "PERMISSION_DENIED", fmt.Sprintf("%s does not have %s access to %s.", call.Principal.Subject, permission, target.Name))
}

// wireError builds the Cloud Tasks error body.
func wireError(code int, statusText, message string) *gateway.RESTResponse {
	return &gateway.RESTResponse{Status: code, JSON: map[string]any{
		"error": map[string]any{"code": code, "status": statusText, "message": message},
	}}
}
