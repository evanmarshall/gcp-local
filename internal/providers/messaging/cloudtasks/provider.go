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

	taskspb "cloud.google.com/go/cloudtasks/apiv2beta3/cloudtaskspb"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
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
	gate    provider.Gate
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
	if err := errors.Join(provider.RegisterPermissions(engine, permissions), provider.RegisterPermissions(engine, statePermissions)); err != nil {
		return nil, fmt.Errorf("cloudtasks permissions: %w", err)
	}
	p := &Provider{
		index: index, engine: engine, tokens: config.Tokens,
		// Cloud Tasks' denials carry no details (recording cloudtasks-dispatch-retry).
		gate:      provider.Gate{Engine: engine, Index: index, Message: provider.LacksPermission},
		scopes:    state.NewStore(newScopeState, state.GlobalScope),
		client:    dispatchClient(),
		clock:     time.Now,
		workers:   map[string]*worker{},
		retention: exhaustedRetention,
	}
	if err := manager.Register(serviceName, backend.Definition{
		Validate: state.GlobalScope,
		New:      provider.NoBackend,
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

// dispatchClient bounds the connection; each attempt's own deadline is its
// task's dispatchDeadline.
func dispatchClient() *http.Client {
	client := &http.Client{}
	if base, ok := http.DefaultTransport.(*http.Transport); ok {
		transport := base.Clone()
		transport.DialContext = (&net.Dialer{Timeout: 5 * time.Second}).DialContext
		client.Transport = transport
	}
	return client
}

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
	for key, active := range p.workers {
		if strings.HasPrefix(key, scope.Project+"\x00") {
			active.cancel()
			delete(p.workers, key)
		}
	}
	p.mu.Unlock()
}

// stopWorker cancels one task's dispatch worker.
// A task created again under the name since keeps its own worker. p.mu is
// held while the store is read; no store callback takes p.mu.
func (p *Provider) stopWorker(scope state.Scope, key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	active := p.workers[provider.WorkerKey(scope, key)]
	if active == nil {
		return
	}
	var current *taskRecord
	_ = p.scopes.With(scope, func(s *scopeState) error {
		current = s.tasks[key]
		return nil
	})
	if current != nil && current == active.own {
		return
	}
	active.cancel()
	delete(p.workers, provider.WorkerKey(scope, key))
}

// queueRecord is one stored queue: its wire fields, defaults filled, and
// the dispatch limiter's state.
type queueRecord struct {
	queue *taskspb.Queue
	// tokens and refilled are the rate limiter's bucket; inflight counts the
	// attempts under way.
	tokens   float64
	refilled time.Time
	inflight int
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
	Deadline       time.Duration
	Created        time.Time
	ScheduleAt     time.Time
	Dispatches     int
	Responses      int
	First, Last    *attemptRecord
	wake           chan struct{}
}

// taskKey addresses one task; task IDs are unique only within a queue.
func taskKey(queue, id string) string { return queue + "/" + id }

// scopeState is one project slice of provider state. Queues are keyed by
// location and ID, tasks by queue key and ID; tombstones hold the names of
// removed tasks until they may be reused.
type scopeState struct {
	queues     map[string]*queueRecord
	tasks      map[string]*taskRecord
	tombstones map[string]time.Time
}

func newScopeState() scopeState {
	return scopeState{queues: map[string]*queueRecord{}, tasks: map[string]*taskRecord{}, tombstones: map[string]time.Time{}}
}

func (s *scopeState) reset() {
	*s = newScopeState()
}

// bury removes a task and tombstones its name for the queue's tombstoneTtl,
// as GCP does after a task is deleted, succeeds, or exhausts its retries.
func (s *scopeState) bury(key string, now time.Time) {
	task := s.tasks[key]
	if task == nil {
		return
	}
	delete(s.tasks, key)
	if queue := s.queues[task.Queue]; queue != nil {
		s.tombstones[key] = now.Add(queue.queue.GetTombstoneTtl().AsDuration())
	}
}

// handle is the gateway entry point for every Cloud Tasks method.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Cloud Tasks request")
	}
	scope := provider.ProjectScope(request.Path["project"])
	decision, target, err := p.authorize(call, request)
	if err != nil {
		return nil, err
	}
	call.Resource = target.Name
	response, err := p.execute(call, request, scope)
	if err != nil {
		return nil, err
	}
	return provider.WithDecision(response, decision), nil
}

// authorize checks the method against its queue, so queue policies grant
// queue and task access, or against the project for a queue that is not
// indexed (which then reads 404) and for collection methods. Queue create
// and delete change the index under the scope lock Nearest holds. A denial
// names the task, the queue, or the location for queue create and list
// (recording cloudtasks-dispatch-retry).
func (p *Provider) authorize(call gateway.Call, request *gateway.RESTRequest) (iam.Decision, iam.Target, error) {
	project, location, queue := request.Path["project"], request.Path["location"], request.Path["queue"]
	candidates := []string{provider.ProjectResource(project)}
	resource := "projects/" + project
	switch {
	case queue != "":
		candidates = []string{queueResource(project, location, queue), candidates[0]}
		resource = queueName(project, location, queue)
		if task := request.Path["task"]; task != "" {
			resource = taskName(project, location, queue, task)
		}
	case location != "":
		resource = locationName(project, location)
	}
	return provider.Nearest(p.gate, p.scopes, provider.ProjectScope(project), call, resource, candidates...)
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
