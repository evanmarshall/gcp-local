package cloudtasks

import (
	"encoding/base64"
	"maps"
	"math/rand/v2"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	taskspb "cloud.google.com/go/cloudtasks/apiv2beta3/cloudtaskspb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
)

// userAgent is the header GCP adds to, and echoes in, every HTTP task.
const userAgent = "Google-Cloud-Tasks"

// Task.dispatch_deadline's documented default and bounds for HTTP tasks.
const (
	defaultDeadline = 10 * time.Minute
	minDeadline     = 15 * time.Second
	maxDeadline     = 30 * time.Minute
)

// tombstoned is GCP's refusal of a task name used too recently (recording
// cloudtasks-dispatch-retry).
const tombstoned = "The task cannot be created because a task with this name existed too recently. " +
	"For more information about task de-duplication see " +
	"https://cloud.google.com/tasks/docs/reference/rest/v2beta3/projects.locations.queues.tasks/create#body.request_body.FIELDS.task."

// createTask stores one HTTP-target task and starts its dispatch worker. The
// OIDC service account is authorized before the task is stored.
func (p *Provider) createTask(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	body := &taskspb.CreateTaskRequest{}
	if err := decodeRequest(request.JSON, body, unmodeledTask...); err != nil {
		return nil, err
	}
	if body.GetTask() == nil {
		return nil, provider.Invalid("A task is required")
	}
	full, err := p.fullView(call, request, body.GetResponseView())
	if err != nil {
		return nil, err
	}
	record, err := p.newTask(body.GetTask(), queueKey(request))
	if err != nil {
		return nil, err
	}
	if record.ServiceAccount != "" {
		if err := p.authorizeActAs(call, scope.Project, record.ServiceAccount); err != nil {
			return nil, err
		}
		if p.tokens == nil {
			return nil, gap("OIDC tokens need the local ID-token issuer")
		}
	}
	document, err := p.storeTask(scope, record, full)
	if err != nil {
		return nil, err
	}
	p.launchDispatch(scope, taskKey(record.Queue, record.ID), record)
	return &gateway.RESTResponse{JSON: document}, nil
}

// storeTask adds one task to its existing queue, unless its name is in use
// or tombstoned.
func (p *Provider) storeTask(scope state.Scope, record *taskRecord, full bool) (map[string]any, error) {
	var document map[string]any
	err := p.scopes.With(scope, func(s *scopeState) error {
		if s.queues[record.Queue] == nil {
			return provider.NotFound("queues", provider.LastSegment(record.Queue))
		}
		key := taskKey(record.Queue, record.ID)
		if s.tasks[key] != nil {
			return statusExists("Task " + record.ID + " already exists")
		}
		if until, found := s.tombstones[key]; found {
			if p.clock().Before(until) {
				return statusExists(tombstoned)
			}
			delete(s.tombstones, key)
		}
		s.tasks[key] = record
		document = taskDocument(scope.Project, record, full)
		return nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // status errors carry the wire status
	}
	return document, nil
}

// newTask validates one task into its record.
func (p *Provider) newTask(task *taskspb.Task, queue string) (*taskRecord, error) {
	httpRequest := task.GetHttpRequest()
	if httpRequest == nil {
		return nil, gap("only HTTP target tasks are supported")
	}
	target, method, err := parseTarget(httpRequest)
	if err != nil {
		return nil, err
	}
	deadline, err := dispatchDeadline(task)
	if err != nil {
		return nil, err
	}
	if len(httpRequest.GetBody()) > maxPayload {
		return nil, provider.Invalid("The task body exceeds 1 MiB")
	}
	id, err := taskID(task.GetName())
	if err != nil {
		return nil, err
	}
	now := p.clock()
	scheduleAt := now
	if task.GetScheduleTime() != nil {
		scheduleAt = task.GetScheduleTime().AsTime()
	}
	record := &taskRecord{
		ID: id, Queue: queue, URL: target, Method: method, Headers: map[string]string{},
		Body: httpRequest.GetBody(), Deadline: deadline, Created: now, ScheduleAt: scheduleAt, wake: make(chan struct{}, 1),
	}
	for key, value := range httpRequest.GetHeaders() {
		record.Headers[http.CanonicalHeaderKey(key)] = value
	}
	if oidc := httpRequest.GetOidcToken(); oidc != nil {
		record.ServiceAccount, record.Audience = oidc.GetServiceAccountEmail(), oidc.GetAudience()
		if record.Audience == "" {
			record.Audience = target
		}
	}
	return record, nil
}

// dispatchDeadline reads a task's deadline, default 10 minutes.
func dispatchDeadline(task *taskspb.Task) (time.Duration, error) {
	deadline := defaultDeadline
	if task.GetDispatchDeadline() != nil {
		deadline = task.GetDispatchDeadline().AsDuration()
	}
	if deadline < minDeadline || deadline > maxDeadline {
		return 0, provider.Invalid("Task.dispatchDeadline must be between [15s, 30m].")
	}
	return deadline, nil
}

// taskID reads the requested task ID, or generates one.
func taskID(name string) (string, error) {
	id := provider.LastSegment(name)
	if id == "" {
		id = strconv.FormatUint(rand.Uint64N(9e18)+1e18, 10)
	}
	if !taskPattern.MatchString(id) {
		return "", provider.Invalid("Invalid task name " + id)
	}
	return id, nil
}

// parseTarget validates the absolute HTTP(S) target URL and the method.
func parseTarget(httpRequest *taskspb.HttpRequest) (string, string, error) {
	target := httpRequest.GetUrl()
	parsed, err := url.Parse(target)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return "", "", provider.Invalid("HttpRequest.url must be an absolute http or https URL")
	}
	method := http.MethodPost
	if httpRequest.GetHttpMethod() != taskspb.HttpMethod_HTTP_METHOD_UNSPECIFIED {
		method = httpRequest.GetHttpMethod().String()
	}
	return target, method, nil
}

// fullView reports whether the caller asked for, and may read, the FULL view,
// which needs cloudtasks.tasks.fullView on the queue.
func (p *Provider) fullView(call gateway.Call, request *gateway.RESTRequest, view taskspb.Task_View) (bool, error) {
	if text := request.Query.Get("responseView"); view == taskspb.Task_VIEW_UNSPECIFIED && text != "" {
		value, known := taskspb.Task_View_value[text]
		if !known {
			return false, provider.FieldViolation("response_view",
				"Invalid value at 'response_view' (type.googleapis.com/google.cloud.tasks.v2beta3.Task.View), \""+text+"\"")
		}
		view = taskspb.Task_View(value)
	}
	if view != taskspb.Task_FULL {
		return false, nil
	}
	call.Method = methodTasksFullView
	if _, _, err := p.authorize(call, request); err != nil {
		return false, err
	}
	return true, nil
}

// taskDocument renders the task wire shape; call it under the scope lock.
func taskDocument(project string, record *taskRecord, full bool) map[string]any {
	location, queue, _ := strings.Cut(record.Queue, "/")
	document := map[string]any{
		keyName:            taskName(project, location, queue, record.ID),
		"httpRequest":      httpRequestDocument(record, full),
		"scheduleTime":     provider.Stamp(record.ScheduleAt),
		"createTime":       provider.Stamp(record.Created),
		"dispatchDeadline": formatDuration(record.Deadline),
		"view":             "BASIC",
	}
	if full {
		document["view"] = "FULL"
	}
	if record.Dispatches > 0 {
		document["dispatchCount"] = record.Dispatches
	}
	if record.Responses > 0 {
		document["responseCount"] = record.Responses
	}
	if record.First != nil {
		// GCP retains only the dispatch time of the first attempt.
		document["firstAttempt"] = map[string]any{"dispatchTime": provider.Stamp(record.First.Dispatch)}
	}
	if record.Last != nil {
		document["lastAttempt"] = attemptDocument(record.Last)
	}
	return document
}

// httpRequestDocument renders the HTTP target; the body and OIDC token are
// FULL-view fields.
func httpRequestDocument(record *taskRecord, full bool) map[string]any {
	headers := map[string]any{"User-Agent": userAgent}
	for key, value := range record.Headers {
		headers[key] = value
	}
	document := map[string]any{"url": record.URL, "httpMethod": record.Method, "headers": headers}
	if !full {
		return document
	}
	if record.Body != nil {
		document["body"] = base64.StdEncoding.EncodeToString(record.Body)
	}
	if record.ServiceAccount != "" {
		document["oidcToken"] = map[string]any{"serviceAccountEmail": record.ServiceAccount, "audience": record.Audience}
	}
	return document
}

// attemptDocument renders one completed dispatch attempt.
func attemptDocument(attempt *attemptRecord) map[string]any {
	response := map[string]any{}
	if attempt.Code != codes.OK {
		response["code"] = int(attempt.Code)
	}
	if attempt.Message != "" {
		response["message"] = attempt.Message
	}
	return map[string]any{
		"scheduleTime": provider.Stamp(attempt.Schedule), "dispatchTime": provider.Stamp(attempt.Dispatch),
		"responseTime": provider.Stamp(attempt.Response), "responseStatus": response,
	}
}

// withTask reads or changes one existing task under the scope lock.
func (p *Provider) withTask(scope state.Scope, request *gateway.RESTRequest, full bool, use func(*scopeState, string) error) (*gateway.RESTResponse, error) {
	key := taskKey(queueKey(request), request.Path["task"])
	var document map[string]any
	err := p.scopes.With(scope, func(s *scopeState) error {
		record := s.tasks[key]
		if record == nil {
			return provider.NotFound("tasks", request.Path["task"])
		}
		document = taskDocument(scope.Project, record, full)
		return use(s, key)
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // status errors carry the wire status
	}
	return &gateway.RESTResponse{JSON: document}, nil
}

func (p *Provider) getTask(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	full, err := p.fullView(call, request, taskspb.Task_VIEW_UNSPECIFIED)
	if err != nil {
		return nil, err
	}
	return p.withTask(scope, request, full, func(*scopeState, string) error { return nil })
}

// tasksPaging is ListTasks' documented page sizes (maximum 1000, the
// default when unset) and recorded refusals; a full last page still carries
// a next token (recording cloudtasks-dispatch-retry).
var tasksPaging = provider.Paging{Default: 1000, Max: 1000, InvalidToken: provider.Invalid("Invalid page_token"),
	NegativeSize: provider.Invalid("ListTasksRequest.pageSize cannot be negative."), FullPageToken: true}

// listTasks returns one page of a queue's tasks in name order. GCP leaves
// the order unspecified.
func (p *Provider) listTasks(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	full, err := p.fullView(call, request, taskspb.Task_VIEW_UNSPECIFIED)
	if err != nil {
		return nil, err
	}
	queue := queueKey(request)
	var body map[string]any
	err = p.scopes.With(scope, func(s *scopeState) error {
		if s.queues[queue] == nil {
			return provider.NotFound("queues", request.Path["queue"])
		}
		keys := slices.Sorted(maps.Keys(s.tasks))
		keys = slices.DeleteFunc(keys, func(key string) bool { return s.tasks[key].Queue != queue })
		page, next, err := provider.Page(keys, request.Query, tasksPaging, gap)
		if err != nil {
			return err
		}
		items := make([]map[string]any, 0, len(page))
		for _, key := range page {
			items = append(items, taskDocument(scope.Project, s.tasks[key], full))
		}
		body = provider.ListBody("tasks", items, next)
		return nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // status errors carry the wire status
	}
	return &gateway.RESTResponse{JSON: body}, nil
}

// deleteTask removes one task, tombstones its name, and stops its worker.
func (p *Provider) deleteTask(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	if _, err := p.withTask(scope, request, false, func(s *scopeState, key string) error {
		s.bury(key, p.clock())
		return nil
	}); err != nil {
		return nil, err
	}
	p.stopWorker(scope, taskKey(queueKey(request), request.Path["task"]))
	return &gateway.RESTResponse{JSON: map[string]any{}}, nil
}

// runTask schedules one task for immediate dispatch and wakes its worker.
func (p *Provider) runTask(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	body := &taskspb.RunTaskRequest{}
	if err := decodeRequest(request.JSON, body); err != nil {
		return nil, err
	}
	full, err := p.fullView(call, request, body.GetResponseView())
	if err != nil {
		return nil, err
	}
	return p.withTask(scope, request, full, func(s *scopeState, key string) error {
		record := s.tasks[key]
		record.ScheduleAt = p.clock()
		select {
		case record.wake <- struct{}{}:
		default:
		}
		return nil
	})
}

// requestSnapshot copies the dispatch inputs of one task; call it under the
// scope lock.
func requestSnapshot(record *taskRecord) taskRecord {
	snapshot := *record
	snapshot.Headers = maps.Clone(record.Headers)
	snapshot.Body = append([]byte(nil), record.Body...)
	return snapshot
}
