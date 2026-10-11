package cloudtasks

import (
	"encoding/base64"
	"fmt"
	"maps"
	"math/rand/v2"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// userAgent is the header GCP adds to, and echoes in, every HTTP task.
const userAgent = "Google-Cloud-Tasks"

// dispatchDeadline is the GCP default HTTP-task dispatch deadline.
const dispatchDeadline = "600s"

// taskMethods are the HttpMethod values an HTTP task accepts.
var taskMethods = map[string]bool{
	http.MethodPost: true, http.MethodGet: true, http.MethodHead: true, http.MethodPut: true,
	http.MethodDelete: true, http.MethodPatch: true, http.MethodOptions: true,
}

// createTask stores one HTTP-target task and starts its dispatch worker. The
// OIDC service account is authorized before the task is stored.
func (p *Provider) createTask(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	body, err := parseBody(request.JSON)
	if err != nil {
		return nil, err
	}
	task, ok := body["task"].(map[string]any)
	if !ok {
		return nil, statusInvalid("A task is required")
	}
	full, err := p.fullView(call, request, textOf(body["responseView"]))
	if err != nil {
		return nil, err
	}
	record, err := p.newTask(task, request.Path["queue"])
	if err != nil {
		return nil, err
	}
	if record.ServiceAccount != "" {
		if err := p.authorizeActAs(call, scope.Project, record.ServiceAccount); err != nil {
			return nil, err
		}
	}
	document, err := p.storeTask(scope, request.Path["location"], record, full)
	if err != nil {
		return nil, err
	}
	p.launchDispatch(scope, taskKey(record.Queue, record.ID))
	return &gateway.RESTResponse{JSON: document}, nil
}

// storeTask adds one task to its existing queue.
func (p *Provider) storeTask(scope state.Scope, location string, record *taskRecord, full bool) (map[string]any, error) {
	var document map[string]any
	err := p.scopes.With(scope, func(s *scopeState) error {
		if s.queues[record.Queue] == nil {
			return notFound("queues", record.Queue)
		}
		key := taskKey(record.Queue, record.ID)
		if s.tasks[key] != nil {
			return statusExists("Task " + record.ID + " already exists")
		}
		s.tasks[key] = record
		document = taskDocument(scope.Project, location, record, full)
		return nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // status errors carry the wire status
	}
	return document, nil
}

// newTask validates one task body into its record.
func (p *Provider) newTask(task map[string]any, queue string) (*taskRecord, error) {
	httpRequest, ok := task["httpRequest"].(map[string]any)
	if !ok {
		return nil, gap("only HTTP target tasks are supported")
	}
	target, method, err := parseTarget(httpRequest)
	if err != nil {
		return nil, err
	}
	scheduleAt, err := parseSchedule(textOf(task["scheduleTime"]), p.clock)
	if err != nil {
		return nil, err
	}
	id := lastSegment(textOf(task["name"]))
	if id == "" {
		id = strconv.FormatUint(rand.Uint64N(9e18)+1e18, 10)
	}
	if !taskPattern.MatchString(id) {
		return nil, statusInvalid("Invalid task name " + id)
	}
	record := &taskRecord{
		ID: id, Queue: queue, URL: target, Method: method, Headers: map[string]string{},
		Created: p.clock(), ScheduleAt: scheduleAt, wake: make(chan struct{}, 1),
	}
	if err := parsePayloadAndHeaders(record, httpRequest); err != nil {
		return nil, err
	}
	if oidc, ok := httpRequest["oidcToken"].(map[string]any); ok {
		record.ServiceAccount, record.Audience = textOf(oidc["serviceAccountEmail"]), textOf(oidc["audience"])
		if record.Audience == "" {
			record.Audience = target
		}
	}
	return record, nil
}

// parseSchedule validates the optional RFC 3339 scheduleTime. A past or
// absent value schedules immediately.
func parseSchedule(value string, now func() time.Time) (time.Time, error) {
	if value == "" {
		return now(), nil
	}
	moment, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, statusInvalid("Invalid scheduleTime " + value)
	}
	return moment, nil
}

// parseTarget validates the absolute HTTP(S) target URL and the method.
func parseTarget(httpRequest map[string]any) (string, string, error) {
	target := textOf(httpRequest["url"])
	parsed, err := url.Parse(target)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return "", "", statusInvalid("HttpRequest.url must be an absolute http or https URL")
	}
	method := textOf(httpRequest["httpMethod"])
	if method == "" {
		method = http.MethodPost
	}
	if !taskMethods[method] {
		return "", "", statusInvalid("Invalid HttpRequest.httpMethod " + method)
	}
	return target, method, nil
}

// parsePayloadAndHeaders decodes the base64 body and copies the headers.
func parsePayloadAndHeaders(record *taskRecord, httpRequest map[string]any) error {
	if encoded, ok := httpRequest["body"].(string); ok {
		decoded, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return statusInvalid("HttpRequest.body must be base64")
		}
		if len(decoded) > maxPayload {
			return statusInvalid("The task body exceeds 1 MiB")
		}
		record.Body = decoded
	}
	if headers, ok := httpRequest["headers"].(map[string]any); ok {
		for key, value := range headers {
			record.Headers[http.CanonicalHeaderKey(key)] = textOf(value)
		}
	}
	return nil
}

// fullView reports whether the caller asked for, and may read, the FULL view.
func (p *Provider) fullView(call gateway.Call, request *gateway.RESTRequest, view string) (bool, error) {
	if view == "" {
		view = request.Query.Get("responseView")
	}
	if view != "FULL" {
		return false, nil
	}
	call.Method = methodTasksFullView
	if _, _, err := p.authorize(call, request); err != nil {
		if code := status.Code(err); code == codes.PermissionDenied || code == codes.Unauthenticated {
			return false, status.Error(codes.PermissionDenied, "The FULL view requires cloudtasks.tasks.fullView")
		}
		return false, fmt.Errorf("authorize the FULL view: %w", err)
	}
	return true, nil
}

// taskDocument renders the task wire shape; call it under the scope lock.
func taskDocument(project, location string, record *taskRecord, full bool) map[string]any {
	document := map[string]any{
		"name":             taskName(project, location, record.Queue, record.ID),
		"httpRequest":      httpRequestDocument(record, full),
		"scheduleTime":     stamp(record.ScheduleAt),
		"createTime":       stamp(record.Created),
		"dispatchDeadline": dispatchDeadline,
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
		document["firstAttempt"] = map[string]any{"dispatchTime": stamp(record.First.Dispatch)}
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
		"scheduleTime": stamp(attempt.Schedule), "dispatchTime": stamp(attempt.Dispatch),
		"responseTime": stamp(attempt.Response), "responseStatus": response,
	}
}

// withTask reads or changes one existing task under the scope lock.
func (p *Provider) withTask(scope state.Scope, request *gateway.RESTRequest, full bool, use func(*scopeState, string) error) (*gateway.RESTResponse, error) {
	key := taskKey(request.Path["queue"], request.Path["task"])
	var document map[string]any
	err := p.scopes.With(scope, func(s *scopeState) error {
		record := s.tasks[key]
		if record == nil {
			return notFound("tasks", request.Path["task"])
		}
		document = taskDocument(scope.Project, request.Path["location"], record, full)
		return use(s, key)
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // status errors carry the wire status
	}
	return &gateway.RESTResponse{JSON: document}, nil
}

// getTask returns one task document.
func (p *Provider) getTask(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	full, err := p.fullView(call, request, "")
	if err != nil {
		return nil, err
	}
	return p.withTask(scope, request, full, func(*scopeState, string) error { return nil })
}

// listTasks returns one queue's tasks.
func (p *Provider) listTasks(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	full, err := p.fullView(call, request, "")
	if err != nil {
		return nil, err
	}
	queue := request.Path["queue"]
	var items []any
	err = p.scopes.With(scope, func(s *scopeState) error {
		if s.queues[queue] == nil {
			return notFound("queues", queue)
		}
		keys := make([]string, 0, len(s.tasks))
		for key, task := range s.tasks {
			if task.Queue == queue {
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		for _, key := range keys {
			items = append(items, taskDocument(scope.Project, request.Path["location"], s.tasks[key], full))
		}
		return nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // status errors carry the wire status
	}
	if items == nil {
		return &gateway.RESTResponse{JSON: map[string]any{}}, nil
	}
	return &gateway.RESTResponse{JSON: map[string]any{"tasks": items}}, nil
}

// deleteTask removes one task and stops its worker.
func (p *Provider) deleteTask(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	if _, err := p.withTask(scope, request, false, func(s *scopeState, key string) error {
		delete(s.tasks, key)
		return nil
	}); err != nil {
		return nil, err
	}
	p.stopWorker(scope, taskKey(request.Path["queue"], request.Path["task"]))
	return &gateway.RESTResponse{JSON: map[string]any{}}, nil
}

// runTask schedules one task for immediate dispatch and wakes its worker.
func (p *Provider) runTask(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	return p.withTask(scope, request, false, func(s *scopeState, key string) error {
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
