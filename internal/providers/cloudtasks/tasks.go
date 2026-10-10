package cloudtasks

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"maps"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// createTask stores one HTTP-target task and starts its dispatch worker.
func (p *Provider) createTask(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	body, err := parseBody(request.JSON)
	if err != nil {
		return nil, err
	}
	httpRequest, ok := body["httpRequest"].(map[string]any)
	if !ok {
		return nil, statusInvalid("A task requires an httpRequest")
	}
	record, refused := p.parseTask(body, httpRequest, request.Path["queue"], p.mustQueue(scope, request.Path["queue"]))
	if refused != nil {
		return nil, refused
	}
	var conflict *gateway.RESTResponse
	err = p.scopes.With(scope, func(s *scopeState) error {
		if s.tasks[record.Name] != nil {
			conflict = wireError(409, "ALREADY_EXISTS", "task "+record.Name+" already exists")
			return nil
		}
		s.tasks[record.Name] = record
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("store the task: %w", err)
	}
	if conflict != nil {
		return conflict, nil
	}
	p.launchDispatch(scope, record.Name)
	return &gateway.RESTResponse{JSON: taskDocument(scope.Project, request.Path["location"], request.Path["queue"], p.mustTask(scope, record.Name))}, nil
}

// parseTask validates one task creation body into its record.
func (p *Provider) parseTask(body map[string]any, httpRequest map[string]any, queue string, owner *queueRecord) (*taskRecord, error) {
	url, method, oidc, refused := parseTarget(httpRequest)
	if refused != nil {
		return nil, refused
	}
	scheduleAt, refused := parseSchedule(textOf(body["scheduleTime"]), time.Now)
	if refused != nil {
		return nil, refused
	}
	record := &taskRecord{
		Name:           lastSegment(textOf(body["name"])),
		Queue:          queue,
		URL:            url,
		Method:         method,
		Headers:        map[string]string{},
		ServiceAccount: textOf(oidc["serviceAccountEmail"]),
		Audience:       textOf(oidc["audience"]),
		Created:        p.clock(),
		ScheduleAt:     scheduleAt,
	}
	if record.Name == "" {
		record.Name = fmt.Sprintf("task-%d", p.clock().UnixNano()%1000000)
	}
	if !validName(record.Name) {
		return nil, statusInvalid("Invalid task name " + record.Name)
	}
	if refused := parsePayloadAndHeaders(record, httpRequest); refused != nil {
		return nil, refused
	}
	if owner == nil {
		return nil, notFound("queues", queue)
	}
	record.State = "PENDING"
	return record, nil
}

// parseSchedule validates the optional RFC 3339 scheduleTime. A past or
// absent value schedules immediately.
func parseSchedule(value string, now func() time.Time) (time.Time, error) {
	if value == "" {
		return now(), nil
	}
	moment, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, statusInvalid("Invalid scheduleTime " + value)
	}
	return moment, nil
}

// parseTarget validates the target URL, method, and OIDC block.
func parseTarget(httpRequest map[string]any) (string, string, map[string]any, error) {
	url := textOf(httpRequest["url"])
	if url == "" || !strings.HasPrefix(url, "http://") {
		return "", "", nil, statusInvalid("A task requires an http:// target URL")
	}
	method := textOf(httpRequest["method"])
	if method == "" {
		method = http.MethodPost
	}
	oidc, _ := httpRequest["oidcToken"].(map[string]any)
	return url, method, oidc, nil
}

// parsePayloadAndHeaders validates the body size and header terms.
func parsePayloadAndHeaders(record *taskRecord, httpRequest map[string]any) error {
	if bodyRaw, ok := httpRequest["body"].(string); ok {
		if len(bodyRaw) > maxPayload {
			return statusInvalid("The task body exceeds 1 MiB")
		}
		record.Body = []byte(bodyRaw)
	}
	if headers, ok := httpRequest["headers"].(map[string]any); ok {
		for key, value := range headers {
			record.Headers[key] = textOf(value)
		}
	}
	return nil
}

// mustTask reads one task record under the scope lock.
func (p *Provider) mustTask(scope state.Scope, id string) *taskRecord {
	var record *taskRecord
	_ = p.scopes.With(scope, func(s *scopeState) error { record = s.tasks[id]; return nil })
	return record
}

// getTask returns one task document.
func (p *Provider) getTask(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	record := p.mustTask(scope, request.Path["task"])
	if record == nil {
		return nil, notFound("tasks", request.Path["task"])
	}
	return &gateway.RESTResponse{JSON: taskDocument(scope.Project, request.Path["location"], request.Path["queue"], record)}, nil
}

// listTasks returns one queue's tasks.
func (p *Provider) listTasks(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	queue := request.Path["queue"]
	var names []string
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for name, task := range s.tasks {
			if task.Queue == queue {
				names = append(names, name)
			}
		}
		return nil
	})
	sort.Strings(names)
	items := make([]any, 0, len(names))
	for _, name := range names {
		items = append(items, taskDocument(scope.Project, request.Path["location"], queue, p.mustTask(scope, name)))
	}
	return &gateway.RESTResponse{JSON: map[string]any{"tasks": items}}, nil
}

// deleteTask removes one pending task.
func (p *Provider) deleteTask(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	id := request.Path["task"]
	_ = p.scopes.With(scope, func(s *scopeState) error {
		if s.tasks[id] != nil {
			delete(s.tasks, id)
		}
		return nil
	})
	return &gateway.RESTResponse{JSON: map[string]any{}}, nil
}

// runTask forces one immediate dispatch attempt.
func (p *Provider) runTask(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	record := p.mustTask(scope, request.Path["task"])
	if record == nil {
		return nil, notFound("tasks", request.Path["task"])
	}
	go func() { _ = p.deliver(context.Background(), scope, request.Path["task"]) }()
	return &gateway.RESTResponse{JSON: taskDocument(scope.Project, request.Path["location"], request.Path["queue"], record)}, nil
}

// launchDispatch starts one background worker for the task.
func (p *Provider) launchDispatch(scope state.Scope, id string) {
	ctx, cancel := context.WithCancel(context.Background())
	p.mu.Lock()
	p.workers[scope.Project] = cancel
	p.mu.Unlock()
	go func() {
		_ = p.dispatchLoop(ctx, scope, id)
	}()
}

// dispatchLoop delivers the task at its scheduled time with backoff retries
// until it succeeds, fails, or the context ends.
func (p *Provider) dispatchLoop(ctx context.Context, scope state.Scope, id string) error {
	record := p.mustTask(scope, id)
	if record == nil {
		return nil
	}
	if wait := time.Until(record.ScheduleAt); wait > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	backoff := time.Second
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if done := p.deliver(ctx, scope, id); done {
			return nil
		}
		backoff *= 2
	}
	return nil
}

// deliver performs one dispatch attempt and records the outcome; it reports
// whether the task reached a terminal state.
func (p *Provider) deliver(ctx context.Context, scope state.Scope, id string) bool {
	record := p.mustTask(scope, id)
	if record == nil || record.State == "SUCCEEDED" {
		return true
	}
	record.Attempts++
	request, ok := p.buildRequest(ctx, scope, record)
	if !ok {
		return false
	}
	return p.sendAttempt(record, request)
}

// buildRequest builds the dispatch request, minting the OIDC token.
func (p *Provider) buildRequest(ctx context.Context, scope state.Scope, record *taskRecord) (*http.Request, bool) {
	headers := map[string]string{"Content-Type": "application/octet-stream"}
	maps.Copy(headers, record.Headers)
	if record.ServiceAccount != "" && p.tokens != nil {
		call := gateway.Call{Context: ctx, Principal: gateway.Principal{
			State: gateway.Authenticated, Subject: "serviceAgent:cloudtasks", Project: scope.Project,
		}}
		minted, err := p.tokens.Mint(call, "serviceAccount:"+record.ServiceAccount, record.Audience)
		if err == nil {
			headers["Authorization"] = "Bearer " + minted
		}
	}
	request, err := http.NewRequestWithContext(ctx, record.Method, record.URL, bytes.NewReader(record.Body))
	if err != nil {
		return nil, false
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	return request, true
}

// sendAttempt performs one HTTP dispatch and records the task outcome.
func (p *Provider) sendAttempt(record *taskRecord, request *http.Request) bool {
	response, err := p.client.Do(request)
	if err != nil {
		record.State = "FAILED"
		record.Detail = "The task target is unreachable"
		return false
	}
	defer func() { _ = response.Body.Close() }()
	_, _ = io.Copy(io.Discard, http.MaxBytesReader(nil, response.Body, 1<<20))
	if response.StatusCode >= 300 {
		record.State = "FAILED"
		record.Detail = "The task target returned " + strconv.Itoa(response.StatusCode)
		return false
	}
	record.State = "SUCCEEDED"
	record.Detail = ""
	return true
}

// scheduleField renders the optional scheduleTime of one task.
func scheduleField(record *taskRecord) string {
	if record.ScheduleAt.IsZero() {
		return ""
	}
	return stamp(record.ScheduleAt)
}
