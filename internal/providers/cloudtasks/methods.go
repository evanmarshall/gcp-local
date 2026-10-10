package cloudtasks

import (
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// maxAttempts bounds the dispatch retries of one task.
const maxAttempts = 5

// execute dispatches one authorized method to its resource family.
func (p *Provider) execute(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodQueuesCreate, methodQueuesGet, methodQueuesList, methodQueuesDelete,
		methodQueuesPause, methodQueuesResume, methodQueuesPurge,
		methodQueuesGetIam, methodQueuesSetIam:
		return p.executeQueue(call, request, scope)
	case methodTasksCreate, methodTasksGet, methodTasksList, methodTasksDelete, methodTasksRun:
		return p.executeTask(call, request, scope)
	}
	if call.Method == methodLocationsList {
		return &gateway.RESTResponse{JSON: map[string]any{"locations": []any{
			map[string]any{"locationId": "us-central1"},
		}}}, nil
	}
	return nil, gap("this method is not implemented")
}

// executeQueue dispatches the queue methods.
func (p *Provider) executeQueue(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodQueuesCreate:
		return p.createQueue(request, scope)
	case methodQueuesGet:
		return p.getQueue(request, scope)
	case methodQueuesList:
		return p.listQueues(scope)
	case methodQueuesDelete:
		return p.deleteQueue(request, scope)
	case methodQueuesGetIam:
		return p.getQueueIam(scope, request)
	case methodQueuesSetIam:
		return p.setQueueIam(request, scope)
	default:
		return p.executeQueueState(call, request, scope)
	}
}

// executeQueueState dispatches the pause/resume/purge state family.
func (p *Provider) executeQueueState(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodQueuesPause:
		return p.setQueueState(request, scope, "PAUSED")
	case methodQueuesResume:
		return p.setQueueState(request, scope, "RUNNING")
	case methodQueuesPurge:
		return p.purgeQueue(request, scope)
	default:
		return nil, gap("this method is not implemented")
	}
}

// executeTask dispatches the task methods.
func (p *Provider) executeTask(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodTasksCreate:
		return p.createTask(request, scope)
	case methodTasksGet:
		return p.getTask(request, scope)
	case methodTasksList:
		return p.listTasks(request, scope)
	case methodTasksDelete:
		return p.deleteTask(request, scope)
	case methodTasksRun:
		return p.runTask(request, scope)
	default:
		return nil, gap("this method is not implemented")
	}
}

// queueDocument renders the queue wire shape.
func queueDocument(project, location string, record *queueRecord) map[string]any {
	state := record.State
	if state == "" {
		state = "RUNNING"
	}
	return map[string]any{
		"name":             queueResource(project, location, record.Name),
		"rateLimits":       record.Rate,
		"state":            state,
		"createTime":       stamp(record.Created),
		"dispatchDeadline": "600s",
	}
}

// taskDocument renders the task wire shape.
func taskDocument(project, location, queue string, record *taskRecord) map[string]any {
	headers := map[string]string{}
	maps.Copy(headers, record.Headers)
	httpRequest := map[string]any{
		"url":     record.URL,
		"method":  record.Method,
		"headers": headers,
	}
	if record.Body != nil {
		httpRequest["body"] = string(record.Body)
	}
	if record.ServiceAccount != "" {
		httpRequest["oidcToken"] = map[string]any{
			"serviceAccountEmail": record.ServiceAccount,
			"audience":            record.Audience,
		}
	}
	return map[string]any{
		"name":             taskResource(project, location, queue, record.Name),
		"httpRequest":      httpRequest,
		"createTime":       stamp(record.Created),
		"dispatchCount":    strconv.Itoa(record.Attempts),
		"dispatchDeadline": "600s",
		"view":             "FULL",
	}
}

// parseBody decodes one request body as a plain map.
func parseBody(body []byte) (map[string]any, error) {
	var value map[string]any
	if json.Unmarshal(body, &value) != nil {
		return nil, statusInvalid("Invalid request body")
	}
	return value, nil
}

// textOf reads a string field as empty-safe text.
func textOf(value any) string {
	if value, ok := value.(string); ok {
		return value
	}
	return ""
}

// createQueue stores one queue.
func (p *Provider) createQueue(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	body, err := parseBody(request.JSON)
	if err != nil {
		return nil, err
	}
	id := queueID(request, body)
	if !validName(id) {
		return nil, statusInvalid("Invalid queue name " + id)
	}
	rate, _ := body["rateLimits"].(map[string]any)
	now := p.clock()
	var conflict *gateway.RESTResponse
	err = p.scopes.With(scope, func(s *scopeState) error {
		if s.queues[id] != nil {
			conflict = wireError(409, "ALREADY_EXISTS", "queue "+id+" already exists")
			return nil
		}
		s.queues[id] = &queueRecord{Name: id, Rate: rate, Created: now}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("store the queue: %w", err)
	}
	if conflict != nil {
		return conflict, nil
	}
	if err := p.index.Put(serviceName, state.Resource{
		Name: queueResource(scope.Project, request.Path["location"], id), Service: serviceName,
		Kind: "queue", Parent: projectResource(scope.Project), Scope: scope,
	}); err != nil {
		return nil, fmt.Errorf("index the queue: %w", err)
	}
	return &gateway.RESTResponse{JSON: queueDocument(scope.Project, request.Path["location"], p.mustQueue(scope, id))}, nil
}

// queueID derives the queue identifier from the path or the body name.
func queueID(request *gateway.RESTRequest, body map[string]any) string {
	if id := request.Path["queue"]; id != "" {
		return id
	}
	return lastSegment(textOf(body["name"]))
}

// lastSegment extracts the trailing identifier of a full resource name.
func lastSegment(name string) string {
	if index := strings.LastIndex(name, "/"); index >= 0 {
		return name[index+1:]
	}
	return name
}

// validName reports whether the identifier matches the queue/task shape.
func validName(id string) bool { return uidPattern.MatchString(id) }

// mustQueue reads one queue record under the scope lock.
func (p *Provider) mustQueue(scope state.Scope, id string) *queueRecord {
	var record *queueRecord
	_ = p.scopes.With(scope, func(s *scopeState) error { record = s.queues[id]; return nil })
	return record
}

// getQueue returns one queue document.
func (p *Provider) getQueue(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	record := p.mustQueue(scope, request.Path["queue"])
	if record == nil {
		return nil, notFound("queues", request.Path["queue"])
	}
	return &gateway.RESTResponse{JSON: queueDocument(scope.Project, request.Path["location"], record)}, nil
}

// listQueues returns the project's queues.
func (p *Provider) listQueues(scope state.Scope) (*gateway.RESTResponse, error) {
	var names []string
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for name := range s.queues {
			names = append(names, name)
		}
		return nil
	})
	sort.Strings(names)
	items := make([]any, 0, len(names))
	for _, name := range names {
		items = append(items, queueDocument(scope.Project, "us-central1", p.mustQueue(scope, name)))
	}
	return &gateway.RESTResponse{JSON: map[string]any{"queues": items}}, nil
}

// deleteQueue removes one queue and its tasks.
func (p *Provider) deleteQueue(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	id := request.Path["queue"]
	_ = p.scopes.With(scope, func(s *scopeState) error {
		if s.queues[id] != nil {
			delete(s.queues, id)
			for name, task := range s.tasks {
				if task.Queue == id {
					delete(s.tasks, name)
				}
			}
		}
		return nil
	})
	_ = p.index.Remove(serviceName, scope, queueResource(scope.Project, request.Path["location"], id))
	return &gateway.RESTResponse{JSON: map[string]any{}}, nil
}

// setQueueState pauses or resumes one queue.
func (p *Provider) setQueueState(request *gateway.RESTRequest, scope state.Scope, state string) (*gateway.RESTResponse, error) {
	id := request.Path["queue"]
	record := p.mustQueue(scope, id)
	if record == nil {
		return nil, notFound("queues", id)
	}
	record.State = state
	return &gateway.RESTResponse{JSON: queueDocument(scope.Project, request.Path["location"], record)}, nil
}

// purgeQueue removes every task of one queue.
func (p *Provider) purgeQueue(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	id := request.Path["queue"]
	record := p.mustQueue(scope, id)
	if record == nil {
		return nil, notFound("queues", id)
	}
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for name, task := range s.tasks {
			if task.Queue == id {
				delete(s.tasks, name)
			}
		}
		return nil
	})
	return &gateway.RESTResponse{JSON: queueDocument(scope.Project, request.Path["location"], record)}, nil
}

// getQueueIam returns the stored queue policy.
func (p *Provider) getQueueIam(scope state.Scope, request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	policy, err := p.engine.GetPolicy(queueResource(scope.Project, request.Path["location"], request.Path["queue"]))
	if err != nil {
		return nil, fmt.Errorf("read queue policy: %w", err)
	}
	return &gateway.RESTResponse{JSON: policy}, nil
}

// setQueueIam stores one unconditional version-1 queue policy.
func (p *Provider) setQueueIam(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	var envelope struct {
		Policy iam.Policy `json:"policy"`
	}
	if json.Unmarshal(request.JSON, &envelope) != nil {
		return nil, statusInvalid("Invalid IAM policy")
	}
	policy, err := p.engine.SetPolicy(queueResource(scope.Project, request.Path["location"], request.Path["queue"]), envelope.Policy)
	if err != nil {
		return nil, fmt.Errorf("write queue policy: %w", err)
	}
	return &gateway.RESTResponse{JSON: policy}, nil
}
