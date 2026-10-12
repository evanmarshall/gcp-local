package cloudtasks

import (
	"encoding/json"
	"fmt"
	"maps"
	"sort"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// execute dispatches one authorized method to its resource family.
func (p *Provider) execute(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodQueuesCreate, methodQueuesGet, methodQueuesList, methodQueuesDelete,
		methodQueuesPause, methodQueuesResume, methodQueuesPurge,
		methodQueuesGetIam, methodQueuesSetIam, methodQueuesUpdate:
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
		if request.Query.Get("filter") != "" {
			return nil, gap("queue list filters are not implemented")
		}
		return p.listQueues(scope)
	case methodQueuesDelete:
		return p.deleteQueue(request, scope)
	case methodQueuesUpdate:
		return p.updateQueue(request, scope)
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
		return p.createTask(call, request, scope)
	case methodTasksGet:
		return p.getTask(call, request, scope)
	case methodTasksList:
		return p.listTasks(call, request, scope)
	case methodTasksDelete:
		return p.deleteTask(request, scope)
	case methodTasksRun:
		return p.runTask(request, scope)
	default:
		return nil, gap("this method is not implemented")
	}
}

// queueDocument renders the queue wire shape; call it under the scope lock.
func queueDocument(project string, record *queueRecord) map[string]any {
	return map[string]any{
		"name":         queueName(project, record.Location, record.Name),
		"rateLimits":   maps.Clone(record.Rate),
		"retryConfig":  record.Retry.wire(),
		"state":        record.State,
		"taskTtl":      defaultTaskTTL,
		"tombstoneTtl": defaultTombstoneTTL,
		"type":         queueTypePush,
	}
}

// parseBody decodes one request body as a plain map; an absent body is empty.
func parseBody(body []byte) (map[string]any, error) {
	if len(body) == 0 {
		return map[string]any{}, nil
	}
	var value map[string]any
	if json.Unmarshal(body, &value) != nil {
		return nil, provider.Invalid("Invalid request body")
	}
	return value, nil
}

func (p *Provider) createQueue(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	body, err := parseBody(request.JSON)
	if err != nil {
		return nil, err
	}
	record, err := newQueue(request, body)
	if err != nil {
		return nil, err
	}
	var document map[string]any
	err = p.scopes.With(scope, func(s *scopeState) error {
		if s.queues[record.Name] != nil {
			return statusExists("Queue " + record.Name + " already exists")
		}
		if err := p.index.Put(serviceName, state.Resource{
			Name: queueResource(scope.Project, record.Location, record.Name), Service: serviceName,
			Kind: "queue", Parent: provider.ProjectResource(scope.Project), Scope: scope,
		}); err != nil {
			return fmt.Errorf("index the queue: %w", err)
		}
		s.queues[record.Name] = record
		document = queueDocument(scope.Project, record)
		return nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // a wire status or the wrapped index error
	}
	return &gateway.RESTResponse{JSON: document}, nil
}

// newQueue validates one queue creation body into its record.
func newQueue(request *gateway.RESTRequest, body map[string]any) (*queueRecord, error) {
	id := provider.LastSegment(provider.Text(body["name"]))
	if !queuePattern.MatchString(id) {
		return nil, provider.Invalid("Invalid queue name " + id)
	}
	rate, _ := body["rateLimits"].(map[string]any)
	retry, err := parseRetry(defaultRetry(), body["retryConfig"])
	if err != nil {
		return nil, err
	}
	return &queueRecord{Name: id, Location: request.Path["location"], Rate: mergeRates(nil, rate), Retry: retry, State: "RUNNING"}, nil
}

// withQueue runs one mutation or read of an existing queue under the scope
// lock and renders the resulting queue document.
func (p *Provider) withQueue(scope state.Scope, id string, use func(*scopeState, *queueRecord) error) (*gateway.RESTResponse, error) {
	var document map[string]any
	err := p.scopes.With(scope, func(s *scopeState) error {
		record := s.queues[id]
		if record == nil {
			return provider.NotFound("queues", id)
		}
		if err := use(s, record); err != nil {
			return err
		}
		document = queueDocument(scope.Project, record)
		return nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // status errors carry the wire status
	}
	return &gateway.RESTResponse{JSON: document}, nil
}

// readQueue makes no change.
func readQueue(*scopeState, *queueRecord) error { return nil }

func (p *Provider) getQueue(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	return p.withQueue(scope, request.Path["queue"], readQueue)
}

// listQueues returns the project's queues.
func (p *Provider) listQueues(scope state.Scope) (*gateway.RESTResponse, error) {
	var items []any
	_ = p.scopes.With(scope, func(s *scopeState) error {
		names := make([]string, 0, len(s.queues))
		for name := range s.queues {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			items = append(items, queueDocument(scope.Project, s.queues[name]))
		}
		return nil
	})
	if items == nil {
		return &gateway.RESTResponse{JSON: map[string]any{}}, nil
	}
	return &gateway.RESTResponse{JSON: map[string]any{"queues": items}}, nil
}

// removeTasks deletes every task of one queue and returns their keys; call
// it under the scope lock.
func removeTasks(s *scopeState, queue string) []string {
	var keys []string
	for key, task := range s.tasks {
		if task.Queue == queue {
			delete(s.tasks, key)
			keys = append(keys, key)
		}
	}
	return keys
}

// deleteQueue removes one queue, its tasks, and its policy. GCP returns an
// empty body.
func (p *Provider) deleteQueue(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	id := request.Path["queue"]
	name := queueResource(scope.Project, request.Path["location"], id)
	var removed []string
	err := p.scopes.With(scope, func(s *scopeState) error {
		if s.queues[id] == nil {
			return provider.NotFound("queues", id)
		}
		if err := p.index.Remove(serviceName, scope, name); err != nil {
			return fmt.Errorf("unindex the queue: %w", err)
		}
		delete(s.queues, id)
		removed = removeTasks(s, id)
		return nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // a wire status or the wrapped index error
	}
	for _, key := range removed {
		p.stopWorker(scope, key)
	}
	p.engine.Forget(name)
	return &gateway.RESTResponse{JSON: map[string]any{}}, nil
}

// setQueueState pauses or resumes one queue.
func (p *Provider) setQueueState(request *gateway.RESTRequest, scope state.Scope, state string) (*gateway.RESTResponse, error) {
	return p.withQueue(scope, request.Path["queue"], func(_ *scopeState, record *queueRecord) error {
		record.State = state
		return nil
	})
}

// purgeQueue removes every task of one queue.
func (p *Provider) purgeQueue(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	var removed []string
	response, err := p.withQueue(scope, request.Path["queue"], func(s *scopeState, record *queueRecord) error {
		removed = removeTasks(s, record.Name)
		return nil
	})
	for _, key := range removed {
		p.stopWorker(scope, key)
	}
	return response, err
}

// getQueueIam returns the stored queue policy.
func (p *Provider) getQueueIam(scope state.Scope, request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	if _, err := p.getQueue(request, scope); err != nil {
		return nil, err
	}
	policy, err := p.engine.GetPolicy(queueResource(scope.Project, request.Path["location"], request.Path["queue"]))
	if err != nil {
		return nil, fmt.Errorf("read queue policy: %w", err)
	}
	return &gateway.RESTResponse{JSON: policy.Wire()}, nil
}

// setQueueIam stores one unconditional version-1 queue policy.
func (p *Provider) setQueueIam(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	var envelope struct {
		Policy iam.Policy `json:"policy"`
	}
	if json.Unmarshal(request.JSON, &envelope) != nil {
		return nil, provider.Invalid("Invalid IAM policy")
	}
	if _, err := p.getQueue(request, scope); err != nil {
		return nil, err
	}
	policy, err := p.engine.SetPolicy(queueResource(scope.Project, request.Path["location"], request.Path["queue"]), envelope.Policy)
	if err != nil {
		return nil, fmt.Errorf("write queue policy: %w", err)
	}
	return &gateway.RESTResponse{JSON: policy.Wire()}, nil
}

// updateQueue patches one queue's rate limits and retry configuration.
func (p *Provider) updateQueue(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	body, err := parseBody(request.JSON)
	if err != nil {
		return nil, err
	}
	return p.withQueue(scope, request.Path["queue"], func(_ *scopeState, record *queueRecord) error {
		retry, err := parseRetry(record.Retry, body["retryConfig"])
		if err != nil {
			return err
		}
		if rate, ok := body["rateLimits"].(map[string]any); ok {
			record.Rate = mergeRates(record.Rate, rate)
		}
		record.Retry = retry
		return nil
	})
}
