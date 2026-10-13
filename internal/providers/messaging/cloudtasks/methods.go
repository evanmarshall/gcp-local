package cloudtasks

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	taskspb "cloud.google.com/go/cloudtasks/apiv2beta3/cloudtaskspb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
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
		return listLocations(request)
	}
	return nil, gap("this method is not implemented")
}

// locationsPaging pages the one served location.
var locationsPaging = provider.Paging{Default: 100, Max: 100}

// listLocations lists the one location the provider serves.
func listLocations(request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	items := []map[string]any{{keyName: "projects/" + request.Path["project"] + "/locations/us-central1", "locationId": "us-central1"}}
	page, next, err := provider.Page(items, request.Query, locationsPaging, gap)
	if err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: provider.ListBody("locations", page, next)}, nil
}

// executeQueue dispatches the queue methods. readMask, which selects the
// queue's stats, is a gap.
func (p *Provider) executeQueue(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	if request.Query.Get("readMask") != "" {
		return nil, gap("the readMask option is not implemented")
	}
	switch call.Method {
	case methodQueuesCreate:
		return p.createQueue(request, scope)
	case methodQueuesGet:
		return p.withQueue(scope, queueKey(request), readQueue)
	case methodQueuesList:
		return p.listQueues(request, scope)
	case methodQueuesDelete:
		return p.deleteQueue(request, scope)
	case methodQueuesUpdate:
		return p.updateQueue(request, scope)
	case methodQueuesGetIam:
		return p.getQueueIam(call, request, scope)
	case methodQueuesSetIam:
		return p.setQueueIam(call, request, scope)
	default:
		return p.executeQueueState(call, request, scope)
	}
}

// queueStateBodies are the request messages of pause, resume, and purge,
// whose bodies bind the whole request; each holds only the queue name.
var queueStateBodies = map[string]func() proto.Message{
	methodQueuesPause:  func() proto.Message { return &taskspb.PauseQueueRequest{} },
	methodQueuesResume: func() proto.Message { return &taskspb.ResumeQueueRequest{} },
	methodQueuesPurge:  func() proto.Message { return &taskspb.PurgeQueueRequest{} },
}

// executeQueueState dispatches the pause/resume/purge state family.
func (p *Provider) executeQueueState(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	body, ok := queueStateBodies[call.Method]
	if !ok {
		return nil, gap("this method is not implemented")
	}
	if err := decodeRequest(request.JSON, body()); err != nil {
		return nil, err
	}
	switch call.Method {
	case methodQueuesPause:
		return p.setQueueState(request, scope, taskspb.Queue_PAUSED)
	case methodQueuesResume:
		return p.setQueueState(request, scope, taskspb.Queue_RUNNING)
	default:
		return p.purgeQueue(request, scope)
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
		return p.runTask(call, request, scope)
	default:
		return nil, gap("this method is not implemented")
	}
}

// queueKey addresses one queue in its project scope; queue IDs are unique
// within a location.
func queueKey(request *gateway.RESTRequest) string {
	return request.Path["location"] + "/" + request.Path["queue"]
}

// queueDocument renders the queue wire shape; call it under the scope lock.
func queueDocument(record *queueRecord) map[string]any {
	return render(record.queue)
}

// createQueue validates, stores, and indexes one queue.
func (p *Provider) createQueue(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	queue, err := decodeQueue(request.JSON)
	if err != nil {
		return nil, err
	}
	location := request.Path["location"]
	id := provider.LastSegment(queue.GetName())
	if !queuePattern.MatchString(id) {
		return nil, provider.Invalid("Invalid queue name " + id)
	}
	if err := normalizeQueue(queue); err != nil {
		return nil, err
	}
	queue.Name = queueName(scope.Project, location, id)
	queue.State, queue.PurgeTime, queue.Stats = taskspb.Queue_RUNNING, nil, nil
	key := location + "/" + id
	var document map[string]any
	err = p.scopes.With(scope, func(s *scopeState) error {
		if s.queues[key] != nil {
			return statusExists("Queue " + queue.GetName() + " already exists")
		}
		if err := p.index.Put(serviceName, state.Resource{
			Name: queueResource(scope.Project, location, id), Service: serviceName,
			Kind: "queue", Parent: provider.ProjectResource(scope.Project), Scope: scope,
		}); err != nil {
			return fmt.Errorf("index the queue: %w", err)
		}
		record := &queueRecord{queue: queue}
		s.queues[key] = record
		document = queueDocument(record)
		return nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // a wire status or the wrapped index error
	}
	return &gateway.RESTResponse{JSON: document}, nil
}

// withQueue runs one mutation or read of an existing queue under the scope
// lock and renders the resulting queue document.
func (p *Provider) withQueue(scope state.Scope, key string, use func(*scopeState, *queueRecord) error) (*gateway.RESTResponse, error) {
	var document map[string]any
	err := p.scopes.With(scope, func(s *scopeState) error {
		record := s.queues[key]
		if record == nil {
			return provider.NotFound("queues", provider.LastSegment(key))
		}
		if err := use(s, record); err != nil {
			return err
		}
		document = queueDocument(record)
		return nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // status errors carry the wire status
	}
	return &gateway.RESTResponse{JSON: document}, nil
}

// readQueue makes no change.
func readQueue(*scopeState, *queueRecord) error { return nil }

// queuesPaging is ListQueues' documented page sizes (maximum 9800, the
// default when unset) and recorded refusals.
var queuesPaging = provider.Paging{Default: 9800, Max: 9800, InvalidToken: provider.Invalid("Invalid page_token"),
	NegativeSize: provider.Invalid("ListQueuesRequest.pageSize cannot be negative.")}

// listQueues returns one page of the location's queues in name order. A
// filter is a gap.
func (p *Provider) listQueues(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	var body map[string]any
	err := p.scopes.With(scope, func(s *scopeState) error {
		prefix := request.Path["location"] + "/"
		keys := slices.Sorted(maps.Keys(s.queues))
		keys = slices.DeleteFunc(keys, func(key string) bool { return !strings.HasPrefix(key, prefix) })
		page, next, err := provider.Page(keys, request.Query, queuesPaging, gap)
		if err != nil {
			return err
		}
		items := make([]map[string]any, 0, len(page))
		for _, key := range page {
			items = append(items, queueDocument(s.queues[key]))
		}
		body = provider.ListBody("queues", items, next)
		return nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // wire statuses
	}
	return &gateway.RESTResponse{JSON: body}, nil
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

// deleteQueue removes one queue, its tasks and tombstones, and its policy.
// GCP returns an empty body.
func (p *Provider) deleteQueue(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	key := queueKey(request)
	var removed []string
	err := p.scopes.With(scope, func(s *scopeState) error {
		if s.queues[key] == nil {
			return provider.NotFound("queues", request.Path["queue"])
		}
		// Removing the index entry also drops the queue's policy.
		if err := p.index.Remove(serviceName, scope, queueResource(scope.Project, request.Path["location"], request.Path["queue"])); err != nil {
			return fmt.Errorf("unindex the queue: %w", err)
		}
		delete(s.queues, key)
		removed = removeTasks(s, key)
		for tombstone := range s.tombstones {
			if strings.HasPrefix(tombstone, key+"/") {
				delete(s.tombstones, tombstone)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // a wire status or the wrapped index error
	}
	for _, key := range removed {
		p.stopWorker(scope, key)
	}
	return &gateway.RESTResponse{JSON: map[string]any{}}, nil
}

// setQueueState pauses or resumes one queue.
func (p *Provider) setQueueState(request *gateway.RESTRequest, scope state.Scope, value taskspb.Queue_State) (*gateway.RESTResponse, error) {
	return p.withQueue(scope, queueKey(request), func(_ *scopeState, record *queueRecord) error {
		record.queue.State = value
		return nil
	})
}

// purgeQueue removes every task of one queue, tombstoning each, and records
// the purge time.
func (p *Provider) purgeQueue(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	var removed []string
	key := queueKey(request)
	response, err := p.withQueue(scope, key, func(s *scopeState, record *queueRecord) error {
		now := p.clock()
		for task, value := range s.tasks {
			if value.Queue == key {
				s.bury(task, now)
				removed = append(removed, task)
			}
		}
		record.queue.PurgeTime = timestamppb.New(now)
		return nil
	})
	for _, task := range removed {
		p.stopWorker(scope, task)
	}
	return response, err
}

// getQueueIam reads the queue's policy.
func (p *Provider) getQueueIam(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	if err := decodeRequest(request.JSON, &iampb.GetIamPolicyRequest{}); err != nil {
		return nil, err
	}
	return p.queuePolicy(call, request, scope, p.engine.GetPolicy)
}

// setQueueIam stores one queue policy. Queues refuse conditional bindings,
// as GCP does (recording cloudtasks-dispatch-retry); audit configs and
// masks beyond bindings and etag are gaps.
func (p *Provider) setQueueIam(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	requested, err := provider.DecodePolicy(request.JSON, gap)
	if err != nil {
		return nil, err
	}
	for _, binding := range requested.Bindings {
		if len(binding.Condition) > 0 && string(binding.Condition) != "null" {
			return nil, provider.Invalid("Can't set conditional policy on this resource")
		}
	}
	return p.queuePolicy(call, request, scope, func(name string) (iam.Policy, error) {
		return p.engine.SetPolicy(name, requested)
	})
}

// queuePolicy checks the queue exists, authorizes the caller on it, and
// reads or writes its policy under one scope lock. Queue delete removes the
// index entry (and so the policy) under that lock, so a policy call racing a
// delete reads NOT_FOUND and never touches a recreated queue's policy.
func (p *Provider) queuePolicy(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, change func(string) (iam.Policy, error)) (*gateway.RESTResponse, error) {
	location, queue := request.Path["location"], request.Path["queue"]
	name := queueResource(scope.Project, location, queue)
	var response *gateway.RESTResponse
	err := p.scopes.With(scope, func(s *scopeState) error {
		if s.queues[queueKey(request)] == nil {
			return provider.NotFound("queues", queue)
		}
		if _, err := p.gate.Check(call, iam.Target{Name: name}, queueName(scope.Project, location, queue)); err != nil {
			return err
		}
		policy, err := change(name)
		if err != nil {
			return fmt.Errorf("change the queue policy: %w", err)
		}
		response = &gateway.RESTResponse{JSON: policy.Wire()}
		return nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // status errors carry the wire status
	}
	return response, nil
}

// updateQueue replaces one queue (no updateMask) or the fields its
// updateMask names, then fills the defaults again and validates the whole
// result before applying it. GCP creates a missing queue here; that is a gap.
func (p *Provider) updateQueue(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	update, err := decodeQueue(request.JSON)
	if err != nil {
		return nil, err
	}
	mask := request.Query.Get("updateMask")
	var paths [][]protoreflect.FieldDescriptor
	if mask != "" {
		if paths, err = provider.MaskPaths(mask, update, queueOutputNames); err != nil {
			return nil, err
		}
	}
	var document map[string]any
	err = p.scopes.With(scope, func(s *scopeState) error {
		record := s.queues[queueKey(request)]
		if record == nil {
			return gap("creating a queue through queues.patch is not implemented")
		}
		merged := proto.CloneOf(update)
		if paths != nil {
			merged = provider.ApplyMask(record.queue, update, paths)
		}
		if err := normalizeQueue(merged); err != nil {
			return err
		}
		merged.Name, merged.State, merged.PurgeTime = record.queue.GetName(), record.queue.GetState(), record.queue.GetPurgeTime()
		record.queue = merged
		document = queueDocument(record)
		return nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // wire statuses
	}
	return &gateway.RESTResponse{JSON: document}, nil
}

// queueOutputNames are queueOutput as proto names, for update masks.
var queueOutputNames = map[protoreflect.Name]bool{keyName: true, "state": true, "purge_time": true, "stats": true}
