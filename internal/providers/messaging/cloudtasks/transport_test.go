package cloudtasks

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Resource names the parity steps use.
const (
	parentName    = "projects/" + testProject + "/locations/" + testLocation
	testQueueName = parentName + "/queues/" + testQueue
	testTaskName  = testQueueName + "/tasks/job-1"
)

// tasksConn serves the harness gateway over an in-memory gRPC listener.
func tasksConn(t *testing.T, h *harness) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := h.gate.NewGRPCServer()
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// bearerContext carries one principal's token; an empty token is anonymous.
func bearerContext(token string) context.Context {
	if token == "" {
		return context.Background()
	}
	return metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+token)
}

// restStatus is the REST status each gRPC code renders as.
var restStatus = map[codes.Code]int{
	codes.OK: 200, codes.InvalidArgument: 400, codes.Unauthenticated: 401, codes.PermissionDenied: 403,
	codes.NotFound: 404, codes.AlreadyExists: 409, codes.Unimplemented: 501,
}

// tasksRPC is one gRPC call of a parity step.
type tasksRPC = func(context.Context, cloudtaskspb.CloudTasksClient) (proto.Message, error)

// taskStep is one operation sent over REST to one provider and over gRPC
// (v2) to an identical second provider.
type taskStep struct {
	name       string
	verb, path string
	body       any
	token      string
	code       codes.Code
	message    string // the exact refusal, when the step is a coverage gap
	rpc        tasksRPC
	response   func() proto.Message
}

// TestRESTAndGRPCShareResults drives one queue and task lifecycle through
// v2beta3 REST and v2 gRPC and requires identical documents, codes, and
// messages for every field v2 carries.
func TestRESTAndGRPCShareResults(t *testing.T) {
	stamp := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	rest, rpc := newHarness(t), newHarness(t)
	rest.provider.clock = func() time.Time { return stamp }
	rpc.provider.clock = rest.provider.clock
	client := cloudtaskspb.NewCloudTasksClient(tasksConn(t, rpc))
	for _, step := range append(queueSteps(), taskSteps()...) {
		recorder := rest.invokeAs(step.token, step.verb, step.path, step.body)
		got, err := step.rpc(bearerContext(step.token), client)
		requireSameTaskResult(t, step, recorder, got, err)
	}
}

// requireSameTaskResult compares one step's REST reply with its gRPC result.
func requireSameTaskResult(t *testing.T, step taskStep, recorder *httptest.ResponseRecorder, got proto.Message, err error) {
	t.Helper()
	if code := status.Code(err); code != step.code || recorder.Code != restStatus[step.code] {
		t.Fatalf("%s: gRPC %v, REST %d %s", step.name, err, recorder.Code, recorder.Body.String())
	}
	if err != nil {
		var body struct {
			Error struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		_ = json.Unmarshal(recorder.Body.Bytes(), &body)
		if status.Convert(err).Message() != body.Error.Message || step.message != "" && body.Error.Message != step.message {
			t.Fatalf("%s: gRPC message %q, REST %q", step.name, status.Convert(err).Message(), body.Error.Message)
		}
		return
	}
	want := step.response()
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(recorder.Body.Bytes(), want); err != nil {
		t.Fatalf("%s: decode REST %s: %v", step.name, recorder.Body.String(), err)
	}
	if !proto.Equal(want, got) {
		t.Fatalf("%s:\nREST %v\ngRPC %v", step.name, want, got)
	}
}

func queueDoc() proto.Message  { return &cloudtaskspb.Queue{} }
func taskDoc() proto.Message   { return &cloudtaskspb.Task{} }
func policyDoc() proto.Message { return &iampb.Policy{} }
func emptyDoc() proto.Message  { return &emptypb.Empty{} }

// later schedules the parity task far ahead so no attempt changes it
// between the two runs.
var later = time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)

var createQueueRPC tasksRPC = func(ctx context.Context, c cloudtaskspb.CloudTasksClient) (proto.Message, error) {
	return c.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{Parent: parentName, Queue: &cloudtaskspb.Queue{
		Name: testQueueName, RateLimits: &cloudtaskspb.RateLimits{MaxDispatchesPerSecond: 5},
		RetryConfig: &cloudtaskspb.RetryConfig{MaxAttempts: 7, MinBackoff: durationpb.New(time.Second)},
	}})
}

var createTaskRPC tasksRPC = func(ctx context.Context, c cloudtaskspb.CloudTasksClient) (proto.Message, error) {
	return c.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{Parent: testQueueName, Task: &cloudtaskspb.Task{
		Name: testTaskName, ScheduleTime: timestamppb.New(later),
		MessageType: &cloudtaskspb.Task_HttpRequest{HttpRequest: &cloudtaskspb.HttpRequest{
			Url: testUnreached, HttpMethod: cloudtaskspb.HttpMethod_PUT, Body: []byte("payload"), Headers: map[string]string{"X-Job": "1"},
		}},
	}})
}

func getTaskRPC(view cloudtaskspb.Task_View) tasksRPC {
	return func(ctx context.Context, c cloudtaskspb.CloudTasksClient) (proto.Message, error) {
		return c.GetTask(ctx, &cloudtaskspb.GetTaskRequest{Name: testTaskName, ResponseView: view})
	}
}

// queueRPC gets, pauses, resumes, purges, or deletes the queue.
func queueRPC(verb string) tasksRPC {
	return func(ctx context.Context, c cloudtaskspb.CloudTasksClient) (proto.Message, error) {
		switch verb {
		case "pause":
			return c.PauseQueue(ctx, &cloudtaskspb.PauseQueueRequest{Name: testQueueName})
		case "resume":
			return c.ResumeQueue(ctx, &cloudtaskspb.ResumeQueueRequest{Name: testQueueName})
		case "purge":
			return c.PurgeQueue(ctx, &cloudtaskspb.PurgeQueueRequest{Name: testQueueName})
		case "delete":
			return c.DeleteQueue(ctx, &cloudtaskspb.DeleteQueueRequest{Name: testQueueName})
		default:
			return c.GetQueue(ctx, &cloudtaskspb.GetQueueRequest{Name: testQueueName})
		}
	}
}

// queueSteps covers the queue methods and the queue policy.
func queueSteps() []taskStep {
	queueBody := map[string]any{"name": testQueueName, "rateLimits": map[string]any{"maxDispatchesPerSecond": 5}, "retryConfig": map[string]any{"maxAttempts": 7, "minBackoff": "1s"}}
	enqueuer := "serviceAccount:" + email("enqueuer")
	return []taskStep{
		{"create queue", http.MethodPost, "/v2beta3/" + parentName + "/queues", queueBody, "actor", codes.OK, "", createQueueRPC, queueDoc},
		{"create queue again", http.MethodPost, "/v2beta3/" + parentName + "/queues", queueBody, "actor", codes.AlreadyExists, "", createQueueRPC, queueDoc},
		{"get queue", http.MethodGet, queuePath, nil, "actor", codes.OK, "", queueRPC("get"), queueDoc},
		{"anonymous get", http.MethodGet, queuePath, nil, "", codes.Unauthenticated, "", queueRPC("get"), queueDoc},
		{"list queues", http.MethodGet, "/v2beta3/" + parentName + "/queues", nil, "actor", codes.OK, "", func(ctx context.Context, c cloudtaskspb.CloudTasksClient) (proto.Message, error) {
			return c.ListQueues(ctx, &cloudtaskspb.ListQueuesRequest{Parent: parentName})
		}, func() proto.Message { return &cloudtaskspb.ListQueuesResponse{} }},
		{"list queues with filter", http.MethodGet, "/v2beta3/" + parentName + "/queues?filter=state%3APAUSED", nil, "actor", codes.Unimplemented,
			"Cloud Tasks coverage gap: queue list filters are not implemented", func(ctx context.Context, c cloudtaskspb.CloudTasksClient) (proto.Message, error) {
				return c.ListQueues(ctx, &cloudtaskspb.ListQueuesRequest{Parent: parentName, Filter: "state:PAUSED"})
			}, nil},
		{"update queue", http.MethodPatch, queuePath + "?updateMask=retryConfig.maxAttempts", map[string]any{"name": testQueueName, "retryConfig": map[string]any{"maxAttempts": 9}}, "actor", codes.OK, "",
			func(ctx context.Context, c cloudtaskspb.CloudTasksClient) (proto.Message, error) {
				return c.UpdateQueue(ctx, &cloudtaskspb.UpdateQueueRequest{Queue: &cloudtaskspb.Queue{Name: testQueueName, RetryConfig: &cloudtaskspb.RetryConfig{MaxAttempts: 9}}})
			}, queueDoc},
		{"pause", http.MethodPost, queuePath + ":pause", nil, "actor", codes.OK, "", queueRPC("pause"), queueDoc},
		{"resume", http.MethodPost, queuePath + ":resume", nil, "actor", codes.OK, "", queueRPC("resume"), queueDoc},
		{"grant enqueuer", http.MethodPost, queuePath + ":setIamPolicy", map[string]any{"policy": map[string]any{"bindings": []any{map[string]any{
			"role": "roles/cloudtasks.enqueuer", "members": []any{enqueuer},
		}}}}, "actor", codes.OK, "", func(ctx context.Context, c cloudtaskspb.CloudTasksClient) (proto.Message, error) {
			return c.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{Resource: testQueueName, Policy: &iampb.Policy{Bindings: []*iampb.Binding{{Role: "roles/cloudtasks.enqueuer", Members: []string{enqueuer}}}}})
		}, policyDoc},
		{"read policy", http.MethodPost, queuePath + ":getIamPolicy", nil, "actor", codes.OK, "", func(ctx context.Context, c cloudtaskspb.CloudTasksClient) (proto.Message, error) {
			return c.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: testQueueName})
		}, policyDoc},
	}
}

// taskSteps covers the task methods, the FULL view, and deletion.
func taskSteps() []taskStep {
	taskBody := map[string]any{"task": map[string]any{"name": testTaskName, "scheduleTime": later.Format(time.RFC3339), "httpRequest": map[string]any{
		"url": testUnreached, "httpMethod": "PUT", "body": "cGF5bG9hZA==", "headers": map[string]any{"X-Job": "1"},
	}}}
	tasks := queuePath + "/tasks"
	return []taskStep{
		{"denied enqueue", http.MethodPost, tasks, taskBody, "outsider", codes.PermissionDenied, "", createTaskRPC, taskDoc},
		{"enqueue", http.MethodPost, tasks, taskBody, "enqueuer", codes.OK, "", createTaskRPC, taskDoc},
		{"enqueue again", http.MethodPost, tasks, taskBody, "enqueuer", codes.AlreadyExists, "", createTaskRPC, taskDoc},
		{"App Engine task", http.MethodPost, tasks, map[string]any{"task": map[string]any{"appEngineHttpRequest": map[string]any{"relativeUri": "/work"}}}, "actor", codes.Unimplemented,
			"Cloud Tasks coverage gap: only HTTP target tasks are supported", func(ctx context.Context, c cloudtaskspb.CloudTasksClient) (proto.Message, error) {
				return c.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{Parent: testQueueName, Task: &cloudtaskspb.Task{
					MessageType: &cloudtaskspb.Task_AppEngineHttpRequest{AppEngineHttpRequest: &cloudtaskspb.AppEngineHttpRequest{RelativeUri: "/work"}},
				}})
			}, nil},
		{"get task", http.MethodGet, tasks + "/job-1", nil, "actor", codes.OK, "", getTaskRPC(cloudtaskspb.Task_VIEW_UNSPECIFIED), taskDoc},
		{"get full task", http.MethodGet, tasks + "/job-1?responseView=FULL", nil, "actor", codes.OK, "", getTaskRPC(cloudtaskspb.Task_FULL), taskDoc},
		{"enqueuer full view", http.MethodGet, tasks + "/job-1?responseView=FULL", nil, "enqueuer", codes.PermissionDenied, "", getTaskRPC(cloudtaskspb.Task_FULL), taskDoc},
		{"list tasks", http.MethodGet, tasks, nil, "actor", codes.OK, "", func(ctx context.Context, c cloudtaskspb.CloudTasksClient) (proto.Message, error) {
			return c.ListTasks(ctx, &cloudtaskspb.ListTasksRequest{Parent: testQueueName})
		}, func() proto.Message { return &cloudtaskspb.ListTasksResponse{} }},
		{"delete task", http.MethodDelete, tasks + "/job-1", nil, "actor", codes.OK, "", func(ctx context.Context, c cloudtaskspb.CloudTasksClient) (proto.Message, error) {
			return c.DeleteTask(ctx, &cloudtaskspb.DeleteTaskRequest{Name: testTaskName})
		}, emptyDoc},
		{"run deleted task", http.MethodPost, tasks + "/job-1:run", nil, "actor", codes.NotFound, "", func(ctx context.Context, c cloudtaskspb.CloudTasksClient) (proto.Message, error) {
			return c.RunTask(ctx, &cloudtaskspb.RunTaskRequest{Name: testTaskName})
		}, taskDoc},
		{"purge", http.MethodPost, queuePath + ":purge", nil, "actor", codes.OK, "", queueRPC("purge"), queueDoc},
		{"delete queue", http.MethodDelete, queuePath, nil, "actor", codes.OK, "", queueRPC("delete"), emptyDoc},
		{"get deleted queue", http.MethodGet, queuePath, nil, "actor", codes.NotFound, "", queueRPC("get"), queueDoc},
	}
}

// TestGRPCRefusesUnservedMethods pins the explicit gap of every v2 method
// without a REST provider method.
func TestGRPCRefusesUnservedMethods(t *testing.T) {
	h := newHarness(t)
	client := cloudtaskspb.NewCloudTasksClient(tasksConn(t, h))
	ctx := bearerContext("actor")
	cmek := parentName + "/cmekConfig"
	calls := map[string]func() *status.Status{
		"TestIamPermissions": func() *status.Status {
			_, err := client.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{Resource: testQueueName})
			return status.Convert(err)
		},
		"BatchCreateTasks": func() *status.Status {
			_, err := client.BatchCreateTasks(ctx, &cloudtaskspb.BatchCreateTasksRequest{Parent: testQueueName})
			return status.Convert(err)
		},
		"BatchDeleteTasks": func() *status.Status {
			_, err := client.BatchDeleteTasks(ctx, &cloudtaskspb.BatchDeleteTasksRequest{Parent: testQueueName})
			return status.Convert(err)
		},
		"GetCmekConfig": func() *status.Status {
			_, err := client.GetCmekConfig(ctx, &cloudtaskspb.GetCmekConfigRequest{Name: cmek})
			return status.Convert(err)
		},
		"UpdateCmekConfig": func() *status.Status {
			_, err := client.UpdateCmekConfig(ctx, &cloudtaskspb.UpdateCmekConfigRequest{CmekConfig: &cloudtaskspb.CmekConfig{Name: cmek}})
			return status.Convert(err)
		},
	}
	for method, call := range calls {
		want := "No provider implements /google.cloud.tasks.v2.CloudTasks/" + method
		if s := call(); s.Code() != codes.Unimplemented || s.Message() != want {
			t.Fatalf("%s = %v, want UNIMPLEMENTED %q", method, s, want)
		}
	}
}
