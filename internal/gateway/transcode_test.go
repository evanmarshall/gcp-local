package gateway

import (
	"context"
	"errors"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/evanmarshall/gcp-local/api/generated"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// restCapture is what one transcoded call delivered to its REST handler.
type restCapture struct {
	call    Call
	request *RESTRequest
}

// capturingGateway registers one REST handler that records its input and
// answers with response, and serves the gateway over gRPC.
func capturingGateway(t *testing.T, method string, response any) (*Gateway, <-chan restCapture) {
	t.Helper()
	r := NewRegistry()
	calls := make(chan restCapture, 1)
	register(t, r, method, func(call Call, input any) (any, error) {
		request, _ := input.(*RESTRequest)
		calls <- restCapture{call, request}
		return &RESTResponse{JSON: response}, nil
	})
	return New(r, nil), calls
}

// TestTranscodedCreateSplitsBodyAndQuery pins the REST request a CreateSecret
// call becomes: the body field as JSON, the other fields as query, the
// resource as path variables, and no project on the REST call.
func TestTranscodedCreateSplitsBodyAndQuery(t *testing.T) {
	g, calls := capturingGateway(t, "secretmanager.secrets.create", map[string]any{"name": "projects/p/secrets/s", "unknownField": true})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	secret, err := secretmanagerpb.NewSecretManagerServiceClient(grpcClient(t, g)).CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent: "projects/p", SecretId: "s", Secret: &secretmanagerpb.Secret{Labels: map[string]string{"k": "v"}},
	})
	if err != nil || secret.GetName() != "projects/p/secrets/s" {
		t.Fatalf("create = %v, %v", secret, err)
	}
	got := <-calls
	if got.call.Method != "secretmanager.secrets.create" || got.call.Resource != "/v1/projects/p/secrets" || got.call.Project != "" ||
		!reflect.DeepEqual(got.request.Path, map[string]string{"project": "p"}) ||
		!reflect.DeepEqual(got.request.Query, url.Values{"secretId": {"s"}}) || string(got.request.JSON) != `{"labels":{"k":"v"}}` {
		t.Fatalf("create reached the provider as %+v %+v", got.call, got.request)
	}
}

// TestTranscodedTasksV2LandOnV2beta3 pins the version rewrite and query
// rendering of a Cloud Tasks v2 GetTask call.
func TestTranscodedTasksV2LandOnV2beta3(t *testing.T) {
	g, calls := capturingGateway(t, "cloudtasks.tasks.get", map[string]any{"name": "projects/p/locations/l/queues/q/tasks/t", "view": "FULL"})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	task, err := cloudtaskspb.NewCloudTasksClient(grpcClient(t, g)).GetTask(ctx, &cloudtaskspb.GetTaskRequest{
		Name: "projects/p/locations/l/queues/q/tasks/t", ResponseView: cloudtaskspb.Task_FULL,
	})
	if err != nil || task.GetView() != cloudtaskspb.Task_FULL {
		t.Fatalf("get task = %v, %v", task, err)
	}
	got := <-calls
	if got.call.Resource != "/v2beta3/projects/p/locations/l/queues/q/tasks/t" || got.request.JSON != nil ||
		!reflect.DeepEqual(got.request.Path, map[string]string{"project": "p", "location": "l", "queue": "q", "task": "t"}) ||
		!reflect.DeepEqual(got.request.Query, url.Values{"responseView": {"FULL"}}) {
		t.Fatalf("get task reached the provider as %+v %+v", got.call, got.request)
	}
}

// TestTranscodingFailsClosed covers every response the gRPC method cannot
// carry, the 401 mapping, and a service this server was not started with.
func TestTranscodingFailsClosed(t *testing.T) {
	r := NewRegistry()
	responses := map[string]any{
		"stream":         &RESTResponse{Body: strings.NewReader("raw")},
		"wrong-type":     "not a response",
		"unknown-status": &RESTResponse{Status: 418, JSON: map[string]any{"error": map[string]any{"status": "TEAPOT", "message": "m"}}},
		"not-a-secret":   &RESTResponse{JSON: map[string]any{"name": 7}},
		"anonymous":      &RESTResponse{Status: 401, JSON: map[string]any{"error": map[string]any{"status": "PERMISSION_DENIED", "message": "Anonymous caller"}}},
		"conflict":       &RESTResponse{Status: 409, JSON: map[string]any{"error": map[string]any{"status": "ALREADY_EXISTS", "message": "exists"}}},
	}
	register(t, r, "secretmanager.secrets.get", func(_ Call, input any) (any, error) {
		request, _ := input.(*RESTRequest)
		return responses[request.Path["secret"]], nil
	})
	g := New(r, nil)
	client := secretmanagerpb.NewSecretManagerServiceClient(grpcClient(t, g))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	const untranscodable = "The gRPC method does not fit its REST provider method"
	want := map[string]*status.Status{
		"stream":         status.New(codes.Internal, untranscodable),
		"wrong-type":     status.New(codes.Internal, untranscodable),
		"unknown-status": status.New(codes.Internal, untranscodable),
		"not-a-secret":   status.New(codes.Internal, untranscodable),
		"anonymous":      status.New(codes.Unauthenticated, "Anonymous caller"),
		"conflict":       status.New(codes.AlreadyExists, "exists"),
	}
	for name, expected := range want {
		_, err := client.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: "projects/p/secrets/" + name})
		if s := status.Convert(err); s.Code() != expected.Code() || s.Message() != expected.Message() {
			t.Fatalf("%s = %v, want %v", name, s, expected)
		}
	}
	// A binding whose REST route is missing never reaches a provider.
	binding := generated.Transcoding{RPC: "/google.cloud.secretmanager.v1.SecretManagerService/GetSecret", REST: "secretmanager.secrets.missing",
		Verb: "GET", Path: "/v1/{name=projects/*/secrets/*}", Field: "name"}
	if _, err := g.transcode(ctx, binding, &secretmanagerpb.GetSecretRequest{Name: "projects/p/secrets/s"}); !errors.Is(err, errUntranscodable) {
		t.Fatalf("unrouted binding = %v", err)
	}
	// Cloud Tasks has no handler on this server.
	_, err := cloudtaskspb.NewCloudTasksClient(grpcClient(t, g)).GetQueue(ctx, &cloudtaskspb.GetQueueRequest{Name: "projects/p/locations/l/queues/q"})
	if s := status.Convert(err); s.Code() != codes.Unimplemented || s.Message() != "Service cloudtasks is not enabled on this server; add cloudtasks to -services" {
		t.Fatalf("disabled service = %v", s)
	}
}
