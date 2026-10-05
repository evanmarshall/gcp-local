package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func register(t *testing.T, r *Registry, method string, h Handler) {
	t.Helper()
	if err := r.Register(method, h); err != nil {
		t.Fatal(err)
	}
}
func request(t *testing.T, client *http.Client, method, url, body string, headers http.Header) (int, map[string]any, http.Header) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if headers != nil {
		req.Header = headers
	}
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result map[string]any
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, result, response.Header
}
func TestRESTDispatchPreservesEscapedResourcesAndIdentity(t *testing.T) {
	r := NewRegistry()
	calls := make(chan Call, 1)
	register(t, r, "storage.objects.get", func(call Call, input any) (any, error) {
		req := input.(*RESTRequest)
		if req.Path["bucket"] != "bucket" || req.Path["object"] != "folder/a%?.txt" || req.Headers.Get("Authorization") != "" {
			return nil, errors.New("bad decoded request")
		}
		calls <- call
		return &RESTResponse{JSON: map[string]any{"name": req.Path["object"]}}, nil
	})
	g := New(r, func(ctx context.Context, token string) (Principal, error) {
		if token != "fixture-token" {
			t.Error("wrong token")
		}
		return Principal{State: Authenticated, Subject: "fixture", Project: "caller-project"}, nil
	})
	server := httptest.NewServer(g)
	defer server.Close()
	trace := "1234567890abcdef1234567890abcdef"
	code, body, headers := request(t, server.Client(), "GET", server.URL+"/storage/v1/b/bucket/o/folder%2Fa%25%3F.txt?project=untrusted", "", http.Header{"Authorization": {"Bearer fixture-token"}, "Traceparent": {"00-" + trace + "-0123456789abcdef-01"}})
	if code != 200 || body["name"] != "folder/a%?.txt" || headers.Get("X-Gcp-Local-Trace-Id") != trace {
		t.Fatalf("%d %v %v", code, body, headers)
	}
	call := <-calls
	if call.Method != "storage.objects.get" || call.Project != "" || call.Location != "global" || call.TraceID != trace || call.Principal.Subject != "fixture" || call.Principal.Project != "caller-project" {
		t.Fatalf("%+v", call)
	}
}
func TestRESTValidationBeforeDispatchAndExplicitGaps(t *testing.T) {
	r := NewRegistry()
	var called atomic.Int32
	register(t, r, "storage.buckets.insert", func(call Call, input any) (any, error) {
		called.Add(1)
		if call.Project != "synthetic-project" {
			t.Errorf("wrong project: %q", call.Project)
		}
		return &RESTResponse{Status: 201, JSON: map[string]any{"name": "bucket"}}, nil
	})
	server := httptest.NewServer(New(r, nil))
	defer server.Close()
	for _, tc := range []struct {
		path, body string
		headers    http.Header
		code       int
	}{
		{"/storage/v1/b?project=synthetic-project", `{"name":"bucket"}`, nil, 201},
		{"/storage/v1/b", `{`, nil, 400}, {"/storage/v1/b", `[]`, nil, 400}, {"/storage/v1/b", `null`, nil, 400},
		{"/storage/v1/b?invalid=%zz", `{}`, nil, 400},
		{"/storage/v1/b", `{}`, http.Header{"Authorization": {"Bearer one", "Bearer two"}}, 401},
		{"/storage/v1/b", `{}`, http.Header{"Authorization": {"Basic one"}}, 401},
		{"/storage/v1/b/bucket/restore", `{}`, nil, 501},
		{"/_local/diagnostics", `{}`, nil, 501},
		{"/unknown", `{}`, nil, 501},
	} {
		code, _, _ := request(t, server.Client(), "POST", server.URL+tc.path, tc.body, tc.headers)
		if code != tc.code {
			t.Fatalf("%s %s: %d", tc.path, tc.body, code)
		}
	}
	if called.Load() != 1 {
		t.Fatalf("malformed/unsupported request reached provider: %d", called.Load())
	}
}
func TestDefaultIdentityNeverAuthenticatesPresentedToken(t *testing.T) {
	r := NewRegistry()
	register(t, r, "storage.buckets.list", func(call Call, input any) (any, error) {
		return &RESTResponse{JSON: map[string]any{"state": call.Principal.State, "subject": call.Principal.Subject}}, nil
	})
	server := httptest.NewServer(New(r, nil))
	defer server.Close()
	for _, tc := range []struct {
		auth  string
		state PrincipalState
	}{{"", Anonymous}, {"Bearer any-real-or-local-token", Unknown}} {
		headers := http.Header{}
		if tc.auth != "" {
			headers.Set("Authorization", tc.auth)
		}
		_, body, _ := request(t, server.Client(), "GET", server.URL+"/storage/v1/b", "", headers)
		if body["state"] != string(tc.state) || body["subject"] != "" {
			t.Fatal(body)
		}
	}
}
func permissionError() error {
	s, _ := status.New(codes.PermissionDenied, "Missing permission").WithDetails(&errdetails.ErrorInfo{Reason: "IAM_PERMISSION_DENIED", Domain: "gcp-local", Metadata: map[string]string{"permission": "storage.objects.get"}})
	return s.Err()
}
func TestRESTErrorsPreserveDetailsAndHideInternalFailures(t *testing.T) {
	for _, tc := range []struct {
		err     error
		code    int
		message string
	}{{permissionError(), 403, "Missing permission"}, {errors.New("secret backend token"), 500, "Internal provider error"}, {context.Canceled, 499, "context canceled"}} {
		r := NewRegistry()
		register(t, r, "storage.objects.get", func(Call, any) (any, error) { return nil, tc.err })
		server := httptest.NewServer(New(r, nil))
		code, body, _ := request(t, server.Client(), "GET", server.URL+"/storage/v1/b/bucket/o/object", "", nil)
		server.Close()
		detail := body["error"].(map[string]any)
		if code != tc.code || detail["message"] != tc.message {
			t.Fatalf("%d %v", code, body)
		}
		if code == 403 {
			details := detail["details"].([]any)
			if details[0].(map[string]any)["reason"] != "IAM_PERMISSION_DENIED" {
				t.Fatal(details)
			}
		}
	}
}
func TestRESTCancellationReachesProvider(t *testing.T) {
	r := NewRegistry()
	entered, done := make(chan struct{}), make(chan struct{})
	register(t, r, "storage.buckets.list", func(call Call, _ any) (any, error) {
		close(entered)
		<-call.Context.Done()
		close(done)
		return nil, call.Context.Err()
	})
	server := httptest.NewServer(New(r, nil))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/storage/v1/b", nil)
	completed := make(chan struct{})
	go func() {
		defer close(completed)
		resp, _ := server.Client().Do(req)
		if resp != nil {
			resp.Body.Close()
		}
	}()
	wait(t, entered)
	cancel()
	wait(t, done)
	wait(t, completed)
}
func grpcClient(t *testing.T, g *Gateway) *grpc.ClientConn {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := g.NewGRPCServer()
	go server.Serve(listener)
	client, err := grpc.NewClient("passthrough:///"+listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close(); server.Stop(); listener.Close() })
	return client
}
func TestGRPCDispatchResourceContextAndErrors(t *testing.T) {
	r := NewRegistry()
	calls := make(chan Call, 1)
	register(t, r, "/google.pubsub.v1.Publisher/GetTopic", func(call Call, input any) (any, error) {
		if strings.HasSuffix(call.Resource, "/denied") {
			return nil, permissionError()
		}
		calls <- call
		return &pb.Topic{Name: input.(*pb.GetTopicRequest).Topic}, nil
	})
	client := pb.NewPublisherClient(grpcClient(t, New(r, nil)))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer unknown")
	var header metadata.MD
	response, err := client.GetTopic(ctx, &pb.GetTopicRequest{Topic: "projects/synthetic/topics/topic"}, grpc.Header(&header))
	if err != nil {
		t.Fatal(err)
	}
	call := <-calls
	if response.Name != call.Resource || call.Project != "synthetic" || call.Location != "global" || call.Principal.State != Unknown || len(call.TraceID) != 32 || header.Get("x-gcp-local-trace-id")[0] != call.TraceID {
		t.Fatalf("%+v %v", call, header)
	}
	_, err = client.GetTopic(ctx, &pb.GetTopicRequest{Topic: "projects/wrong/subscriptions/topic"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	_, err = client.GetTopic(ctx, &pb.GetTopicRequest{Topic: "projects/synthetic/topics/denied"})
	s := status.Convert(err)
	if s.Code() != codes.PermissionDenied || s.Details()[0].(*errdetails.ErrorInfo).Reason != "IAM_PERMISSION_DENIED" {
		t.Fatal(s)
	}
	_, err = client.ListTopics(ctx, &pb.ListTopicsRequest{Project: "projects/synthetic"})
	if status.Code(err) != codes.Unimplemented {
		t.Fatal(err)
	}
}
func TestStreamingPullBidirectionalDeliveryAndCancellation(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "deadline"}[deadline], func(t *testing.T) {
			r := NewRegistry()
			received, done := make(chan struct{}), make(chan struct{})
			if err := r.RegisterStream("/google.pubsub.v1.Subscriber/StreamingPull", func(call Call, stream Stream) error {
				defer close(done)
				var first pb.StreamingPullRequest
				if err := stream.Recv(&first); err != nil {
					return err
				}
				if first.Subscription != "projects/synthetic/subscriptions/sub" || call.Project != "synthetic" || call.Resource != first.Subscription {
					return errors.New("missing first request context")
				}
				if err := stream.Send(&pb.StreamingPullResponse{ReceivedMessages: []*pb.ReceivedMessage{{AckId: "ack", Message: &pb.PubsubMessage{Data: []byte("payload")}}}}); err != nil {
					return err
				}
				var second pb.StreamingPullRequest
				if err := stream.Recv(&second); err != nil {
					return err
				}
				if len(second.AckIds) != 1 || second.AckIds[0] != "ack" {
					return errors.New("missing acknowledgement")
				}
				close(received)
				<-call.Context.Done()
				return call.Context.Err()
			}); err != nil {
				t.Fatal(err)
			}
			client := pb.NewSubscriberClient(grpcClient(t, New(r, nil)))
			timeout := 3 * time.Second
			if deadline {
				timeout = 150 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			stream, err := client.StreamingPull(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := stream.Send(&pb.StreamingPullRequest{Subscription: "projects/synthetic/subscriptions/sub"}); err != nil {
				t.Fatal(err)
			}
			response, err := stream.Recv()
			if err != nil || string(response.ReceivedMessages[0].Message.Data) != "payload" {
				t.Fatalf("%v %v", response, err)
			}
			if err := stream.Send(&pb.StreamingPullRequest{AckIds: []string{"ack"}}); err != nil {
				t.Fatal(err)
			}
			wait(t, received)
			if !deadline {
				cancel()
			}
			_, err = stream.Recv()
			expected := codes.Canceled
			if deadline {
				expected = codes.DeadlineExceeded
			}
			if status.Code(err) != expected {
				t.Fatalf("want %v, got %v", expected, err)
			}
			wait(t, done)
		})
	}
}
func TestUnsupportedStreamReturnsWithoutInitialMessage(t *testing.T) {
	client := pb.NewSubscriberClient(grpcClient(t, New(nil, nil)))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := client.StreamingPull(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.Recv()
	if status.Code(err) != codes.Unimplemented {
		t.Fatal(err)
	}
}
func TestStreamMissingInitialMessageAndWrongResponseType(t *testing.T) {
	r := NewRegistry()
	if err := r.RegisterStream("/google.pubsub.v1.Subscriber/StreamingPull", func(_ Call, stream Stream) error { return stream.Send(&pb.Topic{}) }); err != nil {
		t.Fatal(err)
	}
	client := pb.NewSubscriberClient(grpcClient(t, New(r, nil)))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := client.StreamingPull(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stream.CloseSend()
	_, err = stream.Recv()
	if status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	stream, err = client.StreamingPull(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stream.Send(&pb.StreamingPullRequest{Subscription: "projects/synthetic/subscriptions/sub"})
	_, err = stream.Recv()
	if status.Code(err) != codes.Internal {
		t.Fatal(err)
	}
}
func TestRegistrationRejectsTyposDuplicatesAndWrongTransport(t *testing.T) {
	r := NewRegistry()
	handler := func(Call, any) (any, error) { return nil, io.EOF }
	register(t, r, "storage.buckets.get", handler)
	for _, method := range []string{"storage.buckets.get", "storage.buckets.typo", "/google.pubsub.v1.Subscriber/StreamingPull"} {
		if r.Register(method, handler) == nil {
			t.Fatalf("accepted %s", method)
		}
	}
	if r.RegisterStream("storage.buckets.get", func(Call, Stream) error { return nil }) == nil {
		t.Fatal("accepted unary as stream")
	}
	if got := r.Providers(); len(got) != 1 || got[0] != "storage" {
		t.Fatal(got)
	}
}
func wait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for provider")
	}
}
