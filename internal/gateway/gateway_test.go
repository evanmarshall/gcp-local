package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/firestore/apiv1/firestorepb"
	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// listenTestAddress opens a loopback listener through the context-aware API.
func listenTestAddress() (net.Listener, error) {
	var listen net.ListenConfig
	listener, err := listen.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen on loopback: %w", err)
	}
	return listener, nil
}

const (
	authorizationHeader    = "Authorization"
	bucketCollectionPath   = "/storage/v1/b"
	streamSubscription     = "projects/synthetic/subscriptions/sub"
	ackID                  = "ack"
	permissionDeniedReason = "IAM_PERMISSION_DENIED"
)

// Sentinels returned by the stub handlers in this file.
var (
	errBadDecodedRequest  = errors.New("bad decoded request")
	errSecretBackendToken = errors.New("secret backend token")
	errMissingFirstCall   = errors.New("missing first request context")
	errMissingAck         = errors.New("missing acknowledgement")
)

func register(t *testing.T, r *Registry, method string, h Handler) {
	t.Helper()
	if err := r.Register(method, h); err != nil {
		t.Fatal(err)
	}
}
func request(t *testing.T, client *http.Client, method, url, body string, headers http.Header) (int, map[string]any, http.Header) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), method, url, strings.NewReader(body))
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
		req, ok := input.(*RESTRequest)
		if !ok || !decodedEscapedObject(req) {
			return nil, errBadDecodedRequest
		}
		calls <- call
		return &RESTResponse{JSON: map[string]any{"name": req.Path["object"]}}, nil
	})
	g := New(r, func(_ context.Context, token string) (Principal, error) {
		if token != "fixture-token" {
			t.Error("wrong token")
		}
		return Principal{State: Authenticated, Subject: "fixture", Project: "caller-project"}, nil
	})
	server := httptest.NewServer(g)
	defer server.Close()
	trace := "1234567890abcdef1234567890abcdef"
	code, body, headers := request(t, server.Client(), "GET", server.URL+"/storage/v1/b/bucket/o/folder%2Fa%25%3F.txt?project=untrusted", "", http.Header{authorizationHeader: {"Bearer fixture-token"}, "Traceparent": {"00-" + trace + "-0123456789abcdef-01"}})
	if code != 200 || body["name"] != "folder/a%?.txt" || headers.Get("X-Gcp-Local-Trace-Id") != trace {
		t.Fatalf("%d %v %v", code, body, headers)
	}
	want := callSummary{Method: "storage.objects.get", Location: "global", Resource: "/storage/v1/b/bucket/o/folder/a%?.txt", TraceID: trace, State: Authenticated, Subject: "fixture", PrincipalProject: "caller-project"}
	if call := <-calls; summarize(call) != want {
		t.Fatalf("%+v", call)
	}
}

// decodedEscapedObject reports whether the object path arrived unescaped and
// the bearer token was stripped before the provider saw the request.
func decodedEscapedObject(req *RESTRequest) bool {
	return req.Path["bucket"] == "bucket" && req.Path["object"] == "folder/a%?.txt" && req.Headers.Get(authorizationHeader) == ""
}

// callSummary is the comparable part of a Call that dispatch tests assert.
type callSummary struct {
	Method, Project, Location, Resource, TraceID string
	State                                        PrincipalState
	Subject, PrincipalProject                    string
}

func summarize(call Call) callSummary {
	return callSummary{Method: call.Method, Project: call.Project, Location: call.Location, Resource: call.Resource, TraceID: call.TraceID, State: call.Principal.State, Subject: call.Principal.Subject, PrincipalProject: call.Principal.Project}
}
func TestRESTValidationBeforeDispatchAndExplicitGaps(t *testing.T) {
	r := NewRegistry()
	var called atomic.Int32
	register(t, r, "storage.buckets.insert", func(call Call, _ any) (any, error) {
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
		{bucketCollectionPath, `{`, nil, 400}, {bucketCollectionPath, `[]`, nil, 400}, {bucketCollectionPath, `"text"`, nil, 400},
		{"/storage/v1/b?invalid=%zz", `{}`, nil, 400},
		{bucketCollectionPath, `{}`, http.Header{authorizationHeader: {"Bearer one", "Bearer two"}}, 401},
		{bucketCollectionPath, `{}`, http.Header{authorizationHeader: {"Basic one"}}, 401},
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

// An unserved method under another API's path prefix is a missing method, not
// a Storage XML object in a bucket named after the API.
func TestUnservedAPIPathIsNotAnXMLObject(t *testing.T) {
	r := NewRegistry()
	var objects atomic.Int32
	register(t, r, "storage.xml.objects.get", func(Call, any) (any, error) {
		objects.Add(1)
		return &RESTResponse{Status: 200, JSON: map[string]any{}}, nil
	})
	server := httptest.NewServer(New(r, nil))
	defer server.Close()
	for _, path := range []string{"/bigquery/v2/projects/p/datasets/d/models", "/storage/v1/b/bucket/unserved", "/v1/projects/p/unserved"} {
		code, body, _ := request(t, server.Client(), "GET", server.URL+path, "", nil)
		message, _ := body["error"].(map[string]any)["message"].(string)
		if code != 501 || message != "No provider implements this REST API method" {
			t.Fatalf("%s: %d %v", path, code, body)
		}
	}
	if code, _, _ := request(t, server.Client(), "GET", server.URL+"/bucket/object", "", nil); code != 200 || objects.Load() != 1 {
		t.Fatalf("XML object read: %d, %d calls", code, objects.Load())
	}
}
func TestRESTEmptyBodyIsTheEmptyMessage(t *testing.T) {
	r := NewRegistry()
	register(t, r, "storage.buckets.insert", func(_ Call, input any) (any, error) {
		req, ok := input.(*RESTRequest)
		if !ok || string(req.JSON) != "{}" {
			return nil, errBadDecodedRequest
		}
		return &RESTResponse{JSON: map[string]any{}}, nil
	})
	server := httptest.NewServer(New(r, nil))
	defer server.Close()
	for _, payload := range []string{"", "null", " null\n"} {
		code, body, _ := request(t, server.Client(), "POST", server.URL+bucketCollectionPath+"?project=synthetic-project", payload, nil)
		if code != 200 {
			t.Fatalf("body %q: %d %v", payload, code, body)
		}
	}
}
func TestDefaultIdentityNeverAuthenticatesPresentedToken(t *testing.T) {
	r := NewRegistry()
	register(t, r, "storage.buckets.list", func(call Call, _ any) (any, error) {
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
			headers.Set(authorizationHeader, tc.auth)
		}
		_, body, _ := request(t, server.Client(), "GET", server.URL+bucketCollectionPath, "", headers)
		if body["state"] != string(tc.state) || body["subject"] != "" {
			t.Fatal(body)
		}
	}
}
func permissionError() error {
	s, _ := status.New(codes.PermissionDenied, "Missing permission").WithDetails(&errdetails.ErrorInfo{Reason: permissionDeniedReason, Domain: "gcp-local", Metadata: map[string]string{"permission": "storage.objects.get"}})
	return s.Err()
}
func TestRESTErrorsPreserveDetailsAndHideInternalFailures(t *testing.T) {
	for _, tc := range []struct {
		err     error
		code    int
		message string
	}{{permissionError(), 403, "Missing permission"}, {errSecretBackendToken, 500, "Internal provider error"}, {context.Canceled, 499, "context canceled"}} {
		r := NewRegistry()
		register(t, r, "storage.objects.get", func(Call, any) (any, error) { return nil, tc.err })
		server := httptest.NewServer(New(r, nil))
		code, body, _ := request(t, server.Client(), "GET", server.URL+"/storage/v1/b/bucket/o/object", "", nil)
		server.Close()
		detail, ok := body["error"].(map[string]any)
		if !ok {
			t.Fatalf("error body is not an object: %v", body)
		}
		if code != tc.code || detail["message"] != tc.message {
			t.Fatalf("%d %v", code, body)
		}
		if code == 403 {
			assertPermissionReason(t, detail)
		}
	}
}

// assertPermissionReason checks that a REST error body kept its ErrorInfo reason.
func assertPermissionReason(t *testing.T, detail map[string]any) {
	t.Helper()
	details, ok := detail["details"].([]any)
	if !ok || len(details) == 0 {
		t.Fatalf("missing error details: %v", detail)
	}
	first, ok := details[0].(map[string]any)
	if !ok || first["reason"] != permissionDeniedReason {
		t.Fatal(details)
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
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+bucketCollectionPath, nil)
	completed := make(chan struct{})
	go func() {
		defer close(completed)
		resp, _ := server.Client().Do(req)
		if resp != nil {
			_ = resp.Body.Close()
		}
	}()
	wait(t, entered)
	cancel()
	wait(t, done)
	wait(t, completed)
}
func grpcClient(t *testing.T, g *Gateway) *grpc.ClientConn {
	t.Helper()
	listener, err := listenTestAddress()
	if err != nil {
		t.Fatal(err)
	}
	server := g.NewGRPCServer()
	go func() { _ = server.Serve(listener) }()
	client, err := grpc.NewClient("passthrough:///"+listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(); server.Stop(); _ = listener.Close() })
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
		request, ok := input.(*pb.GetTopicRequest)
		if !ok {
			return nil, errBadDecodedRequest
		}
		return &pb.Topic{Name: request.GetTopic()}, nil
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
	want := callSummary{Method: "/google.pubsub.v1.Publisher/GetTopic", Project: "synthetic", Location: "global", Resource: response.GetName(), TraceID: header.Get("x-gcp-local-trace-id")[0], State: Unknown}
	if summarize(call) != want || len(call.TraceID) != 32 {
		t.Fatalf("%+v %v", call, header)
	}
	_, err = client.GetTopic(ctx, &pb.GetTopicRequest{Topic: "projects/wrong/subscriptions/topic"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	_, err = client.GetTopic(ctx, &pb.GetTopicRequest{Topic: "projects/synthetic/topics/denied"})
	if s := status.Convert(err); s.Code() != codes.PermissionDenied || errorInfoReason(s) != permissionDeniedReason {
		t.Fatal(s)
	}
	_, err = client.ListTopics(ctx, &pb.ListTopicsRequest{Project: "projects/synthetic"})
	if status.Code(err) != codes.Unimplemented {
		t.Fatal(err)
	}
}

// errorInfoReason returns the reason of a status's first ErrorInfo detail.
func errorInfoReason(s *status.Status) string {
	if details := s.Details(); len(details) > 0 {
		if info, ok := details[0].(*errdetails.ErrorInfo); ok {
			return info.GetReason()
		}
	}
	return ""
}

func TestStreamingPullBidirectionalDeliveryAndCancellation(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "deadline"}[deadline], func(t *testing.T) {
			r := NewRegistry()
			received, done := make(chan struct{}), make(chan struct{})
			if err := r.RegisterStream("/google.pubsub.v1.Subscriber/StreamingPull", deliverOneAndAwaitAck(received, done)); err != nil {
				t.Fatal(err)
			}
			client := pb.NewSubscriberClient(grpcClient(t, New(r, nil)))
			timeout, expected := 3*time.Second, codes.Canceled
			if deadline {
				timeout, expected = 150*time.Millisecond, codes.DeadlineExceeded
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			code := pullAndAck(ctx, t, client, func() {
				wait(t, received)
				if !deadline {
					cancel()
				}
			})
			if code != expected {
				t.Fatalf("want %v, got %v", expected, code)
			}
			wait(t, done)
		})
	}
}

// deliverOneAndAwaitAck is a StreamingPull provider that checks the first
// request's resource context, delivers one message, waits for its ack, then
// holds the stream open until the client cancels or the deadline passes.
func deliverOneAndAwaitAck(received, done chan<- struct{}) StreamHandler {
	return func(call Call, stream Stream) error {
		defer close(done)
		var first pb.StreamingPullRequest
		if err := stream.Recv(&first); err != nil {
			return err
		}
		if first.GetSubscription() != streamSubscription || call.Project != "synthetic" || call.Resource != first.GetSubscription() {
			return errMissingFirstCall
		}
		if err := stream.Send(&pb.StreamingPullResponse{ReceivedMessages: []*pb.ReceivedMessage{{AckId: ackID, Message: &pb.PubsubMessage{Data: []byte("payload")}}}}); err != nil {
			return err
		}
		var second pb.StreamingPullRequest
		if err := stream.Recv(&second); err != nil {
			return err
		}
		if len(second.GetAckIds()) != 1 || second.GetAckIds()[0] != ackID {
			return errMissingAck
		}
		close(received)
		<-call.Context.Done()
		return call.Context.Err()
	}
}

// pullAndAck opens a StreamingPull, receives the delivered message, acks it,
// runs afterAck, and returns the status code that ends the stream.
func pullAndAck(ctx context.Context, t *testing.T, client pb.SubscriberClient, afterAck func()) codes.Code {
	t.Helper()
	stream, err := client.StreamingPull(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&pb.StreamingPullRequest{Subscription: streamSubscription}); err != nil {
		t.Fatal(err)
	}
	response, err := stream.Recv()
	if err != nil || string(response.GetReceivedMessages()[0].GetMessage().GetData()) != "payload" {
		t.Fatalf("%v %v", response, err)
	}
	if err := stream.Send(&pb.StreamingPullRequest{AckIds: []string{ackID}}); err != nil {
		t.Fatal(err)
	}
	afterAck()
	_, err = stream.Recv()
	return status.Code(err)
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
	_ = stream.CloseSend()
	_, err = stream.Recv()
	if status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	stream, err = client.StreamingPull(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = stream.Send(&pb.StreamingPullRequest{Subscription: streamSubscription})
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

// TestMultiSegmentVariablesAndTrailingWildcards covers {name=**} REST
// variables and trailing ** gRPC resource patterns.
func TestMultiSegmentVariablesAndTrailingWildcards(t *testing.T) {
	r := NewRegistry()
	paths := make(chan string, 4)
	register(t, r, "firestore.documents.get", func(_ Call, input any) (any, error) {
		req, ok := input.(*RESTRequest)
		if !ok {
			return nil, errBadDecodedRequest
		}
		paths <- req.Path["path"]
		return &RESTResponse{JSON: map[string]any{}}, nil
	})
	server := httptest.NewServer(New(r, nil))
	defer server.Close()
	base := server.URL + "/v1/projects/p/databases/(default)/documents/"
	if code, _, _ := request(t, server.Client(), http.MethodGet, base+"a/b/c%20d/e", "", nil); code != 200 || <-paths != "a/b/c d/e" {
		t.Fatalf("nested path = %d", code)
	}
	for _, bad := range []string{"a/b%2Fc", "a/%2E%2E", "a/./b"} {
		if code, _, _ := request(t, server.Client(), http.MethodGet, base+bad, "", nil); code != http.StatusBadRequest {
			t.Fatalf("%s = %d", bad, code)
		}
	}
	g := New(r, nil)
	for resource, ok := range map[string]bool{
		"projects/p/databases/(default)/documents":         true,
		"projects/p/databases/(default)/documents/a/b/c/d": true,
		"projects/p/databases/(default)":                   false,
		"projects/p/databases/(default)/documents/a//b":    false,
		"projects/p/databases/(default)/other/a":           false,
	} {
		call, err := g.rpcCall(context.Background(), "/google.firestore.v1.Firestore/RunQuery", &firestorepb.RunQueryRequest{Parent: resource})
		if (err == nil) != ok || (ok && call.Project != "p") {
			t.Fatalf("%s: %+v %v", resource, call, err)
		}
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
