package pubsub

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// firstAckID is the scripted backend delivery the client acknowledges.
const firstAckID = "ack-1"

// clientStream is the caller side of a StreamingPull: requests are queued in
// advance and closing the queue is the client's half-close.
type clientStream struct {
	requests chan *pb.StreamingPullRequest
	recvErr  error
	mu       sync.Mutex
	sent     []*pb.StreamingPullResponse
}

func newClientStream(requests ...*pb.StreamingPullRequest) *clientStream {
	queue := make(chan *pb.StreamingPullRequest, len(requests))
	for _, request := range requests {
		queue <- request
	}
	close(queue)
	return &clientStream{requests: queue}
}
func (*clientStream) Context() context.Context { return context.Background() }
func (s *clientStream) Recv(message proto.Message) error {
	if s.recvErr != nil {
		return s.recvErr
	}
	request, open := <-s.requests
	if !open {
		return io.EOF
	}
	proto.Merge(message, request)
	return nil
}
func (s *clientStream) Send(message proto.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	response, ok := message.(*pb.StreamingPullResponse)
	if !ok {
		return status.Error(codes.Internal, "unexpected response type")
	}
	s.sent = append(s.sent, response)
	return nil
}
func (s *clientStream) received() []*pb.StreamingPullResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*pb.StreamingPullResponse(nil), s.sent...)
}

// upstreamStream is the emulator side: scripted responses are delivered before
// the end of stream that follows the provider's CloseSend.
type upstreamStream struct {
	cancelled <-chan struct{}
	cause     func() error
	responses chan *pb.StreamingPullResponse
	closed    chan struct{}
	once      sync.Once
	mu        sync.Mutex
	forwarded []*pb.StreamingPullRequest
}

func newUpstream(ctx context.Context, responses ...*pb.StreamingPullResponse) *upstreamStream {
	queue := make(chan *pb.StreamingPullResponse, len(responses))
	for _, response := range responses {
		queue <- response
	}
	return &upstreamStream{cancelled: ctx.Done(), cause: ctx.Err, responses: queue, closed: make(chan struct{})}
}
func (u *upstreamStream) Header() (metadata.MD, error) { return metadata.MD{}, nil }
func (u *upstreamStream) Trailer() metadata.MD         { return metadata.MD{} }
func (*upstreamStream) Context() context.Context       { return context.Background() }
func (u *upstreamStream) CloseSend() error {
	u.once.Do(func() { close(u.closed) })
	return nil
}
func (u *upstreamStream) SendMsg(message any) error {
	request, ok := message.(*pb.StreamingPullRequest)
	if !ok {
		return status.Error(codes.Internal, "unexpected request type")
	}
	clone, err := cloneMessage(request)
	if err != nil {
		return err
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	u.forwarded = append(u.forwarded, clone)
	return nil
}
func (u *upstreamStream) RecvMsg(message any) error {
	out, ok := message.(proto.Message)
	if !ok {
		return status.Error(codes.Internal, "unexpected response type")
	}
	select {
	case response := <-u.responses:
		proto.Merge(out, response)
		return nil
	default:
	}
	select {
	case response := <-u.responses:
		proto.Merge(out, response)
		return nil
	case <-u.closed:
		return io.EOF
	case <-u.cancelled:
		return u.cause()
	}
}
func (u *upstreamStream) requests() []*pb.StreamingPullRequest {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]*pb.StreamingPullRequest(nil), u.forwarded...)
}

// streamFixture creates a topic and pull subscription and scripts the backend
// stream with the given responses.
func streamFixture(t *testing.T, responses ...*pb.StreamingPullResponse) (*Provider, *memoryPubSub, func() *upstreamStream) {
	t.Helper()
	p, registry, b := memoryFixture(t)
	_, err := invokeAs(registry, owner, publisher+"CreateTopic", testTopic, &pb.Topic{Name: testTopic})
	must(t, err)
	_, err = invokeAs(registry, owner, subscriber+"CreateSubscription", testSub, &pb.Subscription{Name: testSub, Topic: testTopic})
	must(t, err)
	var mu sync.Mutex
	var opened *upstreamStream
	b.mu.Lock()
	b.stream = func(ctx context.Context) (grpc.ClientStream, error) {
		mu.Lock()
		defer mu.Unlock()
		opened = newUpstream(ctx, responses...)
		return opened, nil
	}
	b.mu.Unlock()
	return p, b, func() *upstreamStream { mu.Lock(); defer mu.Unlock(); return opened }
}

// TestStreamingPullRelaysBothDirections proves the provider forwards the
// opening request and later acks upstream, relays responses downstream, and
// ends cleanly when the client half-closes.
func TestStreamingPullRelaysBothDirections(t *testing.T) {
	delivered := &pb.StreamingPullResponse{ReceivedMessages: []*pb.ReceivedMessage{{AckId: firstAckID, Message: &pb.PubsubMessage{Data: []byte("hello")}}}}
	p, _, upstream := streamFixture(t, delivered)
	client := newClientStream(
		&pb.StreamingPullRequest{Subscription: testSub, StreamAckDeadlineSeconds: 10, ClientId: "c", MaxOutstandingMessages: 5},
		&pb.StreamingPullRequest{AckIds: []string{firstAckID}, ModifyDeadlineSeconds: []int32{30}, ModifyDeadlineAckIds: []string{"ack-2"}},
	)
	must(t, p.stream(call(subscriber+"StreamingPull", testSub, owner), client))
	if got := client.received(); len(got) != 1 || string(got[0].GetReceivedMessages()[0].GetMessage().GetData()) != "hello" {
		t.Fatalf("downstream received %v", got)
	}
	forwarded := upstream().requests()
	if len(forwarded) != 2 || forwarded[0].GetSubscription() != testSub || forwarded[1].GetAckIds()[0] != firstAckID {
		t.Fatalf("upstream received %v", forwarded)
	}
}

// TestStreamingPullRejectsBeforeOpeningUpstream covers stream validation and
// authorization that must complete before any backend stream opens.
func TestStreamingPullRejectsBeforeOpeningUpstream(t *testing.T) {
	p, b, _ := streamFixture(t)
	consumerGrant := func() {
		_, err := p.iam.SetPolicy(canonical(testSub), iam.Policy{Bindings: []iam.Binding{{Role: roleViewer, Members: []string{consumer}}}})
		must(t, err)
	}
	consumerGrant()
	tests := []struct {
		name    string
		actor   string
		opening *pb.StreamingPullRequest
		code    codes.Code
	}{
		{"scope mismatch", owner, &pb.StreamingPullRequest{Subscription: "projects/project-a/subscriptions/elsewhere", StreamAckDeadlineSeconds: 10}, codes.InvalidArgument},
		{"bad name", owner, &pb.StreamingPullRequest{Subscription: "projects/project-a/topics/topic", StreamAckDeadlineSeconds: 10}, codes.InvalidArgument},
		{"negative flow", owner, &pb.StreamingPullRequest{Subscription: testSub, StreamAckDeadlineSeconds: 10, MaxOutstandingMessages: -1}, codes.InvalidArgument},
		{"short deadline", owner, &pb.StreamingPullRequest{Subscription: testSub, StreamAckDeadlineSeconds: 5}, codes.InvalidArgument},
		{"mismatched modify", owner, &pb.StreamingPullRequest{Subscription: testSub, StreamAckDeadlineSeconds: 10, ModifyDeadlineSeconds: []int32{10}}, codes.InvalidArgument},
		{"modify range", owner, &pb.StreamingPullRequest{Subscription: testSub, StreamAckDeadlineSeconds: 10, ModifyDeadlineSeconds: []int32{601}, ModifyDeadlineAckIds: []string{"a"}}, codes.InvalidArgument},
		{"unknown protocol version", owner, &pb.StreamingPullRequest{Subscription: testSub, StreamAckDeadlineSeconds: 10, ProtocolVersion: 2}, codes.Unimplemented},
		{"unknown field", owner, withUnknownField(&pb.StreamingPullRequest{Subscription: testSub, StreamAckDeadlineSeconds: 10}), codes.Unimplemented},
		{"no consume grant", consumer, &pb.StreamingPullRequest{Subscription: testSub, StreamAckDeadlineSeconds: 10}, codes.PermissionDenied},
		{"foreign caller", other, &pb.StreamingPullRequest{Subscription: testSub, StreamAckDeadlineSeconds: 10}, codes.PermissionDenied},
	}
	for _, test := range tests {
		err := p.stream(call(subscriber+"StreamingPull", testSub, test.actor), newClientStream(test.opening))
		if status.Code(err) != test.code {
			t.Errorf("%s: got %v (%v), want %v", test.name, status.Code(err), err, test.code)
		}
	}
	broken := newClientStream()
	broken.recvErr = status.Error(codes.Canceled, "client went away")
	if err := p.stream(call(subscriber+"StreamingPull", testSub, owner), broken); status.Code(err) != codes.Canceled {
		t.Fatalf("opening receive error = %v", err)
	}
	if b.called(subscriber+"StreamingPull") != 0 {
		t.Fatal("a rejected stream opened the backend stream")
	}
}

// withUnknownField adds a field newer than the pinned bindings.
func withUnknownField(request *pb.StreamingPullRequest) *pb.StreamingPullRequest {
	request.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 1))
	return request
}

// TestStreamingPullProtocolVersion proves the opening request's
// protocol_version 1, which the Go client sends, reaches the emulator, and
// that other versions and a version on a later request are refused.
func TestStreamingPullProtocolVersion(t *testing.T) {
	p, _, upstream := streamFixture(t)
	client := newClientStream(
		&pb.StreamingPullRequest{Subscription: testSub, StreamAckDeadlineSeconds: 10, ProtocolVersion: 1},
		&pb.StreamingPullRequest{},
	)
	must(t, p.stream(call(subscriber+"StreamingPull", testSub, owner), client))
	if forwarded := upstream().requests(); len(forwarded) != 2 || forwarded[0].GetProtocolVersion() != 1 {
		t.Fatalf("upstream received %v", forwarded)
	}

	refusals := []struct {
		name     string
		requests []*pb.StreamingPullRequest
		code     codes.Code
		message  string
	}{
		{"newer version", []*pb.StreamingPullRequest{{Subscription: testSub, StreamAckDeadlineSeconds: 10, ProtocolVersion: 2}},
			codes.Unimplemented, "Pub/Sub coverage gap: protocol_version 2"},
		{"negative version", []*pb.StreamingPullRequest{{Subscription: testSub, StreamAckDeadlineSeconds: 10, ProtocolVersion: -1}},
			codes.Unimplemented, "Pub/Sub coverage gap: protocol_version -1"},
		{"later version", []*pb.StreamingPullRequest{{Subscription: testSub, StreamAckDeadlineSeconds: 10}, {ProtocolVersion: 1}},
			codes.InvalidArgument, "Subscription, client ID, flow control, and protocol version belong only in the first request"},
	}
	for _, refusal := range refusals {
		p, _, _ := streamFixture(t)
		err := p.stream(call(subscriber+"StreamingPull", testSub, owner), newClientStream(refusal.requests...))
		if status.Code(err) != refusal.code || !strings.HasSuffix(status.Convert(err).Message(), "desc = "+refusal.message) && status.Convert(err).Message() != refusal.message {
			t.Errorf("%s: got %v, want %v %q", refusal.name, err, refusal.code, refusal.message)
		}
	}
}

// TestStreamingPullRejectsInvalidFollowUp proves later requests are validated
// and may not re-target the stream.
func TestStreamingPullRejectsInvalidFollowUp(t *testing.T) {
	p, _, upstream := streamFixture(t)
	client := newClientStream(
		&pb.StreamingPullRequest{Subscription: testSub, StreamAckDeadlineSeconds: 10},
		&pb.StreamingPullRequest{Subscription: "projects/project-a/subscriptions/other"},
	)
	err := p.stream(call(subscriber+"StreamingPull", testSub, owner), client)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("re-targeted follow-up = %v", err)
	}
	if forwarded := upstream().requests(); len(forwarded) != 1 {
		t.Fatalf("invalid follow-up was forwarded: %v", forwarded)
	}
}

// TestStreamingPullReauthorizesFollowUps proves a revoked grant stops a stream
// that is already open.
func TestStreamingPullReauthorizesFollowUps(t *testing.T) {
	p, _, upstream := streamFixture(t)
	_, err := p.iam.SetPolicy(canonical(testSub), iam.Policy{Bindings: []iam.Binding{{Role: roleSubscriber, Members: []string{consumer}}}})
	must(t, err)
	requests := make(chan *pb.StreamingPullRequest, 2)
	requests <- &pb.StreamingPullRequest{Subscription: testSub, StreamAckDeadlineSeconds: 10}
	client := &clientStream{requests: requests}
	done := make(chan error, 1)
	go func() { done <- p.stream(call(subscriber+"StreamingPull", testSub, consumer), client) }()
	deadline := time.Now().Add(2 * time.Second)
	for upstream() == nil || len(upstream().requests()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("stream did not open")
		}
		time.Sleep(5 * time.Millisecond)
	}
	_, err = p.iam.SetPolicy(canonical(testSub), iam.Policy{})
	must(t, err)
	requests <- &pb.StreamingPullRequest{AckIds: []string{"a"}}
	select {
	case err := <-done:
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("revoked stream = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("revoked stream kept running")
	}
	if len(upstream().requests()) != 1 {
		t.Fatal("a revoked caller's ack reached the backend")
	}
}

// TestStreamingPullUpstreamFailures covers a backend that cannot open its
// stream and a pull stream on a push subscription.
func TestStreamingPullUpstreamFailures(t *testing.T) {
	p, b, _ := streamFixture(t)
	b.mu.Lock()
	b.stream = nil
	b.mu.Unlock()
	err := p.stream(call(subscriber+"StreamingPull", testSub, owner), newClientStream(&pb.StreamingPullRequest{Subscription: testSub, StreamAckDeadlineSeconds: 10}))
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("unopenable backend stream = %v", err)
	}
	p.pushes.Store(canonical(testSub), &pushSubscription{scope: scopeOf(call("", testSub, owner)), cancel: func() {}, done: closedChannel()})
	err = p.stream(call(subscriber+"StreamingPull", testSub, owner), newClientStream(&pb.StreamingPullRequest{Subscription: testSub, StreamAckDeadlineSeconds: 10}))
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("pull stream on a push subscription = %v", err)
	}
	_, err = p.handle(call(subscriber+"Pull", testSub, owner), &pb.PullRequest{Subscription: testSub, MaxMessages: 1})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unary pull on a push subscription = %v", err)
	}
}

func closedChannel() chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}
