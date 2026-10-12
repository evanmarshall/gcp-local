//go:build pubsubintegration

package pubsub

import (
	"context"
	"testing"
	"time"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const integrationAgent = "serviceAccount:service-123456@gcp-sa-pubsub.iam.gserviceaccount.com"

// features drives one authenticated owner against the real emulator.
type features struct {
	t   *testing.T
	f   *integration
	pub pb.PublisherClient
	sub pb.SubscriberClient
}

func newFeatures(t *testing.T) *features {
	f := running(t)
	return &features{t: t, f: f, pub: pb.NewPublisherClient(f.connection), sub: pb.NewSubscriberClient(f.connection)}
}

func (x *features) topic(name string) string {
	x.t.Helper()
	topic := "projects/project-a/topics/" + name
	_, err := x.pub.CreateTopic(x.f.owner, &pb.Topic{Name: topic})
	must(x.t, err)
	return topic
}

func (x *features) subscription(s *pb.Subscription) string {
	x.t.Helper()
	_, err := x.sub.CreateSubscription(x.f.owner, s)
	must(x.t, err)
	return s.GetName()
}

func (x *features) publish(topic string, messages ...*pb.PubsubMessage) {
	x.t.Helper()
	_, err := x.pub.Publish(x.f.owner, &pb.PublishRequest{Topic: topic, Messages: messages})
	must(x.t, err)
}

// pull returns what is immediately available, waiting up to wait for at least one message.
func (x *features) pull(subscription string, max int32, wait time.Duration) []*pb.ReceivedMessage {
	x.t.Helper()
	deadline := time.Now().Add(wait)
	for {
		ctx, cancel := context.WithTimeout(x.f.owner, 3*time.Second)
		response, err := x.sub.Pull(ctx, &pb.PullRequest{Subscription: subscription, MaxMessages: max, ReturnImmediately: true})
		cancel()
		must(x.t, err)
		if len(response.GetReceivedMessages()) > 0 || time.Now().After(deadline) {
			return response.GetReceivedMessages()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (x *features) nack(subscription string, received ...*pb.ReceivedMessage) {
	x.t.Helper()
	ids := []string{}
	for _, message := range received {
		ids = append(ids, message.GetAckId())
	}
	_, err := x.sub.ModifyAckDeadline(x.f.owner, &pb.ModifyAckDeadlineRequest{Subscription: subscription, AckIds: ids})
	must(x.t, err)
}

func (x *features) ack(subscription string, received ...*pb.ReceivedMessage) error {
	ids := []string{}
	for _, message := range received {
		ids = append(ids, message.GetAckId())
	}
	_, err := x.sub.Acknowledge(x.f.owner, &pb.AcknowledgeRequest{Subscription: subscription, AckIds: ids})
	return err
}

func data(received []*pb.ReceivedMessage) []string {
	out := []string{}
	for _, message := range received {
		out = append(out, string(message.GetMessage().GetData()))
	}
	return out
}

// TestDeadLetterThroughEmulatorRequiresServiceAgent shows that without the
// service agent's grants a message is redelivered past the limit with counted
// attempts, and that granting both roles lets the emulator dead-letter it.
func TestDeadLetterThroughEmulatorRequiresServiceAgent(t *testing.T) {
	x := newFeatures(t)
	source, deadLetters := x.topic("source"), x.topic("dead-letters")
	collected := x.subscription(&pb.Subscription{Name: "projects/project-a/subscriptions/collected", Topic: deadLetters})
	subscription := x.subscription(&pb.Subscription{Name: testSub, Topic: source, AckDeadlineSeconds: 10,
		DeadLetterPolicy: &pb.DeadLetterPolicy{DeadLetterTopic: deadLetters, MaxDeliveryAttempts: 5}})
	x.publish(source, &pb.PubsubMessage{Data: []byte("poison")})
	for attempt := 1; attempt <= 7; attempt++ {
		received := x.pull(subscription, 1, 5*time.Second)
		if len(received) != 1 || received[0].GetDeliveryAttempt() != int32(attempt) {
			t.Fatalf("ungranted attempt %d: %v", attempt, received)
		}
		x.nack(subscription, received...)
	}
	if got := x.pull(collected, 10, 500*time.Millisecond); len(got) != 0 {
		t.Fatalf("dead-lettered without service-agent grants: %v", data(got))
	}
	_, err := x.f.p.iam.SetPolicy(canonical(deadLetters), iam.Policy{Bindings: []iam.Binding{{Role: "roles/pubsub.publisher", Members: []string{integrationAgent}}}})
	must(t, err)
	_, err = x.f.p.iam.SetPolicy(canonical(subscription), iam.Policy{Bindings: []iam.Binding{{Role: "roles/pubsub.subscriber", Members: []string{integrationAgent}}}})
	must(t, err)
	attempts := []int32{}
	for len(attempts) < 10 {
		received := x.pull(subscription, 1, 2*time.Second)
		if len(received) == 0 {
			break
		}
		attempts = append(attempts, received[0].GetDeliveryAttempt())
		x.nack(subscription, received...)
	}
	if len(attempts) != 5 || attempts[0] != 1 || attempts[4] != 5 {
		t.Fatalf("granted delivery attempts = %v, want 1 through 5", attempts)
	}
	forwarded := x.pull(collected, 10, 5*time.Second)
	if len(forwarded) != 1 || string(forwarded[0].GetMessage().GetData()) != "poison" || forwarded[0].GetMessage().GetAttributes()["CloudPubSubDeadLetterSourceDeliveryCount"] != "5" {
		t.Fatalf("dead-letter topic received %v", forwarded)
	}
	got, err := x.sub.GetSubscription(x.f.owner, &pb.GetSubscriptionRequest{Subscription: subscription})
	must(t, err)
	if got.GetDeadLetterPolicy().GetMaxDeliveryAttempts() != 5 {
		t.Fatalf("subscription policy = %v", got.GetDeadLetterPolicy())
	}
}

// TestOrderingRetryFilterAndExactlyOnceThroughEmulator covers the other
// delivery settings against the real emulator through the provider.
func TestOrderingRetryFilterAndExactlyOnceThroughEmulator(t *testing.T) {
	x := newFeatures(t)
	topic := x.topic("events")
	ordered := x.subscription(&pb.Subscription{Name: "projects/project-a/subscriptions/ordered", Topic: topic, EnableMessageOrdering: true, AckDeadlineSeconds: 10})
	filtered := x.subscription(&pb.Subscription{Name: "projects/project-a/subscriptions/filtered", Topic: topic, Filter: `attributes.kind = "match"`})
	retries := x.topic("retries")
	retried := x.subscription(&pb.Subscription{Name: "projects/project-a/subscriptions/retried", Topic: retries,
		RetryPolicy: &pb.RetryPolicy{MinimumBackoff: durationpb.New(2 * time.Second), MaximumBackoff: durationpb.New(4 * time.Second)}})
	exact := x.subscription(&pb.Subscription{Name: "projects/project-a/subscriptions/exact", Topic: topic, EnableExactlyOnceDelivery: true, AckDeadlineSeconds: 10})
	_, err := x.sub.CreateSubscription(x.f.owner, &pb.Subscription{Name: "projects/project-a/subscriptions/badfilter", Topic: topic, Filter: "attributes.kind = ="})
	expectRefusal(t, "unparsable filter", err, codes.InvalidArgument, "Invalid filter expression")

	for _, value := range []string{"1", "2", "3"} {
		x.publish(topic, &pb.PubsubMessage{Data: []byte(value), OrderingKey: "key", Attributes: map[string]string{"kind": map[bool]string{true: "match", false: "other"}[value == "2"]}})
	}
	// The emulator alone redelivers 2 before a nacked 1; the provider's
	// one-second nack deadline keeps key order.
	first := x.pull(ordered, 1, 5*time.Second)
	if got := data(first); len(got) != 1 || got[0] != "1" {
		t.Fatalf("first ordered delivery = %v", got)
	}
	x.nack(ordered, first...)
	again := x.pull(ordered, 1, 5*time.Second)
	if got := data(again); len(got) != 1 || got[0] != "1" {
		t.Fatalf("ordered redelivery after a nack = %v, want 1", got)
	}
	must(t, x.ack(ordered, again...))
	rest := x.pull(ordered, 10, 5*time.Second)
	if got := data(rest); len(got) != 2 || got[0] != "2" || got[1] != "3" {
		t.Fatalf("remaining ordered deliveries = %v", got)
	}
	must(t, x.ack(ordered, rest...))

	if got := data(x.pull(filtered, 10, 5*time.Second)); len(got) != 1 || got[0] != "2" {
		t.Fatalf("filtered deliveries = %v, want only 2", got)
	}

	x.publish(retries, &pb.PubsubMessage{Data: []byte("retry")})
	retry := x.pull(retried, 1, 5*time.Second)
	// The emulator starts the backoff when it handles the nack, before the RPC returns.
	nacked := time.Now()
	x.nack(retried, retry...)
	if early := x.pull(retried, 1, time.Second); len(early) != 0 {
		t.Fatalf("redelivered %v before the two-second minimum backoff", data(early))
	}
	redelivered := x.pull(retried, 1, 5*time.Second)
	if len(redelivered) != 1 || redelivered[0].GetMessage().GetMessageId() != retry[0].GetMessage().GetMessageId() || time.Since(nacked) < 2*time.Second {
		t.Fatalf("backoff redelivery = %v after %v", redelivered, time.Since(nacked))
	}

	once := x.pull(exact, 1, 5*time.Second)
	must(t, x.ack(exact, once...))
	// GCP answers OK to a repeated ack (pubsub-delivery-features); an unknown
	// ack ID still fails with the emulator's exactly-once details.
	must(t, x.ack(exact, once...))
	// The emulator's ack IDs are "<subscription>:<sequence>"; this one was never issued.
	unknown := &pb.ReceivedMessage{AckId: exact + ":999999"}
	err = x.ack(exact, unknown)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("exactly-once ack of an unknown ID: %v", err)
	}
	reasons := []string{}
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			reasons = append(reasons, info.GetReason()+":"+info.GetMetadata()[unknown.GetAckId()])
		}
	}
	if len(reasons) != 1 || reasons[0] != "EXACTLY_ONCE_ACKID_FAILURE:PERMANENT_FAILURE_INVALID_ACK_ID" {
		t.Fatalf("exactly-once ack failure details = %v (%v)", reasons, err)
	}
}

// TestStreamingNackKeepsKeyOrder covers the nack rewrite on StreamingPull.
func TestStreamingNackKeepsKeyOrder(t *testing.T) {
	x := newFeatures(t)
	topic := x.topic("stream-events")
	ordered := x.subscription(&pb.Subscription{Name: "projects/project-a/subscriptions/stream-ordered", Topic: topic, EnableMessageOrdering: true, AckDeadlineSeconds: 10})
	x.publish(topic, &pb.PubsubMessage{Data: []byte("1"), OrderingKey: "key"})
	x.publish(topic, &pb.PubsubMessage{Data: []byte("2"), OrderingKey: "key"})
	stream, err := x.sub.StreamingPull(x.f.owner)
	must(t, err)
	must(t, stream.Send(&pb.StreamingPullRequest{Subscription: ordered, StreamAckDeadlineSeconds: 10, MaxOutstandingMessages: 1}))
	response, err := stream.Recv()
	must(t, err)
	if !response.GetSubscriptionProperties().GetMessageOrderingEnabled() {
		t.Fatal("stream did not report message ordering")
	}
	seen := data(response.GetReceivedMessages())
	ids := []string{}
	for _, message := range response.GetReceivedMessages() {
		ids = append(ids, message.GetAckId())
	}
	must(t, stream.Send(&pb.StreamingPullRequest{ModifyDeadlineAckIds: ids[:1], ModifyDeadlineSeconds: []int32{0}}))
	response, err = stream.Recv()
	must(t, err)
	if got := data(response.GetReceivedMessages()); len(got) == 0 || got[0] != "1" {
		t.Fatalf("stream redelivery after nacking %v = %v, want 1 first", seen, got)
	}
	must(t, stream.CloseSend())
}

// TestSnapshotsAndSeekThroughEmulator covers snapshot CRUD, labels, and seek.
func TestSnapshotsAndSeekThroughEmulator(t *testing.T) {
	x := newFeatures(t)
	topic := x.topic("replayed")
	subscription := x.subscription(&pb.Subscription{Name: testSub, Topic: topic, RetainAckedMessages: true})
	x.publish(topic, &pb.PubsubMessage{Data: []byte("before")})
	consumed := x.pull(subscription, 10, 5*time.Second)
	must(t, x.ack(subscription, consumed...))
	snapshot, err := x.sub.CreateSnapshot(x.f.owner, &pb.CreateSnapshotRequest{Name: snapshotName, Subscription: subscription, Labels: map[string]string{"stage": "test"}})
	must(t, err)
	if snapshot.GetTopic() != topic || snapshot.GetLabels()["stage"] != "test" {
		t.Fatalf("snapshot = %v", snapshot)
	}
	got, err := x.sub.GetSnapshot(x.f.owner, &pb.GetSnapshotRequest{Snapshot: snapshotName})
	must(t, err)
	if got.GetLabels()["stage"] != "test" {
		t.Fatalf("get snapshot = %v", got)
	}
	x.publish(topic, &pb.PubsubMessage{Data: []byte("after")})
	must(t, x.ack(subscription, x.pull(subscription, 10, 5*time.Second)...))
	_, err = x.sub.Seek(x.f.owner, &pb.SeekRequest{Subscription: subscription, Target: &pb.SeekRequest_Snapshot{Snapshot: snapshotName}})
	must(t, err)
	if replayed := data(x.pull(subscription, 10, 5*time.Second)); len(replayed) != 1 || replayed[0] != "after" {
		t.Fatalf("seek to snapshot replayed %v, want only the message acked after it", replayed)
	}
	_, err = x.sub.Seek(x.f.owner, &pb.SeekRequest{Subscription: subscription, Target: &pb.SeekRequest_Time{Time: timestamppb.New(time.Now().Add(-time.Minute))}})
	must(t, err)
	if replayed := x.pull(subscription, 10, 5*time.Second); len(replayed) < 2 {
		t.Fatalf("seek to time replayed %v, want both retained messages", data(replayed))
	}
	_, err = x.sub.Seek(x.f.other, &pb.SeekRequest{Subscription: subscription, Target: &pb.SeekRequest_Time{Time: timestamppb.Now()}})
	expectCode(t, err, codes.PermissionDenied)
	_, err = x.sub.DeleteSnapshot(x.f.owner, &pb.DeleteSnapshotRequest{Snapshot: snapshotName})
	must(t, err)
	_, err = x.sub.GetSnapshot(x.f.owner, &pb.GetSnapshotRequest{Snapshot: snapshotName})
	expectCode(t, err, codes.NotFound)
}
