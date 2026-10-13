package pubsub

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	deadLetterTopic = "projects/project-a/topics/dead-letters"
	snapshotName    = "projects/project-a/snapshots/checkpoint"
	pubsubAgent     = "serviceAccount:service-123456@gcp-sa-pubsub.iam.gserviceaccount.com"
	reader          = "serviceAccount:reader@project-a.iam.gserviceaccount.com"
)

// expectRefusal asserts both the code and the message of a refusal. A
// denial's message names no permission (recorded); "Missing permission: X"
// then matches the permission its ErrorInfo names.
func expectRefusal(t *testing.T, name string, err error, code codes.Code, message string) {
	t.Helper()
	var text strings.Builder
	text.WriteString(status.Convert(err).Message())
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			text.WriteString(" Missing permission: " + info.GetMetadata()["permission"])
		}
	}
	if status.Code(err) != code || !strings.Contains(text.String(), message) {
		t.Errorf("%s: got %v (%v), want %v containing %q", name, status.Code(err), err, code, message)
	}
}

// deliveryFixture creates the topic, a dead-letter topic, and returns the
// memory-backed provider.
func deliveryFixture(t *testing.T) (*Provider, *memoryPubSub, func(method, resource string, request proto.Message) (any, error), *gateway.Registry) {
	t.Helper()
	p, registry, b := memoryFixture(t)
	as := func(method, resource string, request proto.Message) (any, error) {
		return invokeAs(registry, owner, method, resource, request)
	}
	for _, topic := range []string{testTopic, deadLetterTopic} {
		_, err := as(publisher+"CreateTopic", topic, &pb.Topic{Name: topic})
		must(t, err)
	}
	return p, b, as, registry
}

// typed asserts a dispatch result's concrete type.
func typed[T any](t *testing.T, value any) T {
	t.Helper()
	out, ok := value.(T)
	if !ok {
		t.Fatalf("got %T, want %T", value, out)
	}
	return out
}

// expectForwarded checks whether the backend holds the dead-letter policy and
// how many times the provider changed it.
func expectForwarded(t *testing.T, b *memoryPubSub, forwarded bool, updates int, why string) {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	policy := b.subscriptions[testSub].GetDeadLetterPolicy()
	active := policy.GetDeadLetterTopic() == deadLetterTopic && policy.GetMaxDeliveryAttempts() == 5
	if active != forwarded || (!forwarded && policy != nil) || len(b.updates) != updates {
		t.Fatalf("%s: backend policy %v after %d updates", why, policy, len(b.updates))
	}
}

// TestDeadLetterPolicyFollowsServiceAgentGrants models GCP's rule that
// messages are forwarded only while the Pub/Sub service agent can publish to
// the dead-letter topic and acknowledge on the source.
func TestDeadLetterPolicyFollowsServiceAgentGrants(t *testing.T) {
	p, b, as, _ := deliveryFixture(t)
	request := &pb.Subscription{Name: testSub, Topic: testTopic, DeadLetterPolicy: &pb.DeadLetterPolicy{DeadLetterTopic: deadLetterTopic}}
	created, err := as(subscriber+"CreateSubscription", testSub, request)
	must(t, err)
	if got := typed[*pb.Subscription](t, created).GetDeadLetterPolicy(); got.GetMaxDeliveryAttempts() != 5 {
		t.Fatalf("create echoed %v, want the requested policy with the default 5 attempts", got)
	}
	expectForwarded(t, b, false, 0, "created without grants")
	pull := func() {
		t.Helper()
		_, err := as(subscriber+"Pull", testSub, &pb.PullRequest{Subscription: testSub, MaxMessages: 1})
		must(t, err)
	}
	pull()
	expectForwarded(t, b, false, 0, "pulled without grants")
	publisherGrant, err := p.iam.SetPolicy(canonical(deadLetterTopic), iam.Policy{Bindings: []iam.Binding{{Role: "roles/pubsub.publisher", Members: []string{pubsubAgent}}}})
	must(t, err)
	pull()
	expectForwarded(t, b, false, 0, "publisher alone; the agent also needs subscriber on the source")
	_, err = p.iam.SetPolicy(canonical(testSub), iam.Policy{Bindings: []iam.Binding{{Role: roleSubscriber, Members: []string{pubsubAgent}}}})
	must(t, err)
	pull()
	expectForwarded(t, b, true, 1, "both grants")
	_, err = p.iam.SetPolicy(canonical(deadLetterTopic), iam.Policy{ETag: publisherGrant.ETag})
	must(t, err)
	_, err = as(subscriber+"ModifyAckDeadline", testSub, &pb.ModifyAckDeadlineRequest{Subscription: testSub, AckIds: []string{"none"}})
	must(t, err)
	expectForwarded(t, b, false, 2, "publisher grant revoked before a nack")
	got, err := as(subscriber+"GetSubscription", testSub, &pb.GetSubscriptionRequest{Subscription: testSub})
	must(t, err)
	if typed[*pb.Subscription](t, got).GetDeadLetterPolicy().GetDeadLetterTopic() != deadLetterTopic {
		t.Fatal("get dropped the requested dead-letter policy while it was inactive")
	}
}

// pullOne pulls exactly one message through the provider.
func pullOne(t *testing.T, as func(string, string, proto.Message) (any, error), subscription string) *pb.ReceivedMessage {
	t.Helper()
	pulled, err := as(subscriber+"Pull", subscription, &pb.PullRequest{Subscription: subscription, MaxMessages: 1})
	must(t, err)
	received := typed[*pb.PullResponse](t, pulled).GetReceivedMessages()
	if len(received) != 1 {
		t.Fatalf("%s: pulled %d messages", subscription, len(received))
	}
	return received[0]
}

// TestUngrantedDeadLetterStillCountsAttempts follows the
// pubsub-delivery-features recording: without the service agent's grants GCP
// reports delivery attempts 1 through 7 past max_delivery_attempts 5 and
// never forwards. The provider counts while the emulator lacks the policy.
func TestUngrantedDeadLetterStillCountsAttempts(t *testing.T) {
	p, b, as, _ := deliveryFixture(t)
	_, err := as(subscriber+"CreateSubscription", testSub, &pb.Subscription{Name: testSub, Topic: testTopic,
		DeadLetterPolicy: &pb.DeadLetterPolicy{DeadLetterTopic: deadLetterTopic, MaxDeliveryAttempts: 5}})
	must(t, err)
	publish := func(value string) {
		t.Helper()
		_, err := as(publisher+"Publish", testTopic, &pb.PublishRequest{Topic: testTopic, Messages: []*pb.PubsubMessage{{Data: []byte(value)}}})
		must(t, err)
	}
	publish("poison")
	for want := int32(1); want <= 7; want++ {
		received := pullOne(t, as, testSub)
		if received.GetDeliveryAttempt() != want {
			t.Fatalf("ungranted delivery attempt = %d, want %d", received.GetDeliveryAttempt(), want)
		}
		_, err := as(subscriber+"ModifyAckDeadline", testSub, &pb.ModifyAckDeadlineRequest{Subscription: testSub, AckIds: []string{received.GetAckId()}})
		must(t, err)
	}
	expectForwarded(t, b, false, 0, "attempts past the limit without grants")
	_, err = as(subscriber+"Acknowledge", testSub, &pb.AcknowledgeRequest{Subscription: testSub, AckIds: []string{pullOne(t, as, testSub).GetAckId()}})
	must(t, err)
	publish("next")
	if got := pullOne(t, as, testSub).GetDeliveryAttempt(); got != 1 {
		t.Fatalf("a new message started at attempt %d, want 1", got)
	}
	// A count the emulator stamped while it held the policy is kept.
	received := []*pb.ReceivedMessage{{AckId: "counted", DeliveryAttempt: 3, Message: &pb.PubsubMessage{MessageId: "emulator"}}}
	p.track(testSub, received)
	if received[0].GetDeliveryAttempt() != 3 {
		t.Fatalf("emulator attempt count rewritten to %d", received[0].GetDeliveryAttempt())
	}
}

// TestExactlyOnceRepeatedAckSucceeds follows the pubsub-delivery-features
// recording: GCP answers OK to a unary Acknowledge of an already acknowledged
// ack ID. Other ack IDs still reach the emulator, which fails unknown ones.
func TestExactlyOnceRepeatedAckSucceeds(t *testing.T) {
	_, b, as, _ := deliveryFixture(t)
	_, err := as(subscriber+"CreateSubscription", testSub, &pb.Subscription{Name: testSub, Topic: testTopic, EnableExactlyOnceDelivery: true})
	must(t, err)
	_, err = as(publisher+"Publish", testTopic, &pb.PublishRequest{Topic: testTopic, Messages: []*pb.PubsubMessage{{Data: []byte("once")}}})
	must(t, err)
	ackID := pullOne(t, as, testSub).GetAckId()
	ack := func(ids ...string) {
		t.Helper()
		_, err := as(subscriber+"Acknowledge", testSub, &pb.AcknowledgeRequest{Subscription: testSub, AckIds: ids})
		must(t, err)
	}
	ack(ackID)
	ack(ackID)
	ack(ackID, "unknown")
	b.mu.Lock()
	defer b.mu.Unlock()
	if strings.Join(b.acked, ",") != ackID+",unknown" {
		t.Fatalf("backend acknowledgements = %v, want the first ack and the unknown ID once each", b.acked)
	}
}

func TestDeliveryRefusals(t *testing.T) {
	_, b, as, _ := deliveryFixture(t)
	sub := func(edit func(*pb.Subscription)) *pb.Subscription {
		s := &pb.Subscription{Name: testSub, Topic: testTopic}
		edit(s)
		return s
	}
	deadLetter := func(topic string, attempts int32) *pb.Subscription {
		return sub(func(s *pb.Subscription) {
			s.DeadLetterPolicy = &pb.DeadLetterPolicy{DeadLetterTopic: topic, MaxDeliveryAttempts: attempts}
		})
	}
	retry := func(minimum, maximum time.Duration) *pb.Subscription {
		return sub(func(s *pb.Subscription) {
			s.RetryPolicy = &pb.RetryPolicy{MinimumBackoff: durationpb.New(minimum), MaximumBackoff: durationpb.New(maximum)}
		})
	}
	before := b.total()
	tests := []struct {
		name     string
		method   string
		resource string
		request  proto.Message
		code     codes.Code
		message  string
	}{
		{"too few attempts", subscriber + "CreateSubscription", testSub, deadLetter(deadLetterTopic, 4), codes.OutOfRange, "the minimum value is 5"},
		{"too many attempts", subscriber + "CreateSubscription", testSub, deadLetter(deadLetterTopic, 101), codes.OutOfRange, "the maximum value is 100"},
		{"cross-project dead letter", subscriber + "CreateSubscription", testSub, deadLetter(foreignTopic, 5), codes.Unimplemented, "Pub/Sub coverage gap: cross-project dead-letter topics"},
		{"absent dead letter", subscriber + "CreateSubscription", testSub, deadLetter(absentTopic, 5), codes.NotFound, "Dead letter topic not found"},
		{"backoff range", subscriber + "CreateSubscription", testSub, retry(700*time.Second, 700*time.Second), codes.InvalidArgument, "Retry backoff must be 0 through 600 seconds"},
		{"backoff order", subscriber + "CreateSubscription", testSub, retry(20*time.Second, 10*time.Second), codes.InvalidArgument, "Minimum backoff must not exceed maximum backoff"},
		{"expiration ttl", subscriber + "CreateSubscription", testSub, sub(func(s *pb.Subscription) {
			s.ExpirationPolicy = &pb.ExpirationPolicy{Ttl: durationpb.New(24 * time.Hour)}
		}), codes.Unimplemented, "Pub/Sub coverage gap: subscription expiration TTL"},
		{"long filter", subscriber + "CreateSubscription", testSub, sub(func(s *pb.Subscription) { s.Filter = `attributes.k = "` + strings.Repeat("v", 250) + `"` }), codes.InvalidArgument, "Filter must be at most 256 bytes"},
		{"exactly-once push", subscriber + "CreateSubscription", testSub, sub(func(s *pb.Subscription) {
			s.EnableExactlyOnceDelivery = true
			s.PushConfig = pushOIDC("http://127.0.0.1:9/push", "push@project-a.iam.gserviceaccount.com", "")
		}), codes.Unimplemented, "Pub/Sub coverage gap: exactly-once delivery on push subscriptions"},
		{"seek without target", subscriber + "Seek", testSub, &pb.SeekRequest{Subscription: testSub}, codes.InvalidArgument, "Seek requires a time or a snapshot"},
		{"cross-project snapshot source", subscriber + "CreateSnapshot", snapshotName, &pb.CreateSnapshotRequest{Name: snapshotName, Subscription: "projects/project-b/subscriptions/foreign"}, codes.Unimplemented, "Pub/Sub coverage gap: cross-project subscriptions"},
		{"absent snapshot source", subscriber + "CreateSnapshot", snapshotName, &pb.CreateSnapshotRequest{Name: snapshotName, Subscription: testSub}, codes.NotFound, "Subscription does not exist"},
		{"snapshot iam policy", getIAM, snapshotName, &iampb.GetIamPolicyRequest{Resource: snapshotName}, codes.Unimplemented, "Pub/Sub coverage gap: snapshot IAM policies"},
	}
	for _, test := range tests {
		_, err := as(test.method, test.resource, test.request)
		expectRefusal(t, test.name, err, test.code, test.message)
	}
	if b.total() != before {
		t.Fatalf("refused requests reached the backend: %d calls", b.total()-before)
	}
	// The emulator answers an unparsable filter with UNKNOWN.
	_, err := as(subscriber+"CreateSubscription", testSub, sub(func(s *pb.Subscription) { s.Filter = "unparsable" }))
	expectRefusal(t, "unparsable filter", err, codes.InvalidArgument, "Invalid filter expression")
}

// TestNackDeadlines covers the provider's nack rewrite: an ordered nack waits
// one second, and a retry policy doubles the backoff per consecutive nack.
func TestNackDeadlines(t *testing.T) {
	p, b, as, _ := deliveryFixture(t)
	ordered := "projects/project-a/subscriptions/ordered"
	_, err := as(subscriber+"CreateSubscription", ordered, &pb.Subscription{Name: ordered, Topic: testTopic, EnableMessageOrdering: true})
	must(t, err)
	_, err = as(subscriber+"CreateSubscription", testSub, &pb.Subscription{Name: testSub, Topic: testTopic,
		RetryPolicy: &pb.RetryPolicy{MinimumBackoff: durationpb.New(10 * time.Second), MaximumBackoff: durationpb.New(25 * time.Second)}})
	must(t, err)
	_, err = as(publisher+"Publish", testTopic, &pb.PublishRequest{Topic: testTopic, Messages: []*pb.PubsubMessage{{Data: []byte("m"), OrderingKey: "k"}}})
	must(t, err)
	nack := func(subscription string) int32 {
		t.Helper()
		received := pullOne(t, as, subscription)
		_, err := as(subscriber+"ModifyAckDeadline", subscription, &pb.ModifyAckDeadlineRequest{Subscription: subscription, AckIds: []string{received.GetAckId()}})
		must(t, err)
		return b.modacks[len(b.modacks)-1].GetAckDeadlineSeconds()
	}
	if got := nack(ordered); got != 1 {
		t.Fatalf("ordered nack deadline = %d, want 1", got)
	}
	if got := nack(testSub); got != 10 {
		t.Fatalf("first retry backoff = %d, want 10", got)
	}
	// The memory backend redelivers only zero deadlines, so redelivery of the
	// same message is replayed through the tracker.
	for attempt, want := range []int32{20, 25} {
		ackID := "redelivery-" + string(rune('a'+attempt))
		p.track(testSub, []*pb.ReceivedMessage{{AckId: ackID, Message: &pb.PubsubMessage{MessageId: "1"}}})
		if got := p.nackDeadline(testSub, ackID); got != want {
			t.Fatalf("backoff after %d failures = %d, want %d", attempt+2, got, want)
		}
	}
	if got := p.nackDeadline(testSub, "untracked"); got != 10 {
		t.Fatalf("untracked nack backoff = %d, want the minimum 10", got)
	}
	p.track(testSub, []*pb.ReceivedMessage{{AckId: "acked", Message: &pb.PubsubMessage{MessageId: "1"}}})
	p.acknowledged(testSub, []string{"acked"})
	if d := p.deliveryOf(testSub); len(d.failures) != 0 || len(d.messages) != 0 {
		t.Fatalf("acknowledgement kept retry state: %+v", d)
	}
}

// TestNacksSplitByDeadline proves one ModifyAckDeadline with different
// rewritten deadlines becomes one backend call per deadline.
func TestNacksSplitByDeadline(t *testing.T) {
	p, b, as, _ := deliveryFixture(t)
	_, err := as(subscriber+"CreateSubscription", testSub, &pb.Subscription{Name: testSub, Topic: testTopic,
		RetryPolicy: &pb.RetryPolicy{MinimumBackoff: durationpb.New(10 * time.Second), MaximumBackoff: durationpb.New(25 * time.Second)}})
	must(t, err)
	// Nacks with different deadlines split into one backend call each.
	before := len(b.modacks)
	p.track(testSub, []*pb.ReceivedMessage{{AckId: "first", Message: &pb.PubsubMessage{MessageId: "2"}}, {AckId: "second", Message: &pb.PubsubMessage{MessageId: "3"}}})
	p.nackDeadline(testSub, "first")
	p.track(testSub, []*pb.ReceivedMessage{{AckId: "first-again", Message: &pb.PubsubMessage{MessageId: "2"}}})
	_, err = as(subscriber+"ModifyAckDeadline", testSub, &pb.ModifyAckDeadlineRequest{Subscription: testSub, AckIds: []string{"first-again", "second"}})
	must(t, err)
	if split := b.modacks[before:]; len(split) != 2 || split[0].GetAckDeadlineSeconds() != 10 || split[1].GetAckDeadlineSeconds() != 20 {
		t.Fatalf("split nacks = %v", split)
	}
}

func TestSnapshotLifecycleAndAuthorization(t *testing.T) {
	p, b, as, registry := deliveryFixture(t)
	_, err := as(subscriber+"CreateSubscription", testSub, &pb.Subscription{Name: testSub, Topic: testTopic, RetainAckedMessages: true})
	must(t, err)
	_, err = p.iam.SetPolicy(canonical(testSub), iam.Policy{Bindings: []iam.Binding{{Role: roleSubscriber, Members: []string{reader}}}})
	must(t, err)
	create := &pb.CreateSnapshotRequest{Name: snapshotName, Subscription: testSub, Labels: map[string]string{"stage": "test"}}
	_, err = invokeAs(registry, reader, subscriber+"CreateSnapshot", snapshotName, create)
	expectRefusal(t, "subscriber creating a snapshot", err, codes.PermissionDenied, "Missing permission: pubsub.snapshots.create")
	if b.called(subscriber+"CreateSnapshot") != 0 {
		t.Fatal("denied snapshot creation reached the backend")
	}
	created, err := as(subscriber+"CreateSnapshot", snapshotName, create)
	must(t, err)
	if typed[*pb.Snapshot](t, created).GetLabels()["stage"] != "test" {
		t.Fatalf("snapshot labels = %v", created)
	}
	listed, err := as(subscriber+"ListSnapshots", "projects/project-a", &pb.ListSnapshotsRequest{Project: "projects/project-a"})
	must(t, err)
	if snapshots := typed[*pb.ListSnapshotsResponse](t, listed).GetSnapshots(); len(snapshots) != 1 || snapshots[0].GetLabels()["stage"] != "test" {
		t.Fatalf("listed snapshots = %v", snapshots)
	}
	seek := &pb.SeekRequest{Subscription: testSub, Target: &pb.SeekRequest_Snapshot{Snapshot: snapshotName}}
	// Subscriber on the subscription grants consume and snapshots.seek, but
	// snapshots.seek is evaluated on the snapshot, where reader has no grant.
	_, err = invokeAs(registry, reader, subscriber+"Seek", testSub, seek)
	expectRefusal(t, "seek without a snapshot grant", err, codes.PermissionDenied, "Missing permission: pubsub.snapshots.seek")
	_, err = p.iam.SetPolicy(canonical(snapshotName), iam.Policy{Bindings: []iam.Binding{{Role: roleSubscriber, Members: []string{reader}}}})
	must(t, err)
	_, err = invokeAs(registry, reader, subscriber+"Seek", testSub, seek)
	must(t, err)
	_, err = invokeAs(registry, reader, subscriber+"Seek", testSub, &pb.SeekRequest{Subscription: testSub, Target: &pb.SeekRequest_Time{Time: timestamppb.Now()}})
	must(t, err)
	if b.called(subscriber+"Seek") != 2 {
		t.Fatal("seek did not reach the backend exactly twice")
	}
	_, err = as(subscriber+"Seek", testSub, &pb.SeekRequest{Subscription: testSub, Target: &pb.SeekRequest_Snapshot{Snapshot: "projects/project-a/snapshots/absent"}})
	expectRefusal(t, "absent snapshot", err, codes.NotFound, "Snapshot does not exist")
	// A deny on consume blocks snapshot creation even for the project admin.
	_, err = p.iam.SetPolicy(canonical(testSub), iam.Policy{Version: 3, DenyRules: []iam.DenyRule{{DeniedPrincipals: []string{owner}, DeniedPermissions: []string{"pubsub.subscriptions.consume"}}}})
	must(t, err)
	second := "projects/project-a/snapshots/second"
	_, err = as(subscriber+"CreateSnapshot", second, &pb.CreateSnapshotRequest{Name: second, Subscription: testSub})
	expectRefusal(t, "snapshot of a denied source", err, codes.PermissionDenied, "pubsub.subscriptions.consume")
	_, err = as(subscriber+"DeleteSnapshot", snapshotName, &pb.DeleteSnapshotRequest{Snapshot: snapshotName})
	must(t, err)
	if _, found := p.index.Get(canonical(snapshotName)); found {
		t.Fatal("deleted snapshot stayed indexed")
	}
	if _, found := p.labels.Load(canonical(snapshotName)); found {
		t.Fatal("deleted snapshot kept its labels")
	}
}

func TestResetClearsDeliveryStateAndLabels(t *testing.T) {
	p, _, as, _ := deliveryFixture(t)
	_, err := as(subscriber+"CreateSubscription", testSub, &pb.Subscription{Name: testSub, Topic: testTopic, Labels: map[string]string{"a": "b"}, EnableMessageOrdering: true})
	must(t, err)
	must(t, p.manager.Reset(context.Background(), keyOf(call("", testSub, owner))))
	if p.deliveryOf(testSub) != nil {
		t.Fatal("reset kept delivery state")
	}
	if _, found := p.labels.Load(canonical(testSub)); found {
		t.Fatal("reset kept subscription labels")
	}
}

// TestRESTDeliveryFields covers the REST subscription body: delivery settings
// round-trip, unknown JSON fails, and known unsupported fields are gaps.
func TestRESTDeliveryFields(t *testing.T) {
	_, registry, b := memoryFixture(t)
	c := newRESTClient(t, registry)
	c.do(owner, http.MethodPut, restTopicPath, `{}`, 200)
	c.do(owner, http.MethodPut, "/v1/projects/project-a/topics/rest-dead-letters", `{}`, 200)
	body := `{"topic":"rest-topic","deadLetterPolicy":{"deadLetterTopic":"projects/project-a/topics/rest-dead-letters","maxDeliveryAttempts":7},` +
		`"retryPolicy":{"minimumBackoff":"5s","maximumBackoff":"60s"},"filter":"attributes.k = \"v\"","enableMessageOrdering":true,` +
		`"enableExactlyOnceDelivery":true,"retainAckedMessages":true,"expirationPolicy":{}}`
	sub := c.do(owner, http.MethodPut, restSubPath, body, 200)
	encoded, err := json.Marshal(sub)
	must(t, err)
	for _, want := range []string{`"deadLetterPolicy":{"deadLetterTopic":"projects/project-a/topics/rest-dead-letters","maxDeliveryAttempts":7}`,
		`"retryPolicy":{"maximumBackoff":"60s","minimumBackoff":"5s"}`, `"filter":"attributes.k = \"v\""`, `"enableMessageOrdering":true`,
		`"enableExactlyOnceDelivery":true`, `"retainAckedMessages":true`} {
		if !strings.Contains(string(encoded), want) {
			t.Errorf("REST subscription lacks %s: %s", want, encoded)
		}
	}
	before := b.total()
	refusals := []struct {
		path, body string
		code       int
		message    string
	}{
		{"/v1/projects/project-a/subscriptions/rest-unknown", `{"topic":"rest-topic","notAField":1}`, http.StatusBadRequest, `Invalid JSON payload received. Unknown name "notAField": Cannot find field.`},
		{"/v1/projects/project-a/topics/rest-unknown", `{"notAField":1}`, http.StatusBadRequest, `Invalid JSON payload received. Unknown name "notAField": Cannot find field.`},
		// Recorded in pubsub-rest-data-plane.
		{"/v1/projects/project-a/topics/rest-mistyped", `{"labels":"x"}`, http.StatusBadRequest, `Invalid value at 'labels' (type.googleapis.com/google.pubsub.v1.Topic.LabelsEntry), "x"`},
		{"/v1/projects/project-a/subscriptions/rest-bq", `{"topic":"rest-topic","bigqueryConfig":{"table":"p.d.t"}}`, http.StatusNotImplemented, "Pub/Sub coverage gap: field bigquery_config"},
		{"/v1/projects/project-a/topics/rest-kms", `{"kmsKeyName":"projects/p/locations/l/keyRings/r/cryptoKeys/k"}`, http.StatusNotImplemented, "Pub/Sub coverage gap: field kms_key_name"},
	}
	for _, refusal := range refusals {
		body := c.do(owner, http.MethodPut, refusal.path, refusal.body, refusal.code)
		if detail, _ := body["error"].(map[string]any); detail["message"] != refusal.message {
			t.Errorf("%s: error = %v, want %q", refusal.path, body["error"], refusal.message)
		}
	}
	if b.total() != before {
		t.Fatal("refused REST bodies reached the backend")
	}
}

func TestPushBodyCarriesDeliveryAttempt(t *testing.T) {
	body, ok := pushBody(testSub, &pb.ReceivedMessage{DeliveryAttempt: 3, Message: &pb.PubsubMessage{Data: []byte("x")}})
	if !ok || !strings.Contains(string(body), `"deliveryAttempt":3`) {
		t.Fatalf("push body = %s", body)
	}
	body, _ = pushBody(testSub, &pb.ReceivedMessage{Message: &pb.PubsubMessage{Data: []byte("x")}})
	if strings.Contains(string(body), "deliveryAttempt") {
		t.Fatalf("push body without dead-lettering carries an attempt: %s", body)
	}
}

const pushWorkerEmail = "push-worker@project-a.iam.gserviceaccount.com"

// pushFixture configures the ID-token issuer, a same-project push account, and
// the grants the creator (actAs) and the Pub/Sub service agent (mint) need.
func pushFixture(t *testing.T) (*Provider, *gateway.Registry, *memoryPubSub) {
	t.Helper()
	p, registry, b := memoryFixture(t)
	var err error
	p.tokens, err = identity.NewIDTokens(p.iam, p.index)
	must(t, err)
	account := iam.AccountResource(testProject, pushWorkerEmail)
	must(t, p.index.Put("iam", state.Resource{Name: account, Service: "iam", Kind: "serviceAccount", Number: "123456789012345678901", Parent: provider.ProjectResource(testProject), Scope: state.Scope{Project: testProject, Location: state.Global}}))
	_, err = p.iam.SetPolicy(account, iam.Policy{Bindings: []iam.Binding{
		{Role: "roles/iam.serviceAccountUser", Members: []string{owner}},
		{Role: "roles/iam.serviceAccountTokenCreator", Members: []string{"serviceAccount:service-123456@gcp-sa-pubsub.iam.gserviceaccount.com"}},
	}})
	must(t, err)
	_, err = invokeAs(registry, owner, publisher+"CreateTopic", testTopic, &pb.Topic{Name: testTopic})
	must(t, err)
	return p, registry, b
}

type pushed struct{ subscription, data string }

// pushReceiver verifies each push's OIDC token and envelope, reports it, and
// fails the first delivery so the worker must redeliver.
func pushReceiver(p *Provider, deliveries chan<- pushed) http.Handler {
	var attempts atomic.Int32
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, err := p.tokens.Verify(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "), "push-test")
		if err != nil || claims.Email != pushWorkerEmail || r.Header.Get("Content-Type") != "application/json" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var body struct {
			Subscription string `json:"subscription"`
			Message      struct {
				Data string `json:"data"`
			} `json:"message"`
		}
		if json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body) != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		data, _ := base64.StdEncoding.DecodeString(body.Message.Data)
		deliveries <- pushed{body.Subscription, string(data)}
		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
}

// TestPushWorkerDeliversRetriesAndStops proves the provider-owned worker posts
// an authenticated envelope, redelivers after a failed push, acknowledges an
// accepted one, echoes the push config, and stops when the subscription goes.
func TestPushWorkerDeliversRetriesAndStops(t *testing.T) {
	p, registry, b := pushFixture(t)
	deliveries := make(chan pushed, 10)
	receiver := httptest.NewServer(pushReceiver(p, deliveries))
	defer receiver.Close()

	config := pushOIDC(receiver.URL+"/push", pushWorkerEmail, "push-test")
	_, err := invokeAs(registry, owner, subscriber+"CreateSubscription", testSub, &pb.Subscription{Name: testSub, Topic: testTopic, PushConfig: config})
	must(t, err)
	got, err := invokeAs(registry, owner, subscriber+"GetSubscription", testSub, &pb.GetSubscriptionRequest{Subscription: testSub})
	must(t, err)
	if subscription, _ := got.(*pb.Subscription); subscription.GetPushConfig().GetPushEndpoint() != receiver.URL+"/push" {
		t.Fatalf("push config was not echoed: %v", got)
	}
	_, err = invokeAs(registry, owner, subscriber+"Pull", testSub, &pb.PullRequest{Subscription: testSub, MaxMessages: 1})
	expectCode(t, err, codes.FailedPrecondition)
	_, err = invokeAs(registry, owner, publisher+"Publish", testTopic, &pb.PublishRequest{Topic: testTopic, Messages: []*pb.PubsubMessage{{Data: []byte("pushed")}}})
	must(t, err)
	for attempt := 1; attempt <= 2; attempt++ {
		select {
		case delivery := <-deliveries:
			if delivery.subscription != testSub || delivery.data != "pushed" {
				t.Fatalf("delivery %d = %+v", attempt, delivery)
			}
		case <-time.After(8 * time.Second):
			t.Fatalf("push delivery %d timed out", attempt)
		}
	}
	waitFor(t, func() bool { return b.called(subscriber+"Acknowledge") == 1 })
	if b.called(subscriber+"ModifyAckDeadline") < 1 {
		t.Fatal("a failed push was not nacked for redelivery")
	}

	_, err = invokeAs(registry, owner, subscriber+"DeleteSubscription", testSub, &pb.DeleteSubscriptionRequest{Subscription: testSub})
	must(t, err)
	if _, running := p.pushes.Load(canonical(testSub)); running {
		t.Fatal("deleted subscription kept its push worker")
	}
}

// TestPushWorkersStopOnResetAndRespectTheLimit proves reset stops a running
// worker and the per-project worker limit is enforced before the backend.
func TestPushWorkersStopOnResetAndRespectTheLimit(t *testing.T) {
	p, registry, b := pushFixture(t)
	config := pushOIDC("http://127.0.0.1:9/push", pushWorkerEmail, "")
	_, err := invokeAs(registry, owner, subscriber+"CreateSubscription", testSub, &pb.Subscription{Name: testSub, Topic: testTopic, PushConfig: config})
	must(t, err)
	must(t, p.manager.Reset(context.Background(), keyOf(call("", testSub, owner))))
	if p.pushCount(scopeOf(call("", testSub, owner))) != 0 {
		t.Fatal("reset left a push worker running")
	}

	_, err = invokeAs(registry, owner, publisher+"CreateTopic", testTopic, &pb.Topic{Name: testTopic})
	must(t, err)
	scope := scopeOf(call("", testSub, owner))
	for i := range maxPushSubscriptions {
		p.pushes.Store(canonical(testSub)+string(rune('a'+i)), &pushSubscription{scope: scope, cancel: func() {}, done: closedChannel()})
	}
	before := b.called(subscriber + "CreateSubscription")
	_, err = invokeAs(registry, owner, subscriber+"CreateSubscription", testSub, &pb.Subscription{Name: testSub, Topic: testTopic, PushConfig: config})
	expectCode(t, err, codes.ResourceExhausted)
	if b.called(subscriber+"CreateSubscription") != before {
		t.Fatal("a push subscription over the limit reached the backend")
	}
}

// TestPushAccountMustBeSameProjectAndNumbered covers the push identity
// preconditions that fail closed as coverage gaps.
func TestPushAccountMustBeSameProjectAndNumbered(t *testing.T) {
	p, registry, b := pushFixture(t)
	foreign := pushOIDC("http://127.0.0.1:9/push", "unknown@project-a.iam.gserviceaccount.com", "")
	_, err := invokeAs(registry, owner, subscriber+"CreateSubscription", testSub, &pb.Subscription{Name: testSub, Topic: testTopic, PushConfig: foreign})
	if status.Code(err) != codes.Unimplemented || !strings.Contains(err.Error(), "push service account") {
		t.Fatalf("unknown push account = %v", err)
	}
	scope := state.Scope{Project: testProject, Location: state.Global}
	// Removing the project drops its policy; the unnumbered project gets it back.
	policy, err := p.iam.GetPolicy(provider.ProjectResource(testProject))
	must(t, err)
	policy.ETag = ""
	must(t, p.index.Remove(hierarchy, scope, provider.ProjectResource(testProject)))
	must(t, p.index.Put(hierarchy, state.Resource{Name: provider.ProjectResource(testProject), Service: hierarchy, Kind: kindProject, Scope: scope}))
	_, err = p.iam.SetPolicy(provider.ProjectResource(testProject), policy)
	must(t, err)
	_, err = invokeAs(registry, owner, subscriber+"CreateSubscription", testSub, &pb.Subscription{Name: testSub, Topic: testTopic, PushConfig: pushOIDC("http://127.0.0.1:9/push", pushWorkerEmail, "")})
	if status.Code(err) != codes.Unimplemented || !strings.Contains(err.Error(), "project number") {
		t.Fatalf("unnumbered project = %v", err)
	}
	if b.called(subscriber+"CreateSubscription") != 0 {
		t.Fatal("an unverifiable push identity reached the backend")
	}
}

// waitFor polls a condition for a few seconds.
func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

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
