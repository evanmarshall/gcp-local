package pubsub

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// expectRefusal asserts both the code and the message of a refusal.
func expectRefusal(t *testing.T, name string, err error, code codes.Code, message string) {
	t.Helper()
	if status.Code(err) != code || !strings.Contains(status.Convert(err).Message(), message) {
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
		{"/v1/projects/project-a/subscriptions/rest-unknown", `{"topic":"rest-topic","notAField":1}`, http.StatusBadRequest, "Invalid subscription body"},
		{"/v1/projects/project-a/topics/rest-unknown", `{"notAField":1}`, http.StatusBadRequest, "Invalid request body"},
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
