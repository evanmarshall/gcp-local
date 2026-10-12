package pubsub

import (
	"context"
	"testing"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const consumer = "serviceAccount:consumer@project-a.iam.gserviceaccount.com"

// TestTopicAndSubscriptionLifecycle drives the gRPC control plane through
// create, duplicate create, get, and list with provider-owned labels.
func TestTopicAndSubscriptionLifecycle(t *testing.T) {
	_, registry, b := memoryFixture(t)
	_, err := invokeAs(registry, owner, publisher+"CreateTopic", testTopic, &pb.Topic{Name: testTopic, Labels: map[string]string{"env": "test"}})
	must(t, err)
	_, err = invokeAs(registry, owner, publisher+"CreateTopic", testTopic, &pb.Topic{Name: testTopic})
	expectCode(t, err, codes.AlreadyExists)
	if b.called(publisher+"CreateTopic") != 1 {
		t.Fatal("duplicate create reached the backend")
	}
	created, err := invokeAs(registry, owner, subscriber+"CreateSubscription", testSub, &pb.Subscription{Name: testSub, Topic: testTopic, AckDeadlineSeconds: 20, Labels: map[string]string{"team": "a"}})
	must(t, err)
	if subscriptionLabel(created) != "a" {
		t.Fatalf("create lost provider-owned labels: %v", created)
	}
	got, err := invokeAs(registry, owner, subscriber+"GetSubscription", testSub, &pb.GetSubscriptionRequest{Subscription: testSub})
	must(t, err)
	if subscriptionLabel(got) != "a" {
		t.Fatalf("get lost provider-owned labels: %v", got)
	}
	listed, err := invokeAs(registry, owner, subscriber+"ListSubscriptions", "projects/"+testProject, &pb.ListSubscriptionsRequest{Project: "projects/" + testProject})
	must(t, err)
	if list, _ := listed.(*pb.ListSubscriptionsResponse); len(list.GetSubscriptions()) != 1 || list.GetSubscriptions()[0].GetLabels()["team"] != "a" {
		t.Fatalf("list lost provider-owned labels: %v", listed)
	}
	topics, err := invokeAs(registry, owner, publisher+"ListTopics", "projects/"+testProject, &pb.ListTopicsRequest{Project: "projects/" + testProject})
	must(t, err)
	if list, _ := topics.(*pb.ListTopicsResponse); len(list.GetTopics()) != 1 {
		t.Fatalf("topic list = %v", topics)
	}
}

// TestDeleteReleasesGrantsAndLabels proves deleting a subscription or topic
// unindexes it and a recreated name inherits neither grants nor labels.
func TestDeleteReleasesGrantsAndLabels(t *testing.T) {
	p, registry, _ := memoryFixture(t)
	_, err := invokeAs(registry, owner, publisher+"CreateTopic", testTopic, &pb.Topic{Name: testTopic})
	must(t, err)
	_, err = invokeAs(registry, owner, subscriber+"CreateSubscription", testSub, &pb.Subscription{Name: testSub, Topic: testTopic, Labels: map[string]string{"team": "a"}})
	must(t, err)
	_, err = p.iam.SetPolicy(canonical(testSub), iam.Policy{Bindings: []iam.Binding{{Role: roleSubscriber, Members: []string{consumer}}}})
	must(t, err)
	_, err = invokeAs(registry, owner, subscriber+"DeleteSubscription", testSub, &pb.DeleteSubscriptionRequest{Subscription: testSub})
	must(t, err)
	if _, found := p.index.Get(canonical(testSub)); found {
		t.Fatal("deleted subscription stayed indexed")
	}
	_, err = invokeAs(registry, owner, subscriber+"GetSubscription", testSub, &pb.GetSubscriptionRequest{Subscription: testSub})
	expectCode(t, err, codes.NotFound)
	// Recreating the name must not inherit the deleted subscription's grants or labels.
	_, err = invokeAs(registry, owner, subscriber+"CreateSubscription", testSub, &pb.Subscription{Name: testSub, Topic: testTopic})
	must(t, err)
	policy, err := p.iam.GetPolicy(canonical(testSub))
	must(t, err)
	if len(policy.Bindings) != 0 {
		t.Fatalf("recreated subscription inherited grants: %+v", policy)
	}
	got, err := invokeAs(registry, owner, subscriber+"GetSubscription", testSub, &pb.GetSubscriptionRequest{Subscription: testSub})
	must(t, err)
	if label := subscriptionLabel(got); label != "" {
		t.Fatalf("recreated subscription inherited labels: %v", got)
	}
	_, err = invokeAs(registry, owner, publisher+"DeleteTopic", testTopic, &pb.DeleteTopicRequest{Topic: testTopic})
	must(t, err)
	if _, found := p.index.Get(canonical(testTopic)); found {
		t.Fatal("deleted topic stayed indexed")
	}
}

// TestPublishPullNackAndAck covers message flow: a nacked message is
// redelivered and acknowledged messages leave the subscription.
func TestPublishPullNackAndAck(t *testing.T) {
	_, registry, b := memoryFixture(t)
	_, err := invokeAs(registry, owner, publisher+"CreateTopic", testTopic, &pb.Topic{Name: testTopic})
	must(t, err)
	_, err = invokeAs(registry, owner, subscriber+"CreateSubscription", testSub, &pb.Subscription{Name: testSub, Topic: testTopic})
	must(t, err)
	_, err = invokeAs(registry, owner, publisher+"Publish", testTopic, &pb.PublishRequest{Topic: testTopic, Messages: []*pb.PubsubMessage{{Data: []byte("one")}, {Attributes: map[string]string{"k": "v"}}}})
	must(t, err)
	pulled, err := invokeAs(registry, owner, subscriber+"Pull", testSub, &pb.PullRequest{Subscription: testSub, MaxMessages: 1})
	must(t, err)
	first, ok := pulled.(*pb.PullResponse)
	if !ok || len(first.GetReceivedMessages()) != 1 || string(first.GetReceivedMessages()[0].GetMessage().GetData()) != "one" {
		t.Fatalf("pull = %v", pulled)
	}
	ackID := first.GetReceivedMessages()[0].GetAckId()
	_, err = invokeAs(registry, owner, subscriber+"ModifyAckDeadline", testSub, &pb.ModifyAckDeadlineRequest{Subscription: testSub, AckIds: []string{ackID}, AckDeadlineSeconds: 0})
	must(t, err)
	pulled, err = invokeAs(registry, owner, subscriber+"Pull", testSub, &pb.PullRequest{Subscription: testSub, MaxMessages: 5})
	must(t, err)
	again, _ := pulled.(*pb.PullResponse)
	if len(again.GetReceivedMessages()) != 2 {
		t.Fatalf("nacked message was not redelivered: %v", again)
	}
	ids := make([]string, 0, len(again.GetReceivedMessages()))
	for _, message := range again.GetReceivedMessages() {
		ids = append(ids, message.GetAckId())
	}
	_, err = invokeAs(registry, owner, subscriber+"Acknowledge", testSub, &pb.AcknowledgeRequest{Subscription: testSub, AckIds: ids})
	must(t, err)

	pulled, err = invokeAs(registry, owner, subscriber+"Pull", testSub, &pb.PullRequest{Subscription: testSub, MaxMessages: 5})
	must(t, err)
	if empty, _ := pulled.(*pb.PullResponse); len(empty.GetReceivedMessages()) != 0 || b.called(subscriber+"Acknowledge") != 1 {
		t.Fatalf("acknowledged messages were redelivered: %v", empty)
	}
}

// TestResourceGrantsAreScopedToOneResource proves a subscription grant does not
// extend to its topic and a consumer cannot administer what it consumes.
func TestResourceGrantsAreScopedToOneResource(t *testing.T) {
	p, registry, b := memoryFixture(t)
	_, err := invokeAs(registry, owner, publisher+"CreateTopic", testTopic, &pb.Topic{Name: testTopic})
	must(t, err)
	_, err = invokeAs(registry, owner, subscriber+"CreateSubscription", testSub, &pb.Subscription{Name: testSub, Topic: testTopic})
	must(t, err)
	_, err = p.iam.SetPolicy(canonical(testSub), iam.Policy{Bindings: []iam.Binding{{Role: roleSubscriber, Members: []string{consumer}}}})
	must(t, err)
	before := b.total()
	_, err = invokeAs(registry, consumer, subscriber+"Pull", testSub, &pb.PullRequest{Subscription: testSub, MaxMessages: 1})
	must(t, err)
	if b.total() != before+1 {
		t.Fatal("granted pull did not reach the backend once")
	}
	before = b.total()
	for _, denied := range []struct {
		method, resource string
		request          any
	}{
		{publisher + "Publish", testTopic, &pb.PublishRequest{Topic: testTopic, Messages: []*pb.PubsubMessage{{Data: []byte("x")}}}},
		{subscriber + "DeleteSubscription", testSub, &pb.DeleteSubscriptionRequest{Subscription: testSub}},
		{subscriber + "GetSubscription", testSub, &pb.GetSubscriptionRequest{Subscription: testSub}},
		{publisher + "ListTopics", "projects/" + testProject, &pb.ListTopicsRequest{Project: "projects/" + testProject}},
	} {
		_, err := registry.Invoke(call(denied.method, denied.resource, consumer), denied.request)
		expectCode(t, err, codes.PermissionDenied)
	}
	if b.total() != before {
		t.Fatal("denied calls reached the backend")
	}
}

// TestProjectScopesStayIsolated proves a principal in another project cannot
// address, list, or reset project-a's resources.
func TestProjectScopesStayIsolated(t *testing.T) {
	p, registry, b := memoryFixture(t)
	_, err := invokeAs(registry, owner, publisher+"CreateTopic", testTopic, &pb.Topic{Name: testTopic})
	must(t, err)
	foreign := call(publisher+"GetTopic", testTopic, other)
	foreign.Project = secondProject
	_, err = registry.Invoke(foreign, &pb.GetTopicRequest{Topic: testTopic})
	expectCode(t, err, codes.InvalidArgument)
	_, err = invokeAs(registry, other, publisher+"GetTopic", testTopic, &pb.GetTopicRequest{Topic: testTopic})
	expectCode(t, err, codes.PermissionDenied)
	// A project-b subscription cannot attach to project-a's topic.
	b2 := "projects/project-b/subscriptions/stolen"
	attach := call(subscriber+"CreateSubscription", b2, other)
	attach.Project = secondProject
	_, err = registry.Invoke(attach, &pb.Subscription{Name: b2, Topic: testTopic})
	expectCode(t, err, codes.Unimplemented)
	// Resetting project-b leaves project-a's index entries in place.
	must(t, p.manager.Reset(context.Background(), keyOf(attach)))
	if _, found := p.index.Get(canonical(testTopic)); !found {
		t.Fatal("reset of another project removed this project's topic")
	}
	if b.called(subscriber+"CreateSubscription") != 0 {
		t.Fatal("cross-project attachment reached the backend")
	}
}

// TestRequestValidationRejectsBeforeBackend covers malformed and unsupported
// request shapes; none may start or reach the backend.
func TestRequestValidationRejectsBeforeBackend(t *testing.T) {
	p, registry, b := memoryFixture(t)
	must(t, p.publish(call(publisher+"CreateTopic", testTopic, owner)))
	must(t, p.publish(call(subscriber+"CreateSubscription", testSub, owner)))
	unknown := &pb.AcknowledgeRequest{Subscription: testSub, AckIds: []string{"a"}}
	unknown.ProtoReflect().SetUnknown([]byte{0xf8, 0x7f, 0x01})
	unknownGet := &pb.GetTopicRequest{Topic: testTopic}
	unknownGet.ProtoReflect().SetUnknown([]byte{0xf8, 0x7f, 0x01})
	big := make([]byte, 1<<20)
	tests := []struct {
		name             string
		method, resource string
		request          any
		code             codes.Code
	}{
		{"non-proto", publisher + "GetTopic", testTopic, "not a message", codes.Internal},
		{"reserved prefix", publisher + "CreateTopic", "projects/project-a/topics/goog-topic", &pb.Topic{Name: "projects/project-a/topics/goog-topic"}, codes.InvalidArgument},
		{"bad name", publisher + "GetTopic", shortTopicName, &pb.GetTopicRequest{Topic: shortTopicName}, codes.InvalidArgument},
		{"project mismatch", publisher + "GetTopic", foreignTopic, &pb.GetTopicRequest{Topic: foreignTopic}, codes.InvalidArgument},
		{"unknown fields", subscriber + "Acknowledge", testSub, unknown, codes.Unimplemented},
		{"unknown fields default", publisher + "GetTopic", testTopic, unknownGet, codes.Unimplemented},
		{"pull zero", subscriber + "Pull", testSub, &pb.PullRequest{Subscription: testSub}, codes.InvalidArgument},
		{"pull too many", subscriber + "Pull", testSub, &pb.PullRequest{Subscription: testSub, MaxMessages: 1001}, codes.InvalidArgument},
		{"modify deadline range", subscriber + "ModifyAckDeadline", testSub, &pb.ModifyAckDeadlineRequest{Subscription: testSub, AckIds: []string{"a"}, AckDeadlineSeconds: 601}, codes.InvalidArgument},
		{"modify deadline negative", subscriber + "ModifyAckDeadline", testSub, &pb.ModifyAckDeadlineRequest{Subscription: testSub, AckIds: []string{"a"}, AckDeadlineSeconds: -1}, codes.InvalidArgument},
		{"subscription filter", subscriber + "CreateSubscription", "projects/project-a/subscriptions/filtered", &pb.Subscription{Name: "projects/project-a/subscriptions/filtered", Topic: testTopic, Filter: "attributes.x"}, codes.Unimplemented},
		{"subscription ack deadline", subscriber + "CreateSubscription", "projects/project-a/subscriptions/short", &pb.Subscription{Name: "projects/project-a/subscriptions/short", Topic: testTopic, AckDeadlineSeconds: 5}, codes.InvalidArgument},
		{"subscription topic name", subscriber + "CreateSubscription", "projects/project-a/subscriptions/badtopic", &pb.Subscription{Name: "projects/project-a/subscriptions/badtopic", Topic: "topics/nope"}, codes.InvalidArgument},
		{"subscription missing topic", subscriber + "CreateSubscription", "projects/project-a/subscriptions/orphan", &pb.Subscription{Name: "projects/project-a/subscriptions/orphan", Topic: "projects/project-a/topics/missing"}, codes.NotFound},
		{"publish none", publisher + "Publish", testTopic, &pb.PublishRequest{Topic: testTopic}, codes.InvalidArgument},
		{"publish nil", publisher + "Publish", testTopic, &pb.PublishRequest{Topic: testTopic, Messages: []*pb.PubsubMessage{nil}}, codes.InvalidArgument},
		{"publish empty", publisher + "Publish", testTopic, &pb.PublishRequest{Topic: testTopic, Messages: []*pb.PubsubMessage{{}}}, codes.InvalidArgument},
		{"publish too large", publisher + "Publish", testTopic, &pb.PublishRequest{Topic: testTopic, Messages: []*pb.PubsubMessage{{Data: big}}}, codes.ResourceExhausted},
		{"missing resource", publisher + "GetTopic", absentTopic, &pb.GetTopicRequest{Topic: absentTopic}, codes.NotFound},
		{"push without oidc", subscriber + "CreateSubscription", pushSubName, &pb.Subscription{Name: pushSubName, Topic: testTopic, PushConfig: &pb.PushConfig{PushEndpoint: "http://127.0.0.1:9/push"}}, codes.Unimplemented},
		{"push remote endpoint", subscriber + "CreateSubscription", pushSubName, &pb.Subscription{Name: pushSubName, Topic: testTopic, PushConfig: pushOIDC("https://example.com:443/push", "push@project-a.iam.gserviceaccount.com", "")}, codes.Unimplemented},
		{"push portless endpoint", subscriber + "CreateSubscription", pushSubName, &pb.Subscription{Name: pushSubName, Topic: testTopic, PushConfig: pushOIDC("http://127.0.0.1/push", "push@project-a.iam.gserviceaccount.com", "")}, codes.Unimplemented},
		{"push unparsable endpoint", subscriber + "CreateSubscription", pushSubName, &pb.Subscription{Name: pushSubName, Topic: testTopic, PushConfig: pushOIDC("http://[::1", "push@project-a.iam.gserviceaccount.com", "")}, codes.InvalidArgument},
		{"push empty account", subscriber + "CreateSubscription", pushSubName, &pb.Subscription{Name: pushSubName, Topic: testTopic, PushConfig: pushOIDC("http://127.0.0.1:9/push", "", "aud")}, codes.InvalidArgument},
		{"push audience newline", subscriber + "CreateSubscription", pushSubName, &pb.Subscription{Name: pushSubName, Topic: testTopic, PushConfig: pushOIDC("http://127.0.0.1:9/push", "push@project-a.iam.gserviceaccount.com", "a\nb")}, codes.InvalidArgument},
		{"push without issuer", subscriber + "CreateSubscription", pushSubName, &pb.Subscription{Name: pushSubName, Topic: testTopic, PushConfig: pushOIDC("http://127.0.0.1:9/push", "push@project-a.iam.gserviceaccount.com", "")}, codes.Unimplemented},
	}
	for _, test := range tests {
		_, err := registry.Invoke(call(test.method, test.resource, owner), test.request)
		if status.Code(err) != test.code {
			t.Errorf("%s: got %v (%v), want %v", test.name, status.Code(err), err, test.code)
		}
	}
	if b.total() != 0 || b.starts != 0 {
		t.Fatalf("rejected requests reached the backend: %d calls, %d starts", b.total(), b.starts)
	}
}

// TestUnauthenticatedAndUnconfiguredProjectsFailClosed covers the provider's
// identity and hierarchy preconditions.
func TestUnauthenticatedAndUnconfiguredProjectsFailClosed(t *testing.T) {
	_, registry, b := memoryFixture(t)
	anonymous := call(publisher+"ListTopics", "projects/"+testProject, owner)
	anonymous.Principal = gateway.Principal{State: gateway.Anonymous}
	_, err := registry.Invoke(anonymous, &pb.ListTopicsRequest{Project: "projects/" + testProject})
	expectCode(t, err, codes.Unauthenticated)
	unconfigured := call(publisher+"CreateTopic", "projects/project-c/topics/topic", owner)
	unconfigured.Project = "project-c"
	_, err = registry.Invoke(unconfigured, &pb.Topic{Name: "projects/project-c/topics/topic"})
	expectCode(t, err, codes.Unimplemented)
	if b.total() != 0 {
		t.Fatal("fail-closed request reached the backend")
	}
}

// TestIndexOwnershipMismatchIsACoverageGap proves a resource indexed under
// another service is never authorized as a Pub/Sub resource.
func TestIndexOwnershipMismatchIsACoverageGap(t *testing.T) {
	p, registry, b := memoryFixture(t)
	must(t, p.index.Put("storage", state.Resource{Name: canonical(testTopic), Service: "storage", Kind: "bucket", Parent: provider.ProjectResource(testProject), Scope: state.Scope{Project: testProject, Location: state.Global}}))
	_, err := invokeAs(registry, owner, publisher+"GetTopic", testTopic, &pb.GetTopicRequest{Topic: testTopic})
	expectCode(t, err, codes.Unimplemented)
	_, err = invokeAs(registry, owner, subscriber+"CreateSubscription", testSub, &pb.Subscription{Name: testSub, Topic: testTopic})
	expectCode(t, err, codes.Unimplemented)
	if b.total() != 0 {
		t.Fatal("foreign-owned resource reached the backend")
	}
}

// TestBackendErrorsPropagateWithoutIndexChanges proves a definite backend
// failure is returned verbatim and publishes nothing.
func TestBackendErrorsPropagateWithoutIndexChanges(t *testing.T) {
	p, registry, b := memoryFixture(t)
	b.fault = func(method string) error {
		if method == publisher+"CreateTopic" {
			return status.Error(codes.ResourceExhausted, "injected quota")
		}
		return nil
	}
	_, err := invokeAs(registry, owner, publisher+"CreateTopic", testTopic, &pb.Topic{Name: testTopic})
	expectCode(t, err, codes.ResourceExhausted)
	if _, found := p.index.Get(canonical(testTopic)); found {
		t.Fatal("failed create was indexed")
	}
	if b.called(publisher+"GetTopic") != 0 {
		t.Fatal("a definite failure triggered reconciliation")
	}
}

// TestAmbiguousDeleteReconcilesAgainstBackend proves a lost delete response is
// resolved by reading the resource back.
func TestAmbiguousDeleteReconcilesAgainstBackend(t *testing.T) {
	for _, landed := range []bool{true, false} {
		t.Run(map[bool]string{true: "landed", false: "not landed"}[landed], func(t *testing.T) {
			p, registry, b := memoryFixture(t)
			_, err := invokeAs(registry, owner, publisher+"CreateTopic", testTopic, &pb.Topic{Name: testTopic})
			must(t, err)
			b.fault = func(method string) error {
				if method != publisher+"DeleteTopic" {
					return nil
				}
				if landed {
					delete(b.topics, testTopic)
				}
				return status.Error(codes.Unavailable, "injected lost response")
			}
			_, err = invokeAs(registry, owner, publisher+"DeleteTopic", testTopic, &pb.DeleteTopicRequest{Topic: testTopic})
			expectCode(t, err, codes.Unavailable)
			if _, found := p.index.Get(canonical(testTopic)); found == landed {
				t.Fatalf("index disagrees with backend after ambiguous delete (landed=%v)", landed)
			}
		})
	}
}

// TestUnresolvedSubscriptionCreateBlocksScope proves an unreadable ambiguous
// subscription create poisons only its own project until reset.
func TestUnresolvedSubscriptionCreateBlocksScope(t *testing.T) {
	p, registry, b := memoryFixture(t)
	_, err := invokeAs(registry, owner, publisher+"CreateTopic", testTopic, &pb.Topic{Name: testTopic})
	must(t, err)
	b.fault = func(method string) error {
		if method == subscriber+"CreateSubscription" {
			return status.Error(codes.DeadlineExceeded, "injected lost response")
		}
		if method == subscriber+"GetSubscription" {
			return status.Error(codes.Internal, "injected read failure")
		}
		return nil
	}
	_, err = invokeAs(registry, owner, subscriber+"CreateSubscription", testSub, &pb.Subscription{Name: testSub, Topic: testTopic})
	expectCode(t, err, codes.DeadlineExceeded)
	_, err = invokeAs(registry, owner, publisher+"GetTopic", testTopic, &pb.GetTopicRequest{Topic: testTopic})
	expectCode(t, err, codes.FailedPrecondition)
	b2 := call(publisher+"CreateTopic", foreignTopic, other)
	b2.Project = secondProject
	b.fault = nil
	_, err = registry.Invoke(b2, &pb.Topic{Name: foreignTopic})
	must(t, err)
	must(t, p.manager.Reset(context.Background(), keyOf(call("", testTopic, owner))))
	_, err = invokeAs(registry, owner, publisher+"ListTopics", "projects/"+testProject, &pb.ListTopicsRequest{Project: "projects/" + testProject})
	must(t, err)
}

// pushOIDC builds a push configuration with an explicit OIDC identity.
func pushOIDC(endpoint, email, audience string) *pb.PushConfig {
	return &pb.PushConfig{PushEndpoint: endpoint, AuthenticationMethod: &pb.PushConfig_OidcToken_{OidcToken: &pb.PushConfig_OidcToken{ServiceAccountEmail: email, Audience: audience}}}
}

// subscriptionLabel reads the "team" label of a subscription response.
func subscriptionLabel(response any) string {
	subscription, _ := response.(*pb.Subscription)
	return subscription.GetLabels()["team"]
}
