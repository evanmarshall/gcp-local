package pubsub

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// testProject is the primary fixture project for Pub/Sub provider tests.
const testProject = "project-a"

const owner = "serviceAccount:owner@project-a.iam.gserviceaccount.com"
const other = "serviceAccount:other@project-b.iam.gserviceaccount.com"
const testTopic = "projects/project-a/topics/topic"
const testSub = "projects/project-a/subscriptions/subscription"

type faultDriver struct {
	calls, starts int
	fault         func(string, any, any) error
}

func (b *faultDriver) Start(context.Context) error      { b.starts++; return nil }
func (b *faultDriver) Ready(context.Context) error      { return nil }
func (b *faultDriver) Stop(context.Context) error       { return nil }
func (b *faultDriver) Reset(context.Context) error      { return nil }
func (b *faultDriver) client() grpc.ClientConnInterface { return b }
func (b *faultDriver) Invoke(ctx context.Context, method string, request, response any, _ ...grpc.CallOption) error {
	b.calls++
	if md, _ := metadata.FromOutgoingContext(ctx); len(md) != 0 {
		panic("caller metadata reached backend")
	}
	if b.fault != nil {
		return b.fault(method, request, response)
	}
	return nil
}
func (*faultDriver) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	panic("unexpected backend stream")
}

func fixture(t *testing.T) (*Provider, *gateway.Registry, *faultDriver) {
	t.Helper()
	index := state.NewIndex()
	for _, project := range []string{testProject, "project-b"} {
		must(t, index.Put("resourcemanager", state.Resource{Name: provider.ProjectResource(project), Service: "resourcemanager", Kind: "project", Number: "123456", Scope: state.Scope{Project: project, Location: state.Global}}))
	}
	engine, err := iam.New(index)
	must(t, err)
	_, err = engine.SetPolicy(provider.ProjectResource(testProject), iam.Policy{Bindings: []iam.Binding{{Role: "roles/pubsub.admin", Members: []string{owner}}}})
	must(t, err)
	_, err = engine.SetPolicy(provider.ProjectResource("project-b"), iam.Policy{Bindings: []iam.Binding{{Role: "roles/pubsub.admin", Members: []string{other}}}})
	must(t, err)
	manager := backend.NewManager(time.Second, time.Second)
	registry, b := gateway.NewRegistry(), &faultDriver{}
	p, err := newProvider(registry, manager, index, engine, func(state.Scope) (backend.Backend, error) { return b, nil })
	must(t, err)
	t.Cleanup(func() { must(t, manager.Shutdown(context.Background())) })
	return p, registry, b
}
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func call(method, resource, actor string) gateway.Call {
	return gateway.Call{Context: context.Background(), Method: method, Resource: resource, Project: testProject, Location: state.Global,
		Principal: gateway.Principal{State: gateway.Authenticated, Subject: actor, Project: testProject, Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}}
}

func TestDeniedOrUnsupportedCallsNeverStartOrReachBackend(t *testing.T) {
	p, registry, b := fixture(t)
	must(t, p.publish(call(publisher+"CreateTopic", testTopic, owner)))
	must(t, p.publish(call(subscriber+"CreateSubscription", testSub, owner)))
	tests := []struct {
		call    gateway.Call
		request proto.Message
		code    codes.Code
	}{
		{call(publisher+"CreateTopic", testTopic, other), &pb.Topic{Name: testTopic}, codes.PermissionDenied},
		{call(publisher+"Publish", testTopic, other), &pb.PublishRequest{Topic: testTopic, Messages: []*pb.PubsubMessage{{Data: []byte("denied")}}}, codes.PermissionDenied},
		{call(subscriber+"Pull", testSub, other), &pb.PullRequest{Subscription: testSub, MaxMessages: 1}, codes.PermissionDenied},
		{call(subscriber+"Acknowledge", testSub, other), &pb.AcknowledgeRequest{Subscription: testSub, AckIds: []string{"ack"}}, codes.PermissionDenied},
		{call(subscriber+"CreateSubscription", testSub, owner), &pb.Subscription{Name: testSub, Topic: testTopic, PushConfig: &pb.PushConfig{PushEndpoint: "https://example.invalid"}}, codes.Unimplemented},
		{call(publisher+"CreateTopic", "projects/project-a/topics/schema", owner), &pb.Topic{Name: "projects/project-a/topics/schema", KmsKeyName: "unverified"}, codes.Unimplemented},
		{call(publisher+"Publish", testTopic, owner), &pb.PublishRequest{Topic: testTopic, Messages: []*pb.PubsubMessage{{Data: []byte("ordered"), OrderingKey: "key"}}}, codes.Unimplemented},
	}
	for _, test := range tests {
		_, err := registry.Invoke(test.call, test.request)
		if status.Code(err) != test.code {
			t.Fatalf("%s: %v", test.call.Method, err)
		}
	}
	if b.calls != 0 || b.starts != 0 {
		t.Fatalf("unauthorized/unsupported request reached backend: %+v", b)
	}
}

func TestCreateSubscriptionRequiresTopicAttachmentBeforeBackend(t *testing.T) {
	p, registry, b := fixture(t)
	must(t, p.publish(call(publisher+"CreateTopic", testTopic, owner)))
	// A fail-closed policy feature on the topic must still be evaluated even
	// though the project grants subscription creation.
	must(t, p.iam.MarkUnsupported(canonical(testTopic), []string{"IAM Conditions"}))
	_, err := registry.Invoke(call(subscriber+"CreateSubscription", testSub, owner), &pb.Subscription{Name: testSub, Topic: testTopic})
	if status.Code(err) != codes.Unimplemented || b.calls != 0 || b.starts != 0 {
		t.Fatalf("topic attachment bypass: %v", err)
	}
}

func TestResourceGrantAndOAuthScopeIsolation(t *testing.T) {
	p, registry, b := fixture(t)
	must(t, p.publish(call(publisher+"CreateTopic", testTopic, owner)))
	_, err := p.iam.SetPolicy(canonical(testTopic), iam.Policy{Bindings: []iam.Binding{{Role: "roles/pubsub.publisher", Members: []string{other}}}})
	must(t, err)
	c := call(publisher+"Publish", testTopic, other)
	c.Principal.Project = "project-b"
	request := &pb.PublishRequest{Topic: testTopic, Messages: []*pb.PubsubMessage{{Data: []byte("explicit cross-project grant")}}}
	c.Principal.Scopes = []string{"https://www.googleapis.com/auth/devstorage.full_control"}
	_, err = registry.Invoke(c, request)
	if status.Code(err) != codes.Unimplemented || b.calls != 0 {
		t.Fatal("Storage OAuth scope permitted Pub/Sub", err)
	}
	c.Principal.Scopes = []string{"https://www.googleapis.com/auth/pubsub"}
	c.Context = metadata.NewOutgoingContext(c.Context, metadata.Pairs("authorization", "must-not-forward"))
	_, err = registry.Invoke(c, request)
	must(t, err)
	if b.calls != 1 {
		t.Fatal("resource grant was not honored")
	}
	_, err = p.iam.SetPolicy(canonical(testTopic), iam.Policy{})
	must(t, err)
	_, err = registry.Invoke(c, request)
	if status.Code(err) != codes.PermissionDenied || b.calls != 1 {
		t.Fatal("revoked grant reached backend", err)
	}
}

func TestAmbiguousCreateReconcilesOrBlocksUntilReset(t *testing.T) {
	for _, readable := range []bool{true, false} {
		t.Run(map[bool]string{true: "confirmed", false: "unresolved"}[readable], func(t *testing.T) {
			p, registry, b := fixture(t)
			b.fault = lostCreateResponse(t, readable)
			_, err := registry.Invoke(call(publisher+"CreateTopic", testTopic, owner), &pb.Topic{Name: testTopic})
			if status.Code(err) != codes.DeadlineExceeded {
				t.Fatal("invented successful create", err)
			}
			if _, found := p.index.Get(canonical(testTopic)); found != readable {
				t.Fatal("index disagrees with confirmed backend state")
			}
			_, err = registry.Invoke(call(publisher+"GetTopic", testTopic, owner), &pb.GetTopicRequest{Topic: testTopic})
			if readable {
				must(t, err)
			} else if status.Code(err) != codes.FailedPrecondition || b.calls != 2 {
				t.Fatal("unresolved mutation remained usable", err)
			}
			assertResetClearsTopic(t, p)
		})
	}
}

// lostCreateResponse fails CreateTopic ambiguously, then answers the
// reconciliation read with the topic when readable and an outage otherwise.
func lostCreateResponse(t *testing.T, readable bool) func(string, any, any) error {
	t.Helper()
	return func(method string, request, response any) error {
		if method == publisher+"CreateTopic" {
			return status.Error(codes.DeadlineExceeded, "injected lost response")
		}
		if method != publisher+"GetTopic" {
			t.Fatalf("unexpected reconciliation: %s", method)
		}
		if !readable {
			return status.Error(codes.Unavailable, "injected read failure")
		}
		topic, ok := response.(*pb.Topic)
		ask, askOK := request.(*pb.GetTopicRequest)
		if !ok || !askOK {
			return status.Error(codes.Internal, "unexpected fixture types")
		}
		topic.Name = ask.GetTopic()
		return nil
	}
}

// assertResetClearsTopic resets the test scope and checks that neither the
// indexed topic nor an unresolved-mutation marker survives.
func assertResetClearsTopic(t *testing.T, p *Provider) {
	t.Helper()
	must(t, p.manager.Reset(context.Background(), keyOf(call("", testTopic, owner))))
	if _, found := p.index.Get(canonical(testTopic)); found {
		t.Fatal("reset retained resource")
	}
	if _, failed := p.poisoned.Load(scopeOf(call("", testTopic, owner))); failed {
		t.Fatal("reset retained failure")
	}
}

func TestEmulatorPinMatchesInstaller(t *testing.T) {
	raw, err := os.ReadFile("../../../toolchains/pubsub.json")
	must(t, err)
	var pin struct {
		Version string `json:"version"`
		SHA256  string `json:"jar_sha256"`
	}
	must(t, json.Unmarshal(raw, &pin))
	if pin.Version != EmulatorVersion || pin.SHA256 != EmulatorSHA256 {
		t.Fatal("runtime and installer emulator pins differ")
	}
}

func TestPushActAsDeniedBeforeAnyBackendAccess(t *testing.T) {
	p, registry, b := fixture(t)
	var err error
	p.tokens, err = identity.NewIDTokens(p.iam, p.index)
	must(t, err)
	must(t, p.publish(call(publisher+"CreateTopic", testTopic, owner)))
	email := "push-worker@project-a.iam.gserviceaccount.com"
	must(t, p.index.Put("iam", state.Resource{Name: iam.AccountResource(testProject, email), Service: "iam", Kind: "serviceAccount", Number: "123456789012345678901", Parent: provider.ProjectResource(testProject), Scope: state.Scope{Project: testProject, Location: state.Global}}))
	request := &pb.Subscription{Name: testSub, Topic: testTopic, PushConfig: &pb.PushConfig{PushEndpoint: "http://127.0.0.1:8888/receive", AuthenticationMethod: &pb.PushConfig_OidcToken_{OidcToken: &pb.PushConfig_OidcToken{ServiceAccountEmail: email}}}}
	_, err = registry.Invoke(call(subscriber+"CreateSubscription", testSub, owner), request)
	if status.Code(err) != codes.PermissionDenied || b.calls != 0 || b.starts != 0 {
		t.Fatal("missing actAs reached backend", err)
	}
}

// TestRestWireEchoesPushConfig proves a push subscription reads back with its
// endpoint and OIDC identity, so declarative clients see no drift.
func TestRestWireEchoesPushConfig(t *testing.T) {
	wire := restWire("", &pb.Subscription{
		Name: "projects/p/subscriptions/s", Topic: "projects/p/topics/t",
		PushConfig: &pb.PushConfig{
			PushEndpoint:         "http://127.0.0.1:1/push",
			AuthenticationMethod: &pb.PushConfig_OidcToken_{OidcToken: &pb.PushConfig_OidcToken{ServiceAccountEmail: "push@p.iam.gserviceaccount.com", Audience: "https://aud"}},
		},
	})
	push, _ := wire["pushConfig"].(map[string]any)
	oidc, _ := push["oidcToken"].(map[string]any)
	if push["pushEndpoint"] != "http://127.0.0.1:1/push" || oidc["serviceAccountEmail"] != "push@p.iam.gserviceaccount.com" || oidc["audience"] != "https://aud" {
		t.Fatalf("push wire = %v", wire)
	}
	if _, found := restWire("", &pb.Subscription{Name: "projects/p/subscriptions/s"})["pushConfig"]; found {
		t.Fatal("a pull subscription rendered a push config")
	}
}
