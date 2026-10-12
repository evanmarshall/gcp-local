package pubsub

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/providers/provider/providertest"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/genproto/googleapis/type/expr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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
		{call(publisher+"Publish", testTopic, owner), &pb.PublishRequest{Topic: testTopic, Messages: []*pb.PubsubMessage{{Data: []byte("timed"), PublishTime: timestamppb.Now()}}}, codes.Unimplemented},
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
	if status.Code(err) != codes.PermissionDenied || b.calls != 0 {
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
	raw, err := os.ReadFile("../../../../toolchains/pubsub.json")
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

// TestEmulatorConfigRejectsUnpinnedInputs proves the provider refuses to start
// anything but an executable Java and the committed emulator jar.
func TestEmulatorConfigRejectsUnpinnedInputs(t *testing.T) {
	dir := t.TempDir()
	// The test binary stands in for an executable Java; only the jar pin fails.
	java, err := os.Executable()
	must(t, err)
	plain := filepath.Join(dir, "plain")
	must(t, os.WriteFile(plain, []byte("x"), 0o600))
	jar := filepath.Join(dir, "emulator.jar")
	must(t, os.WriteFile(jar, []byte("not the pinned jar"), 0o600))
	tests := []struct {
		name   string
		config Config
		want   error
	}{
		{"relative java", Config{Java: "java", JAR: jar}, errBackendPaths},
		{"relative jar", Config{Java: java, JAR: "emulator.jar"}, errBackendPaths},
		{"missing java", Config{Java: filepath.Join(dir, "absent"), JAR: jar}, errJavaMissing},
		{"non-executable java", Config{Java: plain, JAR: jar}, errJavaMissing},
		{"directory java", Config{Java: dir, JAR: jar}, errJavaMissing},
		{"jar mismatch", Config{Java: java, JAR: jar}, errJarMismatch},
	}
	for _, test := range tests {
		if err := test.config.validate(); !errors.Is(err, test.want) {
			t.Errorf("%s: got %v, want %v", test.name, err, test.want)
		}
	}
	if err := (Config{Java: java, JAR: filepath.Join(dir, "absent.jar")}).validate(); err == nil || errors.Is(err, errJarMismatch) {
		t.Fatalf("missing jar = %v", err)
	}

	index := state.NewIndex()
	engine, err := iam.New(index)
	must(t, err)
	manager := backend.NewManager(time.Second, time.Second)
	t.Cleanup(func() { must(t, manager.Shutdown(context.Background())) })
	if _, err := New(gateway.NewRegistry(), manager, index, engine, Config{Java: java, JAR: jar}); !errors.Is(err, errJarMismatch) {
		t.Fatalf("New accepted an unpinned jar: %v", err)
	}
	if len(manager.Status()) != 0 {
		t.Fatal("a rejected configuration registered a backend")
	}
}

// TestProviderRequiresCompleteWiring proves construction fails without every
// shared dependency rather than serving partially.
func TestProviderRequiresCompleteWiring(t *testing.T) {
	index := state.NewIndex()
	engine, err := iam.New(index)
	must(t, err)
	manager := backend.NewManager(time.Second, time.Second)
	factory := func(state.Scope) (backend.Backend, error) { return newMemoryPubSub(), nil }
	if _, err := newProvider(nil, manager, index, engine, factory); !errors.Is(err, errProviderShape) {
		t.Fatalf("missing registry = %v", err)
	}
	if _, err := newProvider(gateway.NewRegistry(), manager, nil, engine, factory); !errors.Is(err, errProviderShape) {
		t.Fatalf("missing index = %v", err)
	}
	registry := gateway.NewRegistry()
	_, err = newProvider(registry, manager, index, engine, factory)
	must(t, err)
	if _, err := newProvider(gateway.NewRegistry(), manager, index, engine, factory); err == nil {
		t.Fatal("a second provider registered the same backend service")
	}
}

// TestEmulatorLifecycleWithoutAProcess covers the emulator driver's lifecycle
// before a JVM exists: readiness fails, and stop/reset clean up idempotently.
func TestEmulatorLifecycleWithoutAProcess(t *testing.T) {
	dir := t.TempDir()
	b := &emulator{config: Config{Java: "java"}, scope: state.Scope{Project: testProject, Location: state.Global}}
	if err := b.Start(context.Background()); !errors.Is(err, errBackendPaths) {
		t.Fatalf("start with an invalid config = %v", err)
	}
	if err := b.Ready(context.Background()); !errors.Is(err, backend.ErrProcessExited) {
		t.Fatalf("ready without a process = %v", err)
	}
	scratch := filepath.Join(dir, "scratch")
	must(t, os.Mkdir(scratch, 0o700))
	b.directory = scratch
	must(t, b.Reset(context.Background()))
	if _, err := os.Stat(scratch); !os.IsNotExist(err) {
		t.Fatal("stop left the emulator data directory")
	}
	must(t, b.Stop(context.Background()))
}

// TestIAMPolicyRoundTripOverGRPC sets and reads topic and subscription
// policies through the IAMPolicy service and proves the grants take effect.
func TestIAMPolicyRoundTripOverGRPC(t *testing.T) {
	_, registry, b := memoryFixture(t)
	_, err := invokeAs(registry, owner, publisher+"CreateTopic", testTopic, &pb.Topic{Name: testTopic})
	must(t, err)
	_, err = invokeAs(registry, owner, subscriber+"CreateSubscription", testSub, &pb.Subscription{Name: testSub, Topic: testTopic})
	must(t, err)
	publish := &pb.PublishRequest{Topic: testTopic, Messages: []*pb.PubsubMessage{{Data: []byte("x")}}}
	_, err = invokeAs(registry, consumer, publisher+"Publish", testTopic, publish)
	expectCode(t, err, codes.PermissionDenied)

	set := &iampb.SetIamPolicyRequest{Resource: testTopic, Policy: &iampb.Policy{Bindings: []*iampb.Binding{{Role: "roles/pubsub.publisher", Members: []string{consumer}}}}}
	raw, err := invokeAs(registry, owner, setIAM, testTopic, set)
	must(t, err)
	written, ok := raw.(*iampb.Policy)
	if !ok || len(written.GetBindings()) != 1 || len(written.GetEtag()) == 0 {
		t.Fatalf("set policy = %v", raw)
	}
	raw, err = invokeAs(registry, owner, getIAM, testTopic, &iampb.GetIamPolicyRequest{Resource: testTopic, Options: &iampb.GetPolicyOptions{RequestedPolicyVersion: 3}})
	must(t, err)
	read, _ := raw.(*iampb.Policy)
	if len(read.GetBindings()) != 1 || read.GetBindings()[0].GetMembers()[0] != consumer {
		t.Fatalf("get policy = %v", read)
	}
	_, err = invokeAs(registry, consumer, publisher+"Publish", testTopic, publish)
	must(t, err)

	// The topic grant does not reach the subscription's policy surface.
	_, err = invokeAs(registry, consumer, getIAM, testSub, &iampb.GetIamPolicyRequest{Resource: testSub})
	expectCode(t, err, codes.PermissionDenied)
	if b.called(setIAM) != 0 {
		t.Fatal("the provider-owned policy surface reached the emulator")
	}
}

// TestConditionalPoliciesAreRefused follows the pubsub-rest-data-plane
// recording: GCP refuses a conditional binding on a topic or subscription at
// any policy version, and returns a policy written as version 3 as version 1.
func TestConditionalPoliciesAreRefused(t *testing.T) {
	p, registry, _ := memoryFixture(t)
	_, err := invokeAs(registry, owner, publisher+"CreateTopic", testTopic, &pb.Topic{Name: testTopic})
	must(t, err)
	_, err = invokeAs(registry, owner, subscriber+"CreateSubscription", testSub, &pb.Subscription{Name: testSub, Topic: testTopic})
	must(t, err)
	conditional := []*iampb.Binding{{Role: roleSubscriber, Members: []string{consumer}, Condition: &expr.Expr{
		Title: "until 2999", Description: "expires", Expression: `request.time < timestamp("2999-01-01T00:00:00Z")`,
	}}}
	for _, resource := range []string{testTopic, testSub} {
		for _, version := range []int32{1, 3} {
			_, err = invokeAs(registry, owner, setIAM, resource, &iampb.SetIamPolicyRequest{Resource: resource, Policy: &iampb.Policy{Version: version, Bindings: conditional}})
			if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "Can't set conditional policy on this resource") {
				t.Fatalf("conditional v%d policy on %s: %v", version, resource, err)
			}
		}
		policy, err := p.iam.GetPolicy(canonical(resource))
		must(t, err)
		if len(policy.Bindings) != 0 {
			t.Fatalf("a refused conditional policy was stored on %s: %+v", resource, policy)
		}
	}
	unconditional := []*iampb.Binding{{Role: roleSubscriber, Members: []string{consumer}}}
	raw, err := invokeAs(registry, owner, setIAM, testSub, &iampb.SetIamPolicyRequest{Resource: testSub, Policy: &iampb.Policy{Version: 3, Bindings: unconditional}})
	must(t, err)
	if written, _ := raw.(*iampb.Policy); written.GetVersion() != 1 {
		t.Fatalf("a version 3 write without conditions returned version %d", written.GetVersion())
	}
	for _, requested := range []int32{0, 1, 3} {
		raw, err = invokeAs(registry, owner, getIAM, testSub, &iampb.GetIamPolicyRequest{Resource: testSub, Options: &iampb.GetPolicyOptions{RequestedPolicyVersion: requested}})
		must(t, err)
		if read, _ := raw.(*iampb.Policy); read.GetVersion() != 1 || len(read.GetBindings()) != 1 {
			t.Fatalf("read at requested version %d = %v", requested, read)
		}
	}
}

// TestStoredConditionsAreRendered reads a conditional policy placed in the
// engine directly, over gRPC and REST: a returned policy keeps each binding's
// condition, so a read-modify-write cannot drop it.
func TestStoredConditionsAreRendered(t *testing.T) {
	p, registry, _ := memoryFixture(t)
	_, err := invokeAs(registry, owner, publisher+"CreateTopic", testTopic, &pb.Topic{Name: testTopic})
	must(t, err)
	condition := `{"title":"until 2999","description":"expires","expression":"request.time \u003c timestamp(\"2999-01-01T00:00:00Z\")"}`
	_, err = p.iam.SetPolicy(canonical(testTopic), iam.Policy{Version: 3, Bindings: []iam.Binding{{Role: roleSubscriber, Members: []string{consumer}, Condition: json.RawMessage(condition)}}})
	must(t, err)
	raw, err := invokeAs(registry, owner, getIAM, testTopic, &iampb.GetIamPolicyRequest{Resource: testTopic, Options: &iampb.GetPolicyOptions{RequestedPolicyVersion: 3}})
	must(t, err)
	read, _ := raw.(*iampb.Policy)
	got := read.GetBindings()[0].GetCondition()
	if read.GetVersion() != 3 || got.GetTitle() != "until 2999" || got.GetDescription() != "expires" || got.GetExpression() != `request.time < timestamp("2999-01-01T00:00:00Z")` {
		t.Fatalf("gRPC policy = %v", read)
	}
	c := newRESTClient(t, registry)
	wire := c.do(owner, http.MethodGet, "/v1/"+testTopic+":getIamPolicy?options.requestedPolicyVersion=3", "", 200)
	bindings, _ := wire["bindings"].([]any)
	binding, _ := bindings[0].(map[string]any)
	if rendered, _ := binding["condition"].(map[string]any); rendered["title"] != "until 2999" || rendered["expression"] != got.GetExpression() {
		t.Fatalf("REST policy = %v", wire)
	}
}

// TestIAMPolicyRequestsAreValidated covers malformed and unsupported IAM
// requests, foreign callers, and missing resources.
func TestIAMPolicyRequestsAreValidated(t *testing.T) {
	_, registry, _ := memoryFixture(t)
	_, err := invokeAs(registry, owner, publisher+"CreateTopic", testTopic, &pb.Topic{Name: testTopic})
	must(t, err)
	binding := []*iampb.Binding{{Role: roleViewer, Members: []string{consumer}}}
	tests := []struct {
		name, actor, resource string
		method                string
		request               any
		code                  codes.Code
	}{
		{"invalid name", owner, shortTopicName, getIAM, &iampb.GetIamPolicyRequest{Resource: shortTopicName}, codes.InvalidArgument},
		{"project mismatch", owner, foreignTopic, getIAM, &iampb.GetIamPolicyRequest{Resource: foreignTopic}, codes.InvalidArgument},
		{"non-proto", owner, testTopic, getIAM, "raw", codes.InvalidArgument},
		{"foreign request type", owner, testTopic, getIAM, &pb.Topic{Name: testTopic}, codes.Unimplemented},
		{"version two", owner, testTopic, getIAM, &iampb.GetIamPolicyRequest{Resource: testTopic, Options: &iampb.GetPolicyOptions{RequestedPolicyVersion: 2}}, codes.Unimplemented},
		{"update mask", owner, testTopic, setIAM, &iampb.SetIamPolicyRequest{Resource: testTopic, Policy: &iampb.Policy{Bindings: binding}, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"bindings"}}}, codes.Unimplemented},
		{"missing policy", owner, testTopic, setIAM, &iampb.SetIamPolicyRequest{Resource: testTopic}, codes.InvalidArgument},
		{"nil binding", owner, testTopic, setIAM, &iampb.SetIamPolicyRequest{Resource: testTopic, Policy: &iampb.Policy{Bindings: []*iampb.Binding{nil}}}, codes.InvalidArgument},
		{"audit configs", owner, testTopic, setIAM, &iampb.SetIamPolicyRequest{Resource: testTopic, Policy: &iampb.Policy{AuditConfigs: []*iampb.AuditConfig{{Service: "allServices"}}}}, codes.Unimplemented},
		{"condition without v3", owner, testTopic, setIAM, &iampb.SetIamPolicyRequest{Resource: testTopic, Policy: &iampb.Policy{Version: 1, Bindings: []*iampb.Binding{{Role: roleViewer, Members: []string{consumer}, Condition: &expr.Expr{Expression: "true"}}}}}, codes.InvalidArgument},
		{"foreign caller", other, testTopic, setIAM, &iampb.SetIamPolicyRequest{Resource: testTopic, Policy: &iampb.Policy{Bindings: binding}}, codes.PermissionDenied},
		{"missing topic", owner, absentTopic, getIAM, &iampb.GetIamPolicyRequest{Resource: absentTopic}, codes.NotFound},
	}
	for _, test := range tests {
		_, err := registry.Invoke(call(test.method, test.resource, test.actor), test.request)
		if got := status.Code(err); got != test.code {
			t.Errorf("%s: got %v (%v), want %v", test.name, got, err, test.code)
		}
	}
}

// restHarness drives the Pub/Sub JSON surface through the real REST gateway.
type restHarness struct {
	t *testing.T
	g *gateway.Gateway
}

func newRESTClient(t *testing.T, registry *gateway.Registry) *restHarness {
	t.Helper()
	return &restHarness{t: t, g: gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:" + token, Project: testProject, Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})}
}

// do sends one request as actor ("" is anonymous) and checks the HTTP status.
func (c *restHarness) do(actor, method, path, body string, want int) map[string]any {
	c.t.Helper()
	r := httptest.NewRequestWithContext(context.Background(), method, path, strings.NewReader(body))
	if actor != "" {
		r.Header.Set("Authorization", "Bearer "+strings.TrimPrefix(actor, "serviceAccount:"))
	}
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	c.g.ServeHTTP(w, r)
	if w.Code != want {
		c.t.Fatalf("%s %s: got %d want %d: %s", method, path, w.Code, want, w.Body.String())
	}
	decoded := map[string]any{}
	if w.Body.Len() > 0 {
		_ = json.NewDecoder(bytes.NewReader(w.Body.Bytes())).Decode(&decoded)
	}
	return decoded
}

const restTopicPath = "/v1/projects/project-a/topics/rest-topic"
const restSubPath = "/v1/projects/project-a/subscriptions/rest-sub"

// TestRESTTopicLifecycle covers the REST topic twins and their JSON shape.
func TestRESTTopicLifecycle(t *testing.T) {
	p, registry, b := memoryFixture(t)
	c := newRESTClient(t, registry)
	topic := c.do(owner, http.MethodPut, restTopicPath, `{"labels":{"env":"test"}}`, 200)
	if topic["name"] != "projects/project-a/topics/rest-topic" {
		t.Fatalf("topic = %v", topic)
	}
	if labels, _ := topic["labels"].(map[string]any); labels["env"] != "test" {
		t.Fatalf("topic labels = %v", topic["labels"])
	}
	c.do(owner, http.MethodPut, restTopicPath, `{}`, http.StatusConflict)
	c.do(owner, http.MethodGet, restTopicPath, "", 200)
	topics := c.do(owner, http.MethodGet, "/v1/projects/project-a/topics", "", 200)
	if list, _ := topics["topics"].([]any); len(list) != 1 {
		t.Fatalf("topics = %v", topics)
	}
	c.do(owner, http.MethodDelete, restTopicPath, "", 200)
	c.do(owner, http.MethodGet, restTopicPath, "", http.StatusNotFound)
	if _, found := p.index.Get(canonical("projects/project-a/topics/rest-topic")); found {
		t.Fatal("REST delete left the topic indexed")
	}
	if b.called(publisher+"DeleteTopic") != 1 {
		t.Fatal("REST delete did not reach the backend exactly once")
	}
}

// TestRESTSubscriptionLifecycle covers the REST subscription twins, including
// request defaults and the JSON wire shape.
func TestRESTSubscriptionLifecycle(t *testing.T) {
	p, registry, _ := memoryFixture(t)
	c := newRESTClient(t, registry)
	c.do(owner, http.MethodPut, restTopicPath, `{}`, 200)
	sub := c.do(owner, http.MethodPut, restSubPath, `{"topic":"rest-topic","messageRetentionDuration":"600s","labels":{"team":"a"}}`, 200)
	if sub["topic"] != "projects/project-a/topics/rest-topic" || sub["ackDeadlineSeconds"] != float64(10) || sub["messageRetentionDuration"] != "600s" {
		t.Fatalf("subscription = %v", sub)
	}
	if _, found := sub["pushConfig"]; found {
		t.Fatal("a pull subscription rendered a push config")
	}
	got := c.do(owner, http.MethodGet, restSubPath, "", 200)
	if labels, _ := got["labels"].(map[string]any); labels["team"] != "a" {
		t.Fatalf("subscription labels = %v", got["labels"])
	}
	subs := c.do(owner, http.MethodGet, "/v1/projects/project-a/subscriptions", "", 200)
	if list, _ := subs["subscriptions"].([]any); len(list) != 1 {
		t.Fatalf("subscriptions = %v", subs)
	}
	// A fully qualified topic name is accepted as-is.
	c.do(owner, http.MethodPut, "/v1/projects/project-a/subscriptions/rest-sub-2", `{"topic":"projects/project-a/topics/rest-topic","ackDeadlineSeconds":30}`, 200)

	c.do(owner, http.MethodDelete, restSubPath, "", 200)
	c.do(owner, http.MethodGet, restSubPath, "", http.StatusNotFound)
	if _, found := p.index.Get(canonical("projects/project-a/subscriptions/rest-sub")); found {
		t.Fatal("REST delete left the subscription indexed")
	}
}

// TestRESTRequestsFailClosed covers REST denial, validation, and unsupported
// shapes; none may reach the backend.
func TestRESTRequestsFailClosed(t *testing.T) {
	_, registry, b := memoryFixture(t)
	c := newRESTClient(t, registry)
	c.do(owner, http.MethodPut, restTopicPath, `{}`, 200)
	before := b.total()
	c.do(other, http.MethodGet, restTopicPath, "", http.StatusForbidden)
	c.do(other, http.MethodDelete, restTopicPath, "", http.StatusForbidden)
	c.do(other, http.MethodGet, "/v1/projects/project-a/topics", "", http.StatusForbidden)
	c.do("", http.MethodGet, restTopicPath, "", http.StatusUnauthorized)
	c.do(owner, http.MethodPut, restSubPath, `{"topic":`, http.StatusBadRequest)
	c.do(owner, http.MethodPut, restSubPath, `{"topic":"rest-topic","ackDeadlineSeconds":5}`, http.StatusBadRequest)
	c.do(owner, http.MethodPut, restSubPath, `{"topic":"absent"}`, http.StatusNotFound)
	c.do(owner, http.MethodPut, restSubPath, `{"topic":"rest-topic","pushConfig":{"pushEndpoint":"http://127.0.0.1:9/push","attributes":{"x-goog-version":"v1"},"oidcToken":{"serviceAccountEmail":"push@project-a.iam.gserviceaccount.com","audience":"aud"}}}`, http.StatusNotImplemented)
	c.do(owner, http.MethodGet, "/v1/projects/project-a/topics/x", "", http.StatusBadRequest)
	if b.total() != before {
		t.Fatalf("rejected REST requests reached the backend: %d calls", b.total()-before)
	}
}

// TestRESTPolicyRoundTrip covers the REST IAM policy surface for an authorized
// administrator.
func TestRESTPolicyRoundTrip(t *testing.T) {
	p, registry, _ := memoryFixture(t)
	c := newRESTClient(t, registry)
	c.do(owner, http.MethodPut, restTopicPath, `{}`, 200)
	c.do(owner, http.MethodPut, restSubPath, `{"topic":"rest-topic"}`, 200)
	written := c.do(owner, http.MethodPost, restTopicPath+":setIamPolicy", `{"policy":{"bindings":[{"role":"roles/pubsub.publisher","members":["`+consumer+`"]}]}}`, 200)
	if bindings, _ := written["bindings"].([]any); len(bindings) != 1 {
		t.Fatalf("set policy = %v", written)
	}
	read := c.do(owner, http.MethodGet, restTopicPath+":getIamPolicy", "", 200)
	if bindings, _ := read["bindings"].([]any); len(bindings) != 1 {
		t.Fatalf("get policy = %v", read)
	}
	policy, err := p.iam.GetPolicy(canonical("projects/project-a/topics/rest-topic"))
	must(t, err)
	if len(policy.Bindings) != 1 || policy.Bindings[0].Members[0] != consumer {
		t.Fatalf("engine policy = %+v", policy)
	}
	c.do(owner, http.MethodPost, restSubPath+":setIamPolicy", `{"policy":{"bindings":[]}}`, 200)
	c.do(owner, http.MethodGet, restSubPath+":getIamPolicy", "", 200)
	c.do(owner, http.MethodPost, restSubPath+":setIamPolicy", `{"policy":`, http.StatusBadRequest)
}

// TestRESTPolicyRequiresAuthorization proves the REST IAM surface evaluates the
// caller the way the gRPC IAMPolicy surface does: a foreign or anonymous
// caller cannot read or grant itself access, and a missing topic is 404.
func TestRESTPolicyRequiresAuthorization(t *testing.T) {
	p, registry, _ := memoryFixture(t)
	c := newRESTClient(t, registry)
	c.do(owner, http.MethodPut, restTopicPath, `{}`, 200)
	grant := `{"policy":{"bindings":[{"role":"roles/pubsub.admin","members":["` + other + `"]}]}}`
	c.do(other, http.MethodPost, restTopicPath+":setIamPolicy", grant, http.StatusForbidden)
	c.do("", http.MethodPost, restTopicPath+":setIamPolicy", grant, http.StatusUnauthorized)
	c.do(other, http.MethodGet, restTopicPath+":getIamPolicy", "", http.StatusForbidden)
	c.do(owner, http.MethodPost, "/v1/projects/project-a/topics/absent:setIamPolicy", grant, http.StatusNotFound)
	policy, err := p.iam.GetPolicy(canonical("projects/project-a/topics/rest-topic"))
	must(t, err)
	if len(policy.Bindings) != 0 {
		t.Fatalf("unauthorized caller changed the policy: %+v", policy)
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

// TestConcurrentTopicAndSubscriptionRequests races topic and subscription
// create, get, list, and delete on the in-memory driver; -race fails it if
// provider-owned labels, delivery, or index state is touched unlocked.
func TestConcurrentTopicAndSubscriptionRequests(t *testing.T) {
	_, registry, _ := memoryFixture(t)
	c := newRESTClient(t, registry)
	providertest.Hammer(t, c.g, strings.TrimPrefix(owner, "serviceAccount:"), 8, 10, func(int) []providertest.Request {
		return []providertest.Request{
			{Method: http.MethodPut, Path: restTopicPath, Body: map[string]any{"labels": map[string]string{"env": "test"}}, Want: []int{200, 409}},
			{Method: http.MethodGet, Path: restTopicPath, Want: []int{200, 404}},
			{Method: http.MethodPut, Path: restSubPath, Body: map[string]any{"topic": "rest-topic", "labels": map[string]string{"team": "a"}}, Want: []int{200, 404, 409}},
			{Method: http.MethodGet, Path: restSubPath, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: "/v1/projects/project-a/subscriptions", Want: []int{200}},
			{Method: http.MethodDelete, Path: restSubPath, Want: []int{200, 404}},
			{Method: http.MethodDelete, Path: restTopicPath, Want: []int{200, 404}},
		}
	})
}

const (
	restPublishPath = restTopicPath + ":publish"
	restPullPath    = restSubPath + ":pull"
	restAckPath     = restSubPath + ":acknowledge"
	restModAckPath  = restSubPath + ":modifyAckDeadline"
)

// restDataFixture creates the REST topic and subscription; body is the
// subscription's create body after its topic.
func restDataFixture(t *testing.T, body string) (*Provider, *restHarness, *memoryPubSub) {
	t.Helper()
	p, registry, b := memoryFixture(t)
	c := newRESTClient(t, registry)
	c.do(owner, http.MethodPut, restTopicPath, `{}`, 200)
	c.do(owner, http.MethodPut, restSubPath, `{"topic":"rest-topic"`+body+`}`, 200)
	return p, c, b
}

// received returns the receivedMessages of one REST pull.
func received(t *testing.T, pulled map[string]any) []map[string]any {
	t.Helper()
	list, _ := pulled["receivedMessages"].([]any)
	out := make([]map[string]any, 0, len(list))
	for _, item := range list {
		entry, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("received message = %v", item)
		}
		out = append(out, entry)
	}
	return out
}

// pullOneREST pulls exactly one message over REST.
func pullOneREST(t *testing.T, c *restHarness) map[string]any {
	t.Helper()
	messages := received(t, c.do(owner, http.MethodPost, restPullPath, `{"maxMessages":1}`, 200))
	if len(messages) != 1 {
		t.Fatalf("pulled %d messages, want 1", len(messages))
	}
	return messages[0]
}

// ackIDsBody renders the ackIds body for one received message.
func ackIDsBody(t *testing.T, received map[string]any, extra string) string {
	t.Helper()
	id, ok := received["ackId"].(string)
	if !ok || id == "" {
		t.Fatalf("received message has no ackId: %v", received)
	}
	return `{"ackIds":["` + id + `"]` + extra + `}`
}

// messageOf returns the message of one received entry.
func messageOf(t *testing.T, received map[string]any) map[string]any {
	t.Helper()
	message, ok := received["message"].(map[string]any)
	if !ok {
		t.Fatalf("received entry has no message: %v", received)
	}
	return message
}

// emptyResponse fails unless a REST response is the empty JSON object.
func emptyResponse(t *testing.T, name string, response map[string]any) {
	t.Helper()
	if len(response) != 0 {
		t.Fatalf("%s = %v, want {}", name, response)
	}
}

// TestRESTPublishPullAcknowledge covers the REST data plane end to end in the
// reference's JSON shapes: base64 data, messageIds, and receivedMessages.
func TestRESTPublishPullAcknowledge(t *testing.T) {
	_, c, b := restDataFixture(t, "")
	data := base64.StdEncoding.EncodeToString([]byte("hello from rest"))
	published := c.do(owner, http.MethodPost, restPublishPath, `{"messages":[{"data":"`+data+`","attributes":{"k":"v"}}]}`, 200)
	ids, _ := published["messageIds"].([]any)
	if len(ids) != 1 {
		t.Fatalf("publish = %v", published)
	}
	first := pullOneREST(t, c)
	message := messageOf(t, first)
	attributes, _ := message["attributes"].(map[string]any)
	if message["data"] != data || message["messageId"] != ids[0] || attributes["k"] != "v" {
		t.Fatalf("received = %v", first)
	}
	if _, counted := first["deliveryAttempt"]; counted {
		t.Fatal("a subscription without a dead-letter policy reported deliveryAttempt")
	}
	emptyResponse(t, "modifyAckDeadline", c.do(owner, http.MethodPost, restModAckPath, ackIDsBody(t, first, `,"ackDeadlineSeconds":0`), 200))
	again := pullOneREST(t, c)
	if messageOf(t, again)["messageId"] != ids[0] {
		t.Fatalf("nacked message was not redelivered: %v", again)
	}
	emptyResponse(t, "acknowledge", c.do(owner, http.MethodPost, restAckPath, ackIDsBody(t, again, ""), 200))
	emptyResponse(t, "empty pull", c.do(owner, http.MethodPost, restPullPath, `{"maxMessages":5,"returnImmediately":true}`, 200))
	// GCP accepts a body that repeats the path's topic (pubsub-rest-data-plane).
	c.do(owner, http.MethodPost, restPublishPath, `{"topic":"projects/project-a/topics/rest-topic","messages":[{"data":"eA=="}]}`, 200)
	if b.called(publisher+"Publish") != 2 || b.called(subscriber+"Acknowledge") != 1 {
		t.Fatal("REST data-plane calls did not reach the backend once each")
	}
}

// TestRESTDataPlaneAuthorization proves REST publish, pull, acknowledge, and
// modifyAckDeadline evaluate the gRPC twins' permissions before any backend
// call, and that a resource-scoped grant admits only its own operation.
func TestRESTDataPlaneAuthorization(t *testing.T) {
	p, c, b := restDataFixture(t, "")
	before := b.total()
	c.do(other, http.MethodPost, restPublishPath, `{"messages":[{"data":"bm8="}]}`, http.StatusForbidden)
	c.do("", http.MethodPost, restPublishPath, `{"messages":[{"data":"bm8="}]}`, http.StatusUnauthorized)
	c.do(other, http.MethodPost, restPullPath, `{"maxMessages":1}`, http.StatusForbidden)
	c.do(other, http.MethodPost, restAckPath, `{"ackIds":["x"]}`, http.StatusForbidden)
	c.do(other, http.MethodPost, restModAckPath, `{"ackIds":["x"],"ackDeadlineSeconds":0}`, http.StatusForbidden)
	if b.total() != before {
		t.Fatalf("denied REST data-plane calls reached the backend: %d calls", b.total()-before)
	}
	_, err := p.iam.SetPolicy(canonical("projects/project-a/topics/rest-topic"), iam.Policy{Bindings: []iam.Binding{{Role: "roles/pubsub.publisher", Members: []string{other}}}})
	must(t, err)
	c.do(other, http.MethodPost, restPublishPath, `{"messages":[{"data":"eWVz"}]}`, 200)
	c.do(other, http.MethodPost, restPullPath, `{"maxMessages":1}`, http.StatusForbidden)
}

// restRefusal sends one request and asserts the HTTP status and the error
// message the refusal carries.
func restRefusal(t *testing.T, c *restHarness, path, body string, want int, message string) {
	t.Helper()
	response := c.do(owner, http.MethodPost, path, body, want)
	detail, _ := response["error"].(map[string]any)
	if got, _ := detail["message"].(string); !strings.Contains(got, message) {
		t.Errorf("%s %s: message %q, want %q", path, body, got, message)
	}
}

// TestRESTDataPlaneRefusals covers every refusal of the REST data plane with
// its message; none may reach the backend.
func TestRESTDataPlaneRefusals(t *testing.T) {
	_, c, b := restDataFixture(t, "")
	before := b.total()
	restRefusal(t, c, restPublishPath, `{"topic":"projects/project-a/topics/other","messages":[{"data":"eA=="}]}`, http.StatusBadRequest, "the topic differs from the request path")
	restRefusal(t, c, restPullPath, `{"subscription":"projects/project-a/subscriptions/other","maxMessages":1}`, http.StatusBadRequest, "the subscription differs from the request path")
	restRefusal(t, c, restAckPath, `{"subscription":"x","ackIds":["a"]}`, http.StatusBadRequest, "the subscription differs from the request path")
	restRefusal(t, c, restModAckPath, `{"subscription":"x","ackIds":["a"]}`, http.StatusBadRequest, "the subscription differs from the request path")
	restRefusal(t, c, restPublishPath, `{"messages":[{"data":"eA=="}],"unknown":1}`, http.StatusBadRequest, "Invalid request body")
	restRefusal(t, c, restPublishPath, `{"messages":[]}`, http.StatusBadRequest, "Supply one through 1000 messages")
	restRefusal(t, c, restPublishPath, `{"messages":[{}]}`, http.StatusBadRequest, "Messages need data or attributes")
	restRefusal(t, c, restPublishPath, `{"messages":[{"data":"eA==","publishTime":"2026-01-01T00:00:00Z"}]}`, http.StatusNotImplemented, "Pub/Sub coverage gap: field publish_time")
	restRefusal(t, c, restPullPath, `{}`, http.StatusBadRequest, "Max messages must be one through 1000")
	restRefusal(t, c, restModAckPath, `{"ackIds":["a"],"ackDeadlineSeconds":601}`, http.StatusBadRequest, "Ack deadline must be zero through 600 seconds")
	restRefusal(t, c, "/v1/projects/project-a/topics/absent:publish", `{"messages":[{"data":"eA=="}]}`, http.StatusNotFound, "Pub/Sub resource does not exist")
	if b.total() != before {
		t.Fatalf("refused REST data-plane calls reached the backend: %d calls", b.total()-before)
	}
}

// TestRESTDeliveryFeatures proves the REST data plane shares the provider's
// delivery semantics: dead-letter attempt counting without the service
// agent's grants, and an exactly-once repeated acknowledgement.
func TestRESTDeliveryFeatures(t *testing.T) {
	_, c, _ := restDataFixture(t, `,"deadLetterPolicy":{"deadLetterTopic":"projects/project-a/topics/rest-topic","maxDeliveryAttempts":5}`)
	c.do(owner, http.MethodPost, restPublishPath, `{"messages":[{"data":"cG9pc29u"}]}`, 200)
	for want := float64(1); want <= 6; want++ {
		message := pullOneREST(t, c)
		if message["deliveryAttempt"] != want {
			t.Fatalf("deliveryAttempt = %v, want %v", message["deliveryAttempt"], want)
		}
		c.do(owner, http.MethodPost, restModAckPath, ackIDsBody(t, message, `,"ackDeadlineSeconds":0`), 200)
	}

	_, c, b := restDataFixture(t, `,"enableExactlyOnceDelivery":true`)
	c.do(owner, http.MethodPost, restPublishPath, `{"messages":[{"data":"b25jZQ=="}]}`, 200)
	ack := ackIDsBody(t, pullOneREST(t, c), "")
	c.do(owner, http.MethodPost, restAckPath, ack, 200)
	c.do(owner, http.MethodPost, restAckPath, ack, 200)
	if b.called(subscriber+"Acknowledge") != 1 {
		t.Fatalf("a repeated exactly-once REST ack reached the backend %d times", b.called(subscriber+"Acknowledge"))
	}
}

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
	if status.Code(err) != codes.Unimplemented || !strings.Contains(err.Error(), "cross-project subscriptions") {
		t.Fatalf("cross-project attachment = %v", err)
	}
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
		{"subscription bigquery export", subscriber + "CreateSubscription", "projects/project-a/subscriptions/exported", &pb.Subscription{Name: "projects/project-a/subscriptions/exported", Topic: testTopic, BigqueryConfig: &pb.BigQueryConfig{Table: "p.d.t"}}, codes.Unimplemented},
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

// Shared fixture values for the memory-backed provider tests.
const (
	secondProject  = "project-b"
	kindProject    = "project"
	hierarchy      = "resourcemanager"
	roleAdmin      = "roles/pubsub.admin"
	roleSubscriber = "roles/pubsub.subscriber"
	roleViewer     = "roles/pubsub.viewer"
	pushSubName    = "projects/project-a/subscriptions/push"
	shortTopicName = "projects/project-a/topics/x"
	foreignTopic   = "projects/project-b/topics/topic"
	absentTopic    = "projects/project-a/topics/absent"
	getIAM         = policyRPC + "GetIamPolicy"
	setIAM         = policyRPC + "SetIamPolicy"
)

// memoryPubSub is a small in-process stand-in for the official emulator. It
// keeps topics, subscriptions, and per-subscription queues so provider flows
// observe realistic create/get/delete/publish/pull outcomes without Java.
type memoryPubSub struct {
	mu            sync.Mutex
	calls         []string
	topics        map[string]*pb.Topic
	subscriptions map[string]*pb.Subscription
	queues        map[string][]*pb.PubsubMessage
	outstanding   map[string]*pb.PubsubMessage
	acked         []string
	snapshots     map[string]*pb.Snapshot
	schemas       map[string]*pb.Schema // Avro schemas, unvalidated
	modacks       []*pb.ModifyAckDeadlineRequest
	updates       []*pb.UpdateSubscriptionRequest
	nextID        int
	starts        int
	fault         func(method string) error
	stream        func(ctx context.Context) (grpc.ClientStream, error)
}

func newMemoryPubSub() *memoryPubSub {
	return &memoryPubSub{topics: map[string]*pb.Topic{}, subscriptions: map[string]*pb.Subscription{}, queues: map[string][]*pb.PubsubMessage{}, outstanding: map[string]*pb.PubsubMessage{}, snapshots: map[string]*pb.Snapshot{}}
}

func (m *memoryPubSub) Start(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.starts++
	return nil
}
func (*memoryPubSub) Ready(context.Context) error { return nil }
func (*memoryPubSub) Stop(context.Context) error  { return nil }
func (m *memoryPubSub) Reset(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.topics, m.subscriptions = map[string]*pb.Topic{}, map[string]*pb.Subscription{}
	m.queues, m.outstanding = map[string][]*pb.PubsubMessage{}, map[string]*pb.PubsubMessage{}
	m.snapshots = map[string]*pb.Snapshot{}
	return nil
}
func (m *memoryPubSub) client() grpc.ClientConnInterface { return m }

func (m *memoryPubSub) NewStream(ctx context.Context, _ *grpc.StreamDesc, method string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
	m.mu.Lock()
	m.calls = append(m.calls, method)
	open := m.stream
	m.mu.Unlock()
	if md, _ := metadata.FromOutgoingContext(ctx); len(md) != 0 {
		return nil, status.Error(codes.Internal, "caller metadata reached backend stream")
	}
	if open == nil {
		return nil, status.Error(codes.Unimplemented, "no scripted stream")
	}
	return open(ctx)
}

// called reports how many times the backend saw one method.
func (m *memoryPubSub) called(method string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, seen := range m.calls {
		if seen == method {
			count++
		}
	}
	return count
}

// total reports every backend call so far.
func (m *memoryPubSub) total() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func (m *memoryPubSub) Invoke(ctx context.Context, method string, request, response any, _ ...grpc.CallOption) error {
	if md, _ := metadata.FromOutgoingContext(ctx); len(md) != 0 {
		return status.Error(codes.Internal, "caller metadata reached backend")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, method)
	if m.fault != nil {
		if err := m.fault(method); err != nil {
			return err
		}
	}
	in, inOK := request.(proto.Message)
	out, outOK := response.(proto.Message)
	if !inOK || !outOK {
		return status.Error(codes.Internal, "non-proto backend call")
	}
	result, err := m.apply(method, in)
	if err != nil {
		return err
	}
	proto.Merge(out, result)
	return nil
}

// apply executes one unary method against the in-memory state.
func (m *memoryPubSub) apply(method string, in proto.Message) (proto.Message, error) {
	if result, handled, err := m.applyTopic(in); handled {
		return result, err
	}
	if result, handled, err := m.applySubscription(in); handled {
		return result, err
	}
	if result, handled, err := m.applyMessages(in); handled {
		return result, err
	}
	if result, handled, err := m.applySnapshot(in); handled {
		return result, err
	}
	if result, handled, err := m.applySchema(in); handled {
		return result, err
	}
	return nil, status.Error(codes.Unimplemented, "memory backend does not serve "+method)
}

// applyTopic serves the topic control plane.
func (m *memoryPubSub) applyTopic(in proto.Message) (proto.Message, bool, error) {
	switch r := in.(type) {
	case *pb.Topic:
		if _, found := m.topics[r.GetName()]; found {
			return nil, true, status.Error(codes.AlreadyExists, "Topic already exists")
		}
		stored, err := cloneMessage(r)
		m.topics[r.GetName()] = stored
		return stored, true, err
	case *pb.GetTopicRequest:
		if topic, found := m.topics[r.GetTopic()]; found {
			return topic, true, nil
		}
		return nil, true, status.Error(codes.NotFound, "Topic not found")
	case *pb.DeleteTopicRequest:
		if _, found := m.topics[r.GetTopic()]; !found {
			return nil, true, status.Error(codes.NotFound, "Topic not found")
		}
		delete(m.topics, r.GetTopic())
		return &emptypb.Empty{}, true, nil
	case *pb.ListTopicsRequest:
		list := &pb.ListTopicsResponse{}
		for _, topic := range m.topics {
			list.Topics = append(list.Topics, topic)
		}
		return list, true, nil
	}
	return nil, false, nil
}

// applySubscription serves the subscription control plane. Like the official
// emulator behind the provider, it does not retain labels; the provider does.
func (m *memoryPubSub) applySubscription(in proto.Message) (proto.Message, bool, error) {
	switch r := in.(type) {
	case *pb.Subscription:
		result, err := m.createSubscription(r)
		return result, true, err
	case *pb.GetSubscriptionRequest:
		if subscription, found := m.subscriptions[r.GetSubscription()]; found {
			return subscription, true, nil
		}
		return nil, true, status.Error(codes.NotFound, "Subscription not found")
	case *pb.DeleteSubscriptionRequest:
		if _, found := m.subscriptions[r.GetSubscription()]; !found {
			return nil, true, status.Error(codes.NotFound, "Subscription not found")
		}
		delete(m.subscriptions, r.GetSubscription())
		return &emptypb.Empty{}, true, nil
	case *pb.ListSubscriptionsRequest:
		list := &pb.ListSubscriptionsResponse{}
		for _, subscription := range m.subscriptions {
			list.Subscriptions = append(list.Subscriptions, subscription)
		}
		return list, true, nil
	case *pb.UpdateSubscriptionRequest:
		result, err := m.updateSubscription(r)
		return result, true, err
	}
	return nil, false, nil
}

// updateSubscription applies the one mask the provider sends: the
// dead-letter policy it forwards or clears as the service agent's grants change.
func (m *memoryPubSub) updateSubscription(r *pb.UpdateSubscriptionRequest) (proto.Message, error) {
	stored, found := m.subscriptions[r.GetSubscription().GetName()]
	if !found {
		return nil, status.Error(codes.NotFound, "Subscription not found")
	}
	if paths := r.GetUpdateMask().GetPaths(); len(paths) != 1 || paths[0] != "dead_letter_policy" {
		return nil, status.Error(codes.Internal, "unexpected update mask")
	}
	m.updates = append(m.updates, proto.CloneOf(r))
	stored.DeadLetterPolicy = proto.CloneOf(r.GetSubscription().GetDeadLetterPolicy())
	return stored, nil
}

// applySnapshot serves snapshots and seek. Like the emulator, it drops labels.
func (m *memoryPubSub) applySnapshot(in proto.Message) (proto.Message, bool, error) {
	switch r := in.(type) {
	case *pb.CreateSnapshotRequest:
		source, found := m.subscriptions[r.GetSubscription()]
		if !found {
			return nil, true, status.Error(codes.NotFound, "Subscription does not exist")
		}
		snapshot := &pb.Snapshot{Name: r.GetName(), Topic: source.GetTopic()}
		m.snapshots[r.GetName()] = snapshot
		return snapshot, true, nil
	case *pb.GetSnapshotRequest:
		if snapshot, found := m.snapshots[r.GetSnapshot()]; found {
			return snapshot, true, nil
		}
		return nil, true, status.Error(codes.NotFound, "Snapshot does not exist")
	case *pb.ListSnapshotsRequest:
		list := &pb.ListSnapshotsResponse{}
		for _, snapshot := range m.snapshots {
			list.Snapshots = append(list.Snapshots, snapshot)
		}
		return list, true, nil
	case *pb.DeleteSnapshotRequest:
		delete(m.snapshots, r.GetSnapshot())
		return &emptypb.Empty{}, true, nil
	case *pb.SeekRequest:
		return &pb.SeekResponse{}, true, nil
	}
	return nil, false, nil
}

// createSubscription stores a pull subscription; the provider must have
// stripped any push configuration before forwarding.
func (m *memoryPubSub) createSubscription(r *pb.Subscription) (proto.Message, error) {
	if r.GetPushConfig() != nil {
		return nil, status.Error(codes.Internal, "push config reached the backend")
	}
	if r.GetFilter() == "unparsable" {
		return nil, status.Error(codes.Unknown, "Application error processing RPC")
	}
	if _, found := m.subscriptions[r.GetName()]; found {
		return nil, status.Error(codes.AlreadyExists, "Subscription already exists")
	}
	stored, err := cloneMessage(r)
	if err != nil {
		return nil, err
	}
	stored.Labels = nil
	m.subscriptions[r.GetName()] = stored
	return stored, nil
}

// applyMessages serves publish, pull, ack, and ack-deadline changes.
func (m *memoryPubSub) applyMessages(in proto.Message) (proto.Message, bool, error) {
	switch r := in.(type) {
	case *pb.PublishRequest:
		result, err := m.publish(r)
		return result, true, err
	case *pb.PullRequest:
		result, err := m.pull(r)
		return result, true, err
	case *pb.AcknowledgeRequest:
		for _, id := range r.GetAckIds() {
			delete(m.outstanding, id)
			m.acked = append(m.acked, id)
		}
		return &emptypb.Empty{}, true, nil
	case *pb.ModifyAckDeadlineRequest:
		m.modacks = append(m.modacks, proto.CloneOf(r))
		if r.GetAckDeadlineSeconds() == 0 {
			m.requeue(r.GetSubscription(), r.GetAckIds())
		}
		return &emptypb.Empty{}, true, nil
	}
	return nil, false, nil
}

// requeue returns nacked outstanding messages to the head of their queue.
func (m *memoryPubSub) requeue(subscription string, ackIDs []string) {
	for _, id := range ackIDs {
		if message, found := m.outstanding[id]; found {
			delete(m.outstanding, id)
			m.queues[subscription] = append([]*pb.PubsubMessage{message}, m.queues[subscription]...)
		}
	}
}

func (m *memoryPubSub) publish(r *pb.PublishRequest) (proto.Message, error) {
	if _, found := m.topics[r.GetTopic()]; !found {
		return nil, status.Error(codes.NotFound, "Topic not found")
	}
	response := &pb.PublishResponse{}
	for _, message := range r.GetMessages() {
		m.nextID++
		stored, err := cloneMessage(message)
		if err != nil {
			return nil, err
		}
		stored.MessageId = strconv.Itoa(m.nextID)
		response.MessageIds = append(response.MessageIds, stored.GetMessageId())
		for name, subscription := range m.subscriptions {
			if subscription.GetTopic() == r.GetTopic() {
				m.queues[name] = append(m.queues[name], stored)
			}
		}
	}
	return response, nil
}

func (m *memoryPubSub) pull(r *pb.PullRequest) (proto.Message, error) {
	if _, found := m.subscriptions[r.GetSubscription()]; !found {
		return nil, status.Error(codes.NotFound, "Subscription not found")
	}
	response := &pb.PullResponse{}
	queue := m.queues[r.GetSubscription()]
	for len(queue) > 0 && len(response.GetReceivedMessages()) < int(r.GetMaxMessages()) {
		message := queue[0]
		queue = queue[1:]
		ackID := r.GetSubscription() + "#" + message.GetMessageId()
		m.outstanding[ackID] = message
		response.ReceivedMessages = append(response.ReceivedMessages, &pb.ReceivedMessage{AckId: ackID, Message: message})
	}
	m.queues[r.GetSubscription()] = queue
	return response, nil
}

// memoryFixture builds a provider over the memory backend with two projects:
// owner administers project-a and other administers project-b.
func memoryFixture(t *testing.T) (*Provider, *gateway.Registry, *memoryPubSub) {
	t.Helper()
	index := state.NewIndex()
	admins := map[string]string{testProject: owner, secondProject: other}
	engine, err := iam.New(index)
	must(t, err)
	for project, admin := range admins {
		must(t, index.Put(hierarchy, state.Resource{Name: provider.ProjectResource(project), Service: hierarchy, Kind: kindProject, Number: "123456", Scope: state.Scope{Project: project, Location: state.Global}}))
		_, err = engine.SetPolicy(provider.ProjectResource(project), iam.Policy{Bindings: []iam.Binding{{Role: roleAdmin, Members: []string{admin}}}})
		must(t, err)
	}
	manager := backend.NewManager(time.Second, time.Second)
	registry, b := gateway.NewRegistry(), newMemoryPubSub()
	p, err := newProvider(registry, manager, index, engine, func(state.Scope) (backend.Backend, error) { return b, nil })
	must(t, err)
	t.Cleanup(func() { must(t, manager.Shutdown(context.Background())) })
	return p, registry, b
}

// invokeAs dispatches one gRPC-shaped request through the registry.
func invokeAs(registry *gateway.Registry, actor, method, resource string, request proto.Message) (any, error) {
	response, err := registry.Invoke(call(method, resource, actor), request)
	if err != nil {
		return nil, fmt.Errorf("invoke %s: %w", method, err)
	}
	return response, nil
}

// expectCode fails the test unless err carries the wanted gRPC code.
func expectCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if status.Code(err) != want {
		t.Fatalf("got %v (%v), want %v", status.Code(err), err, want)
	}
}
