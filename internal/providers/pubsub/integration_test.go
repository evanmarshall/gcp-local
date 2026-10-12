//go:build pubsubintegration

package pubsub

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	core "github.com/evanmarshall/gcp-local/internal/runtime"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const subjectSecret = "gls.synthetic-pubsub-sdk-subject-token"

type integration struct {
	p              *Provider
	server         *core.Server
	connection     *grpc.ClientConn
	owner, other   context.Context
	http, endpoint string
	stop           func()
}

func running(t *testing.T, extra ...identity.Enrollment) *integration {
	t.Helper()
	interfaces, err := net.Interfaces()
	must(t, err)
	if os.Getenv("GCP_LOCAL_ISOLATED") != "1" || len(interfaces) != 1 || interfaces[0].Name != "lo" {
		t.Fatal("Run actual Pub/Sub tests in the loopback-only runner")
	}
	config := Config{Java: os.Getenv("GCP_LOCAL_JAVA"), JAR: os.Getenv("GCP_LOCAL_PUBSUB_JAR")}
	registry, index := gateway.NewRegistry(), state.NewIndex()
	for _, project := range []string{"project-a", "project-b"} {
		number := map[string]string{"project-a": "123456", "project-b": "654321"}[project]
		must(t, index.Put("resourcemanager", state.Resource{Name: provider.ProjectResource(project), Service: "resourcemanager", Kind: "project", Number: number, Scope: state.Scope{Project: project, Location: state.Global}}))
	}
	engine, err := iam.New(index)
	must(t, err)
	for project, actor := range map[string]string{"project-a": owner, "project-b": other} {
		_, err = engine.SetPolicy(provider.ProjectResource(project), iam.Policy{Bindings: []iam.Binding{{Role: "roles/pubsub.admin", Members: []string{actor}}}})
		must(t, err)
	}
	identities, err := identity.New(identity.Config{Principals: append([]identity.Enrollment{
		{Subject: owner, Project: "project-a", SubjectToken: subjectSecret},
		{Subject: other, Project: "project-b", SubjectToken: subjectSecret + "-peer"},
	}, extra...)}, nil)
	must(t, err)
	server := core.WithGateway("pubsub-integration", 5*time.Second, gateway.New(registry, identities.Resolve))
	config.IDTokens, err = identity.NewIDTokens(engine, index)
	must(t, err)
	identities.IDTokens = config.IDTokens
	server.Identity, server.IAM = identities, engine
	p, err := New(registry, server.Backends, index, engine, config)
	must(t, err)
	listen := func() net.Listener { l, err := net.Listen("tcp", "127.0.0.1:0"); must(t, err); return l }
	listeners := core.Listeners{HTTP: listen(), GRPC: listen()}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listeners) }()
	connection, err := grpc.NewClient(listeners.GRPC.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	must(t, err)
	testCtx, cancelTest := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancelTest)
	auth := func(secret string) context.Context {
		form := url.Values{"grant_type": {"urn:ietf:params:oauth:grant-type:token-exchange"}, "audience": {identity.Audience}, "subject_token_type": {identity.AccessTokenType}, "requested_token_type": {identity.AccessTokenType}, "subject_token": {secret}, "scope": {identity.CloudPlatform}}
		request := httptest.NewRequest("POST", "/v1/token", strings.NewReader(form.Encode()))
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		identities.ServeHTTP(w, request)
		if w.Code != 200 {
			t.Fatal("local token exchange failed", w.Code)
		}
		var token struct {
			AccessToken string `json:"access_token"`
		}
		must(t, json.Unmarshal(w.Body.Bytes(), &token))
		return metadata.AppendToOutgoingContext(testCtx, "authorization", "Bearer "+token.AccessToken)
	}
	stopped := false
	stop := func() {
		if !stopped {
			stopped = true
			cancel()
			must(t, <-done)
			connection.Close()
		}
	}
	t.Cleanup(stop)
	return &integration{p: p, server: server, connection: connection, owner: auth(subjectSecret), other: auth(subjectSecret + "-peer"), http: "http://" + listeners.HTTP.Addr().String(), endpoint: listeners.GRPC.Addr().String(), stop: stop}
}

func TestOfficialEmulatorThroughAuthenticatedGateway(t *testing.T) {
	f := running(t)
	pub, sub := pb.NewPublisherClient(f.connection), pb.NewSubscriberClient(f.connection)
	_, err := pub.CreateTopic(f.other, &pb.Topic{Name: testTopic})
	if status.Code(err) != codes.PermissionDenied || len(f.server.Backends.Status()) != 0 {
		t.Fatal("denial started backend", err)
	}
	_, err = pub.CreateTopic(f.owner, &pb.Topic{Name: testTopic})
	must(t, err)
	_, err = sub.CreateSubscription(f.owner, &pb.Subscription{Name: testSub, Topic: testTopic, AckDeadlineSeconds: 10})
	must(t, err)
	_, err = pub.Publish(f.other, &pb.PublishRequest{Topic: testTopic, Messages: []*pb.PubsubMessage{{Data: []byte("denied")}}})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatal(err)
	}
	_, err = sub.Pull(f.other, &pb.PullRequest{Subscription: testSub, MaxMessages: 1})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatal(err)
	}

	stream, err := sub.StreamingPull(f.owner)
	must(t, err)
	must(t, stream.Send(&pb.StreamingPullRequest{Subscription: testSub, StreamAckDeadlineSeconds: 10, MaxOutstandingMessages: 10, MaxOutstandingBytes: 10240}))
	_, err = stream.Header()
	must(t, err) // Provider has established its managed session.
	published, err := pub.Publish(f.owner, &pb.PublishRequest{Topic: testTopic, Messages: []*pb.PubsubMessage{{Data: []byte("streamed"), Attributes: map[string]string{"key": "value"}}}})
	must(t, err)
	message, err := stream.Recv()
	must(t, err)
	if len(message.ReceivedMessages) != 1 || string(message.ReceivedMessages[0].Message.Data) != "streamed" || message.ReceivedMessages[0].Message.MessageId != published.MessageIds[0] {
		t.Fatal("stream changed message")
	}
	must(t, stream.Send(&pb.StreamingPullRequest{AckIds: []string{message.ReceivedMessages[0].AckId}}))
	must(t, stream.CloseSend())
	_, err = stream.Recv()
	if err != io.EOF {
		t.Fatal("half-close did not drain", err)
	}

	// Protocol version 1 (the Go client's): the emulator answers a keepalive.
	pinged, err := sub.StreamingPull(f.owner)
	must(t, err)
	must(t, pinged.Send(&pb.StreamingPullRequest{Subscription: testSub, StreamAckDeadlineSeconds: 10, ProtocolVersion: 1}))
	_, err = pinged.Header()
	must(t, err)
	must(t, pinged.Send(&pb.StreamingPullRequest{}))
	pong, err := pinged.Recv()
	must(t, err)
	if len(pong.GetReceivedMessages()) != 0 {
		t.Fatal("keepalive answer carried messages", pong)
	}
	must(t, pinged.CloseSend())
	for { // Drain any further keepalive answers until the emulator closes.
		pong, err = pinged.Recv()
		if err == io.EOF {
			break
		}
		must(t, err)
		if len(pong.GetReceivedMessages()) != 0 {
			t.Fatal("keepalive answer carried messages", pong)
		}
	}

	// Nack and redelivery stay in the official emulator.
	published, err = pub.Publish(f.owner, &pb.PublishRequest{Topic: testTopic, Messages: []*pb.PubsubMessage{{Data: []byte("redeliver")}}})
	must(t, err)
	pulled, err := sub.Pull(f.owner, &pb.PullRequest{Subscription: testSub, MaxMessages: 1})
	must(t, err)
	if len(pulled.ReceivedMessages) != 1 {
		t.Fatal("missing pull")
	}
	_, err = sub.ModifyAckDeadline(f.owner, &pb.ModifyAckDeadlineRequest{Subscription: testSub, AckIds: []string{pulled.ReceivedMessages[0].AckId}, AckDeadlineSeconds: 0})
	must(t, err)
	pulled, err = sub.Pull(f.owner, &pb.PullRequest{Subscription: testSub, MaxMessages: 1})
	must(t, err)
	if len(pulled.ReceivedMessages) != 1 || pulled.ReceivedMessages[0].Message.MessageId != published.MessageIds[0] {
		t.Fatal("nack did not redeliver original message")
	}
	_, err = sub.Acknowledge(f.owner, &pb.AcknowledgeRequest{Subscription: testSub, AckIds: []string{pulled.ReceivedMessages[0].AckId}})
	must(t, err)

	// A client cannot switch subscription within an authorized stream.
	switched, err := sub.StreamingPull(f.owner)
	must(t, err)
	must(t, switched.Send(&pb.StreamingPullRequest{Subscription: testSub, StreamAckDeadlineSeconds: 10}))
	_, err = switched.Header()
	must(t, err)
	must(t, switched.Send(&pb.StreamingPullRequest{Subscription: "projects/project-b/subscriptions/foreign"}))
	_, err = switched.Recv()
	if status.Code(err) != codes.InvalidArgument {
		t.Fatal("stream switched scope", err)
	}

	peerTopic := "projects/project-b/topics/topic"
	_, err = pub.CreateTopic(f.other, &pb.Topic{Name: peerTopic})
	must(t, err)
	cancelCtx, cancel := context.WithCancel(f.owner)
	st, err := sub.StreamingPull(cancelCtx)
	must(t, err)
	must(t, st.Send(&pb.StreamingPullRequest{Subscription: testSub, StreamAckDeadlineSeconds: 10}))
	_, err = st.Header()
	must(t, err)
	cancel()
	_, err = st.Recv()
	if status.Code(err) != codes.Canceled {
		t.Fatal("client cancellation failed", err)
	}

	st, err = sub.StreamingPull(f.owner)
	must(t, err)
	must(t, st.Send(&pb.StreamingPullRequest{Subscription: testSub, StreamAckDeadlineSeconds: 10}))
	_, err = st.Header()
	must(t, err)
	must(t, f.server.Backends.Reset(f.owner, keyOf(call("", testTopic, owner))))
	_, err = st.Recv()
	if status.Code(err) != codes.Canceled {
		t.Fatal("reset did not cancel stream", err)
	}
	_, err = pub.GetTopic(f.owner, &pb.GetTopicRequest{Topic: testTopic})
	if status.Code(err) != codes.NotFound {
		t.Fatal("reset retained topic", err)
	}
	_, err = pub.GetTopic(f.other, &pb.GetTopicRequest{Topic: peerTopic})
	must(t, err)
	_, err = pub.CreateTopic(f.owner, &pb.Topic{Name: testTopic})
	must(t, err)
	_, err = sub.CreateSubscription(f.owner, &pb.Subscription{Name: testSub, Topic: testTopic})
	must(t, err)
	st, err = sub.StreamingPull(f.owner)
	must(t, err)
	must(t, st.Send(&pb.StreamingPullRequest{Subscription: testSub, StreamAckDeadlineSeconds: 10}))
	_, err = st.Header()
	must(t, err)
	f.stop() // Active streams must drain before the JVM is stopped.
	for _, s := range f.server.Backends.Status() {
		if s.Phase != "stopped" {
			t.Fatal("backend survived shutdown", s)
		}
	}
}

func TestPythonSDKKeepsADCOnLocalGRPC(t *testing.T) {
	f := running(t)
	python := os.Getenv("GCP_LOCAL_TEST_PYTHON")
	if python == "" {
		t.Fatal("Explicit pinned Python environment is required")
	}
	directory := t.TempDir()
	adc := localADC(t, f, directory, "owner", subjectSecret)
	peerADC := localADC(t, f, directory, "peer", subjectSecret+"-peer")
	command := exec.CommandContext(f.owner, python, "testdata/sdk.py", f.endpoint)
	command.Env = sdkEnvironment(directory, adc, "GCP_LOCAL_DENIED_ADC="+peerADC)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("Python Pub/Sub SDK: %v\n%s", err, output)
	}
	t.Log(string(output))
	// SDK cleanup must be observable through another authenticated client.
	_, err = pb.NewPublisherClient(f.connection).GetTopic(f.owner, &pb.GetTopicRequest{Topic: "projects/project-a/topics/python-sdk"})
	if status.Code(err) != codes.NotFound {
		t.Fatal("SDK left topic behind", err)
	}
	_, err = pb.NewSubscriberClient(f.connection).GetSubscription(f.owner, &pb.GetSubscriptionRequest{Subscription: "projects/project-a/subscriptions/python-sdk"})
	if status.Code(err) != codes.NotFound {
		t.Fatal("SDK left subscription behind", err)
	}
}

// localADC writes an owner-only local ADC profile for one enrolled subject.
func localADC(t *testing.T, f *integration, directory, label, subject string) string {
	t.Helper()
	secret := filepath.Join(directory, label+"-subject.txt")
	must(t, os.WriteFile(secret, []byte(subject), 0600))
	profile := map[string]any{"type": "external_account", "audience": identity.Audience, "subject_token_type": identity.AccessTokenType, "token_url": f.http + "/v1/token", "credential_source": map[string]string{"file": secret}}
	raw, err := json.Marshal(profile)
	must(t, err)
	adc := filepath.Join(directory, label+"-adc.json")
	must(t, os.WriteFile(adc, raw, 0600))
	return adc
}

// sdkEnvironment is the isolated environment for a Python SDK script.
func sdkEnvironment(directory, adc string, extra ...string) []string {
	return append([]string{"PATH=/usr/bin:/bin", "GOOGLE_APPLICATION_CREDENTIALS=" + adc, "GOOGLE_CLOUD_PROJECT=project-a", "CLOUDSDK_CONFIG=" + filepath.Join(directory, "empty-cloudsdk"), "GOOGLE_API_USE_CLIENT_CERTIFICATE=false", "GOOGLE_API_USE_MTLS_ENDPOINT=never", "GCP_LOCAL_ISOLATED=1"}, extra...)
}

// TestPythonSDKDeliveryFeatures drives ordering, resume_publish, dead-letter
// grants, filters, and exactly-once results through google-cloud-pubsub.
func TestPythonSDKDeliveryFeatures(t *testing.T) {
	f := running(t)
	python := os.Getenv("GCP_LOCAL_TEST_PYTHON")
	if python == "" {
		t.Fatal("Explicit pinned Python environment is required")
	}
	directory := t.TempDir()
	command := exec.CommandContext(f.owner, python, "testdata/sdk_delivery.py", f.endpoint, "123456")
	command.Env = sdkEnvironment(directory, localADC(t, f, directory, "owner", subjectSecret))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("Python Pub/Sub delivery SDK: %v\n%s", err, output)
	}
	t.Log(string(output))
	listed, err := pb.NewPublisherClient(f.connection).ListTopics(f.owner, &pb.ListTopicsRequest{Project: "projects/project-a"})
	must(t, err)
	if len(listed.GetTopics()) != 0 {
		t.Fatalf("SDK left topics behind: %v", listed.GetTopics())
	}
}

// TestPythonSDKSchemas drives SchemaServiceClient and schema-bound topics
// through google-cloud-pubsub with local ADC.
func TestPythonSDKSchemas(t *testing.T) {
	f := running(t)
	python := os.Getenv("GCP_LOCAL_TEST_PYTHON")
	if python == "" {
		t.Fatal("Explicit pinned Python environment is required")
	}
	directory := t.TempDir()
	command := exec.CommandContext(f.owner, python, "testdata/sdk_schemas.py", f.endpoint)
	command.Env = sdkEnvironment(directory, localADC(t, f, directory, "owner", subjectSecret))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("Python Pub/Sub schema SDK: %v\n%s", err, output)
	}
	t.Log(string(output))
	listed, err := pb.NewSchemaServiceClient(f.connection).ListSchemas(f.owner, &pb.ListSchemasRequest{Parent: "projects/project-a"})
	must(t, err)
	if len(listed.GetSchemas()) != 0 {
		t.Fatalf("SDK left schemas behind: %v", listed.GetSchemas())
	}
}

func TestPolicyRPCRevocationStopsDelivery(t *testing.T) {
	f := running(t)
	pub, sub, policies := pb.NewPublisherClient(f.connection), pb.NewSubscriberClient(f.connection), iampb.NewIAMPolicyClient(f.connection)
	_, err := pub.CreateTopic(f.owner, &pb.Topic{Name: testTopic})
	must(t, err)
	_, err = sub.CreateSubscription(f.owner, &pb.Subscription{Name: testSub, Topic: testTopic})
	must(t, err)
	_, err = policies.SetIamPolicy(f.other, &iampb.SetIamPolicyRequest{Resource: testTopic, Policy: &iampb.Policy{Bindings: []*iampb.Binding{{Role: "roles/pubsub.admin", Members: []string{other}}}}})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatal("policy elevation succeeded", err)
	}
	original, err := policies.GetIamPolicy(f.owner, &iampb.GetIamPolicyRequest{Resource: testSub})
	must(t, err)
	granted, err := policies.SetIamPolicy(f.owner, &iampb.SetIamPolicyRequest{Resource: testSub, Policy: &iampb.Policy{Etag: original.Etag, Bindings: []*iampb.Binding{{Role: "roles/pubsub.subscriber", Members: []string{other}}}}})
	must(t, err)
	_, err = policies.SetIamPolicy(f.owner, &iampb.SetIamPolicyRequest{Resource: testSub, Policy: original})
	if status.Code(err) != codes.Aborted {
		t.Fatal("stale policy etag accepted", err)
	}
	_, err = policies.SetIamPolicy(f.owner, &iampb.SetIamPolicyRequest{Resource: testTopic, Policy: &iampb.Policy{Bindings: []*iampb.Binding{{Role: "roles/pubsub.publisher", Members: []string{other}}}}})
	must(t, err)
	_, err = pub.Publish(f.other, &pb.PublishRequest{Topic: testTopic, Messages: []*pb.PubsubMessage{{Data: []byte("resource-grant")}}})
	must(t, err)
	pulled, err := sub.Pull(f.other, &pb.PullRequest{Subscription: testSub, MaxMessages: 1})
	must(t, err)
	if len(pulled.ReceivedMessages) != 1 {
		t.Fatal("resource grant did not deliver")
	}
	_, err = sub.Acknowledge(f.other, &pb.AcknowledgeRequest{Subscription: testSub, AckIds: []string{pulled.ReceivedMessages[0].AckId}})
	must(t, err)
	stream, err := sub.StreamingPull(f.other)
	must(t, err)
	must(t, stream.Send(&pb.StreamingPullRequest{Subscription: testSub, StreamAckDeadlineSeconds: 10}))
	_, err = stream.Header()
	must(t, err)
	_, err = policies.SetIamPolicy(f.owner, &iampb.SetIamPolicyRequest{Resource: testSub, Policy: &iampb.Policy{Etag: granted.Etag}})
	must(t, err)
	_, err = pub.Publish(f.owner, &pb.PublishRequest{Topic: testTopic, Messages: []*pb.PubsubMessage{{Data: []byte("after-revocation")}}})
	must(t, err)
	_, err = stream.Recv()
	if status.Code(err) != codes.PermissionDenied {
		t.Fatal("revoked stream delivered data", err)
	}
	_, err = sub.Acknowledge(f.other, &pb.AcknowledgeRequest{Subscription: testSub, AckIds: []string{"invented"}})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatal("revoked subscriber acknowledged", err)
	}
}
