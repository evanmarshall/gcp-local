//go:build pubsubintegration

package pubsub

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	core "github.com/evanmarshall/gcp-local/internal/runtime"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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

// recorderRun starts the provider with the scenario's fixture, allowed, and
// denied identities and writes their local ADC bundle. Only fixture is a
// project-level Pub/Sub admin.
func recorderRun(t *testing.T) (*integration, string, string) {
	t.Helper()
	var enrollments []identity.Enrollment
	for _, actor := range []string{"fixture", "allowed", "denied"} {
		enrollments = append(enrollments, identity.Enrollment{Subject: "serviceAccount:" + actor + "@project-a.iam.gserviceaccount.com", Project: "project-a", SubjectToken: subjectSecret + "-recorder-" + actor})
	}
	f := running(t, enrollments...)
	_, err := f.p.iam.SetPolicy(provider.ProjectResource("project-a"), iam.Policy{Bindings: []iam.Binding{{Role: "roles/pubsub.admin", Members: []string{owner, enrollments[0].Subject}}}})
	must(t, err)
	directory := t.TempDir()
	profiles := map[string]any{}
	for i, actor := range []string{"fixture", "allowed", "denied"} {
		home := filepath.Join(directory, actor)
		must(t, os.Mkdir(home, 0700))
		subject := filepath.Join(home, "subject.txt")
		must(t, os.WriteFile(subject, []byte(enrollments[i].SubjectToken), 0600))
		profile := map[string]any{"type": "external_account", "audience": identity.Audience, "subject_token_type": identity.AccessTokenType, "token_url": f.http + "/v1/token", "credential_source": map[string]string{"file": subject}}
		raw, err := json.Marshal(profile)
		must(t, err)
		adc := filepath.Join(home, "adc.json")
		must(t, os.WriteFile(adc, raw, 0600))
		profiles[actor] = map[string]string{"email": actor + "@project-a.iam.gserviceaccount.com", "adc": adc}
	}
	raw, err := json.Marshal(map[string]any{"project": "project-a", "identities": profiles})
	must(t, err)
	path := filepath.Join(directory, "identities.json")
	must(t, os.WriteFile(path, raw, 0600))
	return f, path, directory
}

// runScenarioScript runs one scenario driver from testdata with the repository on PYTHONPATH.
func runScenarioScript(t *testing.T, f *integration, script string, args ...string) {
	t.Helper()
	root, err := filepath.Abs("../../../..")
	must(t, err)
	command := exec.CommandContext(f.owner, os.Getenv("GCP_LOCAL_TEST_PYTHON"), append([]string{script, "http://" + f.endpoint}, args...)...)
	command.Env = []string{"PATH=/usr/bin:/bin", "PYTHONPATH=" + root, "GCP_LOCAL_ISOLATED=1", "GOOGLE_API_USE_CLIENT_CERTIFICATE=false", "GOOGLE_API_USE_MTLS_ENDPOINT=never"}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("%s: %v\n%s", script, err, output)
	}
	t.Log(string(output))
}

func TestBoundedRecorderThroughProductionLocalADC(t *testing.T) {
	f, identities, directory := recorderRun(t)
	runScenarioScript(t, f, "testdata/recording.py", identities, filepath.Join(directory, "state"))
}

// TestDeliveryScenarioThroughProductionLocalADC runs the
// pubsub-delivery-features scenario locally and compares it with its GCP
// recording.
func TestDeliveryScenarioThroughProductionLocalADC(t *testing.T) {
	f, identities, directory := recorderRun(t)
	runScenarioScript(t, f, "testdata/delivery_recording.py", identities, filepath.Join(directory, "state"), "123456")
}

// TestSchemasScenarioThroughProductionLocalADC runs the pubsub-schemas
// scenario locally and compares it with its GCP recording.
func TestSchemasScenarioThroughProductionLocalADC(t *testing.T) {
	f, identities, directory := recorderRun(t)
	runScenarioScript(t, f, "testdata/schemas_recording.py", identities, filepath.Join(directory, "state"))
}

// restTestTopic is one topic name used across the REST surface tests.
const restTestTopic = "/v1/projects/project-a/topics/rest-topic"

// restClient is the HTTP client for the REST surface tests.
var restClient = &http.Client{}

// restToken exchanges one subject token for the local access token.
func restToken(f *integration, secret string) (string, error) {
	form := "grant_type=urn:ietf:params:oauth:grant-type:token-exchange&audience=" + identity.Audience +
		"&subject_token_type=" + identity.AccessTokenType + "&requested_token_type=" + identity.AccessTokenType +
		"&subject_token=" + secret + "&scope=" + identity.CloudPlatform
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, f.http+"/v1/token", strings.NewReader(form))
	if err != nil {
		return "", err
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := restClient.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	var token struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(response.Body).Decode(&token); err != nil {
		return "", err
	}
	return token.AccessToken, nil
}

// restCall posts one REST request against the running fixture gateway.
func restCall(f *integration, t testing.TB, verb, path, secret string, body any) (int, []byte) {
	t.Helper()
	encoded := []byte{}
	if body != nil {
		encoded, _ = json.Marshal(body)
	}
	token, err := restToken(f, secret)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(context.Background(), verb, f.http+path, bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := restClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, payload
}

// TestRestSurfaceTopicLifecycle drives the JSON REST surface through the
// same emulator and IAM evaluation as the gRPC dispatch.
func TestRestSurfaceTopicLifecycle(t *testing.T) {
	f := running(t)
	code, payload := restCall(f, t, "PUT", restTestTopic, subjectSecret, map[string]any{"labels": map[string]any{"env": "test"}})
	if code != 200 {
		t.Fatalf("topic create status = %d body %s", code, payload)
	}
	var created struct {
		Name   string            `json:"name"`
		Labels map[string]string `json:"labels"`
	}
	if err := json.Unmarshal(payload, &created); err != nil {
		t.Fatalf("decode create: %v", err)
	}
	if created.Name != "projects/project-a/topics/rest-topic" || created.Labels["env"] != "test" {
		t.Fatalf("topic = %+v", created)
	}
	code, payload = restCall(f, t, "PUT", restTestTopic, subjectSecret, map[string]any{})
	if code != 409 {
		t.Fatalf("duplicate create status = %d body %s", code, payload)
	}
	code, _ = restCall(f, t, "GET", restTestTopic, subjectSecret, nil)
	if code != 200 {
		t.Fatalf("topic get status = %d", code)
	}
	code, payload = restCall(f, t, "GET", "/v1/projects/project-a/topics", subjectSecret, nil)
	if code != 200 {
		t.Fatalf("topic list status = %d", code)
	}
	if !strings.Contains(string(payload), "rest-topic") {
		t.Fatalf("list body = %s", payload)
	}
	// The gRPC surface observes the REST-created topic (shared state).
	pub := pb.NewPublisherClient(f.connection)
	if _, err := pub.GetTopic(f.owner, &pb.GetTopicRequest{Topic: "projects/project-a/topics/rest-topic"}); err != nil {
		t.Fatalf("grpc get after rest create: %v", err)
	}
	code, _ = restCall(f, t, "DELETE", restTestTopic, subjectSecret, nil)
	if code != 200 {
		t.Fatalf("topic delete status = %d", code)
	}
	code, _ = restCall(f, t, "GET", restTestTopic, subjectSecret, nil)
	if code != 404 {
		t.Fatalf("get after delete status = %d", code)
	}
}

// TestRestSurfaceSubscriptionLifecycle creates a subscription over REST and
// verifies it against the gRPC surface.
func TestRestSurfaceSubscriptionLifecycle(t *testing.T) {
	f := running(t)
	if code, _ := restCall(f, t, "PUT", restTestTopic, subjectSecret, map[string]any{}); code != 200 {
		t.Fatalf("topic create status = %d", code)
	}
	code, payload := restCall(f, t, "PUT", "/v1/projects/project-a/subscriptions/rest-sub", subjectSecret,
		map[string]any{"topic": "projects/project-a/topics/rest-topic", "ackDeadlineSeconds": 20})
	if code != 200 {
		t.Fatalf("subscription create status = %d body %s", code, payload)
	}
	code, payload = restCall(f, t, "GET", "/v1/projects/project-a/subscriptions/rest-sub", subjectSecret, nil)
	if code != 200 {
		t.Fatalf("subscription get status = %d", code)
	}
	var sub struct {
		Name               string `json:"name"`
		Topic              string `json:"topic"`
		AckDeadlineSeconds int    `json:"ackDeadlineSeconds"`
	}
	if err := json.Unmarshal(payload, &sub); err != nil {
		t.Fatalf("decode subscription: %v", err)
	}
	if sub.AckDeadlineSeconds != 20 || sub.Topic != "projects/project-a/topics/rest-topic" {
		t.Fatalf("subscription = %+v", sub)
	}
	if code, _ := restCall(f, t, "DELETE", "/v1/projects/project-a/subscriptions/rest-sub", subjectSecret, nil); code != 200 {
		t.Fatalf("subscription delete status = %d", code)
	}
}

// TestRestSurfaceDeniedIsDenied exercises authorization parity: the peer
// principal is refused on the REST surface exactly as on gRPC.
func TestRestSurfaceDeniedIsDenied(t *testing.T) {
	f := running(t)
	if code, _ := restCall(f, t, "PUT", restTestTopic, subjectSecret+"-peer", map[string]any{"labels": map[string]any{}}); code != 403 {
		t.Fatalf("denied create status = %d", code)
	}
}

// restPulled is the REST pull response shape.
type restPulled struct {
	ReceivedMessages []struct {
		AckID   string `json:"ackId"`
		Message struct {
			Data        string            `json:"data"`
			MessageID   string            `json:"messageId"`
			PublishTime string            `json:"publishTime"`
			Attributes  map[string]string `json:"attributes"`
		} `json:"message"`
	} `json:"receivedMessages"`
}

// TestRestSurfaceDataPlane publishes, pulls, nacks, and acknowledges over REST
// against the official emulator, and checks the gRPC surface sees the same
// subscription state.
func TestRestSurfaceDataPlane(t *testing.T) {
	f := running(t)
	const sub = "/v1/projects/project-a/subscriptions/rest-data"
	if code, _ := restCall(f, t, "PUT", restTestTopic, subjectSecret, map[string]any{}); code != 200 {
		t.Fatalf("topic create status = %d", code)
	}
	if code, payload := restCall(f, t, "PUT", sub, subjectSecret, map[string]any{"topic": "rest-topic"}); code != 200 {
		t.Fatalf("subscription create status = %d body %s", code, payload)
	}
	if code, _ := restCall(f, t, "POST", restTestTopic+":publish", subjectSecret+"-peer", map[string]any{"messages": []any{map[string]any{"data": "bm8="}}}); code != 403 {
		t.Fatalf("denied publish status = %d", code)
	}
	code, payload := restCall(f, t, "POST", restTestTopic+":publish", subjectSecret, map[string]any{"messages": []any{map[string]any{"data": "aGVsbG8=", "attributes": map[string]any{"k": "v"}}}})
	var published struct {
		MessageIDs []string `json:"messageIds"`
	}
	if code != 200 || json.Unmarshal(payload, &published) != nil || len(published.MessageIDs) != 1 {
		t.Fatalf("publish status = %d body %s", code, payload)
	}
	pull := func() restPulled {
		t.Helper()
		code, payload := restCall(f, t, "POST", sub+":pull", subjectSecret, map[string]any{"maxMessages": 1})
		var out restPulled
		if code != 200 || json.Unmarshal(payload, &out) != nil || len(out.ReceivedMessages) != 1 {
			t.Fatalf("pull status = %d body %s", code, payload)
		}
		return out
	}
	first := pull().ReceivedMessages[0]
	if first.Message.Data != "aGVsbG8=" || first.Message.MessageID != published.MessageIDs[0] || first.Message.PublishTime == "" || first.Message.Attributes["k"] != "v" {
		t.Fatalf("received = %+v", first)
	}
	if code, payload := restCall(f, t, "POST", sub+":modifyAckDeadline", subjectSecret, map[string]any{"ackIds": []string{first.AckID}, "ackDeadlineSeconds": 0}); code != 200 || string(bytes.TrimSpace(payload)) != "{}" {
		t.Fatalf("nack status = %d body %s", code, payload)
	}
	second := pull().ReceivedMessages[0]
	if second.Message.MessageID != first.Message.MessageID {
		t.Fatalf("nacked message was not redelivered: %+v", second)
	}
	if code, payload := restCall(f, t, "POST", sub+":acknowledge", subjectSecret, map[string]any{"ackIds": []string{second.AckID}}); code != 200 {
		t.Fatalf("ack status = %d body %s", code, payload)
	}
	response, err := pb.NewSubscriberClient(f.connection).Pull(f.owner, &pb.PullRequest{Subscription: "projects/project-a/subscriptions/rest-data", MaxMessages: 1, ReturnImmediately: true})
	if err != nil || len(response.GetReceivedMessages()) != 0 {
		t.Fatalf("grpc pull after the REST ack = %v, %v", response, err)
	}
}

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

const pushEmail = "push-worker@project-a.iam.gserviceaccount.com"
const pushAgent = "serviceAccount:service-123456@gcp-sa-pubsub.iam.gserviceaccount.com"

func pushAccount(t *testing.T, f *integration) string {
	t.Helper()
	account := iam.AccountResource("project-a", pushEmail)
	must(t, f.p.index.Put("iam", state.Resource{Name: account, Service: "iam", Kind: "serviceAccount", Number: "123456789012345678901", Parent: provider.ProjectResource("project-a"), Scope: state.Scope{Project: "project-a", Location: state.Global}}))
	return account
}
func grantPush(t *testing.T, f *integration, account string, agent bool) {
	t.Helper()
	bindings := []iam.Binding{{Role: "roles/iam.serviceAccountUser", Members: []string{owner}}}
	if agent {
		bindings = append(bindings, iam.Binding{Role: "roles/iam.serviceAccountTokenCreator", Members: []string{pushAgent}})
	}
	_, err := f.p.iam.SetPolicy(account, iam.Policy{Bindings: bindings})
	must(t, err)
}
func pushConfig(endpoint string) *pb.PushConfig {
	return &pb.PushConfig{PushEndpoint: endpoint, AuthenticationMethod: &pb.PushConfig_OidcToken_{OidcToken: &pb.PushConfig_OidcToken{ServiceAccountEmail: pushEmail, Audience: "push-test"}}}
}

type receivedPush struct{ token, id, data, subscription string }

func awaitPush(t *testing.T, messages <-chan receivedPush) receivedPush {
	t.Helper()
	select {
	case message := <-messages:
		return message
	case <-time.After(8 * time.Second):
		t.Fatal("push delivery timed out")
		return receivedPush{}
	}
}

func TestAuthenticatedPushPermissionsRetriesAndDeletion(t *testing.T) {
	f := running(t)
	defer f.stop()
	account := pushAccount(t, f)
	pub, sub := pb.NewPublisherClient(f.connection), pb.NewSubscriberClient(f.connection)
	messages := make(chan receivedPush, 10)
	var attempts atomic.Int32
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" {
			w.WriteHeader(400)
			return
		}
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		claims, err := f.p.tokens.Verify(token, "push-test")
		if err != nil || claims.Email != pushEmail || claims.Subject != "123456789012345678901" {
			w.WriteHeader(401)
			return
		}
		var body struct {
			Subscription string `json:"subscription"`
			Message      struct {
				ID   string `json:"messageId"`
				Data string `json:"data"`
			} `json:"message"`
		}
		if json.NewDecoder(io.LimitReader(r.Body, 2<<20)).Decode(&body) != nil {
			w.WriteHeader(400)
			return
		}
		data, err := base64.StdEncoding.DecodeString(body.Message.Data)
		if err != nil {
			w.WriteHeader(400)
			return
		}
		messages <- receivedPush{token: token, id: body.Message.ID, data: string(data), subscription: body.Subscription}
		if attempts.Add(1) == 1 {
			w.WriteHeader(500)
		} else {
			w.WriteHeader(204)
		}
	}))
	defer receiver.Close()
	_, err := pub.CreateTopic(f.owner, &pb.Topic{Name: testTopic})
	must(t, err)
	request := &pb.Subscription{Name: testSub, Topic: testTopic, PushConfig: pushConfig(receiver.URL), AckDeadlineSeconds: 10}
	_, err = sub.CreateSubscription(f.owner, request)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatal("missing actAs accepted", err)
	}
	_, err = sub.GetSubscription(f.owner, &pb.GetSubscriptionRequest{Subscription: testSub})
	if status.Code(err) != codes.NotFound {
		t.Fatal("denied creation made subscription", err)
	}
	grantPush(t, f, account, false) // Creation needs actAs; delivery needs the separate agent grant.
	created, err := sub.CreateSubscription(f.owner, request)
	must(t, err)
	if !proto.Equal(created.PushConfig, request.PushConfig) {
		t.Fatal("push config was not preserved")
	}
	must(t, f.server.Backends.Use(f.owner, keyOf(call("", testSub, owner)), func(ctx context.Context, b backend.Backend) error {
		actual, err := invoke(ctx, b.(driver), subscriber+"GetSubscription", &pb.GetSubscriptionRequest{Subscription: testSub})
		if err == nil && actual.(*pb.Subscription).GetPushConfig().GetPushEndpoint() != "" {
			t.Error("backend received push URL")
		}
		return err
	}))
	published, err := pub.Publish(f.owner, &pb.PublishRequest{Topic: testTopic, Messages: []*pb.PubsubMessage{{Data: []byte("authenticated")}}})
	must(t, err)
	select {
	case <-messages:
		t.Fatal("missing service-agent permission still delivered")
	case <-time.After(1200 * time.Millisecond):
	}
	grantPush(t, f, account, true)
	first, second := awaitPush(t, messages), awaitPush(t, messages)
	if first.id != published.MessageIds[0] || second.id != first.id || first.data != "authenticated" || second.subscription != testSub {
		t.Fatal("push retry changed message")
	}
	// Validate the token independently with Google's Python JWT verifier. Token
	// bytes travel only through stdin; reports and command arguments contain none.
	certClient := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{Proxy: nil}}
	certResponse, err := certClient.Get(f.http + "/_local/identity/certs")
	must(t, err)
	var certs map[string]string
	must(t, json.NewDecoder(io.LimitReader(certResponse.Body, 64<<10)).Decode(&certs))
	certResponse.Body.Close()
	certClient.CloseIdleConnections()
	if certResponse.StatusCode != 200 || len(certs) != 1 {
		t.Fatal("local issuer did not expose its public certificate")
	}
	encoded, _ := json.Marshal(map[string]any{"token": second.token, "certs": certs})
	command := exec.CommandContext(f.owner, os.Getenv("GCP_LOCAL_TEST_PYTHON"), "-c", `import json,sys; from google.auth import jwt; v=json.load(sys.stdin); c=jwt.decode(v['token'],certs=v['certs'],audience='push-test'); assert c['iss']=='https://identity.gcp-local.invalid' and c['email']=='push-worker@project-a.iam.gserviceaccount.com' and c['email_verified'] is True`)
	command.Env = []string{"PATH=/usr/bin:/bin"}
	command.Stdin = bytes.NewReader(encoded)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("Independent JWT verification: %v\n%s", err, output)
	}
	_, err = sub.Pull(f.owner, &pb.PullRequest{Subscription: testSub, MaxMessages: 1})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatal("pull stole a push message", err)
	}
	grantPush(t, f, account, false)
	_, err = pub.Publish(f.owner, &pb.PublishRequest{Topic: testTopic, Messages: []*pb.PubsubMessage{{Data: []byte("revoked")}}})
	must(t, err)
	select {
	case <-messages:
		t.Fatal("revoked service agent delivered")
	case <-time.After(1200 * time.Millisecond):
	}
	value, _ := f.p.pushes.Load(canonical(testSub))
	worker := value.(*pushSubscription)
	_, err = sub.DeleteSubscription(f.owner, &pb.DeleteSubscriptionRequest{Subscription: testSub})
	must(t, err)
	select {
	case <-worker.done:
	default:
		t.Fatal("deletion left a worker")
	}
	if f.p.pushCount(scopeOf(call("", testSub, owner))) != 0 {
		t.Fatal("deletion retained push state")
	}
}

func TestPushResetAndShutdownCancelReceiversAndNeverFollowRedirects(t *testing.T) {
	f := running(t)
	defer f.stop()
	account := pushAccount(t, f)
	grantPush(t, f, account, true)
	pub, sub := pb.NewPublisherClient(f.connection), pb.NewSubscriberClient(f.connection)
	var forbidden atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { forbidden.Add(1); w.WriteHeader(204) }))
	defer target.Close()
	redirected := make(chan struct{}, 1)
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case redirected <- struct{}{}:
		default:
		}
		http.Redirect(w, r, target.URL, 302)
	}))
	defer redirect.Close()
	_, err := pub.CreateTopic(f.owner, &pb.Topic{Name: testTopic})
	must(t, err)
	_, err = sub.CreateSubscription(f.owner, &pb.Subscription{Name: testSub, Topic: testTopic, PushConfig: pushConfig(redirect.URL)})
	must(t, err)
	_, err = pub.Publish(f.owner, &pb.PublishRequest{Topic: testTopic, Messages: []*pb.PubsubMessage{{Data: []byte("redirect")}}})
	must(t, err)
	select {
	case <-redirected:
	case <-time.After(5 * time.Second):
		t.Fatal("no redirect attempt")
	}
	_, err = sub.DeleteSubscription(f.owner, &pb.DeleteSubscriptionRequest{Subscription: testSub})
	must(t, err)
	if forbidden.Load() != 0 {
		t.Fatal("push followed a redirect")
	}
	entered, cancelled := make(chan struct{}), make(chan struct{})
	blocked := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		close(entered)
		<-r.Context().Done()
		close(cancelled)
	}))
	defer blocked.Close()
	_, err = sub.CreateSubscription(f.owner, &pb.Subscription{Name: testSub, Topic: testTopic, PushConfig: pushConfig(blocked.URL)})
	must(t, err)
	_, err = pub.Publish(f.owner, &pb.PublishRequest{Topic: testTopic, Messages: []*pb.PubsubMessage{{Data: []byte("cancel")}}})
	must(t, err)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("receiver was not called")
	}
	must(t, f.server.Backends.Reset(f.owner, keyOf(call("", testSub, owner))))
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("reset left HTTP delivery active")
	}
	if f.p.pushCount(scopeOf(call("", testSub, owner))) != 0 {
		t.Fatal("reset retained push workers")
	}
	if _, found := f.p.index.Get(account); !found {
		t.Fatal("Pub/Sub reset removed the independently owned service account")
	}
	// The runtime must also stop a push that is active while listeners drain.
	shutdownEntered, shutdownCancelled := make(chan struct{}), make(chan struct{})
	shutdownReceiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		close(shutdownEntered)
		<-r.Context().Done()
		close(shutdownCancelled)
	}))
	defer shutdownReceiver.Close()
	_, err = pub.CreateTopic(f.owner, &pb.Topic{Name: testTopic})
	must(t, err)
	_, err = sub.CreateSubscription(f.owner, &pb.Subscription{Name: testSub, Topic: testTopic, PushConfig: pushConfig(shutdownReceiver.URL)})
	must(t, err)
	_, err = pub.Publish(f.owner, &pb.PublishRequest{Topic: testTopic, Messages: []*pb.PubsubMessage{{Data: []byte("shutdown")}}})
	must(t, err)
	select {
	case <-shutdownEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown receiver was not called")
	}
	f.stop()
	select {
	case <-shutdownCancelled:
	case <-time.After(time.Second):
		t.Fatal("shutdown left HTTP delivery active")
	}
}
