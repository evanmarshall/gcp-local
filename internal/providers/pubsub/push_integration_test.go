//go:build pubsubintegration

package pubsub

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const pushEmail = "push-worker@project-a.iam.gserviceaccount.com"
const pushAgent = "serviceAccount:service-123456@gcp-sa-pubsub.iam.gserviceaccount.com"

func pushAccount(t *testing.T, f *integration) string {
	t.Helper()
	account := iam.AccountResource("project-a", pushEmail)
	must(t, f.p.index.Put("iam", state.Resource{Name: account, Service: "iam", Kind: "serviceAccount", Number: "123456789012345678901", Parent: projectResource("project-a"), Scope: state.Scope{Project: "project-a", Location: state.Global}}))
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
