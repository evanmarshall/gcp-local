package pubsub

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
