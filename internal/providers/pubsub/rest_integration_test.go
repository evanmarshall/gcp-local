//go:build pubsubintegration

package pubsub

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/internal/identity"
)

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
