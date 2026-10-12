package pubsub

import (
	"encoding/base64"
	"net/http"
	"strings"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/iam"
)

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
