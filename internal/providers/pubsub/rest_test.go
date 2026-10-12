package pubsub

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/gateway"
)

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
