package cloudscheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/scheduler/apiv1/schedulerpb"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Names repeated across the tests.
const (
	testProject  = "demo-scheduler"
	testLocation = "us-central1"
	testNumber   = "123456789012"
	agent        = "serviceAccount:service-" + testNumber + "@gcp-sa-cloudscheduler.iam.gserviceaccount.com"
	basePath     = "/v1/projects/" + testProject + "/locations/" + testLocation + "/jobs"
	projectName  = "//cloudresourcemanager.googleapis.com/projects/" + testProject
)

// start is the manual clock's initial time: 06:02 in New York.
var start = time.Date(2026, 10, 12, 10, 2, 0, 0, time.UTC)

// manualClock is a Clock that moves only when a test advances it.
type manualClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []waiter
}

type waiter struct {
	at time.Time
	ch chan time.Time
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- c.now
		return ch
	}
	c.waiters = append(c.waiters, waiter{c.now.Add(d), ch})
	return ch
}

// Advance moves the clock and fires every timer that is due.
func (c *manualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	pending := c.waiters[:0]
	for _, w := range c.waiters {
		if w.at.After(c.now) {
			pending = append(pending, w)
			continue
		}
		w.ch <- c.now
	}
	c.waiters = pending
}

// waiting counts timers that have not fired.
func (c *manualClock) waiting() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.waiters)
}

// harness drives the provider through the gateway with a manual clock.
type harness struct {
	t        *testing.T
	gate     *gateway.Gateway
	engine   *iam.Engine
	index    *state.Index
	registry *gateway.Registry
	provider *Provider
	clock    *manualClock
}

// account is the full IAM name of one test service account.
func account(name string) string { return iam.AccountResource(testProject, email(name)) }

// email names one test principal's service account.
func email(name string) string { return name + "@" + testProject + ".iam.gserviceaccount.com" }

func newHarness(t *testing.T, configure func(*Config, *state.Index, *iam.Engine)) *harness {
	t.Helper()
	registry, index := gateway.NewRegistry(), state.NewIndex()
	engine, err := iam.New(index)
	must(t, err)
	must(t, index.Put("resourcemanager", state.Resource{Name: projectName, Service: "resourcemanager", Kind: "project",
		Number: testNumber, Scope: state.Scope{Project: testProject, Location: state.Global}}))
	_, err = engine.SetPolicy(projectName, iam.Policy{Bindings: []iam.Binding{
		{Role: "roles/cloudscheduler.admin", Members: []string{"serviceAccount:" + email("actor")}},
		{Role: "roles/cloudscheduler.viewer", Members: []string{"serviceAccount:" + email("viewer")}},
		{Role: "roles/cloudscheduler.serviceAgent", Members: []string{agent}},
	}})
	must(t, err)
	for i, name := range []string{"worker", "other"} {
		must(t, index.Put("iam", state.Resource{Name: account(name), Service: "iam", Kind: "serviceAccount",
			Parent: projectName, Number: "10000000000000000000" + string(rune('1'+i)), Scope: state.Scope{Project: testProject, Location: state.Global}}))
	}
	_, err = engine.SetPolicy(account("worker"), iam.Policy{Bindings: []iam.Binding{
		{Role: "roles/iam.serviceAccountUser", Members: []string{"serviceAccount:" + email("actor")}},
	}})
	must(t, err)
	clock := &manualClock{now: start}
	config := Config{Clock: clock}
	if configure != nil {
		configure(&config, index, engine)
	}
	p, err := New(context.Background(), config, registry, backend.NewManager(time.Second, time.Second), index, engine)
	must(t, err)
	g := gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:" + email(token), Project: testProject, Scopes: []string{identity.CloudPlatform}}, nil
	})
	return &harness{t: t, gate: g, engine: engine, index: index, registry: registry, provider: p, clock: clock}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// call issues one REST request as the named principal; "" is anonymous.
func (h *harness) call(token, verb, path string, body any) (int, map[string]any) {
	h.t.Helper()
	var encoded []byte
	if body != nil {
		encoded, _ = json.Marshal(body) //nolint:errchkjson // plain map bodies
	}
	request := httptest.NewRequestWithContext(context.Background(), verb, path, bytes.NewReader(encoded))
	if encoded != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	recorder := httptest.NewRecorder()
	h.gate.ServeHTTP(recorder, request)
	document := map[string]any{}
	_ = json.Unmarshal(recorder.Body.Bytes(), &document)
	return recorder.Code, document
}

// create creates one job as the admin and fails the test unless it succeeds.
func (h *harness) create(id string, job map[string]any) map[string]any {
	h.t.Helper()
	job["name"] = "projects/" + testProject + "/locations/" + testLocation + "/jobs/" + id
	code, document := h.call("actor", http.MethodPost, basePath, job)
	if code != http.StatusOK {
		h.t.Fatalf("create %s = %d %v", id, code, document)
	}
	return document
}

// get reads one job as the admin.
func (h *harness) get(id string) map[string]any {
	h.t.Helper()
	_, document := h.call("actor", http.MethodGet, basePath+"/"+id, nil)
	return document
}

// eventually waits in real time for a worker to reach a state.
func eventually(t *testing.T, what string, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// errorMessage returns a REST error body's message.
func errorMessage(document map[string]any) string {
	inner, _ := document["error"].(map[string]any)
	message, _ := inner["message"].(string)
	return message
}

// target records the requests an HTTP target receives and answers each with
// the next status in its script, then 200.
type target struct {
	mu       sync.Mutex
	script   []int
	requests []*http.Request
	bodies   []string
	server   *httptest.Server
}

func newTarget(t *testing.T, script ...int) *target {
	t.Helper()
	tg := &target{script: script}
	tg.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := new(bytes.Buffer)
		_, _ = body.ReadFrom(r.Body)
		tg.mu.Lock()
		tg.requests, tg.bodies = append(tg.requests, r), append(tg.bodies, body.String())
		code := http.StatusOK
		if len(tg.script) > 0 {
			code, tg.script = tg.script[0], tg.script[1:]
		}
		tg.mu.Unlock()
		w.WriteHeader(code)
	}))
	t.Cleanup(tg.server.Close)
	return tg
}

func (tg *target) count() int {
	tg.mu.Lock()
	defer tg.mu.Unlock()
	return len(tg.requests)
}

// lookup reads a dotted path of a decoded JSON document; a missing step is nil.
func lookup(document map[string]any, path string) any {
	var current any = document
	for part := range strings.SplitSeq(path, ".") {
		object, _ := current.(map[string]any)
		current = object[part]
	}
	return current
}

// expect fails the test unless the call answered code with each wanted
// value at its dotted path; a nil value means the path is absent.
func expect(t *testing.T, label string, code int, document map[string]any, wantCode int, want map[string]any) {
	t.Helper()
	if code != wantCode {
		t.Fatalf("%s: status %d, want %d (%v)", label, code, wantCode, document)
	}
	for path, value := range want {
		if got := lookup(document, path); !reflect.DeepEqual(got, value) {
			t.Fatalf("%s: %s = %#v, want %#v (in %v)", label, path, got, value, document)
		}
	}
}

func TestJobLifecycleMatchesRecordedShapes(t *testing.T) {
	h := newHarness(t, nil)
	created := h.create("nightly", map[string]any{
		"description": "gcp-local", "schedule": "0 9 * * *", "timeZone": "America/New_York",
		"pubsubTarget": map[string]any{"topicName": "projects/" + testProject + "/topics/events", "data": "Z2NwLWxvY2Fs"},
		"retryConfig":  map[string]any{"retryCount": 1},
	})
	// 09:00 in New York (EDT) is 13:00 UTC; a job that has not run reports
	// code -1; a supplied retryConfig echoes every default (recorded).
	expect(t, "create", 200, created, 200, map[string]any{
		"scheduleTime": "2026-10-12T13:00:00Z", "state": "ENABLED", "status.code": float64(-1), "attemptDeadline": nil,
		"retryConfig.retryCount": float64(1), "retryConfig.maxRetryDuration": "0s", "retryConfig.minBackoffDuration": "5s",
		"retryConfig.maxBackoffDuration": "3600s", "retryConfig.maxDoublings": float64(5),
	})
	code, listed := h.call("viewer", http.MethodGet, basePath, nil)
	expect(t, "list", code, listed, 200, map[string]any{"nextPageToken": nil})
	h.clock.Advance(time.Minute)
	code, paused := h.call("actor", http.MethodPost, basePath+"/nightly:pause", map[string]any{})
	expect(t, "pause", code, paused, 200, map[string]any{"scheduleTime": nil, "state": "PAUSED", "userUpdateTime": "2026-10-12T10:03:00Z"})
	code, resumed := h.call("actor", http.MethodPost, basePath+"/nightly:resume", map[string]any{})
	expect(t, "resume", code, resumed, 200, map[string]any{"state": "ENABLED", "scheduleTime": "2026-10-12T13:00:00Z"})
	code, patched := h.call("actor", http.MethodPatch, basePath+"/nightly?updateMask=description,schedule", map[string]any{"description": "updated", "schedule": "30 2 1 1 *"})
	expect(t, "patch", code, patched, 200, map[string]any{"description": "updated", "scheduleTime": "2027-01-01T07:30:00Z", "timeZone": "America/New_York"})
	code, deleted := h.call("actor", http.MethodDelete, basePath+"/nightly", nil)
	expect(t, "delete", code, deleted, 200, nil)
	code, missing := h.call("actor", http.MethodGet, basePath+"/nightly", nil)
	expect(t, "deleted get", code, missing, 404, nil)
	if len(h.provider.workers) != 0 {
		t.Fatal("delete leaves the worker running")
	}
}

func TestHTTPTargetDefaultsAndPaging(t *testing.T) {
	h := newHarness(t, nil)
	hook := "https://example.invalid/hook"
	created := h.create("http", map[string]any{
		"schedule": "*/5 * * * *",
		"httpTarget": map[string]any{"uri": hook, "body": "Z2NwLWxvY2Fs", "headers": map[string]any{"X-Gcp-Local": "1", "User-Agent": "mine"},
			"oidcToken": map[string]any{"serviceAccountEmail": email("worker")}},
	})
	// An absent retryConfig stays absent (recorded).
	expect(t, "create", 200, created, 200, map[string]any{
		"httpTarget.httpMethod": "POST", "httpTarget.headers.User-Agent": userAgent, "httpTarget.headers.Content-Type": octetStream,
		"httpTarget.headers.X-Gcp-Local": "1", "httpTarget.oidcToken.audience": hook,
		"attemptDeadline": "180s", "retryConfig": nil,
	})
	h.create("second", map[string]any{"schedule": "*/5 * * * *", "pubsubTarget": map[string]any{"topicName": "projects/" + testProject + "/topics/t", "attributes": map[string]any{"a": "b"}}})
	code, page := h.call("actor", http.MethodGet, basePath+"?pageSize=1", nil)
	expect(t, "first page", code, page, 200, map[string]any{"nextPageToken": "1", "jobs": []any{h.get("http")}})
	code, page = h.call("actor", http.MethodGet, basePath+"?pageSize=1&pageToken=1", nil)
	expect(t, "second page", code, page, 200, map[string]any{"nextPageToken": nil, "jobs": []any{h.get("second")}})
}

// attempted reads a job once its worker has recorded count target requests
// and moved scheduleTime to next.
func (h *harness) attempted(tg *target, id string, count int, next string) map[string]any {
	h.t.Helper()
	var job map[string]any
	eventually(h.t, id+" attempt "+next, func() bool {
		job = h.get(id)
		return tg.count() == count && job["scheduleTime"] == next
	})
	return job
}

func TestScheduledHTTPDeliveryWithOIDCAndRetries(t *testing.T) {
	var tokens *identity.IDTokens
	h := newHarness(t, func(config *Config, index *state.Index, engine *iam.Engine) {
		var err error
		tokens, err = identity.NewIDTokens(engine, index)
		must(t, err)
		config.IDTokens = tokens
	})
	tg := newTarget(t, http.StatusServiceUnavailable, http.StatusServiceUnavailable)
	h.create("ping", map[string]any{
		"schedule": "*/5 * * * *", "timeZone": "America/New_York", "attemptDeadline": "30s",
		"retryConfig": map[string]any{"retryCount": 2, "minBackoffDuration": "10s", "maxBackoffDuration": "60s", "maxDoublings": 1},
		"httpTarget":  map[string]any{"uri": tg.server.URL + "/ping", "httpMethod": "PUT", "body": "aGk=", "oidcToken": map[string]any{"serviceAccountEmail": email("worker")}},
	})
	eventually(t, "the worker to wait for 10:05", func() bool { return h.clock.waiting() == 1 })
	h.clock.Advance(3 * time.Minute)
	failed := h.attempted(tg, "ping", 1, "2026-10-12T10:05:10Z")
	expect(t, "after a 503", 200, failed, 200, map[string]any{"status.code": float64(14), "lastAttemptTime": "2026-10-12T10:05:00Z"})
	request := tg.requests[0]
	delivered := map[string]any{"method": request.Method, "body": tg.bodies[0], "User-Agent": request.Header.Get("User-Agent"),
		"X-Cloudscheduler": request.Header.Get("X-Cloudscheduler"), "X-Cloudscheduler-Jobname": request.Header.Get("X-Cloudscheduler-Jobname")}
	expect(t, "delivery", 200, delivered, 200, map[string]any{"method": http.MethodPut, "body": "hi", "User-Agent": userAgent,
		"X-Cloudscheduler": "true", "X-Cloudscheduler-Jobname": "ping"})
	claims, err := tokens.Verify(strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer "), tg.server.URL+"/ping")
	must(t, err)
	if claims.Email != email("worker") {
		t.Fatalf("OIDC token names %s", claims.Email)
	}
	eventually(t, "the retry timer", func() bool { return h.clock.waiting() == 1 })
	h.clock.Advance(10 * time.Second)
	// The second retry waits twice the minimum backoff.
	h.attempted(tg, "ping", 2, "2026-10-12T10:05:30Z")
	eventually(t, "the second retry timer", func() bool { return h.clock.waiting() == 1 })
	h.clock.Advance(20 * time.Second)
	succeeded := h.attempted(tg, "ping", 3, "2026-10-12T10:10:00Z")
	// A successful attempt reports an empty status (recorded).
	expect(t, "after success", 200, succeeded, 200, map[string]any{"status": map[string]any{}})
}

func TestPauseHoldsAndRunDispatchesNow(t *testing.T) {
	h := newHarness(t, nil)
	tg := newTarget(t)
	h.create("held", map[string]any{"schedule": "*/5 * * * *", "httpTarget": map[string]any{"uri": tg.server.URL, "httpMethod": "GET"}})
	h.call("actor", http.MethodPost, basePath+"/held:pause", map[string]any{})
	h.clock.Advance(time.Hour)
	if tg.count() != 0 {
		t.Fatal("a paused job ran on schedule")
	}
	code, ran := h.call("actor", http.MethodPost, basePath+"/held:run", map[string]any{})
	expect(t, "run", code, ran, 200, map[string]any{"state": "PAUSED"})
	eventually(t, "the forced run", func() bool { return tg.count() == 1 && h.get("held")["lastAttemptTime"] != nil })
	expect(t, "after the run", 200, h.get("held"), 200, map[string]any{"scheduleTime": nil})
	must(t, h.provider.Reset(provider.ProjectScope(testProject)))
	code, missing := h.call("actor", http.MethodGet, basePath+"/held", nil)
	expect(t, "after reset", code, missing, 404, nil)
	if len(h.provider.workers) != 0 {
		t.Fatal("reset leaves the worker running")
	}
}

func TestPauseStopsRetries(t *testing.T) {
	h := newHarness(t, nil)
	tg := newTarget(t, http.StatusInternalServerError)
	h.create("flaky", map[string]any{"schedule": "0 0 1 1 *", "retryConfig": map[string]any{"retryCount": 2},
		"httpTarget": map[string]any{"uri": tg.server.URL}})
	eventually(t, "the worker to wait for its schedule", func() bool { return h.clock.waiting() == 1 })
	h.call("actor", http.MethodPost, basePath+"/flaky:run", map[string]any{})
	// The yearly schedule's timer and the retry timer.
	eventually(t, "the retry timer", func() bool { return tg.count() == 1 && h.clock.waiting() == 2 })
	h.call("actor", http.MethodPost, basePath+"/flaky:pause", map[string]any{})
	h.clock.Advance(time.Minute)
	time.Sleep(200 * time.Millisecond) // Room for a wrongly retrying worker to attempt.
	if tg.count() != 1 {
		t.Fatalf("a paused job retried: %d attempts", tg.count())
	}
}

// fakePublish stands in for the Pub/Sub provider: it authorizes the caller
// for pubsub.topics.publish on the topic, as the provider does, and records
// each published request.
func fakePublish(t *testing.T, h *harness) chan *pubsubpb.PublishRequest {
	t.Helper()
	topic := "//pubsub.googleapis.com/projects/" + testProject + "/topics/events"
	must(t, h.index.Put("pubsub", state.Resource{Name: topic, Service: "pubsub", Kind: "topic", Parent: projectName, Scope: provider.ProjectScope(testProject)}))
	must(t, h.engine.Register(publishMethod, iam.Rule{Baseline: []string{"pubsub.topics.publish"}}))
	published := make(chan *pubsubpb.PublishRequest, 4)
	must(t, h.registry.Register(publishMethod, func(call gateway.Call, raw any) (any, error) {
		if _, err := h.engine.Authorize(call, iam.Target{Name: "//pubsub.googleapis.com/" + call.Resource}, nil); err != nil {
			return nil, err //nolint:wrapcheck // the test reads the status
		}
		request, _ := raw.(*pubsubpb.PublishRequest)
		if call.Principal.Subject != agent {
			t.Errorf("published as %s", call.Principal.Subject)
		}
		published <- request
		return &pubsubpb.PublishResponse{MessageIds: []string{"1"}}, nil
	}))
	return published
}

func TestPubSubTargetPublishesAsServiceAgent(t *testing.T) {
	h := newHarness(t, nil)
	published := fakePublish(t, h)
	h.create("events", map[string]any{"schedule": "0 0 1 1 *", "pubsubTarget": map[string]any{
		"topicName": "projects/" + testProject + "/topics/events", "data": "Z2NwLWxvY2Fs", "attributes": map[string]any{"origin": "gcp-local"}}})
	h.call("actor", http.MethodPost, basePath+"/events:run", map[string]any{})
	select {
	case request := <-published:
		if string(request.GetMessages()[0].GetData()) != "gcp-local" || request.GetMessages()[0].GetAttributes()["origin"] != "gcp-local" {
			t.Fatalf("published %v", request)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the job did not publish")
	}
	eventually(t, "the attempt status", func() bool {
		status, _ := h.get("events")["status"].(map[string]any)
		return status != nil && len(status) == 0
	})
	// Without the service agent's role, publishing is denied and the job
	// reports PERMISSION_DENIED.
	_, err := h.engine.SetPolicy(projectName, iam.Policy{Bindings: []iam.Binding{
		{Role: "roles/cloudscheduler.admin", Members: []string{"serviceAccount:" + email("actor")}},
	}})
	must(t, err)
	h.call("actor", http.MethodPost, basePath+"/events:run", map[string]any{})
	eventually(t, "the denied attempt", func() bool {
		status, _ := h.get("events")["status"].(map[string]any)
		return status["code"] == float64(7)
	})
}

// workerSubject is the synthetic local subject credential of the job account.
const workerSubject = "gls.synthetic-scheduler-worker-subject"

func TestOAuthTokenNamesTheJobAccount(t *testing.T) {
	identities, err := identity.New(identity.Config{Principals: []identity.Enrollment{
		{Subject: "serviceAccount:" + email("worker"), Project: testProject, SubjectToken: workerSubject},
	}}, nil)
	must(t, err)
	h := newHarness(t, func(config *Config, _ *state.Index, _ *iam.Engine) { config.Identity = identities })
	tg := newTarget(t)
	h.create("oauth", map[string]any{"schedule": "0 0 1 1 *", "httpTarget": map[string]any{
		"uri": tg.server.URL, "oauthToken": map[string]any{"serviceAccountEmail": email("worker")}}})
	expect(t, "default scope", 200, h.get("oauth"), 200, map[string]any{"httpTarget.oauthToken.scope": identity.CloudPlatform})
	h.call("actor", http.MethodPost, basePath+"/oauth:run", map[string]any{})
	eventually(t, "the OAuth delivery", func() bool { return tg.count() == 1 })
	principal, err := identities.Resolve(context.Background(), strings.TrimPrefix(tg.requests[0].Header.Get("Authorization"), "Bearer "))
	if err != nil || principal.Subject != "serviceAccount:"+email("worker") {
		t.Fatalf("token principal = %v, %v", principal, err)
	}
}

func TestDeliveryGapsBecomeAttemptStatuses(t *testing.T) {
	h := newHarness(t, nil)
	tg := newTarget(t)
	for id, token := range map[string]string{"oidc": "oidcToken", "oauth": "oauthToken"} {
		h.create(id, map[string]any{"schedule": "0 0 1 1 *", "httpTarget": map[string]any{
			"uri": tg.server.URL, token: map[string]any{"serviceAccountEmail": email("worker")}}})
		h.call("actor", http.MethodPost, basePath+"/"+id+":run", map[string]any{})
	}
	for id, message := range map[string]string{
		"oidc":  "Cloud Scheduler coverage gap: OIDC tokens need the local ID-token issuer",
		"oauth": "Cloud Scheduler coverage gap: OAuth tokens need the local identity service",
	} {
		eventually(t, id+" gap status", func() bool {
			status, _ := h.get(id)["status"].(map[string]any)
			return status["code"] == float64(12) && status["message"] == message
		})
	}
	if tg.count() != 0 {
		t.Fatal("a job without its token was delivered")
	}
}

func TestRefusalsAndTheirMessages(t *testing.T) {
	h := newHarness(t, nil)
	h.create("existing", map[string]any{"schedule": "0 0 1 1 *", "pubsubTarget": map[string]any{"topicName": "projects/" + testProject + "/topics/t", "data": "eA=="}})
	topic := map[string]any{"topicName": "projects/" + testProject + "/topics/t", "data": "eA=="}
	jobName := func(id string) string {
		return "projects/" + testProject + "/locations/" + testLocation + "/jobs/" + id
	}
	cases := []struct {
		name, token, verb, path string
		body                    any
		code                    int
		message                 string
	}{
		{"anonymous", "", http.MethodGet, basePath + "/existing", nil, 401, "Request had invalid authentication credentials."},
		{"denied get", "stranger", http.MethodGet, basePath + "/existing", nil, 403,
			"The caller does not have cloudscheduler.jobs.get access to //cloudscheduler.googleapis.com/" + jobName("existing") + "."},
		{"denied create", "stranger", http.MethodPost, basePath, map[string]any{"name": jobName("x"), "schedule": "* * * * *", "pubsubTarget": topic}, 403,
			"The caller does not have cloudscheduler.jobs.create access to " + projectName + "."},
		{"viewer cannot run", "viewer", http.MethodPost, basePath + "/existing:run", map[string]any{}, 403,
			"The caller does not have cloudscheduler.jobs.run access to //cloudscheduler.googleapis.com/" + jobName("existing") + "."},
		{"viewer cannot resume", "viewer", http.MethodPost, basePath + "/existing:resume", map[string]any{}, 403,
			"The caller does not have cloudscheduler.jobs.enable access to //cloudscheduler.googleapis.com/" + jobName("existing") + "."},
		{"actAs", "actor", http.MethodPost, basePath, map[string]any{"name": jobName("a"), "schedule": "* * * * *",
			"httpTarget": map[string]any{"uri": "https://example.invalid", "oidcToken": map[string]any{"serviceAccountEmail": email("other")}}}, 403,
			"The caller lacks iam.serviceAccounts.actAs on " + email("other")},
		{"undeclared account", "actor", http.MethodPost, basePath, map[string]any{"name": jobName("u"), "schedule": "* * * * *",
			"httpTarget": map[string]any{"uri": "https://example.invalid", "oauthToken": map[string]any{"serviceAccountEmail": email("ghost")}}}, 501,
			"Cloud Scheduler coverage gap: token service accounts must be declared same-project local service accounts"},
		{"app engine", "actor", http.MethodPost, basePath, map[string]any{"name": jobName("ae"), "schedule": "* * * * *",
			"appEngineHttpTarget": map[string]any{"relativeUri": "/"}}, 501, "Cloud Scheduler coverage gap: App Engine HTTP targets are not implemented"},
		{"english schedule", "actor", http.MethodPost, basePath, map[string]any{"name": jobName("en"), "schedule": "every 5 minutes", "pubsubTarget": topic}, 501,
			"Cloud Scheduler coverage gap: English-like schedules are not implemented; use unix-cron"},
		{"bad cron", "actor", http.MethodPost, basePath, map[string]any{"name": jobName("c"), "schedule": "not a schedule", "pubsubTarget": topic}, 400,
			"Invalid schedule not a schedule: expected exactly 5 fields, found 3: [not a schedule]"},
		{"bad zone", "actor", http.MethodPost, basePath, map[string]any{"name": jobName("z"), "schedule": "* * * * *", "timeZone": "Mars/Olympus_Mons", "pubsubTarget": topic}, 400,
			"Invalid time zone Mars/Olympus_Mons"},
		{"no target", "actor", http.MethodPost, basePath, map[string]any{"name": jobName("n"), "schedule": "* * * * *"}, 400, "A job needs an httpTarget or a pubsubTarget"},
		{"other project topic", "actor", http.MethodPost, basePath, map[string]any{"name": jobName("o"), "schedule": "* * * * *",
			"pubsubTarget": map[string]any{"topicName": "projects/elsewhere/topics/t", "data": "eA=="}}, 400, "pubsubTarget.topicName must name a topic in the job's project"},
		{"short deadline", "actor", http.MethodPost, basePath, map[string]any{"name": jobName("d"), "schedule": "* * * * *", "attemptDeadline": "5s",
			"httpTarget": map[string]any{"uri": "https://example.invalid"}}, 400, "attemptDeadline must be between 15 seconds and 30 minutes for HTTP targets"},
		{"body on GET", "actor", http.MethodPost, basePath, map[string]any{"name": jobName("g"), "schedule": "* * * * *",
			"httpTarget": map[string]any{"uri": "https://example.invalid", "httpMethod": "GET", "body": "eA=="}}, 400, "A request body needs the POST, PUT, or PATCH method"},
		{"retry count", "actor", http.MethodPost, basePath, map[string]any{"name": jobName("r"), "schedule": "* * * * *", "pubsubTarget": topic,
			"retryConfig": map[string]any{"retryCount": 6}}, 400, "retryConfig.retryCount must be between 0 and 5"},
		{"duplicate", "actor", http.MethodPost, basePath, map[string]any{"name": jobName("existing"), "schedule": "* * * * *", "pubsubTarget": topic}, 409,
			"Job " + jobName("existing") + " already exists"},
		{"other location name", "actor", http.MethodPost, basePath, map[string]any{"name": "projects/" + testProject + "/locations/europe-west1/jobs/x", "schedule": "* * * * *", "pubsubTarget": topic}, 400,
			"Invalid job name projects/" + testProject + "/locations/europe-west1/jobs/x"},
		{"output mask", "actor", http.MethodPatch, basePath + "/existing?updateMask=state", map[string]any{}, 400, "Invalid update mask path state"},
		{"missing job", "actor", http.MethodPost, basePath + "/missing:pause", map[string]any{}, 404, "The resource 'jobs/missing' was not found"},
	}
	for _, tc := range cases {
		code, document := h.call(tc.token, tc.verb, tc.path, tc.body)
		if code != tc.code || errorMessage(document) != tc.message {
			t.Errorf("%s = %d %q, want %d %q", tc.name, code, errorMessage(document), tc.code, tc.message)
		}
	}
	if code, document := h.call("actor", http.MethodPost, basePath, map[string]any{"name": jobName("f"), "schedule": "* * * * *", "pubsubTarget": topic, "bogus": 1}); code != 400 ||
		!strings.HasPrefix(errorMessage(document), "Invalid job: ") {
		t.Errorf("unknown field = %d %v", code, document)
	}
}

func TestServiceAgentNeedsTheProjectNumber(t *testing.T) {
	h := newHarness(t, nil)
	const project = "demo-numberless"
	name := "//cloudresourcemanager.googleapis.com/projects/" + project
	must(t, h.index.Put("resourcemanager", state.Resource{Name: name, Service: "resourcemanager", Kind: "project", Scope: provider.ProjectScope(project)}))
	_, err := h.engine.SetPolicy(name, iam.Policy{Bindings: []iam.Binding{{Role: "roles/cloudscheduler.admin", Members: []string{"serviceAccount:" + email("actor")}}}})
	must(t, err)
	base := "/v1/projects/" + project + "/locations/" + testLocation + "/jobs"
	code, _ := h.call("actor", http.MethodPost, base, map[string]any{"name": "projects/" + project + "/locations/" + testLocation + "/jobs/n",
		"schedule": "0 0 1 1 *", "pubsubTarget": map[string]any{"topicName": "projects/" + project + "/topics/t", "data": "eA=="}})
	if code != 200 {
		t.Fatalf("create = %d", code)
	}
	h.call("actor", http.MethodPost, base+"/n:run", map[string]any{})
	eventually(t, "the gap status", func() bool {
		_, job := h.call("actor", http.MethodGet, base+"/n", nil)
		status, _ := job["status"].(map[string]any)
		return status["message"] == "Cloud Scheduler coverage gap: the Cloud Scheduler service agent needs the project's number in the resource index"
	})
}

func TestRetryAfterFollowsRetryConfig(t *testing.T) {
	cases := []struct {
		count   int32
		limit   time.Duration
		retries int
		elapsed time.Duration
		want    bool
	}{
		{0, 0, 0, 0, false},
		{2, 0, 1, time.Hour, true},
		{2, 0, 2, 0, false},
		{0, time.Minute, 3, 30 * time.Second, true},
		{0, time.Minute, 3, time.Minute, false},
		{1, time.Minute, 1, 10 * time.Second, true},
		{1, time.Minute, 0, 2 * time.Minute, true},
	}
	for _, tc := range cases {
		config := &schedulerpb.RetryConfig{RetryCount: tc.count, MaxRetryDuration: durationpb.New(tc.limit),
			MinBackoffDuration: durationpb.New(time.Second), MaxBackoffDuration: durationpb.New(time.Minute), MaxDoublings: 5}
		if _, got := retryAfter(config, tc.retries, tc.elapsed); got != tc.want {
			t.Errorf("retryAfter(count %d, limit %s, retries %d, elapsed %s) = %v", tc.count, tc.limit, tc.retries, tc.elapsed, got)
		}
	}
}
