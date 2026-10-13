package cloudtasks

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/providers/provider/providertest"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// Names and keys repeated across the queue and task tests.
const (
	testProject   = "demo-tasks"
	testLocation  = "us-central1"
	testQueue     = "workflow"
	testUnreached = "http://127.0.0.1:1/"
)

// harness drives the provider through the gateway.
type harness struct {
	t        *testing.T
	gate     *gateway.Gateway
	engine   *iam.Engine
	index    *state.Index
	tokens   *identity.IDTokens
	provider *Provider
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	registry := gateway.NewRegistry()
	index := state.NewIndex()
	engine, err := iam.New(index)
	if err != nil {
		t.Fatalf("build the iam engine: %v", err)
	}
	if err := index.Put("resourcemanager", state.Resource{
		Name: "//cloudresourcemanager.googleapis.com/projects/" + testProject, Service: "resourcemanager",
		Kind: "project", Number: "123456", Scope: state.Scope{Project: testProject, Location: state.Global},
	}); err != nil {
		t.Fatalf("register the project: %v", err)
	}
	if _, err := engine.SetPolicy("//cloudresourcemanager.googleapis.com/projects/"+testProject, iam.Policy{
		Bindings: []iam.Binding{{Role: "roles/cloudtasks.admin", Members: []string{"serviceAccount:actor@" + testProject + ".iam.gserviceaccount.com"}}},
	}); err != nil {
		t.Fatalf("grant the project: %v", err)
	}
	tokens, err := identity.NewIDTokens(engine, index)
	if err != nil {
		t.Fatalf("build the ID-token issuer: %v", err)
	}
	provider, err := New(context.Background(), DeliveryConfig{Tokens: tokens}, registry, backend.NewManager(time.Second, time.Second), index, engine)
	if err != nil {
		t.Fatalf("build the cloudtasks provider: %v", err)
	}
	if err := index.Put("iam", state.Resource{
		Name: iam.AccountResource(testProject, email("denied")), Service: "iam", Kind: "serviceAccount",
		Parent: "//cloudresourcemanager.googleapis.com/projects/" + testProject, Number: "1234567",
		Scope: state.Scope{Project: testProject, Location: state.Global},
	}); err != nil {
		t.Fatalf("register the service account: %v", err)
	}
	g := gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:" + token + "@" + testProject + ".iam.gserviceaccount.com", Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})
	return &harness{t: t, gate: g, engine: engine, index: index, tokens: tokens, provider: provider}
}

// email names one test principal's service account.
func email(token string) string { return token + "@" + testProject + ".iam.gserviceaccount.com" }

// invoke issues one REST request as the fixture principal.
func (h *harness) invoke(verb, path string, body any) *httptest.ResponseRecorder {
	h.t.Helper()
	return h.invokeAs("actor", verb, path, body)
}

// invokeAs issues one REST request as the named principal.
func (h *harness) invokeAs(token, verb, path string, body any) *httptest.ResponseRecorder {
	h.t.Helper()
	var encoded []byte
	if body != nil {
		encoded, _ = json.Marshal(body) //nolint:errchkjson // plain map bodies
	}
	request, _ := http.NewRequestWithContext(context.Background(), verb, path, bytes.NewReader(encoded))
	if encoded != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	recorder := httptest.NewRecorder()
	h.gate.ServeHTTP(recorder, request)
	return recorder
}

// basePrefix is the REST path base of the tests.
var basePrefix = "/v2beta3/projects/" + testProject + "/locations/" + testLocation

func TestQueueLifecycleAndPurge(t *testing.T) {
	h := newHarness(t)
	recorder := h.invoke(http.MethodPost, basePrefix+"/queues", map[string]any{
		"name": "projects/" + testProject + "/locations/" + testLocation + "/queues/" + testQueue,
	})
	if recorder.Code != 200 {
		t.Fatalf("queue create status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodGet, basePrefix+"/queues/"+testQueue, nil)
	if recorder.Code != 200 {
		t.Fatalf("queue get status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodPost, basePrefix+"/queues/"+testQueue+":pause", nil)
	if recorder.Code != 200 {
		t.Fatalf("pause status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodPost, basePrefix+"/queues/"+testQueue+":resume", nil)
	if recorder.Code != 200 {
		t.Fatalf("resume status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodDelete, basePrefix+"/queues/"+testQueue, nil)
	if recorder.Code != 200 {
		t.Fatalf("queue delete status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodGet, basePrefix+"/queues/"+testQueue, nil)
	if recorder.Code != 404 {
		t.Fatalf("deleted queue get status = %d", recorder.Code)
	}
}

func TestTaskLifecycleAndDispatch(t *testing.T) {
	h := newHarness(t)
	h.invoke(http.MethodPost, basePrefix+"/queues", map[string]any{"name": testQueue})
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	recorder := h.invoke(http.MethodPost, basePrefix+"/queues/"+testQueue+"/tasks", map[string]any{"task": map[string]any{
		"scheduleTime": time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
		"httpRequest":  map[string]any{"url": target.URL, "httpMethod": "POST"},
	}})
	if recorder.Code != 200 {
		t.Fatalf("task create status = %d body %s", recorder.Code, recorder.Body.String())
	}
	var task struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &task); err != nil {
		t.Fatalf("decode task: %v", err)
	}
	recorder = h.invoke(http.MethodGet, basePrefix+"/queues/"+testQueue+"/tasks", nil)
	if recorder.Code != 200 {
		t.Fatalf("task list status = %d", recorder.Code)
	}
	var listing struct {
		Tasks []map[string]any `json:"tasks"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &listing); err != nil {
		t.Fatalf("decode listing: %v", err)
	}
	if len(listing.Tasks) != 1 {
		t.Fatalf("listing = %v", listing)
	}
	recorder = h.invoke(http.MethodDelete, basePrefix+"/queues/"+testQueue+"/tasks/"+task.Name[strings.LastIndex(task.Name, "/")+1:], nil)
	if recorder.Code != 200 || recorder.Body.String() != "{}" {
		t.Fatalf("task delete status = %d body %s", recorder.Code, recorder.Body.String())
	}
}

func TestTaskOnMissingQueueIsRefused(t *testing.T) {
	h := newHarness(t)
	recorder := h.invoke(http.MethodPost, basePrefix+"/queues/missing/tasks", map[string]any{"task": map[string]any{
		"httpRequest": map[string]any{"url": testUnreached, "httpMethod": "POST"},
	}})
	if recorder.Code != 404 {
		t.Fatalf("task on missing queue status = %d body %s", recorder.Code, recorder.Body.String())
	}
}

func TestQueueIAMPolicyGrantsAndReads(t *testing.T) {
	h := newHarness(t)
	h.invoke(http.MethodPost, basePrefix+"/queues", map[string]any{"name": testQueue})
	recorder := h.invoke(http.MethodGet, basePrefix+"/queues/"+testQueue+":getIamPolicy", nil)
	if recorder.Code != 200 {
		t.Fatalf("getIamPolicy status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodPost, basePrefix+"/queues/"+testQueue+":setIamPolicy", map[string]any{
		"policy": map[string]any{"bindings": []any{map[string]any{
			"role":    "roles/cloudtasks.enqueuer",
			"members": []any{"serviceAccount:enqueuer@" + testProject + ".iam.gserviceaccount.com"},
		}}},
	})
	if recorder.Code != 200 {
		t.Fatalf("setIamPolicy status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodGet, basePrefix+"/queues/"+testQueue+":getIamPolicy", nil)
	var policy struct {
		Bindings []map[string]any `json:"bindings"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &policy); err != nil {
		t.Fatalf("decode policy: %v", err)
	}
	if len(policy.Bindings) != 1 {
		t.Fatalf("policy = %v", policy)
	}
}

func TestScheduledTaskWaitsThenDispatches(t *testing.T) {
	h := newHarness(t)
	h.invoke(http.MethodPost, basePrefix+"/queues", map[string]any{"name": testQueue})
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	schedule := time.Now().Add(2 * time.Second).UTC().Format(time.RFC3339)
	recorder := h.invoke(http.MethodPost, basePrefix+"/queues/"+testQueue+"/tasks", map[string]any{"task": map[string]any{
		"scheduleTime": schedule,
		"httpRequest":  map[string]any{"url": target.URL, "httpMethod": "POST"},
	}})
	if recorder.Code != 200 {
		t.Fatalf("scheduled task create status = %d body %s", recorder.Code, recorder.Body.String())
	}
	var task struct {
		ScheduleTime string `json:"scheduleTime"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &task); err != nil {
		t.Fatalf("decode task: %v", err)
	}
	if task.ScheduleTime == "" {
		t.Fatalf("scheduleTime missing in %s", recorder.Body.String())
	}
	recorder = h.invoke(http.MethodGet, basePrefix+"/queues/"+testQueue+"/tasks", nil)
	var listing struct {
		Tasks []map[string]any `json:"tasks"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &listing); err != nil {
		t.Fatalf("decode listing: %v", err)
	}
	if len(listing.Tasks) != 1 {
		t.Fatalf("listing = %v", listing)
	}
}

func TestInvalidScheduleTimeIsRefused(t *testing.T) {
	h := newHarness(t)
	h.invoke(http.MethodPost, basePrefix+"/queues", map[string]any{"name": testQueue})
	recorder := h.invoke(http.MethodPost, basePrefix+"/queues/"+testQueue+"/tasks", map[string]any{"task": map[string]any{
		"scheduleTime": "not-a-time",
		"httpRequest":  map[string]any{"url": testUnreached, "httpMethod": "POST"},
	}})
	if recorder.Code != 400 {
		t.Fatalf("invalid schedule status = %d body %s", recorder.Code, recorder.Body.String())
	}
}

func TestQueueUpdateReplacesRateLimits(t *testing.T) {
	h := newHarness(t)
	h.invoke(http.MethodPost, basePrefix+"/queues", map[string]any{"name": testQueue})
	recorder := h.invoke(http.MethodPatch, basePrefix+"/queues/"+testQueue, map[string]any{
		"rateLimits": map[string]any{"maxDispatchesPerSecond": 5},
	})
	if recorder.Code != 200 {
		t.Fatalf("queue update status = %d body %s", recorder.Code, recorder.Body.String())
	}
	var queue struct {
		RateLimits map[string]any `json:"rateLimits"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &queue); err != nil {
		t.Fatalf("decode queue: %v", err)
	}
	if queue.RateLimits["maxDispatchesPerSecond"] == nil {
		t.Fatalf("rate limits = %v", queue.RateLimits)
	}
	// GCP creates a missing queue on patch; that is a gap, not a 404.
	recorder = h.invoke(http.MethodPatch, basePrefix+"/queues/missing", map[string]any{})
	if recorder.Code != 501 || !strings.Contains(recorder.Body.String(), "creating a queue through queues.patch is not implemented") {
		t.Fatalf("missing queue update = %d %s", recorder.Code, recorder.Body.String())
	}
}

func TestEveryRouteIsHandled(t *testing.T) {
	h := newHarness(t)
	h.invoke(http.MethodPost, basePrefix+"/queues", map[string]any{"name": testQueue})
	gap := "this method is not implemented"
	probe := func(verb, path string) {
		recorder := h.invoke(verb, path, nil)
		if body := recorder.Body.String(); recorder.Code == 501 && strings.Contains(body, gap) {
			t.Fatalf("%s %s hit the dispatch gap", verb, path)
		}
	}
	probe(http.MethodGet, basePrefix+"/queues/"+testQueue)
	probe(http.MethodGet, basePrefix+"/queues")
	probe(http.MethodGet, basePrefix+"/queues/"+testQueue+":getIamPolicy")
	probe(http.MethodPost, basePrefix+"/queues/"+testQueue+":pause")
	probe(http.MethodPost, basePrefix+"/queues/"+testQueue+":resume")
	probe(http.MethodGet, basePrefix+"/queues/"+testQueue+"/tasks")
	probe(http.MethodDelete, basePrefix+"/queues/"+testQueue)
	probe(http.MethodGet, "/v2beta3/projects/"+testProject+"/locations")
}

// queuePath is the REST path of the test queue.
var queuePath = basePrefix + "/queues/" + testQueue

// decode reads one JSON response body.
func decode(t *testing.T, recorder *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var value map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &value); err != nil {
		t.Fatalf("decode %s: %v", recorder.Body.String(), err)
	}
	return value
}

// fieldNames lists a document's sorted top-level field names.
func fieldNames(value map[string]any) []string {
	names := make([]string, 0, len(value))
	for name := range value {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// expectError checks the standard error shape and status.
func expectError(t *testing.T, recorder *httptest.ResponseRecorder, code int, statusText string) {
	t.Helper()
	body, _ := decode(t, recorder)["error"].(map[string]any)
	if recorder.Code != code || body["code"] != float64(code) || body["status"] != statusText || body["message"] == nil {
		t.Fatalf("error = %d %s, want %d %s", recorder.Code, recorder.Body.String(), code, statusText)
	}
}

// createRetryQueue creates the test queue with the recorded retryConfig.
func (h *harness) createRetryQueue() map[string]any {
	h.t.Helper()
	recorder := h.invoke(http.MethodPost, basePrefix+"/queues", map[string]any{
		"name":        "projects/" + testProject + "/locations/" + testLocation + "/queues/" + testQueue,
		"retryConfig": map[string]any{"maxAttempts": 2, "minBackoff": "1s", "maxBackoff": "1s"},
	})
	if recorder.Code != 200 {
		h.t.Fatalf("queue create status = %d body %s", recorder.Code, recorder.Body.String())
	}
	return decode(h.t, recorder)
}

// taskBody builds one task creation body for an HTTP target.
func taskBody(target string, oidc string) map[string]any {
	request := map[string]any{"url": target, "httpMethod": "POST", "body": "Z2NwLWxvY2Fs"}
	if oidc != "" {
		request["oidcToken"] = map[string]any{"serviceAccountEmail": oidc}
	}
	return map[string]any{"task": map[string]any{"httpRequest": request}}
}

func TestQueueDocumentMatchesRecordedShape(t *testing.T) {
	h := newHarness(t)
	queue := h.createRetryQueue()
	fields := []string{"name", "rateLimits", "retryConfig", "state", "taskTtl", "tombstoneTtl", "type"}
	if !reflect.DeepEqual(fieldNames(queue), fields) {
		t.Fatalf("queue fields = %v", fieldNames(queue))
	}
	if queue["name"] != "projects/"+testProject+"/locations/"+testLocation+"/queues/"+testQueue || queue["state"] != "RUNNING" {
		t.Fatalf("queue = %v", queue)
	}
	retry := map[string]any{"maxAttempts": float64(2), "maxBackoff": "1s", "maxDoublings": float64(16), "minBackoff": "1s"}
	if !reflect.DeepEqual(queue["retryConfig"], retry) {
		t.Fatalf("retryConfig = %v", queue["retryConfig"])
	}
	for verb, want := range map[string]string{":pause": "PAUSED", ":resume": "RUNNING"} {
		paused := decode(t, h.invoke(http.MethodPost, queuePath+verb, map[string]any{}))
		if paused["state"] != want || !reflect.DeepEqual(fieldNames(paused), fields) {
			t.Fatalf("%s queue = %v", verb, paused)
		}
	}
	recorder := h.invoke(http.MethodDelete, queuePath, nil)
	if recorder.Code != 200 || recorder.Body.String() != "{}" {
		t.Fatalf("queue delete = %d %s", recorder.Code, recorder.Body.String())
	}
	expectError(t, h.invoke(http.MethodGet, queuePath, nil), 404, "NOT_FOUND")
}

func TestQueueRetryDefaults(t *testing.T) {
	h := newHarness(t)
	queue := decode(t, h.invoke(http.MethodPost, basePrefix+"/queues", map[string]any{"name": testQueue}))
	retry := map[string]any{"maxAttempts": float64(100), "maxBackoff": "3600s", "maxDoublings": float64(16), "minBackoff": "0.100s"}
	if !reflect.DeepEqual(queue["retryConfig"], retry) {
		t.Fatalf("default retryConfig = %v", queue["retryConfig"])
	}
	policy := retryPolicy{MinBackoff: time.Second, MaxBackoff: 10 * time.Second, MaxDoublings: 2}
	for failures, want := range map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second, 4: 8 * time.Second, 5: 10 * time.Second} {
		if got := policy.backoff(failures); got != want {
			t.Fatalf("backoff(%d) = %v, want %v", failures, got, want)
		}
	}
}

func TestQueueEnqueuerAuthorizesTaskCreate(t *testing.T) {
	h := newHarness(t)
	h.createRetryQueue()
	policy := decode(t, h.invoke(http.MethodPost, queuePath+":getIamPolicy", map[string]any{}))
	if !reflect.DeepEqual(fieldNames(policy), []string{"etag"}) {
		t.Fatalf("empty policy = %v", policy)
	}
	expectError(t, h.invokeAs("enqueuer", http.MethodPost, queuePath+"/tasks", taskBody("https://10.255.255.1/gcp-local", "")), 403, "PERMISSION_DENIED")
	granted := decode(t, h.invoke(http.MethodPost, queuePath+":setIamPolicy", map[string]any{"policy": map[string]any{"bindings": []any{
		map[string]any{"role": "roles/cloudtasks.enqueuer", "members": []any{"serviceAccount:" + email("enqueuer")}},
	}}}))
	if !reflect.DeepEqual(fieldNames(granted), []string{"bindings", "etag", "version"}) {
		t.Fatalf("granted policy = %v", granted)
	}
	_ = h.invoke(http.MethodPost, queuePath+":pause", map[string]any{}) // Hold the task so its view stays stable.
	recorder := h.invokeAs("enqueuer", http.MethodPost, queuePath+"/tasks", taskBody("https://10.255.255.1/gcp-local", ""))
	if recorder.Code != 200 {
		t.Fatalf("enqueuer create = %d %s", recorder.Code, recorder.Body.String())
	}
	task := decode(t, recorder)
	if !reflect.DeepEqual(fieldNames(task), []string{"createTime", "dispatchDeadline", "httpRequest", "name", "scheduleTime", "view"}) {
		t.Fatalf("task fields = %v", fieldNames(task))
	}
	echo := map[string]any{"headers": map[string]any{"User-Agent": "Google-Cloud-Tasks"}, "httpMethod": "POST", "url": "https://10.255.255.1/gcp-local"}
	name, _ := task["name"].(string)
	if task["view"] != "BASIC" || !reflect.DeepEqual(task["httpRequest"], echo) || !strings.HasPrefix(name, "projects/"+testProject+"/locations/"+testLocation+"/queues/"+testQueue+"/tasks/") {
		t.Fatalf("task = %v", task)
	}
	expectError(t, h.invokeAs("enqueuer", http.MethodGet, queuePath+"/tasks", nil), 403, "PERMISSION_DENIED")
}

func TestOIDCTaskRequiresActAs(t *testing.T) {
	h := newHarness(t)
	h.createRetryQueue()
	expectError(t, h.invoke(http.MethodPost, queuePath+"/tasks", taskBody(testUnreached, email("denied"))), 403, "PERMISSION_DENIED")
	if tasks := decode(t, h.invoke(http.MethodGet, queuePath+"/tasks", nil)); len(tasks) != 0 {
		t.Fatalf("refused task was stored: %v", tasks)
	}
	if _, err := h.engine.SetPolicy(iam.AccountResource(testProject, email("denied")), iam.Policy{Bindings: []iam.Binding{
		{Role: "roles/iam.serviceAccountUser", Members: []string{"serviceAccount:" + email("actor")}},
	}}); err != nil {
		t.Fatalf("grant actAs: %v", err)
	}
	_ = h.invoke(http.MethodPost, queuePath+":pause", map[string]any{})
	if recorder := h.invoke(http.MethodPost, queuePath+"/tasks", taskBody(testUnreached, email("denied"))); recorder.Code != 200 {
		t.Fatalf("task with actAs = %d %s", recorder.Code, recorder.Body.String())
	}
}

func TestNonHTTPTaskURLIsRefused(t *testing.T) {
	h := newHarness(t)
	h.createRetryQueue()
	expectError(t, h.invoke(http.MethodPost, queuePath+"/tasks", taskBody("ftp://10.255.255.1/x", "")), 400, "INVALID_ARGUMENT")
}

// pollTask reads one task until the condition holds or the deadline passes.
func (h *harness) pollTask(path string, done func(*httptest.ResponseRecorder) bool) *httptest.ResponseRecorder {
	h.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if recorder := h.invoke(http.MethodGet, path+"?responseView=BASIC", nil); done(recorder) {
			return recorder
		}
		time.Sleep(50 * time.Millisecond)
	}
	h.t.Fatalf("task %s did not reach the expected state", path)
	return nil
}

func TestRetryExhaustionRemovesTask(t *testing.T) {
	h := newHarness(t)
	h.provider.retention = 500 * time.Millisecond
	h.createRetryQueue()
	created := decode(t, h.invoke(http.MethodPost, queuePath+"/tasks", taskBody(testUnreached, "")))
	name, _ := created["name"].(string)
	path := queuePath + "/tasks/" + name[strings.LastIndex(name, "/")+1:]
	first := decode(t, h.pollTask(path, func(recorder *httptest.ResponseRecorder) bool {
		last, _ := decode(t, recorder)["lastAttempt"].(map[string]any)
		return last["responseStatus"] != nil
	}))
	fields := []string{"createTime", "dispatchCount", "dispatchDeadline", "firstAttempt", "httpRequest", "lastAttempt", "name", "scheduleTime", "view"}
	last, _ := first["lastAttempt"].(map[string]any)
	response, _ := last["responseStatus"].(map[string]any)
	if !reflect.DeepEqual(fieldNames(first), fields) || !reflect.DeepEqual(fieldNames(last), []string{"dispatchTime", "responseStatus", "responseTime", "scheduleTime"}) || response["code"] != float64(2) {
		t.Fatalf("first attempt = %v", first)
	}
	started := time.Now()
	h.pollTask(path, func(recorder *httptest.ResponseRecorder) bool { return recorder.Code == 404 })
	if elapsed := time.Since(started); elapsed < 500*time.Millisecond {
		t.Fatalf("task removed after %v; the second attempt waits minBackoff", elapsed)
	}
}

func TestSuccessfulDispatchRemovesTask(t *testing.T) {
	h := newHarness(t)
	h.createRetryQueue()
	agents := make(chan string, 4)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		agents <- r.Header.Get("User-Agent")
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	created := decode(t, h.invoke(http.MethodPost, queuePath+"/tasks", taskBody(target.URL, "")))
	name, _ := created["name"].(string)
	h.pollTask(queuePath+"/tasks/"+name[strings.LastIndex(name, "/")+1:], func(recorder *httptest.ResponseRecorder) bool { return recorder.Code == 404 })
	if agent := <-agents; agent != "Google-Cloud-Tasks" {
		t.Fatalf("dispatch User-Agent = %q", agent)
	}
}

// TestQueuePatchIsAtomic rejects a patch with one bad retry field without
// applying the valid rate limit sent beside it.
func TestQueuePatchIsAtomic(t *testing.T) {
	h := newHarness(t)
	queue := basePrefix + "/queues/" + testQueue
	if recorder := h.invoke(http.MethodPost, basePrefix+"/queues", map[string]any{"name": testQueue}); recorder.Code != 200 {
		t.Fatalf("create status = %d", recorder.Code)
	}
	before := h.invoke(http.MethodGet, queue, nil).Body.String()
	recorder := h.invoke(http.MethodPatch, queue, map[string]any{
		"rateLimits":  map[string]any{"maxDispatchesPerSecond": 7},
		"retryConfig": map[string]any{"maxAttempts": -5},
	})
	if recorder.Code != 400 {
		t.Fatalf("bad patch status = %d", recorder.Code)
	}
	if after := h.invoke(http.MethodGet, queue, nil).Body.String(); after != before {
		t.Fatalf("a rejected patch changed the queue:\nbefore %s\nafter  %s", before, after)
	}
}

// TestConcurrentQueueAndTaskRequests races queue and task methods on one
// queue while dispatch workers deliver and retry its tasks; -race fails it if
// a handler or worker touches a record unlocked.
func TestConcurrentQueueAndTaskRequests(t *testing.T) {
	h := newHarness(t)
	var hits atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1)%2 == 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	queues := basePrefix + "/queues"
	queue := queues + "/" + testQueue
	retry := map[string]any{"minBackoff": "0.001s", "maxBackoff": "0.002s", "maxAttempts": 3}
	providertest.Hammer(t, h.gate, "actor", 8, 15, func(worker int) []providertest.Request {
		task := queue + "/tasks/t" + strconv.Itoa(worker%2)
		return []providertest.Request{
			{Method: http.MethodPost, Path: queues, Body: map[string]any{"name": testQueue, "retryConfig": retry}, Want: []int{200, 409}},
			{Method: http.MethodPost, Path: queue + "/tasks", Body: map[string]any{"task": map[string]any{
				"name": strings.TrimPrefix(task, "/v2beta3/"), "httpRequest": map[string]any{
					"url": target.URL, "headers": map[string]any{"X-Worker": "w"}, "body": "aGk=",
				},
			}}, Want: []int{200, 404, 409}},
			{Method: http.MethodGet, Path: task + "?responseView=FULL", Want: []int{200, 404}},
			{Method: http.MethodGet, Path: queue + "/tasks", Want: []int{200, 404}},
			{Method: http.MethodPost, Path: task + ":run", Want: []int{200, 404}},
			{Method: http.MethodGet, Path: queue, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: queues, Want: []int{200}},
			{Method: http.MethodPatch, Path: queue, Body: map[string]any{"rateLimits": map[string]any{"maxDispatchesPerSecond": worker + 1}, "retryConfig": retry}, Want: []int{200, 501}},
			{Method: http.MethodPost, Path: queue + ":pause", Want: []int{200, 404}},
			{Method: http.MethodPost, Path: queue + ":resume", Want: []int{200, 404}},
			{Method: http.MethodDelete, Path: task, Want: []int{200, 404}},
			{Method: http.MethodPost, Path: queue + ":purge", Want: []int{200, 404}},
			{Method: http.MethodPost, Path: queue + ":setIamPolicy", Body: map[string]any{"policy": map[string]any{"bindings": []any{map[string]any{
				"role": "roles/cloudtasks.enqueuer", "members": []any{"serviceAccount:" + email("enqueuer")},
			}}}}, Want: []int{200, 404}},
			{Method: http.MethodPost, Path: queue + ":getIamPolicy", Body: map[string]any{}, Want: []int{200, 404}},
			{Method: http.MethodDelete, Path: queue, Want: []int{200, 404}},
		}
	})
	// The index entry and the queue record change together.
	_, indexed := h.provider.index.Get(queueResource(testProject, testLocation, testQueue))
	if stored := h.invoke(http.MethodGet, queue, nil).Code == 200; stored != indexed {
		t.Fatalf("queue stored = %v but indexed = %v", stored, indexed)
	}
}

// Lists page in name order with GCP's recorded refusals.
func TestListsPage(t *testing.T) {
	h := newHarness(t)
	h.createRetryQueue()
	_ = h.invoke(http.MethodPost, queuePath+":pause", map[string]any{})
	for _, id := range []string{"t-b", "t-a"} {
		if recorder := h.invoke(http.MethodPost, queuePath+"/tasks", namedTask(testUnreached, id, "")); recorder.Code != 200 {
			t.Fatalf("create %s = %d %s", id, recorder.Code, recorder.Body.String())
		}
	}
	first := decode(t, h.invoke(http.MethodGet, queuePath+"/tasks?pageSize=1", nil))
	token, _ := first["nextPageToken"].(string)
	second := decode(t, h.invoke(http.MethodGet, queuePath+"/tasks?pageSize=1&pageToken="+token, nil))
	names := func(page map[string]any) []string {
		items, _ := page["tasks"].([]any)
		out := make([]string, 0, len(items))
		for _, item := range items {
			name, _ := object(item)["name"].(string)
			out = append(out, name[strings.LastIndex(name, "/")+1:])
		}
		return out
	}
	if !reflect.DeepEqual(names(first), []string{"t-a"}) || !reflect.DeepEqual(names(second), []string{"t-b"}) || second["nextPageToken"] == nil {
		t.Fatalf("pages = %v %v", first, second)
	}
	for path, message := range map[string]string{
		queuePath + "/tasks?pageToken=bogus":    "Invalid page_token",
		queuePath + "/tasks?pageSize=-1":        "ListTasksRequest.pageSize cannot be negative.",
		basePrefix + "/queues?pageToken=bogus":  "Invalid page_token",
		basePrefix + "/queues?pageSize=-1":      "ListQueuesRequest.pageSize cannot be negative.",
		queuePath + "/tasks?responseView=BOGUS": `Invalid value at 'response_view' (type.googleapis.com/google.cloud.tasks.v2beta3.Task.View), "BOGUS"`,
	} {
		recorder := h.invoke(http.MethodGet, path, nil)
		if recorder.Code != 400 || !strings.Contains(recorder.Body.String(), strconv.Quote(message)[1:len(strconv.Quote(message))-1]) {
			t.Errorf("%s = %d %s", path, recorder.Code, recorder.Body.String())
		}
	}
}

// Request fields the provider would otherwise accept and ignore are explicit
// gaps naming the field.
func TestUnmodeledFieldsAreGaps(t *testing.T) {
	h := newHarness(t)
	queues := basePrefix + "/queues"
	cases := []struct {
		verb, path string
		body       map[string]any
		want       string
	}{
		{http.MethodPost, queues, map[string]any{"name": "q1", "appEngineHttpQueue": map[string]any{"appEngineRoutingOverride": map[string]any{"service": "s"}}}, "appEngineHttpQueue is not modeled"},
		{http.MethodPost, queues, map[string]any{"name": "q1", "httpTarget": map[string]any{"uriOverride": map[string]any{"host": "h"}}}, "httpTarget is not modeled"},
		{http.MethodPost, queues, map[string]any{"name": "q1", "stackdriverLoggingConfig": map[string]any{"samplingRatio": 1}}, "stackdriverLoggingConfig is not modeled"},
		{http.MethodPost, queues, map[string]any{"name": "q1", "type": "PULL"}, "pull queues are not implemented"},
		{http.MethodPost, queues, map[string]any{"name": "q1", "taskTtl": "86400s"}, "taskTtl other than the default 2678400s is not implemented"},
		{http.MethodGet, queues + "?readMask=stats", nil, "the readMask option is not implemented"},
		{http.MethodGet, queues + "?filter=state%3APAUSED", nil, "list filter is not implemented"},
	}
	for _, c := range cases {
		recorder := h.invoke(c.verb, c.path, c.body)
		if recorder.Code != 501 || !strings.Contains(recorder.Body.String(), "Cloud Tasks coverage gap: "+c.want) {
			t.Errorf("%s %s %v = %d %s", c.verb, c.path, c.body, recorder.Code, recorder.Body.String())
		}
	}
	h.createRetryQueue()
	oauth := map[string]any{"task": map[string]any{"httpRequest": map[string]any{"url": testUnreached,
		"oauthToken": map[string]any{"serviceAccountEmail": email("denied")}}}}
	for body, want := range map[string]map[string]any{
		"task.httpRequest.oauthToken is not modeled": oauth,
		"task.pullMessage is not modeled":            {"task": map[string]any{"pullMessage": map[string]any{"payload": "eA=="}}},
	} {
		recorder := h.invoke(http.MethodPost, queuePath+"/tasks", want)
		if recorder.Code != 501 || !strings.Contains(recorder.Body.String(), "Cloud Tasks coverage gap: "+body) {
			t.Errorf("%s = %d %s", body, recorder.Code, recorder.Body.String())
		}
	}
	if recorder := h.invoke(http.MethodPost, queuePath+"/tasks", map[string]any{"task": map[string]any{"httpRequest": map[string]any{"url": testUnreached}}, "bogus": 1}); recorder.Code != 400 ||
		!strings.Contains(recorder.Body.String(), `Unknown name \"bogus\": Cannot find field.`) {
		t.Errorf("unknown task request field = %d %s", recorder.Code, recorder.Body.String())
	}
}

// A task's name is tombstoned for its queue's tombstoneTtl after the task is
// deleted (recording cloudtasks-dispatch-retry).
func TestDeletedTaskNamesAreTombstoned(t *testing.T) {
	h := newHarness(t)
	now := time.Now()
	h.provider.clock = func() time.Time { return now }
	h.createRetryQueue()
	_ = h.invoke(http.MethodPost, queuePath+":pause", map[string]any{})
	body := namedTask(testUnreached, "named", "")
	if recorder := h.invoke(http.MethodPost, queuePath+"/tasks", body); recorder.Code != 200 {
		t.Fatalf("create = %d %s", recorder.Code, recorder.Body.String())
	}
	if recorder := h.invoke(http.MethodDelete, queuePath+"/tasks/named", nil); recorder.Code != 200 {
		t.Fatalf("delete = %d %s", recorder.Code, recorder.Body.String())
	}
	recorder := h.invoke(http.MethodPost, queuePath+"/tasks", body)
	if recorder.Code != 409 || !strings.Contains(recorder.Body.String(), "a task with this name existed too recently") {
		t.Fatalf("recreate = %d %s", recorder.Code, recorder.Body.String())
	}
	now = now.Add(defaultTombstoneTTL + time.Second)
	if recorder := h.invoke(http.MethodPost, queuePath+"/tasks", body); recorder.Code != 200 {
		t.Fatalf("recreate after the tombstone = %d %s", recorder.Code, recorder.Body.String())
	}
}

// dispatchDeadline is echoed, bounded, and ends a slow attempt with
// DEADLINE_EXCEEDED.
func TestDispatchDeadline(t *testing.T) {
	h := newHarness(t)
	h.createRetryQueue()
	_ = h.invoke(http.MethodPost, queuePath+":pause", map[string]any{})
	slow := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(3 * time.Second):
		}
	}))
	defer slow.Close()
	created := decode(t, h.invoke(http.MethodPost, queuePath+"/tasks", namedTask(slow.URL, "slow", "20s")))
	if created["dispatchDeadline"] != "20s" {
		t.Fatalf("created = %v", created)
	}
	if recorder := h.invoke(http.MethodPost, queuePath+"/tasks", namedTask(slow.URL, "short", "5s")); recorder.Code != 400 || !strings.Contains(recorder.Body.String(), "Task.dispatchDeadline must be between [15s, 30m].") {
		t.Fatalf("short deadline = %d %s", recorder.Code, recorder.Body.String())
	}
	// Shorten the stored deadline so the test need not wait 15 seconds.
	if err := h.provider.scopes.With(state.Scope{Project: testProject, Location: state.Global}, func(s *scopeState) error {
		s.tasks[testLocation+"/"+testQueue+"/slow"].Deadline = 100 * time.Millisecond
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	_ = h.invoke(http.MethodPost, queuePath+":resume", map[string]any{})
	attempted := decode(t, h.pollTask(queuePath+"/tasks/slow", func(recorder *httptest.ResponseRecorder) bool {
		last, _ := decode(t, recorder)["lastAttempt"].(map[string]any)
		return last["responseStatus"] != nil
	}))
	if code := object(object(attempted["lastAttempt"])["responseStatus"])["code"]; code != float64(4) {
		t.Fatalf("slow attempt = %v", attempted)
	}
}

// maxConcurrentDispatches holds attempts beyond the limit, and the dispatch
// rate spaces them.
func TestRateLimitsHoldDispatches(t *testing.T) {
	h := newHarness(t)
	var inflight, peak atomic.Int64
	var mu sync.Mutex
	var starts []time.Time
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		starts = append(starts, time.Now())
		mu.Unlock()
		raise(&peak, inflight.Add(1))
		time.Sleep(50 * time.Millisecond)
		inflight.Add(-1)
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	recorder := h.invoke(http.MethodPost, basePrefix+"/queues", map[string]any{"name": testQueue,
		"rateLimits": map[string]any{"maxConcurrentDispatches": 1, "maxDispatchesPerSecond": 10, "maxBurstSize": 1}})
	if recorder.Code != 200 {
		t.Fatalf("queue = %d %s", recorder.Code, recorder.Body.String())
	}
	for range 4 {
		if recorder := h.invoke(http.MethodPost, queuePath+"/tasks", taskBody(target.URL, "")); recorder.Code != 200 {
			t.Fatalf("task = %d %s", recorder.Code, recorder.Body.String())
		}
	}
	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(starts) == 4
	})
	mu.Lock()
	defer mu.Unlock()
	sort.Slice(starts, func(i, j int) bool { return starts[i].Before(starts[j]) })
	if peak.Load() != 1 || starts[3].Sub(starts[0]) < 250*time.Millisecond {
		t.Fatalf("peak concurrency %d, spread %v", peak.Load(), starts[3].Sub(starts[0]))
	}
}

// maxRetryDuration and maxAttempts must both be spent before a task stops.
func TestRetryNeedsBothLimitsSpent(t *testing.T) {
	policy := retryPolicy{MaxAttempts: 2, MaxDuration: time.Minute}
	for _, c := range []struct {
		dispatches int
		since      time.Duration
		want       bool
	}{{2, time.Second, false}, {1, 2 * time.Minute, false}, {2, 2 * time.Minute, true}} {
		if got := policy.exhausted(c.dispatches, c.since); got != c.want {
			t.Errorf("exhausted(%d, %v) = %v", c.dispatches, c.since, got)
		}
	}
	if (retryPolicy{MaxAttempts: -1}).exhausted(1000, time.Hour) || !(retryPolicy{MaxAttempts: 2}).exhausted(2, 0) {
		t.Error("unlimited attempts stopped, or attempts without a duration limit did not")
	}
}

// An update mask changes only the fields it names.
func TestQueueUpdateMask(t *testing.T) {
	h := newHarness(t)
	h.createRetryQueue()
	recorder := h.invoke(http.MethodPatch, queuePath+"?updateMask=retryConfig.maxAttempts,retryConfig.maxRetryDuration",
		map[string]any{"retryConfig": map[string]any{"maxAttempts": 3, "maxDoublings": 2, "maxRetryDuration": "30s"}})
	retry, _ := decode(t, recorder)["retryConfig"].(map[string]any)
	want := map[string]any{"maxAttempts": float64(3), "maxRetryDuration": "30s", "minBackoff": "1s", "maxBackoff": "1s", "maxDoublings": float64(16)}
	if !reflect.DeepEqual(retry, want) {
		t.Fatalf("masked update = %d %v", recorder.Code, retry)
	}
	expectError(t, h.invoke(http.MethodPatch, queuePath+"?updateMask=state", map[string]any{}), 400, "INVALID_ARGUMENT")
}

// Queues refuse conditional bindings, as GCP does (recording
// cloudtasks-dispatch-retry).
func TestQueuePoliciesRefuseConditions(t *testing.T) {
	h := newHarness(t)
	h.createRetryQueue()
	recorder := h.invoke(http.MethodPost, queuePath+":setIamPolicy", map[string]any{"policy": map[string]any{"version": 3, "bindings": []any{map[string]any{
		"role": "roles/cloudtasks.viewer", "members": []any{"serviceAccount:" + email("viewer")},
		"condition": map[string]any{"title": "t", "expression": `resource.type == "cloudtasks.googleapis.com/Queue"`},
	}}}})
	if recorder.Code != 400 || !strings.Contains(recorder.Body.String(), "Can't set conditional policy on this resource") {
		t.Fatalf("conditional policy = %d %s", recorder.Code, recorder.Body.String())
	}
}

// namedTask builds a task body with a name in the test queue and an
// optional dispatchDeadline.
func namedTask(target, id, deadline string) map[string]any {
	task := map[string]any{"name": strings.TrimPrefix(queuePath, "/v2beta3/") + "/tasks/" + id,
		"httpRequest": map[string]any{"url": target, "httpMethod": "POST"}}
	if deadline != "" {
		task["dispatchDeadline"] = deadline
	}
	return map[string]any{"task": task}
}

// object reads a decoded JSON object, or an empty one.
func object(value any) map[string]any {
	entry, _ := value.(map[string]any)
	return entry
}

// raise lifts peak to current if current is higher.
func raise(peak *atomic.Int64, current int64) {
	for previous := peak.Load(); current > previous; previous = peak.Load() {
		if peak.CompareAndSwap(previous, current) {
			return
		}
	}
}

// waitUntil polls a condition for up to ten seconds.
func waitUntil(t *testing.T, condition func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); !condition(); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the condition did not hold in time")
		}
	}
}

// A worker finishing an attempt must not touch a task created again under
// its name (here after the queue is deleted and recreated); that task has
// its own worker. The old worker once dereferenced the new task's missing
// first attempt.
func TestStaleWorkerLeavesARecreatedTaskAlone(t *testing.T) {
	h := newHarness(t)
	entered, release := make(chan struct{}, 4), make(chan struct{})
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		entered <- struct{}{}
		<-release
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer target.Close()
	defer close(release)
	h.createRetryQueue()
	if recorder := h.invoke(http.MethodPost, queuePath+"/tasks", namedTask(target.URL, "same", "")); recorder.Code != 200 {
		t.Fatalf("create = %d %s", recorder.Code, recorder.Body.String())
	}
	<-entered
	// Remove the queue without stopping its worker, as a delete does in the
	// moment before it cancels the worker.
	if err := h.provider.scopes.With(state.Scope{Project: testProject, Location: state.Global}, func(s *scopeState) error {
		delete(s.queues, testLocation+"/"+testQueue)
		removeTasks(s, testLocation+"/"+testQueue)
		return h.provider.index.Remove(serviceName, state.Scope{Project: testProject, Location: state.Global},
			queueResource(testProject, testLocation, testQueue))
	}); err != nil {
		t.Fatal(err)
	}
	h.createRetryQueue()
	_ = h.invoke(http.MethodPost, queuePath+":pause", map[string]any{})
	if recorder := h.invoke(http.MethodPost, queuePath+"/tasks", namedTask(target.URL, "same", "")); recorder.Code != 200 {
		t.Fatalf("recreate = %d %s", recorder.Code, recorder.Body.String())
	}
	release <- struct{}{}
	time.Sleep(100 * time.Millisecond)
	task := decode(t, h.invoke(http.MethodGet, queuePath+"/tasks/same", nil))
	if task["dispatchCount"] != nil || task["lastAttempt"] != nil {
		t.Fatalf("the old worker changed the recreated task: %v", task)
	}
}
