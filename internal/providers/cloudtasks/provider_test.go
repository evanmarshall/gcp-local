package cloudtasks

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
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
	provider, err := New(context.Background(), DeliveryConfig{}, registry, backend.NewManager(time.Second, time.Second), index, engine)
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
	return &harness{t: t, gate: g, engine: engine, provider: provider}
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
	request.Header.Set("Authorization", "Bearer "+token)
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
	recorder = h.invoke(http.MethodPatch, basePrefix+"/queues/missing", map[string]any{})
	if recorder.Code != 404 {
		t.Fatalf("missing queue update status = %d", recorder.Code)
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
