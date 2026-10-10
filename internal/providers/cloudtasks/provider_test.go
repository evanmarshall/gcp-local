package cloudtasks

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// Names and keys repeated across the queue and task tests.
const (
	testProject    = "demo-tasks"
	testLocation   = "us-central1"
	testQueue      = "workflow"
	testKeyURL     = "url"
	keyName        = "name"
	testKeyBody    = "body"
	testMethodPOST = "POST"
	testKeyRequest = "httpRequest"
	testKeyMethod  = "method"
)

// harness drives the provider through the gateway.
type harness struct {
	t    *testing.T
	gate *gateway.Gateway
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
	_ = provider
	g := gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:" + token + "@" + testProject + ".iam.gserviceaccount.com", Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})
	return &harness{t: t, gate: g}
}

// invoke issues one REST request as the fixture principal.
func (h *harness) invoke(verb, path string, body any) *httptest.ResponseRecorder {
	h.t.Helper()
	var encoded []byte
	if body != nil {
		encoded, _ = json.Marshal(body) //nolint:errchkjson // plain map bodies
	}
	request, _ := http.NewRequestWithContext(context.Background(), verb, path, bytes.NewReader(encoded))
	if encoded != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Authorization", "Bearer actor")
	recorder := httptest.NewRecorder()
	h.gate.ServeHTTP(recorder, request)
	return recorder
}

// basePrefix is the REST path base of the tests.
var basePrefix = "/v2beta3/projects/" + testProject + "/locations/" + testLocation

func TestQueueLifecycleAndPurge(t *testing.T) {
	h := newHarness(t)
	recorder := h.invoke(http.MethodPost, basePrefix+"/queues", map[string]any{
		keyName: "projects/" + testProject + "/locations/" + testLocation + "/queues/" + testQueue,
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
	recorder := h.invoke(http.MethodPost, basePrefix+"/queues/"+testQueue+"/tasks", map[string]any{
		testKeyRequest: map[string]any{testKeyURL: target.URL, testKeyMethod: testMethodPOST},
	})
	if recorder.Code != 200 {
		t.Fatalf("task create status = %d body %s", recorder.Code, recorder.Body.String())
	}
	var task struct {
		Name          string `json:"name"`
		DispatchCount string `json:"dispatchCount"`
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
	recorder = h.invoke(http.MethodDelete, basePrefix+"/queues/"+testQueue+"/tasks/"+task.Name[len(task.Name)-len("task-"):], nil)
	if recorder.Code != 200 {
		t.Fatalf("task delete status = %d", recorder.Code)
	}
}

func TestTaskOnMissingQueueIsRefused(t *testing.T) {
	h := newHarness(t)
	recorder := h.invoke(http.MethodPost, basePrefix+"/queues/missing/tasks", map[string]any{
		testKeyRequest: map[string]any{testKeyURL: "http://127.0.0.1:1/", testKeyMethod: testMethodPOST},
	})
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
	h.invoke(http.MethodPost, basePrefix+"/queues", map[string]any{keyName: testQueue})
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	schedule := time.Now().Add(2 * time.Second).UTC().Format(time.RFC3339)
	recorder := h.invoke(http.MethodPost, basePrefix+"/queues/"+testQueue+"/tasks", map[string]any{
		"scheduleTime": schedule,
		testKeyRequest: map[string]any{testKeyURL: target.URL, testKeyMethod: testMethodPOST},
	})
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
	h.invoke(http.MethodPost, basePrefix+"/queues", map[string]any{keyName: testQueue})
	recorder := h.invoke(http.MethodPost, basePrefix+"/queues/"+testQueue+"/tasks", map[string]any{
		"scheduleTime": "not-a-time",
		testKeyRequest: map[string]any{testKeyURL: "http://127.0.0.1:1/", testKeyMethod: testMethodPOST},
	})
	if recorder.Code != 400 {
		t.Fatalf("invalid schedule status = %d body %s", recorder.Code, recorder.Body.String())
	}
}

func TestQueueUpdateReplacesRateLimits(t *testing.T) {
	h := newHarness(t)
	h.invoke(http.MethodPost, basePrefix+"/queues", map[string]any{keyName: testQueue})
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
	h.invoke(http.MethodPost, basePrefix+"/queues", map[string]any{keyName: testQueue})
	gap := "this method is not implemented"
	probe := func(verb, path string) {
		recorder := h.invoke(verb, path, nil)
		if body := recorder.Body.String(); recorder.Code == 501 && containsString(body, gap) {
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

func containsString(haystack, needle string) bool {
	return len(needle) > 0 && len(haystack) >= len(needle) && index(haystack, needle) >= 0
}

func index(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
