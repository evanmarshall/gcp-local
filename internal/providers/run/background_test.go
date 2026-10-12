package run

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/state"
)

// exitingRuntime is the fake runtime whose containers exit only when a test
// sends their exit error.
type exitingRuntime struct {
	*fakeRuntime
	exits map[string]chan error
}

func (e *exitingRuntime) exit(container string) chan error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.exits[container] == nil {
		e.exits[container] = make(chan error, 1)
	}
	return e.exits[container]
}

func (e *exitingRuntime) Ready(ctx context.Context, container string) error {
	select {
	case err := <-e.exit(container):
		return err
	case <-ctx.Done():
		return nil
	}
}

// errExited is a container exit the tests inject.
var errExited = errors.New("the container exited")

// rolloutState reads the service's rollout operation: done, and its error code.
func (h *harness) rolloutState(t *testing.T) (bool, float64) {
	t.Helper()
	recorder := h.invoke(http.MethodGet, "/v2/projects/demo-project/locations/us-central1/operations/rel-svc", nil)
	var operation struct {
		Done  bool `json:"done"`
		Error struct {
			Code float64 `json:"code"`
		} `json:"error"`
	}
	if recorder.Code != http.StatusOK || json.Unmarshal(recorder.Body.Bytes(), &operation) != nil {
		t.Fatalf("operation get = %d %s", recorder.Code, recorder.Body.String())
	}
	return operation.Done, operation.Error.Code
}

// hasReaper reports whether one service's idle reaper is registered.
func (h *harness) hasReaper(scope state.Scope, id string) bool {
	h.provider.reaperMu.Lock()
	defer h.provider.reaperMu.Unlock()
	return h.provider.reapers[reaperKey(scope, id)] != nil
}

// Service IDs repeat across locations; resetting one location must leave the
// other location's reaper running.
func TestResettingOneLocationKeepsAnotherLocationsReaper(t *testing.T) {
	h := newHarness(t)
	h.create(t)
	europe := map[string]any{"template": map[string]any{"containers": []any{map[string]any{"image": imageReference}}}}
	if recorder := h.invoke(http.MethodPost, "/v2/projects/demo-project/locations/europe-west1/services?serviceId=svc", europe); recorder.Code != http.StatusOK {
		t.Fatalf("europe create = %d %s", recorder.Code, recorder.Body.String())
	}
	central := state.Scope{Project: "demo-project", Location: "us-central1"}
	if err := h.provider.Reset(context.Background(), central); err != nil {
		t.Fatal(err)
	}
	if h.hasReaper(central, "svc") || !h.hasReaper(state.Scope{Project: "demo-project", Location: "europe-west1"}, "svc") {
		t.Fatal("resetting us-central1 cancelled the europe-west1 reaper or kept its own")
	}
}

// Deleting one service must leave its neighbors' reapers running.
func TestDeletingOneServiceKeepsAnothersReaper(t *testing.T) {
	h := newHarness(t)
	h.create(t)
	other := map[string]any{"template": map[string]any{"containers": []any{map[string]any{"image": "example.local/other:1"}}}}
	if recorder := h.invoke(http.MethodPost, servicesPath+"?serviceId=other", other); recorder.Code != http.StatusOK {
		t.Fatalf("second create = %d %s", recorder.Code, recorder.Body.String())
	}
	if recorder := h.invoke(http.MethodDelete, "/v2/"+serviceReference, nil); recorder.Code != http.StatusOK {
		t.Fatalf("delete = %d %s", recorder.Code, recorder.Body.String())
	}
	scope := state.Scope{Project: "demo-project", Location: "us-central1"}
	if h.hasReaper(scope, "svc") || !h.hasReaper(scope, "other") {
		t.Fatal("deleting svc cancelled the other service's reaper or kept its own")
	}
}

// A finished operation is final: a container that exits after its rollout
// succeeded must not turn the operation into an error.
func TestFinishedRolloutNeverFlipsToError(t *testing.T) {
	h := newHarness(t)
	revision := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer revision.Close()
	h.runtime.address = revision.Listener.Addr().String()
	runtime := &exitingRuntime{fakeRuntime: h.runtime, exits: map[string]chan error{}}
	h.provider.runtime = runtime
	h.create(t)
	waitFor(t, func() bool { done, _ := h.rolloutState(t); return done })
	if _, code := h.rolloutState(t); code != 0 {
		t.Fatalf("rollout failed with %v", code)
	}
	runtime.exit(imageReference + "-container") <- errExited
	time.Sleep(300 * time.Millisecond)
	if done, code := h.rolloutState(t); !done || code != 0 {
		t.Fatalf("succeeded rollout became done=%v code=%v", done, code)
	}
}

// Rollouts share one operation name; the first rollout's container exiting
// must not finish the second rollout's operation.
func TestStaleWatcherCannotFinishTheNextRollout(t *testing.T) {
	h := newHarness(t)
	runtime := &exitingRuntime{fakeRuntime: h.runtime, exits: map[string]chan error{}}
	h.provider.runtime = runtime
	h.create(t)
	update := map[string]any{"template": map[string]any{"containers": []any{map[string]any{"image": "example.local/svc:2"}}}}
	if recorder := h.invoke(http.MethodPatch, "/v2/"+serviceReference, update); recorder.Code != http.StatusOK {
		t.Fatalf("update = %d %s", recorder.Code, recorder.Body.String())
	}
	runtime.exit(imageReference + "-container") <- errExited
	time.Sleep(300 * time.Millisecond)
	if done, code := h.rolloutState(t); done {
		t.Fatalf("the first rollout's exit finished the second rollout with code %v", code)
	}
}

// waitFor polls a condition for up to ten seconds.
func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("the condition did not hold in time")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// An invocation of a reaped revision cold-starts its container and rearms
// the idle reaper.
func TestInvocationColdStartsAReapedRevision(t *testing.T) {
	h := newHarness(t)
	h.create(t)
	if recorder := h.invoke(http.MethodPost, "/v2/"+serviceReference+":setIamPolicy", map[string]any{"policy": map[string]any{
		"bindings": []any{map[string]any{"role": "roles/run.invoker", "members": []any{"allUsers"}}},
	}}); recorder.Code != http.StatusOK {
		t.Fatalf("setIamPolicy = %d %s", recorder.Code, recorder.Body.String())
	}
	scope := state.Scope{Project: "demo-project", Location: "us-central1"}
	h.provider.clock = func() time.Time { return time.Now().Add(2 * idleTimeout) }
	if !h.provider.reap(context.Background(), scope, "svc") {
		t.Fatal("the idle revision was not reaped")
	}
	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/_run/svc-demo-project.us-central1.run.app/", nil)
	h.provider.Invocation().ServeHTTP(httptest.NewRecorder(), request)
	h.runtime.mu.Lock()
	launches := len(h.runtime.started)
	h.runtime.mu.Unlock()
	if launches != 2 || !h.hasReaper(scope, "svc") {
		t.Fatalf("after the invocation: %d launches, reaper armed %v", launches, h.hasReaper(scope, "svc"))
	}
}
