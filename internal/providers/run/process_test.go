package run

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"
)

// helperImage maps to this test binary re-run as a revision process; the
// service's MODE env selects what it does.
const helperImage = "example.local/helper:1"

// TestRunHelperProcess is the revision process, not a test: it serves PORT,
// exiting 1 when the port is taken (after a second in slow mode, like an
// application that initializes before it binds), or exits 3 at once.
func TestRunHelperProcess(*testing.T) {
	switch os.Getenv("MODE") {
	case "slow":
		time.Sleep(time.Second)
		fallthrough
	case "serve":
		listener, err := net.Listen("tcp", "127.0.0.1:"+os.Getenv("PORT")) //nolint:noctx // the helper process has no context
		if err != nil {
			os.Exit(1)
		}
		_ = http.Serve(listener, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })) //nolint:gosec // a loopback test helper
		os.Exit(0)
	case "exit":
		os.Exit(3)
	}
}

// helperRuntime runs this binary as the helper image's revision process.
func helperRuntime(t *testing.T) *ProcessRuntime {
	t.Helper()
	return NewProcessRuntime(map[string][]string{helperImage: {os.Args[0], "-test.run=^TestRunHelperProcess$"}})
}

// Ready may be called by the readiness watcher and by Stop's caller; every
// caller sees the one exit, never a second wait's "no child processes".
func TestProcessReadyReportsOneExitToEveryWaiter(t *testing.T) {
	runtime := helperRuntime(t)
	started, err := runtime.Start(context.Background(), StartSpec{Image: helperImage, Env: []string{"MODE=exit"}})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if exit := runtime.Ready(context.Background(), started.Container); !errors.Is(exit, errProcessExited) {
			t.Fatalf("Ready = %v, want the process exit", exit)
		}
	}
	if err := runtime.Stop(context.Background(), started.Container); err != nil {
		t.Fatal(err)
	}
}

// Another process can bind the port between reserve and the revision's own
// bind. Its answers must not make the revision ready: the rollout fails when
// the revision exits on the taken port, and never reports success first.
func TestRolloutIgnoresAForeignListenerOnTheRevisionPort(t *testing.T) {
	foreign := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer foreign.Close()
	taken, err := strconv.Atoi(foreign.URL[len("http://127.0.0.1:"):])
	if err != nil {
		t.Fatal(err)
	}
	h := newHarness(t)
	runtime := helperRuntime(t)
	runtime.reserve = func(context.Context) (int, error) { return taken, nil }
	h.provider.runtime = runtime
	body := map[string]any{"template": map[string]any{"containers": []any{map[string]any{
		"image": helperImage, "env": []any{map[string]any{"name": "MODE", "value": "slow"}},
	}}}}
	if recorder := h.invoke(http.MethodPost, servicesPath+"?serviceId=svc", body); recorder.Code != http.StatusOK {
		t.Fatalf("create = %d %s", recorder.Code, recorder.Body.String())
	}
	var code float64
	waitFor(t, func() bool { done, observed := h.rolloutState(t); code = observed; return done })
	if code == 0 {
		t.Fatal("the rollout succeeded on a foreign listener's answers")
	}
}

// A revision that binds its own port becomes ready, and the runtime sees
// its listener.
func TestRolloutSucceedsWhenTheRevisionListens(t *testing.T) {
	h := newHarness(t)
	runtime := helperRuntime(t)
	h.provider.runtime = runtime
	body := map[string]any{"template": map[string]any{"containers": []any{map[string]any{
		"image": helperImage, "env": []any{map[string]any{"name": "MODE", "value": "serve"}},
	}}}}
	if recorder := h.invoke(http.MethodPost, servicesPath+"?serviceId=svc", body); recorder.Code != http.StatusOK {
		t.Fatalf("create = %d %s", recorder.Code, recorder.Body.String())
	}
	t.Cleanup(func() { _ = h.invoke(http.MethodDelete, "/v2/"+serviceReference, nil) })
	waitFor(t, func() bool { done, _ := h.rolloutState(t); return done })
	if _, code := h.rolloutState(t); code != 0 {
		t.Fatalf("rollout failed with code %v", code)
	}
}
