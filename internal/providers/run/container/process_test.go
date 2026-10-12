package container

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
// bind. Its answers must not count as the revision listening: Listening stays
// false until the revision exits on the taken port.
func TestListeningIgnoresAForeignListenerOnTheRevisionPort(t *testing.T) {
	foreign := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer foreign.Close()
	taken, err := strconv.Atoi(foreign.URL[len("http://127.0.0.1:"):])
	if err != nil {
		t.Fatal(err)
	}
	runtime := helperRuntime(t)
	runtime.reserve = func(context.Context) (int, error) { return taken, nil }
	started, err := runtime.Start(context.Background(), StartSpec{Image: helperImage, Env: []string{"MODE=slow"}})
	if err != nil {
		t.Fatal(err)
	}
	if runtime.Listening(started.Container) {
		t.Fatal("a foreign listener counted as the revision")
	}
	if exit := runtime.Ready(context.Background(), started.Container); !errors.Is(exit, errProcessExited) {
		t.Fatalf("Ready = %v, want the process exit", exit)
	}
	if runtime.Listening(started.Container) {
		t.Fatal("an exited revision counted as listening")
	}
	if err := runtime.Stop(context.Background(), started.Container); err != nil {
		t.Fatal(err)
	}
}

// A revision that binds its own port is seen listening.
func TestListeningSeesTheRevision(t *testing.T) {
	runtime := helperRuntime(t)
	started, err := runtime.Start(context.Background(), StartSpec{Image: helperImage, Env: []string{"MODE=serve"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Stop(context.Background(), started.Container) })
	deadline := time.Now().Add(10 * time.Second)
	for !runtime.Listening(started.Container) {
		if time.Now().After(deadline) {
			t.Fatal("the revision's own listener was not seen")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
