package backend_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/providers/storage"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// The test executable doubles as a real child backend, avoiding an installed
// emulator or shell. It never loads credentials and listens on loopback only.
func TestProcessFixture(_ *testing.T) {
	if os.Getenv("GCP_LOCAL_PROCESS_FIXTURE") != "1" {
		return
	}
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") != "" || os.Getenv("HTTPS_PROXY") != "" {
		os.Exit(42)
	}
	mode := os.Getenv("GCP_LOCAL_FIXTURE_MODE")
	if mode == "exit" {
		os.Exit(23)
	}
	if mode == "ignore-term" {
		signal.Ignore(syscall.SIGTERM)
	}
	listener, err := listenTestAddress()
	if err != nil {
		os.Exit(24)
	}
	writeFixtureAssets(os.Getenv("GCP_LOCAL_FIXTURE_DIR"), listener.Addr().String())
	server := &http.Server{ReadHeaderTimeout: time.Second, Handler: fixtureHandler(mode)}
	signals := []os.Signal{os.Interrupt}
	if mode != "ignore-term" {
		signals = append(signals, syscall.SIGTERM)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), signals...)
	go func() { <-ctx.Done(); _ = server.Close() }()
	_ = server.Serve(listener)
	cancel()
	os.Exit(0)
}

// writeFixtureAssets records the listener address and the owned payload that a
// reset must remove.
func writeFixtureAssets(directory, address string) {
	if os.WriteFile(filepath.Join(directory, "address"), []byte(address), 0600) != nil { //nolint:gosec // the fixture harness owns this directory
		os.Exit(25)
	}
	if os.WriteFile(filepath.Join(directory, "payload"), []byte("owned data"), 0600) != nil { //nolint:gosec // the fixture harness owns this directory
		os.Exit(26)
	}
}

// fixtureHandler serves readiness for mode; /exit kills the process mid-request.
func fixtureHandler(mode string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/exit" {
			os.Exit(27)
		}
		if mode == "unready" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
}

type fixture struct {
	process    *backend.Process
	directory  string
	failReset  atomic.Bool
	blockProbe atomic.Bool
	resets     atomic.Int32
}

// listenTestAddress opens a loopback listener through the context-aware API.
func listenTestAddress() (net.Listener, error) {
	var listen net.ListenConfig
	listener, err := listen.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen on loopback: %w", err)
	}
	return listener, nil
}

// Sentinels returned by the test's injected failures.
var (
	errInjectedReset  = errors.New("injected asset reset failure")
	errResetCrossed   = errors.New("reset crossed active session")
	errPublisherStuck = errors.New("publisher was blocked by the session")
)

func newFixture(t *testing.T, mode string) *fixture {
	t.Helper()
	directory := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	result := &fixture{directory: directory}
	result.process, err = backend.NewProcess(backend.ProcessConfig{
		Command:     []string{executable, "-test.run=^TestProcessFixture$"},
		Environment: []string{"GCP_LOCAL_PROCESS_FIXTURE=1", "GCP_LOCAL_FIXTURE_MODE=" + mode, "GCP_LOCAL_FIXTURE_DIR=" + directory, "GORACE=atexit_sleep_ms=0"},
		Probe:       result.probe,
		ResetAssets: result.resetAssets,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = result.process.Stop(ctx)
	})
	return result
}

// probe checks the child's HTTP readiness, or blocks until cancelled when the
// test holds the probe.
func (f *fixture) probe(ctx context.Context) error {
	if f.blockProbe.Load() {
		<-ctx.Done()
		return ctx.Err()
	}
	address, err := os.ReadFile(filepath.Join(f.directory, "address"))
	if err != nil {
		return fmt.Errorf("read address: %w", err)
	}
	probe, err := backend.HTTPProbe("http://" + string(address) + "/health")
	if err != nil {
		return fmt.Errorf("probe endpoint: %w", err)
	}
	return probe(ctx)
}

// resetAssets removes the child's owned files unless the test injects a failure.
func (f *fixture) resetAssets(ctx context.Context) error {
	if f.failReset.Load() {
		return errInjectedReset
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, name := range []string{"payload", "address"} {
		if err := os.Remove(filepath.Join(f.directory, name)); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove %s: %w", name, err)
		}
	}
	f.resets.Add(1)
	return nil
}

func key(project string) backend.Key {
	return backend.Key{Service: "storage", Scope: state.Scope{Project: project, Location: state.Global}}
}

// start brings up the backend for k through Use, the path providers take.
func start(ctx context.Context, manager *backend.Manager, k backend.Key) error {
	if err := manager.Use(ctx, k, func(context.Context, backend.Backend) error { return nil }); err != nil {
		return fmt.Errorf("start %s: %w", k.Service, err)
	}
	return nil
}

func shutdown(t *testing.T, manager *backend.Manager) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := manager.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}
func stopped(t *testing.T, fixtures ...*fixture) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, f := range fixtures {
		if !errors.Is(f.process.Ready(ctx), backend.ErrProcessExited) {
			t.Fatal("backend process remained alive")
		}
	}
}

func TestLazyStartIsConcurrentAndDoesNotInheritCredentials(t *testing.T) {
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "must-not-reach-backend")
	t.Setenv("HTTPS_PROXY", "must-not-reach-backend")
	manager := backend.NewManager(2*time.Second, time.Second)
	fixture := newFixture(t, "ready")
	var creates atomic.Int32
	if err := manager.Register("storage", backend.Definition{Validate: state.GlobalScope, New: func(state.Scope) (backend.Backend, error) { creates.Add(1); return fixture.process, nil }, AfterReset: func(state.Scope) error { return nil }}); err != nil {
		t.Fatal(err)
	}
	if len(manager.Status()) != 0 || creates.Load() != 0 {
		t.Fatal("backend started eagerly")
	}
	if err := startConcurrently(manager, key("project-a"), 16); err != nil {
		t.Fatal(err)
	}
	if creates.Load() != 1 || manager.Status()[0].Phase != backend.Ready {
		t.Fatal("lazy start ran more than once")
	}
	if manager.Status()[0].Snapshot || manager.Status()[0].Restore {
		t.Fatal("claimed persistence")
	}
	shutdown(t, manager)
	stopped(t, fixture)
	if err := start(context.Background(), manager, key("project-a")); !errors.Is(err, backend.ErrClosed) {
		t.Fatal("restarted a shut down manager")
	}
}

// startConcurrently races n starts of the same backend and joins their errors.
func startConcurrently(manager *backend.Manager, k backend.Key, n int) error {
	var group sync.WaitGroup
	failures := make([]error, n)
	for i := range n {
		group.Go(func() { failures[i] = start(context.Background(), manager, k) })
	}
	group.Wait()
	return errors.Join(failures...)
}

// bucketKnown reports whether the catalog and the shared index still hold a bucket.
func bucketKnown(catalog *storage.Catalog, index *state.Index, scope state.Scope, name string) (bool, bool) {
	_, inCatalog, _ := catalog.Bucket(scope, name)
	_, inIndex := index.Get(storage.BucketResource(name))
	return inCatalog, inIndex
}

// startWithBucket starts k's backend and then publishes one bucket in its scope.
func startWithBucket(t *testing.T, manager *backend.Manager, catalog *storage.Catalog, k backend.Key, name string) {
	t.Helper()
	if err := start(context.Background(), manager, k); err != nil {
		t.Fatal(err)
	}
	if err := catalog.PublishBucket(k.Scope, storage.Bucket{Name: name, Location: "us-central1"}); err != nil {
		t.Fatal(err)
	}
}

// failResetOnce makes one reset of k fail on its assets, runs check against the
// failed state, verifies the backend refuses requests, and clears the injection.
func failResetOnce(t *testing.T, manager *backend.Manager, f *fixture, k backend.Key, check func()) {
	t.Helper()
	f.failReset.Store(true)
	if manager.Reset(context.Background(), k) == nil {
		t.Fatal("reset ignored backend failure")
	}
	stopped(t, f)
	check()
	if err := start(context.Background(), manager, k); !errors.Is(err, backend.ErrFailed) {
		t.Fatalf("failed backend served requests: %v", err)
	}
	f.failReset.Store(false)
}

// registerFixturePerScope gives every scope its own ready child process and
// returns the scope-to-fixture map, valid once those scopes have started.
func registerFixturePerScope(t *testing.T, manager *backend.Manager, afterReset func(state.Scope) error) map[state.Scope]*fixture {
	t.Helper()
	fixtures := map[state.Scope]*fixture{}
	var mu sync.Mutex
	if err := manager.Register("storage", backend.Definition{Validate: state.GlobalScope, New: func(scope state.Scope) (backend.Backend, error) {
		f := newFixture(t, "ready")
		mu.Lock()
		fixtures[scope] = f
		mu.Unlock()
		return f.process, nil
	}, AfterReset: afterReset}); err != nil {
		t.Fatal(err)
	}
	return fixtures
}

func TestResetFailurePreservesMetadataUntilAssetsAreClean(t *testing.T) {
	index := state.NewIndex()
	catalog := storage.NewCatalog(index)
	manager := backend.NewManager(2*time.Second, time.Second)
	fixtures := registerFixturePerScope(t, manager, catalog.Reset)
	a, b := key("project-a"), key("project-b")
	startWithBucket(t, manager, catalog, a, "bucket-a")
	startWithBucket(t, manager, catalog, b, "bucket-b")
	failResetOnce(t, manager, fixtures[a.Scope], a, func() {
		if inCatalog, inIndex := bucketKnown(catalog, index, a.Scope, "bucket-a"); !inCatalog || !inIndex {
			t.Fatalf("failed reset discarded metadata: catalog %v, index %v", inCatalog, inIndex)
		}
	})
	if err := manager.Reset(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if inCatalog, inIndex := bucketKnown(catalog, index, a.Scope, "bucket-a"); inCatalog || inIndex {
		t.Fatalf("successful reset retained metadata: catalog %v, index %v", inCatalog, inIndex)
	}
	if _, found, _ := catalog.Bucket(b.Scope, "bucket-b"); !found {
		t.Fatal("reset crossed scope")
	}
	if err := fixtures[b.Scope].process.Ready(context.Background()); err != nil {
		t.Fatal("reset stopped other project")
	}
	if err := start(context.Background(), manager, a); err != nil {
		t.Fatal("reset did not allow lazy restart:", err)
	}
	if _, found, _ := catalog.Bucket(a.Scope, "bucket-a"); found {
		t.Fatal("restart restored discarded metadata")
	}
	shutdown(t, manager)
	stopped(t, fixtures[a.Scope], fixtures[b.Scope])
}
func TestStartupAndReadinessFailuresAreCleanedAndReported(t *testing.T) {
	for _, mode := range []string{"exit", "unready"} {
		t.Run(mode, func(t *testing.T) {
			manager := backend.NewManager(200*time.Millisecond, time.Second)
			fixture := newFixture(t, mode)
			_ = manager.Register("storage", backend.Definition{New: func(state.Scope) (backend.Backend, error) { return fixture.process, nil }, AfterReset: func(state.Scope) error { return nil }})
			if err := start(context.Background(), manager, key("project-a")); err == nil {
				t.Fatal("failed process reported ready")
			}
			status := manager.Status()[0]
			if status.Phase != backend.Failed || status.Failure == "" {
				t.Fatal(status)
			}
			stopped(t, fixture)
			if err := manager.Reset(context.Background(), key("project-a")); err != nil {
				t.Fatal(err)
			}
			if fixture.resets.Load() != 1 {
				t.Fatal("assets not reset")
			}
			shutdown(t, manager)
		})
	}
}
func TestReadinessDetectsUnexpectedExit(t *testing.T) {
	manager := backend.NewManager(2*time.Second, time.Second)
	fixture := newFixture(t, "ready")
	_ = manager.Register("storage", backend.Definition{New: func(state.Scope) (backend.Backend, error) { return fixture.process, nil }, AfterReset: func(state.Scope) error { return nil }})
	if err := start(context.Background(), manager, key("project-a")); err != nil {
		t.Fatal(err)
	}
	address, _ := os.ReadFile(filepath.Join(fixture.directory, "address"))
	client := http.Client{Timeout: time.Second}
	request, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+string(address)+"/exit", nil)
	response, _ := client.Do(request)
	if response != nil {
		_ = response.Body.Close()
	}
	if err := start(context.Background(), manager, key("project-a")); err == nil {
		t.Fatal("crashed backend remained ready")
	}
	if manager.Status()[0].Phase != backend.Failed {
		t.Fatal(manager.Status())
	}
	shutdown(t, manager)
	stopped(t, fixture)
}
func TestShutdownCancelsStartingBackend(t *testing.T) {
	manager := backend.NewManager(10*time.Second, time.Second)
	fixture := newFixture(t, "unready")
	_ = manager.Register("storage", backend.Definition{New: func(state.Scope) (backend.Backend, error) { return fixture.process, nil }, AfterReset: func(state.Scope) error { return nil }})
	result := make(chan error, 1)
	go func() { err := start(context.Background(), manager, key("project-a")); result <- err }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(fixture.directory, "address")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fixture did not start")
		}
		time.Sleep(5 * time.Millisecond)
	}
	shutdown(t, manager)
	if err := <-result; err == nil {
		t.Fatal("cancelled startup succeeded")
	}
	stopped(t, fixture)
}
func TestProcessDeadlineForcesTermination(t *testing.T) {
	fixture := newFixture(t, "ignore-term")
	manager := backend.NewManager(2*time.Second, 30*time.Millisecond)
	_ = manager.Register("storage", backend.Definition{New: func(state.Scope) (backend.Backend, error) { return fixture.process, nil }, AfterReset: func(state.Scope) error { return nil }})
	if err := start(context.Background(), manager, key("project-a")); err != nil {
		t.Fatal(err)
	}
	if err := fixture.process.Reset(context.Background()); err == nil {
		t.Fatal("reset running process")
	}
	if err := manager.Shutdown(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	stopped(t, fixture)
}
func TestHTTPProbeRejectsExternalOrRedirectedReadiness(t *testing.T) {
	for _, endpoint := range []string{"https://storage.googleapis.com/", "http://localhost:123/", "http://127.0.0.1/", "http://user:secret@127.0.0.1:123/"} {
		if _, err := backend.HTTPProbe(endpoint); err == nil {
			t.Fatalf("accepted %q", endpoint)
		}
	}
	listener, err := listenTestAddress()
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{ReadHeaderTimeout: time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", "https://storage.googleapis.com/")
		w.WriteHeader(http.StatusFound)
	})}
	go func() { _ = server.Serve(listener) }()
	defer server.Close()
	probe, err := backend.HTTPProbe("http://" + listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if err := probe(context.Background()); err == nil {
		t.Fatal("followed redirect or accepted 302 readiness")
	}
}

func TestCancelledProbeKeepsHealthyBackend(t *testing.T) {
	manager := backend.NewManager(time.Second, time.Second)
	fixture := newFixture(t, "ready")
	_ = manager.Register("storage", backend.Definition{New: func(state.Scope) (backend.Backend, error) { return fixture.process, nil }, AfterReset: func(state.Scope) error { return nil }})
	if err := start(context.Background(), manager, key("project-a")); err != nil {
		t.Fatal(err)
	}
	fixture.blockProbe.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := start(ctx, manager, key("project-a")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if manager.Status()[0].Phase != backend.Ready {
		t.Fatal("cancelled caller marked backend failed")
	}
	fixture.blockProbe.Store(false)
	if err := start(context.Background(), manager, key("project-a")); err != nil {
		t.Fatal(err)
	}
	shutdown(t, manager)
}
func TestResetWaitsForProviderMutationAndShutdownCancelsUse(t *testing.T) {
	index := state.NewIndex()
	catalog := storage.NewCatalog(index)
	manager := backend.NewManager(time.Second, time.Second)
	fixture := newFixture(t, "ready")
	scope := key("project-a")
	_ = manager.Register("storage", backend.Definition{New: func(state.Scope) (backend.Backend, error) { return fixture.process, nil }, AfterReset: catalog.Reset})
	entered, release := make(chan struct{}), make(chan struct{})
	operation := make(chan error, 1)
	go func() {
		operation <- manager.Use(context.Background(), scope, func(_ context.Context, _ backend.Backend) error {
			close(entered)
			<-release
			return catalog.PublishBucket(scope.Scope, storage.Bucket{Name: "bucket", Location: "us-central1"})
		})
	}()
	<-entered
	reset := make(chan error, 1)
	go func() { reset <- manager.Reset(context.Background(), scope) }()
	select {
	case err := <-reset:
		t.Fatalf("reset crossed an active mutation: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if err := <-operation; err != nil {
		t.Fatal(err)
	}
	if err := <-reset; err != nil {
		t.Fatal(err)
	}
	if _, found, _ := catalog.Bucket(scope.Scope, "bucket"); found {
		t.Fatal("in-flight write escaped reset")
	}
	if _, found := index.Get(storage.BucketResource("bucket")); found {
		t.Fatal("in-flight index publication escaped reset")
	}
	entered = make(chan struct{})
	go func() {
		operation <- manager.Use(context.Background(), scope, func(ctx context.Context, _ backend.Backend) error { close(entered); <-ctx.Done(); return ctx.Err() })
	}()
	<-entered
	shutdown(t, manager)
	if err := <-operation; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	stopped(t, fixture)
}

func TestSessionsAllowPublishingAndDrainBeforeReset(t *testing.T) {
	manager := backend.NewManager(time.Second, time.Second)
	f := newFixture(t, "ready")
	var ended atomic.Bool
	_ = manager.Register("storage", backend.Definition{New: func(state.Scope) (backend.Backend, error) { return f.process, nil }, AfterReset: func(state.Scope) error {
		if !ended.Load() {
			return errResetCrossed
		}
		return nil
	}})
	entered, published := make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- manager.Session(context.Background(), key("project-a"), func(ctx context.Context, _ backend.Backend) error {
			close(entered)
			select {
			case <-published:
			case <-ctx.Done():
				return errPublisherStuck
			}
			<-ctx.Done()
			ended.Store(true)
			return ctx.Err()
		})
	}()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := manager.Use(ctx, key("project-a"), func(context.Context, backend.Backend) error { close(published); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := manager.Reset(ctx, key("project-a")); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	stopped(t, f)
	// Reset permits new sessions; listener draining permanently rejects them.
	if err := manager.Session(ctx, key("project-a"), func(context.Context, backend.Backend) error { return nil }); err != nil {
		t.Fatal(err)
	}
	manager.DrainSessions()
	if err := manager.Session(ctx, key("project-a"), func(context.Context, backend.Backend) error { t.Fatal("session started after drain"); return nil }); !errors.Is(err, backend.ErrClosed) {
		t.Fatal(err)
	}
	if err := manager.Use(ctx, key("project-a"), func(context.Context, backend.Backend) error { return nil }); err != nil {
		t.Fatal("ordinary request lost its drain period", err)
	}
	shutdown(t, manager)
}

func TestSessionResetTimeoutRetainsMetadataAndAllowsRetry(t *testing.T) {
	manager := backend.NewManager(time.Second, 50*time.Millisecond)
	f := newFixture(t, "ready")
	var resets atomic.Int32
	_ = manager.Register("storage", backend.Definition{New: func(state.Scope) (backend.Backend, error) { return f.process, nil }, AfterReset: func(state.Scope) error { resets.Add(1); return nil }})
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- manager.Session(context.Background(), key("project-a"), func(context.Context, backend.Backend) error { close(entered); <-release; return nil })
	}()
	<-entered
	if err := manager.Reset(context.Background(), key("project-a")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if resets.Load() != 0 || manager.Status()[0].Phase != backend.Failed {
		t.Fatal("failed reset discarded metadata or remained usable")
	}
	close(release)
	<-done
	if err := manager.Reset(context.Background(), key("project-a")); err != nil {
		t.Fatal(err)
	}
	if resets.Load() != 1 {
		t.Fatal("retry did not reset")
	}
	shutdown(t, manager)
}
