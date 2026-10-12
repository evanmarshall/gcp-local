package pubsub

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// TestEmulatorConfigRejectsUnpinnedInputs proves the provider refuses to start
// anything but an executable Java and the committed emulator jar.
func TestEmulatorConfigRejectsUnpinnedInputs(t *testing.T) {
	dir := t.TempDir()
	// The test binary stands in for an executable Java; only the jar pin fails.
	java, err := os.Executable()
	must(t, err)
	plain := filepath.Join(dir, "plain")
	must(t, os.WriteFile(plain, []byte("x"), 0o600))
	jar := filepath.Join(dir, "emulator.jar")
	must(t, os.WriteFile(jar, []byte("not the pinned jar"), 0o600))
	tests := []struct {
		name   string
		config Config
		want   error
	}{
		{"relative java", Config{Java: "java", JAR: jar}, errBackendPaths},
		{"relative jar", Config{Java: java, JAR: "emulator.jar"}, errBackendPaths},
		{"missing java", Config{Java: filepath.Join(dir, "absent"), JAR: jar}, errJavaMissing},
		{"non-executable java", Config{Java: plain, JAR: jar}, errJavaMissing},
		{"directory java", Config{Java: dir, JAR: jar}, errJavaMissing},
		{"jar mismatch", Config{Java: java, JAR: jar}, errJarMismatch},
	}
	for _, test := range tests {
		if err := test.config.validate(); !errors.Is(err, test.want) {
			t.Errorf("%s: got %v, want %v", test.name, err, test.want)
		}
	}
	if err := (Config{Java: java, JAR: filepath.Join(dir, "absent.jar")}).validate(); err == nil || errors.Is(err, errJarMismatch) {
		t.Fatalf("missing jar = %v", err)
	}

	index := state.NewIndex()
	engine, err := iam.New(index)
	must(t, err)
	manager := backend.NewManager(time.Second, time.Second)
	t.Cleanup(func() { must(t, manager.Shutdown(context.Background())) })
	if _, err := New(gateway.NewRegistry(), manager, index, engine, Config{Java: java, JAR: jar}); !errors.Is(err, errJarMismatch) {
		t.Fatalf("New accepted an unpinned jar: %v", err)
	}
	if len(manager.Status()) != 0 {
		t.Fatal("a rejected configuration registered a backend")
	}
}

// TestProviderRequiresCompleteWiring proves construction fails without every
// shared dependency rather than serving partially.
func TestProviderRequiresCompleteWiring(t *testing.T) {
	index := state.NewIndex()
	engine, err := iam.New(index)
	must(t, err)
	manager := backend.NewManager(time.Second, time.Second)
	factory := func(state.Scope) (backend.Backend, error) { return newMemoryPubSub(), nil }
	if _, err := newProvider(nil, manager, index, engine, factory); !errors.Is(err, errProviderShape) {
		t.Fatalf("missing registry = %v", err)
	}
	if _, err := newProvider(gateway.NewRegistry(), manager, nil, engine, factory); !errors.Is(err, errProviderShape) {
		t.Fatalf("missing index = %v", err)
	}
	registry := gateway.NewRegistry()
	_, err = newProvider(registry, manager, index, engine, factory)
	must(t, err)
	if _, err := newProvider(gateway.NewRegistry(), manager, index, engine, factory); err == nil {
		t.Fatal("a second provider registered the same backend service")
	}
}

// TestEmulatorLifecycleWithoutAProcess covers the emulator driver's lifecycle
// before a JVM exists: readiness fails, and stop/reset clean up idempotently.
func TestEmulatorLifecycleWithoutAProcess(t *testing.T) {
	dir := t.TempDir()
	b := &emulator{config: Config{Java: "java"}, scope: state.Scope{Project: testProject, Location: state.Global}}
	if err := b.Start(context.Background()); !errors.Is(err, errBackendPaths) {
		t.Fatalf("start with an invalid config = %v", err)
	}
	if err := b.Ready(context.Background()); !errors.Is(err, backend.ErrProcessExited) {
		t.Fatalf("ready without a process = %v", err)
	}
	scratch := filepath.Join(dir, "scratch")
	must(t, os.Mkdir(scratch, 0o700))
	b.directory = scratch
	must(t, b.Reset(context.Background()))
	if _, err := os.Stat(scratch); !os.IsNotExist(err) {
		t.Fatal("stop left the emulator data directory")
	}
	must(t, b.Stop(context.Background()))
}
