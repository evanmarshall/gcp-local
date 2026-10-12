package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	runprovider "github.com/evanmarshall/gcp-local/internal/providers/run"
	core "github.com/evanmarshall/gcp-local/internal/runtime"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// loopbackHost is where revision processes and a bridge-less core listen.
const loopbackHost = "127.0.0.1"

// Cloud Run runtime flag combinations that are refused.
var (
	errRunDockerFlags  = errors.New("run-docker-network and run-docker-environment require run-docker-socket")
	errRunBothRuntimes = errors.New("select either run-docker-socket or run-process, not both")
)

// assembleRun registers the Cloud Run provider and its invocation surface.
func assembleRun(ctx context.Context, options serveOptions, selected map[string]bool, registry *gateway.Registry, server *core.Server, index *state.Index, engine *iam.Engine, identities *identity.Service) error {
	if !selected["run"] {
		return nil
	}
	setup, err := runRuntime(ctx, options, server)
	if err != nil {
		return err
	}
	config := runprovider.Config{BaseURL: runBaseURL(options.listen)}
	if identities != nil {
		// Workload identity: each revision's configured service account gets
		// its own metadata handler on an address the revision can reach.
		// Without an identity config the feature stays off. The same key set
		// verifies the invocation audience tokens.
		config.Metadata = identities.Metadata
		config.MetadataBind = workloadMetadataBind(ctx, setup.metadataHost)
		config.Tokens = identities.IDTokens
	}
	provider, err := runprovider.New(ctx, config, registry, server.Backends, index, engine, setup.runtime)
	if err != nil {
		return fmt.Errorf("register the Cloud Run provider: %w", err)
	}
	server.Run = provider.Invocation()
	return nil
}

// runSetup is the revision runtime and the address its revisions dial for
// their metadata servers.
type runSetup struct {
	runtime      runprovider.Runtime
	metadataHost string
}

// runRuntime builds the provider-owned revision runtime. Without a Docker
// socket or a process mapping the provider refuses every launch.
func runRuntime(ctx context.Context, options serveOptions, server *core.Server) (runSetup, error) {
	if err := checkRunFlags(options); err != nil {
		return runSetup{}, err
	}
	if options.runSocket == "" && len(options.runProcess) == 0 {
		return runSetup{runprovider.NoopRuntime{}, loopbackHost}, nil
	}
	if options.runSocket == "" {
		// Revision processes share the core's loopback.
		return runSetup{processRuntime(options.runProcess), loopbackHost}, nil
	}
	docker, err := runprovider.NewDockerRuntime(runprovider.DockerConfig{Socket: options.runSocket, Network: options.runNetwork, Environment: options.runEnv})
	if err != nil {
		return runSetup{}, fmt.Errorf("configure the Cloud Run Docker runtime: %w", err)
	}
	host, err := dockerMetadataHost(ctx, docker, options.runNetwork)
	if err != nil {
		return runSetup{}, err
	}
	if options.runEnv != "" {
		// A previous core may have exited without stopping its revisions.
		if err := docker.Sweep(ctx); err != nil {
			return runSetup{}, fmt.Errorf("remove earlier revision containers: %w", err)
		}
		server.AfterShutdown = docker.Sweep
	}
	return runSetup{docker, host}, nil
}

// checkRunFlags refuses Docker options without a socket and two runtimes at once.
func checkRunFlags(options serveOptions) error {
	if options.runSocket == "" && (options.runNetwork != "" || options.runEnv != "") {
		return errRunDockerFlags
	}
	if options.runSocket != "" && len(options.runProcess) > 0 {
		return errRunBothRuntimes
	}
	return nil
}

// processRuntime maps images to local commands.
func processRuntime(mapped map[string]string) *runprovider.ProcessRuntime {
	commands := map[string][]string{}
	for image, command := range mapped {
		commands[image] = strings.Fields(command)
	}
	return runprovider.NewProcessRuntime(commands)
}

// dockerMetadataHost picks the core address revision containers can dial: its
// own address on the configured network, or the default bridge gateway when
// the core runs on the engine host.
func dockerMetadataHost(ctx context.Context, docker *runprovider.DockerRuntime, network string) (string, error) {
	if network != "" {
		host, err := docker.LocalAddress(ctx)
		if err != nil {
			return "", fmt.Errorf("find the core address on network %s: %w", network, err)
		}
		return host, nil
	}
	if bridgeExists("172.17.0.1") {
		return "172.17.0.1", nil
	}
	return loopbackHost, nil
}

// workloadMetadataBind returns the binder that serves one per-revision
// metadata handler on host and reports the dialable host:port.
func workloadMetadataBind(ctx context.Context, host string) func(http.Handler) (string, func(), error) {
	return func(handler http.Handler) (string, func(), error) {
		var listen net.ListenConfig
		listener, err := listen.Listen(ctx, "tcp", net.JoinHostPort(host, "0"))
		if err != nil {
			return "", nil, fmt.Errorf("bind the workload metadata listener: %w", err)
		}
		server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 20 * time.Second}
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = server.Serve(listener)
		}()
		return listener.Addr().String(), func() { _ = server.Close(); <-done }, nil
	}
}

// bridgeExists reports whether an interface owns the given address.
func bridgeExists(address string) bool {
	interfaces, err := net.InterfaceAddrs()
	if err != nil {
		return false
	}
	for _, candidate := range interfaces {
		if ip, ok := candidate.(*net.IPNet); ok && ip.IP.String() == address {
			return true
		}
	}
	return false
}
