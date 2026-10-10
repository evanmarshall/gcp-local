package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"

	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/providers/pubsub"
	runprovider "github.com/evanmarshall/gcp-local/internal/providers/run"
	"github.com/evanmarshall/gcp-local/internal/providers/storage"
	core "github.com/evanmarshall/gcp-local/internal/runtime"
	"github.com/evanmarshall/gcp-local/internal/state"
)

var version = "dev"

func run(ctx context.Context, args []string, output, errors io.Writer) int {
	if code, handled := runSubcommand(ctx, args, output, errors); handled {
		return code
	}
	options, ok := parseServeFlags(args, errors)
	if !ok {
		return 2
	}
	if options.showVersion {
		fmt.Fprintln(output, version)
		return 0
	}
	server, listeners, err := assemble(ctx, options)
	if err != nil {
		fmt.Fprintln(errors, err)
		if listenFailure(err) {
			return 1
		}
		return 2
	}
	logger := slog.New(slog.NewJSONHandler(output, nil))
	logger.Info("gcp-local listening", "address", listeners.HTTP.Addr().String(), "grpc_address", listeners.GRPC.Addr().String(), "version", version)
	if listeners.Metadata != nil {
		logger.Info("workload metadata listening", "address", listeners.Metadata.Addr().String())
	}
	if err := server.Serve(ctx, listeners); err != nil {
		logger.Error("gcp-local stopped with an error", "error", err.Error())
		return 1
	}
	logger.Info("gcp-local stopped")
	return 0
}

// errListen marks listener-binding failures, which exit 1 rather than 2.
var errListen = errors.New("listener binding failed")

// errMetadataRequirements states what the metadata listener needs.
var errMetadataRequirements = errors.New("metadata requires a loopback address, identity config, and an enrolled service account")

// listenFailure reports whether a start failure came from binding a listener.
func listenFailure(err error) bool { return errors.Is(err, errListen) }

// runSubcommand dispatches the diagnostic subcommands and reports whether the
// arguments named one.
func runSubcommand(ctx context.Context, args []string, output, errors io.Writer) (int, bool) {
	if len(args) == 0 {
		return 0, false
	}
	switch args[0] {
	case "health":
		return health(ctx, args[1:], output, errors), true
	case "why-denied":
		return whyDenied(ctx, args[1:], output, errors), true
	default:
		return 0, false
	}
}

// serveOptions carries the parsed server flags. Diagnostic subcommands use
// their own parsers; this one describes the long-running emulator only.
type serveOptions struct {
	listen, grpcListen, metadataListen string
	shutdown                           time.Duration
	iamPath, identityPath              string
	services, pubsubJava, pubsubJAR    string
	metadataPrincipal                  string
	showVersion                        bool
}

// parseServeFlags reads the server flags and rejects unknown or invalid input.
func parseServeFlags(args []string, errors io.Writer) (serveOptions, bool) {
	flags := flag.NewFlagSet("gcp-local", flag.ContinueOnError)
	flags.SetOutput(errors)
	var options serveOptions
	flags.StringVar(&options.listen, "listen", "127.0.0.1:4566", "HTTP listen address")
	flags.StringVar(&options.grpcListen, "grpc-listen", "127.0.0.1:4567", "gRPC listen address")
	flags.DurationVar(&options.shutdown, "shutdown-timeout", 10*time.Second, "maximum duration to drain requests")
	flags.StringVar(&options.iamPath, "iam-config", "", "owner-only local hierarchy and IAM policy configuration")
	flags.StringVar(&options.services, "services", "", "comma-separated explicit services: storage,pubsub")
	flags.StringVar(&options.pubsubJava, "pubsub-java", "", "absolute Java executable for the official emulator")
	flags.StringVar(&options.pubsubJAR, "pubsub-jar", "", "absolute checksum-pinned Pub/Sub emulator JAR")
	flags.StringVar(&options.identityPath, "identity-config", "", "owner-only local identity configuration file")
	flags.StringVar(&options.metadataListen, "metadata-listen", "", "optional per-workload loopback metadata listener")
	flags.StringVar(&options.metadataPrincipal, "metadata-principal", "", "enrolled service account bound to the metadata listener")
	flags.BoolVar(&options.showVersion, "version", false, "print the build version")
	if flags.Parse(args) != nil || flags.NArg() != 0 || options.shutdown <= 0 {
		fmt.Fprintln(errors, "Supply valid flags and a positive shutdown timeout")
		return serveOptions{}, false
	}
	return options, true
}

// assemble wires identity, IAM, providers, and listeners from one options set.
// Listeners already bound on failure are closed before returning.
func assemble(ctx context.Context, options serveOptions) (*core.Server, core.Listeners, error) {
	resolver, identities, metadataHandler, err := assembleIdentity(options)
	if err != nil {
		return nil, core.Listeners{}, err
	}
	registry := gateway.NewRegistry()
	server := core.WithGateway(version, options.shutdown, gateway.New(registry, resolver))
	if identities != nil {
		server.Identity = identities
	}
	if options.iamPath != "" {
		engine, err := assembleIAM(ctx, options, registry, server, identities)
		if err != nil {
			return nil, core.Listeners{}, err
		}
		server.IAM = engine
	}
	listeners, err := bindListeners(ctx, options, server, metadataHandler)
	if err != nil {
		return nil, core.Listeners{}, err
	}
	return server, listeners, nil
}

// assembleIdentity loads the local identity service and metadata handler.
func assembleIdentity(options serveOptions) (gateway.Resolver, *identity.Service, http.Handler, error) {
	if options.identityPath == "" {
		return nil, nil, nil, nil
	}
	config, err := identity.Load(options.identityPath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("load the identity configuration: %w", err)
	}
	identities, err := identity.New(config, nil)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("build the identity service: %w", err)
	}
	handler, err := metadataHandler(identities, options)
	if err != nil {
		return nil, nil, nil, err
	}
	return identities.Resolve, identities, handler, nil
}

// metadataHandler validates the metadata listener options and builds the handler.
func metadataHandler(identities *identity.Service, options serveOptions) (http.Handler, error) {
	if options.metadataListen == "" && options.metadataPrincipal == "" {
		return nil, nil //nolint:nilnil // "no metadata listener configured" is the valid empty case
	}
	host, _, err := net.SplitHostPort(options.metadataListen)
	ip := net.ParseIP(host)
	if err != nil || ip == nil || !ip.IsLoopback() || options.metadataPrincipal == "" {
		return nil, errMetadataRequirements
	}
	handler, err := identities.Metadata(options.metadataPrincipal)
	if err != nil {
		return nil, fmt.Errorf("build the metadata handler: %w", err)
	}
	return handler, nil
}

// assembleIAM loads the IAM engine and registers the selected providers.
func assembleIAM(ctx context.Context, options serveOptions, registry *gateway.Registry, server *core.Server, identities *identity.Service) (*iam.Engine, error) {
	engine, index, err := iam.Load(options.iamPath)
	if err != nil {
		return nil, fmt.Errorf("load the IAM configuration: %w", err)
	}
	selected := map[string]bool{}
	for service := range strings.SplitSeq(options.services, ",") {
		if service != "" {
			selected[service] = true
		}
	}
	if err := assembleServices(ctx, options, selected, registry, server, index, engine, identities); err != nil {
		return nil, err
	}
	return engine, nil
}

// assembleServices registers each selected provider with its dependencies.
func assembleServices(ctx context.Context, options serveOptions, selected map[string]bool, registry *gateway.Registry, server *core.Server, index *state.Index, engine *iam.Engine, identities *identity.Service) error {
	if selected["storage"] {
		if _, err := storage.New(registry, server.Backends, index, engine); err != nil {
			return fmt.Errorf("register the Storage provider: %w", err)
		}
	}
	if selected["pubsub"] {
		tokens, err := identity.NewIDTokens(engine, index)
		if err != nil {
			return fmt.Errorf("mint the ID-token keys: %w", err)
		}
		identities.IDTokens = tokens
		config := pubsub.Config{Java: options.pubsubJava, JAR: options.pubsubJAR, IDTokens: tokens}
		if _, err := pubsub.New(registry, server.Backends, index, engine, config); err != nil {
			return fmt.Errorf("register the Pub/Sub provider: %w", err)
		}
	}
	if selected["run"] {
		// The container runtime is provider-owned; restricted hosts refuse launch.
		if _, err := runprovider.New(ctx, registry, server.Backends, index, engine, runprovider.NoopRuntime{}); err != nil {
			return fmt.Errorf("register the Cloud Run provider: %w", err)
		}
	}
	return nil
}

// bindListeners binds HTTP, gRPC, and optionally the metadata listener. Any
// already-bound listener is closed when a later bind fails.
func bindListeners(ctx context.Context, options serveOptions, server *core.Server, metadataHandler http.Handler) (core.Listeners, error) {
	var listen net.ListenConfig
	listener, err := listen.Listen(ctx, "tcp", options.listen)
	if err != nil {
		return core.Listeners{}, fmt.Errorf("unable to bind the requested HTTP listener: %w: %w", err, errListen)
	}
	grpcListener, err := listen.Listen(ctx, "tcp", options.grpcListen)
	if err != nil {
		_ = listener.Close()
		return core.Listeners{}, fmt.Errorf("unable to bind the requested gRPC listener: %w: %w", err, errListen)
	}
	listeners := core.Listeners{HTTP: listener, GRPC: grpcListener}
	if metadataHandler == nil {
		return listeners, nil
	}
	listeners.Metadata, err = listen.Listen(ctx, "tcp", options.metadataListen)
	if err != nil {
		_ = listener.Close()
		_ = grpcListener.Close()
		return core.Listeners{}, fmt.Errorf("unable to bind the requested metadata listener: %w: %w", err, errListen)
	}
	server.Metadata = &http.Server{Handler: metadataHandler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
	return listeners, nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}
