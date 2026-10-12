package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/dataplane"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/localtls"
	artifactregistryprovider "github.com/evanmarshall/gcp-local/internal/providers/artifactregistry"
	computeprovider "github.com/evanmarshall/gcp-local/internal/providers/compute"
	bigqueryprovider "github.com/evanmarshall/gcp-local/internal/providers/databases/bigquery"
	bigtableprovider "github.com/evanmarshall/gcp-local/internal/providers/databases/bigtable"
	cloudsqlprovider "github.com/evanmarshall/gcp-local/internal/providers/databases/cloudsql"
	firestoreprovider "github.com/evanmarshall/gcp-local/internal/providers/databases/firestore"
	memorystoreprovider "github.com/evanmarshall/gcp-local/internal/providers/databases/memorystore"
	spannerprovider "github.com/evanmarshall/gcp-local/internal/providers/databases/spanner"
	loggingprovider "github.com/evanmarshall/gcp-local/internal/providers/logging"
	cloudschedulerprovider "github.com/evanmarshall/gcp-local/internal/providers/messaging/cloudscheduler"
	cloudtasksprovider "github.com/evanmarshall/gcp-local/internal/providers/messaging/cloudtasks"
	"github.com/evanmarshall/gcp-local/internal/providers/messaging/pubsub"
	cloudkmsprovider "github.com/evanmarshall/gcp-local/internal/providers/security/cloudkms"
	iamadminprovider "github.com/evanmarshall/gcp-local/internal/providers/security/iam"
	resourcemanagerprovider "github.com/evanmarshall/gcp-local/internal/providers/security/resourcemanager"
	secretmanagerprovider "github.com/evanmarshall/gcp-local/internal/providers/security/secretmanager"
	"github.com/evanmarshall/gcp-local/internal/providers/storage"
	vertexaiprovider "github.com/evanmarshall/gcp-local/internal/providers/vertexai"
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
	if listeners.GRPCTLS != nil {
		logger.Info("gRPC TLS listening", "address", listeners.GRPCTLS.Addr().String(), "ca", filepath.Join(options.grpcTLSDir, localtls.CAFile))
	}
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
	grpcTLSListen, grpcTLSDir          string
	shutdown                           time.Duration
	iamPath, identityPath              string
	runSocket, runNetwork, runEnv      string
	runProcess                         map[string]string
	services, pubsubJava, pubsubJAR    string
	firestoreJava, firestoreJAR        string
	spannerEmulator, spannerLibraries  string
	redisServer                        string
	cloudsql                           cloudSQLOptions
	dataPorts, dataListen              string
	dataPlane                          *dataplane.Pool
	vertex                             vertexaiprovider.Config
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
	flags.StringVar(&options.grpcTLSListen, "grpc-tls-listen", "", "optional gRPC TLS listen address; requires -grpc-tls-dir")
	flags.StringVar(&options.grpcTLSDir, "grpc-tls-dir", "", "owner-only directory holding the gRPC TLS CA and server key; created with new material when missing")
	flags.DurationVar(&options.shutdown, "shutdown-timeout", 10*time.Second, "maximum duration to drain requests")
	flags.StringVar(&options.iamPath, "iam-config", "", "owner-only local hierarchy and IAM policy configuration")
	flags.StringVar(&options.services, "services", "", "comma-separated explicit services: storage,pubsub,kms,firestore,bigquery,spanner,bigtable,artifactregistry,resourcemanager,iam,logging,vertexai,cloudtasks,cloudscheduler")
	flags.StringVar(&options.vertex.Backend, "vertex-backend", "stub", "Vertex AI model backend: stub (deterministic, default) or ollama")
	flags.StringVar(&options.vertex.OllamaURL, "vertex-ollama-url", "", "Ollama server URL for -vertex-backend=ollama, e.g. http://127.0.0.1:11434")
	flags.Var(modelMap{&options.vertex.Models}, "vertex-model", "Vertex AI model ID=Ollama model, e.g. gemini-2.5-flash=llama3.2; * matches any ID; repeatable")
	flags.StringVar(&options.pubsubJava, "pubsub-java", "", "absolute Java executable for the official emulator")
	flags.StringVar(&options.pubsubJAR, "pubsub-jar", "", "absolute checksum-pinned Pub/Sub emulator JAR")
	flags.StringVar(&options.firestoreJava, "firestore-java", "", "absolute Java executable for the official Firestore emulator")
	flags.StringVar(&options.firestoreJAR, "firestore-jar", "", "absolute checksum-pinned Firestore emulator JAR")
	flags.StringVar(&options.spannerEmulator, "spanner-emulator", "", "absolute checksum-pinned Spanner emulator_main binary (Linux only)")
	flags.StringVar(&options.spannerLibraries, "spanner-libraries", "", "absolute directory holding the emulator's pinned libstdc++.so.6")
	flags.StringVar(&options.redisServer, "redis-server", "", "absolute redis-server built by scripts/install/redis.py (Memorystore)")
	flags.StringVar(&options.cloudsql.postgres, "cloudsql-postgres", "", "absolute root of the checksum-pinned PostgreSQL installs (Linux only)")
	flags.StringVar(&options.cloudsql.mysql, "cloudsql-mysql", "", "absolute root of the checksum-pinned MySQL installs (Linux only)")
	flags.StringVar(&options.cloudsql.runAs, "cloudsql-run-as", "", "uid:gid the PostgreSQL and MySQL servers run as; required when gcp-local runs as root")
	flags.Var(connectorList{&options.cloudsql.connectors}, "cloudsql-connector", "project:region:instance,principal,loopback-address; repeatable")
	flags.StringVar(&options.dataPorts, "data-ports", "", "FIRST-LAST port range that publishes Redis and public-IP Cloud SQL instances; empty publishes none")
	flags.StringVar(&options.dataListen, "data-listen", "127.0.0.1", "IP address the data-plane ports bind")
	flags.StringVar(&options.identityPath, "identity-config", "", "owner-only local identity configuration file")
	flags.StringVar(&options.runSocket, "run-docker-socket", "", "Docker engine socket for Cloud Run containers; empty refuses launch")
	flags.StringVar(&options.runNetwork, "run-docker-network", "", "Docker network revision containers join; empty publishes their ports on the default bridge")
	flags.StringVar(&options.runEnv, "run-docker-environment", "", "label value that marks this core's revision containers; swept at startup and shutdown")
	flags.Var(processMap{&options.runProcess}, "run-process", "dockerless revision launch: image=command; repeatable; commands run as local processes")
	flags.StringVar(&options.metadataListen, "metadata-listen", "", "optional per-workload loopback metadata listener")
	flags.StringVar(&options.metadataPrincipal, "metadata-principal", "", "enrolled service account bound to the metadata listener")
	flags.BoolVar(&options.showVersion, "version", false, "print the build version")
	if flags.Parse(args) != nil || flags.NArg() != 0 || options.shutdown <= 0 || (options.grpcTLSListen == "") != (options.grpcTLSDir == "") {
		fmt.Fprintln(errors, "Supply valid flags, a positive shutdown timeout, and -grpc-tls-listen together with -grpc-tls-dir")
		return serveOptions{}, false
	}
	pool, err := dataplane.Parse(options.dataPorts, options.dataListen)
	if err != nil {
		fmt.Fprintln(errors, err)
		return serveOptions{}, false
	}
	options.dataPlane = pool
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
	var tlsConfig *tls.Config
	if options.grpcTLSDir != "" {
		if tlsConfig, err = localtls.Load(options.grpcTLSDir, time.Now()); err != nil {
			return nil, core.Listeners{}, fmt.Errorf("load the gRPC TLS material: %w", err)
		}
	}
	listeners, err := bindListeners(ctx, options, server, metadataHandler, tlsConfig)
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
	tokens, err := assembleIDTokens(selected, index, engine, identities)
	if err != nil {
		return err
	}
	if err := assembleDelivery(ctx, options, selected, registry, server, index, engine, identities, tokens); err != nil {
		return err
	}
	if err := assembleIAMProvider(ctx, selected, registry, server, index, engine, iamadminprovider.Credentials{Tokens: identities, IDTokens: tokens}); err != nil {
		return err
	}
	// Providers in front of an official or pinned backend binary, or a
	// configured model backend, take their backend settings from the serve
	// options.
	for _, assemble := range []func(context.Context, serveOptions, map[string]bool, *gateway.Registry, *core.Server, *state.Index, *iam.Engine) error{
		assembleFirestore, assembleSpanner, assembleMemorystore, assembleCloudSQLService, assembleVertexAI,
	} {
		if err := assemble(ctx, options, selected, registry, server, index, engine); err != nil {
			return err
		}
	}
	if err := assembleCompute(ctx, selected, registry, server, index, engine); err != nil {
		return err
	}
	if err := assembleStandard(ctx, selected, registry, server, index, engine); err != nil {
		return err
	}
	return assembleRun(ctx, options, selected, registry, server, index, engine, identities)
}

// assembleIAMProvider registers the IAM and IAM Credentials provider with the
// identity service and ID-token keys it mints with.
func assembleIAMProvider(ctx context.Context, selected map[string]bool, registry *gateway.Registry, server *core.Server, index *state.Index, engine *iam.Engine, credentials iamadminprovider.Credentials) error {
	if !selected["iam"] {
		return nil
	}
	if _, err := iamadminprovider.New(ctx, registry, server.Backends, index, engine, credentials); err != nil {
		return fmt.Errorf("register the IAM provider: %w", err)
	}
	return nil
}

// standardProvider is a provider whose constructor needs only the shared
// registry, backend manager, index, and IAM engine.
type standardProvider struct {
	selection string // the --services name
	label     string // the provider name in registration errors
	register  func(context.Context, *gateway.Registry, *backend.Manager, *state.Index, *iam.Engine) error
}

// standardProviders registers in this order after the backend-configured ones.
var standardProviders = []standardProvider{
	{"secretmanager", "Secret Manager", discard(secretmanagerprovider.New)},
	{"logging", "Logging", discard(loggingprovider.New)},
	{"kms", "Cloud KMS", discard(cloudkmsprovider.New)},
	{"bigquery", "BigQuery", discard(bigqueryprovider.New)},
	{"bigtable", "Bigtable", discard(bigtableprovider.New)},
	{"artifactregistry", "Artifact Registry", discard(artifactregistryprovider.New)},
	{"resourcemanager", "resourcemanager", discard(resourcemanagerprovider.New)},
}

// assembleStandard registers the selected standard providers.
func assembleStandard(ctx context.Context, selected map[string]bool, registry *gateway.Registry, server *core.Server, index *state.Index, engine *iam.Engine) error {
	for _, service := range standardProviders {
		if !selected[service.selection] {
			continue
		}
		if err := service.register(ctx, registry, server.Backends, index, engine); err != nil {
			return fmt.Errorf("register the %s provider: %w", service.label, err)
		}
	}
	return nil
}

// discard adapts a provider constructor whose provider value the caller does
// not keep: registration with the gateway is its only effect.
func discard[P any](construct func(context.Context, *gateway.Registry, *backend.Manager, *state.Index, *iam.Engine) (P, error)) func(context.Context, *gateway.Registry, *backend.Manager, *state.Index, *iam.Engine) error {
	return func(ctx context.Context, registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine) error {
		_, err := construct(ctx, registry, manager, index, engine)
		return err
	}
}

// assembleIDTokens builds the one local ID-token key set that Pub/Sub push,
// Cloud Tasks, Cloud Scheduler, and IAM Credentials generateIdToken mint with
// and Cloud Run invocation verifies. The minting method registers a single
// permission plan, so the key set is shared, not per provider.
func assembleIDTokens(selected map[string]bool, index *state.Index, engine *iam.Engine, identities *identity.Service) (*identity.IDTokens, error) {
	if !selected["pubsub"] && !selected["cloudtasks"] && !selected["cloudscheduler"] && !selected["iam"] {
		return nil, nil //nolint:nilnil // no minting provider selected
	}
	tokens, err := identity.NewIDTokens(engine, index)
	if err != nil {
		return nil, fmt.Errorf("mint the ID-token keys: %w", err)
	}
	if identities != nil {
		identities.IDTokens = tokens
	}
	return tokens, nil
}

// assemblePubSub registers the Pub/Sub provider with the shared ID-token keys.
func assemblePubSub(options serveOptions, selected map[string]bool, registry *gateway.Registry, server *core.Server, index *state.Index, engine *iam.Engine, tokens *identity.IDTokens) error {
	if !selected["pubsub"] {
		return nil
	}
	config := pubsub.Config{Java: options.pubsubJava, JAR: options.pubsubJAR, IDTokens: tokens}
	if _, err := pubsub.New(registry, server.Backends, index, engine, config); err != nil {
		return fmt.Errorf("register the Pub/Sub provider: %w", err)
	}
	return nil
}

// assembleCompute registers the networking control-plane provider.
func assembleCompute(ctx context.Context, selected map[string]bool, registry *gateway.Registry, server *core.Server, index *state.Index, engine *iam.Engine) error {
	if !selected["compute"] {
		return nil
	}
	provider, err := computeprovider.New(ctx, registry, server.Backends, index, engine)
	if err != nil {
		return fmt.Errorf("register the Compute provider: %w", err)
	}
	server.Analyzer = provider.Analyzer()
	return nil
}

// assembleCloudTasks registers the Cloud Tasks provider and its dispatcher.
func assembleCloudTasks(ctx context.Context, selected map[string]bool, registry *gateway.Registry, server *core.Server, index *state.Index, engine *iam.Engine, tokens *identity.IDTokens) error {
	if !selected["cloudtasks"] {
		return nil
	}
	if _, err := cloudtasksprovider.New(ctx, cloudtasksprovider.DeliveryConfig{Tokens: tokens}, registry, server.Backends, index, engine); err != nil {
		return fmt.Errorf("register the Cloud Tasks provider: %w", err)
	}
	return nil
}

// assembleDelivery registers the providers whose workers deliver as service
// agents with the shared ID-token keys: Pub/Sub, Cloud Tasks, and Cloud
// Scheduler, which also mints OAuth tokens through the identity service.
func assembleDelivery(ctx context.Context, options serveOptions, selected map[string]bool, registry *gateway.Registry, server *core.Server, index *state.Index, engine *iam.Engine, identities *identity.Service, tokens *identity.IDTokens) error {
	if err := assemblePubSub(options, selected, registry, server, index, engine, tokens); err != nil {
		return err
	}
	if err := assembleCloudTasks(ctx, selected, registry, server, index, engine, tokens); err != nil {
		return err
	}
	if !selected["cloudscheduler"] {
		return nil
	}
	config := cloudschedulerprovider.Config{IDTokens: tokens, Identity: identities}
	if _, err := cloudschedulerprovider.New(ctx, config, registry, server.Backends, index, engine); err != nil {
		return fmt.Errorf("register the Cloud Scheduler provider: %w", err)
	}
	return nil
}

// assembleSpanner registers the Spanner provider in front of the official emulator.
func assembleSpanner(ctx context.Context, options serveOptions, selected map[string]bool, registry *gateway.Registry, server *core.Server, index *state.Index, engine *iam.Engine) error {
	if !selected["spanner"] {
		return nil
	}
	config := spannerprovider.Config{Emulator: options.spannerEmulator, Libraries: options.spannerLibraries}
	if _, err := spannerprovider.New(ctx, registry, server.Backends, index, engine, config); err != nil {
		return fmt.Errorf("register the Spanner provider: %w", err)
	}
	return nil
}

// assembleMemorystore registers the Memorystore for Redis provider ("redis").
func assembleMemorystore(ctx context.Context, options serveOptions, selected map[string]bool, registry *gateway.Registry, server *core.Server, index *state.Index, engine *iam.Engine) error {
	if !selected["redis"] {
		return nil
	}
	config := memorystoreprovider.Config{Server: options.redisServer, Publish: options.dataPlane}
	if _, err := memorystoreprovider.New(ctx, registry, server.Backends, index, engine, config); err != nil {
		return fmt.Errorf("register the Memorystore provider: %w", err)
	}
	return nil
}

// assembleFirestore registers the Firestore provider and its emulator paths.
func assembleFirestore(ctx context.Context, options serveOptions, selected map[string]bool, registry *gateway.Registry, server *core.Server, index *state.Index, engine *iam.Engine) error {
	if !selected["firestore"] {
		return nil
	}
	config := firestoreprovider.Config{Java: options.firestoreJava, JAR: options.firestoreJAR}
	if _, err := firestoreprovider.New(ctx, registry, server.Backends, index, engine, config); err != nil {
		return fmt.Errorf("register the Firestore provider: %w", err)
	}
	return nil
}

// runBaseURL returns the base of local service URIs for the HTTP listener.
// A wildcard listener (the packaged container binds 0.0.0.0) is named by its
// loopback address: the push agent and revision processes dial service URIs
// from the core's own network namespace, and the packaged environment
// publishes the same port on host loopback.
func runBaseURL(listen string) string {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "http://" + listen
	}
	if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
		host = loopbackHost
	}
	return "http://" + net.JoinHostPort(host, port)
}

// bindListeners binds HTTP, gRPC, and optionally the gRPC TLS and metadata
// listeners. Any already-bound listener is closed when a later bind fails.
func bindListeners(ctx context.Context, options serveOptions, server *core.Server, metadataHandler http.Handler, tlsConfig *tls.Config) (core.Listeners, error) {
	var listen net.ListenConfig
	var listeners core.Listeners
	for _, bind := range []struct {
		name, address string
		target        *net.Listener
		enabled       bool
	}{
		{"HTTP", options.listen, &listeners.HTTP, true},
		{"gRPC", options.grpcListen, &listeners.GRPC, true},
		{"gRPC TLS", options.grpcTLSListen, &listeners.GRPCTLS, tlsConfig != nil},
		{"metadata", options.metadataListen, &listeners.Metadata, metadataHandler != nil},
	} {
		if !bind.enabled {
			continue
		}
		listener, err := listen.Listen(ctx, "tcp", bind.address)
		if err != nil {
			for _, bound := range []net.Listener{listeners.HTTP, listeners.GRPC, listeners.GRPCTLS} {
				if bound != nil {
					_ = bound.Close()
				}
			}
			return core.Listeners{}, fmt.Errorf("unable to bind the requested %s listener: %w: %w", bind.name, err, errListen)
		}
		*bind.target = listener
	}
	if tlsConfig != nil {
		listeners.GRPCTLS = tls.NewListener(listeners.GRPCTLS, tlsConfig)
	}
	if metadataHandler != nil {
		server.Metadata = &http.Server{Handler: metadataHandler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
	}
	return listeners, nil
}

func main() {
	bigqueryprovider.RequireUTC()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

// errRunProcessShape rejects a malformed run-process flag value.
var errRunProcessShape = errors.New("run-process expects image=command")

// Vertex AI flag sentinels.
var (
	errVertexModelShape = errors.New("vertex-model expects VERTEX_MODEL_ID=OLLAMA_MODEL")
	errVertexUnselected = errors.New("vertex AI backend flags require vertexai in -services")
)

// modelMap collects repeatable Vertex AI model ID=Ollama model flags.
type modelMap struct {
	target *map[string]string
}

func (m modelMap) String() string { return "" }

func (m modelMap) Set(value string) error {
	model, alias, found := strings.Cut(value, "=")
	if !found || model == "" || alias == "" {
		return errVertexModelShape
	}
	if *m.target == nil {
		*m.target = map[string]string{}
	}
	(*m.target)[model] = alias
	return nil
}

// assembleVertexAI registers the Vertex AI provider with its explicitly
// configured model backend.
func assembleVertexAI(ctx context.Context, options serveOptions, selected map[string]bool, registry *gateway.Registry, _ *core.Server, index *state.Index, engine *iam.Engine) error {
	if !selected["vertexai"] {
		if options.vertex.OllamaURL != "" || len(options.vertex.Models) > 0 || (options.vertex.Backend != "" && options.vertex.Backend != vertexaiprovider.BackendStub) {
			return errVertexUnselected
		}
		return nil
	}
	if _, err := vertexaiprovider.New(ctx, registry, index, engine, options.vertex); err != nil {
		return fmt.Errorf("register the Vertex AI provider: %w", err)
	}
	return nil
}

// processMap collects repeatable image=command flags.
type processMap struct {
	target *map[string]string
}

func (m processMap) String() string {
	if m.target == nil {
		return ""
	}
	return ""
}

func (m processMap) Set(value string) error {
	image, command, found := strings.Cut(value, "=")
	if !found || image == "" || command == "" {
		return errRunProcessShape
	}
	if *m.target == nil {
		*m.target = map[string]string{}
	}
	(*m.target)[image] = command
	return nil
}

// cloudSQLOptions carries the Cloud SQL backend and connector flags.
type cloudSQLOptions struct {
	postgres, mysql, runAs string
	connectors             []string
	publish                *dataplane.Pool
}

// errCloudSQLFlags rejects malformed Cloud SQL flag values.
var errCloudSQLFlags = errors.New("cloudsql-run-as expects uid:gid and cloudsql-connector expects connection,principal,address")

// connectorList collects repeatable connector specifications.
type connectorList struct{ target *[]string }

func (c connectorList) String() string { return "" }

func (c connectorList) Set(value string) error {
	*c.target = append(*c.target, value)
	return nil
}

// assembleCloudSQLService adapts assembleCloudSQL to the backend assembly loop.
func assembleCloudSQLService(ctx context.Context, options serveOptions, selected map[string]bool, registry *gateway.Registry, server *core.Server, index *state.Index, engine *iam.Engine) error {
	cloudsql := options.cloudsql
	cloudsql.publish = options.dataPlane
	return assembleCloudSQL(ctx, cloudsql, selected, registry, server, index, engine)
}

// assembleCloudSQL registers the Cloud SQL provider and opens its connectors.
func assembleCloudSQL(ctx context.Context, options cloudSQLOptions, selected map[string]bool, registry *gateway.Registry, server *core.Server, index *state.Index, engine *iam.Engine) error {
	if !selected["cloudsql"] {
		return nil
	}
	config := cloudsqlprovider.Config{Root: options.postgres, MySQLRoot: options.mysql, Publish: options.publish}
	if options.runAs != "" {
		uid, gid, found := strings.Cut(options.runAs, ":")
		user, userErr := strconv.ParseUint(uid, 10, 32)
		group, groupErr := strconv.ParseUint(gid, 10, 32)
		if !found || userErr != nil || groupErr != nil {
			return errCloudSQLFlags
		}
		config.RunAs = &syscall.Credential{Uid: uint32(user), Gid: uint32(group)}
	}
	provider, err := cloudsqlprovider.New(ctx, registry, server.Backends, index, engine, config)
	if err != nil {
		return fmt.Errorf("register the Cloud SQL provider: %w", err)
	}
	for _, value := range options.connectors {
		parts := strings.SplitN(value, ",", 3)
		if len(parts) != 3 {
			return errCloudSQLFlags
		}
		spec := cloudsqlprovider.ConnectorSpec{ConnectionName: parts[0], Principal: parts[1], Address: parts[2]}
		if _, err := provider.OpenConnector(ctx, spec); err != nil {
			return fmt.Errorf("open the Cloud SQL connector for %s: %w", parts[0], err)
		}
	}
	return nil
}
