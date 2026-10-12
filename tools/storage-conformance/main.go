//go:build conformance

// Command storage-conformance is only a test credential adapter. All API,
// authorization, state, and backend behavior uses the production implementation.
// Fixed fixture tokens are deliberately absent from cmd/gcp-local.
package main

import (
	"sync"
	"fmt"
	"context"
	"errors"
	"encoding/json"
	"flag"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	artifactregistryprovider "github.com/evanmarshall/gcp-local/internal/providers/artifactregistry"
	bigqueryprovider "github.com/evanmarshall/gcp-local/internal/providers/bigquery"
	bigtableprovider "github.com/evanmarshall/gcp-local/internal/providers/bigtable"
	"github.com/evanmarshall/gcp-local/internal/providers/cloudkms"
	cloudsqlprovider "github.com/evanmarshall/gcp-local/internal/providers/cloudsql"
	cloudtasksprovider "github.com/evanmarshall/gcp-local/internal/providers/cloudtasks"
	computeprovider "github.com/evanmarshall/gcp-local/internal/providers/compute"
	firestoreprovider "github.com/evanmarshall/gcp-local/internal/providers/firestore"
	memorystoreprovider "github.com/evanmarshall/gcp-local/internal/providers/memorystore"
	iamadminprovider "github.com/evanmarshall/gcp-local/internal/providers/iam"
	pubsubprovider "github.com/evanmarshall/gcp-local/internal/providers/pubsub"
	resourcemanagerprovider "github.com/evanmarshall/gcp-local/internal/providers/resourcemanager"
	runprovider "github.com/evanmarshall/gcp-local/internal/providers/run"
	secretmanager "github.com/evanmarshall/gcp-local/internal/providers/secretmanager"
	spannerprovider "github.com/evanmarshall/gcp-local/internal/providers/spanner"
	"github.com/evanmarshall/gcp-local/internal/providers/storage"
	core "github.com/evanmarshall/gcp-local/internal/runtime"
	"github.com/evanmarshall/gcp-local/internal/state"
)

func main() {
	trace := flag.Bool("trace", false, "Log local request methods, paths and status codes only")
	fault := flag.Bool("fault-rpo", false, "Inject incorrect bucket RPO to verify acceptance-test failures")
	flag.Parse()
	interfaces, err := net.Interfaces()
	if err != nil || len(interfaces) != 1 || interfaces[0].Name != "lo" || os.Getenv("GCP_LOCAL_ISOLATED") != "1" {
		log.Fatal("Run the conformance fixture inside scripts/run_isolated.py")
	}
	index := state.NewIndex()
	engine, err := iam.New(index)
	must(err)
	for i, project := range []string{"gcp-local-test", "synthetic-peer"} {
		name := "//cloudresourcemanager.googleapis.com/projects/" + project
		number := "000000000001"
		if i == 1 {
			number = "000000000002"
		}
		must(index.Put("resourcemanager", state.Resource{Name: name, Service: "resourcemanager", Kind: "project", Number: number, Scope: state.Scope{Project: project, Location: state.Global}}))
		_, err := engine.SetPolicy(name, iam.Policy{Bindings: []iam.Binding{
			{Role: "roles/storage.admin", Members: []string{"serviceAccount:fixture@" + project + ".iam.gserviceaccount.com"}},
			// The KMS acceptance case drives key rings and crypto keys with
			// the same fixture principal.
			{Role: "roles/cloudkms.admin", Members: []string{"serviceAccount:fixture@" + project + ".iam.gserviceaccount.com"}},
			{Role: "roles/secretmanager.admin", Members: []string{"serviceAccount:fixture@" + project + ".iam.gserviceaccount.com"}},
			{Role: "roles/cloudtasks.admin", Members: []string{"serviceAccount:fixture@" + project + ".iam.gserviceaccount.com"}},
			{Role: "roles/datastore.user", Members: []string{"serviceAccount:fixture@" + project + ".iam.gserviceaccount.com"}},
			{Role: "roles/datastore.indexAdmin", Members: []string{"serviceAccount:fixture@" + project + ".iam.gserviceaccount.com"}},
			{Role: "roles/bigquery.dataOwner", Members: []string{"serviceAccount:fixture@" + project + ".iam.gserviceaccount.com"}},
			{Role: "roles/bigquery.jobUser", Members: []string{"serviceAccount:fixture@" + project + ".iam.gserviceaccount.com"}},
			{Role: "roles/spanner.admin", Members: []string{"serviceAccount:fixture@" + project + ".iam.gserviceaccount.com"}},
			{Role: "roles/pubsub.admin", Members: []string{"serviceAccount:fixture@" + project + ".iam.gserviceaccount.com"}},
			{Role: "roles/run.admin", Members: []string{"serviceAccount:fixture@" + project + ".iam.gserviceaccount.com"}},
			{Role: "roles/compute.admin", Members: []string{"serviceAccount:fixture@" + project + ".iam.gserviceaccount.com"}},
			{Role: "roles/networkmanagement.admin", Members: []string{"serviceAccount:fixture@" + project + ".iam.gserviceaccount.com"}},
			{Role: "roles/artifactregistry.admin", Members: []string{"serviceAccount:fixture@" + project + ".iam.gserviceaccount.com"}},
			{Role: "roles/bigtable.admin", Members: []string{"serviceAccount:fixture@" + project + ".iam.gserviceaccount.com"}},
			{Role: "roles/redis.admin", Members: []string{"serviceAccount:fixture@" + project + ".iam.gserviceaccount.com"}},
			{Role: "roles/resourcemanager.projectIamAdmin", Members: []string{"serviceAccount:fixture@" + project + ".iam.gserviceaccount.com"}},
			{Role: "roles/iam.serviceAccountAdmin", Members: []string{"serviceAccount:fixture@" + project + ".iam.gserviceaccount.com"}},
			{Role: "roles/iam.serviceAccountKeyAdmin", Members: []string{"serviceAccount:fixture@" + project + ".iam.gserviceaccount.com"}},
		}})
		must(err)
	}
	// The scenario identities exist as service accounts, so actAs checks on
	// them (Cloud Tasks OIDC) evaluate rather than reporting a coverage gap.
	for i, actor := range []string{"fixture", "allowed", "denied"} {
		must(index.Put("iam", state.Resource{
			Name: iam.AccountResource("gcp-local-test", actor+"@gcp-local-test.iam.gserviceaccount.com"), Service: "iam",
			Kind: "serviceAccount", Number: fmt.Sprintf("1000000000000000000%02d", i),
			Parent: "//cloudresourcemanager.googleapis.com/projects/gcp-local-test",
			Scope:  state.Scope{Project: "gcp-local-test", Location: state.Global},
		}))
	}
	resolver := func(ctx context.Context, token string) (gateway.Principal, error) {
		for _, actor := range []string{"fixture", "allowed", "denied", "peer"} {
			if token == "gcp-local-test:"+actor {
				project := "gcp-local-test"
				if actor == "peer" {
					project = "synthetic-peer"
				}
				return gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:" + actor + "@" + project + ".iam.gserviceaccount.com", Project: project, Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
			}
		}
		return gateway.Principal{State: gateway.Unknown}, nil
	}
	registry := gateway.NewRegistry()
	server := core.WithGateway("conformance", 5*time.Second, gateway.New(registry, resolver))
	server.IAM = engine
	_, err = storage.New(registry, server.Backends, index, engine)
	must(err)
	_, err = cloudkms.New(context.Background(), registry, server.Backends, index, engine)
	must(err)
	_, err = secretmanager.New(context.Background(), registry, server.Backends, index, engine)
	must(err)
	_, err = bigqueryprovider.New(context.Background(), registry, server.Backends, index, engine)
	must(err)
	spannerEmulator, spannerLibraries := os.Getenv("GCP_LOCAL_SPANNER_EMULATOR"), os.Getenv("GCP_LOCAL_SPANNER_LIBRARIES")
	if spannerEmulator == "" || spannerLibraries == "" {
		log.Fatal("The conformance fixture needs GCP_LOCAL_SPANNER_EMULATOR and GCP_LOCAL_SPANNER_LIBRARIES for the Spanner provider")
	}
	_, err = spannerprovider.New(context.Background(), registry, server.Backends, index, engine,
		spannerprovider.Config{Emulator: spannerEmulator, Libraries: spannerLibraries})
	must(err)
	java := os.Getenv("GCP_LOCAL_JAVA")
	jar := os.Getenv("GCP_LOCAL_PUBSUB_JAR")
	if java == "" || jar == "" {
		log.Fatal("The conformance fixture needs GCP_LOCAL_JAVA and GCP_LOCAL_PUBSUB_JAR for the Pub/Sub provider")
	}
	redisServer := os.Getenv("GCP_LOCAL_REDIS_SERVER")
	if redisServer == "" {
		log.Fatal("The conformance fixture needs GCP_LOCAL_REDIS_SERVER for the Memorystore provider")
	}
	_, err = memorystoreprovider.New(context.Background(), registry, server.Backends, index, engine,
		memorystoreprovider.Config{Server: redisServer})
	must(err)
	firestoreJAR := os.Getenv("GCP_LOCAL_FIRESTORE_JAR")
	if firestoreJAR == "" {
		log.Fatal("The conformance fixture needs GCP_LOCAL_JAVA and GCP_LOCAL_FIRESTORE_JAR for the Firestore provider")
	}
	_, err = firestoreprovider.New(context.Background(), registry, server.Backends, index, engine, firestoreprovider.Config{Java: java, JAR: firestoreJAR})
	must(err)
	tokens, tokenErr := identity.NewIDTokens(engine, index)
	must(tokenErr)
	_, err = pubsubprovider.New(registry, server.Backends, index, engine, pubsubprovider.Config{Java: java, JAR: jar, IDTokens: tokens})
	must(err)
	_, err = runprovider.New(context.Background(), runprovider.Config{BaseURL: "http://127.0.0.1:4566"}, registry, server.Backends, index, engine, &servingRuntime{servers: map[string]*http.Server{}})
	must(err)
	_, err = computeprovider.New(context.Background(), registry, server.Backends, index, engine)
	must(err)
	_, err = artifactregistryprovider.New(context.Background(), registry, server.Backends, index, engine)
	must(err)
	_, err = bigtableprovider.New(context.Background(), registry, server.Backends, index, engine)
	must(err)
	_, err = resourcemanagerprovider.New(context.Background(), registry, server.Backends, index, engine)
	must(err)
	_, err = iamadminprovider.New(context.Background(), registry, server.Backends, index, engine)
	must(err)
	_, err = cloudtasksprovider.New(context.Background(), cloudtasksprovider.DeliveryConfig{Tokens: tokens}, registry, server.Backends, index, engine)
	must(err)
	if root := os.Getenv("GCP_LOCAL_POSTGRES_ROOT"); root != "" {
		must(addCloudSQL(root, registry, server, index, engine))
	}
	if *trace || *fault {
		handler := server.HTTP.Handler
		server.HTTP.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			recorded := httptest.NewRecorder()
			handler.ServeHTTP(recorded, r)
			if *trace {
				log.Printf("%s %s %d", r.Method, r.URL.Path, recorded.Code)
			}
			body := recorded.Body.Bytes()
			if *fault && recorded.Code == 200 {
				var value map[string]any
				if json.Unmarshal(body, &value) == nil && value["kind"] == "storage#bucket" {
					value["rpo"] = "ASYNC_TURBO"
					body, _ = json.Marshal(value)
				}
			}
			for key, values := range recorded.Header() {
				w.Header()[key] = values
			}
			w.Header().Del("Content-Length")
			w.WriteHeader(recorded.Code)
			_, _ = w.Write(body)
		})
	}
	http, err := net.Listen("tcp", "127.0.0.1:4566")
	must(err)
	grpc, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	must(server.Serve(ctx, core.Listeners{HTTP: http, GRPC: grpc}))
}
func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// servingRuntime is a test-only Cloud Run runtime: each revision is a loopback
// HTTP server answering 200, so control-plane cases (Terraform) observe real
// readiness without a container engine. Production uses the Docker or process runtime.
type servingRuntime struct {
	mu      sync.Mutex
	servers map[string]*http.Server
}

func (r *servingRuntime) Start(_ context.Context, _ runprovider.StartSpec) (runprovider.Started, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return runprovider.Started{}, fmt.Errorf("listen for the revision: %w", err)
	}
	server := &http.Server{ReadHeaderTimeout: time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})}
	go func() { _ = server.Serve(listener) }()
	id := listener.Addr().String()
	r.mu.Lock()
	r.servers[id] = server
	r.mu.Unlock()
	return runprovider.Started{Address: id, Container: id}, nil
}

func (r *servingRuntime) Ready(context.Context, string) error { return nil }

func (r *servingRuntime) Stop(ctx context.Context, container string) error {
	r.mu.Lock()
	server := r.servers[container]
	delete(r.servers, container)
	r.mu.Unlock()
	if server == nil {
		return nil
	}
	if err := server.Shutdown(ctx); err != nil {
		return fmt.Errorf("stop the revision server: %w", err)
	}
	return nil
}

// addCloudSQL registers the Cloud SQL provider only when its PostgreSQL
// install is supplied, granting the fixture principal roles/cloudsql.admin on
// both projects. Registration fails until the roles/cloudsql.* snapshots are
// recorded, so the default fixture never selects it.
func addCloudSQL(root string, registry *gateway.Registry, server *core.Server, index *state.Index, engine *iam.Engine) error {
	config := cloudsqlprovider.Config{Root: root}
	if uid := os.Getenv("GCP_LOCAL_POSTGRES_UID"); uid != "" {
		user, userErr := strconv.ParseUint(uid, 10, 32)
		group, groupErr := strconv.ParseUint(os.Getenv("GCP_LOCAL_POSTGRES_GID"), 10, 32)
		if userErr != nil || groupErr != nil {
			return fmt.Errorf("parse the PostgreSQL uid/gid: %w", errors.Join(userErr, groupErr))
		}
		config.RunAs = &syscall.Credential{Uid: uint32(user), Gid: uint32(group)}
	}
	if _, err := cloudsqlprovider.New(context.Background(), registry, server.Backends, index, engine, config); err != nil {
		return fmt.Errorf("register the Cloud SQL provider: %w", err)
	}
	for _, project := range []string{"gcp-local-test", "synthetic-peer"} {
		name := "//cloudresourcemanager.googleapis.com/projects/" + project
		policy, err := engine.GetPolicy(name)
		if err != nil {
			return fmt.Errorf("read the fixture policy: %w", err)
		}
		policy.Bindings = append(policy.Bindings, iam.Binding{Role: "roles/cloudsql.admin", Members: []string{"serviceAccount:fixture@" + project + ".iam.gserviceaccount.com"}})
		if _, err := engine.SetPolicy(name, policy); err != nil {
			return fmt.Errorf("grant the fixture Cloud SQL role: %w", err)
		}
	}
	return nil
}
