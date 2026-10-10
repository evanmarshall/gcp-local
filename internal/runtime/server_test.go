package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Sentinels returned by the runtime test handlers.
var (
	errRequestUnfinished = errors.New("request did not finish")
	errProviderReached   = errors.New("diagnostics reached provider")
)

// listenTestAddress opens a loopback listener through the context-aware API.
func listenTestAddress() (net.Listener, error) {
	var listen net.ListenConfig
	return listen.Listen(context.Background(), "tcp", "127.0.0.1:0")
}

// healthPath is the local readiness endpoint asserted across runtime tests.
const healthPath = "/_local/health"

func start(t *testing.T, s *Server) (string, context.CancelFunc, <-chan error) {
	t.Helper()
	listener, err := listenTestAddress()
	if err != nil {
		t.Fatal(err)
	}
	grpcListener, err := listenTestAddress()
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- s.Serve(ctx, Listeners{HTTP: listener, GRPC: grpcListener}) }()
	t.Cleanup(cancel)
	return "http://" + listener.Addr().String(), cancel, result
}

func TestHealthAndUnsupportedAPI(t *testing.T) {
	s := New("test-version", time.Second)
	url, cancel, result := start(t, s)
	client := &http.Client{Timeout: time.Second}
	for _, path := range []string{healthPath, "/_local/diagnostics", "/storage/v1/b"} {
		response, err := client.Get(url + path)
		if err != nil {
			t.Fatal(err)
		}
		var body map[string]any
		err = json.NewDecoder(response.Body).Decode(&body)
		_ = response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if path == "/storage/v1/b" {
			detail, ok := body["error"].(map[string]any)
			if response.StatusCode != http.StatusNotImplemented || !ok || detail["status"] != "UNIMPLEMENTED" {
				t.Fatalf("unexpected API response: %v", body)
			}
		} else if response.StatusCode != http.StatusOK {
			t.Fatalf("diagnostics status %d", response.StatusCode)
		}
		if path == healthPath && (body["ready"] != true || body["version"] != "test-version") {
			t.Fatalf("unexpected health: %v", body)
		}
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if _, err := client.Get(url + healthPath); err == nil {
		t.Fatal("listener remained open")
	}
}

func TestShutdownDrainsActiveRequest(t *testing.T) {
	s := New("test", time.Second)
	entered, release := make(chan struct{}), make(chan struct{})
	s.HTTP.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		_, _ = io.WriteString(w, "finished")
	})
	url, cancel, result := start(t, s)
	response := make(chan error, 1)
	go func() {
		client := &http.Client{Timeout: 2 * time.Second}
		r, err := client.Get(url)
		if err == nil {
			defer r.Body.Close()
			var body []byte
			body, err = io.ReadAll(r.Body)
			if string(body) != "finished" {
				err = errRequestUnfinished
			}
		}
		response <- err
	}()
	<-entered
	cancel()
	select {
	case err := <-result:
		t.Fatalf("returned before request drained: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-response; err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestShutdownDeadlineClosesBlockedRequest(t *testing.T) {
	s := New("test", 20*time.Millisecond)
	entered := make(chan struct{})
	s.HTTP.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-r.Context().Done() })
	url, cancel, result := start(t, s)
	completed := make(chan struct{})
	go func() {
		defer close(completed)
		client := &http.Client{Timeout: time.Second}
		r, _ := client.Get(url)
		if r != nil {
			_ = r.Body.Close()
		}
	}()
	<-entered
	cancel()
	if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline, got %v", err)
	}
	<-completed
}

func TestDiagnosticsDoNotDispatchProviders(t *testing.T) {
	registry := gateway.NewRegistry()
	var calls atomic.Int32
	err := registry.Register("storage.buckets.list", func(gateway.Call, any) (any, error) {
		calls.Add(1)
		return nil, errProviderReached
	})
	if err != nil {
		t.Fatal(err)
	}
	s := WithGateway("test", time.Second, gateway.New(registry, nil))
	url, cancel, result := start(t, s)
	for _, path := range []string{healthPath, "/_local/diagnostics", "/_local/unknown"} {
		response, err := http.Get(url + path)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
	}
	if calls.Load() != 0 {
		t.Fatal("diagnostics reached provider")
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestShutdownDrainsOrCancelsGRPC(t *testing.T) {
	for _, force := range []bool{false, true} {
		t.Run(map[bool]string{false: "drain", true: "force"}[force], func(t *testing.T) {
			registry := gateway.NewRegistry()
			entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			if err := registry.Register("/google.pubsub.v1.Publisher/GetTopic", func(call gateway.Call, input any) (any, error) {
				close(entered)
				defer close(done)
				select {
				case <-release:
					request, ok := input.(*pb.GetTopicRequest)
					if !ok {
						return nil, errors.New("unexpected fixture request")
					}
					return &pb.Topic{Name: request.GetTopic()}, nil
				case <-call.Context.Done():
					return nil, call.Context.Err()
				}
			}); err != nil {
				t.Fatal(err)
			}
			timeout := time.Second
			if force {
				timeout = 30 * time.Millisecond
			}
			s := WithGateway("test", timeout, gateway.New(registry, nil))
			httpListener, err := listenTestAddress()
			if err != nil {
				t.Fatal(err)
			}
			grpcListener, err := listenTestAddress()
			if err != nil {
				_ = httpListener.Close()
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- s.Serve(ctx, Listeners{HTTP: httpListener, GRPC: grpcListener}) }()
			connection, err := grpc.NewClient("passthrough:///"+grpcListener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			response := make(chan error, 1)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				_, err := pb.NewPublisherClient(connection).GetTopic(ctx, &pb.GetTopicRequest{Topic: "projects/synthetic/topics/topic"})
				response <- err
			}()
			<-entered
			cancel()
			if !force {
				select {
				case err := <-result:
					t.Fatalf("shutdown skipped active RPC: %v", err)
				case <-time.After(20 * time.Millisecond):
				}
				close(release)
			}
			err = <-result
			if force && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("expected forced shutdown, got %v", err)
			}
			if !force && err != nil {
				t.Fatal(err)
			}
			if err := <-response; !force && err != nil {
				t.Fatal(err)
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("RPC context not cancelled")
			}
			if next, err := net.DialTimeout("tcp", grpcListener.Addr().String(), time.Second); err == nil {
				_ = next.Close()
				t.Fatal("gRPC listener remained open")
			}
		})
	}
}

type managedBackend struct{ stopped atomic.Bool }

func (b *managedBackend) Start(context.Context) error { return nil }
func (b *managedBackend) Ready(context.Context) error { return nil }
func (b *managedBackend) Stop(context.Context) error  { b.stopped.Store(true); return nil }
func (b *managedBackend) Reset(context.Context) error { return nil }
func TestRuntimeShutdownStopsManagedBackends(t *testing.T) {
	s := New("test", time.Second)
	child := &managedBackend{}
	if err := s.Backends.Register("fixture", backend.Definition{New: func(state.Scope) (backend.Backend, error) { return child, nil }, AfterReset: func(state.Scope) error { return nil }}); err != nil {
		t.Fatal(err)
	}
	if err := s.Backends.Preload(context.Background(), []backend.Key{{Service: "fixture", Scope: state.Scope{Project: "project-a", Location: state.Global}}}); err != nil {
		t.Fatal(err)
	}
	url, cancel, result := start(t, s)
	response, err := http.Get(url + "/_local/diagnostics")
	if err != nil {
		t.Fatal(err)
	}
	var body struct{ Backends []backend.Status }
	err = json.NewDecoder(response.Body).Decode(&body)
	_ = response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(body.Backends) != 1 || body.Backends[0].Phase != backend.Ready {
		t.Fatal(body)
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if !child.stopped.Load() {
		t.Fatal("core left backend running")
	}
}
