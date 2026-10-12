package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	errUnexpectedFixtureRequest = errors.New("unexpected fixture request")
	errRequestUnfinished        = errors.New("request did not finish")
	errProviderReached          = errors.New("diagnostics reached provider")
)

// newServer builds a Server with an empty gateway for listener and shutdown tests.
func newServer(version string, shutdownTimeout time.Duration) *Server {
	return WithGateway(version, shutdownTimeout, gateway.New(nil, nil))
}

// getWithClient issues a context-aware GET and closes nothing on the caller's
// behalf; the returned response is always safe to read or close.
func getWithClient(t *testing.T, client *http.Client, url string) (*http.Response, error) {
	t.Helper()
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", url, err)
	}
	return response, nil
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

// healthPath is the local readiness endpoint asserted across runtime tests.
const healthPath = "/_local/health"

// start serves s on fresh loopback listeners and returns its HTTP URL, the
// cancel that begins shutdown, and Serve's result.
func start(t *testing.T, s *Server) (string, context.CancelFunc, <-chan error) {
	t.Helper()
	listeners, cancel, result := serveLoopback(t, s)
	return "http://" + listeners.HTTP.Addr().String(), cancel, result
}

func serveLoopback(t *testing.T, s *Server) (Listeners, context.CancelFunc, <-chan error) {
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
	listeners := Listeners{HTTP: listener, GRPC: grpcListener}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- s.Serve(ctx, listeners) }()
	t.Cleanup(cancel)
	return listeners, cancel, result
}

// getJSON fetches url and decodes its JSON object body.
func getJSON(t *testing.T, client *http.Client, url string) (int, map[string]any) {
	t.Helper()
	response, err := getWithClient(t, client, url)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var body map[string]any
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, body
}

func TestHealthAndUnsupportedAPI(t *testing.T) {
	s := newServer("test-version", time.Second)
	url, cancel, result := start(t, s)
	client := &http.Client{Timeout: time.Second}
	if code, body := getJSON(t, client, url+healthPath); code != http.StatusOK || body["ready"] != true || body["version"] != "test-version" {
		t.Fatalf("unexpected health: %d %v", code, body)
	}
	if code, _ := getJSON(t, client, url+"/_local/diagnostics"); code != http.StatusOK {
		t.Fatalf("diagnostics status %d", code)
	}
	code, body := getJSON(t, client, url+"/storage/v1/b")
	if detail, ok := body["error"].(map[string]any); code != http.StatusNotImplemented || !ok || detail["status"] != "UNIMPLEMENTED" {
		t.Fatalf("unexpected API response: %v", body)
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if response, err := getWithClient(t, client, url+healthPath); err == nil {
		_ = response.Body.Close()
		t.Fatal("listener remained open")
	}
}

func TestShutdownDrainsActiveRequest(t *testing.T) {
	s := newServer("test", time.Second)
	entered, release := make(chan struct{}), make(chan struct{})
	s.HTTP.Handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		_, _ = io.WriteString(w, "finished")
	})
	url, cancel, result := start(t, s)
	response := make(chan error, 1)
	go func() {
		client := &http.Client{Timeout: 2 * time.Second}
		r, err := getWithClient(t, client, url)
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
	s := newServer("test", 20*time.Millisecond)
	entered := make(chan struct{})
	s.HTTP.Handler = http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { close(entered); <-r.Context().Done() })
	url, cancel, result := start(t, s)
	completed := make(chan struct{})
	go func() {
		defer close(completed)
		client := &http.Client{Timeout: time.Second}
		r, _ := getWithClient(t, client, url)
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
		response, err := getWithClient(t, http.DefaultClient, url+path)
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
			if err := registry.Register("/google.pubsub.v1.Publisher/GetTopic", blockingGetTopic(entered, release, done)); err != nil {
				t.Fatal(err)
			}
			timeout := time.Second
			if force {
				timeout = 30 * time.Millisecond
			}
			listeners, cancel, result := serveLoopback(t, WithGateway("test", timeout, gateway.New(registry, nil)))
			grpcAddress := listeners.GRPC.Addr().String()
			response := getTopic(t, grpcAddress)
			<-entered
			cancel()
			if !force {
				stillDraining(t, result)
				close(release)
			}
			checkGRPCShutdown(t, force, <-result, <-response)
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("RPC context not cancelled")
			}
			listenerClosed(t, grpcAddress)
		})
	}
}

// stillDraining fails if Serve returns while an RPC is still active.
func stillDraining(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("shutdown skipped active RPC: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
}

// listenerClosed fails if address still accepts connections.
func listenerClosed(t *testing.T, address string) {
	t.Helper()
	dialer := &net.Dialer{Timeout: time.Second}
	if next, err := dialer.DialContext(context.Background(), "tcp", address); err == nil {
		_ = next.Close()
		t.Fatal("gRPC listener remained open")
	}
}

// blockingGetTopic answers GetTopic once release closes, or returns the RPC's
// context error when shutdown cancels it first.
func blockingGetTopic(entered chan<- struct{}, release <-chan struct{}, done chan<- struct{}) gateway.Handler {
	return func(call gateway.Call, input any) (any, error) {
		close(entered)
		defer close(done)
		select {
		case <-release:
			request, ok := input.(*pb.GetTopicRequest)
			if !ok {
				return nil, errUnexpectedFixtureRequest
			}
			return &pb.Topic{Name: request.GetTopic()}, nil
		case <-call.Context.Done():
			return nil, call.Context.Err()
		}
	}
}

// getTopic issues one GetTopic RPC in the background and returns its result.
func getTopic(t *testing.T, address string) <-chan error {
	t.Helper()
	connection, err := grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	response := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := pb.NewPublisherClient(connection).GetTopic(ctx, &pb.GetTopicRequest{Topic: "projects/synthetic/topics/topic"})
		response <- err
	}()
	return response
}

// checkGRPCShutdown requires a forced shutdown to report its deadline, and a
// draining shutdown to finish both Serve and the RPC cleanly.
func checkGRPCShutdown(t *testing.T, force bool, serveErr, rpcErr error) {
	t.Helper()
	if force {
		if !errors.Is(serveErr, context.DeadlineExceeded) {
			t.Fatalf("expected forced shutdown, got %v", serveErr)
		}
		return
	}
	if serveErr != nil {
		t.Fatal(serveErr)
	}
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
}

type managedBackend struct{ stopped atomic.Bool }

func (b *managedBackend) Start(context.Context) error { return nil }
func (b *managedBackend) Ready(context.Context) error { return nil }
func (b *managedBackend) Stop(context.Context) error  { b.stopped.Store(true); return nil }
func (b *managedBackend) Reset(context.Context) error { return nil }
func TestRuntimeShutdownStopsManagedBackends(t *testing.T) {
	s := newServer("test", time.Second)
	child := &managedBackend{}
	if err := s.Backends.Register("fixture", backend.Definition{New: func(state.Scope) (backend.Backend, error) { return child, nil }, AfterReset: func(state.Scope) error { return nil }}); err != nil {
		t.Fatal(err)
	}
	fixture := backend.Key{Service: "fixture", Scope: state.Scope{Project: "project-a", Location: state.Global}}
	if err := s.Backends.Use(context.Background(), fixture, func(context.Context, backend.Backend) error { return nil }); err != nil {
		t.Fatal(err)
	}
	url, cancel, result := start(t, s)
	response, err := getWithClient(t, http.DefaultClient, url+"/_local/diagnostics")
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		Backends []backend.Status `json:"backends"`
	}
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
