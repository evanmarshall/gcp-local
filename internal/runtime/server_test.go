package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

func start(t *testing.T, s *Server) (string, context.CancelFunc, <-chan error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- s.Serve(ctx, listener) }()
	t.Cleanup(cancel)
	return "http://" + listener.Addr().String(), cancel, result
}

func TestHealthAndUnsupportedAPI(t *testing.T) {
	s := New("test-version", time.Second)
	url, cancel, result := start(t, s)
	client := &http.Client{Timeout: time.Second}
	for _, path := range []string{"/_local/health", "/_local/diagnostics", "/storage/v1/b"} {
		response, err := client.Get(url + path)
		if err != nil {
			t.Fatal(err)
		}
		var body map[string]any
		err = json.NewDecoder(response.Body).Decode(&body)
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if path == "/storage/v1/b" {
			if response.StatusCode != 501 || body["error"].(map[string]any)["status"] != "UNIMPLEMENTED" {
				t.Fatalf("unexpected API response: %v", body)
			}
		} else if response.StatusCode != 200 {
			t.Fatalf("diagnostics status %d", response.StatusCode)
		}
		if path == "/_local/health" && (body["ready"] != true || body["version"] != "test-version") {
			t.Fatalf("unexpected health: %v", body)
		}
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if _, err := client.Get(url + "/_local/health"); err == nil {
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
				err = errors.New("request did not finish")
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
			r.Body.Close()
		}
	}()
	<-entered
	cancel()
	if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline, got %v", err)
	}
	<-completed
}
