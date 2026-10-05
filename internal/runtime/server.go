// Package runtime owns listeners and their shutdown lifecycle.
package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"google.golang.org/grpc"
)

type Server struct {
	HTTP            *http.Server
	GRPC            *grpc.Server
	Backends        *backend.Manager
	Identity        http.Handler
	IAM             http.Handler
	Metadata        *http.Server
	ShutdownTimeout time.Duration
	ready           atomic.Bool
}

func New(version string, shutdownTimeout time.Duration) *Server {
	return WithGateway(version, shutdownTimeout, gateway.New(nil, nil))
}
func WithGateway(version string, shutdownTimeout time.Duration, g *gateway.Gateway) *Server {
	s := &Server{ShutdownTimeout: shutdownTimeout, GRPC: g.NewGRPCServer(), Backends: backend.NewManager(30*time.Second, shutdownTimeout)}
	s.HTTP = &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Dispatch directly, preserving escaped object paths without ServeMux redirects.
		switch {
		case r.Method == "GET" && r.URL.Path == "/_local/health":
			ready := s.ready.Load()
			code := http.StatusOK
			if !ready {
				code = http.StatusServiceUnavailable
			}
			writeJSON(w, code, map[string]any{"service": "gcp-local", "version": version, "ready": ready, "providers": g.Registry.Providers(), "backends": s.Backends.Status()})
		case r.Method == "GET" && r.URL.Path == "/_local/diagnostics":
			writeJSON(w, 200, map[string]any{"runtime": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH, "providers": g.Registry.Providers(), "backends": s.Backends.Status()})
		case s.IAM != nil && strings.HasPrefix(r.URL.Path, "/_local/iam/decisions/"):
			s.IAM.ServeHTTP(w, r)
		case s.Identity != nil && (r.URL.Path == "/v1/token" || r.URL.Path == "/_local/identity"):
			s.Identity.ServeHTTP(w, r)
		default:
			g.ServeHTTP(w, r)
		}
	}), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 1 << 20}
	return s
}

// Serve binds neither listener itself. Cancellation drains both transports under
// a shared deadline, then forcibly closes remaining HTTP requests/gRPC streams.
type Listeners struct{ HTTP, GRPC, Metadata net.Listener }

func (s *Server) Serve(ctx context.Context, listeners Listeners) error {
	httpListener, grpcListener := listeners.HTTP, listeners.GRPC
	if httpListener == nil || grpcListener == nil || (s.Metadata == nil) != (listeners.Metadata == nil) {
		for _, listener := range []net.Listener{httpListener, grpcListener, listeners.Metadata} {
			if listener != nil {
				listener.Close()
			}
		}
		return errors.New("supply HTTP/gRPC listeners and pair the metadata handler/listener")
	}
	closeListeners := func() {
		httpListener.Close()
		grpcListener.Close()
		if listeners.Metadata != nil {
			listeners.Metadata.Close()
		}
	}

	if s.ShutdownTimeout <= 0 {
		closeListeners()
		return errors.New("shutdown timeout must be positive")
	}
	if ctx.Err() != nil {
		closeListeners()
		cleanup, cancel := context.WithTimeout(context.Background(), s.ShutdownTimeout)
		defer cancel()
		return s.Backends.Shutdown(cleanup)
	}
	s.ready.Store(true)
	count := 2
	if listeners.Metadata != nil {
		count++
	}
	served := make(chan error, count)
	go func() { served <- s.HTTP.Serve(httpListener) }()
	go func() { served <- s.GRPC.Serve(grpcListener) }()
	if listeners.Metadata != nil {
		go func() { served <- s.Metadata.Serve(listeners.Metadata) }()
	}
	var serveErr error
	remaining := count
	select {
	case serveErr = <-served:
		remaining--
	case <-ctx.Done():
	}
	s.ready.Store(false)
	s.Backends.DrainSessions()
	deadline, cancel := context.WithTimeout(context.Background(), s.ShutdownTimeout)
	defer cancel()
	httpCount := 1
	if listeners.Metadata != nil {
		httpCount++
	}
	httpDone := make(chan error, httpCount)
	grpcDone := make(chan struct{})
	go func() { httpDone <- s.HTTP.Shutdown(deadline) }()
	if s.Metadata != nil {
		go func() { httpDone <- s.Metadata.Shutdown(deadline) }()
	}
	go func() { s.GRPC.GracefulStop(); close(grpcDone) }()
	var shutdownErr error
	select {
	case <-grpcDone:
	case <-deadline.Done():
		shutdownErr = deadline.Err()
		s.GRPC.Stop()
		<-grpcDone
	}
	for range httpCount {
		if err := <-httpDone; err != nil {
			shutdownErr = errors.Join(shutdownErr, err)
			_ = s.HTTP.Close()
			if s.Metadata != nil {
				_ = s.Metadata.Close()
			}
		}
	}
	for i := 0; i < remaining; i++ {
		if err := <-served; err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, grpc.ErrServerStopped) {
			serveErr = err
		}
	}
	if err := s.Backends.Shutdown(deadline); err != nil {
		shutdownErr = errors.Join(shutdownErr, err)
	}
	if shutdownErr != nil {
		return shutdownErr
	}
	if errors.Is(serveErr, http.ErrServerClosed) || errors.Is(serveErr, grpc.ErrServerStopped) {
		return nil
	}
	return serveErr
}
func writeJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}
