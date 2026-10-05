// Package runtime owns listeners and their shutdown lifecycle.
package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"google.golang.org/grpc"
)

type Server struct {
	HTTP            *http.Server
	GRPC            *grpc.Server
	ShutdownTimeout time.Duration
	ready           atomic.Bool
}

func New(version string, shutdownTimeout time.Duration) *Server {
	return WithGateway(version, shutdownTimeout, gateway.New(nil, nil))
}
func WithGateway(version string, shutdownTimeout time.Duration, g *gateway.Gateway) *Server {
	s := &Server{ShutdownTimeout: shutdownTimeout, GRPC: g.NewGRPCServer()}
	s.HTTP = &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Dispatch directly, preserving escaped object paths without ServeMux redirects.
		switch {
		case r.Method == "GET" && r.URL.Path == "/_local/health":
			ready := s.ready.Load()
			code := http.StatusOK
			if !ready {
				code = http.StatusServiceUnavailable
			}
			writeJSON(w, code, map[string]any{"service": "gcp-local", "version": version, "ready": ready, "providers": g.Registry.Providers()})
		case r.Method == "GET" && r.URL.Path == "/_local/diagnostics":
			writeJSON(w, 200, map[string]any{"runtime": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH, "providers": g.Registry.Providers()})
		default:
			g.ServeHTTP(w, r)
		}
	}), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 1 << 20}
	return s
}

// Serve binds neither listener itself. Cancellation drains both transports under
// a shared deadline, then forcibly closes remaining HTTP requests/gRPC streams.
func (s *Server) Serve(ctx context.Context, httpListener, grpcListener net.Listener) error {
	if s.ShutdownTimeout <= 0 {
		httpListener.Close()
		grpcListener.Close()
		return errors.New("shutdown timeout must be positive")
	}
	if ctx.Err() != nil {
		httpListener.Close()
		grpcListener.Close()
		return nil
	}
	s.ready.Store(true)
	served := make(chan error, 2)
	go func() { served <- s.HTTP.Serve(httpListener) }()
	go func() { served <- s.GRPC.Serve(grpcListener) }()
	var serveErr error
	remaining := 2
	select {
	case serveErr = <-served:
		remaining--
	case <-ctx.Done():
	}
	s.ready.Store(false)
	deadline, cancel := context.WithTimeout(context.Background(), s.ShutdownTimeout)
	defer cancel()
	httpDone := make(chan error, 1)
	grpcDone := make(chan struct{})
	go func() { httpDone <- s.HTTP.Shutdown(deadline) }()
	go func() { s.GRPC.GracefulStop(); close(grpcDone) }()
	var shutdownErr error
	select {
	case <-grpcDone:
	case <-deadline.Done():
		shutdownErr = deadline.Err()
		s.GRPC.Stop()
		<-grpcDone
	}
	if err := <-httpDone; err != nil {
		shutdownErr = err
		_ = s.HTTP.Close()
	}
	for i := 0; i < remaining; i++ {
		if err := <-served; err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, grpc.ErrServerStopped) {
			serveErr = err
		}
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
