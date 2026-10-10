// Package runtime owns listeners and their shutdown lifecycle.
package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

// New builds a Server with an empty gateway. Tests use it to exercise listener and
// shutdown behavior without registering providers; production uses WithGateway.
func New(version string, shutdownTimeout time.Duration) *Server {
	return WithGateway(version, shutdownTimeout, gateway.New(nil, nil))
}

func WithGateway(version string, shutdownTimeout time.Duration, g *gateway.Gateway) *Server {
	s := &Server{ShutdownTimeout: shutdownTimeout, GRPC: g.NewGRPCServer(), Backends: backend.NewManager(30*time.Second, shutdownTimeout)}
	s.HTTP = &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.adminRoute(w, r, g, version) {
			return
		}
		// Dispatch directly, preserving escaped object paths without ServeMux redirects.
		g.ServeHTTP(w, r)
	}), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 1 << 20}
	return s
}

// adminRoute serves the local administrative endpoints and reports whether the
// request matched one. Service traffic falls through to the gateway.
func (s *Server) adminRoute(w http.ResponseWriter, r *http.Request, g *gateway.Gateway, version string) bool {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/_local/health":
		writeJSON(w, s.healthCode(), map[string]any{"service": "gcp-local", "version": version, "ready": s.ready.Load(), "providers": g.Registry.Providers(), "backends": s.Backends.Status()})
	case r.Method == http.MethodGet && r.URL.Path == "/_local/diagnostics":
		writeJSON(w, 200, map[string]any{"runtime": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH, "providers": g.Registry.Providers(), "backends": s.Backends.Status()})
	case strings.HasPrefix(r.URL.Path, "/_local/iam/decisions/"):
		s.serveIAM(w, r)
	case r.URL.Path == "/v1/token" || r.URL.Path == "/_local/identity" || r.URL.Path == "/_local/identity/certs":
		s.serveIdentity(w, r)
	default:
		return false
	}
	return true
}

// serveIAM answers the decision-diagnostics route when the engine is mounted.
func (s *Server) serveIAM(w http.ResponseWriter, r *http.Request) {
	if s.IAM == nil {
		return
	}
	s.IAM.ServeHTTP(w, r)
}

// serveIdentity answers the identity routes when the service is mounted.
func (s *Server) serveIdentity(w http.ResponseWriter, r *http.Request) {
	if s.Identity == nil {
		return
	}
	s.Identity.ServeHTTP(w, r)
}

// healthCode is 200 once startup work completes, 503 before that.
func (s *Server) healthCode() int {
	if s.ready.Load() {
		return http.StatusOK
	}
	return http.StatusServiceUnavailable
}

// Listeners carries the bound sockets Serve owns. Serve binds none of them
// itself; cancellation drains both transports under a shared deadline, then
// forcibly closes remaining HTTP requests and gRPC streams.
type Listeners struct{ HTTP, GRPC, Metadata net.Listener }

// errListeners rejects an incomplete listener pairing.
var errListeners = errors.New("supply http/grpc listeners and pair the metadata handler/listener")

// errShutdownTimeout rejects a server without a positive drain budget.
var errShutdownTimeout = errors.New("shutdown timeout must be positive")

func (s *Server) Serve(ctx context.Context, listeners Listeners) error {
	httpListener, grpcListener := listeners.HTTP, listeners.GRPC
	if httpListener == nil || grpcListener == nil || (s.Metadata == nil) != (listeners.Metadata == nil) {
		for _, listener := range []net.Listener{httpListener, grpcListener, listeners.Metadata} {
			if listener != nil {
				_ = listener.Close()
			}
		}
		return errListeners
	}
	closeListeners := func() {
		_ = httpListener.Close()
		_ = grpcListener.Close()
		if listeners.Metadata != nil {
			_ = listeners.Metadata.Close()
		}
	}

	if s.ShutdownTimeout <= 0 {
		closeListeners()
		return errShutdownTimeout
	}
	if ctx.Err() != nil {
		closeListeners()
		cleanup, cancel := context.WithTimeout(context.Background(), s.ShutdownTimeout)
		defer cancel()
		if err := s.Backends.Shutdown(cleanup); err != nil { //nolint:contextcheck // draining must outlive the cancelled request
			return fmt.Errorf("drain backends: %w", err)
		}
		return nil
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
	for range remaining {
		if err := <-served; err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, grpc.ErrServerStopped) {
			serveErr = err
		}
	}
	if err := s.Backends.Shutdown(deadline); err != nil { //nolint:contextcheck // draining must outlive the cancelled request
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
	// Marshal before committing a status so an encoding failure is still
	// reportable as a 500 instead of a silent truncated 200.
	encoded, err := json.Marshal(value)
	if err != nil {
		http.Error(w, `{"error":"response encoding failed"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_, _ = w.Write(encoded)
}
