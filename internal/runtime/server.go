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
	HTTP     *http.Server
	GRPC     *grpc.Server
	Backends *backend.Manager
	Identity http.Handler
	IAM      http.Handler
	Run      http.Handler
	Analyzer http.Handler
	Metadata *http.Server
	// AfterShutdown runs once the backends have stopped, within the drain
	// budget (the Cloud Run Docker runtime removes its containers here).
	AfterShutdown   func(context.Context) error
	ShutdownTimeout time.Duration
	ready           atomic.Bool
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
	default:
		return s.adminSubroutes(w, r)
	}
	return true
}

// adminSubroutes serves the diagnostics, IAM, run, and identity mounts.
func (s *Server) adminSubroutes(w http.ResponseWriter, r *http.Request) bool {
	switch {
	case strings.HasPrefix(r.URL.Path, "/_local/iam/decisions/"):
		s.serveIAM(w, r)
	case strings.HasPrefix(r.URL.Path, "/_run/"):
		s.serveRun(w, r)
	case strings.HasPrefix(r.URL.Path, "/_local/networking/"):
		s.serveAnalyzer(w, r)
	case r.URL.Path == "/v1/token" || r.URL.Path == "/_local/identity" || r.URL.Path == "/_local/identity/certs":
		s.serveIdentity(w, r)
	default:
		return false
	}
	return true
}

// serveAnalyzer answers the local reachability surface when mounted.
func (s *Server) serveAnalyzer(w http.ResponseWriter, r *http.Request) {
	if s.Analyzer == nil {
		writeJSON(w, 501, map[string]any{"error": map[string]any{"code": 501, "message": "The networking provider is not selected"}})
		return
	}
	s.Analyzer.ServeHTTP(w, r)
}

// serveRun answers the local Cloud Run invocation surface when mounted.
func (s *Server) serveRun(w http.ResponseWriter, r *http.Request) {
	if s.Run == nil {
		writeJSON(w, 501, map[string]any{"error": map[string]any{"code": 501, "message": "The Cloud Run provider is not selected"}})
		return
	}
	s.Run.ServeHTTP(w, r)
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
// forcibly closes remaining HTTP requests and gRPC streams. GRPCTLS is an
// optional TLS listener served by the same gRPC server, handlers, and auth.
type Listeners struct{ HTTP, GRPC, GRPCTLS, Metadata net.Listener }

// errListeners rejects an incomplete listener pairing.
var errListeners = errors.New("supply http/grpc listeners and pair the metadata handler/listener")

// errShutdownTimeout rejects a server without a positive drain budget.
var errShutdownTimeout = errors.New("shutdown timeout must be positive")

func (s *Server) Serve(ctx context.Context, listeners Listeners) error {
	if err := s.validate(listeners); err != nil {
		listeners.close()
		return err
	}
	if ctx.Err() != nil {
		listeners.close()
		return s.drainBackends() //nolint:contextcheck // draining must outlive the cancelled request
	}
	s.ready.Store(true)
	served, remaining := s.serve(listeners)
	var serveErr error
	select {
	case serveErr = <-served:
		remaining--
	case <-ctx.Done():
	}
	s.ready.Store(false)
	s.Backends.DrainSessions()
	deadline, cancel := context.WithTimeout(context.Background(), s.ShutdownTimeout)
	defer cancel()
	shutdownErr := s.shutdownTransports(deadline) //nolint:contextcheck // draining must outlive the cancelled request
	serveErr = awaitServers(served, remaining, serveErr)
	if err := s.Backends.Shutdown(deadline); err != nil { //nolint:contextcheck // draining must outlive the cancelled request
		shutdownErr = errors.Join(shutdownErr, err)
	}
	if s.AfterShutdown != nil {
		if err := s.AfterShutdown(deadline); err != nil { //nolint:contextcheck // draining must outlive the cancelled request
			shutdownErr = errors.Join(shutdownErr, fmt.Errorf("after shutdown: %w", err))
		}
	}
	if shutdownErr != nil {
		return shutdownErr
	}
	if isServerClosed(serveErr) {
		return nil
	}
	return serveErr
}

// validate requires both service listeners, a metadata listener exactly when a
// metadata server is configured, and a positive drain budget.
func (s *Server) validate(listeners Listeners) error {
	if listeners.HTTP == nil || listeners.GRPC == nil || (s.Metadata == nil) != (listeners.Metadata == nil) {
		return errListeners
	}
	if s.ShutdownTimeout <= 0 {
		return errShutdownTimeout
	}
	return nil
}

// close releases every supplied listener.
func (l Listeners) close() {
	for _, listener := range []net.Listener{l.HTTP, l.GRPC, l.GRPCTLS, l.Metadata} {
		if listener != nil {
			_ = listener.Close()
		}
	}
}

// drainBackends stops managed backends when Serve is cancelled before it starts.
func (s *Server) drainBackends() error {
	cleanup, cancel := context.WithTimeout(context.Background(), s.ShutdownTimeout)
	defer cancel()
	if err := s.Backends.Shutdown(cleanup); err != nil {
		return fmt.Errorf("drain backends: %w", err)
	}
	return nil
}

// serve starts each transport on its listener and returns the channel their
// Serve results arrive on, with the number of transports started.
func (s *Server) serve(listeners Listeners) (<-chan error, int) {
	count := 2
	for _, optional := range []net.Listener{listeners.GRPCTLS, listeners.Metadata} {
		if optional != nil {
			count++
		}
	}
	served := make(chan error, count)
	go func() { served <- s.HTTP.Serve(listeners.HTTP) }()
	go func() { served <- s.GRPC.Serve(listeners.GRPC) }()
	if listeners.GRPCTLS != nil {
		go func() { served <- s.GRPC.Serve(listeners.GRPCTLS) }()
	}
	if listeners.Metadata != nil {
		go func() { served <- s.Metadata.Serve(listeners.Metadata) }()
	}
	return served, count
}

// shutdownTransports drains HTTP, metadata, and gRPC under one deadline. Past
// the deadline it stops gRPC streams and closes HTTP servers that failed to drain.
func (s *Server) shutdownTransports(deadline context.Context) error {
	httpCount := 1
	if s.Metadata != nil {
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
			s.closeHTTP()
		}
	}
	return shutdownErr
}

// closeHTTP forcibly closes the HTTP servers' remaining connections.
func (s *Server) closeHTTP() {
	_ = s.HTTP.Close()
	if s.Metadata != nil {
		_ = s.Metadata.Close()
	}
}

// awaitServers collects the remaining Serve results, keeping the last
// unexpected failure over serveErr.
func awaitServers(served <-chan error, remaining int, serveErr error) error {
	for range remaining {
		if err := <-served; err != nil && !isServerClosed(err) {
			serveErr = err
		}
	}
	return serveErr
}

// isServerClosed reports the errors transports return after a requested shutdown.
func isServerClosed(err error) bool {
	return errors.Is(err, http.ErrServerClosed) || errors.Is(err, grpc.ErrServerStopped)
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
