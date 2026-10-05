// Package runtime owns the core listener and its shutdown lifecycle.
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
)

// Server is a local core process. Service providers are added by later slices.
type Server struct {
	HTTP            *http.Server
	ShutdownTimeout time.Duration
	ready           atomic.Bool
}

func New(version string, shutdownTimeout time.Duration) *Server {
	s := &Server{ShutdownTimeout: shutdownTimeout}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /_local/health", func(w http.ResponseWriter, r *http.Request) {
		code := http.StatusOK
		if !s.ready.Load() {
			code = http.StatusServiceUnavailable
		}
		writeJSON(w, code, map[string]any{"service": "gcp-local", "version": version, "ready": s.ready.Load(), "providers": []string{}})
	})
	mux.HandleFunc("GET /_local/diagnostics", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"runtime": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH, "providers": []string{}})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusNotImplemented, map[string]any{"error": map[string]any{"code": 501, "status": "UNIMPLEMENTED", "message": "No provider implements this API method"}})
	})
	s.HTTP = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second,
		IdleTimeout: 60 * time.Second, MaxHeaderBytes: 1 << 20}
	return s
}

// Serve accepts an already-bound listener so callers can report its actual port.
// On cancellation it drains existing requests before forcing the listener closed.
func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	if s.ShutdownTimeout <= 0 {
		listener.Close()
		return errors.New("shutdown timeout must be positive")
	}
	if ctx.Err() != nil {
		listener.Close()
		return nil
	}
	s.ready.Store(true)
	served := make(chan error, 1)
	go func() { served <- s.HTTP.Serve(listener) }()
	select {
	case err := <-served:
		s.ready.Store(false)
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		s.ready.Store(false)
		deadline, cancel := context.WithTimeout(context.Background(), s.ShutdownTimeout)
		defer cancel()
		err := s.HTTP.Shutdown(deadline)
		if err != nil {
			_ = s.HTTP.Close()
		}
		serveErr := <-served
		if err != nil {
			return err
		}
		if !errors.Is(serveErr, http.ErrServerClosed) {
			return serveErr
		}
		return nil
	}
}

func writeJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}
