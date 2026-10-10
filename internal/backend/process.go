package backend

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

var ErrProcessExited = errors.New("backend process exited")

type ProcessConfig struct {
	Command   []string
	Directory string
	// Only explicitly supplied environment variables reach the backend. In
	// particular, host GCP credentials and proxy variables are never inherited.
	Environment []string
	Probe       func(context.Context) error
	// Reset removes only assets owned by this scoped instance, after Stop.
	ResetAssets func(context.Context) error
}

var (
	errProcessConfig   = errors.New("process command, readiness probe, and reset hook are required")
	errProcessRunning  = errors.New("backend process already running")
	errProcessNotDone  = errors.New("stop backend before resetting assets")
	errProbeEndpoint   = errors.New("readiness requires a loopback HTTP endpoint with an explicit port")
	errProbeStatusCode = errors.New("backend readiness returned an unexpected status")
)

type Process struct {
	config  ProcessConfig
	mu      sync.Mutex
	command *exec.Cmd
	done    chan struct{}
}

func NewProcess(config ProcessConfig) (*Process, error) {
	if len(config.Command) == 0 || !filepath.IsAbs(config.Command[0]) || config.Probe == nil || config.ResetAssets == nil {
		return nil, errProcessConfig
	}
	config.Command = append([]string(nil), config.Command...)
	config.Environment = append([]string{}, config.Environment...)
	return &Process{config: config}, nil
}
func (p *Process) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.command != nil {
		select {
		case <-p.done:
		default:
			return errProcessRunning
		}
	}
	command := exec.Command(p.config.Command[0], p.config.Command[1:]...) //nolint:gosec // the operator supplies the pinned backend command
	command.Dir = p.config.Directory
	command.Env = p.config.Environment
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	// No stdout/stderr pipes: the child cannot leak credentials into diagnostics
	// or keep Wait blocked through a descendant holding an inherited pipe open.
	if err := command.Start(); err != nil {
		return fmt.Errorf("start backend command: %w", err)
	}
	p.command = command
	p.done = make(chan struct{})
	done := p.done
	go func() {
		_ = command.Wait()
		// Reap the owned group if a backend leader exits before its workers.
		_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		close(done)
	}()
	return nil
}
func (p *Process) Ready(ctx context.Context) error {
	p.mu.Lock()
	done := p.done
	p.mu.Unlock()
	if done == nil {
		return ErrProcessExited
	}
	select {
	case <-done:
		return ErrProcessExited
	default:
	}
	if err := p.config.Probe(ctx); err != nil {
		return err
	}
	select {
	case <-done:
		return ErrProcessExited
	default:
		return nil
	}
}
func (p *Process) Stop(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.command == nil {
		return nil
	}
	select {
	case <-p.done:
		return nil
	default:
	}
	pid := p.command.Process.Pid
	if err := syscall.Kill(-pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("signal backend process: %w", err)
	}
	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
			return fmt.Errorf("kill backend process: %w", err)
		}
		<-p.done
		return ctx.Err()
	}
}
func (p *Process) Reset(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done != nil {
		select {
		case <-p.done:
		default:
			return errProcessNotDone
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return p.config.ResetAssets(ctx)
}

// HTTPProbe accepts only explicit numeric loopback HTTP endpoints, with no
// proxy, redirects, or credentials. Provider-specific probes can use gRPC too.
func HTTPProbe(endpoint string) (func(context.Context) error, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse readiness endpoint: %w", err)
	}
	ip := net.ParseIP(parsed.Hostname())
	if parsed.Scheme != "http" || ip == nil || !ip.IsLoopback() || parsed.Port() == "" || parsed.User != nil || parsed.Fragment != "" {
		return nil, errProbeEndpoint
	}
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return func(ctx context.Context) error {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return fmt.Errorf("build readiness request: %w", err)
		}
		response, err := client.Do(request)
		if err != nil {
			return fmt.Errorf("readiness request: %w", err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("%w: HTTP %d", errProbeStatusCode, response.StatusCode)
		}
		return nil
	}, nil
}
