package run

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"syscall"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Sentinel errors for the revision process states the watcher distinguishes.
var (
	errProcessGone   = errors.New("the revision process is gone")
	errProcessExited = errors.New("the revision process exited")
)

// ProcessRuntime launches Cloud Run revisions as local processes for hosts
// without a container engine. It is the restricted-guest development path:
// the packaged container acceptance still runs on host Docker in CI, and a
// process launch carries no image-layer semantics. One image maps to one
// explicit command; images without a mapping refuse to launch.
type ProcessRuntime struct {
	mu       sync.Mutex
	commands map[string][]string
	running  map[string]*launch
	// reserve picks the port a revision is told to bind.
	reserve func(context.Context) (int, error)
}

// launch is one running revision process. Its single waiter reaps the
// process and records the exit for every caller of Ready and Stop.
type launch struct {
	cmd  *exec.Cmd
	port int
	done chan struct{}
	exit error
}

// NewProcessRuntime builds the dockerless runtime from an image-to-command
// map. An empty map is valid: every launch is then refused.
func NewProcessRuntime(commands map[string][]string) *ProcessRuntime {
	copied := map[string][]string{}
	for image, command := range commands {
		copied[image] = append([]string(nil), command...)
	}
	return &ProcessRuntime{commands: copied, running: map[string]*launch{}, reserve: freePort}
}

// freePort asks the kernel for an unused loopback port. The port is free
// again once this returns, so another process can take it before the
// revision binds it; Listening tells the two apart.
func freePort(ctx context.Context) (int, error) {
	var listen net.ListenConfig
	listener, err := listen.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("allocate the revision port: %w", err)
	}
	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		return 0, status.Errorf(codes.Internal, "The revision port is not a TCP address")
	}
	if closeErr := listener.Close(); closeErr != nil {
		return 0, fmt.Errorf("reserve the revision port: %w", closeErr)
	}
	return addr.Port, nil
}

// Start runs one revision process. The container contract stays identical:
// the process receives PORT, the explicit environment, and the runtime
// reports its loopback address and a process-handle identifier.
func (p *ProcessRuntime) Start(ctx context.Context, spec StartSpec) (Started, error) {
	command, ok := p.commands[spec.Image]
	if !ok {
		return Started{}, status.Errorf(codes.Unimplemented, "No process command is mapped for image %s", spec.Image)
	}
	port, err := p.reserve(ctx)
	if err != nil {
		return Started{}, err
	}
	// The revision outlives the launch request; Stop owns termination. The
	// detached context satisfies CommandContext without coupling the
	// revision lifetime to the launch request.
	run := exec.CommandContext(context.WithoutCancel(ctx), command[0], command[1:]...) //nolint:gosec // the operator maps commands explicitly
	// Workloads get only their declared environment, never the core's own
	// variables (credentials, configuration, or proxy settings).
	run.Env = append(append([]string{"PATH=/usr/local/bin:/usr/bin:/bin"}, spec.Env...), "PORT="+strconv.Itoa(port))
	// Like Cloud Run's container logs, revision output goes to the core's
	// own log stream instead of being discarded.
	run.Stdout, run.Stderr = os.Stderr, os.Stderr
	// Its own process group lets Listening find a listener a child of the
	// mapped command opened.
	run.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := run.Start(); err != nil {
		return Started{}, fmt.Errorf("launch the revision process: %w", err)
	}
	started := &launch{cmd: run, port: port, done: make(chan struct{})}
	go func() {
		started.exit = run.Wait()
		close(started.done)
	}()
	handle := "pid-" + strconv.Itoa(run.Process.Pid)
	p.mu.Lock()
	p.running[handle] = started
	p.mu.Unlock()
	return Started{Address: "127.0.0.1:" + strconv.Itoa(port), Container: handle, Peer: "127.0.0.1"}, nil
}

// Ready blocks until the revision process exits and reports its status. A
// process the runtime no longer tracks reports gone.
func (p *ProcessRuntime) Ready(ctx context.Context, container string) error {
	started, ok := p.lookup(container)
	if !ok {
		return errProcessGone
	}
	select {
	case <-ctx.Done():
		return fmt.Errorf("%w: %w", errProcessGone, ctx.Err())
	case <-started.done:
	}
	if exited, ok := errors.AsType[*exec.ExitError](started.exit); ok {
		return fmt.Errorf("%w: %s", errProcessExited, exited.String())
	}
	if started.exit != nil {
		return fmt.Errorf("%w: %w", errProcessGone, started.exit)
	}
	return nil
}

// Listening reports whether the revision process, or a process in its group,
// holds the listening socket on its port. Readiness requires it: another
// process can take the port between reserve and the revision's bind.
func (p *ProcessRuntime) Listening(container string) bool {
	started, ok := p.lookup(container)
	if !ok {
		return false
	}
	select {
	case <-started.done:
		return false
	default:
	}
	return groupListens(started.cmd.Process.Pid, started.port)
}

// Stop terminates the revision process and waits for its waiter to reap it.
// A process the runtime no longer tracks is success.
func (p *ProcessRuntime) Stop(_ context.Context, container string) error {
	started, ok := p.lookup(container)
	if !ok {
		return nil
	}
	// The whole group goes, so a server the mapped command forked does not
	// outlive the revision.
	_ = syscall.Kill(-started.cmd.Process.Pid, syscall.SIGKILL)
	<-started.done
	p.mu.Lock()
	delete(p.running, container)
	p.mu.Unlock()
	return nil
}

// lookup returns the tracked launch for one handle.
func (p *ProcessRuntime) lookup(container string) (*launch, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	started, ok := p.running[container]
	return started, ok
}
