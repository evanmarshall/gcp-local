package run

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"sync"

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
	running  map[string]*exec.Cmd
}

// NewProcessRuntime builds the dockerless runtime from an image-to-command
// map. An empty map is valid: every launch is then refused.
func NewProcessRuntime(commands map[string][]string) *ProcessRuntime {
	copied := map[string][]string{}
	for image, command := range commands {
		copied[image] = append([]string(nil), command...)
	}
	return &ProcessRuntime{commands: copied, running: map[string]*exec.Cmd{}}
}

// Start runs one revision process. The container contract stays identical:
// the process receives PORT, the explicit environment, and the runtime
// reports its loopback address and a process-handle identifier.
func (p *ProcessRuntime) Start(ctx context.Context, spec StartSpec) (Started, error) {
	command, ok := p.commands[spec.Image]
	if !ok {
		return Started{}, status.Errorf(codes.Unimplemented, "No process command is mapped for image %s", spec.Image)
	}
	var listen net.ListenConfig
	listener, err := listen.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return Started{}, fmt.Errorf("allocate the revision port: %w", err)
	}
	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		return Started{}, status.Errorf(codes.Internal, "The revision port is not a TCP address")
	}
	port := addr.Port
	if closeErr := listener.Close(); closeErr != nil {
		return Started{}, fmt.Errorf("reserve the revision port: %w", closeErr)
	}
	// The revision outlives the launch request; Stop owns termination. The
	// detached context satisfies CommandContext without coupling the
	// revision lifetime to the launch request.
	run := exec.CommandContext(context.WithoutCancel(ctx), command[0], command[1:]...) //nolint:gosec // the operator maps commands explicitly
	// Workloads get only their declared environment, never the core's own
	// variables (credentials, configuration, or proxy settings).
	run.Env = append(append([]string{"PATH=/usr/local/bin:/usr/bin:/bin"}, spec.Env...), "PORT="+strconv.Itoa(port))
	if err := run.Start(); err != nil {
		return Started{}, fmt.Errorf("launch the revision process: %w", err)
	}
	handle := "pid-" + strconv.Itoa(run.Process.Pid)
	p.mu.Lock()
	p.running[handle] = run
	p.mu.Unlock()
	return Started{Address: "127.0.0.1:" + strconv.Itoa(port), Container: handle}, nil
}

// Ready reports whether the revision process has exited, with its status.
// A process the runtime no longer tracks reports exited.
func (p *ProcessRuntime) Ready(_ context.Context, container string) error {
	run, ok := p.lookup(container)
	if !ok {
		return errProcessGone
	}
	state, waitErr := run.Process.Wait()
	switch {
	case waitErr == nil && !state.Success():
		return fmt.Errorf("%w: %s", errProcessExited, state.String())
	case waitErr != nil:
		return fmt.Errorf("%w: %w", errProcessGone, waitErr)
	}
	return nil
}

// Stop terminates the revision process. A process the runtime no longer
// tracks is success.
func (p *ProcessRuntime) Stop(_ context.Context, container string) error {
	run, ok := p.lookup(container)
	if !ok {
		return nil
	}
	_ = run.Process.Kill()
	_, _ = run.Process.Wait()
	p.mu.Lock()
	delete(p.running, container)
	p.mu.Unlock()
	return nil
}

// lookup returns the tracked launch for one handle.
func (p *ProcessRuntime) lookup(container string) (*exec.Cmd, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	run, ok := p.running[container]
	return run, ok
}
