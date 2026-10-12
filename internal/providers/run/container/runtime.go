package container

import (
	"context"
	"errors"
	"time"
)

// errNoRuntime reports a host without a container engine.
var errNoRuntime = errors.New("no container runtime is configured")

// Runtime owns one container per revision. The provider never touches the
// container engine directly: LocalStack-style runtime ownership stays in this
// seam, and the Docker executor is the only implementation with host access.
type Runtime interface {
	// Start launches the image with the given environment and port and returns
	// its handle: the loopback address and the engine's container identifier,
	// neither of which implies the application is serving yet.
	Start(ctx context.Context, spec StartSpec) (Started, error)
	// Ready reports whether the container has exited (with an error) or keeps
	// running. It never blocks on application-level health probes.
	Ready(ctx context.Context, container string) (exit error)
	// Stop terminates and removes the container. A missing container is success.
	Stop(ctx context.Context, container string) error
}

// Started is the handle of one launched container: the address the
// invocation proxy dials, the engine identifier, and Peer, the IP address the
// workload's own connections come from. Only Peer may use the revision's
// metadata server.
type Started struct {
	Address   string
	Container string
	Peer      string
}

// StartSpec is the provider-owned container contract: only the image, port,
// and explicit environment reach the engine.
type StartSpec struct {
	Image string
	Port  int
	Env   []string
	// IdleTimeout stops the container after its last invocation when nonzero.
	IdleTimeout time.Duration
}

// NoopRuntime is the exported refusal runtime for hosts without Docker.
type NoopRuntime struct{}

func (NoopRuntime) Start(context.Context, StartSpec) (Started, error) { return Started{}, errNoRuntime }
func (NoopRuntime) Ready(context.Context, string) error               { return nil }
func (NoopRuntime) Stop(context.Context, string) error                { return nil }
