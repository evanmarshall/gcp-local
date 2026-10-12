package memorystore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
)

// ServerVersion is the pinned official Redis source release (toolchains/redis.json).
const ServerVersion = "7.2.16"

// adminUser is the provider's private ACL user on each server. Its password is
// random per server and never leaves the provider.
const adminUser = "gcp-local-admin"

// blockedCommands are the commands Memorystore for Redis documents as blocked
// (answered with NOPERM); SLAVEOF is REPLICAOF's alias. They are removed from
// the client-facing default user with ACL rules, so the error class matches.
var blockedCommands = []string{
	"acl", "bgrewriteaof", "bgsave", "client", "cluster", "command", "config", "debug",
	"lastsave", "migrate", "object", "replicaof", "slaveof", "save", "shutdown",
}

// Backend configuration sentinels.
var (
	errServerPath    = errors.New("memorystore requires an absolute redis-server path")
	errServerMissing = errors.New("redis-server is not an executable file")
	errServerVersion = errors.New("redis-server does not report the pinned version")
	errUnknownServer = errors.New("redis instance has no running server")
	errServerRunning = errors.New("redis instance already has a server")
)

// Config locates the redis-server built by scripts/install_redis.py.
type Config struct {
	Server string
}

// validate checks the binary and that it reports the pinned release.
func (c Config) validate(ctx context.Context) error {
	if !filepath.IsAbs(c.Server) {
		return errServerPath
	}
	info, err := os.Stat(c.Server)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return errServerMissing
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, c.Server, "--version") //nolint:gosec // operator-configured absolute path
	command.Env = []string{"PATH=/usr/bin:/bin"}
	output, err := command.Output()
	if err != nil {
		return fmt.Errorf("query redis-server version: %w", err)
	}
	if !strings.Contains(string(output), " v="+ServerVersion+" ") {
		return fmt.Errorf("%w: want %s", errServerVersion, ServerVersion)
	}
	return nil
}

// launchSpec is what a new server needs from the provider's instance record.
type launchSpec struct {
	AuthString string // empty disables AUTH, as authEnabled=false does
	MemoryGB   int64
}

// driver is the backend contract the provider uses: the manager lifecycle plus
// per-instance server control inside one project/region scope.
type driver interface {
	backend.Backend
	launch(ctx context.Context, id string, spec launchSpec) (int, error)
	resize(ctx context.Context, id string, memoryGB int64) error
	remove(ctx context.Context, id string) error
}

// fleet owns every Redis server in one project/region scope under a private
// temporary directory. The backend manager starts it lazily, probes it, and
// stops it on reset or shutdown; the provider launches and removes servers
// inside Manager.Use, so these calls are serialized per scope.
type fleet struct {
	config    Config
	mu        sync.Mutex
	directory string
	servers   map[string]*server
}

// server is one redis-server process bound to a loopback port.
type server struct {
	process   *backend.Process
	directory string
	address   string
	admin     string
}

func newFleet(config Config) *fleet { return &fleet{config: config} }

func (f *fleet) Start(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	directory, err := os.MkdirTemp("", "gcp-local-redis-")
	if err != nil {
		return fmt.Errorf("create the redis directory: %w", err)
	}
	f.directory, f.servers = directory, map[string]*server{}
	return nil
}

// Ready probes every running server; an exited server fails the scope.
func (f *fleet) Ready(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.servers == nil {
		return backend.ErrProcessExited
	}
	for id, current := range f.servers {
		if err := current.process.Ready(ctx); err != nil {
			return fmt.Errorf("redis instance %s readiness: %w", id, err)
		}
	}
	return nil
}

// Stop terminates every server and removes the scope's directory. A failed
// stop keeps the directory for diagnosis and leaves the manager failed.
func (f *fleet) Stop(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	var failures []error
	for id, current := range f.servers {
		if err := current.process.Stop(ctx); err != nil {
			failures = append(failures, fmt.Errorf("stop redis instance %s: %w", id, err))
		}
	}
	if len(failures) > 0 {
		return errors.Join(failures...)
	}
	f.servers = nil
	if f.directory != "" {
		if err := os.RemoveAll(f.directory); err != nil {
			return fmt.Errorf("remove the redis directory: %w", err)
		}
		f.directory = ""
	}
	return nil
}

// Reset runs after Stop; servers keep all data in memory, so nothing remains.
func (f *fleet) Reset(ctx context.Context) error { return f.Stop(ctx) }

// launch starts one server and waits until it answers its admin probe.
func (f *fleet) launch(ctx context.Context, id string, spec launchSpec) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.servers == nil {
		return 0, backend.ErrProcessExited
	}
	if f.servers[id] != nil {
		return 0, errServerRunning
	}
	current, port, err := f.prepare(ctx, id, spec)
	if err != nil {
		return 0, err
	}
	if err := current.process.Start(ctx); err != nil {
		_ = os.RemoveAll(current.directory)
		return 0, fmt.Errorf("start redis-server: %w", err)
	}
	if err := waitReady(ctx, current.process); err != nil {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = current.process.Stop(cleanup)
		_ = os.RemoveAll(current.directory)
		return 0, err
	}
	f.servers[id] = current
	return port, nil
}

// prepare reserves a loopback port and writes the owner-only configuration.
func (f *fleet) prepare(ctx context.Context, id string, spec launchSpec) (*server, int, error) {
	var listen net.ListenConfig
	listener, err := listen.Listen(ctx, "tcp", loopbackHost+":0")
	if err != nil {
		return nil, 0, fmt.Errorf("reserve a redis port: %w", err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	_, portText, err := net.SplitHostPort(address)
	if err != nil {
		return nil, 0, fmt.Errorf("parse the redis port: %w", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return nil, 0, fmt.Errorf("parse the redis port: %w", err)
	}
	admin, err := secret(32)
	if err != nil {
		return nil, 0, err
	}
	directory := filepath.Join(f.directory, id)
	if err := os.Mkdir(directory, 0o700); err != nil {
		return nil, 0, fmt.Errorf("create the instance directory: %w", err)
	}
	configPath := filepath.Join(directory, "redis.conf")
	if err := os.WriteFile(configPath, []byte(serverConfig(port, directory, admin, spec)), 0o600); err != nil {
		_ = os.RemoveAll(directory)
		return nil, 0, fmt.Errorf("write the redis configuration: %w", err)
	}
	current := &server{directory: directory, address: address, admin: admin}
	current.process, err = backend.NewProcess(backend.ProcessConfig{
		Command:     []string{f.config.Server, configPath},
		Directory:   directory,
		Environment: []string{"PATH=/usr/bin:/bin", "HOME=" + directory, "TMPDIR=" + directory},
		Probe:       current.probe,
		ResetAssets: func(context.Context) error { return nil },
	})
	if err != nil {
		_ = os.RemoveAll(directory)
		return nil, 0, fmt.Errorf("configure redis-server: %w", err)
	}
	return current, port, nil
}

// serverConfig renders one server's configuration. Secrets live only in this
// owner-only file, never on the command line. The client-facing default user
// has the documented blocked commands removed; AUTH is required only when the
// instance enables it.
func serverConfig(port int, directory, admin string, spec launchSpec) string {
	password := "nopass"
	if spec.AuthString != "" {
		password = ">" + spec.AuthString
	}
	rules := make([]string, 0, len(blockedCommands))
	for _, command := range blockedCommands {
		rules = append(rules, "-"+command)
	}
	lines := []string{
		"bind " + loopbackHost,
		"port " + strconv.Itoa(port),
		"protected-mode yes",
		"daemonize no",
		"dir " + directory,
		`save ""`,
		"appendonly no",
		"maxmemory " + strconv.FormatInt(spec.MemoryGB, 10) + "gb",
		"maxmemory-policy volatile-lru",
		"user default on " + password + " ~* &* +@all " + strings.Join(rules, " "),
		"user " + adminUser + " on >" + admin + " ~* &* +@all",
	}
	return strings.Join(lines, "\n") + "\n"
}

// probe authenticates as the private admin user and pings the server.
func (s *server) probe(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	conn, err := s.adminConn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	reply, err := conn.do("PING")
	if err != nil {
		return err
	}
	if reply != "PONG" {
		return errReplyShape
	}
	return nil
}

// adminConn opens an authenticated admin connection.
func (s *server) adminConn(ctx context.Context) (*respConn, error) {
	conn, err := dialRESP(ctx, s.address)
	if err != nil {
		return nil, err
	}
	if _, err := conn.do("AUTH", adminUser, s.admin); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

// resize applies a new memory limit. Memorystore preserves data when a
// Basic Tier instance is scaled, so the keyspace is kept.
func (f *fleet) resize(ctx context.Context, id string, memoryGB int64) error {
	f.mu.Lock()
	current := f.servers[id]
	f.mu.Unlock()
	if current == nil {
		return errUnknownServer
	}
	conn, err := current.adminConn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.do("CONFIG", "SET", "maxmemory", strconv.FormatInt(memoryGB, 10)+"gb"); err != nil {
		return err
	}
	return nil
}

// remove stops one server and deletes its directory.
func (f *fleet) remove(ctx context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	current := f.servers[id]
	if current == nil {
		return errUnknownServer
	}
	if err := current.process.Stop(ctx); err != nil {
		return fmt.Errorf("stop redis-server: %w", err)
	}
	delete(f.servers, id)
	if err := os.RemoveAll(current.directory); err != nil {
		return fmt.Errorf("remove the instance directory: %w", err)
	}
	return nil
}

// waitReady polls one new server until it answers or ctx ends.
func waitReady(ctx context.Context, process *backend.Process) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		err := process.Ready(ctx)
		if err == nil {
			return nil
		}
		if errors.Is(err, backend.ErrProcessExited) {
			return fmt.Errorf("redis-server readiness: %w", err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("redis-server readiness: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// secret returns n random bytes as hex.
func secret(n int) (string, error) {
	raw := make([]byte, n)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate a secret: %w", err)
	}
	return hex.EncodeToString(raw), nil
}
