package cloudsql

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
)

// Fleet sentinels.
var (
	errServerExists  = errors.New("a PostgreSQL server already exists for this instance")
	errServerMissing = errors.New("no PostgreSQL server exists for this instance")
	errVersion       = errors.New("database version is not installed")
	errFleetStopped  = errors.New("the Cloud SQL backend is not started")
	errSocketPath    = errors.New("PostgreSQL socket path exceeds the Unix limit")
)

// maxSocketPath keeps the socket under the 107-byte sun_path limit.
const maxSocketPath = 100

// fleet is one project's set of PostgreSQL servers. Each server keeps its
// data directory and Unix socket under one private temporary root. Servers
// listen on no TCP address: only the provider and the authorized connector
// reach the socket. Administrative sessions use a random per-server SCRAM
// secret that stays in this process's memory.
type fleet struct {
	versions map[string]distribution
	runAs    *syscall.Credential
	mu       sync.Mutex
	root     string
	next     int
	servers  map[string]*server
}

// server is one instance's running PostgreSQL.
type server struct {
	directory, socket, password string
	process                     *backend.Process
	done                        chan struct{}
}

func newFleet(versions map[string]distribution, runAs *syscall.Credential) *fleet {
	return &fleet{versions: versions, runAs: runAs, servers: map[string]*server{}}
}

// Start creates the private root; servers start per instance after authorization.
func (f *fleet) Start(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	root, err := os.MkdirTemp("", "gcp-local-cloudsql-")
	if err != nil {
		return fmt.Errorf("create the Cloud SQL directory: %w", err)
	}
	if err := f.own(root); err != nil {
		_ = os.RemoveAll(root)
		return err
	}
	f.root = root
	return nil
}

// own hands a directory or file to the unprivileged server user.
func (f *fleet) own(path string) error {
	if f.runAs == nil {
		return nil
	}
	if err := os.Chown(path, int(f.runAs.Uid), int(f.runAs.Gid)); err != nil {
		return fmt.Errorf("hand %s to the PostgreSQL user: %w", filepath.Base(path), err)
	}
	return nil
}

// Ready reports whether every started server is still accepting connections.
func (f *fleet) Ready(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.root == "" {
		return backend.ErrProcessExited
	}
	for _, name := range sortedKeys(f.servers) {
		if err := f.servers[name].process.Ready(ctx); err != nil {
			return fmt.Errorf("PostgreSQL for instance %s: %w", name, err)
		}
	}
	return nil
}

// Stop stops every server and removes the private root.
func (f *fleet) Stop(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	var failures []error
	for _, name := range sortedKeys(f.servers) {
		if err := f.servers[name].stop(ctx); err != nil {
			failures = append(failures, err)
			continue
		}
		delete(f.servers, name)
	}
	if len(failures) > 0 {
		return errors.Join(failures...)
	}
	if f.root != "" {
		if err := os.RemoveAll(f.root); err != nil {
			return fmt.Errorf("remove the Cloud SQL directory: %w", err)
		}
		f.root = ""
	}
	return nil
}

// Reset discards every server's data: the data directories live in the root.
func (f *fleet) Reset(ctx context.Context) error { return f.Stop(ctx) }

// create initializes, starts, and bootstraps one instance's server.
func (f *fleet) create(ctx context.Context, spec serverSpec) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	dist, ok := f.versions[spec.Version]
	if !ok {
		return fmt.Errorf("%w: %s", errVersion, spec.Version)
	}
	if f.root == "" {
		return errFleetStopped
	}
	if f.servers[spec.Instance] != nil {
		return errServerExists
	}
	f.next++
	s := &server{directory: filepath.Join(f.root, "i"+strconv.Itoa(f.next)), password: hex.EncodeToString(randomBytes(24)), done: make(chan struct{})}
	s.socket = filepath.Join(s.directory, "s", ".s.PGSQL.5432")
	if len(s.socket) > maxSocketPath {
		return errSocketPath
	}
	if err := f.initialize(ctx, dist, s); err != nil {
		_ = os.RemoveAll(s.directory)
		return err
	}
	if err := f.launch(ctx, dist, s, spec); err != nil {
		_ = s.stop(context.WithoutCancel(ctx))
		_ = os.RemoveAll(s.directory)
		return err
	}
	f.servers[spec.Instance] = s
	return nil
}

// environment is the complete child environment: no credentials or proxies.
func environment(dist distribution, home string) []string {
	return []string{"PATH=/usr/bin:/bin", "HOME=" + home, "TMPDIR=" + home, "LC_ALL=C", "LD_LIBRARY_PATH=" + filepath.Join(dist.root, "lib")}
}

// initialize runs initdb with SCRAM-only local authentication and no TCP access.
func (f *fleet) initialize(ctx context.Context, dist distribution, s *server) error {
	for _, directory := range []string{s.directory, filepath.Join(s.directory, "s")} {
		if err := os.Mkdir(directory, 0o700); err != nil {
			return fmt.Errorf("create the instance directory: %w", err)
		}
		if err := f.own(directory); err != nil {
			return err
		}
	}
	secret := filepath.Join(s.directory, "pw")
	if err := os.WriteFile(secret, []byte(s.password+"\n"), 0o600); err != nil {
		return fmt.Errorf("write the administrative secret: %w", err)
	}
	defer os.Remove(secret)
	if err := f.own(secret); err != nil {
		return err
	}
	command := exec.CommandContext(ctx, filepath.Join(dist.root, "bin", "initdb"), "-D", filepath.Join(s.directory, "data"), //nolint:gosec // pinned binary path
		"-U", adminRole, "--pwfile="+secret, "--auth-local=scram-sha-256", "--auth-host=reject", "--no-locale", "-E", "UTF8", "--no-sync")
	command.Env = environment(dist, s.directory)
	command.Dir = s.directory
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Credential: f.runAs}
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, &output
	if err := command.Run(); err != nil {
		return fmt.Errorf("initdb: %w: %s", err, bytes.TrimSpace(output.Bytes()[:min(output.Len(), 512)]))
	}
	return nil
}

// launch starts the server, waits for an administrative session, and creates
// the Cloud SQL role layout.
func (f *fleet) launch(ctx context.Context, dist distribution, s *server, spec serverSpec) error {
	settings := []string{"listen_addresses=", "unix_socket_directories=" + filepath.Dir(s.socket), "unix_socket_permissions=0700",
		"port=5432", "fsync=off", "synchronous_commit=off", "full_page_writes=off", "shared_buffers=16MB", "max_connections=50",
		"dynamic_shared_memory_type=mmap", "logging_collector=off", "password_encryption=scram-sha-256"}
	command := make([]string, 0, 3+2*len(settings))
	command = append(command, filepath.Join(dist.root, "bin", "postgres"), "-D", filepath.Join(s.directory, "data"))
	for _, setting := range settings {
		command = append(command, "-c", setting)
	}
	var err error
	s.process, err = backend.NewProcess(backend.ProcessConfig{
		Command: command, Directory: s.directory, Environment: environment(dist, s.directory), Credential: f.runAs,
		Probe:       s.probe,
		ResetAssets: func(context.Context) error { return nil },
	})
	if err != nil {
		return fmt.Errorf("configure PostgreSQL: %w", err)
	}
	if err := s.process.Start(ctx); err != nil {
		return fmt.Errorf("start PostgreSQL: %w", err)
	}
	if err := s.waitReady(ctx); err != nil {
		return err
	}
	_, err = s.exec(ctx, "postgres", bootstrap(spec.RootPassword))
	return err
}

// bootstrap creates the cloudsqlsuperuser group and the default postgres user.
// Without a root password the postgres user exists but cannot log in, as in
// Cloud SQL before its password is set.
func bootstrap(rootPassword string) string {
	password := "NULL"
	if rootPassword != "" {
		password = quoteLiteral(rootPassword)
	}
	return "CREATE ROLE " + superuserRole + " NOLOGIN CREATEDB CREATEROLE; " +
		"CREATE ROLE " + defaultUser + " LOGIN CREATEDB CREATEROLE IN ROLE " + superuserRole + " PASSWORD " + password + "; " +
		"GRANT ALL ON DATABASE postgres TO " + superuserRole + "; " +
		"GRANT ALL ON SCHEMA public TO " + superuserRole
}

// probe accepts a connection on the private socket.
func (s *server) probe(ctx context.Context) error {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", s.socket)
	if err != nil {
		return fmt.Errorf("dial PostgreSQL socket: %w", err)
	}
	return conn.Close() //nolint:wrapcheck // the probe only needs success or failure
}

// waitReady polls until an administrative session succeeds.
func (s *server) waitReady(ctx context.Context) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := s.process.Ready(ctx); errors.Is(err, backend.ErrProcessExited) {
			return fmt.Errorf("PostgreSQL exited during startup: %w", err)
		}
		if _, err := s.exec(ctx, "postgres", "SELECT 1"); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for PostgreSQL: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// exec runs SQL in one administrative session.
func (s *server) exec(ctx context.Context, database, sql string) ([][]string, error) {
	session, err := dialPostgres(ctx, "unix", s.socket, adminRole, database, s.password)
	if err != nil {
		return nil, err
	}
	defer session.close()
	return session.query(ctx, sql)
}

// stop signals connector sessions and stops the process.
func (s *server) stop(ctx context.Context) error {
	select {
	case <-s.done:
	default:
		close(s.done)
	}
	if s.process == nil {
		return nil
	}
	if err := s.process.Stop(ctx); err != nil {
		return fmt.Errorf("stop PostgreSQL: %w", err)
	}
	return nil
}

// remove stops one server and deletes its data directory.
func (f *fleet) remove(ctx context.Context, instance string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.servers[instance]
	if s == nil {
		return errServerMissing
	}
	if err := s.stop(ctx); err != nil {
		return err
	}
	delete(f.servers, instance)
	if err := os.RemoveAll(s.directory); err != nil {
		return fmt.Errorf("remove the instance directory: %w", err)
	}
	return nil
}

// exec runs administrative SQL on one instance's server.
func (f *fleet) exec(ctx context.Context, instance, database, sql string) ([][]string, error) {
	f.mu.Lock()
	s := f.servers[instance]
	f.mu.Unlock()
	if s == nil {
		return nil, errServerMissing
	}
	return s.exec(ctx, database, sql)
}

// endpoint returns one server's private socket for the connector.
func (f *fleet) endpoint(instance string) (string, <-chan struct{}, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.servers[instance]
	if s == nil {
		return "", nil, errServerMissing
	}
	return s.socket, s.done, nil
}
