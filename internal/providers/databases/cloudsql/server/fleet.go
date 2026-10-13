// Package server runs the Cloud SQL backend: one private PostgreSQL or MySQL
// server per instance, reachable only through a Unix socket under the
// fleet's private root. It verifies the pinned binaries, creates and removes
// servers, restarts them with database flags, and takes and restores
// physical backups. Authorization is the provider's: nothing here checks IAM.
package server

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/evanmarshall/gcp-local/internal/backend"
)

// pinned are the SHA-256 checksums of the installed server binaries per
// architecture and Cloud SQL databaseVersion. They mirror toolchains/postgres.json.
var pinned = map[string]map[string]struct{ initdb, postgres, release string }{
	"arm64": {
		"POSTGRES_16": {"d370000e13da111995726b463ffb64b4ebfc8713c10e5214d2a95f771d78db8d", "a3fc636a0a25fef762baa67ffd3f8f05b526ba2cd14236555523c368eeaeb0f3", "16_15"},
		"POSTGRES_15": {"d9e3af7ba8cbda3c201e9963861b39a2c4dedded5996eeefb5ee3d7c48939690", "bea46c1a96f495267a3bedbae03b2d3a1825f28fdbf184b78cd2016f7567d8e7", "15_19"},
	},
	"amd64": {
		"POSTGRES_16": {"b49dde31f6b284e39369dc4c8e4f55e83296da9721a05773a539deaccbb2e982", "ea144a02d075d44f28ef2247ec42f8817b45bc89da6e5b5e54313270982b61f6", "16_15"},
		"POSTGRES_15": {"b446b26a90fd3599d03efa401529073e5cf7a352af6e8d2b960598489e966ff7", "f7480af9888937ad8f4c4c9be62d61e84cb7062a907e016f81226229aa66e77d", "15_19"},
	},
}

// mysqlPinned are the SHA-256 checksums of the installed (section-stripped)
// mysqld binaries and clone plugins. They mirror the binaries entries of
// toolchains/mysql.json.
var mysqlPinned = map[string]map[string]struct{ mysqld, clone, release string }{
	"arm64": {
		"MYSQL_8_4": {"c572ff8332cd2f9600fb59cdcd472958f7cd8d755e7c030e5c7647769bcdf7ee", "09a80e3bd2a220e5410cd25488336328cd6a41408f9d1fca5ffda68705b4f4cc", "8_4_11"},
		"MYSQL_8_0": {"c90066c4c334253e2a3bd87063388a40259602a9b26d36e78f7ce5e5f5b3120b", "3ddaf66d7821442fdb941d23390454f72c42d8c7b20e18b2d199c3deebd7a510", "8_0_46"},
	},
	"amd64": {
		"MYSQL_8_4": {"b4f8367f8edb7ddc68db48803982aa800562d0f3e7009eb3ee17abd92948c31d", "3a9bd142cd937dc8fd4351f7eaffea8890c1aaca813aecbcb7a6b366e0ca9f56", "8_4_11"},
		"MYSQL_8_0": {"eb80259f78723db02f2bfeab0a39a9b10464a33176da4319c562297bbaae2d0a", "6822fc2f0744d66b6bd264c9c69ca7e7cdb18985569d89bdffc497e42cb6a8f5", "8_0_46"},
	},
}

// Backend configuration sentinels.
var (
	errPlatform      = errors.New("the Cloud SQL backends run only on linux/amd64 and linux/arm64")
	errInstallRoot   = errors.New("cloud SQL requires an absolute PostgreSQL or MySQL install root")
	errVersionsNone  = errors.New("no pinned PostgreSQL or MySQL version is installed under the install roots")
	errPinMismatch   = errors.New("database server binary does not match the committed pin")
	errRunAsRequired = errors.New("PostgreSQL and MySQL refuse to run as root; configure an unprivileged uid:gid")
)

// maxPinnedFile bounds hashing of the pinned binaries.
const maxPinnedFile = 128 << 20

// Account layout shared by both engines: the private administrative role,
// the Cloud SQL superuser role users are granted, and PostgreSQL's default user.
const (
	AdminRole     = "cloudsqladmin"
	SuperuserRole = "cloudsqlsuperuser"
	DefaultUser   = "postgres"
)

// Distribution is one verified server version.
type Distribution struct {
	root, release string
	mysql         bool
}

// Release is the installed patch release, as databaseInstalledVersion reports it.
func (d Distribution) Release() string { return d.release }

// Verify checks the binaries under the install roots against their pins
// and returns the usable versions. postgresRoot holds one directory per
// PostgreSQL major version (16, 15), as scripts/install/postgres.py lays
// them out; mysqlRoot holds 8.4 and 8.0 from scripts/install/mysql.py.
// Either may be empty.
func Verify(postgresRoot, mysqlRoot string, runAs *syscall.Credential) (map[string]Distribution, error) {
	pins, ok := pinned[runtime.GOARCH]
	if runtime.GOOS != "linux" || !ok {
		return nil, errPlatform
	}
	if err := checkRoots(postgresRoot, mysqlRoot, runAs); err != nil {
		return nil, err
	}
	versions := map[string]Distribution{}
	if postgresRoot != "" {
		if err := installed(postgresRoot, pins, versions); err != nil {
			return nil, err
		}
	}
	if mysqlRoot != "" {
		if err := installedMySQL(mysqlRoot, mysqlPinned[runtime.GOARCH], versions); err != nil {
			return nil, err
		}
	}
	if len(versions) == 0 {
		return nil, errVersionsNone
	}
	return versions, nil
}

// checkRoots requires absolute install roots, at least one, and an
// unprivileged server user when gcp-local runs as root.
func checkRoots(postgresRoot, mysqlRoot string, runAs *syscall.Credential) error {
	for _, root := range []string{postgresRoot, mysqlRoot} {
		if root != "" && !filepath.IsAbs(root) {
			return errInstallRoot
		}
	}
	if postgresRoot == "" && mysqlRoot == "" {
		return errInstallRoot
	}
	if os.Geteuid() == 0 && (runAs == nil || runAs.Uid == 0) {
		return errRunAsRequired
	}
	return nil
}

// installed verifies every installed PostgreSQL major version under root against its pins.
func installed(root string, pins map[string]struct{ initdb, postgres, release string }, versions map[string]Distribution) error {
	for version, pin := range pins {
		directory := filepath.Join(root, version[len("POSTGRES_"):])
		if _, err := os.Stat(filepath.Join(directory, "bin", "postgres")); errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err := verifyPin(filepath.Join(directory, "bin", "postgres"), pin.postgres); err != nil {
			return err
		}
		if err := verifyPin(filepath.Join(directory, "bin", "initdb"), pin.initdb); err != nil {
			return err
		}
		versions[version] = Distribution{root: directory, release: pin.release}
	}
	return nil
}

// installedMySQL verifies every installed MySQL version under root against its
// pins: MYSQL_8_4 lives in root/8.4. The clone plugin is verified when present.
func installedMySQL(root string, pins map[string]struct{ mysqld, clone, release string }, versions map[string]Distribution) error {
	for version, pin := range pins {
		directory := filepath.Join(root, strings.ReplaceAll(version[len("MYSQL_"):], "_", "."))
		if _, err := os.Stat(filepath.Join(directory, "bin", "mysqld")); errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err := verifyPin(filepath.Join(directory, "bin", "mysqld"), pin.mysqld); err != nil {
			return err
		}
		// An install from before the clone plugin serves everything but backups.
		if clone := filepath.Join(directory, "lib", "plugin", clonePlugin); fileExists(clone) {
			if err := verifyPin(clone, pin.clone); err != nil {
				return err
			}
		}
		versions[version] = Distribution{root: directory, release: pin.release, mysql: true}
	}
	return nil
}

// verifyPin compares one file's SHA-256 with its pin.
func verifyPin(path, want string) error {
	file, err := os.Open(path) //nolint:gosec // operator-configured absolute path
	if err != nil {
		return fmt.Errorf("open %s: %w", filepath.Base(path), err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, io.LimitReader(file, maxPinnedFile)); err != nil {
		return fmt.Errorf("hash %s: %w", filepath.Base(path), err)
	}
	if hex.EncodeToString(hash.Sum(nil)) != want {
		return fmt.Errorf("%w: %s", errPinMismatch, path)
	}
	return nil
}

// fileExists reports whether path names a regular file.
func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}

// IsMySQL reports whether a Cloud SQL databaseVersion is served by MySQL.
func IsMySQL(version string) bool { return strings.HasPrefix(version, "MYSQL_") }

// Spec describes one instance's server: its version, the initial password
// of the default user, and the database flags it starts with.
type Spec struct {
	Instance, Version, RootPassword string
	Flags                           []Flag
}

// Fleet sentinels.
var (
	ErrServerMissing = errors.New("no database server exists for this instance")
	errServerExists  = errors.New("a database server already exists for this instance")
	errVersion       = errors.New("database version is not installed")
	errFleetStopped  = errors.New("the Cloud SQL backend is not started")
	errSocketPath    = errors.New("database server socket path exceeds the Unix limit")
)

// maxSocketPath keeps the socket under the 107-byte sun_path limit.
const maxSocketPath = 100

// Fleet is one project's set of PostgreSQL and MySQL servers. Each server
// keeps its data directory, backups, and Unix socket under one private
// temporary root. Servers listen on no TCP address: only the provider and the
// connections it admits reach the socket. Administrative sessions use a
// random per-server secret (SCRAM for PostgreSQL, mysql_native_password for
// MySQL) that stays in this process's memory. The backend manager serializes
// every call that changes a server, so a restart never races another one.
type Fleet struct {
	versions map[string]Distribution
	runAs    *syscall.Credential
	mu       sync.Mutex
	root     string
	next     int
	servers  map[string]*server
}

// server is one instance's running PostgreSQL or MySQL server.
type server struct {
	directory, socket, password string
	dist                        Distribution
	flags                       []Flag
	process                     *backend.Process
	done                        chan struct{}
	// backups are the instance's physical backups by backup run ID.
	backups map[string]backup
}

// NewFleet returns a stopped fleet of the verified versions.
func NewFleet(versions map[string]Distribution, runAs *syscall.Credential) *Fleet {
	return &Fleet{versions: versions, runAs: runAs, servers: map[string]*server{}}
}

// Start creates the private root; servers start per instance after authorization.
func (f *Fleet) Start(context.Context) error {
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
func (f *Fleet) own(path string) error {
	if f.runAs == nil {
		return nil
	}
	if err := os.Lchown(path, int(f.runAs.Uid), int(f.runAs.Gid)); err != nil {
		return fmt.Errorf("hand %s to the database server user: %w", filepath.Base(path), err)
	}
	return nil
}

// Ready reports whether every started server is still accepting connections.
func (f *Fleet) Ready(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.root == "" {
		return backend.ErrProcessExited
	}
	for _, name := range slices.Sorted(maps.Keys(f.servers)) {
		if err := f.servers[name].process.Ready(ctx); err != nil {
			return fmt.Errorf("database server for instance %s: %w", name, err)
		}
	}
	return nil
}

// Stop stops every server and removes the private root.
func (f *Fleet) Stop(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	var failures []error
	for _, name := range slices.Sorted(maps.Keys(f.servers)) {
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

// Reset discards every server's data and backups: they live in the root.
func (f *Fleet) Reset(ctx context.Context) error { return f.Stop(ctx) }

// Create initializes, starts, and bootstraps one instance's server.
func (f *Fleet) Create(ctx context.Context, spec Spec) error {
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
	s := &server{directory: filepath.Join(f.root, "i"+strconv.Itoa(f.next)), password: rand.Text(),
		dist: dist, flags: append([]Flag(nil), spec.Flags...), backups: map[string]backup{}, done: make(chan struct{})}
	s.socket = filepath.Join(s.directory, "s", ".s.PGSQL.5432")
	if dist.mysql {
		s.socket = filepath.Join(s.directory, "s", "mysql.sock")
	}
	if len(s.socket) > maxSocketPath {
		return errSocketPath
	}
	if err := f.initialize(ctx, s); err != nil {
		_ = os.RemoveAll(s.directory)
		return err
	}
	if err := f.launch(ctx, s, spec.RootPassword); err != nil {
		_ = s.stop(context.WithoutCancel(ctx))
		_ = os.RemoveAll(s.directory)
		return err
	}
	f.servers[spec.Instance] = s
	return nil
}

// initialize creates the instance directories and the engine's data directory.
func (f *Fleet) initialize(ctx context.Context, s *server) error {
	for _, directory := range []string{s.directory, filepath.Join(s.directory, "s"), filepath.Join(s.directory, "backups")} {
		if err := os.Mkdir(directory, 0o700); err != nil {
			return fmt.Errorf("create the instance directory: %w", err)
		}
		if err := f.own(directory); err != nil {
			return err
		}
	}
	if s.dist.mysql {
		return f.initializeMySQL(ctx, s)
	}
	return f.initializePostgres(ctx, s)
}

// launch starts a new server and bootstraps the Cloud SQL account layout.
func (f *Fleet) launch(ctx context.Context, s *server, rootPassword string) error {
	if s.dist.mysql {
		return f.launchMySQL(ctx, s, rootPassword)
	}
	if err := f.startPostgres(ctx, s); err != nil {
		return err
	}
	_, err := s.exec(ctx, "postgres", bootstrap(rootPassword))
	return err
}

// restart starts an existing server's data directory again with its flags
// and waits for its administrative account.
func (f *Fleet) restart(ctx context.Context, s *server) error {
	s.done = make(chan struct{})
	if s.dist.mysql {
		return f.startMySQL(ctx, s)
	}
	return f.startPostgres(ctx, s)
}

// Configure restarts one server with new database flags, as Cloud SQL
// restarts an instance for a flag change. Open sessions end.
func (f *Fleet) Configure(ctx context.Context, instance string, flags []Flag) error {
	s, err := f.server(instance)
	if err != nil {
		return err
	}
	if err := s.stop(ctx); err != nil {
		return err
	}
	previous := s.flags
	s.flags = append([]Flag(nil), flags...)
	if err := f.restart(ctx, s); err != nil {
		// A flag the engine refused at startup: come back with the old ones.
		_ = s.stop(context.WithoutCancel(ctx))
		s.flags = previous
		return errors.Join(err, f.restart(context.WithoutCancel(ctx), s))
	}
	return nil
}

// server returns one instance's server under the fleet lock.
func (f *Fleet) server(instance string) (*server, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.servers[instance]
	if s == nil {
		return nil, ErrServerMissing
	}
	return s, nil
}

// environment is the complete child environment: no credentials or proxies.
func environment(dist Distribution, home string) []string {
	return []string{"PATH=/usr/bin:/bin", "HOME=" + home, "TMPDIR=" + home, "LC_ALL=C", "LD_LIBRARY_PATH=" + filepath.Join(dist.root, "lib")}
}

// probe accepts a connection on the private socket.
func (s *server) probe(ctx context.Context) error {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", s.socket)
	if err != nil {
		return fmt.Errorf("dial the database server socket: %w", err)
	}
	return conn.Close() //nolint:wrapcheck // the probe only needs success or failure
}

// newProcess builds the server's process from its command line.
func (s *server) newProcess(command []string, runAs *syscall.Credential) error {
	process, err := backend.NewProcess(backend.ProcessConfig{
		Command: command, Directory: s.directory, Environment: environment(s.dist, s.directory), Credential: runAs,
		Probe:       s.probe,
		ResetAssets: func(context.Context) error { return nil },
	})
	if err != nil {
		return fmt.Errorf("configure the database server: %w", err)
	}
	s.process = process
	return nil
}

// exec runs SQL in one administrative session.
func (s *server) exec(ctx context.Context, database, sql string) ([][]string, error) {
	if s.dist.mysql {
		return s.execMySQL(ctx, sql)
	}
	return s.execPostgres(ctx, database, sql)
}

// stop signals open sessions and stops the process.
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
		return fmt.Errorf("stop the database server: %w", err)
	}
	return nil
}

// Remove stops one server and deletes its data directory and backups.
func (f *Fleet) Remove(ctx context.Context, instance string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.servers[instance]
	if s == nil {
		return ErrServerMissing
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

// Exec runs administrative SQL on one instance's server; database selects
// the PostgreSQL database and is ignored by MySQL.
func (f *Fleet) Exec(ctx context.Context, instance, database, sql string) ([][]string, error) {
	s, err := f.server(instance)
	if err != nil {
		return nil, err
	}
	return s.exec(ctx, database, sql)
}

// Endpoint returns one server's private socket and a channel closed when the
// server stops, restarts, or is removed.
func (f *Fleet) Endpoint(instance string) (string, <-chan struct{}, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.servers[instance]
	if s == nil {
		return "", nil, ErrServerMissing
	}
	return s.socket, s.done, nil
}
