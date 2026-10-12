package cloudsql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/dataplane"
	"github.com/evanmarshall/gcp-local/internal/providers/databases/cloudsql/mysqlwire"
	"github.com/evanmarshall/gcp-local/internal/providers/databases/cloudsql/pgwire"
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
// mysqld binaries. They mirror the binaries entries of toolchains/mysql.json.
var mysqlPinned = map[string]map[string]struct{ mysqld, release string }{
	"arm64": {
		"MYSQL_8_4": {"c572ff8332cd2f9600fb59cdcd472958f7cd8d755e7c030e5c7647769bcdf7ee", "8_4_11"},
		"MYSQL_8_0": {"c90066c4c334253e2a3bd87063388a40259602a9b26d36e78f7ce5e5f5b3120b", "8_0_46"},
	},
	"amd64": {
		"MYSQL_8_4": {"b4f8367f8edb7ddc68db48803982aa800562d0f3e7009eb3ee17abd92948c31d", "8_4_11"},
		"MYSQL_8_0": {"eb80259f78723db02f2bfeab0a39a9b10464a33176da4319c562297bbaae2d0a", "8_0_46"},
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

// Config locates the installed binaries. Root holds one directory per
// PostgreSQL major version (Root/16, Root/15), as scripts/install/postgres.py
// lays them out; MySQLRoot holds Root/8.4 and Root/8.0 from
// scripts/install/mysql.py. Either may be empty. RunAs is the unprivileged
// user the servers run as when gcp-local runs as root. Publish, when set,
// gives every public-IP instance a port from the data-plane range.
type Config struct {
	Root, MySQLRoot string
	RunAs           *syscall.Credential
	Publish         *dataplane.Pool
}

// distribution is one verified server version.
type distribution struct {
	root, release string
	mysql         bool
}

// validate verifies the installed binaries and returns the usable versions.
func (c Config) validate() (map[string]distribution, error) {
	pins, ok := pinned[runtime.GOARCH]
	if runtime.GOOS != "linux" || !ok {
		return nil, errPlatform
	}
	if !c.rootsValid() {
		return nil, errInstallRoot
	}
	if os.Geteuid() == 0 && (c.RunAs == nil || c.RunAs.Uid == 0) {
		return nil, errRunAsRequired
	}
	versions := map[string]distribution{}
	if err := c.installed(pins, mysqlPinned[runtime.GOARCH], versions); err != nil {
		return nil, err
	}
	if len(versions) == 0 {
		return nil, errVersionsNone
	}
	return versions, nil
}

// installed verifies the versions under every configured install root.
func (c Config) installed(pins map[string]struct{ initdb, postgres, release string }, mysql map[string]struct{ mysqld, release string }, versions map[string]distribution) error {
	if c.Root != "" {
		if err := installed(c.Root, pins, versions); err != nil {
			return err
		}
	}
	if c.MySQLRoot != "" {
		return installedMySQL(c.MySQLRoot, mysql, versions)
	}
	return nil
}

// rootsValid reports whether at least one install root is set and every set root is absolute.
func (c Config) rootsValid() bool {
	for _, root := range []string{c.Root, c.MySQLRoot} {
		if root != "" && !filepath.IsAbs(root) {
			return false
		}
	}
	return c.Root != "" || c.MySQLRoot != ""
}

// installed verifies every installed PostgreSQL major version under root against its pins.
func installed(root string, pins map[string]struct{ initdb, postgres, release string }, versions map[string]distribution) error {
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
		versions[version] = distribution{root: directory, release: pin.release}
	}
	return nil
}

// installedMySQL verifies every installed MySQL version under root against its
// pins: MYSQL_8_4 lives in root/8.4.
func installedMySQL(root string, pins map[string]struct{ mysqld, release string }, versions map[string]distribution) error {
	for version, pin := range pins {
		directory := filepath.Join(root, strings.ReplaceAll(version[len("MYSQL_"):], "_", "."))
		if _, err := os.Stat(filepath.Join(directory, "bin", "mysqld")); errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err := verifyPin(filepath.Join(directory, "bin", "mysqld"), pin.mysqld); err != nil {
			return err
		}
		versions[version] = distribution{root: directory, release: pin.release, mysql: true}
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

// serverSpec describes one instance's server.
type serverSpec struct {
	Instance, Version, RootPassword string
}

// driver is the backend contract the provider uses: a project's fleet of
// PostgreSQL and MySQL servers, one per Cloud SQL instance.
type driver interface {
	backend.Backend
	create(ctx context.Context, spec serverSpec) error
	remove(ctx context.Context, instance string) error
	// exec runs SQL as the private administrative account; database selects
	// the PostgreSQL database and is ignored by MySQL.
	exec(ctx context.Context, instance, database, sql string) ([][]string, error)
	// endpoint returns the server's private socket and a channel closed when
	// the server is removed or stopped.
	endpoint(instance string) (string, <-chan struct{}, error)
}

// errBackendType reports a backend that is not a Cloud SQL driver.
var errBackendType = errors.New("cloud SQL backend is not a database server fleet")

// driverOf narrows the manager's backend to the fleet contract.
func driverOf(active backend.Backend) (driver, error) {
	value, ok := active.(driver)
	if !ok {
		return nil, errBackendType
	}
	return value, nil
}

// sortedKeys returns a map's keys in order.
func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// Fleet sentinels.
var (
	errServerExists  = errors.New("a database server already exists for this instance")
	errServerMissing = errors.New("no database server exists for this instance")
	errVersion       = errors.New("database version is not installed")
	errFleetStopped  = errors.New("the Cloud SQL backend is not started")
	errSocketPath    = errors.New("database server socket path exceeds the Unix limit")
)

// maxSocketPath keeps the socket under the 107-byte sun_path limit.
const maxSocketPath = 100

// fleet is one project's set of PostgreSQL and MySQL servers. Each server
// keeps its data directory and Unix socket under one private temporary root.
// Servers listen on no TCP address: only the provider and the authorized
// connector reach the socket. Administrative sessions use a random per-server
// secret (SCRAM for PostgreSQL, mysql_native_password for MySQL) that stays in
// this process's memory.
type fleet struct {
	versions map[string]distribution
	runAs    *syscall.Credential
	mu       sync.Mutex
	root     string
	next     int
	servers  map[string]*server
}

// server is one instance's running PostgreSQL or MySQL server.
type server struct {
	directory, socket, password string
	mysql                       bool
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
	s := &server{directory: filepath.Join(f.root, "i"+strconv.Itoa(f.next)), password: hex.EncodeToString(randomBytes(24)),
		mysql: dist.mysql, done: make(chan struct{})}
	s.socket = filepath.Join(s.directory, "s", ".s.PGSQL.5432")
	initialize, launch := f.initialize, f.launch
	if s.mysql {
		s.socket = filepath.Join(s.directory, "s", "mysql.sock")
		initialize, launch = f.initializeMySQL, f.launchMySQL
	}
	if len(s.socket) > maxSocketPath {
		return errSocketPath
	}
	if err := initialize(ctx, dist, s); err != nil {
		_ = os.RemoveAll(s.directory)
		return err
	}
	if err := launch(ctx, dist, s, spec); err != nil {
		_ = s.stop(context.WithoutCancel(ctx))
		_ = os.RemoveAll(s.directory)
		return err
	}
	f.servers[spec.Instance] = s
	return nil
}

// instanceDirectories creates the instance directory and its socket
// directory, owned by the server user.
func (f *fleet) instanceDirectories(s *server) error {
	for _, directory := range []string{s.directory, filepath.Join(s.directory, "s")} {
		if err := os.Mkdir(directory, 0o700); err != nil {
			return fmt.Errorf("create the instance directory: %w", err)
		}
		if err := f.own(directory); err != nil {
			return err
		}
	}
	return nil
}

// environment is the complete child environment: no credentials or proxies.
func environment(dist distribution, home string) []string {
	return []string{"PATH=/usr/bin:/bin", "HOME=" + home, "TMPDIR=" + home, "LC_ALL=C", "LD_LIBRARY_PATH=" + filepath.Join(dist.root, "lib")}
}

// initialize runs initdb with SCRAM-only local authentication and no TCP access.
func (f *fleet) initialize(ctx context.Context, dist distribution, s *server) error {
	if err := f.instanceDirectories(s); err != nil {
		return err
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
		password = pgwire.QuoteLiteral(rootPassword)
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
	if s.mysql {
		return s.execMySQL(ctx, sql)
	}
	session, err := pgwire.Dial(ctx, "unix", s.socket, adminRole, database, s.password)
	if err != nil {
		return nil, fmt.Errorf("open an administrative session: %w", err)
	}
	defer session.Close()
	rows, err := session.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("run administrative SQL: %w", err)
	}
	return rows, nil
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

// MySQL account layout. Cloud SQL for MySQL creates root@% (rootPassword) and
// gives users created through the API the cloudsqlsuperuser role; the
// administrative account and the role are hidden from users.list.
const (
	mysqlRootUser    = "root"
	mysqlAnyHost     = "%"
	mysqlAdminHost   = "localhost"
	mysqlSessionMode = "SET SESSION sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE," +
		"ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'; "
	// mysqlRolePrivileges approximates cloudsqlsuperuser: everything except
	// SUPER, FILE, SHUTDOWN, and the dynamic administrative privileges
	// (including SYSTEM_USER, which protects the administrative account).
	mysqlRolePrivileges = "SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, REFERENCES, INDEX, ALTER, SHOW DATABASES, " +
		"CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER, " +
		"CREATE ROLE, DROP ROLE"
)

// mysqlHidden are the accounts users.list never shows.
var mysqlHidden = []string{adminRole, superuserRole, "mysql.sys", "mysql.session", "mysql.infoschema"}

// mysqlSystemSchemas are the schemas databases.delete refuses to drop.
var mysqlSystemSchemas = map[string]bool{"mysql": true, "information_schema": true, "performance_schema": true, "sys": true}

// errMySQLExited reports a server that stopped during startup.
var errMySQLExited = errors.New("MySQL exited during startup")

// isMySQL reports whether a Cloud SQL databaseVersion is served by MySQL.
func isMySQL(version string) bool { return strings.HasPrefix(version, "MYSQL_") }

// mysqlTuning keeps each server small and fast to create: no TCP or X
// Protocol listener, no TLS (the connector forwards cleartext over loopback),
// no binary log, and durability off, as for PostgreSQL.
func mysqlTuning(dist distribution, s *server) []string {
	return []string{"--basedir=" + dist.root, "--datadir=" + filepath.Join(s.directory, "data"), "--tmpdir=" + s.directory,
		"--innodb-redo-log-capacity=32M", "--innodb-buffer-pool-size=16M", "--innodb-log-buffer-size=1M",
		"--innodb-flush-log-at-trx-commit=0", "--innodb-doublewrite=OFF", "--skip-log-bin", "--sync-binlog=0"}
}

// initializeMySQL creates the data directory with mysqld --initialize-insecure:
// root@localhost has an empty password until launchMySQL replaces it.
func (f *fleet) initializeMySQL(ctx context.Context, dist distribution, s *server) error {
	if err := f.instanceDirectories(s); err != nil {
		return err
	}
	arguments := append([]string{"--no-defaults", "--initialize-insecure"}, mysqlTuning(dist, s)...)
	arguments = append(arguments, "--log-error="+filepath.Join(s.directory, "initialize.log"))
	command := exec.CommandContext(ctx, filepath.Join(dist.root, "bin", "mysqld"), arguments...) //nolint:gosec // pinned binary path
	command.Env = environment(dist, s.directory)
	command.Dir = s.directory
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Credential: f.runAs}
	if err := command.Run(); err != nil {
		return fmt.Errorf("mysqld --initialize: %w: %s", err, logTail(filepath.Join(s.directory, "initialize.log")))
	}
	return nil
}

// logTail returns the end of a server log for an error message.
func logTail(path string) []byte {
	data, _ := os.ReadFile(path) //nolint:gosec // the instance's own log
	return bytes.TrimSpace(data[max(0, len(data)-512):])
}

// launchMySQL starts the server on its private socket, then replaces the
// initial root@localhost with the administrative account and creates the
// Cloud SQL account layout.
func (f *fleet) launchMySQL(ctx context.Context, dist distribution, s *server, spec serverSpec) error {
	command := append([]string{filepath.Join(dist.root, "bin", "mysqld"), "--no-defaults"}, mysqlTuning(dist, s)...)
	command = append(command, "--socket="+s.socket, "--pid-file="+filepath.Join(s.directory, "mysqld.pid"),
		"--log-error="+filepath.Join(s.directory, "error.log"), "--skip-networking", "--mysqlx=OFF", "--tls-version=",
		"--performance-schema=OFF", "--max-connections=50", "--secure-file-priv=NULL", "--skip-name-resolve")
	if strings.HasPrefix(dist.release, "8_4") {
		// MySQL 8.4 no longer loads mysql_native_password by default.
		command = append(command, "--mysql-native-password=ON")
	}
	var err error
	s.process, err = backend.NewProcess(backend.ProcessConfig{
		Command: command, Directory: s.directory, Environment: environment(dist, s.directory), Credential: f.runAs,
		Probe:       s.probe,
		ResetAssets: func(context.Context) error { return nil },
	})
	if err != nil {
		return fmt.Errorf("configure MySQL: %w", err)
	}
	if err := s.process.Start(ctx); err != nil {
		return fmt.Errorf("start MySQL: %w", err)
	}
	session, err := s.waitMySQL(ctx)
	if err != nil {
		return err
	}
	defer session.Close()
	if _, err := session.Query(ctx, mysqlBootstrap(s.password, spec.RootPassword)); err != nil {
		return fmt.Errorf("bootstrap MySQL: %w", err)
	}
	return nil
}

// waitMySQL polls until root@localhost can log in with its empty password.
func (s *server) waitMySQL(ctx context.Context) (*mysqlwire.Conn, error) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := s.process.Ready(ctx); errors.Is(err, backend.ErrProcessExited) {
			return nil, fmt.Errorf("%w: %s", errMySQLExited, logTail(filepath.Join(s.directory, "error.log")))
		}
		if session, err := mysqlwire.Dial(ctx, "unix", s.socket, mysqlRootUser, ""); err == nil {
			return session, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for MySQL: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// mysqlAccount renders 'user'@'host'.
func mysqlAccount(user, host string) string {
	return mysqlwire.QuoteLiteral(user) + "@" + mysqlwire.QuoteLiteral(host)
}

// mysqlIdentified renders the authentication clause of a built-in user:
// mysql_native_password with the password, or a locked account when there
// is none, as the PostgreSQL backend gives such a role no password.
func mysqlIdentified(password string) string {
	if password == "" {
		return " ACCOUNT LOCK"
	}
	return " IDENTIFIED WITH " + mysqlwire.NativePassword + " BY " + mysqlwire.QuoteLiteral(password)
}

// mysqlCreateUser creates a built-in user in the cloudsqlsuperuser role.
func mysqlCreateUser(user, host, password string) string {
	account, role := mysqlAccount(user, host), mysqlAccount(superuserRole, mysqlAnyHost)
	return "CREATE USER " + account + mysqlIdentified(password) + "; GRANT " + role + " TO " + account +
		"; SET DEFAULT ROLE " + role + " TO " + account
}

// mysqlBootstrap creates the administrative account (holding SYSTEM_USER,
// so role members cannot alter it), the cloudsqlsuperuser role, and root@%,
// then drops the initial root@localhost.
func mysqlBootstrap(adminPassword, rootPassword string) string {
	admin := mysqlAccount(adminRole, mysqlAdminHost)
	return mysqlSessionMode +
		"CREATE USER " + admin + " IDENTIFIED WITH " + mysqlwire.NativePassword + " BY " + mysqlwire.QuoteLiteral(adminPassword) + "; " +
		"GRANT ALL PRIVILEGES ON *.* TO " + admin + " WITH GRANT OPTION; " +
		"CREATE ROLE " + mysqlAccount(superuserRole, mysqlAnyHost) + "; " +
		"GRANT " + mysqlRolePrivileges + " ON *.* TO " + mysqlAccount(superuserRole, mysqlAnyHost) + " WITH GRANT OPTION; " +
		mysqlCreateUser(mysqlRootUser, mysqlAnyHost, rootPassword) + "; " +
		"DROP USER " + mysqlAccount(mysqlRootUser, mysqlAdminHost)
}

// execMySQL runs SQL in one administrative session with a fixed sql_mode, so
// the literal quoting does not depend on a mode a user set globally.
func (s *server) execMySQL(ctx context.Context, sql string) ([][]string, error) {
	session, err := mysqlwire.Dial(ctx, "unix", s.socket, adminRole, s.password)
	if err != nil {
		return nil, fmt.Errorf("open an administrative session: %w", err)
	}
	defer session.Close()
	rows, err := session.Query(ctx, mysqlSessionMode+sql)
	if err != nil {
		return nil, fmt.Errorf("run administrative SQL: %w", err)
	}
	return rows, nil
}
