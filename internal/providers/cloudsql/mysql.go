package cloudsql

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
)

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
	defer session.close()
	_, err = session.query(ctx, mysqlBootstrap(s.password, spec.RootPassword))
	return err
}

// waitMySQL polls until root@localhost can log in with its empty password.
func (s *server) waitMySQL(ctx context.Context) (*myConn, error) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := s.process.Ready(ctx); errors.Is(err, backend.ErrProcessExited) {
			return nil, fmt.Errorf("%w: %s", errMySQLExited, logTail(filepath.Join(s.directory, "error.log")))
		}
		if session, err := dialMySQL(ctx, "unix", s.socket, mysqlRootUser, ""); err == nil {
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
	return quoteMySQLLiteral(user) + "@" + quoteMySQLLiteral(host)
}

// mysqlIdentified renders the authentication clause of a built-in user:
// mysql_native_password with the password, or a locked account when there
// is none, as the PostgreSQL backend gives such a role no password.
func mysqlIdentified(password string) string {
	if password == "" {
		return " ACCOUNT LOCK"
	}
	return " IDENTIFIED WITH " + mysqlNativePassword + " BY " + quoteMySQLLiteral(password)
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
		"CREATE USER " + admin + " IDENTIFIED WITH " + mysqlNativePassword + " BY " + quoteMySQLLiteral(adminPassword) + "; " +
		"GRANT ALL PRIVILEGES ON *.* TO " + admin + " WITH GRANT OPTION; " +
		"CREATE ROLE " + mysqlAccount(superuserRole, mysqlAnyHost) + "; " +
		"GRANT " + mysqlRolePrivileges + " ON *.* TO " + mysqlAccount(superuserRole, mysqlAnyHost) + " WITH GRANT OPTION; " +
		mysqlCreateUser(mysqlRootUser, mysqlAnyHost, rootPassword) + "; " +
		"DROP USER " + mysqlAccount(mysqlRootUser, mysqlAdminHost)
}

// execMySQL runs SQL in one administrative session with a fixed sql_mode, so
// the literal quoting does not depend on a mode a user set globally.
func (s *server) execMySQL(ctx context.Context, sql string) ([][]string, error) {
	session, err := dialMySQL(ctx, "unix", s.socket, adminRole, s.password)
	if err != nil {
		return nil, err
	}
	defer session.close()
	return session.query(ctx, mysqlSessionMode+sql)
}
