package server

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
	"github.com/evanmarshall/gcp-local/internal/providers/databases/cloudsql/server/mysqlwire"
)

// MySQL account layout. Cloud SQL for MySQL creates root@% (rootPassword) and
// gives users created through the API the cloudsqlsuperuser role.
const (
	MySQLRootUser    = "root"
	MySQLAnyHost     = "%"
	mysqlAdminHost   = "localhost"
	mysqlSessionMode = "SET SESSION sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE," +
		"ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'; "
	// mysqlRolePrivileges approximates cloudsqlsuperuser: everything except
	// SUPER, FILE, SHUTDOWN, and the dynamic administrative privileges
	// (including SYSTEM_USER, which protects the administrative account).
	mysqlRolePrivileges = "SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, REFERENCES, INDEX, ALTER, SHOW DATABASES, " +
		"CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER, " +
		"CREATE ROLE, DROP ROLE"
	// clonePlugin takes MySQL's online physical backups.
	clonePlugin = "mysql_clone.so"
)

// errMySQLExited reports a server that stopped during startup.
var errMySQLExited = errors.New("MySQL exited during startup")

// mysqlTuning keeps each server small and fast to create: no TCP or X
// Protocol listener, no TLS (the provider terminates TLS), no binary log, and
// durability off, as for PostgreSQL.
func mysqlTuning(s *server) []string {
	return []string{"--basedir=" + s.dist.root, "--datadir=" + s.data(), "--tmpdir=" + s.directory,
		"--innodb-redo-log-capacity=32M", "--innodb-buffer-pool-size=16M", "--innodb-log-buffer-size=1M",
		"--innodb-flush-log-at-trx-commit=0", "--innodb-doublewrite=OFF", "--skip-log-bin", "--sync-binlog=0"}
}

// initializeMySQL creates the data directory with mysqld --initialize-insecure:
// root@localhost has an empty password until launchMySQL replaces it.
func (f *Fleet) initializeMySQL(ctx context.Context, s *server) error {
	arguments := append([]string{"--no-defaults", "--initialize-insecure"}, mysqlTuning(s)...)
	arguments = append(arguments, "--log-error="+filepath.Join(s.directory, "initialize.log"))
	command := exec.CommandContext(ctx, filepath.Join(s.dist.root, "bin", "mysqld"), arguments...) //nolint:gosec // pinned binary path
	command.Env = environment(s.dist, s.directory)
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

// startMySQLProcess starts mysqld on its private socket with its flags.
func (f *Fleet) startMySQLProcess(ctx context.Context, s *server) error {
	command := append([]string{filepath.Join(s.dist.root, "bin", "mysqld"), "--no-defaults"}, mysqlTuning(s)...)
	command = append(command, "--socket="+s.socket, "--pid-file="+filepath.Join(s.directory, "mysqld.pid"),
		"--log-error="+filepath.Join(s.directory, "error.log"), "--skip-networking", "--mysqlx=OFF", "--tls-version=",
		"--performance-schema=OFF", "--max-connections=50", "--secure-file-priv="+filepath.Join(s.directory, "backups"), "--skip-name-resolve")
	if strings.HasPrefix(s.dist.release, "8_4") {
		// MySQL 8.4 no longer loads mysql_native_password by default.
		command = append(command, "--mysql-native-password=ON")
	}
	if fileExists(filepath.Join(s.dist.root, "lib", "plugin", clonePlugin)) {
		command = append(command, "--plugin-load-add="+clonePlugin)
	}
	for _, flag := range s.flags {
		command = append(command, "--"+flag.Name+"="+flag.Value)
	}
	if err := s.newProcess(command, f.runAs); err != nil {
		return err
	}
	if err := s.process.Start(ctx); err != nil {
		return fmt.Errorf("start MySQL: %w", err)
	}
	return nil
}

// launchMySQL starts a new server, then replaces the initial root@localhost
// with the administrative account and creates the Cloud SQL account layout.
func (f *Fleet) launchMySQL(ctx context.Context, s *server, rootPassword string) error {
	if err := f.startMySQLProcess(ctx, s); err != nil {
		return err
	}
	session, err := s.waitMySQL(ctx, MySQLRootUser, "")
	if err != nil {
		return err
	}
	defer session.Close()
	if _, err := session.Query(ctx, mysqlBootstrap(s.password, rootPassword)); err != nil {
		return fmt.Errorf("bootstrap MySQL: %w", err)
	}
	return nil
}

// startMySQL starts an existing data directory and waits for the
// administrative account.
func (f *Fleet) startMySQL(ctx context.Context, s *server) error {
	if err := f.startMySQLProcess(ctx, s); err != nil {
		return err
	}
	session, err := s.waitMySQL(ctx, AdminRole, s.password)
	if err != nil {
		return err
	}
	session.Close()
	return nil
}

// waitMySQL polls until user can log in.
func (s *server) waitMySQL(ctx context.Context, user, password string) (*mysqlwire.Conn, error) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := s.process.Ready(ctx); errors.Is(err, backend.ErrProcessExited) {
			return nil, fmt.Errorf("%w: %s", errMySQLExited, logTail(filepath.Join(s.directory, "error.log")))
		}
		if session, err := mysqlwire.Dial(ctx, "unix", s.socket, user, password); err == nil {
			return session, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for MySQL: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// MySQLAccount renders 'user'@'host'.
func MySQLAccount(user, host string) string {
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

// MySQLCreateUser creates a built-in user in the cloudsqlsuperuser role.
func MySQLCreateUser(user, host, password string) string {
	account, role := MySQLAccount(user, host), MySQLAccount(SuperuserRole, MySQLAnyHost)
	return "CREATE USER " + account + mysqlIdentified(password) + "; GRANT " + role + " TO " + account +
		"; SET DEFAULT ROLE " + role + " TO " + account
}

// mysqlBootstrap creates the administrative account (holding SYSTEM_USER,
// so role members cannot alter it), the cloudsqlsuperuser role, and root@%,
// then drops the initial root@localhost.
func mysqlBootstrap(adminPassword, rootPassword string) string {
	admin := MySQLAccount(AdminRole, mysqlAdminHost)
	return mysqlSessionMode +
		"CREATE USER " + admin + " IDENTIFIED WITH " + mysqlwire.NativePassword + " BY " + mysqlwire.QuoteLiteral(adminPassword) + "; " +
		"GRANT ALL PRIVILEGES ON *.* TO " + admin + " WITH GRANT OPTION; " +
		"CREATE ROLE " + MySQLAccount(SuperuserRole, MySQLAnyHost) + "; " +
		"GRANT " + mysqlRolePrivileges + " ON *.* TO " + MySQLAccount(SuperuserRole, MySQLAnyHost) + " WITH GRANT OPTION; " +
		MySQLCreateUser(MySQLRootUser, MySQLAnyHost, rootPassword) + "; " +
		"DROP USER " + MySQLAccount(MySQLRootUser, mysqlAdminHost)
}

// execMySQL runs SQL in one administrative session with a fixed sql_mode, so
// the literal quoting does not depend on a mode a user set globally.
func (s *server) execMySQL(ctx context.Context, sql string) ([][]string, error) {
	session, err := mysqlwire.Dial(ctx, "unix", s.socket, AdminRole, s.password)
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
