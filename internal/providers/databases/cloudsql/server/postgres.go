package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/providers/databases/cloudsql/server/pgwire"
)

// initializePostgres runs initdb with SCRAM-only local authentication and no TCP access.
func (f *Fleet) initializePostgres(ctx context.Context, s *server) error {
	secret := filepath.Join(s.directory, "pw")
	if err := os.WriteFile(secret, []byte(s.password+"\n"), 0o600); err != nil {
		return fmt.Errorf("write the administrative secret: %w", err)
	}
	defer os.Remove(secret)
	if err := f.own(secret); err != nil {
		return err
	}
	command := exec.CommandContext(ctx, filepath.Join(s.dist.root, "bin", "initdb"), "-D", s.data(), //nolint:gosec // pinned binary path
		"-U", AdminRole, "--pwfile="+secret, "--auth-local=scram-sha-256", "--auth-host=reject", "--no-locale", "-E", "UTF8", "--no-sync")
	command.Env = environment(s.dist, s.directory)
	command.Dir = s.directory
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Credential: f.runAs}
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, &output
	if err := command.Run(); err != nil {
		return fmt.Errorf("initdb: %w: %s", err, bytes.TrimSpace(output.Bytes()[:min(output.Len(), 512)]))
	}
	return nil
}

// data is the server's data directory.
func (s *server) data() string { return filepath.Join(s.directory, "data") }

// postgresSettings are the fixed server settings: the private socket only,
// durability off, and enough WAL kept for an online physical backup.
// Database flags follow and win; the provider refuses flags that would
// override the socket settings.
func postgresSettings(s *server) []string {
	return []string{"listen_addresses=", "unix_socket_directories=" + filepath.Dir(s.socket), "unix_socket_permissions=0700",
		"port=5432", "fsync=off", "synchronous_commit=off", "full_page_writes=off", "shared_buffers=16MB", "max_connections=50",
		"dynamic_shared_memory_type=mmap", "logging_collector=off", "password_encryption=scram-sha-256",
		"wal_level=replica", "wal_keep_size=128MB"}
}

// startPostgres starts the server with its flags and waits for an
// administrative session.
func (f *Fleet) startPostgres(ctx context.Context, s *server) error {
	settings := postgresSettings(s)
	for _, flag := range s.flags {
		settings = append(settings, flag.Name+"="+flag.Value)
	}
	command := make([]string, 0, 3+2*len(settings))
	command = append(command, filepath.Join(s.dist.root, "bin", "postgres"), "-D", s.data())
	for _, setting := range settings {
		command = append(command, "-c", setting)
	}
	if err := s.newProcess(command, f.runAs); err != nil {
		return err
	}
	if err := s.process.Start(ctx); err != nil {
		return fmt.Errorf("start PostgreSQL: %w", err)
	}
	return s.waitPostgres(ctx)
}

// bootstrap creates the cloudsqlsuperuser group and the default postgres user.
// Without a root password the postgres user exists but cannot log in, as in
// Cloud SQL before its password is set.
func bootstrap(rootPassword string) string {
	password := "NULL"
	if rootPassword != "" {
		password = pgwire.QuoteLiteral(rootPassword)
	}
	return "CREATE ROLE " + SuperuserRole + " NOLOGIN CREATEDB CREATEROLE; " +
		"CREATE ROLE " + DefaultUser + " LOGIN CREATEDB CREATEROLE IN ROLE " + SuperuserRole + " PASSWORD " + password + "; " +
		"GRANT ALL ON DATABASE postgres TO " + SuperuserRole + "; " +
		"GRANT ALL ON SCHEMA public TO " + SuperuserRole
}

// waitPostgres polls until an administrative session succeeds.
func (s *server) waitPostgres(ctx context.Context) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := s.process.Ready(ctx); errors.Is(err, backend.ErrProcessExited) {
			return fmt.Errorf("PostgreSQL exited during startup: %w", err)
		}
		if _, err := s.execPostgres(ctx, "postgres", "SELECT 1"); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for PostgreSQL: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// execPostgres runs SQL in one administrative session.
func (s *server) execPostgres(ctx context.Context, database, sql string) ([][]string, error) {
	session, err := pgwire.Dial(ctx, "unix", s.socket, AdminRole, database, s.password)
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
