package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/providers/databases/cloudsql/server/mysqlwire"
	"github.com/evanmarshall/gcp-local/internal/providers/databases/cloudsql/server/pgwire"
)

// Backup sentinels.
var (
	ErrBackupMissing = errors.New("no backup with this ID exists for the instance")
	ErrBackupVersion = errors.New("the backup was taken from another database version")
	errBackupExists  = errors.New("a backup with this ID already exists for the instance")
	errClonePlugin   = errors.New("the MySQL clone plugin is not installed; rerun python3 scripts/install/mysql.py")
)

// backup is one physical backup: a data directory the server can start
// from, and the administrative secret that directory's accounts carry.
type backup struct {
	directory, password string
	dist                Distribution
}

// Backup takes an online physical backup of one server into its private
// directory, as Cloud SQL backs up an instance's disk: PostgreSQL's
// non-exclusive base backup with the WAL it needs, or a MySQL clone. Open
// sessions continue. id names the backup, as the backup run ID does.
func (f *Fleet) Backup(ctx context.Context, instance, id string) error {
	s, err := f.server(instance)
	if err != nil {
		return err
	}
	if _, exists := s.backups[id]; exists {
		return errBackupExists
	}
	directory := filepath.Join(s.directory, "backups", id)
	if err := os.Mkdir(directory, 0o700); err != nil {
		return fmt.Errorf("create the backup directory: %w", err)
	}
	if err := f.own(directory); err != nil {
		return errors.Join(err, os.RemoveAll(directory))
	}
	take := f.backupPostgres
	if s.dist.mysql {
		take = f.backupMySQL
	}
	if err := take(ctx, s, filepath.Join(directory, "data")); err != nil {
		return errors.Join(err, os.RemoveAll(directory))
	}
	s.backups[id] = backup{directory: directory, password: s.password, dist: s.dist}
	return nil
}

// postgresVolatile are data directory entries a base backup leaves empty
// or omits (PostgreSQL's "Making a Base Backup Using the Low Level API").
var postgresVolatile = map[string]bool{"pg_wal": true, "pg_replslot": true, "pg_dynshmem": true, "pg_notify": true,
	"pg_serial": true, "pg_snapshots": true, "pg_stat_tmp": true, "pg_subtrans": true}

// backupPostgres copies the data directory between pg_backup_start and
// pg_backup_stop in one administrative session, writes the backup label,
// then copies the WAL the backup needs; the server replays it on start.
func (f *Fleet) backupPostgres(ctx context.Context, s *server, target string) error {
	session, err := pgwire.Dial(ctx, "unix", s.socket, AdminRole, "postgres", s.password)
	if err != nil {
		return fmt.Errorf("open an administrative session: %w", err)
	}
	// Closing the session aborts a backup that did not stop.
	defer session.Close()
	if _, err := session.Query(ctx, "SELECT pg_backup_start('gcp-local', true)"); err != nil {
		return fmt.Errorf("start the base backup: %w", err)
	}
	if err := f.copyTree(s.data(), target, skipVolatile); err != nil {
		return err
	}
	rows, err := session.Query(ctx, "SELECT labelfile, spcmapfile FROM pg_backup_stop(false)")
	if err != nil || len(rows) != 1 || len(rows[0]) != 2 {
		return fmt.Errorf("stop the base backup: %w", errors.Join(err, errBackupRows))
	}
	if err := f.writeLabels(target, rows[0][0], rows[0][1]); err != nil {
		return err
	}
	wal := func(_ string, entry fs.DirEntry) bool { return entry.IsDir() }
	return f.copyTree(filepath.Join(s.data(), "pg_wal"), filepath.Join(target, "pg_wal"), wal)
}

// skipVolatile names what a base backup leaves out: the contents of the
// volatile directories, the postmaster's files, and temporary files.
func skipVolatile(relative string, entry fs.DirEntry) bool {
	top, rest, nested := strings.Cut(relative, string(filepath.Separator))
	switch {
	case strings.HasPrefix(entry.Name(), "pgsql_tmp"):
		return true
	case nested:
		return postgresVolatile[top] && rest != ""
	default:
		return top == "postmaster.pid" || top == "postmaster.opts"
	}
}

// writeLabels writes pg_backup_stop's backup label and tablespace map.
func (f *Fleet) writeLabels(target, label, tablespaces string) error {
	if err := f.writeOwned(filepath.Join(target, "backup_label"), label); err != nil {
		return err
	}
	if tablespaces == "" {
		return nil
	}
	return f.writeOwned(filepath.Join(target, "tablespace_map"), tablespaces)
}

// errBackupRows reports an unexpected pg_backup_stop result.
var errBackupRows = errors.New("pg_backup_stop returned no backup label")

// backupMySQL clones the server's data into target with the clone plugin.
func (f *Fleet) backupMySQL(ctx context.Context, s *server, target string) error {
	if !fileExists(filepath.Join(s.dist.root, "lib", "plugin", clonePlugin)) {
		return errClonePlugin
	}
	if _, err := s.execMySQL(ctx, "CLONE LOCAL DATA DIRECTORY = "+mysqlwire.QuoteLiteral(target)); err != nil {
		return fmt.Errorf("clone the MySQL data directory: %w", err)
	}
	return nil
}

// Restore replaces target's data with backup id of source, which may be
// target itself, and restarts it: Cloud SQL restores a backup over the
// whole instance, users included. A failed restore brings back the old data.
func (f *Fleet) Restore(ctx context.Context, target, source, id string) error {
	t, err := f.server(target)
	if err != nil {
		return err
	}
	from, err := f.server(source)
	if err != nil {
		return err
	}
	saved, ok := from.backups[id]
	if !ok {
		return ErrBackupMissing
	}
	if saved.dist != t.dist {
		return ErrBackupVersion
	}
	staging := filepath.Join(t.directory, "restoring")
	if err := f.copyTree(filepath.Join(saved.directory, "data"), staging, nil); err != nil {
		return errors.Join(err, os.RemoveAll(staging))
	}
	if err := t.stop(ctx); err != nil {
		return errors.Join(err, os.RemoveAll(staging))
	}
	return f.swap(ctx, t, staging, saved.password)
}

// swap replaces a stopped server's data directory with staging, which
// carries password's administrative account, and starts it. A failure
// brings the old data back.
func (f *Fleet) swap(ctx context.Context, t *server, staging, password string) error {
	previous, old := filepath.Join(t.directory, "replaced"), t.password
	if err := os.Rename(t.data(), previous); err != nil {
		return errors.Join(fmt.Errorf("set the current data aside: %w", err), os.RemoveAll(staging), f.restart(ctx, t))
	}
	if err := os.Rename(staging, t.data()); err != nil {
		return errors.Join(fmt.Errorf("move the restored data in place: %w", err), os.Rename(previous, t.data()), f.restart(ctx, t))
	}
	t.password = password
	if err := f.restart(ctx, t); err != nil {
		_ = t.stop(context.WithoutCancel(ctx))
		t.password = old
		return errors.Join(err, os.RemoveAll(t.data()), os.Rename(previous, t.data()), f.restart(context.WithoutCancel(ctx), t))
	}
	if err := os.RemoveAll(previous); err != nil {
		return fmt.Errorf("remove the replaced data: %w", err)
	}
	return nil
}

// DeleteBackup removes one backup's files.
func (f *Fleet) DeleteBackup(_ context.Context, instance, id string) error {
	s, err := f.server(instance)
	if err != nil {
		return err
	}
	saved, ok := s.backups[id]
	if !ok {
		return ErrBackupMissing
	}
	if err := os.RemoveAll(saved.directory); err != nil {
		return fmt.Errorf("remove the backup: %w", err)
	}
	delete(s.backups, id)
	return nil
}

// copyTree copies the regular files and directories under source into a
// new target, owned by the server user, skipping entries skip names (by
// path relative to source). Files that vanish mid-copy are skipped, as a
// base backup of a running server expects.
func (f *Fleet) copyTree(source, target string, skip func(string, fs.DirEntry) bool) error {
	return filepath.WalkDir(source, func(path string, entry fs.DirEntry, err error) error { //nolint:wrapcheck // the walk's own errors are already descriptive
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return fmt.Errorf("relate %s to the copy root: %w", path, err)
		}
		if relative != "." && skip != nil && skip(relative, entry) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		return f.copyEntry(path, filepath.Join(target, relative), relative, entry)
	})
}

// copyEntry copies one walked directory or regular file; other entries are skipped.
func (f *Fleet) copyEntry(path, destination, relative string, entry fs.DirEntry) error {
	switch {
	case entry.IsDir() && relative == "." && isDirectory(destination):
		return nil // the WAL copy fills the base backup's empty pg_wal
	case entry.IsDir():
		if err := os.Mkdir(destination, 0o700); err != nil {
			return fmt.Errorf("create %s: %w", relative, err)
		}
		return f.own(destination)
	case entry.Type().IsRegular():
		return f.copyFile(path, destination)
	default:
		return nil
	}
}

// copyFile copies one regular file, owned by the server user.
func (f *Fleet) copyFile(source, target string) error {
	in, err := os.Open(source) //nolint:gosec // a file under the fleet's private root
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open %s: %w", filepath.Base(source), err)
	}
	defer in.Close()
	out, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) //nolint:gosec // a new file under the fleet's private root
	if err != nil {
		return fmt.Errorf("create %s: %w", filepath.Base(target), err)
	}
	_, copyErr := io.Copy(out, in)
	if err := errors.Join(copyErr, out.Close()); err != nil {
		return fmt.Errorf("copy %s: %w", filepath.Base(source), err)
	}
	return f.own(target)
}

// writeOwned writes a new file owned by the server user.
func (f *Fleet) writeOwned(path, content string) error {
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		return fmt.Errorf("write %s: %w", filepath.Base(path), err)
	}
	return f.own(path)
}

// isDirectory reports whether path is an existing directory.
func isDirectory(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}
