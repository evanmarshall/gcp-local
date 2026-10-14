package bigquery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	_ "unsafe" // linkname reaches the engine's analyzer options; see analyzerOptions

	"github.com/evanmarshall/gcp-local/internal/backend"
	googlesql "github.com/goccy/go-googlesql"
	"github.com/goccy/googlesqlite"
)

// engineBackend runs one GoogleSQL engine (goccy/googlesqlite: the GoogleSQL
// parser and analyzer over SQLite, pure Go) in-process for one project. It
// owns every table's columns and rows; the provider owns datasets and table
// metadata. It opens no port and is reached only through the provider after
// authorization.
type engineBackend struct {
	project string
	db      *sql.DB
	// conn is the engine's only connection. A private in-memory database
	// lives in its connection, so a second pooled connection would be a new,
	// empty database; Manager.Use serializes every caller.
	conn *sql.Conn
}

// RequireUTC sets the process's local time zone to UTC. The engine's Go
// runtime (googlesqlite v0.3.1) builds DATE and TIMESTAMP values with
// time.Unix and no zone (value.DateFromInt64Value, the value decoder, the
// literal encoder), so they render in time.Local, a day off on hosts west of
// UTC; GCP reports UTC. Each binary that serves BigQuery calls it before it
// starts.
func RequireUTC() { time.Local = time.UTC } //nolint:gosmopolitan // the engine reads time.Local; see above

// analyzerOptions returns the engine's one GoogleSQL analyzer configuration,
// shared by every connection (googlesqlite v0.3.1,
// internal.getSharedAnalyzerOptions). googlesqlite exposes no setting for it,
// so it is reached by name.
//
//go:linkname analyzerOptions github.com/goccy/googlesqlite/internal.getSharedAnalyzerOptions
func analyzerOptions() (*googlesql.AnalyzerOptions, error)

// analyzeInUTC sets the analyzer's default time zone to UTC, as BigQuery's.
// The analyzer defaults to America/Los_Angeles and applies it wherever it
// folds a string into a TIMESTAMP or a TIMESTAMP into a string: literals
// (TIMESTAMP '2020-01-01'), casts of literals, string literals compared with
// or inserted into TIMESTAMP columns, and CAST(TIMESTAMP ... AS STRING).
// go-googlesql v0.3.0 has no time zone constructor; a zero handle is the
// zero absl::TimeZone, whose null zone is UTC. Tests assert UTC results.
var analyzeInUTC = sync.OnceValue(func() error {
	if err := googlesql.Init(); err != nil {
		return fmt.Errorf("initialize GoogleSQL: %w", err)
	}
	options, err := analyzerOptions()
	if err != nil {
		return fmt.Errorf("GoogleSQL analyzer options: %w", err)
	}
	if err := options.SetDefaultTimeZone(&googlesql.TimeZone{}); err != nil {
		return fmt.Errorf("GoogleSQL analyzer time zone: %w", err)
	}
	return nil
})

// moduleCalls counts calls into the GoogleSQL module since the last
// collection.
var moduleCalls atomic.Int64

// moduleCallsPerCollection bounds the module calls between collections.
const moduleCallsPerCollection = 32

// collectModuleGarbage runs the Go collector every moduleCallsPerCollection
// calls into the GoogleSQL module. The engine and the authorization parse
// share one module (go-googlesql v0.3.0), whose wasm memory holds catalogs
// and parser and analyzer outputs and is freed only by Go finalizers. The Go
// collector cannot see that memory fill, so under load it runs too rarely:
// the module passes 2 GiB and every later call panics on a negative int32
// pointer (measured: about 250 CREATE and DROP TABLE cycles; each new table
// builds a catalog of every BigQuery function).
func collectModuleGarbage() {
	if moduleCalls.Add(1)%moduleCallsPerCollection == 0 {
		runtime.GC()
	}
}

// errEngineStopped reports use of a stopped engine.
var errEngineStopped = errors.New("bigquery engine is stopped")

func (e *engineBackend) Start(ctx context.Context) error {
	if err := analyzeInUTC(); err != nil {
		return err
	}
	db, err := sql.Open("googlesqlite", ":memory:")
	if err != nil {
		return fmt.Errorf("open the bigquery engine: %w", err)
	}
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("connect to the bigquery engine: %w", err)
	}
	// Every table path the provider sends is fully qualified, so the name
	// path only anchors the project.
	if err := conn.Raw(func(raw any) error {
		engine, ok := raw.(*googlesqlite.Conn)
		if !ok {
			return errEngineStopped
		}
		engine.SetMaxNamePath(3)
		return engine.SetNamePath([]string{e.project})
	}); err != nil {
		_ = conn.Close()
		_ = db.Close()
		return fmt.Errorf("scope the bigquery engine: %w", err)
	}
	e.db, e.conn = db, conn
	return nil
}

func (e *engineBackend) Ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if e.conn == nil {
		return errEngineStopped
	}
	return nil
}

// Stop discards the engine and every table it held.
func (e *engineBackend) Stop(ctx context.Context) error {
	if e.conn != nil {
		_ = e.conn.Close()
	}
	if e.db != nil {
		_ = e.db.Close()
	}
	e.db, e.conn = nil, nil
	return ctx.Err()
}

// Reset has no assets beyond the in-memory database that Stop discards.
func (e *engineBackend) Reset(ctx context.Context) error { return e.Stop(ctx) }

// engineOf returns the running engine behind one manager backend.
func engineOf(driver backend.Backend) (*engineBackend, error) {
	e, ok := driver.(*engineBackend)
	if !ok || e.conn == nil {
		return nil, errEngineStopped
	}
	return e, nil
}

// canonicalPath names one table fully qualified, one quoted identifier per
// part, so the engine resolves exactly that table and implicit aliases stay
// the table id. Dataset and table ids never hold a backtick, backslash, or
// dot (tableIDPattern).
func canonicalPath(project, dataset, table string) string {
	return "`" + project + "`.`" + dataset + "`.`" + table + "`"
}

// engineResult is one statement's columns and rows as the engine returned
// them, or the rows a DML statement changed.
type engineResult struct {
	columns  []engineColumn
	rows     [][]any
	affected *int64
}

// query runs one script and reads the rows of its last statement.
func (e *engineBackend) query(ctx context.Context, script string, args []any) (*engineResult, error) {
	defer collectModuleGarbage()
	rows, err := e.conn.QueryContext(ctx, script, args...)
	if err != nil {
		return nil, engineError(err)
	}
	defer rows.Close()
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, engineError(err)
	}
	result := &engineResult{}
	for _, column := range types {
		parsed, err := parseEngineType(column.DatabaseTypeName())
		if err != nil {
			return nil, err
		}
		result.columns = append(result.columns, engineColumn{name: column.Name(), kind: parsed})
	}
	for rows.Next() {
		values := make([]any, len(types))
		pointers := make([]any, len(types))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return nil, engineError(err)
		}
		result.rows = append(result.rows, values)
	}
	if err := rows.Err(); err != nil {
		return nil, engineError(err)
	}
	return result, nil
}

// exec runs one DML statement and reports the rows it changed.
func (e *engineBackend) exec(ctx context.Context, statement string, args []any) (*engineResult, error) {
	defer collectModuleGarbage()
	result, err := e.conn.ExecContext(ctx, statement, args...)
	if err != nil {
		return nil, engineError(err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return nil, engineError(err)
	}
	return &engineResult{affected: &affected}, nil
}

// columnsOf reads a table's columns, or reports that it does not exist.
func (e *engineBackend) columnsOf(ctx context.Context, dataset, table string) ([]engineColumn, bool, error) {
	// Every column is the point: this reads the table's schema.
	result, err := e.query(ctx, "SELECT * FROM "+canonicalPath(e.project, dataset, table)+" LIMIT 0", nil) //nolint:unqueryvet // reads every column's type
	if err != nil {
		if strings.Contains(err.Error(), "Not found: Table") {
			return nil, false, nil
		}
		return nil, false, err
	}
	return result.columns, true, nil
}

// insertRows adds rows in one transaction: either every row lands, or none
// does and the failing row's index is reported.
func (e *engineBackend) insertRows(ctx context.Context, dataset, table string, fields []map[string]any, rows []map[string]any) (int, error) {
	defer collectModuleGarbage()
	names := make([]string, 0, len(fields))
	placeholders := make([]string, 0, len(fields))
	for _, field := range fields {
		name, _ := field["name"].(string)
		names = append(names, "`"+name+"`")
		placeholders = append(placeholders, placeholder(field))
	}
	// Identifiers are validated table and column names; values are bound.
	statement := "INSERT INTO " + canonicalPath(e.project, dataset, table) + //nolint:gosec // G202: validated identifiers, bound values
		" (" + strings.Join(names, ", ") + ") VALUES (" + strings.Join(placeholders, ", ") + ")"
	tx, err := e.conn.BeginTx(ctx, nil)
	if err != nil {
		return -1, engineError(err)
	}
	for index, row := range rows {
		args := make([]any, 0, len(fields))
		for _, field := range fields {
			name, _ := field["name"].(string)
			value, err := insertValue(field, row[name])
			if err != nil {
				_ = tx.Rollback()
				return index, err
			}
			args = append(args, value)
		}
		if _, err := tx.ExecContext(ctx, statement, args...); err != nil {
			_ = tx.Rollback()
			return index, engineError(err)
		}
	}
	if err := tx.Commit(); err != nil {
		return -1, engineError(err)
	}
	return -1, nil
}
