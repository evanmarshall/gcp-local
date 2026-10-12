package cloudsql

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// fakeFleet stands in for the PostgreSQL fleet. It records administrative SQL
// and serves each instance's "socket" with a Unix listener that captures the
// bytes a connector forwards. It does not interpret SQL beyond the catalog
// queries the provider issues.
type fakeFleet struct {
	mu        sync.Mutex
	started   bool
	stops     int
	directory string
	servers   map[string]*fakeServer
	statement []string
}

type fakeServer struct {
	listener  net.Listener
	socket    string
	done      chan struct{}
	databases []string
	users     []string
	received  chan []byte
}

func newFakeFleet() *fakeFleet { return &fakeFleet{servers: map[string]*fakeServer{}} }

func (f *fakeFleet) Start(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	directory, err := os.MkdirTemp("", "cloudsql-fake-")
	if err != nil {
		return err //nolint:wrapcheck // test double
	}
	f.directory, f.started = directory, true
	return nil
}

func (f *fakeFleet) Ready(context.Context) error { return nil }

func (f *fakeFleet) Stop(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for name, server := range f.servers {
		server.close()
		delete(f.servers, name)
	}
	if f.started {
		f.stops++
		_ = os.RemoveAll(f.directory)
	}
	f.started = false
	return nil
}

func (f *fakeFleet) Reset(ctx context.Context) error { return f.Stop(ctx) }

func (f *fakeFleet) create(ctx context.Context, spec serverSpec) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	socket := filepath.Join(f.directory, spec.Instance+".sock")
	var listen net.ListenConfig
	listener, err := listen.Listen(ctx, "unix", socket)
	if err != nil {
		return err //nolint:wrapcheck // test double
	}
	server := &fakeServer{listener: listener, socket: socket, done: make(chan struct{}), databases: []string{"postgres"},
		users: []string{defaultUser}, received: make(chan []byte, 8)}
	go server.accept()
	f.servers[spec.Instance] = server
	return nil
}

// accept captures the first bytes of each forwarded connection.
func (s *fakeServer) accept() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		buffer := make([]byte, 4096)
		n, _ := conn.Read(buffer)
		s.received <- buffer[:n]
		_ = conn.Close()
	}
}

func (s *fakeServer) close() {
	close(s.done)
	_ = s.listener.Close()
}

func (f *fakeFleet) remove(_ context.Context, instance string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	server := f.servers[instance]
	if server == nil {
		return errServerMissing
	}
	server.close()
	delete(f.servers, instance)
	return nil
}

func (f *fakeFleet) exec(_ context.Context, instance, _, sql string) ([][]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	server := f.servers[instance]
	if server == nil {
		return nil, errServerMissing
	}
	f.statement = append(f.statement, sql)
	name := ""
	if fields := strings.Fields(sql); len(fields) > 2 {
		name = strings.Trim(fields[2], `"`)
	}
	switch {
	case strings.HasPrefix(sql, "CREATE DATABASE "):
		server.databases = append(server.databases, name)
	case strings.HasPrefix(sql, "CREATE ROLE "):
		server.users = append(server.users, name)
	case strings.HasPrefix(sql, "SELECT datname"):
		return server.databaseRows(sql), nil
	case strings.HasPrefix(sql, "SELECT rolname"):
		return rowsOf(server.users), nil
	case strings.HasPrefix(sql, "DROP DATABASE "):
		return nil, &pgError{Severity: "ERROR", Code: "55006", Message: "database is being accessed by other users"}
	default:
	}
	return nil, nil
}

// databaseRows answers the provider's pg_database query.
func (s *fakeServer) databaseRows(sql string) [][]string {
	var rows [][]string
	for _, name := range s.databases {
		if !strings.Contains(sql, "datname = ") || strings.Contains(sql, "'"+name+"'") {
			rows = append(rows, []string{name, "UTF8", "C"})
		}
	}
	return rows
}

// rowsOf renders single-column rows.
func rowsOf(values []string) [][]string {
	rows := make([][]string, 0, len(values))
	for _, value := range values {
		rows = append(rows, []string{value})
	}
	return rows
}

func (f *fakeFleet) endpoint(instance string) (string, <-chan struct{}, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	server := f.servers[instance]
	if server == nil {
		return "", nil, errServerMissing
	}
	return server.socket, server.done, nil
}

// statements returns the recorded administrative SQL.
func (f *fakeFleet) statements() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.statement...)
}

// server returns one fake server, or nil.
func (f *fakeFleet) server(instance string) *fakeServer {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.servers[instance]
}
