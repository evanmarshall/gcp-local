//go:build cloudsqlintegration

package cloudsql

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/dataplane"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// realConfig reads the installed binaries and, under root, the server user.
func realConfig(t *testing.T) Config {
	t.Helper()
	config := Config{Root: os.Getenv("GCP_LOCAL_POSTGRES_ROOT")}
	if uid := os.Getenv("GCP_LOCAL_POSTGRES_UID"); uid != "" {
		user, err := strconv.ParseUint(uid, 10, 32)
		must(t, err)
		group, err := strconv.ParseUint(os.Getenv("GCP_LOCAL_POSTGRES_GID"), 10, 32)
		must(t, err)
		config.RunAs = &syscall.Credential{Uid: uint32(user), Gid: uint32(group)}
	}
	return config
}

// fleetRoots lists the private backend roots currently on disk.
func fleetRoots(t *testing.T) []string {
	t.Helper()
	roots, err := filepath.Glob(filepath.Join(os.TempDir(), "gcp-local-cloudsql-*"))
	must(t, err)
	return roots
}

// TestPostgresThroughConnector drives real PostgreSQL servers: authorization
// before any server starts, real databases and roles, SQL through the
// authorized connector, refusal for an unauthorized identity, isolation, and
// reset discarding the server and its data.
func TestPostgresThroughConnector(t *testing.T) {
	interfaces, err := net.Interfaces()
	must(t, err)
	if os.Getenv("GCP_LOCAL_ISOLATED") != "1" || len(interfaces) != 1 || interfaces[0].Name != "lo" {
		t.Fatal("Run actual Cloud SQL tests in the loopback-only runner")
	}
	if err := rolesRecorded(); err != nil {
		skipUnrecorded(t, err)
		t.Fatal(err)
	}
	h := &harness{t: t, index: state.NewIndex()}
	h.engine, err = iam.New(h.index)
	must(t, err)
	grants(t, h.index, h.engine)
	h.manager = backend.NewManager(30*time.Second, 20*time.Second)
	t.Cleanup(func() { must(t, h.manager.Shutdown(context.Background())) })
	registry := gateway.NewRegistry()
	config := realConfig(t)
	config.Publish, err = dataplane.Parse("16400-16401", "127.0.0.1")
	must(t, err)
	h.provider, err = New(context.Background(), registry, h.manager, h.index, h.engine, config)
	must(t, err)
	h.gate = gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: principal(token), Scopes: []string{cloudPlatform}}, nil
	})
	before := len(fleetRoots(t))

	h.expect(h.invoke(viewerToken, http.MethodPost, testInstances, insertBody()), http.StatusForbidden)
	if status := h.manager.Status(); len(status) != 0 {
		t.Fatalf("a denied request started a backend: %+v", status)
	}
	h.createInstance()
	h.expect(h.invoke(adminToken, http.MethodPost, testPath+"/databases", map[string]any{"name": "app"}), http.StatusOK)
	h.expect(h.invoke(adminToken, http.MethodPost, testPath+"/databases", map[string]any{"name": "app"}), http.StatusConflict)
	h.expect(h.invoke(adminToken, http.MethodPost, testPath+"/users", map[string]any{"name": "app_user", "password": "app-secret'\\"}), http.StatusOK)
	database := h.expect(h.invoke(viewerToken, http.MethodGet, testPath+"/databases/app", nil), http.StatusOK)
	if database["charset"] != "UTF8" || database["collation"] != "C" {
		t.Fatalf("database = %v", database)
	}
	users := h.expect(h.invoke(viewerToken, http.MethodGet, testPath+"/users", nil), http.StatusOK)["items"].([]any)
	if len(users) != 2 || users[0].(map[string]any)["name"] != "app_user" || users[1].(map[string]any)["name"] != defaultUser {
		t.Fatalf("users = %v", users)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	name := testProject + ":us-central1:" + testInstance
	client, err := h.provider.OpenConnector(ctx, ConnectorSpec{ConnectionName: name, Principal: principal(clientToken), Address: "127.0.0.1:0"})
	must(t, err)
	defer client.Close()
	viewer, err := h.provider.OpenConnector(ctx, ConnectorSpec{ConnectionName: name, Principal: principal(viewerToken), Address: "127.0.0.1:0"})
	must(t, err)
	defer viewer.Close()

	session, err := dialPostgres(ctx, "tcp", client.listener.Addr().String(), "app_user", "app", "app-secret'\\")
	must(t, err)
	rows, err := session.query(ctx, "CREATE TABLE items (id int PRIMARY KEY, name text); INSERT INTO items VALUES (1, 'one'), (2, 'two'); SELECT name FROM items ORDER BY id")
	must(t, err)
	if len(rows) != 2 || rows[0][0] != "one" || rows[1][0] != "two" {
		t.Fatalf("rows = %v", rows)
	}
	session.close()
	root, err := dialPostgres(ctx, "tcp", client.listener.Addr().String(), defaultUser, "postgres", "root-secret")
	must(t, err)
	rows, err = root.query(ctx, "SELECT current_user, pg_has_role('cloudsqlsuperuser', 'member')::text, version()")
	if err != nil || rows[0][0] != defaultUser || rows[0][1] != "true" {
		t.Fatalf("root session = %v, %v", rows, err)
	}
	t.Logf("server: %s", rows[0][2])
	root.close()
	var refused *pgError
	if _, err := dialPostgres(ctx, "tcp", client.listener.Addr().String(), "app_user", "app", "wrong"); !errors.As(err, &refused) || refused.Code != "28P01" {
		t.Fatalf("wrong password = %v", err)
	}
	if _, err := dialPostgres(ctx, "tcp", viewer.listener.Addr().String(), "app_user", "app", "app-secret'\\"); !errors.As(err, &refused) || refused.Code != "28000" {
		t.Fatalf("viewer connection = %v", err)
	}
	if _, err := dialPostgres(ctx, "tcp", client.listener.Addr().String(), adminRole, "postgres", "guess"); !errors.As(err, &refused) || refused.Code != "28000" {
		t.Fatalf("administrative connection = %v", err)
	}

	published := h.publishedAddress(testInstance)
	direct, err := dialPostgres(ctx, "tcp", published, "app_user", "app", "app-secret'\\")
	must(t, err)
	if rows, err := direct.query(ctx, "SELECT count(*) FROM items"); err != nil || rows[0][0] != "2" {
		t.Fatalf("public-IP session = %v, %v", rows, err)
	}
	if _, err := dialPostgres(ctx, "tcp", published, "app_user", "app", "wrong"); !errors.As(err, &refused) || refused.Code != "28P01" {
		t.Fatalf("public-IP wrong password = %v", err)
	}
	if _, err := dialPostgres(ctx, "tcp", published, adminRole, "postgres", "guess"); !errors.As(err, &refused) || refused.Code != "28000" {
		t.Fatalf("public-IP administrative connection = %v", err)
	}
	direct.close()

	// The role owns a table, so PostgreSQL refuses to drop it until the database goes.
	h.expect(h.invoke(adminToken, http.MethodDelete, testPath+"/users?name=app_user", nil), http.StatusBadRequest)
	h.expect(h.invoke(adminToken, http.MethodDelete, testPath+"/databases/app", nil), http.StatusOK)
	h.expect(h.invoke(adminToken, http.MethodDelete, testPath+"/users?name=app_user", nil), http.StatusOK)

	peer := "/sql/v1beta4/projects/" + peerProject + "/instances"
	h.expect(h.invoke(peerToken, http.MethodGet, testPath, nil), http.StatusForbidden)
	older := insertBody()
	older["databaseVersion"] = "POSTGRES_15"
	h.expect(h.invoke(peerToken, http.MethodPost, peer, older), http.StatusOK)
	if got := h.expect(h.invoke(peerToken, http.MethodGet, peer+"/"+testInstance, nil), http.StatusOK)["databaseInstalledVersion"]; got != "POSTGRES_15_19" {
		t.Fatalf("peer installed version = %v", got)
	}

	open, err := dialPostgres(ctx, "tcp", client.listener.Addr().String(), defaultUser, "postgres", "root-secret")
	must(t, err)
	if got := len(fleetRoots(t)); got != before+2 {
		t.Fatalf("backend roots = %d, want %d", got, before+2)
	}
	must(t, h.manager.Reset(context.Background(), backend.Key{Service: serviceName, Scope: scopeOf(testProject)}))
	if _, err := open.query(ctx, "SELECT 1"); err == nil {
		t.Fatal("a session survived reset")
	}
	if got := len(fleetRoots(t)); got != before+1 {
		t.Fatalf("reset left %d backend roots, want %d", got, before+1)
	}
	expectRefused(t, published)
	h.expect(h.invoke(adminToken, http.MethodGet, testPath, nil), http.StatusNotFound)
	peerInstance := h.expect(h.invoke(peerToken, http.MethodGet, peer+"/"+testInstance, nil), http.StatusOK)
	peerPort, _ := object(t, peerInstance["ipAddresses"].([]any)[0])["port"].(float64)
	peerAddress := net.JoinHostPort("127.0.0.1", strconv.Itoa(int(peerPort)))
	peerSession, err := dialPostgres(ctx, "tcp", peerAddress, defaultUser, "postgres", "root-secret")
	must(t, err)
	h.expect(h.invoke(peerToken, http.MethodDelete, peer+"/"+testInstance, nil), http.StatusOK)
	if _, err := peerSession.query(ctx, "SELECT 1"); err == nil {
		t.Fatal("a public-IP session survived instance deletion")
	}
	expectRefused(t, peerAddress)
}
