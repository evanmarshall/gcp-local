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
	"github.com/evanmarshall/gcp-local/internal/providers/databases/cloudsql/server/mysqlwire"
	"github.com/evanmarshall/gcp-local/internal/providers/databases/cloudsql/server/pgwire"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// realConfig reads the installed binaries and, under root, the server user.
func realConfig(t *testing.T) Config {
	t.Helper()
	config := Config{Root: os.Getenv("GCP_LOCAL_POSTGRES_ROOT"), MySQLRoot: os.Getenv("GCP_LOCAL_MYSQL_ROOT")}
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
// realHarness drives the provider with the installed servers, publishing
// public-IP instances on ports.
func realHarness(t *testing.T, ports string) *harness {
	t.Helper()
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
	config.Publish, err = dataplane.Parse(ports, "127.0.0.1")
	must(t, err)
	h.provider, err = New(context.Background(), registry, h.manager, h.index, h.engine, config)
	must(t, err)
	h.gate = gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: principal(token), Scopes: []string{cloudPlatform}}, nil
	})
	return h
}

func TestPostgresThroughConnector(t *testing.T) {
	h := realHarness(t, "16400-16401")
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

	session, err := pgwire.Dial(ctx, "tcp", client.listener.Addr().String(), "app_user", "app", "app-secret'\\")
	must(t, err)
	rows, err := session.Query(ctx, "CREATE TABLE items (id int PRIMARY KEY, name text); INSERT INTO items VALUES (1, 'one'), (2, 'two'); SELECT name FROM items ORDER BY id")
	must(t, err)
	if len(rows) != 2 || rows[0][0] != "one" || rows[1][0] != "two" {
		t.Fatalf("rows = %v", rows)
	}
	session.Close()
	root, err := pgwire.Dial(ctx, "tcp", client.listener.Addr().String(), defaultUser, "postgres", "root-secret")
	must(t, err)
	rows, err = root.Query(ctx, "SELECT current_user, pg_has_role('cloudsqlsuperuser', 'member')::text, version()")
	if err != nil || rows[0][0] != defaultUser || rows[0][1] != "true" {
		t.Fatalf("root session = %v, %v", rows, err)
	}
	t.Logf("server: %s", rows[0][2])
	root.Close()
	var refused *pgwire.Error
	if _, err := pgwire.Dial(ctx, "tcp", client.listener.Addr().String(), "app_user", "app", "wrong"); !errors.As(err, &refused) || refused.Code != "28P01" {
		t.Fatalf("wrong password = %v", err)
	}
	if _, err := pgwire.Dial(ctx, "tcp", viewer.listener.Addr().String(), "app_user", "app", "app-secret'\\"); !errors.As(err, &refused) || refused.Code != "28000" {
		t.Fatalf("viewer connection = %v", err)
	}
	if _, err := pgwire.Dial(ctx, "tcp", client.listener.Addr().String(), adminRole, "postgres", "guess"); !errors.As(err, &refused) || refused.Code != "28000" {
		t.Fatalf("administrative connection = %v", err)
	}

	published := h.publishedAddress(testInstance)
	direct, err := pgwire.Dial(ctx, "tcp", published, "app_user", "app", "app-secret'\\")
	must(t, err)
	if rows, err := direct.Query(ctx, "SELECT count(*) FROM items"); err != nil || rows[0][0] != "2" {
		t.Fatalf("public-IP session = %v, %v", rows, err)
	}
	if _, err := pgwire.Dial(ctx, "tcp", published, "app_user", "app", "wrong"); !errors.As(err, &refused) || refused.Code != "28P01" {
		t.Fatalf("public-IP wrong password = %v", err)
	}
	if _, err := pgwire.Dial(ctx, "tcp", published, adminRole, "postgres", "guess"); !errors.As(err, &refused) || refused.Code != "28000" {
		t.Fatalf("public-IP administrative connection = %v", err)
	}
	direct.Close()

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

	open, err := pgwire.Dial(ctx, "tcp", client.listener.Addr().String(), defaultUser, "postgres", "root-secret")
	must(t, err)
	if got := len(fleetRoots(t)); got != before+2 {
		t.Fatalf("backend roots = %d, want %d", got, before+2)
	}
	must(t, h.manager.Reset(context.Background(), backend.Key{Service: serviceName, Scope: provider.ProjectScope(testProject)}))
	if _, err := open.Query(ctx, "SELECT 1"); err == nil {
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
	peerSession, err := pgwire.Dial(ctx, "tcp", peerAddress, defaultUser, "postgres", "root-secret")
	must(t, err)
	h.expect(h.invoke(peerToken, http.MethodDelete, peer+"/"+testInstance, nil), http.StatusOK)
	if _, err := peerSession.Query(ctx, "SELECT 1"); err == nil {
		t.Fatal("a public-IP session survived instance deletion")
	}
	expectRefused(t, peerAddress)
}

// expectMySQLRefusal asserts a MySQL login fails with one error number.
func expectMySQLRefusal(t *testing.T, ctx context.Context, address, user, password string, code uint16) {
	t.Helper()
	var refused *mysqlwire.Error
	if _, err := mysqlwire.Dial(ctx, "tcp", address, user, password); !errors.As(err, &refused) || refused.Code != code {
		t.Fatalf("%s at %s = %v, want MySQL error %d", user, address, err, code)
	}
}

// TestMySQLThroughConnector drives real MySQL 8.4 and 8.0 servers:
// authorization before any server starts, real schemas and accounts, SQL
// through the authorized connector and the published port with
// mysql_native_password, refusal of the administrative account, isolation,
// and delete and reset closing access.
func TestMySQLThroughConnector(t *testing.T) {
	h := realHarness(t, "16410-16411")
	before := len(fleetRoots(t))
	body := mysqlInsertBody()
	body["databaseVersion"] = "MYSQL_8_4"
	h.expect(h.invoke(viewerToken, http.MethodPost, testInstances, body), http.StatusForbidden)
	if status := h.manager.Status(); len(status) != 0 {
		t.Fatalf("a denied request started a backend: %+v", status)
	}
	h.expect(h.invoke(adminToken, http.MethodPost, testInstances, body), http.StatusOK)
	if got := h.expect(h.invoke(viewerToken, http.MethodGet, testMySQLPath, nil), http.StatusOK)["databaseInstalledVersion"]; got != "MYSQL_8_4_11" {
		t.Fatalf("installed version = %v", got)
	}
	h.expect(h.invoke(adminToken, http.MethodPost, testMySQLPath+"/databases", map[string]any{"name": "app", "charset": "latin1", "collation": "latin1_swedish_ci"}), http.StatusOK)
	h.expect(h.invoke(adminToken, http.MethodPost, testMySQLPath+"/databases", map[string]any{"name": "app"}), http.StatusBadRequest)
	h.expect(h.invoke(adminToken, http.MethodPost, testMySQLPath+"/databases", map[string]any{"name": "bad", "charset": "latin1", "collation": "utf8mb4_bin"}), http.StatusBadRequest)
	h.expect(h.invoke(adminToken, http.MethodPost, testMySQLPath+"/databases", map[string]any{"name": "plain"}), http.StatusOK)
	database := h.expect(h.invoke(viewerToken, http.MethodGet, testMySQLPath+"/databases/app", nil), http.StatusOK)
	plain := h.expect(h.invoke(viewerToken, http.MethodGet, testMySQLPath+"/databases/plain", nil), http.StatusOK)
	if database["charset"] != "latin1" || database["collation"] != "latin1_swedish_ci" || plain["charset"] != "utf8mb4" || plain["collation"] != "utf8mb4_0900_ai_ci" {
		t.Fatalf("databases = %v, %v", database, plain)
	}
	const password = "app-secret'\\"
	h.expect(h.invoke(adminToken, http.MethodPost, testMySQLPath+"/users", map[string]any{"name": "app_user", "password": password}), http.StatusOK)
	h.expect(h.invoke(adminToken, http.MethodPost, testMySQLPath+"/users", map[string]any{"name": "app_user", "password": "again"}), http.StatusConflict)
	users := h.expect(h.invoke(viewerToken, http.MethodGet, testMySQLPath+"/users", nil), http.StatusOK)["items"].([]any)
	if len(users) != 2 || object(t, users[0])["name"] != "app_user" || object(t, users[1])["name"] != mysqlRootUser || object(t, users[1])["host"] != mysqlAnyHost {
		t.Fatalf("users = %v", users)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	name := testProject + ":us-central1:" + testMySQL
	client, err := h.provider.OpenConnector(ctx, ConnectorSpec{ConnectionName: name, Principal: principal(clientToken), Address: "127.0.0.1:0"})
	must(t, err)
	defer client.Close()
	viewer, err := h.provider.OpenConnector(ctx, ConnectorSpec{ConnectionName: name, Principal: principal(viewerToken), Address: "127.0.0.1:0"})
	must(t, err)
	defer viewer.Close()
	connector := client.listener.Addr().String()

	session, err := mysqlwire.Dial(ctx, "tcp", connector, "app_user", password)
	must(t, err)
	rows, err := session.Query(ctx, "USE app; CREATE TABLE items (id int PRIMARY KEY, name text); INSERT INTO items VALUES (1, 'one'), (2, NULL); SELECT id, name FROM items ORDER BY id")
	if err != nil || len(rows) != 2 || rows[0][1] != "one" || rows[1][1] != "" {
		t.Fatalf("rows = %v, %v", rows, err)
	}
	session.Close()
	root, err := mysqlwire.Dial(ctx, "tcp", connector, mysqlRootUser, "root-secret")
	must(t, err)
	rows, err = root.Query(ctx, "SELECT CURRENT_USER(), CURRENT_ROLE(), VERSION()")
	if err != nil || rows[0][0] != "root@%" || rows[0][1] != "`cloudsqlsuperuser`@`%`" {
		t.Fatalf("root session = %v, %v", rows, err)
	}
	t.Logf("server: %s", rows[0][2])
	var denied *mysqlwire.Error
	if _, err := root.Query(ctx, "ALTER USER 'cloudsqladmin'@'localhost' IDENTIFIED BY 'taken'"); !errors.As(err, &denied) || denied.Code != 1227 {
		t.Fatalf("root altered the administrative account: %v", err)
	}
	root.Close()
	expectMySQLRefusal(t, ctx, connector, "app_user", "wrong", mysqlwire.AccessDenied)
	expectMySQLRefusal(t, ctx, viewer.listener.Addr().String(), "app_user", password, mysqlwire.AccessDenied)
	expectMySQLRefusal(t, ctx, connector, adminRole, "guess", mysqlwire.AccessDenied)

	published := h.publishedAddress(testMySQL)
	direct, err := mysqlwire.Dial(ctx, "tcp", published, "app_user", password)
	must(t, err)
	if rows, err := direct.Query(ctx, "SELECT count(*) FROM app.items"); err != nil || rows[0][0] != "2" {
		t.Fatalf("public-IP session = %v, %v", rows, err)
	}
	direct.Close()
	expectMySQLRefusal(t, ctx, published, "app_user", "wrong", mysqlwire.AccessDenied)
	expectMySQLRefusal(t, ctx, published, adminRole, "guess", mysqlwire.AccessDenied)

	h.expect(h.invoke(adminToken, http.MethodDelete, testMySQLPath+"/databases/app", nil), http.StatusOK)
	h.expect(h.invoke(adminToken, http.MethodDelete, testMySQLPath+"/databases/app", nil), http.StatusNotFound)
	h.expect(h.invoke(adminToken, http.MethodDelete, testMySQLPath+"/users?name=app_user&host=%25", nil), http.StatusOK)
	h.expect(h.invoke(adminToken, http.MethodDelete, testMySQLPath+"/users?name=app_user", nil), http.StatusNotFound)
	// MySQL answers an unknown account with 1045 or, when it emulates the
	// default caching_sha2_password plugin, a full-auth request that needs TLS
	// or an RSA key exchange over TCP.
	var refused *mysqlwire.Error
	if _, err := mysqlwire.Dial(ctx, "tcp", published, "app_user", password); !errors.Is(err, mysqlwire.ErrAuth) && (!errors.As(err, &refused) || refused.Code != mysqlwire.AccessDenied) {
		t.Fatalf("deleted user login = %v", err)
	}

	peer := "/sql/v1beta4/projects/" + peerProject + "/instances"
	h.expect(h.invoke(peerToken, http.MethodGet, testMySQLPath, nil), http.StatusForbidden)
	h.expect(h.invoke(peerToken, http.MethodPost, peer, mysqlInsertBody()), http.StatusOK)
	peerInstance := h.expect(h.invoke(peerToken, http.MethodGet, peer+"/"+testMySQL, nil), http.StatusOK)
	if peerInstance["databaseInstalledVersion"] != "MYSQL_8_0_46" {
		t.Fatalf("peer instance = %v", peerInstance)
	}

	open, err := mysqlwire.Dial(ctx, "tcp", connector, mysqlRootUser, "root-secret")
	must(t, err)
	if got := len(fleetRoots(t)); got != before+2 {
		t.Fatalf("backend roots = %d, want %d", got, before+2)
	}
	must(t, h.manager.Reset(context.Background(), backend.Key{Service: serviceName, Scope: provider.ProjectScope(testProject)}))
	if _, err := open.Query(ctx, "SELECT 1"); err == nil {
		t.Fatal("a session survived reset")
	}
	if got := len(fleetRoots(t)); got != before+1 {
		t.Fatalf("reset left %d backend roots, want %d", got, before+1)
	}
	expectRefused(t, published)
	h.expect(h.invoke(adminToken, http.MethodGet, testMySQLPath, nil), http.StatusNotFound)
	peerPort, _ := object(t, peerInstance["ipAddresses"].([]any)[0])["port"].(float64)
	peerAddress := net.JoinHostPort("127.0.0.1", strconv.Itoa(int(peerPort)))
	peerSession, err := mysqlwire.Dial(ctx, "tcp", peerAddress, mysqlRootUser, "root-secret")
	must(t, err)
	if rows, err := peerSession.Query(ctx, "SELECT VERSION()"); err != nil || rows[0][0] != "8.0.46" {
		t.Fatalf("peer version = %v, %v", rows, err)
	}
	h.expect(h.invoke(peerToken, http.MethodDelete, peer+"/"+testMySQL, nil), http.StatusOK)
	if _, err := peerSession.Query(ctx, "SELECT 1"); err == nil {
		t.Fatal("a public-IP session survived instance deletion")
	}
	expectRefused(t, peerAddress)
}

// depthSettings adds backups, a flag, TLS, and the loopback network to an insert body.
func depthSettings(t *testing.T, body map[string]any, flag string) map[string]any {
	t.Helper()
	settings := object(t, body[keySettings])
	settings["backupConfiguration"] = map[string]any{"enabled": true, "startTime": "03:00"}
	settings["databaseFlags"] = []any{map[string]any{"name": "max_connections", "value": flag}}
	settings["ipConfiguration"] = map[string]any{"sslMode": sslEncrypted, "authorizedNetworks": []any{map[string]any{"value": "127.0.0.1/32"}}}
	return body
}

// backupID takes an on-demand backup and returns its run ID.
func (h *harness) backupID(instance string) string {
	h.t.Helper()
	operation := h.expect(h.invoke(adminToken, http.MethodPost, testInstances+"/"+instance+"/backupRuns", map[string]any{"description": "test"}), http.StatusOK)
	id, _ := object(h.t, operation["backupContext"])["backupId"].(string)
	if operation["operationType"] != "BACKUP_VOLUME" || id == "" {
		h.t.Fatalf("backup operation = %v", operation)
	}
	return id
}

// restore restores backup id of source into target.
func (h *harness) restore(target, source, id string) {
	h.t.Helper()
	body := map[string]any{"restoreBackupContext": map[string]any{"backupRunId": id, "instanceId": source, "project": testProject}}
	if operation := h.expect(h.invoke(adminToken, http.MethodPost, testInstances+"/"+target+"/restoreBackup", body), http.StatusOK); operation["operationType"] != "RESTORE_VOLUME" {
		h.t.Fatalf("restore operation = %v", operation)
	}
}

// patchIP patches settings.ipConfiguration.
func (h *harness) patchIP(instance string, ip map[string]any) {
	h.t.Helper()
	h.expect(h.invoke(adminToken, http.MethodPatch, testInstances+"/"+instance, settingsBodyOf(map[string]any{"ipConfiguration": ip})), http.StatusOK)
}

// issueCert creates a client certificate and returns the insert response's clientCert.
func (h *harness) issueCert(instance, name string) map[string]any {
	h.t.Helper()
	inserted := h.expect(h.invoke(adminToken, http.MethodPost, testInstances+"/"+instance+"/sslCerts", map[string]any{"commonName": name}), http.StatusOK)
	return object(h.t, inserted["clientCert"])
}

// TestPostgresDepth drives a real PostgreSQL server through flags applied at
// startup and on patch, TLS required on the published port (ENCRYPTED_ONLY,
// then TRUSTED_CLIENT_CERTIFICATE_REQUIRED with an sslCerts certificate that
// stops working when deleted), authorized networks by source address, and an
// on-demand physical backup restored in place and into a second instance.
func TestPostgresDepth(t *testing.T) {
	h := realHarness(t, "16420-16423")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	h.expect(h.invoke(adminToken, http.MethodPost, testInstances, depthSettings(t, insertBody(), "37")), http.StatusOK)
	instance := h.expect(h.invoke(adminToken, http.MethodGet, testPath, nil), http.StatusOK)
	ca, _ := object(t, instance["serverCaCert"])["cert"].(string)
	published, secure := h.publishedAddress(testInstance), clientTLS(t, ca, nil)

	var refused *pgwire.Error
	if _, err := pgwire.Dial(ctx, "tcp", published, defaultUser, "postgres", "root-secret"); !errors.As(err, &refused) || refused.Code != "28000" {
		t.Fatalf("cleartext login under ENCRYPTED_ONLY = %v", err)
	}
	session, err := pgwire.DialTLS(ctx, "tcp", published, defaultUser, "postgres", "root-secret", secure)
	must(t, err)
	rows, err := session.Query(ctx, "SHOW max_connections; SELECT ssl::text FROM pg_stat_ssl WHERE pid = pg_backend_pid()")
	if err != nil || rows[0][0] != "37" {
		t.Fatalf("flag = %v, %v", rows, err)
	}
	if _, err := session.Query(ctx, "CREATE TABLE items (id int); INSERT INTO items VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	id := h.backupID(testInstance)
	if _, err := session.Query(ctx, "INSERT INTO items VALUES (2)"); err != nil {
		t.Fatal(err)
	}
	h.restore(testInstance, testInstance, id)
	if _, err := session.Query(ctx, "SELECT 1"); err == nil {
		t.Fatal("a session survived the restore's restart")
	}
	session.Close()
	session, err = pgwire.DialTLS(ctx, "tcp", published, defaultUser, "postgres", "root-secret", secure)
	must(t, err)
	if rows, err := session.Query(ctx, "SELECT count(*) FROM items; SHOW max_connections"); err != nil || rows[0][0] != "1" || rows[1][0] != "37" {
		t.Fatalf("restored rows = %v, %v", rows, err)
	}
	session.Close()

	copyBody := insertBody()
	copyBody[keyName], copyBody["rootPassword"] = "demo-pg-copy", "copy-secret"
	h.expect(h.invoke(adminToken, http.MethodPost, testInstances, copyBody), http.StatusOK)
	h.restore("demo-pg-copy", testInstance, id)
	copied, err := pgwire.Dial(ctx, "tcp", h.publishedAddress("demo-pg-copy"), defaultUser, "postgres", "root-secret")
	must(t, err)
	if rows, err := copied.Query(ctx, "SELECT count(*) FROM items"); err != nil || rows[0][0] != "1" {
		t.Fatalf("rows restored into another instance = %v, %v", rows, err)
	}
	copied.Close()

	h.patchIP(testInstance, map[string]any{"sslMode": sslTrustedCerts})
	if _, err := pgwire.DialTLS(ctx, "tcp", published, defaultUser, "postgres", "root-secret", secure); err == nil {
		t.Fatal("TRUSTED_CLIENT_CERTIFICATE_REQUIRED admitted a connection without a client certificate")
	}
	cert := h.issueCert(testInstance, "app")
	trusted := clientTLS(t, ca, cert)
	session, err = pgwire.DialTLS(ctx, "tcp", published, defaultUser, "postgres", "root-secret", trusted)
	must(t, err)
	session.Close()
	fingerprint, _ := object(t, cert["certInfo"])["sha1Fingerprint"].(string)
	h.expect(h.invoke(adminToken, http.MethodDelete, testPath+"/sslCerts/"+fingerprint, nil), http.StatusOK)
	if _, err := pgwire.DialTLS(ctx, "tcp", published, defaultUser, "postgres", "root-secret", trusted); err == nil {
		t.Fatal("a deleted client certificate still connects")
	}

	h.patchIP(testInstance, map[string]any{"sslMode": sslEncrypted, "authorizedNetworks": []any{map[string]any{"value": "10.0.0.0/8"}}})
	if _, err := pgwire.DialTLS(ctx, "tcp", published, defaultUser, "postgres", "root-secret", secure); err == nil {
		t.Fatal("a source outside the authorized networks connected")
	}
	h.patchIP(testInstance, map[string]any{"authorizedNetworks": []any{map[string]any{"value": "127.0.0.0/8"}}})
	h.expect(h.invoke(adminToken, http.MethodPatch, testPath, settingsBodyOf(map[string]any{
		"databaseFlags": []any{map[string]any{"name": "max_connections", "value": "41"}}})), http.StatusOK)
	session, err = pgwire.DialTLS(ctx, "tcp", published, defaultUser, "postgres", "root-secret", secure)
	must(t, err)
	if rows, err := session.Query(ctx, "SHOW max_connections"); err != nil || rows[0][0] != "41" {
		t.Fatalf("patched flag = %v, %v", rows, err)
	}
	session.Close()
	h.expect(h.invoke(adminToken, http.MethodDelete, testPath+"/backupRuns/"+id, nil), http.StatusOK)
	if run := h.expect(h.invoke(adminToken, http.MethodGet, testPath+"/backupRuns/"+id, nil), http.StatusOK); run["status"] != "DELETED" {
		t.Fatalf("deleted run = %v", run)
	}
}

// TestMySQLDepth drives a real MySQL 8.4 server through a flag, TLS
// terminated on the published port with the handshake renumbered, a
// client certificate, and a clone backup restored in place.
func TestMySQLDepth(t *testing.T) {
	h := realHarness(t, "16430-16431")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	body := depthSettings(t, mysqlInsertBody(), "37")
	body["databaseVersion"] = "MYSQL_8_4"
	h.expect(h.invoke(adminToken, http.MethodPost, testInstances, body), http.StatusOK)
	instance := h.expect(h.invoke(adminToken, http.MethodGet, testMySQLPath, nil), http.StatusOK)
	ca, _ := object(t, instance["serverCaCert"])["cert"].(string)
	published, secure := h.publishedAddress(testMySQL), clientTLS(t, ca, nil)

	var refused *mysqlwire.Error
	if _, err := mysqlwire.Dial(ctx, "tcp", published, mysqlRootUser, "root-secret"); !errors.As(err, &refused) || refused.Code != secureTransportRequired {
		t.Fatalf("cleartext login under ENCRYPTED_ONLY = %v", err)
	}
	session, err := mysqlwire.DialTLS(ctx, "tcp", published, mysqlRootUser, "root-secret", secure)
	must(t, err)
	rows, err := session.Query(ctx, "SELECT @@max_connections")
	if err != nil || rows[0][0] != "37" {
		t.Fatalf("flag = %v, %v", rows, err)
	}
	if _, err := session.Query(ctx, "CREATE DATABASE app; CREATE TABLE app.items (id int PRIMARY KEY); INSERT INTO app.items VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	id := h.backupID(testMySQL)
	if _, err := session.Query(ctx, "INSERT INTO app.items VALUES (2)"); err != nil {
		t.Fatal(err)
	}
	session.Close()
	h.restore(testMySQL, testMySQL, id)
	session, err = mysqlwire.DialTLS(ctx, "tcp", published, mysqlRootUser, "root-secret", secure)
	must(t, err)
	if rows, err := session.Query(ctx, "SELECT count(*) FROM app.items"); err != nil || rows[0][0] != "1" {
		t.Fatalf("restored rows = %v, %v", rows, err)
	}
	session.Close()

	h.patchIP(testMySQL, map[string]any{"sslMode": sslTrustedCerts})
	if _, err := mysqlwire.DialTLS(ctx, "tcp", published, mysqlRootUser, "root-secret", secure); err == nil {
		t.Fatal("TRUSTED_CLIENT_CERTIFICATE_REQUIRED admitted a connection without a client certificate")
	}
	session, err = mysqlwire.DialTLS(ctx, "tcp", published, mysqlRootUser, "root-secret", clientTLS(t, ca, h.issueCert(testMySQL, "app")))
	must(t, err)
	session.Close()
	h.expect(h.invoke(adminToken, http.MethodDelete, testMySQLPath+"/backupRuns/"+id, nil), http.StatusOK)
}
