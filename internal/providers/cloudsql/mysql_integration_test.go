//go:build cloudsqlintegration

package cloudsql

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
)

// expectMySQLRefusal asserts a MySQL login fails with one error number.
func expectMySQLRefusal(t *testing.T, ctx context.Context, address, user, password string, code uint16) {
	t.Helper()
	var refused *mysqlError
	if _, err := dialMySQL(ctx, "tcp", address, user, password); !errors.As(err, &refused) || refused.Code != code {
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

	session, err := dialMySQL(ctx, "tcp", connector, "app_user", password)
	must(t, err)
	rows, err := session.query(ctx, "USE app; CREATE TABLE items (id int PRIMARY KEY, name text); INSERT INTO items VALUES (1, 'one'), (2, NULL); SELECT id, name FROM items ORDER BY id")
	if err != nil || len(rows) != 2 || rows[0][1] != "one" || rows[1][1] != "" {
		t.Fatalf("rows = %v, %v", rows, err)
	}
	session.close()
	root, err := dialMySQL(ctx, "tcp", connector, mysqlRootUser, "root-secret")
	must(t, err)
	rows, err = root.query(ctx, "SELECT CURRENT_USER(), CURRENT_ROLE(), VERSION()")
	if err != nil || rows[0][0] != "root@%" || rows[0][1] != "`cloudsqlsuperuser`@`%`" {
		t.Fatalf("root session = %v, %v", rows, err)
	}
	t.Logf("server: %s", rows[0][2])
	var denied *mysqlError
	if _, err := root.query(ctx, "ALTER USER 'cloudsqladmin'@'localhost' IDENTIFIED BY 'taken'"); !errors.As(err, &denied) || denied.Code != 1227 {
		t.Fatalf("root altered the administrative account: %v", err)
	}
	root.close()
	expectMySQLRefusal(t, ctx, connector, "app_user", "wrong", mysqlAccessDenied)
	expectMySQLRefusal(t, ctx, viewer.listener.Addr().String(), "app_user", password, mysqlAccessDenied)
	expectMySQLRefusal(t, ctx, connector, adminRole, "guess", mysqlAccessDenied)

	published := h.publishedAddress(testMySQL)
	direct, err := dialMySQL(ctx, "tcp", published, "app_user", password)
	must(t, err)
	if rows, err := direct.query(ctx, "SELECT count(*) FROM app.items"); err != nil || rows[0][0] != "2" {
		t.Fatalf("public-IP session = %v, %v", rows, err)
	}
	direct.close()
	expectMySQLRefusal(t, ctx, published, "app_user", "wrong", mysqlAccessDenied)
	expectMySQLRefusal(t, ctx, published, adminRole, "guess", mysqlAccessDenied)

	h.expect(h.invoke(adminToken, http.MethodDelete, testMySQLPath+"/databases/app", nil), http.StatusOK)
	h.expect(h.invoke(adminToken, http.MethodDelete, testMySQLPath+"/databases/app", nil), http.StatusNotFound)
	h.expect(h.invoke(adminToken, http.MethodDelete, testMySQLPath+"/users?name=app_user&host=%25", nil), http.StatusOK)
	h.expect(h.invoke(adminToken, http.MethodDelete, testMySQLPath+"/users?name=app_user", nil), http.StatusNotFound)
	// MySQL answers an unknown account with 1045 or, when it emulates the
	// default caching_sha2_password plugin, a full-auth request that needs TLS
	// or an RSA key exchange over TCP.
	var refused *mysqlError
	if _, err := dialMySQL(ctx, "tcp", published, "app_user", password); !errors.Is(err, errMySQLAuth) && (!errors.As(err, &refused) || refused.Code != mysqlAccessDenied) {
		t.Fatalf("deleted user login = %v", err)
	}

	peer := "/sql/v1beta4/projects/" + peerProject + "/instances"
	h.expect(h.invoke(peerToken, http.MethodGet, testMySQLPath, nil), http.StatusForbidden)
	h.expect(h.invoke(peerToken, http.MethodPost, peer, mysqlInsertBody()), http.StatusOK)
	peerInstance := h.expect(h.invoke(peerToken, http.MethodGet, peer+"/"+testMySQL, nil), http.StatusOK)
	if peerInstance["databaseInstalledVersion"] != "MYSQL_8_0_46" {
		t.Fatalf("peer instance = %v", peerInstance)
	}

	open, err := dialMySQL(ctx, "tcp", connector, mysqlRootUser, "root-secret")
	must(t, err)
	if got := len(fleetRoots(t)); got != before+2 {
		t.Fatalf("backend roots = %d, want %d", got, before+2)
	}
	must(t, h.manager.Reset(context.Background(), backend.Key{Service: serviceName, Scope: provider.ProjectScope(testProject)}))
	if _, err := open.query(ctx, "SELECT 1"); err == nil {
		t.Fatal("a session survived reset")
	}
	if got := len(fleetRoots(t)); got != before+1 {
		t.Fatalf("reset left %d backend roots, want %d", got, before+1)
	}
	expectRefused(t, published)
	h.expect(h.invoke(adminToken, http.MethodGet, testMySQLPath, nil), http.StatusNotFound)
	peerPort, _ := object(t, peerInstance["ipAddresses"].([]any)[0])["port"].(float64)
	peerAddress := net.JoinHostPort("127.0.0.1", strconv.Itoa(int(peerPort)))
	peerSession, err := dialMySQL(ctx, "tcp", peerAddress, mysqlRootUser, "root-secret")
	must(t, err)
	if rows, err := peerSession.query(ctx, "SELECT VERSION()"); err != nil || rows[0][0] != "8.0.46" {
		t.Fatalf("peer version = %v, %v", rows, err)
	}
	h.expect(h.invoke(peerToken, http.MethodDelete, peer+"/"+testMySQL, nil), http.StatusOK)
	if _, err := peerSession.query(ctx, "SELECT 1"); err == nil {
		t.Fatal("a public-IP session survived instance deletion")
	}
	expectRefused(t, peerAddress)
}
