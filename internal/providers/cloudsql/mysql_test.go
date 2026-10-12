package cloudsql

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

const (
	testMySQL     = "demo-mysql"
	testMySQLPath = testInstances + "/" + testMySQL
)

// mysqlInsertBody is a minimal MYSQL_8_0 insert request.
func mysqlInsertBody() map[string]any {
	return map[string]any{keyName: testMySQL, "region": "us-central1", "databaseVersion": testMySQLVersion, "rootPassword": "root-secret",
		keySettings: map[string]any{"tier": "db-f1-micro"}}
}

// expectError asserts one administrator request's error status and exact message.
func (h *harness) expectError(verb, path string, body any, code int, message string) {
	h.t.Helper()
	reply := h.expect(h.invoke(adminToken, verb, path, body), code)
	if got, _ := object(h.t, reply["error"])["message"].(string); got != message {
		h.t.Fatalf("%s %s message = %q, want %q", verb, path, got, message)
	}
}

// createMySQL inserts the MySQL test instance.
func (h *harness) createMySQL() {
	h.t.Helper()
	h.expect(h.invoke(adminToken, http.MethodPost, testInstances, mysqlInsertBody()), http.StatusOK)
}

// sinceStatements returns the administrative SQL issued after a mark.
func (h *harness) sinceStatements(mark int) string {
	return strings.Join(h.fleet().statements()[mark:], "\n")
}

// items returns a list response's items.
func items(t *testing.T, body map[string]any) []any {
	t.Helper()
	list, ok := body["items"].([]any)
	if !ok {
		t.Fatalf("no items: %v", body)
	}
	return list
}

func TestMySQLInstance(t *testing.T) {
	h := newHarness(t)
	h.expect(h.invoke(viewerToken, http.MethodPost, testInstances, mysqlInsertBody()), http.StatusForbidden)
	flagged := mysqlInsertBody()
	object(t, flagged[keySettings])["databaseFlags"] = []any{map[string]any{"name": "max_connections", "value": "20"}}
	h.expectError(http.MethodPost, testInstances, flagged, http.StatusNotImplemented, "Cloud SQL coverage gap: settings.databaseFlags")
	if h.fleet() != nil {
		t.Fatal("a refused insert started a backend")
	}
	h.createMySQL()
	instance := h.expect(h.invoke(viewerToken, http.MethodGet, testMySQLPath, nil), http.StatusOK)
	if addresses, _ := instance["ipAddresses"].([]any); len(addresses) != 1 || instance["databaseVersion"] != testMySQLVersion || instance["databaseInstalledVersion"] != "MYSQL_8_0_46" {
		t.Fatalf("instance = %v", instance)
	}
}

func TestMySQLDatabases(t *testing.T) {
	h := newHarness(t)
	h.createMySQL()
	mark := len(h.fleet().statements())
	h.expect(h.invoke(viewerToken, http.MethodPost, testMySQLPath+"/databases", map[string]any{keyName: testDatabase}), http.StatusForbidden)
	h.expect(h.invoke(adminToken, http.MethodPost, testMySQLPath+"/databases", map[string]any{keyName: testDatabase,
		"charset": "utf8mb4", "collation": "utf8mb4_unicode_ci"}), http.StatusOK)
	h.expect(h.invoke(adminToken, http.MethodPost, testMySQLPath+"/databases", map[string]any{keyName: "plain"}), http.StatusOK)
	h.expectError(http.MethodPost, testMySQLPath+"/databases", map[string]any{keyName: "bad", "collation": "en-US"},
		http.StatusBadRequest, "Invalid charset or collation.")
	if got := h.sinceStatements(mark); got != "CREATE DATABASE `app` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci\nCREATE DATABASE `plain`" {
		t.Fatalf("statements = %q", got)
	}
	if databases := h.expect(h.invoke(viewerToken, http.MethodGet, testMySQLPath+"/databases", nil), http.StatusOK); count(databases) != 4 {
		t.Fatalf("databases = %v", databases)
	}
	h.expect(h.invoke(viewerToken, http.MethodGet, testMySQLPath+"/databases/mysql", nil), http.StatusOK)
	h.expectError(http.MethodDelete, testMySQLPath+"/databases/mysql", nil, http.StatusBadRequest,
		"gcp-local: the MySQL system database mysql cannot be deleted.")
	h.expect(h.invoke(adminToken, http.MethodDelete, testMySQLPath+"/databases/app", nil), http.StatusOK)
}

func TestMySQLUsers(t *testing.T) {
	h := newHarness(t)
	h.createMySQL()
	mark := len(h.fleet().statements())
	h.expect(h.invoke(adminToken, http.MethodPost, testMySQLPath+"/users", map[string]any{keyName: "app", "host": "10.0.0.%", "password": "it's\\secret"}), http.StatusOK)
	h.expect(h.invoke(adminToken, http.MethodPost, testMySQLPath+"/users", map[string]any{keyName: "nopassword"}), http.StatusOK)
	h.expectError(http.MethodPost, testMySQLPath+"/users", map[string]any{keyName: "bad", "host": "a b"}, http.StatusBadRequest, "Invalid user.")
	h.expectError(http.MethodPost, testMySQLPath+"/users", map[string]any{keyName: "mysql.sys"}, http.StatusBadRequest, "Invalid user.")
	want := `CREATE USER 'app'@'10.0.0.%' IDENTIFIED WITH mysql_native_password BY 'it\'s\\secret'; GRANT 'cloudsqlsuperuser'@'%' TO 'app'@'10.0.0.%'; SET DEFAULT ROLE 'cloudsqlsuperuser'@'%' TO 'app'@'10.0.0.%'` + "\n" +
		`CREATE USER 'nopassword'@'%' ACCOUNT LOCK; GRANT 'cloudsqlsuperuser'@'%' TO 'nopassword'@'%'; SET DEFAULT ROLE 'cloudsqlsuperuser'@'%' TO 'nopassword'@'%'`
	if got := h.sinceStatements(mark); got != want {
		t.Fatalf("statements = %q", got)
	}
	users := items(t, h.expect(h.invoke(viewerToken, http.MethodGet, testMySQLPath+"/users", nil), http.StatusOK))
	if first := object(t, users[0]); len(users) != 3 || first["host"] != mysqlAnyHost || first[keyName] != mysqlRootUser {
		t.Fatalf("users = %v", users)
	}
	h.expect(h.invoke(adminToken, http.MethodDelete, testMySQLPath+"/users?name=missing", nil), http.StatusNotFound)
	mark = len(h.fleet().statements())
	h.expect(h.invoke(adminToken, http.MethodDelete, testMySQLPath+"/users?name=app&host=10.0.0.%25", nil), http.StatusOK)
	if got := h.sinceStatements(mark); got != `DROP USER 'app'@'10.0.0.%'` {
		t.Fatalf("delete statement = %q", got)
	}
}

func TestUserFieldGaps(t *testing.T) {
	h := newHarness(t)
	h.createInstance()
	h.expectError(http.MethodPost, testPath+"/users", map[string]any{keyName: "app", "passwordPolicy": map[string]any{"allowedFailedAttempts": 3}},
		http.StatusNotImplemented, "Cloud SQL coverage gap: users.insert passwordPolicy")
	h.expectError(http.MethodPost, testPath+"/users", map[string]any{keyName: "app", "dualPasswordType": "DUAL_PASSWORD"},
		http.StatusNotImplemented, "Cloud SQL coverage gap: users.insert dualPasswordType")
}

// mysqlHandshakeResponse builds a 4.1 HandshakeResponse for user.
func mysqlHandshakeResponse(user string, flags uint32) []byte {
	response := binary.LittleEndian.AppendUint32(nil, flags)
	response = binary.LittleEndian.AppendUint32(response, mysqlMaxPacket)
	response = append(response, mysqlCharsetUTF8MB4)
	response = append(response, make([]byte, 23)...)
	return append(append(append(response, user...), 0, 0), mysqlNativePassword+"\x00"...)
}

// clientFlags are the capabilities of the test handshake responses.
const clientFlags = capProtocol41 | capSecureConnection | capPluginAuth

// dialMySQLHandshake reads the greeting at address, answers it for user,
// and returns the greeting and whatever follows. A refusal before the
// greeting returns the ERR payload as the greeting.
func dialMySQLHandshake(t *testing.T, address, user string) ([]byte, []byte) {
	t.Helper()
	conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(t.Context(), "tcp", address)
	must(t, err)
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	hello, _, err := readMySQLPacket(conn)
	must(t, err)
	if hello[0] == 0xff {
		return hello, nil
	}
	_, err = conn.Write(mysqlPacket(1, mysqlHandshakeResponse(user, clientFlags)))
	must(t, err)
	reply, _ := io.ReadAll(conn)
	return hello, reply
}

// expectForwarded waits for the fake server to receive one packet.
func expectForwarded(t *testing.T, server *fakeServer, want []byte) {
	t.Helper()
	select {
	case got := <-server.received:
		if !bytes.Equal(got, want) {
			t.Fatalf("forwarded = %q, want %q", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("nothing was forwarded to MySQL")
	}
}

func TestMySQLPublicAddress(t *testing.T) {
	h := newHarness(t)
	h.provider.pool = onePortPool(t)
	h.createMySQL()
	address := h.publishedAddress(testMySQL)
	server := h.fleet().server(testMySQL)

	hello, reply := dialMySQLHandshake(t, address, adminRole)
	parsed, err := parseGreeting(hello)
	must(t, err)
	if flags := binary.LittleEndian.Uint16(hello[bytes.IndexByte(hello, 0)+14:]); flags&capSSL != 0 || string(parsed.scramble) != "abcdefghijklmnopqrst" {
		t.Fatalf("greeting = %x", hello)
	}
	if !bytes.Equal(reply, mysqlErrorPacket(2, mysqlAccessDenied, "28000", "gcp-local: the internal administrative role is not reachable through the connector")) {
		t.Fatalf("admin role reply = %q", reply)
	}
	_, reply = dialMySQLHandshake(t, address, mysqlRootUser)
	expectForwarded(t, server, mysqlPacket(1, mysqlHandshakeResponse(mysqlRootUser, clientFlags)))
	if len(reply) != 0 {
		t.Fatalf("reply = %q", reply)
	}
	h.expect(h.invoke(adminToken, http.MethodDelete, testMySQLPath, nil), http.StatusOK)
	expectRefused(t, address)
}

func TestMySQLConnectorRefusesBeforeGreeting(t *testing.T) {
	h := newHarness(t)
	viewer := h.openConnector(viewerToken, testProject+":us-central1:"+testMySQL)
	h.createMySQL()
	refused, _ := dialMySQLHandshake(t, viewer.listener.Addr().String(), mysqlRootUser)
	failure := parseMySQLError(refused)
	if !strings.HasPrefix(failure.Error(), "ERROR 1045 (28000): gcp-local: "+principal(viewerToken)+" is not authorized to connect") ||
		!strings.Contains(failure.Error(), "cloudsql.instances.connect") {
		t.Fatalf("viewer refusal = %v", failure)
	}
}

func TestMySQLScrambles(t *testing.T) {
	// Expected values were computed independently with Python's hashlib from
	// the protocol definitions; the integration test authenticates against mysqld.
	scramble := []byte{9, 103, 5, 73, 72, 88, 107, 99, 122, 7, 125, 16, 123, 117, 18, 79, 8, 50, 4, 96}
	if got := hex.EncodeToString(nativeScramble(scramble, "secret")); got != "de24aeb32dc1ea308b37ba2d7fb910e5c78944b4" {
		t.Fatalf("native scramble = %s", got)
	}
	if got := hex.EncodeToString(sha2Scramble(scramble, "secret")); got != "78b2547329d46aa4fecdb2432322f129dc9befc7603de8e65dc6bbde1b9579ce" {
		t.Fatalf("caching_sha2 scramble = %s", got)
	}
	if nativeScramble(scramble, "") != nil || sha2Scramble(scramble, "") != nil {
		t.Fatal("empty passwords must send no auth data")
	}
}

func TestMySQLHandshakeUser(t *testing.T) {
	if user, ok := handshakeUser(mysqlHandshakeResponse("app", capProtocol41)); !ok || user != "app" {
		t.Fatalf("user = %q", user)
	}
	if _, ok := handshakeUser(mysqlHandshakeResponse("app", capProtocol41|capSSL)); ok {
		t.Fatal("accepted an SSLRequest")
	}
	if _, ok := handshakeUser(mysqlHandshakeResponse("app", 0)); ok {
		t.Fatal("accepted a pre-4.1 handshake")
	}
}

func TestMySQLRowsAndQuoting(t *testing.T) {
	row, err := parseTextRow([]byte{0xfb, 3, 'a', 'b', 'c', 0xfc, 1, 0, 'z'}, 3)
	if err != nil || strings.Join(row, ",") != ",abc,z" {
		t.Fatalf("row = %q, %v", row, err)
	}
	if _, err := parseTextRow([]byte{5, 'a'}, 1); err == nil {
		t.Fatal("accepted a truncated row")
	}
	if failure := parseMySQLError(mysqlErrorPacket(0, 1045, "28000", "denied")[4:]); failure.Error() != "ERROR 1045 (28000): denied" {
		t.Fatalf("error = %v", failure)
	}
	if quoteMySQLIdentifier("a`b") != "`a``b`" || quoteMySQLLiteral(`a'b\c`) != `'a\'b\\c'` {
		t.Fatal("MySQL quoting is wrong")
	}
}
