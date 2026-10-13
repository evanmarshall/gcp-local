package cloudsql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/databases/cloudsql/server"
	"github.com/evanmarshall/gcp-local/internal/providers/databases/cloudsql/server/mysqlwire"
	"github.com/evanmarshall/gcp-local/internal/providers/databases/cloudsql/server/pgwire"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/providers/provider/providertest"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// Names repeated across the provider tests.
const (
	testProject      = "demo-sql"
	peerProject      = "peer-sql"
	testInstance     = "demo-pg"
	testInstances    = "/sql/v1beta4/projects/" + testProject + "/instances"
	testOperations   = "/sql/v1beta4/projects/" + testProject + "/operations"
	testVersion      = "POSTGRES_16"
	testMySQLVersion = "MYSQL_8_0"
	testDatabase     = "app"
	smallTier        = "db-g1-small"
	testPath         = testInstances + "/" + testInstance
	adminToken       = "admin"
	clientToken      = "client"
	viewerToken      = "viewer"
	peerToken        = "peer"
)

// harness drives the provider through the gateway with one fake fleet per project.
type harness struct {
	t        *testing.T
	gate     *gateway.Gateway
	engine   *iam.Engine
	index    *state.Index
	manager  *backend.Manager
	provider *Provider
	mu       sync.Mutex
	fleets   map[string][]*fakeFleet
}

func principal(token string) string {
	project := testProject
	if token == peerToken {
		project = peerProject
	}
	return "serviceAccount:" + token + "@" + project + ".iam.gserviceaccount.com"
}

// grants seeds the project policies shared by the unit and integration tests.
func grants(t *testing.T, index *state.Index, engine *iam.Engine) {
	t.Helper()
	bindings := map[string][]iam.Binding{
		testProject: {
			{Role: "roles/cloudsql.admin", Members: []string{principal(adminToken)}},
			{Role: "roles/cloudsql.client", Members: []string{principal(clientToken)}},
			{Role: "roles/cloudsql.viewer", Members: []string{principal(viewerToken)}},
		},
		peerProject: {{Role: "roles/cloudsql.admin", Members: []string{principal(peerToken)}}},
	}
	for project, binding := range bindings {
		must(t, index.Put(ownerProjects, state.Resource{Name: provider.ProjectResource(project), Service: ownerProjects,
			Kind: kindProject, Number: "123456", Scope: provider.ProjectScope(project)}))
		_, err := engine.SetPolicy(provider.ProjectResource(project), iam.Policy{Bindings: binding})
		must(t, err)
	}
}

// skipUnrecorded skips until the roles/cloudsql.* snapshots are recorded.
func skipUnrecorded(t *testing.T, err error) {
	t.Helper()
	if errors.Is(err, ErrRolesUnrecorded) {
		t.Skip("roles/cloudsql.* snapshots are not recorded yet; see docs/services/databases/cloudsql.md")
	}
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	if err := rolesRecorded(); err != nil {
		skipUnrecorded(t, err)
		t.Fatal(err)
	}
	h := &harness{t: t, index: state.NewIndex(), fleets: map[string][]*fakeFleet{}}
	engine, err := iam.New(h.index)
	must(t, err)
	h.engine = engine
	grants(t, h.index, engine)
	h.manager = backend.NewManager(time.Second, time.Second)
	registry := gateway.NewRegistry()
	h.provider, err = newProvider(registry, h.manager, h.index, engine, map[string]string{testVersion: "16_15", testMySQLVersion: "8_0_46"}, func(scope state.Scope) (backend.Backend, error) {
		h.mu.Lock()
		defer h.mu.Unlock()
		fleet := newFakeFleet()
		h.fleets[scope.Project] = append(h.fleets[scope.Project], fleet)
		return fleet, nil
	})
	must(t, err)
	t.Cleanup(func() { must(t, h.manager.Shutdown(context.Background())) })
	h.gate = gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: principal(token), Scopes: []string{cloudPlatform}}, nil
	})
	return h
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// fleet returns the test project's current fake fleet, or nil when none started.
func (h *harness) fleet() *fakeFleet {
	h.mu.Lock()
	defer h.mu.Unlock()
	fleets := h.fleets[testProject]
	if len(fleets) == 0 {
		return nil
	}
	return fleets[len(fleets)-1]
}

// invoke issues one REST request as serviceAccount:<token>.
func (h *harness) invoke(token, verb, path string, body any) *httptest.ResponseRecorder {
	h.t.Helper()
	var encoded []byte
	if body != nil {
		var err error
		encoded, err = json.Marshal(body)
		must(h.t, err)
	}
	request := httptest.NewRequestWithContext(context.Background(), verb, path, bytes.NewReader(encoded))
	request.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	h.gate.ServeHTTP(recorder, request)
	return recorder
}

// expect asserts one response status and decodes the body.
func (h *harness) expect(recorder *httptest.ResponseRecorder, code int) map[string]any {
	h.t.Helper()
	if recorder.Code != code {
		h.t.Fatalf("status = %d, want %d: %s", recorder.Code, code, recorder.Body.String())
	}
	var body map[string]any
	must(h.t, json.Unmarshal(recorder.Body.Bytes(), &body))
	return body
}

// object narrows a decoded JSON value to an object, failing the test otherwise.
func object(t *testing.T, value any) map[string]any {
	t.Helper()
	result, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("not an object: %v", value)
	}
	return result
}

// count returns the number of items in a list response.
func count(body map[string]any) int {
	items, _ := body["items"].([]any)
	return len(items)
}

// settingsBodyOf wraps settings fields in a patch body.
func settingsBodyOf(fields map[string]any) map[string]any {
	return map[string]any{keySettings: fields}
}

// insertBody is a minimal POSTGRES_16 insert request for the test instance.
func insertBody() map[string]any {
	return map[string]any{keyName: testInstance, "region": "us-central1", "databaseVersion": testVersion, "rootPassword": "root-secret",
		keySettings: map[string]any{"tier": "db-f1-micro", "edition": "ENTERPRISE", "userLabels": map[string]string{"managed-by": "gcp-local"}}}
}

// createInstance inserts the test instance and checks the completed operation.
func (h *harness) createInstance() {
	h.t.Helper()
	operation := h.expect(h.invoke(adminToken, http.MethodPost, testInstances, insertBody()), http.StatusOK)
	// The integration harness keeps the production latency, so the operation may still be PENDING.
	if operation[keyKind] != "sql#operation" || (operation["status"] != statusDone && operation["status"] != statusPending) || operation["operationType"] != "CREATE" ||
		operation["targetId"] != testInstance || operation["user"] != "admin@demo-sql.iam.gserviceaccount.com" {
		h.t.Fatalf("operation = %v", operation)
	}
}

func TestDeniedInsertStartsNoBackend(t *testing.T) {
	h := newHarness(t)
	h.expect(h.invoke(peerToken, http.MethodPost, testInstances, insertBody()), http.StatusForbidden)
	h.expect(h.invoke(viewerToken, http.MethodPost, testInstances, insertBody()), http.StatusForbidden)
	if status := h.manager.Status(); len(status) != 0 || h.fleet() != nil {
		t.Fatalf("a denied request started a backend: %+v", status)
	}
	body := h.expect(h.invoke(adminToken, http.MethodPost, testInstances, map[string]any{keyName: "mssql", "databaseVersion": "SQLSERVER_2022_EXPRESS",
		keySettings: map[string]any{"tier": "db-f1-micro"}}), http.StatusNotImplemented)
	if message, _ := object(t, body["error"])["message"].(string); !strings.Contains(message, `coverage gap: databaseVersion "SQLSERVER_2022_EXPRESS" (installed: MYSQL_8_0, POSTGRES_16)`) {
		t.Fatalf("unsupported version = %v", body)
	}
	if h.fleet() != nil {
		t.Fatal("an unsupported request started a backend")
	}
}

func TestInstanceReads(t *testing.T) {
	h := newHarness(t)
	h.createInstance()
	h.expect(h.invoke(adminToken, http.MethodPost, testInstances, insertBody()), http.StatusConflict)
	instance := h.expect(h.invoke(viewerToken, http.MethodGet, testPath, nil), http.StatusOK)
	if instance["connectionName"] != testProject+":us-central1:"+testInstance || instance["state"] != statusRunnable ||
		instance["databaseVersion"] != testVersion || instance["databaseInstalledVersion"] != "POSTGRES_16_15" {
		t.Fatalf("instance = %v", instance)
	}
	if list := h.expect(h.invoke(viewerToken, http.MethodGet, testInstances, nil), http.StatusOK); count(list) != 1 {
		t.Fatalf("list = %v", list)
	}
}

func TestInstancePatch(t *testing.T) {
	h := newHarness(t)
	h.createInstance()
	h.expect(h.invoke(viewerToken, http.MethodPatch, testPath, settingsBodyOf(map[string]any{"tier": smallTier})), http.StatusForbidden)
	h.expect(h.invoke(adminToken, http.MethodPatch, testPath, settingsBodyOf(map[string]any{
		keyDeletionProtection: true, "userLabels": map[string]string{"team": "data"}, keySettingsVersion: "1"})), http.StatusOK)
	h.expect(h.invoke(adminToken, http.MethodPatch, testPath, settingsBodyOf(map[string]any{"tier": smallTier, keySettingsVersion: "1"})), http.StatusPreconditionFailed)
	h.expect(h.invoke(adminToken, http.MethodPatch, testPath, map[string]any{"region": "europe-west1"}), http.StatusNotImplemented)
	settings := object(t, h.expect(h.invoke(adminToken, http.MethodGet, testPath, nil), http.StatusOK)[keySettings])
	if settings[keyDeletionProtection] != true || settings[keySettingsVersion] != "2" || object(t, settings["userLabels"])["team"] != "data" {
		t.Fatalf("settings = %v", settings)
	}
	h.expect(h.invoke(adminToken, http.MethodDelete, testPath, nil), http.StatusBadRequest)
}

func TestInstanceDeleteAndOperations(t *testing.T) {
	h := newHarness(t)
	h.createInstance()
	h.expect(h.invoke(adminToken, http.MethodPatch, testPath, settingsBodyOf(map[string]any{"tier": smallTier})), http.StatusOK)
	h.expect(h.invoke(viewerToken, http.MethodDelete, testPath, nil), http.StatusForbidden)
	deleted := h.expect(h.invoke(adminToken, http.MethodDelete, testPath, nil), http.StatusOK)
	h.expect(h.invoke(adminToken, http.MethodGet, testPath, nil), http.StatusNotFound)
	if _, ok := h.index.Get(instanceResource(testProject, testInstance)); ok || h.fleet().server(testInstance) != nil {
		t.Fatal("delete left the reference or server behind")
	}
	operations := h.expect(h.invoke(viewerToken, http.MethodGet, testOperations+"?instance="+testInstance, nil), http.StatusOK)
	items, _ := operations["items"].([]any)
	if len(items) != 3 || object(t, items[0])["operationType"] != "DELETE" || object(t, items[2])["operationType"] != "CREATE" {
		t.Fatalf("operations = %v", operations)
	}
	name, _ := deleted[keyName].(string)
	read := h.expect(h.invoke(viewerToken, http.MethodGet, testOperations+"/"+name, nil), http.StatusOK)
	if read["operationType"] != "DELETE" || read["status"] != statusDone {
		t.Fatalf("operation = %v", read)
	}
}

func TestDatabasesAndUsers(t *testing.T) {
	h := newHarness(t)
	h.expect(h.invoke(adminToken, http.MethodPost, testPath+"/databases", map[string]any{keyName: testDatabase}), http.StatusNotFound)
	h.createInstance()
	h.expect(h.invoke(viewerToken, http.MethodPost, testPath+"/databases", map[string]any{keyName: testDatabase}), http.StatusForbidden)
	h.expect(h.invoke(viewerToken, http.MethodPost, testPath+"/users", map[string]any{keyName: testDatabase, "password": "x"}), http.StatusForbidden)
	before := len(h.fleet().statements())
	h.expect(h.invoke(adminToken, http.MethodPost, testPath+"/databases", map[string]any{keyName: testDatabase}), http.StatusOK)
	h.expect(h.invoke(adminToken, http.MethodPost, testPath+"/databases", map[string]any{keyName: adminRole}), http.StatusBadRequest)
	h.expect(h.invoke(adminToken, http.MethodPost, testPath+"/users", map[string]any{keyName: testDatabase, "password": "it's\\secret"}), http.StatusOK)
	h.expect(h.invoke(adminToken, http.MethodPost, testPath+"/users", map[string]any{keyName: "iam@x", "type": "CLOUD_IAM_USER"}), http.StatusNotImplemented)
	statements := h.fleet().statements()[before:]
	if len(statements) != 2 || statements[0] != `CREATE DATABASE "app" OWNER cloudsqlsuperuser` ||
		statements[1] != `CREATE ROLE "app" LOGIN CREATEDB CREATEROLE IN ROLE cloudsqlsuperuser PASSWORD E'it''s\\secret'` {
		t.Fatalf("statements = %q", statements)
	}
	database := h.expect(h.invoke(viewerToken, http.MethodGet, testPath+"/databases/app", nil), http.StatusOK)
	if database[keyKind] != "sql#database" || database["charset"] != "UTF8" || database["instance"] != testInstance {
		t.Fatalf("database = %v", database)
	}
	h.expect(h.invoke(viewerToken, http.MethodGet, testPath+"/databases/missing", nil), http.StatusNotFound)
	if databases := h.expect(h.invoke(viewerToken, http.MethodGet, testPath+"/databases", nil), http.StatusOK); count(databases) != 2 {
		t.Fatalf("databases = %v", databases)
	}
	if users := h.expect(h.invoke(viewerToken, http.MethodGet, testPath+"/users", nil), http.StatusOK); count(users) != 2 {
		t.Fatalf("users = %v", users)
	}
	h.expect(h.invoke(adminToken, http.MethodDelete, testPath+"/databases/app", nil), http.StatusBadRequest)
	h.expect(h.invoke(adminToken, http.MethodDelete, testPath+"/users?name="+adminRole, nil), http.StatusBadRequest)
	h.expect(h.invoke(adminToken, http.MethodDelete, testPath+"/users?name=app", nil), http.StatusOK)
}

func TestProjectIsolationAndReset(t *testing.T) {
	h := newHarness(t)
	h.createInstance()
	peer := "/sql/v1beta4/projects/" + peerProject + "/instances"
	h.expect(h.invoke(peerToken, http.MethodGet, testPath, nil), http.StatusForbidden)
	h.expect(h.invoke(peerToken, http.MethodGet, peer+"/"+testInstance, nil), http.StatusNotFound)
	h.expect(h.invoke(peerToken, http.MethodPost, peer, insertBody()), http.StatusOK)
	original := h.fleet()
	must(t, h.manager.Reset(context.Background(), backend.Key{Service: serviceName, Scope: provider.ProjectScope(testProject)}))
	if original.stops != 1 || original.server(testInstance) != nil {
		t.Fatal("reset did not stop the project's servers")
	}
	h.expect(h.invoke(adminToken, http.MethodGet, testPath, nil), http.StatusNotFound)
	if _, ok := h.index.Get(instanceResource(testProject, testInstance)); ok {
		t.Fatal("reset left the instance reference")
	}
	if operations := h.expect(h.invoke(adminToken, http.MethodGet, testOperations, nil), http.StatusOK); count(operations) != 0 {
		t.Fatalf("reset kept operations: %v", operations)
	}
	h.expect(h.invoke(peerToken, http.MethodGet, peer+"/"+testInstance, nil), http.StatusOK)
	h.createInstance()
}

// TestPatchIsAtomic rejects a patch with one unmodeled field without applying
// the modeled one.
func TestPatchIsAtomic(t *testing.T) {
	h := newHarness(t)
	h.createInstance()
	h.expect(h.invoke(adminToken, http.MethodPatch, testPath, settingsBodyOf(map[string]any{"tier": smallTier, "maintenanceWindow": map[string]any{"day": 1}})), http.StatusNotImplemented)
	settings := object(t, h.expect(h.invoke(adminToken, http.MethodGet, testPath, nil), http.StatusOK)[keySettings])
	if settings["tier"] != "db-f1-micro" || settings[keySettingsVersion] != "1" {
		t.Fatalf("a rejected patch changed the instance: %v", settings)
	}
}

// TestConcurrentInstanceRequests races insert, get, list, patch, database
// create, and delete on one instance; -race fails it if a handler touches the
// catalog unlocked.
func TestConcurrentInstanceRequests(t *testing.T) {
	h := newHarness(t)
	providertest.Hammer(t, h.gate, adminToken, 8, 20, func(int) []providertest.Request {
		return []providertest.Request{
			{Method: http.MethodPost, Path: testInstances, Body: insertBody(), Want: []int{200, 409}},
			{Method: http.MethodGet, Path: testPath, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: testInstances, Want: []int{200}},
			{Method: http.MethodPatch, Path: testPath, Body: settingsBodyOf(map[string]any{"tier": smallTier, "userLabels": map[string]string{"worker": "w"}}), Want: []int{200, 404}},
			{Method: http.MethodPost, Path: testPath + "/databases", Body: map[string]any{keyName: testDatabase}, Want: []int{200, 404, 409}},
			{Method: http.MethodGet, Path: testOperations, Want: []int{200}},
			{Method: http.MethodDelete, Path: testPath, Want: []int{200, 404}},
		}
	})
}

// TestConcurrentMySQLRequests races the MySQL paths on one instance: insert,
// get, database and user create, list, and delete, and instance delete, so
// every engine branch reads the catalog under its lock and a request that
// loses to a delete gets 404.
func TestConcurrentMySQLRequests(t *testing.T) {
	h := newHarness(t)
	providertest.Hammer(t, h.gate, adminToken, 8, 20, func(int) []providertest.Request {
		return []providertest.Request{
			{Method: http.MethodPost, Path: testInstances, Body: mysqlInsertBody(), Want: []int{200, 409}},
			{Method: http.MethodGet, Path: testMySQLPath, Want: []int{200, 404}},
			{Method: http.MethodPost, Path: testMySQLPath + "/databases", Body: map[string]any{keyName: testDatabase, "charset": "utf8mb4"}, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: testMySQLPath + "/databases", Want: []int{200, 404}},
			{Method: http.MethodPost, Path: testMySQLPath + "/users", Body: map[string]any{keyName: "app", "password": "pw"}, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: testMySQLPath + "/users", Want: []int{200, 404}},
			{Method: http.MethodDelete, Path: testMySQLPath + "/users?name=app", Want: []int{200, 404}},
			{Method: http.MethodDelete, Path: testMySQLPath + "/databases/" + testDatabase, Want: []int{200, 404}},
			{Method: http.MethodDelete, Path: testMySQLPath, Want: []int{200, 404}},
		}
	})
}

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
	object(t, flagged[keySettings])["databaseFlags"] = []any{map[string]any{"name": "cloudsql_iam_authentication", "value": "on"}}
	h.expectError(http.MethodPost, testInstances, flagged, http.StatusNotImplemented, "Cloud SQL coverage gap: database flag cloudsql_iam_authentication")
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
		http.StatusNotImplemented, "Cloud SQL coverage gap: passwordPolicy is not modeled")
	h.expectError(http.MethodPost, testPath+"/users", map[string]any{keyName: "app", "dualPasswordType": "DUAL_PASSWORD"},
		http.StatusNotImplemented, "Cloud SQL coverage gap: dualPasswordType is not modeled")
}

// mysqlHandshakeResponse builds a 4.1 HandshakeResponse for user.

// clientFlags are the capabilities of the test handshake responses.

// dialMySQLHandshake reads the greeting at address, answers it for user,

// and returns the greeting and whatever follows. A refusal before the

// greeting returns the ERR payload as the greeting.

// expectForwarded waits for the fake server to receive one packet.

// withSettings is the test insert body with extra settings fields.
func withSettings(extra map[string]any) map[string]any {
	body := insertBody()
	settings, _ := body[keySettings].(map[string]any)
	maps.Copy(settings, extra)
	return body
}

// TestSettingsAreModeledOrRefused covers the settings that stay gaps, each
// naming its field, and the defaults the local server accepts.
func TestSettingsAreModeledOrRefused(t *testing.T) {
	h := newHarness(t)
	gaps := map[string]map[string]any{
		"Cloud SQL coverage gap: settings.activationPolicy NEVER":           {"activationPolicy": "NEVER"},
		"Cloud SQL coverage gap: settings.pricingPlan PACKAGE":              {"pricingPlan": "PACKAGE"},
		"Cloud SQL coverage gap: settings.maintenanceWindow is not modeled": {"maintenanceWindow": map[string]any{"day": 1, "hour": 3}},
		"Cloud SQL coverage gap: settings.ipConfiguration.pscConfig is not modeled": {
			"ipConfiguration": map[string]any{"pscConfig": map[string]any{"pscEnabled": true}}},
	}
	for message, extra := range gaps {
		h.expectError(http.MethodPost, testInstances, withSettings(extra), http.StatusNotImplemented, message)
	}
	cmek := insertBody()
	cmek["diskEncryptionConfiguration"] = map[string]any{"kmsKeyName": "projects/demo-sql/locations/us-central1/keyRings/r/cryptoKeys/k"}
	h.expectError(http.MethodPost, testInstances, cmek, http.StatusNotImplemented, "Cloud SQL coverage gap: diskEncryptionConfiguration is not modeled")
	// GCP drops names outside the schema and then checks the tier first (recorded).
	h.expectError(http.MethodPost, testInstances, map[string]any{keyName: testInstance, "bogus": true}, http.StatusBadRequest, "Missing parameter: Tier.")
	if h.fleet() != nil {
		t.Fatal("a refused insert started a backend")
	}
	// The defaults are accepted and reported as requested.
	h.expect(h.invoke(adminToken, http.MethodPost, testInstances, withSettings(map[string]any{
		"availabilityType": "ZONAL", "activationPolicy": "ALWAYS", "pricingPlan": "PER_USE",
		"backupConfiguration": map[string]any{"enabled": false, "binaryLogEnabled": false},
		"ipConfiguration": map[string]any{"ipv4Enabled": true, "requireSsl": false, "authorizedNetworks": []any{}, "sslMode": "ALLOW_UNENCRYPTED_AND_ENCRYPTED",
			"serverCaMode": serverCaMode, "serverCertificateRotationMode": rotationUnspecified},
	})), http.StatusOK)
	settings := object(t, h.expect(h.invoke(adminToken, http.MethodGet, testPath, nil), http.StatusOK)[keySettings])
	if settings["availabilityType"] != "ZONAL" || settings["activationPolicy"] != "ALWAYS" || settings["pricingPlan"] != "PER_USE" {
		t.Fatalf("settings = %v", settings)
	}
	h.expectError(http.MethodPatch, testPath, settingsBodyOf(map[string]any{"ipConfiguration": map[string]any{"privateNetwork": "projects/p/global/networks/n"}}),
		http.StatusNotImplemented, "Cloud SQL coverage gap: instances.patch field settings.ipConfiguration.privateNetwork")
	h.expectError(http.MethodGet, testPath+"?location=us-central1", nil, http.StatusNotImplemented, "Cloud SQL coverage gap: the location query parameter")
}

// TestListsPage covers instances.list and operations.list paging.
func TestListsPage(t *testing.T) {
	h := newHarness(t)
	for _, name := range []string{"pg-c", "pg-a", "pg-b"} {
		body := insertBody()
		body[keyName] = name
		h.expect(h.invoke(adminToken, http.MethodPost, testInstances, body), http.StatusOK)
	}
	first := h.expect(h.invoke(viewerToken, http.MethodGet, testInstances+"?maxResults=2", nil), http.StatusOK)
	next, _ := first["nextPageToken"].(string)
	if names := listNames(t, first); len(names) != 2 || names[0] != "pg-a" || names[1] != "pg-b" || next == "" || first[keyKind] != nil {
		t.Fatalf("first page = %v", first)
	}
	second := h.expect(h.invoke(viewerToken, http.MethodGet, testInstances+"?maxResults=2&pageToken="+next, nil), http.StatusOK)
	if names := listNames(t, second); len(names) != 1 || names[0] != "pg-c" || second["nextPageToken"] != nil {
		t.Fatalf("second page = %v", second)
	}
	h.expectError(http.MethodGet, testInstances+"?pageToken=bogus", nil, http.StatusBadRequest, "Invalid request: Invalid page token.")
	h.expectError(http.MethodGet, testInstances+"?maxResults=-1", nil, http.StatusBadRequest, `Invalid value at 'max_results' (TYPE_UINT32), "-1"`)
	h.expectError(http.MethodGet, testInstances+"?filter=state:RUNNABLE", nil, http.StatusNotImplemented, "Cloud SQL coverage gap: list filter is not implemented")
}

// TestOperationsPage covers operations.list paging, newest first.
func TestOperationsPage(t *testing.T) {
	h := newHarness(t)
	for _, name := range []string{"pg-c", "pg-a", "pg-b"} {
		body := insertBody()
		body[keyName] = name
		h.expect(h.invoke(adminToken, http.MethodPost, testInstances, body), http.StatusOK)
	}
	operations := h.expect(h.invoke(viewerToken, http.MethodGet, testOperations+"?maxResults=1", nil), http.StatusOK)
	token, _ := operations["nextPageToken"].(string)
	if items := items(t, operations); len(items) != 1 || object(t, items[0])["targetId"] != "pg-b" || token == "" {
		t.Fatalf("operations page = %v", operations)
	}
	rest := h.expect(h.invoke(viewerToken, http.MethodGet, testOperations+"?pageToken="+token, nil), http.StatusOK)
	if items := items(t, rest); len(items) != 2 || object(t, items[1])["targetId"] != "pg-c" {
		t.Fatalf("operations rest = %v", rest)
	}
}

// listNames returns a list response's item names in order.
func listNames(t *testing.T, body map[string]any) []string {
	t.Helper()
	list := items(t, body)
	names := make([]string, 0, len(list))
	for _, item := range list {
		name, _ := object(t, item)[keyName].(string)
		names = append(names, name)
	}
	return names
}

// TestOperationsReportPending covers the operation latency and cancel.
func TestOperationsReportPending(t *testing.T) {
	h := newHarness(t)
	h.provider.latency = 50 * time.Millisecond
	created := h.expect(h.invoke(adminToken, http.MethodPost, testInstances, insertBody()), http.StatusOK)
	name, _ := created[keyName].(string)
	if created["status"] != statusPending || created["endTime"] != nil {
		t.Fatalf("a fresh operation reported done: %v", created)
	}
	h.expect(h.invoke(viewerToken, http.MethodPost, testOperations+"/"+name+"/cancel", nil), http.StatusForbidden)
	h.expectError(http.MethodPost, testOperations+"/missing/cancel", nil, http.StatusBadRequest, "Request contains an invalid argument.")
	h.expect(h.invoke(adminToken, http.MethodPost, testOperations+"/00000000-0000-4000-8000-000000000000/cancel", nil), http.StatusNotFound)
	time.Sleep(60 * time.Millisecond)
	if done := h.expect(h.invoke(viewerToken, http.MethodGet, testOperations+"/"+name, nil), http.StatusOK); done["status"] != statusDone || done["endTime"] == nil {
		t.Fatalf("operation = %v", done)
	}
	h.expectError(http.MethodPost, testOperations+"/"+name+"/cancel", nil, http.StatusBadRequest,
		"Invalid request: You can't cancel operation "+name+" because Cloud SQL doesn't support the cancellation of the CREATE operation.")
}

// TestSlowWorkStillReportsPending covers a machine where creating the server
// takes longer than the latency: the response is still PENDING.
func TestSlowWorkStillReportsPending(t *testing.T) {
	h := newHarness(t)
	h.provider.latency = 50 * time.Millisecond
	var mu sync.Mutex
	now := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
	h.provider.clock = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		now = now.Add(time.Second) // every reading is a second later
		return now
	}
	if created := h.expect(h.invoke(adminToken, http.MethodPost, testInstances, insertBody()), http.StatusOK); created["status"] != statusPending {
		t.Fatalf("slow work reported done: %v", created)
	}
}

// TestConditionsSeeTheInstance grants roles/cloudsql.viewer on one instance
// through a project binding conditioned on resource.name, resource.type, and
// resource.service.
func TestConditionsSeeTheInstance(t *testing.T) {
	h := newHarness(t)
	h.createInstance()
	other := insertBody()
	other[keyName] = "other-pg"
	h.expect(h.invoke(adminToken, http.MethodPost, testInstances, other), http.StatusOK)
	policy, err := h.engine.GetPolicy(provider.ProjectResource(testProject))
	must(t, err)
	policy.Version = 3
	policy.Bindings = append(policy.Bindings, iam.Binding{Role: "roles/cloudsql.viewer", Members: []string{principal("reader")},
		Condition: []byte(`{"title":"one instance","expression":"resource.service == 'sqladmin.googleapis.com' && ` +
			`resource.type == 'sqladmin.googleapis.com/Instance' && resource.name == 'projects/demo-sql/instances/demo-pg'"}`)})
	_, err = h.engine.SetPolicy(provider.ProjectResource(testProject), policy)
	must(t, err)
	h.expect(h.invoke("reader", http.MethodGet, testPath, nil), http.StatusOK)
	h.expect(h.invoke("reader", http.MethodGet, testInstances+"/other-pg", nil), http.StatusForbidden)
	h.expect(h.invoke("reader", http.MethodGet, testInstances, nil), http.StatusForbidden)
}

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
	// flags, backups, and restores record the fleet calls they name.
	flags    map[string][]server.Flag
	backups  map[string][]string
	restores []string
}

type fakeServer struct {
	listener  net.Listener
	socket    string
	done      chan struct{}
	databases []string
	users     []string
	received  chan []byte
	// greeting is the MySQL handshake a MySQL server sends first; nil for PostgreSQL.
	greeting []byte
}

func newFakeFleet() *fakeFleet {
	return &fakeFleet{servers: map[string]*fakeServer{}, flags: map[string][]server.Flag{}, backups: map[string][]string{}}
}

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
	for name, fake := range f.servers {
		fake.close()
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

func (f *fakeFleet) Create(ctx context.Context, spec server.Spec) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	socket := filepath.Join(f.directory, spec.Instance+".sock")
	var listen net.ListenConfig
	listener, err := listen.Listen(ctx, "unix", socket)
	if err != nil {
		return err //nolint:wrapcheck // test double
	}
	fake := &fakeServer{listener: listener, socket: socket, done: make(chan struct{}), databases: []string{"postgres"},
		users: []string{defaultUser}, received: make(chan []byte, 8)}
	if isMySQL(spec.Version) {
		fake.databases, fake.users, fake.greeting = []string{"information_schema", "mysql"}, []string{mysqlRootUser}, fakeGreeting()
	}
	go fake.accept()
	f.servers[spec.Instance] = fake
	f.flags[spec.Instance] = spec.Flags
	return nil
}

// accept captures the first bytes of each forwarded connection.
func (s *fakeServer) accept() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		if s.greeting != nil {
			_, _ = conn.Write(mysqlwire.Packet(0, s.greeting))
		}
		buffer := make([]byte, 4096)
		n, _ := conn.Read(buffer)
		if n > 0 { // a refused MySQL handshake closes the dialed socket without sending
			s.received <- buffer[:n]
		}
		_ = conn.Close()
	}
}

func (s *fakeServer) close() {
	close(s.done)
	_ = s.listener.Close()
}

func (f *fakeFleet) Remove(_ context.Context, instance string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	fake := f.servers[instance]
	if fake == nil {
		return server.ErrServerMissing
	}
	fake.close()
	delete(f.servers, instance)
	return nil
}

func (f *fakeFleet) Exec(_ context.Context, instance, _, sql string) ([][]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fake := f.servers[instance]
	if fake == nil {
		return nil, server.ErrServerMissing
	}
	f.statement = append(f.statement, sql)
	name := ""
	if fields := strings.Fields(sql); len(fields) > 2 {
		name, _, _ = strings.Cut(strings.Trim(fields[2], `"`+"`'"), "'")
	}
	switch {
	case strings.HasPrefix(sql, "CREATE DATABASE "):
		fake.databases = append(fake.databases, name)
	case strings.HasPrefix(sql, "CREATE ROLE "), strings.HasPrefix(sql, "CREATE USER "):
		fake.users = append(fake.users, name)
	case strings.HasPrefix(sql, "SELECT datname"), strings.HasPrefix(sql, "SELECT SCHEMA_NAME"):
		return fake.databaseRows(sql), nil
	case strings.HasPrefix(sql, "SELECT rolname"):
		return rowsOf(fake.users), nil
	case strings.HasPrefix(sql, "DROP DATABASE ") && fake.greeting == nil:
		return nil, &pgwire.Error{Severity: "ERROR", Code: "55006", Message: "database is being accessed by other users"}
	default:
		return fake.mysqlRows(sql)
	}
	return nil, nil
}

// mysqlRows answers the MySQL-only statements the provider issues.
func (s *fakeServer) mysqlRows(sql string) ([][]string, error) {
	switch {
	case strings.HasPrefix(sql, "SELECT User, Host"):
		rows := rowsOf(s.users)
		for i := range rows {
			rows[i] = append(rows[i], mysqlAnyHost)
		}
		return rows, nil
	case strings.HasPrefix(sql, "DROP USER 'missing'"):
		return nil, &mysqlwire.Error{Code: 1396, State: "HY000", Message: "Operation DROP USER failed for 'missing'@'%'"}
	default:
		return nil, nil
	}
}

// fakeGreeting is a minimal MySQL 8 Handshake v10 that advertises TLS.
func fakeGreeting() []byte {
	payload := append([]byte{mysqlwire.ProtocolVersion}, "8.0.46\x00"...)
	payload = append(payload, 1, 0, 0, 0)                                     // thread id
	payload = append(payload, "abcdefgh"...)                                  // scramble part 1
	payload = append(payload, 0, 0xff, 0xff)                                  // filler, capabilities (with CLIENT_SSL)
	payload = append(payload, mysqlwire.CharsetUTF8MB4, 2, 0, 0xff, 0x0f, 21) // charset, status, capabilities, scramble length
	payload = append(payload, make([]byte, 10)...)                            // reserved
	payload = append(payload, "ijklmnopqrst\x00"...)                          // scramble part 2
	return append(payload, mysqlwire.NativePassword+"\x00"...)
}

// databaseRows answers the provider's pg_database query.
func (s *fakeServer) databaseRows(sql string) [][]string {
	var rows [][]string
	for _, name := range s.databases {
		if !strings.Contains(sql, " = ") || strings.Contains(sql, "'"+name+"'") {
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

func (f *fakeFleet) Endpoint(instance string) (string, <-chan struct{}, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fake := f.servers[instance]
	if fake == nil {
		return "", nil, server.ErrServerMissing
	}
	return fake.socket, fake.done, nil
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

func (f *fakeFleet) Configure(_ context.Context, instance string, flags []server.Flag) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.servers[instance] == nil {
		return server.ErrServerMissing
	}
	f.flags[instance] = flags
	return nil
}

func (f *fakeFleet) Backup(_ context.Context, instance, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.servers[instance] == nil {
		return server.ErrServerMissing
	}
	f.backups[instance] = append(f.backups[instance], id)
	return nil
}

func (f *fakeFleet) Restore(_ context.Context, target, source, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.servers[target] == nil || f.servers[source] == nil {
		return server.ErrServerMissing
	}
	if !slices.Contains(f.backups[source], id) {
		return server.ErrBackupMissing
	}
	f.restores = append(f.restores, source+"/"+id+">"+target)
	return nil
}

func (f *fakeFleet) DeleteBackup(_ context.Context, instance, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	index := slices.Index(f.backups[instance], id)
	if index < 0 {
		return server.ErrBackupMissing
	}
	f.backups[instance] = slices.Delete(f.backups[instance], index, index+1)
	return nil
}
