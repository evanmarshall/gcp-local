//go:build spannerintegration

package spanner

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TestOfficialEmulatorLifecycle drives the real pinned emulator: the
// recorded admin flow, project isolation, and reset clearing emulator state.
func TestOfficialEmulatorLifecycle(t *testing.T) {
	interfaces, err := net.Interfaces()
	must(t, err)
	if os.Getenv("GCP_LOCAL_ISOLATED") != "1" || len(interfaces) != 1 || interfaces[0].Name != "lo" {
		t.Fatal("Run actual Spanner tests in the loopback-only runner")
	}
	h := &harness{t: t, index: state.NewIndex()}
	h.engine, err = iam.New(h.index)
	must(t, err)
	for project, token := range map[string]string{testProject: adminToken, peerProject: peerToken} {
		must(t, h.index.Put(ownerProjects, state.Resource{Name: provider.ProjectResource(project), Service: ownerProjects,
			Kind: kindProject, Number: "123456", Scope: provider.ProjectScope(project)}))
		_, err := h.engine.SetPolicy(provider.ProjectResource(project), iam.Policy{Bindings: []iam.Binding{
			{Role: "roles/spanner.admin", Members: []string{principal(token)}}}})
		must(t, err)
	}
	h.manager = backend.NewManager(30*time.Second, 10*time.Second)
	t.Cleanup(func() { must(t, h.manager.Shutdown(context.Background())) })
	registry := gateway.NewRegistry()
	config := Config{Emulator: os.Getenv("GCP_LOCAL_SPANNER_EMULATOR"), Libraries: os.Getenv("GCP_LOCAL_SPANNER_LIBRARIES")}
	_, err = New(context.Background(), registry, h.manager, h.index, h.engine, config)
	must(t, err)
	h.gate = gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: principal(token),
			Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})

	h.expect(h.invoke(peerToken, http.MethodPost, testInstances, instanceBody(testProject)), http.StatusForbidden)
	if status := h.manager.Status(); len(status) != 0 {
		t.Fatalf("a denied request started an emulator: %+v", status)
	}
	h.createInstance()
	h.expect(h.invoke(adminToken, http.MethodPost, testPath+"/databases", map[string]any{
		"createStatement": "CREATE DATABASE `" + testDatabase + "`",
		"extraStatements": []string{"CREATE TABLE Items (Id STRING(36) NOT NULL, Name STRING(64)) PRIMARY KEY (Id)"},
	}), http.StatusOK)
	ddl := h.expect(h.invoke(adminToken, http.MethodGet, testPath+"/databases/"+testDatabase+"/ddl", nil), http.StatusOK)
	if statements, _ := ddl["statements"].([]any); len(statements) != 1 || statements[0] != "CREATE TABLE Items (\n  Id STRING(36) NOT NULL,\n  Name STRING(64),\n) PRIMARY KEY(Id)" {
		t.Fatalf("ddl = %v", ddl)
	}
	peerInstances := "/v1/projects/" + peerProject + "/instances"
	h.expect(h.invoke(peerToken, http.MethodGet, peerInstances+"/"+testInstance, nil), http.StatusNotFound)
	h.expect(h.invoke(peerToken, http.MethodPost, peerInstances, instanceBody(peerProject)), http.StatusOK)

	must(t, h.manager.Reset(context.Background(), backend.Key{Service: serviceName, Scope: provider.ProjectScope(testProject)}))
	h.expect(h.invoke(adminToken, http.MethodGet, testPath, nil), http.StatusNotFound)
	h.expect(h.invoke(peerToken, http.MethodGet, peerInstances+"/"+testInstance, nil), http.StatusOK)
	h.createInstance()
	h.expect(h.invoke(adminToken, http.MethodGet, testPath+"/databases/"+testDatabase, nil), http.StatusNotFound)
}

// Local principals of the data-plane integration test.
var (
	dataAdmin  = "serviceAccount:data-admin@" + testProject + ".iam.gserviceaccount.com"
	dataViewer = "serviceAccount:data-viewer@" + testProject + ".iam.gserviceaccount.com"
)

// TestOfficialEmulatorDataPlane drives the official Go client through the
// authenticated gRPC gateway against the real emulator: mutations, DML, batch
// DML, point reads, streaming queries, read-only transactions, partitioned
// queries, rollback, cancellation, and denial before the backend.
func TestOfficialEmulatorDataPlane(t *testing.T) {
	interfaces, err := net.Interfaces()
	must(t, err)
	if os.Getenv("GCP_LOCAL_ISOLATED") != "1" || len(interfaces) != 1 || interfaces[0].Name != "lo" {
		t.Fatal("Run actual Spanner tests in the loopback-only runner")
	}
	h := &harness{t: t, index: state.NewIndex()}
	h.engine, err = iam.New(h.index)
	must(t, err)
	must(t, h.index.Put(ownerProjects, state.Resource{Name: provider.ProjectResource(testProject), Service: ownerProjects,
		Kind: kindProject, Number: "123456", Scope: provider.ProjectScope(testProject)}))
	_, err = h.engine.SetPolicy(provider.ProjectResource(testProject), iam.Policy{Bindings: []iam.Binding{
		{Role: "roles/spanner.databaseAdmin", Members: []string{dataAdmin}},
		{Role: "roles/spanner.admin", Members: []string{dataAdmin}},
		{Role: "roles/spanner.viewer", Members: []string{dataViewer}}}})
	must(t, err)
	h.manager = backend.NewManager(30*time.Second, 10*time.Second)
	t.Cleanup(func() { must(t, h.manager.Shutdown(context.Background())) })
	registry := gateway.NewRegistry()
	config := Config{Emulator: os.Getenv("GCP_LOCAL_SPANNER_EMULATOR"), Libraries: os.Getenv("GCP_LOCAL_SPANNER_LIBRARIES")}
	_, err = New(context.Background(), registry, h.manager, h.index, h.engine, config)
	must(t, err)
	enrollments := []identity.Enrollment{
		{Subject: dataAdmin, Project: testProject, SubjectToken: "gls.synthetic-spanner-data-admin-subject-token"},
		{Subject: dataViewer, Project: testProject, SubjectToken: "gls.synthetic-spanner-data-viewer-subject-token"},
	}
	identities, err := identity.New(identity.Config{Principals: enrollments}, nil)
	must(t, err)
	h.gate = gateway.New(registry, identities.Resolve)
	admin, viewer := exchangeToken(t, identities, enrollments[0].SubjectToken), exchangeToken(t, identities, enrollments[1].SubjectToken)
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	must(t, err)
	server := h.gate.NewGRPCServer()
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	endpoint := listener.Addr().String()

	// A denied data call neither starts nor reaches the emulator.
	database := databaseName(testProject, testInstance, testDatabase)
	raw := spannerpb.NewSpannerClient(dial(t, endpoint, viewer))
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if _, err := raw.CreateSession(ctx, &spannerpb.CreateSessionRequest{Database: database}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("viewer CreateSession = %v", err)
	}
	if _, err := spannerpb.NewSpannerClient(dial(t, endpoint, "")).CreateSession(ctx, &spannerpb.CreateSessionRequest{Database: database}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("anonymous CreateSession = %v", err)
	}
	if running := h.manager.Status(); len(running) != 0 {
		t.Fatalf("a denied data call started the emulator: %+v", running)
	}

	h.expect(h.invoke(admin, http.MethodPost, testInstances, instanceBody(testProject)), http.StatusOK)
	h.expect(h.invoke(admin, http.MethodPost, testPath+"/databases", map[string]any{
		"createStatement": "CREATE DATABASE `" + testDatabase + "`",
		"extraStatements": []string{"CREATE TABLE Items (Id STRING(36) NOT NULL, Name STRING(64)) PRIMARY KEY (Id)"},
	}), http.StatusOK)

	client := spannerClient(ctx, t, endpoint, admin, database)
	if _, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.InsertOrUpdate("Items", []string{"Id", "Name"}, []any{"a", "alpha"}),
		spanner.InsertOrUpdate("Items", []string{"Id", "Name"}, []any{"b", "beta"}),
	}); err != nil {
		t.Fatalf("Commit mutations: %v", err)
	}
	if _, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		if count, err := txn.Update(ctx, spanner.Statement{SQL: "INSERT INTO Items (Id, Name) VALUES ('c', 'gamma')"}); err != nil || count != 1 {
			return errors.Join(err, errors.New("unexpected DML count"))
		}
		counts, err := txn.BatchUpdate(ctx, []spanner.Statement{
			{SQL: "UPDATE Items SET Name = 'ALPHA' WHERE Id = 'a'"},
			{SQL: "DELETE FROM Items WHERE Id = 'b'"},
		})
		if err != nil || len(counts) != 2 {
			return errors.Join(err, errors.New("unexpected batch DML counts"))
		}
		return nil
	}); err != nil {
		t.Fatalf("read-write transaction: %v", err)
	}
	row, err := client.Single().ReadRow(ctx, "Items", spanner.Key{"a"}, []string{"Name"})
	must(t, err)
	var name string
	must(t, row.Columns(&name))
	if name != "ALPHA" {
		t.Fatalf("ReadRow name = %q", name)
	}
	if got := query(ctx, t, client.Single(), "SELECT Id FROM Items ORDER BY Id"); got != "a,c" {
		t.Fatalf("query = %q", got)
	}
	snapshot := client.ReadOnlyTransaction()
	if got := query(ctx, t, snapshot, "SELECT Id FROM Items WHERE Id = 'c'"); got != "c" {
		t.Fatalf("read-only query = %q", got)
	}
	snapshot.Close()

	// A transaction whose function fails is rolled back.
	rollback := errors.New("roll back")
	if _, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		if _, err := txn.Update(ctx, spanner.Statement{SQL: "DELETE FROM Items WHERE Id = 'a'"}); err != nil {
			return err
		}
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatalf("rolled-back transaction = %v", err)
	}
	if got := query(ctx, t, client.Single(), "SELECT Id FROM Items ORDER BY Id"); got != "a,c" {
		t.Fatalf("query after rollback = %q", got)
	}

	batch, err := client.BatchReadOnlyTransaction(ctx, spanner.StrongRead())
	must(t, err)
	partitions, err := batch.PartitionQuery(ctx, spanner.Statement{SQL: "SELECT Id FROM Items"}, spanner.PartitionOptions{})
	must(t, err)
	rows := 0
	for _, partition := range partitions {
		must(t, batch.Execute(ctx, partition).Do(func(*spanner.Row) error { rows++; return nil }))
	}
	if rows != 2 {
		t.Fatalf("partitioned query rows = %d", rows)
	}
	batch.Close()

	// Cancelling a streaming query ends it without wedging the scope.
	cancelled, stop := context.WithCancel(ctx)
	stream := client.Single().Query(cancelled, spanner.Statement{SQL: "SELECT Id FROM Items ORDER BY Id"})
	stop()
	if _, err := stream.Next(); spanner.ErrCode(err) != codes.Canceled {
		t.Fatalf("cancelled query = %v", err)
	}
	stream.Stop()
	if got := query(ctx, t, client.Single(), "SELECT COUNT(*) AS n FROM Items"); got != "2" {
		t.Fatalf("count after cancel = %q", got)
	}

	// A viewer holds no data permission: reads and writes are denied.
	denied := spannerClient(ctx, t, endpoint, viewer, database)
	if _, err := denied.Single().ReadRow(ctx, "Items", spanner.Key{"a"}, []string{"Name"}); spanner.ErrCode(err) != codes.PermissionDenied {
		t.Fatalf("viewer read = %v", err)
	}
	if _, err := denied.Apply(ctx, []*spanner.Mutation{spanner.Delete("Items", spanner.Key{"a"})}); spanner.ErrCode(err) != codes.PermissionDenied {
		t.Fatalf("viewer write = %v", err)
	}
	if got := query(ctx, t, client.Single(), "SELECT Id FROM Items ORDER BY Id"); got != "a,c" {
		t.Fatalf("rows after denied write = %q", got)
	}
}

// exchangeToken performs the local ADC token exchange for one subject token.
func exchangeToken(t *testing.T, identities *identity.Service, subject string) string {
	t.Helper()
	form := url.Values{"grant_type": {"urn:ietf:params:oauth:grant-type:token-exchange"}, "audience": {identity.Audience},
		"subject_token_type": {identity.AccessTokenType}, "requested_token_type": {identity.AccessTokenType},
		"subject_token": {subject}, "scope": {identity.CloudPlatform}}
	request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/token", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	recorder := httptest.NewRecorder()
	identities.ServeHTTP(recorder, request)
	var token struct {
		AccessToken string `json:"access_token"`
	}
	if recorder.Code != http.StatusOK || json.Unmarshal(recorder.Body.Bytes(), &token) != nil {
		t.Fatalf("local token exchange = %d", recorder.Code)
	}
	return token.AccessToken
}

// dial connects to the gateway with one local access token; empty is anonymous.
func dial(t *testing.T, endpoint, token string) *grpc.ClientConn {
	t.Helper()
	options := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if token != "" {
		options = append(options, grpc.WithPerRPCCredentials(bearer(token)))
	}
	conn, err := grpc.NewClient("passthrough:///"+endpoint, options...)
	must(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// spannerClient opens the official Go client over the gateway as one token.
func spannerClient(ctx context.Context, t *testing.T, endpoint, token, database string) *spanner.Client {
	t.Helper()
	client, err := spanner.NewClientWithConfig(ctx, database, spanner.ClientConfig{DisableNativeMetrics: true},
		option.WithGRPCConn(dial(t, endpoint, token)))
	must(t, err)
	t.Cleanup(client.Close)
	return client
}

// queryer runs one query in a single-use or read-only transaction.
type queryer interface {
	Query(context.Context, spanner.Statement) *spanner.RowIterator
}

// query joins the first column of every result row.
func query(ctx context.Context, t *testing.T, source queryer, sql string) string {
	t.Helper()
	iter := source.Query(ctx, spanner.Statement{SQL: sql})
	defer iter.Stop()
	var values []string
	for {
		row, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			return strings.Join(values, ",")
		}
		must(t, err)
		var value spanner.GenericColumnValue
		must(t, row.Column(0, &value))
		values = append(values, value.Value.GetStringValue()) // STRING and INT64 both encode as strings
	}
}
