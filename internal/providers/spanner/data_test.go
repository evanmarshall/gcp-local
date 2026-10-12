package spanner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// bearer attaches one token to every RPC over the plaintext loopback test gateway.
type bearer string

func (b bearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + string(b)}, nil
}
func (bearer) RequireTransportSecurity() bool { return false }

func TestDataPlanFeatures(t *testing.T) {
	readOnly := &spannerpb.TransactionOptions{Mode: &spannerpb.TransactionOptions_ReadOnly_{ReadOnly: &spannerpb.TransactionOptions_ReadOnly{}}}
	readWrite := &spannerpb.TransactionOptions{Mode: &spannerpb.TransactionOptions_ReadWrite_{ReadWrite: &spannerpb.TransactionOptions_ReadWrite{}}}
	pdml := &spannerpb.TransactionOptions{Mode: &spannerpb.TransactionOptions_PartitionedDml_{PartitionedDml: &spannerpb.TransactionOptions_PartitionedDml{}}}
	begin := func(options *spannerpb.TransactionOptions) *spannerpb.TransactionSelector {
		return &spannerpb.TransactionSelector{Selector: &spannerpb.TransactionSelector_Begin{Begin: options}}
	}
	single := func(options *spannerpb.TransactionOptions) *spannerpb.TransactionSelector {
		return &spannerpb.TransactionSelector{Selector: &spannerpb.TransactionSelector_SingleUse{SingleUse: options}}
	}
	none := map[string]bool{featureBeginReadOnly: false, featureBeginReadWrite: false}
	cases := []struct {
		name, method string
		request      proto.Message
		plan         string
		features     map[string]bool
		code         codes.Code
	}{
		{"session create", rpcCreateSession, &spannerpb.CreateSessionRequest{}, rpcCreateSession, nil, codes.OK},
		{"creator role", rpcCreateSession, &spannerpb.CreateSessionRequest{Session: &spannerpb.Session{CreatorRole: "r"}}, "", nil, codes.Unimplemented},
		{"batch creator role", rpcBatchCreateSessions, &spannerpb.BatchCreateSessionsRequest{SessionTemplate: &spannerpb.Session{CreatorRole: "r"}}, "", nil, codes.Unimplemented},
		{"query", rpcExecuteSQL, &spannerpb.ExecuteSqlRequest{}, rpcExecuteSQL, none, codes.OK},
		{"query single-use read", rpcExecuteSQL, &spannerpb.ExecuteSqlRequest{Transaction: single(readOnly)}, rpcExecuteSQL, none, codes.OK},
		{"query single-use write", rpcExecuteSQL, &spannerpb.ExecuteSqlRequest{Transaction: single(readWrite)}, "", nil, codes.Unimplemented},
		{"query begin read", rpcExecuteStreamingSQL, &spannerpb.ExecuteSqlRequest{Transaction: begin(readOnly)}, rpcExecuteStreamingSQL,
			map[string]bool{featureBeginReadOnly: true, featureBeginReadWrite: false}, codes.OK},
		{"dml begin write", rpcExecuteSQL, &spannerpb.ExecuteSqlRequest{Transaction: begin(readWrite)}, rpcExecuteSQL,
			map[string]bool{featureBeginReadOnly: false, featureBeginReadWrite: true}, codes.OK},
		{"inline pdml", rpcExecuteSQL, &spannerpb.ExecuteSqlRequest{Transaction: begin(pdml)}, "", nil, codes.Unimplemented},
		{"data boost", rpcExecuteSQL, &spannerpb.ExecuteSqlRequest{DataBoostEnabled: true}, "", nil, codes.Unimplemented},
		{"read data boost", rpcRead, &spannerpb.ReadRequest{DataBoostEnabled: true}, "", nil, codes.Unimplemented},
		{"batch dml", rpcExecuteBatchDml, &spannerpb.ExecuteBatchDmlRequest{Transaction: begin(readWrite)}, rpcExecuteBatchDml,
			map[string]bool{featureBeginReadOnly: false, featureBeginReadWrite: true}, codes.OK},
		{"begin read-write", rpcBeginTransaction, &spannerpb.BeginTransactionRequest{Options: readWrite}, rpcBeginTransaction, nil, codes.OK},
		{"begin read-only", rpcBeginTransaction, &spannerpb.BeginTransactionRequest{Options: readOnly}, planBeginReadOnly, nil, codes.OK},
		{"begin pdml", rpcBeginTransaction, &spannerpb.BeginTransactionRequest{Options: pdml}, "", nil, codes.Unimplemented},
		{"commit", rpcCommit, &spannerpb.CommitRequest{}, rpcCommit, none, codes.OK},
		{"commit single-use", rpcCommit, &spannerpb.CommitRequest{Transaction: &spannerpb.CommitRequest_SingleUseTransaction{SingleUseTransaction: readWrite}},
			rpcCommit, map[string]bool{featureBeginReadOnly: false, featureBeginReadWrite: true}, codes.OK},
		{"commit single-use read", rpcCommit, &spannerpb.CommitRequest{Transaction: &spannerpb.CommitRequest_SingleUseTransaction{SingleUseTransaction: readOnly}}, "", nil, codes.Unimplemented},
		{"rollback", rpcRollback, &spannerpb.RollbackRequest{}, rpcRollback, nil, codes.OK},
		{"partition read", rpcPartitionRead, &spannerpb.PartitionReadRequest{Transaction: begin(readOnly)}, rpcPartitionRead,
			map[string]bool{featureBeginReadOnly: true, featureBeginReadWrite: false}, codes.OK},
	}
	for _, c := range cases {
		plan, features, err := dataFeatures(c.method, c.request)
		if status.Code(err) != c.code || plan != c.plan || !reflect.DeepEqual(features, c.features) {
			t.Errorf("%s = %q %v %v, want %q %v %v", c.name, plan, features, err, c.plan, c.features, c.code)
		}
	}
	want := map[string]string{
		rpcCreateSession: "spanner.sessions.create", rpcBatchCreateSessions: "spanner.sessions.create",
		rpcGetSession: "spanner.sessions.get", rpcListSessions: "spanner.sessions.list", rpcDeleteSession: "spanner.sessions.delete",
		rpcExecuteSQL: "spanner.databases.select", rpcExecuteStreamingSQL: "spanner.databases.select",
		rpcExecuteBatchDml: permWrite, rpcRead: "spanner.databases.read", rpcStreamingRead: "spanner.databases.read",
		rpcBeginTransaction: permBeginReadWrite, planBeginReadOnly: permBeginReadOnly, rpcCommit: permWrite,
		rpcRollback: permBeginReadWrite, rpcPartitionQuery: "spanner.databases.partitionQuery", rpcPartitionRead: "spanner.databases.partitionRead",
	}
	for method, permission := range want {
		if dataPlans[method].permission != permission {
			t.Errorf("%s plan = %q, want %q", method, dataPlans[method].permission, permission)
		}
	}
	if len(dataPlans) != len(want) {
		t.Errorf("plans = %d, want %d", len(dataPlans), len(want))
	}
}

// serveGRPC starts the harness gateway's gRPC server on loopback.
func (h *harness) serveGRPC() string {
	h.t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	must(h.t, err)
	server := h.gate.NewGRPCServer()
	go func() { _ = server.Serve(listener) }()
	h.t.Cleanup(server.Stop)
	return listener.Addr().String()
}

// dataClient dials the gateway as one token; an empty token is anonymous.
func dataClient(t *testing.T, endpoint, token string) spannerpb.SpannerClient { //nolint:ireturn // the generated client constructor returns this interface
	t.Helper()
	options := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if token != "" {
		options = append(options, grpc.WithPerRPCCredentials(bearer(token)))
	}
	conn, err := grpc.NewClient("passthrough:///"+endpoint, options...)
	must(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return spannerpb.NewSpannerClient(conn)
}

// TestDataPlaneDeniedBeforeBackend proves every data RPC authorizes against the
// session's database before forwarding: denied, anonymous, and unsupported
// calls never reach the emulator, and allowed calls do.
func TestDataPlaneDeniedBeforeBackend(t *testing.T) {
	h := newHarness(t)
	h.createInstance()
	h.expect(h.invoke(adminToken, http.MethodPost, testPath+"/databases", createDatabase(testDatabase)), http.StatusOK)
	policy, err := h.engine.GetPolicy(projectResource(testProject))
	must(t, err)
	policy.Bindings = append(policy.Bindings, iam.Binding{Role: "roles/spanner.viewer", Members: []string{principal(viewerToken)}})
	_, err = h.engine.SetPolicy(projectResource(testProject), policy)
	must(t, err)
	endpoint := h.serveGRPC()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	database := databaseName(testProject, testInstance, testDatabase)
	session := database + "/sessions/fake"
	readOnly := &spannerpb.TransactionOptions{Mode: &spannerpb.TransactionOptions_ReadOnly_{ReadOnly: &spannerpb.TransactionOptions_ReadOnly{}}}
	_, before := h.fake(testProject).counts()
	expectCodes(t, "viewer", everyDataCall(ctx, dataClient(t, endpoint, viewerToken), database), codes.PermissionDenied)
	expectCodes(t, "anonymous", everyDataCall(ctx, dataClient(t, endpoint, ""), database), codes.Unauthenticated)
	admin := dataClient(t, endpoint, adminToken)
	if _, err := admin.ExecuteSql(ctx, &spannerpb.ExecuteSqlRequest{Session: session, DataBoostEnabled: true}); status.Code(err) != codes.Unimplemented {
		t.Errorf("data boost = %v, want Unimplemented", err)
	}
	if _, err := admin.CreateSession(ctx, &spannerpb.CreateSessionRequest{Database: database, Session: &spannerpb.Session{CreatorRole: "reader"}}); status.Code(err) != codes.Unimplemented {
		t.Errorf("creator role = %v, want Unimplemented", err)
	}
	if _, after := h.fake(testProject).counts(); after != before {
		t.Fatalf("refused data calls reached the emulator: %d calls", after-before)
	}
	// The viewer role holds beginReadOnlyTransaction, so a read-only begin is forwarded.
	if _, err := dataClient(t, endpoint, viewerToken).BeginTransaction(ctx, &spannerpb.BeginTransactionRequest{Session: session, Options: readOnly}); err != nil {
		t.Fatalf("viewer read-only begin: %v", err)
	}
	allowedCalls(ctx, t, admin, database)
	if _, after := h.fake(testProject).counts(); after != before+4 {
		t.Fatalf("allowed data calls = %d, want 4", after-before)
	}
	// A peer project's grant does not reach this project's database.
	if _, err := dataClient(t, endpoint, peerToken).CreateSession(ctx, &spannerpb.CreateSessionRequest{Database: database}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("peer CreateSession = %v, want PermissionDenied", err)
	}
}

// everyDataCall issues each data RPC once against one database's fake session.
func everyDataCall(ctx context.Context, client spannerpb.SpannerClient, database string) map[string]error {
	session := database + "/sessions/fake"
	readWrite := &spannerpb.TransactionOptions{Mode: &spannerpb.TransactionOptions_ReadWrite_{ReadWrite: &spannerpb.TransactionOptions_ReadWrite{}}}
	results := map[string]error{}
	_, results["CreateSession"] = client.CreateSession(ctx, &spannerpb.CreateSessionRequest{Database: database})
	_, results["BatchCreateSessions"] = client.BatchCreateSessions(ctx, &spannerpb.BatchCreateSessionsRequest{Database: database, SessionCount: 1})
	_, results["GetSession"] = client.GetSession(ctx, &spannerpb.GetSessionRequest{Name: session})
	_, results["ListSessions"] = client.ListSessions(ctx, &spannerpb.ListSessionsRequest{Database: database})
	_, results["DeleteSession"] = client.DeleteSession(ctx, &spannerpb.DeleteSessionRequest{Name: session})
	_, results["ExecuteSql"] = client.ExecuteSql(ctx, &spannerpb.ExecuteSqlRequest{Session: session, Sql: selectOne})
	_, results["ExecuteBatchDml"] = client.ExecuteBatchDml(ctx, &spannerpb.ExecuteBatchDmlRequest{Session: session})
	_, results["Read"] = client.Read(ctx, &spannerpb.ReadRequest{Session: session, Table: "Items"})
	_, results["BeginTransaction"] = client.BeginTransaction(ctx, &spannerpb.BeginTransactionRequest{Session: session, Options: readWrite})
	_, results["Commit"] = client.Commit(ctx, &spannerpb.CommitRequest{Session: session})
	_, results["Rollback"] = client.Rollback(ctx, &spannerpb.RollbackRequest{Session: session})
	_, results["PartitionQuery"] = client.PartitionQuery(ctx, &spannerpb.PartitionQueryRequest{Session: session})
	_, results["PartitionRead"] = client.PartitionRead(ctx, &spannerpb.PartitionReadRequest{Session: session})
	results["ExecuteStreamingSql"] = drain(client.ExecuteStreamingSql(ctx, &spannerpb.ExecuteSqlRequest{Session: session, Sql: selectOne}))
	results["StreamingRead"] = drain(client.StreamingRead(ctx, &spannerpb.ReadRequest{Session: session, Table: "Items"}))
	return results
}

// allowedCalls creates a session, queries, and commits as an admin.
func allowedCalls(ctx context.Context, t *testing.T, admin spannerpb.SpannerClient, database string) {
	t.Helper()
	session := database + "/sessions/fake"
	created, err := admin.CreateSession(ctx, &spannerpb.CreateSessionRequest{Database: database})
	if err != nil || created.GetName() != session {
		t.Fatalf("admin CreateSession = %v, %v", created, err)
	}
	if _, err := admin.ExecuteSql(ctx, &spannerpb.ExecuteSqlRequest{Session: session, Sql: selectOne}); err != nil {
		t.Fatalf("admin ExecuteSql: %v", err)
	}
	commit := &spannerpb.CommitRequest{Session: session, Transaction: &spannerpb.CommitRequest_TransactionId{TransactionId: []byte("tx")}}
	if _, err := admin.Commit(ctx, commit); err != nil {
		t.Fatalf("admin Commit: %v", err)
	}
}

// expectCodes asserts every call ended with one status code.
func expectCodes(t *testing.T, label string, results map[string]error, code codes.Code) {
	t.Helper()
	for method, err := range results {
		if status.Code(err) != code {
			t.Errorf("%s %s = %v, want %v", label, method, err, code)
		}
	}
}

// selectOne is the trivial query the unit tests forward.
const selectOne = "SELECT 1"

// receiver is one server-streaming client.
type receiver[T any] interface{ Recv() (T, error) }

// drain reads a server stream to its end and returns its status.
func drain[T any](stream receiver[T], err error) error {
	if err != nil {
		return err
	}
	for {
		if _, err := stream.Recv(); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("receive: %w", err)
		}
	}
}

// TestRESTSessionsShareTheDataPlan proves the unary REST session routes use
// the same plans: a viewer is denied before the backend, and an admin's calls
// are transcoded and forwarded.
func TestRESTSessionsShareTheDataPlan(t *testing.T) {
	h := newHarness(t)
	h.createInstance()
	h.expect(h.invoke(adminToken, http.MethodPost, testPath+"/databases", createDatabase(testDatabase)), http.StatusOK)
	sessions := testPath + "/databases/" + testDatabase + "/sessions"
	_, before := h.fake(testProject).counts()
	denied := h.expect(h.invoke(peerToken, http.MethodPost, sessions, map[string]any{}), http.StatusForbidden)
	if object(t, denied["error"])["status"] != "PERMISSION_DENIED" {
		t.Fatalf("denied body = %v", denied)
	}
	h.expect(h.invoke(adminToken, http.MethodPost, sessions+"/fake:executeSql", map[string]any{"sql": selectOne, "dataBoostEnabled": true}), http.StatusNotImplemented)
	if _, after := h.fake(testProject).counts(); after != before {
		t.Fatal("a refused REST session call reached the backend")
	}
	created := h.expect(h.invoke(adminToken, http.MethodPost, sessions, map[string]any{"session": map[string]any{"labels": map[string]string{"a": "b"}}}), http.StatusOK)
	if created["name"] != databaseName(testProject, testInstance, testDatabase)+"/sessions/fake" {
		t.Fatalf("created = %v", created)
	}
	h.expect(h.invoke(adminToken, http.MethodPost, sessions+"/fake:executeSql", map[string]any{"sql": selectOne}), http.StatusOK)
	fake := h.fake(testProject)
	fake.mu.Lock()
	last, ok := fake.invoked[len(fake.invoked)-1].(*spannerpb.ExecuteSqlRequest)
	fake.mu.Unlock()
	if !ok || last.GetSession() != databaseName(testProject, testInstance, testDatabase)+"/sessions/fake" || last.GetSql() != selectOne {
		t.Fatalf("forwarded = %v", last)
	}
}
