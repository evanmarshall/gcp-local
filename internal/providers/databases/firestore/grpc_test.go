package firestore

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// fakeRPC is the fake emulator's gRPC side: it records the method and the
// authorization metadata of every call that reaches it.
type fakeRPC struct {
	pb.UnimplementedFirestoreServer
	mu         sync.Mutex
	calls      []string
	authorized []string
	commits    []*pb.CommitRequest
	server     *grpc.Server
	connection *grpc.ClientConn
}

func (f *fakeRPC) record(ctx context.Context, method string) {
	md, _ := metadata.FromIncomingContext(ctx)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, method)
	f.authorized = append(f.authorized, strings.Join(md.Get("authorization"), ","))
}

func (f *fakeRPC) seen() ([]string, []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...), append([]string(nil), f.authorized...)
}

func (f *fakeRPC) GetDocument(ctx context.Context, r *pb.GetDocumentRequest) (*pb.Document, error) {
	f.record(ctx, "GetDocument")
	return &pb.Document{Name: r.GetName()}, nil
}

func (f *fakeRPC) Commit(ctx context.Context, r *pb.CommitRequest) (*pb.CommitResponse, error) {
	f.record(ctx, "Commit")
	f.mu.Lock()
	f.commits = append(f.commits, proto.CloneOf(r))
	f.mu.Unlock()
	return &pb.CommitResponse{}, nil
}

func (f *fakeRPC) RunQuery(r *pb.RunQueryRequest, stream pb.Firestore_RunQueryServer) error {
	f.record(stream.Context(), "RunQuery")
	if err := stream.Send(&pb.RunQueryResponse{Document: &pb.Document{Name: r.GetParent() + "/books/one"}}); err != nil {
		return fmt.Errorf("send the fake query response: %w", err)
	}
	return nil
}

func (f *fakeRPC) Listen(stream pb.Firestore_ListenServer) error {
	f.record(stream.Context(), "Listen")
	for {
		if _, err := stream.Recv(); err != nil {
			return nil //nolint:nilerr // a closed client ends the fake stream
		}
		if err := stream.Send(&pb.ListenResponse{ResponseType: &pb.ListenResponse_TargetChange{TargetChange: &pb.TargetChange{}}}); err != nil {
			return fmt.Errorf("send the fake listen response: %w", err)
		}
	}
}

func (f *fakeEmulator) startGRPC(ctx context.Context) error {
	var listen net.ListenConfig
	listener, err := listen.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("listen for the fake gRPC emulator: %w", err)
	}
	f.rpc = &fakeRPC{server: grpc.NewServer()}
	pb.RegisterFirestoreServer(f.rpc.server, f.rpc)
	go func() { _ = f.rpc.server.Serve(listener) }()
	if f.rpc.connection, err = grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials())); err != nil {
		return fmt.Errorf("connect to the fake gRPC emulator: %w", err)
	}
	return nil
}

func (f *fakeEmulator) stopGRPC() {
	if f.rpc != nil {
		_ = f.rpc.connection.Close()
		f.rpc.server.Stop()
	}
}

func (f *fakeEmulator) client() grpc.ClientConnInterface {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rpc.connection
}

const testDatabase = "projects/" + testProject + "/databases/(default)"

func testDocument(path string) string { return testDatabase + "/documents/" + path }

func update(name string, precondition *pb.Precondition) *pb.Write {
	return &pb.Write{Operation: &pb.Write_Update{Update: &pb.Document{Name: name}}, CurrentDocument: precondition}
}

// TestRequestPlansFollowTheDocumentedTable pins the per-method plans and the
// commit write-kind mapping from Google's Firestore IAM table.
func TestRequestPlansFollowTheDocumentedTable(t *testing.T) {
	exists := func(value bool) *pb.Precondition {
		return &pb.Precondition{ConditionType: &pb.Precondition_Exists{Exists: value}}
	}
	cases := []struct {
		name    string
		request proto.Message
		want    []string
	}{
		{"get nested", &pb.GetDocumentRequest{Name: testDocument("a/b/c/d")}, []string{methodDocumentsGet}},
		{"list", &pb.ListDocumentsRequest{Parent: testDatabase + "/documents", CollectionId: "a"}, []string{planDocumentsList}},
		{"create in subcollection", &pb.CreateDocumentRequest{Parent: testDocument("a/b"), CollectionId: "c"}, []string{methodDocumentsCreate}},
		{"batch get", &pb.BatchGetDocumentsRequest{Database: testDatabase, Documents: []string{testDocument("a/b")}}, []string{methodBatchGet}},
		{"batch get in new transaction", &pb.BatchGetDocumentsRequest{Database: testDatabase, ConsistencySelector: &pb.BatchGetDocumentsRequest_NewTransaction{NewTransaction: &pb.TransactionOptions{}}}, []string{methodBatchGet, methodBeginTransaction}},
		{"begin", &pb.BeginTransactionRequest{Database: testDatabase}, []string{methodBeginTransaction}},
		{"rollback", &pb.RollbackRequest{Database: testDatabase, Transaction: []byte("t")}, []string{methodRollback}},
		{"query root", &pb.RunQueryRequest{Parent: testDatabase + "/documents"}, []string{methodRunQuery}},
		{"query subcollection", &pb.RunQueryRequest{Parent: testDocument("a/b")}, []string{methodRunQuery}},
		{"aggregate", &pb.RunAggregationQueryRequest{Parent: testDatabase + "/documents"}, []string{methodRunAggregation}},
		{"collection ids", &pb.ListCollectionIdsRequest{Parent: testDocument("a/b")}, []string{methodListCollectionIDs}},
		{"listen", &pb.ListenRequest{Database: testDatabase, TargetChange: &pb.ListenRequest_AddTarget{AddTarget: &pb.Target{TargetType: &pb.Target_Query{Query: &pb.Target_QueryTarget{Parent: testDatabase + "/documents"}}}}}, []string{planListen}},
		{"empty commit", &pb.CommitRequest{Database: testDatabase, Transaction: []byte("t")}, []string{planWriteNone}},
		{"commit kinds", &pb.CommitRequest{Database: testDatabase, Writes: []*pb.Write{
			update(testDocument("a/1"), exists(false)),
			update(testDocument("a/2"), exists(true)),
			update(testDocument("a/3"), nil),
			update(testDocument("a/4"), &pb.Precondition{ConditionType: &pb.Precondition_UpdateTime{}}),
			{Operation: &pb.Write_Delete{Delete: testDocument("a/5")}},
			{Operation: &pb.Write_Transform{Transform: &pb.DocumentTransform{Document: testDocument("a/6")}}},
		}}, []string{planWriteCreate, planWriteUpdate, planWriteUpsert, planWriteDelete}},
		{"batch write", &pb.BatchWriteRequest{Database: testDatabase, Writes: []*pb.Write{{Operation: &pb.Write_Delete{Delete: testDocument("a/5")}}}}, []string{planWriteDelete}},
	}
	for _, c := range cases {
		got, err := requestPlans(testProject, c.request)
		if err != nil || !slices.Equal(got, c.want) {
			t.Errorf("%s: plans = %v, %v; want %v", c.name, got, err, c.want)
		}
	}
	want := map[string][]string{
		methodDocumentsGet:      {permissionEntitiesGet},
		planDocumentsList:       {permissionEntitiesGet, permissionEntitiesList},
		methodRunQuery:          {permissionEntitiesGet, permissionEntitiesList},
		methodRunAggregation:    {permissionEntitiesGet, permissionEntitiesList},
		methodListCollectionIDs: {permissionEntitiesList},
		methodBatchGet:          {permissionEntitiesGet},
		methodBeginTransaction:  {permissionDatabasesGet},
		methodRollback:          {permissionDatabasesGet},
		planWriteCreate:         {permissionEntitiesCreate},
		planWriteUpdate:         {permissionEntitiesUpdate},
		planWriteUpsert:         {permissionEntitiesCreate, permissionEntitiesUpdate},
		planWriteDelete:         {"datastore.entities.delete"},
	}
	for plan, permissions := range want {
		if !slices.Equal(plans[plan], permissions) {
			t.Errorf("plan %s = %v, want %v", plan, plans[plan], permissions)
		}
	}
}

// TestRequestPlansRejectNamesOutsideTheDatabase keeps every name inside the
// request project's (default) database.
func TestRequestPlansRejectNamesOutsideTheDatabase(t *testing.T) {
	other := "projects/" + otherProject + "/databases/(default)/documents/a/b"
	cases := []struct {
		name    string
		request proto.Message
		code    codes.Code
	}{
		{"cross-project batch get", &pb.BatchGetDocumentsRequest{Database: testDatabase, Documents: []string{other}}, codes.InvalidArgument},
		{"cross-project commit", &pb.CommitRequest{Database: testDatabase, Writes: []*pb.Write{update(other, nil)}}, codes.InvalidArgument},
		{"cross-project listen", &pb.ListenRequest{Database: testDatabase, TargetChange: &pb.ListenRequest_AddTarget{AddTarget: &pb.Target{TargetType: &pb.Target_Documents{Documents: &pb.Target_DocumentsTarget{Documents: []string{other}}}}}}, codes.InvalidArgument},
		{"collection as document", &pb.GetDocumentRequest{Name: testDocument("a")}, codes.InvalidArgument},
		{"document as parent", &pb.RunQueryRequest{Parent: testDocument("a")}, codes.InvalidArgument},
		{"dot segment", &pb.GetDocumentRequest{Name: testDocument("a/..")}, codes.InvalidArgument},
		{"empty write", &pb.CommitRequest{Database: testDatabase, Writes: []*pb.Write{{}}}, codes.InvalidArgument},
		{"named database", &pb.BeginTransactionRequest{Database: "projects/" + testProject + "/databases/other"}, codes.Unimplemented},
		{"named database document", &pb.GetDocumentRequest{Name: "projects/" + testProject + "/databases/other/documents/a/b"}, codes.Unimplemented},
	}
	for _, c := range cases {
		if _, err := requestPlans(testProject, c.request); status.Code(err) != c.code {
			t.Errorf("%s: %v, want %s", c.name, err, c.code)
		}
	}
}

// TestCommitDeniesEachWriteKindBeforeTheBackend shows that a reader holding
// only get/list is refused for each write kind, naming that kind's permission.
func TestCommitDeniesEachWriteKindBeforeTheBackend(t *testing.T) {
	h := newHarness(t)
	viewer := "viewer:" + testProject
	name := testDocument(testCollection + "/" + testDoc)
	for write, permission := range map[string]string{
		`{"update":{"name":"` + name + `"},"currentDocument":{"exists":false}}`: permissionEntitiesCreate,
		`{"update":{"name":"` + name + `"},"currentDocument":{"exists":true}}`:  permissionEntitiesUpdate,
		`{"delete":"` + name + `"}`: "datastore.entities.delete",
	} {
		recorder := h.invokeRaw(viewer, http.MethodPost, testDocsPath+":commit", `{"writes":[`+write+`]}`)
		// GCP's denial names no permission (firestore-document-lifecycle).
		if recorder.Code != http.StatusForbidden || !strings.Contains(recorder.Body.String(), "Missing or insufficient permissions.") {
			t.Fatalf("commit %s = %d %s", write, recorder.Code, recorder.Body.String())
		}
		request := &pb.CommitRequest{}
		if err := protojson.Unmarshal([]byte(`{"writes":[`+write+`]}`), request); err != nil {
			t.Fatal(err)
		}
		request.Database = "projects/" + testProject + "/databases/(default)"
		planned, err := requestPlans(testProject, request)
		if err != nil || len(planned) != 1 || !slices.Contains(plans[planned[0]], permission) {
			t.Fatalf("commit %s plans %v (%v), want %s", write, planned, err, permission)
		}
	}
	if recorder := h.invokeRaw(viewer, http.MethodPost, testDocsPath+":batchWrite", `{"writes":[{"delete":"`+name+`"}]}`); recorder.Code != http.StatusForbidden {
		t.Fatalf("batch write = %d %s", recorder.Code, recorder.Body.String())
	}
	if len(h.started()) != 0 {
		t.Fatal("a denied commit started the backend")
	}
}
