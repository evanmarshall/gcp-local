package firestore

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"slices"
	"strings"
	"testing"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// TestRESTRefusesBeforeTheEmulator refuses a stranger, an unknown body
// field, a viewer's nested create, and a create on a document path.
func TestRESTRefusesBeforeTheEmulator(t *testing.T) {
	h := newHarness(t)
	for _, attempt := range []struct {
		token, path, body string
		code              int
	}{
		{"actor:" + otherProject, testDocsPath + ":runQuery", `{"structuredQuery":{}}`, http.StatusForbidden},
		{actor, testDocsPath + ":runQuery", `{"unknownField":1}`, http.StatusBadRequest},
		{"viewer:" + testProject, testDocsPath + "/books/b1/pages?documentId=p3", `{}`, http.StatusForbidden},
		{actor, testDocsPath + "/books/b1", `{}`, http.StatusBadRequest},
	} {
		if recorder := h.invokeRaw(attempt.token, http.MethodPost, attempt.path, attempt.body); recorder.Code != attempt.code {
			t.Fatalf("%s as %s = %d %s", attempt.path, attempt.token, recorder.Code, recorder.Body.String())
		}
	}
	if len(h.started()) != 0 {
		t.Fatal("a refused request started the backend")
	}
}

// TestRESTQueriesAndSubcollectionsReachTheEmulator forwards allowed queries
// and nested document paths unchanged with only the owner credential.
func TestRESTQueriesAndSubcollectionsReachTheEmulator(t *testing.T) {
	h := newHarness(t)
	viewer := "viewer:" + testProject
	for _, attempt := range []struct{ token, verb, path, body string }{
		{viewer, http.MethodPost, testDocsPath + "/books/b1:runQuery", `{"structuredQuery":{"from":[{"collectionId":"pages"}]}}`},
		{viewer, http.MethodPost, testDocsPath + "/books/b1:listCollectionIds", `{}`},
		{viewer, http.MethodGet, testDocsPath + "/books/b1/pages/p1", ""},
		{viewer, http.MethodGet, testDocsPath + "/books/b1/pages", ""},
		{actor, http.MethodPost, testDocsPath + "/books/b1/pages?documentId=p2", `{}`},
		{actor, http.MethodPost, testDocsPath + ":beginTransaction", `{}`},
	} {
		if recorder := h.invokeRaw(attempt.token, attempt.verb, attempt.path, attempt.body); recorder.Code != http.StatusOK {
			t.Fatalf("%s %s = %d %s", attempt.verb, attempt.path, recorder.Code, recorder.Body.String())
		}
	}
	_, _, requests := h.started()[0].counts()
	paths := make([]string, len(requests))
	for i, request := range requests {
		paths[i] = strings.ReplaceAll(strings.ReplaceAll(request.Path, "%28", "("), "%29", ")")
		if request.Authorization != ownerBearer {
			t.Fatalf("forwarded authorization = %q", request.Authorization)
		}
	}
	want := []string{
		testDocsPath + "/books/b1:runQuery", testDocsPath + "/books/b1:listCollectionIds",
		testDocsPath + "/books/b1/pages/p1", testDocsPath + "/books/b1/pages",
		testDocsPath + "/books/b1/pages", testDocsPath + ":beginTransaction",
	}
	if !slices.Equal(paths, want) {
		t.Fatalf("forwarded paths = %v", paths)
	}
}

// rpcCaller is one authenticated client of the gateway's gRPC server.
type rpcCaller struct {
	client pb.FirestoreClient
	ctx    context.Context //nolint:containedctx // one test caller carries its request context
}

// rpc serves the gateway's gRPC server on loopback for one caller token.
func (h *harness) rpc(token string) rpcCaller {
	h.t.Helper()
	var listen net.ListenConfig
	listener, err := listen.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		h.t.Fatal(err)
	}
	server := h.gate.NewGRPCServer()
	go func() { _ = server.Serve(listener) }()
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { _ = connection.Close(); server.Stop() })
	ctx, cancel := context.WithCancel(context.Background())
	h.t.Cleanup(cancel)
	return rpcCaller{pb.NewFirestoreClient(connection), metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)}
}

// firstQueryResponse opens a query and reads its first response.
func (c rpcCaller) firstQueryResponse(parent string) (*pb.RunQueryResponse, error) {
	query, err := c.client.RunQuery(c.ctx, &pb.RunQueryRequest{Parent: parent})
	if err != nil {
		return nil, fmt.Errorf("open the query: %w", err)
	}
	response, err := query.Recv()
	if err != nil {
		return nil, fmt.Errorf("read the query: %w", err)
	}
	return response, nil
}

// listenSequence opens Listen and, for each request in order, sends it and
// reads one response. It returns the index of the first failing exchange.
func (c rpcCaller) listenSequence(requests ...*pb.ListenRequest) (int, error) {
	listen, err := c.client.Listen(c.ctx)
	if err != nil {
		return 0, fmt.Errorf("open listen: %w", err)
	}
	for i, request := range requests {
		if err := listen.Send(request); err != nil {
			return i, fmt.Errorf("send listen: %w", err)
		}
		if _, err := listen.Recv(); err != nil {
			return i, fmt.Errorf("read listen: %w", err)
		}
	}
	return len(requests), nil
}

// assertOwnerCalls checks the emulator calls and that each carried only the
// owner credential.
func assertOwnerCalls(t *testing.T, instance *fakeEmulator, want []string) {
	t.Helper()
	calls, authorized := instance.rpc.seen()
	if !slices.Equal(calls, want) {
		t.Fatalf("emulator calls = %v", calls)
	}
	for _, value := range authorized {
		if value != ownerBearer {
			t.Fatalf("emulator authorization = %q", value)
		}
	}
}

// TestGRPCDeniesBeforeTheBackend refuses unary, server-streaming, and Listen
// calls before any emulator starts.
func TestGRPCDeniesBeforeTheBackend(t *testing.T) {
	h := newHarness(t)
	stranger, viewer, owner := h.rpc("actor:"+otherProject), h.rpc("viewer:"+testProject), h.rpc(actor)
	if _, err := stranger.client.GetDocument(stranger.ctx, &pb.GetDocumentRequest{Name: testDocument("books/b1")}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("stranger get = %v", err)
	}
	if _, err := stranger.firstQueryResponse(testDatabase + "/documents"); status.Code(errors.Unwrap(err)) != codes.PermissionDenied {
		t.Fatalf("stranger query = %v", err)
	}
	if _, err := stranger.listenSequence(&pb.ListenRequest{Database: testDatabase}); status.Code(errors.Unwrap(err)) != codes.PermissionDenied {
		t.Fatalf("stranger listen = %v", err)
	}
	if _, err := viewer.client.Commit(viewer.ctx, &pb.CommitRequest{Database: testDatabase, Writes: []*pb.Write{update(testDocument("books/b1"), nil)}}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("viewer upsert = %v", err)
	}
	if _, err := owner.client.Commit(owner.ctx, &pb.CommitRequest{Database: testDatabase, Writes: []*pb.Write{update("projects/"+otherProject+"/databases/(default)/documents/books/b1", nil)}}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("cross-project commit = %v", err)
	}
	if len(h.started()) != 0 {
		t.Fatal("a refused gRPC request started the backend")
	}
}

// TestGRPCForwardsAsOwner forwards allowed unary, server-streaming, and
// Listen calls with only the owner credential, and ends a Listen stream whose
// later request leaves the project.
func TestGRPCForwardsAsOwner(t *testing.T) {
	h := newHarness(t)
	viewer, owner := h.rpc("viewer:"+testProject), h.rpc(actor)
	if _, err := owner.client.GetDocument(owner.ctx, &pb.GetDocumentRequest{Name: testDocument("books/b1/pages/p1")}); err != nil {
		t.Fatalf("nested get: %v", err)
	}
	if _, err := owner.client.Commit(owner.ctx, &pb.CommitRequest{Database: testDatabase, Writes: []*pb.Write{update(testDocument("books/b1"), nil)}}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if response, err := viewer.firstQueryResponse(testDocument("books/b1")); err != nil || response.GetDocument().GetName() != testDocument("books/b1/books/one") {
		t.Fatalf("query response = %v %v", response, err)
	}
	// The second Listen request names another project and ends the stream
	// before it is forwarded.
	exchanged, err := viewer.listenSequence(&pb.ListenRequest{Database: testDatabase},
		&pb.ListenRequest{Database: "projects/" + otherProject + "/databases/(default)"})
	if exchanged != 1 || status.Code(errors.Unwrap(err)) != codes.InvalidArgument {
		t.Fatalf("listen = %d exchanges, %v", exchanged, err)
	}
	assertOwnerCalls(t, h.started()[0], []string{"GetDocument", "Commit", "RunQuery", "Listen"})
}
