//go:build firestoreintegration

package firestore

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// realHarness drives the provider against the pinned official emulator.
type realHarness struct {
	t       *testing.T
	gate    *gateway.Gateway
	manager *backend.Manager
}

func newRealHarness(t *testing.T) *realHarness {
	t.Helper()
	config := Config{Java: os.Getenv("GCP_LOCAL_JAVA"), JAR: os.Getenv("GCP_LOCAL_FIRESTORE_JAR")}
	if err := config.validate(); err != nil {
		t.Fatalf("set GCP_LOCAL_JAVA and GCP_LOCAL_FIRESTORE_JAR to the pinned runtime: %v", err)
	}
	registry, index := gateway.NewRegistry(), state.NewIndex()
	engine, err := iam.New(index)
	if err != nil {
		t.Fatal(err)
	}
	for _, project := range []string{testProject, otherProject} {
		if err := index.Put("resourcemanager", state.Resource{Name: provider.ProjectResource(project), Service: "resourcemanager", Kind: "project", Number: "123456", Scope: provider.ProjectScope(project)}); err != nil {
			t.Fatal(err)
		}
		if _, err := engine.SetPolicy(provider.ProjectResource(project), iam.Policy{Bindings: []iam.Binding{
			{Role: "roles/datastore.user", Members: []string{principal("actor", project)}},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	manager := backend.NewManager(60*time.Second, 10*time.Second)
	if _, err := New(context.Background(), registry, manager, index, engine, config); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := manager.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})
	gate := gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		actor, project, _ := bytes.Cut([]byte(token), []byte(":"))
		return gateway.Principal{State: gateway.Authenticated, Subject: principal(string(actor), string(project)), Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})
	return &realHarness{t: t, gate: gate, manager: manager}
}

func (h *realHarness) invoke(token, verb, path string, body any) (int, map[string]any) {
	h.t.Helper()
	var encoded []byte
	if body != nil {
		encoded, _ = json.Marshal(body) //nolint:errchkjson // plain map bodies
	}
	request, _ := http.NewRequestWithContext(context.Background(), verb, path, bytes.NewReader(encoded))
	if encoded != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	h.gate.ServeHTTP(recorder, request)
	var decoded map[string]any
	_ = json.Unmarshal(recorder.Body.Bytes(), &decoded)
	return recorder.Code, decoded
}

func documentsPath(project string) string {
	return "/v1/projects/" + project + "/databases/(default)/documents/" + testCollection
}

func TestEmulatorDeniedBeforeStart(t *testing.T) {
	h := newRealHarness(t)
	if code, _ := h.invoke("actor:"+otherProject, http.MethodPatch, documentsPath(testProject)+"/"+testDoc, titleBody("no")); code != http.StatusForbidden {
		t.Fatalf("cross-project write = %d", code)
	}
	if statuses := h.manager.Status(); len(statuses) != 0 && statuses[0].Phase != backend.Stopped {
		t.Fatalf("a denied request started the emulator: %+v", statuses)
	}
}

func TestEmulatorLifecycleIsolationAndReset(t *testing.T) {
	h := newRealHarness(t)
	owner, other := "actor:"+testProject, "actor:"+otherProject
	code, created := h.invoke(owner, http.MethodPost, documentsPath(testProject)+"?documentId="+testDoc, titleBody("fog"))
	if code != http.StatusOK || created["name"] != "projects/"+testProject+"/databases/(default)/documents/"+testCollection+"/"+testDoc {
		t.Fatalf("create = %d %+v", code, created)
	}
	if code, _ := h.invoke(owner, http.MethodPost, documentsPath(testProject)+"?documentId="+testDoc, titleBody("fog")); code != http.StatusConflict {
		t.Fatalf("duplicate create = %d", code)
	}
	// The same collection and ID in another project is a different document.
	if code, _ := h.invoke(other, http.MethodGet, documentsPath(otherProject)+"/"+testDoc, nil); code != http.StatusNotFound {
		t.Fatalf("other project read = %d", code)
	}
	if code, listed := h.invoke(other, http.MethodGet, documentsPath(otherProject), nil); code != http.StatusOK || listed["documents"] != nil {
		t.Fatalf("other project listing = %d %+v", code, listed)
	}
	if code, _ := h.invoke(other, http.MethodPost, documentsPath(otherProject)+"?documentId="+testDoc, titleBody("other")); code != http.StatusOK {
		t.Fatalf("other project create = %d", code)
	}
	code, read := h.invoke(owner, http.MethodGet, documentsPath(testProject)+"/"+testDoc, nil)
	if fields, _ := read["fields"].(map[string]any); code != http.StatusOK || fields["title"].(map[string]any)["stringValue"] != "fog" {
		t.Fatalf("project document after other write = %d %+v", code, read)
	}
	if err := h.manager.Reset(context.Background(), backend.Key{Service: serviceName, Scope: provider.ProjectScope(testProject)}); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if code, _ := h.invoke(owner, http.MethodGet, documentsPath(testProject)+"/"+testDoc, nil); code != http.StatusNotFound {
		t.Fatalf("read after reset = %d", code)
	}
	if code, _ := h.invoke(other, http.MethodGet, documentsPath(otherProject)+"/"+testDoc, nil); code != http.StatusOK {
		t.Fatalf("reset reached another project: %d", code)
	}
}

// TestEmulatorRESTQueriesTransactionsAndSubcollections drives the new REST
// methods against the real emulator.
func TestEmulatorRESTQueriesTransactionsAndSubcollections(t *testing.T) {
	h := newRealHarness(t)
	owner := "actor:" + testProject
	root := "/v1/projects/" + testProject + "/databases/(default)/documents"
	name := func(path string) string { return "projects/" + testProject + "/databases/(default)/documents/" + path }
	population := func(n string) map[string]any {
		return map[string]any{"fields": map[string]any{"population": map[string]any{"integerValue": n}}}
	}
	for id, n := range map[string]string{"sf": "870000", "la": "3900000"} {
		if code, body := h.invoke(owner, http.MethodPost, root+"/cities?documentId="+id, population(n)); code != http.StatusOK {
			t.Fatalf("create %s = %d %+v", id, code, body)
		}
	}
	if code, body := h.invoke(owner, http.MethodPost, root+"/cities/sf/landmarks?documentId=ggb", titleBody("bridge")); code != http.StatusOK || body["name"] != name("cities/sf/landmarks/ggb") {
		t.Fatalf("nested create = %d %+v", code, body)
	}
	if code, body := h.invoke(owner, http.MethodGet, root+"/cities/sf/landmarks", nil); code != http.StatusOK || len(body["documents"].([]any)) != 1 {
		t.Fatalf("nested list = %d %+v", code, body)
	}
	if code, body := h.invoke(owner, http.MethodPost, root+"/cities/sf:listCollectionIds", map[string]any{}); code != http.StatusOK || body["collectionIds"].([]any)[0] != "landmarks" {
		t.Fatalf("collection ids = %d %+v", code, body)
	}
	query := map[string]any{"structuredQuery": map[string]any{
		"from":    []any{map[string]any{"collectionId": "cities"}},
		"where":   map[string]any{"fieldFilter": map[string]any{"field": map[string]any{"fieldPath": "population"}, "op": "GREATER_THAN", "value": map[string]any{"integerValue": "1000000"}}},
		"orderBy": []any{map[string]any{"field": map[string]any{"fieldPath": "population"}, "direction": "DESCENDING"}},
		"limit":   1,
	}}
	code, results := h.invokeArray(owner, root+":runQuery", query)
	if code != http.StatusOK || len(results) != 1 || results[0]["document"].(map[string]any)["name"] != name("cities/la") {
		t.Fatalf("query = %d %+v", code, results)
	}
	code, begun := h.invoke(owner, http.MethodPost, root+":beginTransaction", map[string]any{})
	if code != http.StatusOK || begun["transaction"] == "" {
		t.Fatalf("begin = %d %+v", code, begun)
	}
	commit := map[string]any{"transaction": begun["transaction"], "writes": []any{
		map[string]any{"update": map[string]any{"name": name("cities/sf"), "fields": population("1")["fields"]}, "currentDocument": map[string]any{"exists": true}},
		map[string]any{"delete": name("cities/la")},
	}}
	if code, body := h.invoke(owner, http.MethodPost, root+":commit", commit); code != http.StatusOK || body["commitTime"] == nil {
		t.Fatalf("commit = %d %+v", code, body)
	}
	if code, _ := h.invoke(owner, http.MethodGet, root+"/cities/la", nil); code != http.StatusNotFound {
		t.Fatalf("deleted document read = %d", code)
	}
	if code, _ := h.invoke("actor:"+otherProject, http.MethodPost, root+":commit", commit); code != http.StatusForbidden {
		t.Fatalf("cross-project commit = %d", code)
	}
}

// TestEmulatorGRPCListenDeliversDocumentChanges opens a Listen stream through
// the gateway's gRPC server against the real emulator.
func TestEmulatorGRPCListenDeliversDocumentChanges(t *testing.T) {
	h := newRealHarness(t)
	owner := "actor:" + testProject
	if code, _ := h.invoke(owner, http.MethodPost, documentsPath(testProject)+"?documentId="+testDoc, titleBody("watched")); code != http.StatusOK {
		t.Fatalf("create = %d", code)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := h.gate.NewGRPCServer()
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+owner)
	database := "projects/" + testProject + "/databases/(default)"
	stream, err := pb.NewFirestoreClient(connection).Listen(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&pb.ListenRequest{Database: database, TargetChange: &pb.ListenRequest_AddTarget{AddTarget: &pb.Target{
		TargetId: 1, TargetType: &pb.Target_Documents{Documents: &pb.Target_DocumentsTarget{Documents: []string{database + "/documents/" + testCollection + "/" + testDoc}}},
	}}}); err != nil {
		t.Fatal(err)
	}
	for {
		response, err := stream.Recv()
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		if change := response.GetDocumentChange(); change != nil {
			if change.GetDocument().GetFields()["title"].GetStringValue() != "watched" {
				t.Fatalf("document change = %v", change)
			}
			return
		}
	}
}

// invokeArray issues a POST whose response is a JSON array (streamed methods).
func (h *realHarness) invokeArray(token, path string, body any) (int, []map[string]any) {
	h.t.Helper()
	encoded, _ := json.Marshal(body) //nolint:errchkjson // plain map bodies
	request, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, path, bytes.NewReader(encoded))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	h.gate.ServeHTTP(recorder, request)
	var decoded []map[string]any
	_ = json.Unmarshal(recorder.Body.Bytes(), &decoded)
	return recorder.Code, decoded
}
