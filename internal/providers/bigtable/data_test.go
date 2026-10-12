package bigtable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Local identities enrolled for the data-plane tests.
const (
	viewerActor = "serviceAccount:viewer@" + testProject + ".iam.gserviceaccount.com"
	userActor   = "serviceAccount:user@" + testProject + ".iam.gserviceaccount.com"
	roleViewer  = "roles/bigtable.viewer"
)

// bearer attaches one local access token to every RPC. The gateway listens on
// plaintext loopback, so the credential does not demand transport security.
type bearer string

func (b bearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + string(b)}, nil
}
func (bearer) RequireTransportSecurity() bool { return false }

// dataGateway serves the harness registry over loopback gRPC with the local
// identity service resolving tokens, as the runtime does.
type dataGateway struct {
	endpoint string
	tokens   map[string]string // principal -> local access token
}

// serveData replaces the harness gateway with one backed by local identity,
// enrolls the actor, a project viewer, and a project user, and starts gRPC.
func serveData(t *testing.T, h *harness) *dataGateway {
	t.Helper()
	subjects := []string{testActor, viewerActor, userActor}
	enrollments := make([]identity.Enrollment, 0, len(subjects))
	for _, subject := range subjects {
		enrollments = append(enrollments, identity.Enrollment{Subject: subject, Project: testProject, SubjectToken: "gls.synthetic-bigtable-data-subject-token-" + strings.Split(strings.TrimPrefix(subject, "serviceAccount:"), "@")[0]})
	}
	identities, err := identity.New(identity.Config{Principals: enrollments}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for role, member := range map[string]string{roleViewer: viewerActor, "roles/bigtable.user": userActor} {
		policy, err := h.engine.GetPolicy(provider.ProjectResource(testProject))
		if err != nil {
			t.Fatal(err)
		}
		policy.Bindings = append(policy.Bindings, iam.Binding{Role: role, Members: []string{member}})
		if _, err := h.engine.SetPolicy(provider.ProjectResource(testProject), policy); err != nil {
			t.Fatal(err)
		}
	}
	h.gate = gateway.New(h.gate.Registry, identities.Resolve)
	d := &dataGateway{tokens: map[string]string{}}
	for _, enrollment := range enrollments {
		d.tokens[enrollment.Subject] = exchange(t, identities, enrollment.SubjectToken)
	}
	h.provider.serverOptions = append(h.provider.serverOptions, grpc.ChainStreamInterceptor(
		func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			h.backendCalls.Add(1)
			return handler(srv, ss)
		}))
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := h.gate.NewGRPCServer()
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	d.endpoint = listener.Addr().String()
	return d
}

// exchange performs the local ADC token exchange for one subject token.
func exchange(t *testing.T, identities *identity.Service, subject string) string {
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

// conn dials the gateway as one principal; an empty principal is anonymous.
func (d *dataGateway) conn(t *testing.T, principal string) *grpc.ClientConn {
	t.Helper()
	options := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if principal != "" {
		options = append(options, grpc.WithPerRPCCredentials(bearer(d.tokens[principal])))
	}
	conn, err := grpc.NewClient("passthrough:///"+d.endpoint, options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// client opens the official Go data client as one principal.
func (d *dataGateway) client(t *testing.T, principal string) *bigtable.Client {
	t.Helper()
	client, err := bigtable.NewClientWithConfig(context.Background(), testProject, testInstance,
		bigtable.ClientConfig{MetricsProvider: bigtable.NoopMetricsProvider{}},
		option.WithGRPCConn(d.conn(t, principal)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// TestDataPlaneThroughAuthenticatedGateway drives the official Go data client
// with local access tokens over gRPC: writes, point and range reads, samples,
// bulk, conditional, and read-modify-write mutations, and PingAndWarm.
func TestDataPlaneThroughAuthenticatedGateway(t *testing.T) {
	h := newHarness(t)
	d := serveData(t, h)
	expectOK(t, "instance", h.invokeAs(d.tokens[testActor], http.MethodPost, testInstances, createBody()))
	expectOK(t, "table", h.invokeAs(d.tokens[testActor], http.MethodPost, testInstances+"/"+testInstance+"/tables", map[string]any{
		"tableId": testTable, "table": map[string]any{"columnFamilies": map[string]any{familyOne: map[string]any{}}},
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	table := d.client(t, userActor).Open(testTable)
	writeRows(ctx, t, table)
	readRows(ctx, t, table)
	conditional := bigtable.NewMutation()
	conditional.Set(familyOne, "seen", bigtable.Now(), []byte("yes"))
	var matched bool
	if err := table.Apply(ctx, "row-1", bigtable.NewCondMutation(bigtable.ValueFilter("alpha"), conditional, nil),
		bigtable.GetCondMutationResult(&matched)); err != nil || !matched {
		t.Fatalf("CheckAndMutateRow = %v, %v", matched, err)
	}
	increment := bigtable.NewReadModifyWrite()
	increment.Increment(familyOne, "count", 5)
	if row, err := table.ApplyReadModifyWrite(ctx, "row-1", increment); err != nil || len(row[familyOne]) != 1 {
		t.Fatalf("ReadModifyWriteRow = %v, %v", row, err)
	}
	ping := bigtablepb.NewBigtableClient(d.conn(t, userActor))
	if _, err := ping.PingAndWarm(ctx, &bigtablepb.PingAndWarmRequest{Name: instanceName(testProject, testInstance)}); err != nil {
		t.Fatalf("PingAndWarm: %v", err)
	}
}

// writeRows applies one single-row and one bulk mutation.
func writeRows(ctx context.Context, t *testing.T, table *bigtable.Table) {
	t.Helper()
	write := bigtable.NewMutation()
	write.Set(familyOne, "name", bigtable.Now(), []byte("alpha"))
	if err := table.Apply(ctx, "row-1", write); err != nil {
		t.Fatalf("MutateRow: %v", err)
	}
	bulk := []*bigtable.Mutation{bigtable.NewMutation(), bigtable.NewMutation()}
	bulk[0].Set(familyOne, "name", bigtable.Now(), []byte("beta"))
	bulk[1].Set(familyOne, "name", bigtable.Now(), []byte("gamma"))
	if errs, err := table.ApplyBulk(ctx, []string{"row-2", "row-3"}, bulk); err != nil || errs != nil {
		t.Fatalf("MutateRows: %v %v", err, errs)
	}
}

// readRows reads the written rows back by key, by prefix, and as samples.
func readRows(ctx context.Context, t *testing.T, table *bigtable.Table) {
	t.Helper()
	row, err := table.ReadRow(ctx, "row-1")
	if err != nil || len(row[familyOne]) != 1 || string(row[familyOne][0].Value) != "alpha" {
		t.Fatalf("ReadRow = %v, %v", row, err)
	}
	var keys []string
	if err := table.ReadRows(ctx, bigtable.PrefixRange("row-"), func(r bigtable.Row) bool {
		keys = append(keys, r.Key())
		return true
	}); err != nil || strings.Join(keys, ",") != "row-1,row-2,row-3" {
		t.Fatalf("ReadRows = %v, %v", keys, err)
	}
	if _, err := table.SampleRowKeys(ctx); err != nil {
		t.Fatalf("SampleRowKeys: %v", err)
	}
}

// TestDataPlaneDeniedBeforeBackend proves denied, anonymous, and unsupported
// data calls fail before any emulator call, for unary and streaming RPCs.
func TestDataPlaneDeniedBeforeBackend(t *testing.T) {
	h := newHarness(t)
	d := serveData(t, h)
	expectOK(t, "instance", h.invokeAs(d.tokens[testActor], http.MethodPost, testInstances, createBody()))
	expectOK(t, "table", h.invokeAs(d.tokens[testActor], http.MethodPost, testInstances+"/"+testInstance+"/tables", map[string]any{
		"tableId": testTable, "table": map[string]any{"columnFamilies": map[string]any{familyOne: map[string]any{}}},
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	name := tableName(testProject, testInstance, testTable)
	mutation := []*bigtablepb.Mutation{{Mutation: &bigtablepb.Mutation_SetCell_{SetCell: &bigtablepb.Mutation_SetCell{
		FamilyName: familyOne, ColumnQualifier: []byte("c"), TimestampMicros: -1, Value: []byte("v")}}}}
	calls := func(conn *grpc.ClientConn) map[string]error {
		client := bigtablepb.NewBigtableClient(conn)
		results := map[string]error{}
		_, results["MutateRow"] = client.MutateRow(ctx, &bigtablepb.MutateRowRequest{TableName: name, RowKey: []byte("r"), Mutations: mutation})
		_, results["CheckAndMutateRow"] = client.CheckAndMutateRow(ctx, &bigtablepb.CheckAndMutateRowRequest{TableName: name, RowKey: []byte("r"), TrueMutations: mutation})
		_, results["ReadModifyWriteRow"] = client.ReadModifyWriteRow(ctx, &bigtablepb.ReadModifyWriteRowRequest{TableName: name, RowKey: []byte("r"),
			Rules: []*bigtablepb.ReadModifyWriteRule{{FamilyName: familyOne, ColumnQualifier: []byte("n"), Rule: &bigtablepb.ReadModifyWriteRule_IncrementAmount{IncrementAmount: 1}}}})
		_, results["PingAndWarm"] = client.PingAndWarm(ctx, &bigtablepb.PingAndWarmRequest{Name: instanceName(testProject, testInstance)})
		results["ReadRows"] = drain(client.ReadRows(ctx, &bigtablepb.ReadRowsRequest{TableName: name}))
		results["SampleRowKeys"] = drain(client.SampleRowKeys(ctx, &bigtablepb.SampleRowKeysRequest{TableName: name}))
		results["MutateRows"] = drain(client.MutateRows(ctx, &bigtablepb.MutateRowsRequest{TableName: name,
			Entries: []*bigtablepb.MutateRowsRequest_Entry{{RowKey: []byte("r"), Mutations: mutation}}}))
		return results
	}
	before := h.backendCalls.Load()
	for method, err := range calls(d.conn(t, viewerActor)) {
		if status.Code(err) != codes.PermissionDenied {
			t.Errorf("viewer %s = %v, want PermissionDenied", method, err)
		}
	}
	for method, err := range calls(d.conn(t, "")) {
		if status.Code(err) != codes.Unauthenticated {
			t.Errorf("anonymous %s = %v, want Unauthenticated", method, err)
		}
	}
	client := bigtablepb.NewBigtableClient(d.conn(t, testActor))
	view := drain(client.ReadRows(ctx, &bigtablepb.ReadRowsRequest{TableName: name, AuthorizedViewName: name + "/authorizedViews/v"}))
	if status.Code(view) != codes.Unimplemented {
		t.Errorf("authorized view = %v, want Unimplemented", view)
	}
	if after := h.backendCalls.Load(); after != before {
		t.Fatalf("refused data calls reached the emulator: %d calls", after-before)
	}
	missing := tableName(testProject, "missing-bt", testTable)
	if err := drain(client.ReadRows(ctx, &bigtablepb.ReadRowsRequest{TableName: missing})); status.Code(err) != codes.NotFound {
		t.Errorf("missing instance = %v, want NotFound", err)
	}
	viewer := bigtablepb.NewBigtableClient(d.conn(t, viewerActor))
	if err := drain(viewer.ReadRows(ctx, &bigtablepb.ReadRowsRequest{TableName: missing})); status.Code(err) != codes.PermissionDenied {
		t.Errorf("viewer missing instance = %v, want PermissionDenied", err)
	}
}

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
