package secretmanager

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// Resource names the parity steps use.
const (
	projectName = "projects/" + testProject
	secretName  = projectName + "/secrets/" + testSecret
	numberName  = "projects/123456/secrets/" + testSecret
	secretPath  = "/v1/" + secretName
)

// filterGap is the refusal of a list filter on either transport.
const filterGap = "Secret Manager coverage gap: list filter is not implemented"

// grpcConn serves the harness gateway over an in-memory gRPC listener.
func grpcConn(t *testing.T, h *harness) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := h.gate.NewGRPCServer()
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// as returns a request context carrying one principal's bearer token; an
// empty actor is anonymous.
func as(actor string) context.Context {
	ctx := context.Background()
	if actor == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+actor)
}

// httpCodes is the REST status each gRPC code renders as.
var httpCodes = map[codes.Code]int{
	codes.OK: 200, codes.InvalidArgument: 400, codes.FailedPrecondition: 400, codes.Unauthenticated: 401,
	codes.PermissionDenied: 403, codes.NotFound: 404, codes.AlreadyExists: 409, codes.Unimplemented: 501,
}

// smRPC is one gRPC call of a parity step.
type smRPC = func(context.Context, secretmanagerpb.SecretManagerServiceClient) (proto.Message, error)

// parityStep is one operation sent over REST to one provider and over gRPC
// to an identical second provider.
type parityStep struct {
	name       string
	verb, path string
	body       any
	actor      string
	code       codes.Code
	rpc        smRPC
	response   func() proto.Message
}

// TestRESTAndGRPCShareResults drives the same lifecycle through both
// transports and requires identical documents, codes, and messages.
func TestRESTAndGRPCShareResults(t *testing.T) {
	stamp := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	rest, rpc := newHarness(t), newHarness(t)
	rest.provider.clock = func() time.Time { return stamp }
	rpc.provider.clock = rest.provider.clock
	client := secretmanagerpb.NewSecretManagerServiceClient(grpcConn(t, rpc))
	for _, step := range append(secretSteps(), versionSteps()...) {
		recorder := rest.invoke(step.verb, step.path, step.body, step.actor)
		got, err := step.rpc(as(step.actor), client)
		requireSameResult(t, step, recorder, got, err)
	}
}

// requireSameResult compares one step's REST reply with its gRPC result.
func requireSameResult(t *testing.T, step parityStep, recorder *httptest.ResponseRecorder, got proto.Message, err error) {
	t.Helper()
	if code := status.Code(err); code != step.code || recorder.Code != httpCodes[step.code] {
		t.Fatalf("%s: gRPC %v, REST %d %s", step.name, err, recorder.Code, recorder.Body.String())
	}
	if err != nil {
		var body struct {
			Error struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		_ = json.Unmarshal(recorder.Body.Bytes(), &body)
		if step.code == codes.Unimplemented && body.Error.Message != filterGap {
			t.Fatalf("%s: refusal %q", step.name, body.Error.Message)
		}
		if status.Convert(err).Message() != body.Error.Message {
			t.Fatalf("%s: gRPC message %q, REST %q", step.name, status.Convert(err).Message(), body.Error.Message)
		}
		return
	}
	want := step.response()
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(recorder.Body.Bytes(), want); err != nil {
		t.Fatalf("%s: decode REST %s: %v", step.name, recorder.Body.String(), err)
	}
	if !proto.Equal(want, got) {
		t.Fatalf("%s:\nREST %v\ngRPC %v", step.name, want, got)
	}
}

func secretDoc() proto.Message  { return &secretmanagerpb.Secret{} }
func versionDoc() proto.Message { return &secretmanagerpb.SecretVersion{} }
func accessDoc() proto.Message  { return &secretmanagerpb.AccessSecretVersionResponse{} }
func policyDoc() proto.Message  { return &iampb.Policy{} }

var createRPC smRPC = func(ctx context.Context, c secretmanagerpb.SecretManagerServiceClient) (proto.Message, error) {
	return c.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{Parent: projectName, SecretId: testSecret, Secret: &secretmanagerpb.Secret{
		Replication: &secretmanagerpb.Replication{Replication: &secretmanagerpb.Replication_Automatic_{Automatic: &secretmanagerpb.Replication_Automatic{}}},
		Labels:      map[string]string{"env": "synthetic"},
	}})
}

func getRPC(name string) smRPC {
	return func(ctx context.Context, c secretmanagerpb.SecretManagerServiceClient) (proto.Message, error) {
		return c.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: name})
	}
}

func accessRPC(name string) smRPC {
	return func(ctx context.Context, c secretmanagerpb.SecretManagerServiceClient) (proto.Message, error) {
		return c.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: name})
	}
}

// versionRPC disables, enables, or destroys version 1.
func versionRPC(verb string) smRPC {
	return func(ctx context.Context, c secretmanagerpb.SecretManagerServiceClient) (proto.Message, error) {
		name := secretName + "/versions/1"
		switch verb {
		case "disable":
			return c.DisableSecretVersion(ctx, &secretmanagerpb.DisableSecretVersionRequest{Name: name})
		case "enable":
			return c.EnableSecretVersion(ctx, &secretmanagerpb.EnableSecretVersionRequest{Name: name})
		default:
			return c.DestroySecretVersion(ctx, &secretmanagerpb.DestroySecretVersionRequest{Name: name})
		}
	}
}

// secretSteps covers the secret-level methods and IAM.
func secretSteps() []parityStep {
	accessor := "serviceAccount:" + testAccessor + "@" + testProject + ".iam.gserviceaccount.com"
	createBody := map[string]any{"replication": replicationAuto(), "labels": map[string]any{"env": "synthetic"}}
	return []parityStep{
		{"create", http.MethodPost, "/v1/" + projectName + "/secrets?secretId=" + testSecret, createBody, "actor", codes.OK, createRPC, secretDoc},
		{"create again", http.MethodPost, "/v1/" + projectName + "/secrets?secretId=" + testSecret, createBody, "actor", codes.AlreadyExists, createRPC, secretDoc},
		{"get by number", http.MethodGet, "/v1/" + numberName, nil, "actor", codes.OK, getRPC(numberName), secretDoc},
		{"anonymous get", http.MethodGet, secretPath, nil, "", codes.Unauthenticated, getRPC(secretName), secretDoc},
		{"update labels", http.MethodPatch, secretPath + "?updateMask=labels", map[string]any{"name": secretName, "labels": map[string]any{"env": "changed"}}, "actor", codes.OK,
			func(ctx context.Context, c secretmanagerpb.SecretManagerServiceClient) (proto.Message, error) {
				return c.UpdateSecret(ctx, &secretmanagerpb.UpdateSecretRequest{Secret: &secretmanagerpb.Secret{Name: secretName, Labels: map[string]string{"env": "changed"}},
					UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}}})
			}, secretDoc},
		{"list", http.MethodGet, "/v1/" + projectName + "/secrets?pageSize=10", nil, "actor", codes.OK, func(ctx context.Context, c secretmanagerpb.SecretManagerServiceClient) (proto.Message, error) {
			return c.ListSecrets(ctx, &secretmanagerpb.ListSecretsRequest{Parent: projectName, PageSize: 10})
		}, func() proto.Message { return &secretmanagerpb.ListSecretsResponse{} }},
		{"list with filter", http.MethodGet, "/v1/" + projectName + "/secrets?filter=labels.env%3Dchanged", nil, "actor", codes.Unimplemented, func(ctx context.Context, c secretmanagerpb.SecretManagerServiceClient) (proto.Message, error) {
			return c.ListSecrets(ctx, &secretmanagerpb.ListSecretsRequest{Parent: projectName, Filter: "labels.env=changed"})
		}, nil},
		{"grant accessor", http.MethodPost, secretPath + ":setIamPolicy", map[string]any{"policy": map[string]any{"bindings": []any{map[string]any{
			"role": "roles/secretmanager.secretAccessor", "members": []any{accessor},
		}}}}, "actor", codes.OK, func(ctx context.Context, c secretmanagerpb.SecretManagerServiceClient) (proto.Message, error) {
			return c.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{Resource: secretName, Policy: &iampb.Policy{Bindings: []*iampb.Binding{{Role: "roles/secretmanager.secretAccessor", Members: []string{accessor}}}}})
		}, policyDoc},
		{"read policy", http.MethodGet, secretPath + ":getIamPolicy", nil, "actor", codes.OK, func(ctx context.Context, c secretmanagerpb.SecretManagerServiceClient) (proto.Message, error) {
			return c.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: secretName})
		}, policyDoc},
	}
}

// versionSteps covers versions, payload access, and deletion.
func versionSteps() []parityStep {
	payload := []byte("hello")
	return []parityStep{
		{"add version", http.MethodPost, secretPath + ":addVersion", map[string]any{"payload": map[string]any{"data": base64Of(string(payload))}}, "actor", codes.OK,
			func(ctx context.Context, c secretmanagerpb.SecretManagerServiceClient) (proto.Message, error) {
				return c.AddSecretVersion(ctx, &secretmanagerpb.AddSecretVersionRequest{Parent: secretName, Payload: &secretmanagerpb.SecretPayload{Data: payload}})
			}, versionDoc},
		{"denied access", http.MethodGet, secretPath + "/versions/latest:access", nil, "outsider", codes.PermissionDenied, accessRPC(secretName + "/versions/latest"), accessDoc},
		{"granted access", http.MethodGet, "/v1/" + numberName + "/versions/latest:access", nil, testAccessor, codes.OK, accessRPC(numberName + "/versions/latest"), accessDoc},
		{"get version", http.MethodGet, secretPath + "/versions/1", nil, "actor", codes.OK, func(ctx context.Context, c secretmanagerpb.SecretManagerServiceClient) (proto.Message, error) {
			return c.GetSecretVersion(ctx, &secretmanagerpb.GetSecretVersionRequest{Name: secretName + "/versions/1"})
		}, versionDoc},
		{"list versions", http.MethodGet, secretPath + "/versions", nil, "actor", codes.OK, func(ctx context.Context, c secretmanagerpb.SecretManagerServiceClient) (proto.Message, error) {
			return c.ListSecretVersions(ctx, &secretmanagerpb.ListSecretVersionsRequest{Parent: secretName})
		}, func() proto.Message { return &secretmanagerpb.ListSecretVersionsResponse{} }},
		{"list versions with filter", http.MethodGet, secretPath + "/versions?filter=state%3DENABLED", nil, "actor", codes.Unimplemented, func(ctx context.Context, c secretmanagerpb.SecretManagerServiceClient) (proto.Message, error) {
			return c.ListSecretVersions(ctx, &secretmanagerpb.ListSecretVersionsRequest{Parent: secretName, Filter: "state=ENABLED"})
		}, nil},
		{"disable", http.MethodPost, secretPath + "/versions/1:disable", map[string]any{}, "actor", codes.OK, versionRPC("disable"), versionDoc},
		{"disabled access", http.MethodGet, secretPath + "/versions/1:access", nil, testAccessor, codes.FailedPrecondition, accessRPC(secretName + "/versions/1"), accessDoc},
		{"enable", http.MethodPost, secretPath + "/versions/1:enable", map[string]any{}, "actor", codes.OK, versionRPC("enable"), versionDoc},
		{"destroy", http.MethodPost, secretPath + "/versions/1:destroy", map[string]any{}, "actor", codes.OK, versionRPC("destroy"), versionDoc},
		{"destroy again", http.MethodPost, secretPath + "/versions/1:destroy", map[string]any{}, "actor", codes.FailedPrecondition, versionRPC("destroy"), versionDoc},
		{"delete", http.MethodDelete, secretPath, nil, "actor", codes.OK, func(ctx context.Context, c secretmanagerpb.SecretManagerServiceClient) (proto.Message, error) {
			return c.DeleteSecret(ctx, &secretmanagerpb.DeleteSecretRequest{Name: secretName})
		}, func() proto.Message { return &emptypb.Empty{} }},
		{"get deleted", http.MethodGet, secretPath, nil, "actor", codes.NotFound, getRPC(secretName), secretDoc},
	}
}

// TestGRPCRefusesUnservedMethodsAndBindings pins the explicit gaps: methods
// with no REST provider method and the regional (locations) bindings.
func TestGRPCRefusesUnservedMethodsAndBindings(t *testing.T) {
	h := newHarness(t)
	client := secretmanagerpb.NewSecretManagerServiceClient(grpcConn(t, h))
	ctx := as("actor")
	regional := "projects/" + testProject + "/locations/us-central1/secrets/" + testSecret
	const method = "No provider implements /google.cloud.secretmanager.v1.SecretManagerService/"
	cases := []struct {
		call func() *status.Status
		want string
	}{
		{func() *status.Status {
			_, err := client.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{Resource: secretName, Permissions: []string{"secretmanager.secrets.get"}})
			return status.Convert(err)
		}, method + "TestIamPermissions"},
		{func() *status.Status {
			_, err := client.RotateSecret(ctx, &secretmanagerpb.RotateSecretRequest{Parent: secretName})
			return status.Convert(err)
		}, method + "RotateSecret"},
		{func() *status.Status {
			_, err := client.EnableManagedRotation(ctx, &secretmanagerpb.EnableManagedRotationRequest{Parent: secretName})
			return status.Convert(err)
		}, method + "EnableManagedRotation"},
		{func() *status.Status {
			_, err := client.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: regional})
			return status.Convert(err)
		}, method + "GetSecret for projects/*/locations/*/secrets/* resources"},
		{func() *status.Status {
			_, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{Parent: projectName + "/locations/us-central1", SecretId: testSecret})
			return status.Convert(err)
		}, method + "CreateSecret for projects/*/locations/* resources"},
	}
	for _, c := range cases {
		if s := c.call(); s.Code() != codes.Unimplemented || s.Message() != c.want {
			t.Fatalf("got %v, want UNIMPLEMENTED %q", s, c.want)
		}
	}
	// A malformed name outside every binding stays INVALID_ARGUMENT.
	if _, err := client.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: projectName + "/topics/x"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("malformed name = %v", err)
	}
}

// TestGRPCReturnsTheDecisionID exposes the IAM decision like Pub/Sub does.
func TestGRPCReturnsTheDecisionID(t *testing.T) {
	h := newHarness(t)
	client := secretmanagerpb.NewSecretManagerServiceClient(grpcConn(t, h))
	var header metadata.MD
	_, err := client.ListSecrets(as("actor"), &secretmanagerpb.ListSecretsRequest{Parent: projectName}, grpc.Header(&header))
	if err != nil || len(header.Get("x-gcp-local-decision-id")) != 1 || len(header.Get("x-gcp-local-trace-id")) != 1 {
		t.Fatalf("list = %v, header %v", err, header)
	}
}
