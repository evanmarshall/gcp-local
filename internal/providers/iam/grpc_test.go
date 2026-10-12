package iam

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"testing"

	admin "cloud.google.com/go/iam/admin/apiv1"
	"cloud.google.com/go/iam/admin/apiv1/adminpb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// bearer attaches one harness token to every RPC over plaintext loopback.
type bearer string

func (b bearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + string(b)}, nil
}
func (bearer) RequireTransportSecurity() bool { return false }

// serveGRPC starts the harness gateway's gRPC server and dials it as token.
func (h *harness) serveGRPC(tokens ...string) []*grpc.ClientConn {
	h.t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		h.t.Fatal(err)
	}
	server := h.gate.NewGRPCServer()
	go func() { _ = server.Serve(listener) }()
	h.t.Cleanup(server.Stop)
	conns := make([]*grpc.ClientConn, 0, len(tokens))
	for _, token := range tokens {
		conn, err := grpc.NewClient("passthrough:///"+listener.Addr().String(),
			grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithPerRPCCredentials(bearer(token)))
		if err != nil {
			h.t.Fatal(err)
		}
		h.t.Cleanup(func() { _ = conn.Close() })
		conns = append(conns, conn)
	}
	return conns
}

// refused requires one gRPC status code and message.
func refused(t *testing.T, step string, err error, code codes.Code, message string) {
	t.Helper()
	if s := status.Convert(err); s.Code() != code || s.Message() != message {
		t.Fatalf("%s: got %v, want %s %q", step, err, code, message)
	}
}

// TestGoClientOverGRPC drives cloud.google.com/go/iam/admin/apiv1 through the
// gRPC gateway onto the REST provider methods: account create, get, list,
// patch, policy, keys, a denial, and delete.
func TestGoClientOverGRPC(t *testing.T) {
	h := newHarness(t)
	conns := h.serveGRPC(testToken, "peer")
	ctx := context.Background()
	client, err := admin.NewIamClient(ctx, option.WithGRPCConn(conns[0]))
	if err != nil {
		t.Fatal(err)
	}
	email := accountEmail(testProject, testAccount)
	name := createAccountOverGRPC(t, client)
	patched, err := client.PatchServiceAccount(ctx, &adminpb.PatchServiceAccountRequest{
		ServiceAccount: &adminpb.ServiceAccount{Name: name, DisplayName: "renamed"}, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}}})
	if err != nil || patched.GetDisplayName() != "renamed" {
		t.Fatalf("patch = %v, %v", patched, err)
	}
	if policy, err := client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: name}); err != nil || len(policy.InternalProto.GetEtag()) == 0 {
		t.Fatalf("policy = %v, %v", policy, err)
	}
	peer, err := admin.NewIamClient(ctx, option.WithGRPCConn(conns[1]))
	if err != nil {
		t.Fatal(err)
	}
	_, err = peer.GetServiceAccount(ctx, &adminpb.GetServiceAccountRequest{Name: name})
	refused(t, "ungranted get", err, codes.PermissionDenied,
		"The caller does not have iam.serviceAccounts.get access to "+accountResource(testProject, email)+".")
	keysOverGRPC(t, h, client, name)
	if err := client.DeleteServiceAccount(ctx, &adminpb.DeleteServiceAccountRequest{Name: name}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	_, err = client.GetServiceAccount(ctx, &adminpb.GetServiceAccountRequest{Name: name})
	refused(t, "deleted get", err, codes.NotFound, "The resource 'serviceAccount/"+testAccount+"' was not found")
}

// createAccountOverGRPC creates, reads, and lists the test account and
// returns its name.
func createAccountOverGRPC(t *testing.T, client *admin.IamClient) string {
	t.Helper()
	ctx := context.Background()
	project := "projects/" + testProject
	email := accountEmail(testProject, testAccount)
	name := project + "/serviceAccounts/" + email
	account, err := client.CreateServiceAccount(ctx, &adminpb.CreateServiceAccountRequest{Name: project, AccountId: testAccount,
		ServiceAccount: &adminpb.ServiceAccount{DisplayName: "worker"}})
	if err != nil || account.GetEmail() != email || account.GetName() != name || account.GetUniqueId() == "" {
		t.Fatalf("create = %v, %v", account, err)
	}
	if got, err := client.GetServiceAccount(ctx, &adminpb.GetServiceAccountRequest{Name: name}); err != nil || got.GetDisplayName() != "worker" {
		t.Fatalf("get = %v, %v", got, err)
	}
	if listed, err := client.ListServiceAccounts(ctx, &adminpb.ListServiceAccountsRequest{Name: project}).Next(); err != nil || listed.GetEmail() != email {
		t.Fatalf("list = %v, %v", listed, err)
	}
	return name
}

// keysOverGRPC lists, reads, and deletes a key created over REST, whose key
// file names the HTTP origin.
func keysOverGRPC(t *testing.T, h *harness, client *admin.IamClient, account string) {
	t.Helper()
	ctx := context.Background()
	var created struct {
		Name string `json:"name"`
	}
	recorder := h.expect(http.MethodPost, testBase+"/"+testAccount+"/keys", map[string]any{}, 200)
	if json.Unmarshal(recorder.Body.Bytes(), &created) != nil {
		t.Fatalf("key body %s", recorder.Body.String())
	}
	keys, err := client.ListServiceAccountKeys(ctx, &adminpb.ListServiceAccountKeysRequest{Name: account})
	if err != nil || len(keys.GetKeys()) != 1 || keys.GetKeys()[0].GetKeyType() != adminpb.ListServiceAccountKeysRequest_USER_MANAGED {
		t.Fatalf("list keys = %v, %v", keys, err)
	}
	keyName := account + "/keys/" + created.Name[strings.LastIndex(created.Name, "/")+1:]
	if key, err := client.GetServiceAccountKey(ctx, &adminpb.GetServiceAccountKeyRequest{Name: keyName}); err != nil || key.GetKeyAlgorithm() != adminpb.ServiceAccountKeyAlgorithm_KEY_ALG_RSA_2048 {
		t.Fatalf("get key = %v, %v", key, err)
	}
	if err := client.DeleteServiceAccountKey(ctx, &adminpb.DeleteServiceAccountKeyRequest{Name: keyName}); err != nil {
		t.Fatalf("delete key: %v", err)
	}
}

// TestGapsOverGRPC asserts each gRPC refusal of the IAM admin service.
func TestGapsOverGRPC(t *testing.T) {
	h := newHarness(t)
	conn := h.serveGRPC(testToken)[0]
	client := adminpb.NewIAMClient(conn)
	ctx := context.Background()
	name := "projects/" + testProject + "/serviceAccounts/" + accountEmail(testProject, testAccount)
	_, createKey := client.CreateServiceAccountKey(ctx, &adminpb.CreateServiceAccountKeyRequest{Name: name})
	// SignBlob and its name field are deprecated in the client, so the
	// request is sent as GetServiceAccountRequest, whose field 1 is also name.
	signBlob := conn.Invoke(ctx, "/google.iam.admin.v1.IAM/SignBlob", &adminpb.GetServiceAccountRequest{Name: name}, &adminpb.SignBlobResponse{})
	_, update := client.UpdateServiceAccount(ctx, &adminpb.ServiceAccount{Name: name})
	_, testPermissions := client.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{Resource: name})
	// ListRoles has no resource variable on its primary binding.
	_, listRoles := client.ListRoles(ctx, &adminpb.ListRolesRequest{})
	for method, err := range map[string]error{
		"CreateServiceAccountKey": createKey, "SignBlob": signBlob, "UpdateServiceAccount": update,
		"TestIamPermissions": testPermissions, "ListRoles": listRoles,
	} {
		refused(t, method, err, codes.Unimplemented, "No provider implements /google.iam.admin.v1.IAM/"+method)
	}
}
