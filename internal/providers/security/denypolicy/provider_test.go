package denypolicy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"testing"
	"time"

	iamapi "cloud.google.com/go/iam/apiv2"
	iamv2 "cloud.google.com/go/iam/apiv2/iampb"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider/providertest"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	nodes       = "//cloudresourcemanager.googleapis.com/"
	testOrg     = "organizations/800"
	testFolder  = "folders/801"
	testProject = "dp-project"
	testNumber  = "802"
	adminToken  = "admin"
	reviewToken = "reviewer"
	victimToken = "victim"
	objectGet   = "storage.objects.get"
)

// collection is the project's deny policy collection, addressed by ID.
var collection = "/v2/policies/" + url.PathEscape("cloudresourcemanager.googleapis.com/projects/"+testProject) + "/denypolicies"

// world is the provider behind a gateway: an organization holding a folder
// holding a project. admin holds Deny Admin on the organization, reviewer
// Deny Reviewer on the project, and victim Storage Object Viewer on the
// project.
type world struct {
	t       *testing.T
	gateway *gateway.Gateway
	engine  *iam.Engine
}

func newWorld(t *testing.T) *world {
	t.Helper()
	index := state.NewIndex()
	engine, err := iam.New(index)
	if err != nil {
		t.Fatal(err)
	}
	global := state.Scope{Location: state.Global}
	for _, node := range []state.Resource{
		{Name: nodes + testOrg, Service: "resourcemanager", Kind: "organization", Scope: global},
		{Name: nodes + testFolder, Service: "resourcemanager", Kind: "folder", Parent: nodes + testOrg, Scope: global},
		{Name: nodes + "projects/" + testProject, Service: "resourcemanager", Kind: "project", Number: testNumber,
			Parent: nodes + testFolder, Scope: state.Scope{Project: testProject, Location: state.Global}},
	} {
		if err := index.Put("resourcemanager", node); err != nil {
			t.Fatal(err)
		}
	}
	grants := map[string][]iam.Binding{
		nodes + testOrg: {{Role: "roles/iam.denyAdmin", Members: []string{member(adminToken)}}},
		nodes + "projects/" + testProject: {
			{Role: "roles/iam.denyReviewer", Members: []string{member(reviewToken)}},
			{Role: "roles/storage.objectViewer", Members: []string{member(victimToken)}},
		},
	}
	for node, bindings := range grants {
		if _, err := engine.SetPolicy(node, iam.Policy{Bindings: bindings}); err != nil {
			t.Fatal(err)
		}
	}
	registry := gateway.NewRegistry()
	if _, err := New(context.Background(), registry, backend.NewManager(time.Second, time.Second), index, engine); err != nil {
		t.Fatal(err)
	}
	resolve := func(_ context.Context, token string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: member(token),
			Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	}
	return &world{t: t, gateway: gateway.New(registry, resolve), engine: engine}
}

// member is a token's principal.
func member(token string) string {
	return "serviceAccount:" + token + "@" + testProject + ".iam.gserviceaccount.com"
}

// send issues one REST request and decodes the JSON reply.
func (w *world) send(token, method, path string, body any, want int) map[string]any {
	w.t.Helper()
	var encoded []byte
	if body != nil {
		var err error
		if encoded, err = json.Marshal(body); err != nil {
			w.t.Fatal(err)
		}
	}
	request := httptest.NewRequestWithContext(context.Background(), method, path, bytes.NewReader(encoded))
	request.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	w.gateway.ServeHTTP(recorder, request)
	if recorder.Code != want {
		w.t.Fatalf("%s %s = %d, want %d: %s", method, path, recorder.Code, want, recorder.Body)
	}
	reply := map[string]any{}
	_ = json.Unmarshal(recorder.Body.Bytes(), &reply)
	return reply
}

// victimHolds reports whether victim holds storage.objects.get on the project.
func (w *world) victimHolds() bool {
	w.t.Helper()
	call := gateway.Call{Context: context.Background(), Principal: gateway.Principal{State: gateway.Authenticated, Subject: member(victimToken),
		Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}}
	held, _, err := w.engine.Permitted(call, iam.Target{Name: nodes + "projects/" + testProject}, []string{objectGet})
	if err != nil {
		w.t.Fatal(err)
	}
	return slices.Contains(held, objectGet)
}

// errorOf is an error reply's message and ErrorInfo reason.
func errorOf(reply map[string]any) (string, string) {
	body, _ := reply["error"].(map[string]any)
	message, _ := body["message"].(string)
	details, _ := body["details"].([]any)
	for _, detail := range details {
		if info, ok := detail.(map[string]any); ok {
			reason, _ := info["reason"].(string)
			return message, reason
		}
	}
	return message, ""
}

func denyBody(principal string) map[string]any {
	return map[string]any{"displayName": "d", "rules": []any{map[string]any{"denyRule": map[string]any{
		"deniedPrincipals": []string{principal}, "deniedPermissions": []string{"storage.googleapis.com/objects.get"}}}}}
}

const victimPrincipal = "principal://iam.googleapis.com/projects/-/serviceAccounts/victim@dp-project.iam.gserviceaccount.com"

// object is a reply's nested JSON object, or an empty one.
func object(reply map[string]any, key string) map[string]any {
	value, _ := reply[key].(map[string]any)
	return value
}

// A create answers with the finished operation GCP returns, without done,
// and the operation reads back done without metadata (recorded:
// iam-deny-policy-lifecycle).
func TestDenyPolicyCreateOperation(t *testing.T) {
	w := newWorld(t)
	if reply := w.send(adminToken, http.MethodGet, collection, nil, 200); len(reply) != 0 {
		t.Fatalf("empty list = %v", reply)
	}
	created := w.send(adminToken, http.MethodPost, collection+"?policyId=block", denyBody(victimPrincipal), 200)
	response := object(created, "response")
	name := "policies/cloudresourcemanager.googleapis.com%2Fprojects%2F802/denypolicies/block"
	if _, done := created["done"]; done || response["name"] != name || response["kind"] != "DenyPolicy" || response["uid"] == nil {
		t.Fatalf("create = %v", created)
	}
	if w.victimHolds() {
		t.Fatal("the created deny policy did not deny")
	}
	operationName, _ := created["name"].(string)
	operation := w.send(adminToken, http.MethodGet, "/v2/"+operationName, nil, 200)
	if _, metadata := operation["metadata"]; operation["done"] != true || metadata {
		t.Fatalf("operation = %v", operation)
	}
}

// Reads by ID and number agree; lists carry neither rules nor etags.
func TestDenyPolicyReadsAndLists(t *testing.T) {
	w := newWorld(t)
	w.send(adminToken, http.MethodPost, collection+"?policyId=block", denyBody(victimPrincipal), 200)
	stored := w.send(reviewToken, http.MethodGet, collection+"/block", nil, 200)
	byNumber := "/v2/policies/" + url.PathEscape("cloudresourcemanager.googleapis.com/projects/"+testNumber) + "/denypolicies/block"
	if w.send(reviewToken, http.MethodGet, byNumber, nil, 200)["etag"] != stored["etag"] {
		t.Fatal("a read by number differs")
	}
	listed, _ := w.send(reviewToken, http.MethodGet, collection, nil, 200)["policies"].([]any)
	if len(listed) != 1 {
		t.Fatalf("list = %v", listed)
	}
	if item, _ := listed[0].(map[string]any); item["rules"] != nil || item["etag"] != nil {
		t.Fatalf("listed policy = %v", item)
	}
}

// Duplicates and stale etags are refused; an update with the current etag
// applies at once; a delete answers with the policy without its uid.
func TestDenyPolicyConflictsUpdateAndDelete(t *testing.T) {
	w := newWorld(t)
	w.send(adminToken, http.MethodPost, collection+"?policyId=block", denyBody(victimPrincipal), 200)
	stored := w.send(reviewToken, http.MethodGet, collection+"/block", nil, 200)
	message, reason := errorOf(w.send(adminToken, http.MethodPost, collection+"?policyId=block", denyBody(victimPrincipal), 409))
	if reason != "POLICY_ALREADY_EXISTS" || message != "Policy ("+collection[len("/v2/"):]+"/block) already exists" {
		t.Fatalf("duplicate = %s %s", message, reason)
	}
	stale := denyBody(victimPrincipal)
	stale["etag"] = "stale"
	if _, reason := errorOf(w.send(adminToken, http.MethodPut, collection+"/block", stale, 409)); reason != "INVALID_ETAG" {
		t.Fatalf("stale update = %s", reason)
	}
	exempt := map[string]any{"etag": stored["etag"], "rules": []any{map[string]any{"denyRule": map[string]any{
		"deniedPrincipals": []string{victimPrincipal}, "exceptionPrincipals": []string{victimPrincipal},
		"deniedPermissions": []string{"storage.googleapis.com/objects.get"}}}}}
	updated := object(w.send(adminToken, http.MethodPut, collection+"/block", exempt, 200), "response")
	if updated["etag"] == stored["etag"] || !w.victimHolds() {
		t.Fatalf("update = %v", updated)
	}
	w.send(adminToken, http.MethodDelete, collection+"/block?etag=stale", nil, 409)
	gone := object(w.send(adminToken, http.MethodDelete, collection+"/block", nil, 200), "response")
	if _, uid := gone["uid"]; uid || gone["deleteTime"] == nil {
		t.Fatalf("delete = %v", gone)
	}
	message, reason = errorOf(w.send(reviewToken, http.MethodGet, collection+"/block", nil, 404))
	if reason != "POLICY_NOT_FOUND" || message != "Policy ("+collection[len("/v2/"):]+"/block) does not exist" {
		t.Fatalf("deleted read = %s %s", message, reason)
	}
}

// Denials name the v2 permission and the attachment point as addressed,
// with the project by number in the ErrorInfo; request refusals use GCP's
// recorded text.
func TestDenialsAndRefusals(t *testing.T) {
	w := newWorld(t)
	message, reason := errorOf(w.send(reviewToken, http.MethodPost, collection+"?policyId=block", denyBody(victimPrincipal), 403))
	if reason != "IAM_PERMISSION_DENIED" || message != "Permission iam.googleapis.com/denypolicies.create denied on resource cloudresourcemanager.googleapis.com/projects/"+testProject {
		t.Fatalf("reviewer create = %s %s", message, reason)
	}
	missing := "/v2/policies/" + url.PathEscape("cloudresourcemanager.googleapis.com/projects/absent") + "/denypolicies"
	w.send(adminToken, http.MethodGet, missing, nil, 403)
	refusals := map[string]string{
		collection + "?policyId=Bad_ID": "Policy id (Bad_ID) does not follow the pattern [a-z][a-z0-9-.]+, or is not between 3 and 63 characters.",
		collection:                      "Request does not have required field `policy_id`",
	}
	for path, want := range refusals {
		if message, _ := errorOf(w.send(adminToken, http.MethodPost, path, denyBody(victimPrincipal), 400)); message != want {
			t.Fatalf("%s = %s", path, message)
		}
	}
	other := "/v2/policies/" + url.PathEscape("cloudresourcemanager.googleapis.com/projects/"+testProject) + "/allowpolicies"
	if message, _ := errorOf(w.send(adminToken, http.MethodGet, other, nil, 400)); message != "Policy kind (allowpolicies) is not supported" {
		t.Fatalf("other kind = %s", message)
	}
	if _, reason := errorOf(w.send(adminToken, http.MethodPost, collection+"?policyId=bad", denyBody("serviceAccount:victim@dp-project.iam.gserviceaccount.com"), 400)); reason != "INVALID_PRINCIPAL" {
		t.Fatalf("v1 principal = %s", reason)
	}
	managed := denyBody(victimPrincipal)
	managed["managingAuthority"] = "x"
	if message, _ := errorOf(w.send(adminToken, http.MethodPost, collection+"?policyId=managed", managed, 501)); message != "IAM deny policies coverage gap: managingAuthority is not modeled" {
		t.Fatalf("managingAuthority = %s", message)
	}
	// Folders and organizations are attachment points too.
	for _, parent := range []string{testFolder, testOrg} {
		path := "/v2/policies/" + url.PathEscape("cloudresourcemanager.googleapis.com/"+parent) + "/denypolicies"
		w.send(adminToken, http.MethodPost, path+"?policyId=upper", denyBody(victimPrincipal), 200)
		if w.victimHolds() {
			t.Fatalf("a policy on %s did not deny", parent)
		}
		w.send(adminToken, http.MethodDelete, path+"/upper", nil, 200)
	}
}

// client dials the world's gRPC server with the Go IAM v2 client as admin.
func (w *world) client() *iamapi.PoliciesClient {
	w.t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		w.t.Fatal(err)
	}
	server := w.gateway.NewGRPCServer()
	go func() { _ = server.Serve(listener) }()
	w.t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///"+listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(tokenCredentials(adminToken)))
	if err != nil {
		w.t.Fatal(err)
	}
	w.t.Cleanup(func() { _ = conn.Close() })
	client, err := iamapi.NewPoliciesClient(context.Background(), option.WithGRPCConn(conn))
	if err != nil {
		w.t.Fatal(err)
	}
	return client
}

// tokenCredentials sends a bearer token over plaintext loopback.
type tokenCredentials string

func (c tokenCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + string(c)}, nil
}

func (tokenCredentials) RequireTransportSecurity() bool { return false }

// grpcParent is the project's deny policy collection as a gRPC parent.
var grpcParent = "policies/" + url.PathEscape("cloudresourcemanager.googleapis.com/projects/"+testProject) + "/denypolicies"

// The Go IAM v2 client creates, reads, and lists through the gateway's
// gRPC transcoding, waiting on the create operation.
func TestGoClientCreateAndRead(t *testing.T) {
	w := newWorld(t)
	ctx, client := context.Background(), w.client()
	created := createOverGRPC(t, client, "grpc")
	if created.GetName() != "policies/cloudresourcemanager.googleapis.com%2Fprojects%2F802/denypolicies/grpc" || w.victimHolds() {
		t.Fatalf("create = %v", created)
	}
	read, err := client.GetPolicy(ctx, &iamv2.GetPolicyRequest{Name: created.GetName()})
	if err != nil || read.GetEtag() != created.GetEtag() {
		t.Fatalf("get = %v, %v", read, err)
	}
	listed := client.ListPolicies(ctx, &iamv2.ListPoliciesRequest{Parent: grpcParent})
	if first, err := listed.Next(); err != nil || first.GetName() != created.GetName() {
		t.Fatalf("list = %v, %v", first, err)
	}
	if _, err := listed.Next(); !errors.Is(err, iterator.Done) {
		t.Fatalf("list end = %v", err)
	}
}

// The Go IAM v2 client updates and deletes, waiting on each operation.
func TestGoClientUpdateAndDelete(t *testing.T) {
	w := newWorld(t)
	ctx, client := context.Background(), w.client()
	created := createOverGRPC(t, client, "grpc")
	created.Rules[0].GetDenyRule().ExceptionPrincipals = []string{victimPrincipal}
	update, err := client.UpdatePolicy(ctx, &iamv2.UpdatePolicyRequest{Policy: created})
	if err != nil {
		t.Fatal(err)
	}
	if updated, err := update.Wait(ctx); err != nil || updated.GetEtag() == created.GetEtag() || !w.victimHolds() {
		t.Fatalf("update = %v, %v", updated, err)
	}
	removal, err := client.DeletePolicy(ctx, &iamv2.DeletePolicyRequest{Name: created.GetName()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := removal.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetPolicy(ctx, &iamv2.GetPolicyRequest{Name: created.GetName()}); status.Code(err) != codes.NotFound {
		t.Fatalf("deleted = %v", err)
	}
}

// createOverGRPC creates the deny policy id over gRPC and waits for it.
func createOverGRPC(t *testing.T, client *iamapi.PoliciesClient, id string) *iamv2.Policy {
	t.Helper()
	rule := &iamv2.DenyRule{DeniedPrincipals: []string{victimPrincipal}, DeniedPermissions: []string{"storage.googleapis.com/objects.get"}}
	policy := &iamv2.Policy{Rules: []*iamv2.PolicyRule{{Kind: &iamv2.PolicyRule_DenyRule{DenyRule: rule}}}}
	operation, err := client.CreatePolicy(context.Background(), &iamv2.CreatePolicyRequest{Parent: grpcParent, PolicyId: id, Policy: policy})
	if err != nil {
		t.Fatal(err)
	}
	created, err := operation.Wait(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return created
}

// TestConcurrentDenyPolicyWrites races creates, updates, reads, lists, and
// deletes of one policy; run it under -race.
func TestConcurrentDenyPolicyWrites(t *testing.T) {
	w := newWorld(t)
	body := denyBody(victimPrincipal)
	providertest.Hammer(t, w.gateway, adminToken, 8, 20, func(int) []providertest.Request {
		return []providertest.Request{
			{Method: http.MethodPost, Path: collection + "?policyId=race", Body: body, Want: []int{200, 409}},
			{Method: http.MethodPut, Path: collection + "/race", Body: body, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: collection + "/race", Want: []int{200, 404}},
			{Method: http.MethodGet, Path: collection, Want: []int{200}},
			{Method: http.MethodDelete, Path: collection + "/race", Want: []int{200, 404}},
		}
	})
}
