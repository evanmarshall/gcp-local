package orgpolicy

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	orgpolicyapi "cloud.google.com/go/orgpolicy/apiv2"
	"cloud.google.com/go/orgpolicy/apiv2/orgpolicypb"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider/providertest"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	nodes       = "//cloudresourcemanager.googleapis.com/"
	testOrg     = "organizations/700"
	testFolder  = "folders/701"
	testProject = "op-project"
	testNumber  = "702"
	adminToken  = "admin"
	viewerToken = "viewer"
	readerToken = "reader"
	locations   = "/policies/gcp.resourceLocations"
)

// world is the provider behind a gateway: an organization holding a folder
// holding a project. admin holds Organization Policy Administrator on the
// organization, viewer Organization Policy Viewer on the project, and
// reader nothing.
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
	for node, binding := range map[string]iam.Binding{
		nodes + testOrg:                   {Role: "roles/orgpolicy.policyAdmin", Members: []string{member(adminToken)}},
		nodes + "projects/" + testProject: {Role: "roles/orgpolicy.policyViewer", Members: []string{member(viewerToken)}},
	} {
		if _, err := engine.SetPolicy(node, iam.Policy{Bindings: []iam.Binding{binding}}); err != nil {
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

// message is an error reply's message.
func message(reply map[string]any) string {
	errorBody, _ := reply["error"].(map[string]any)
	text, _ := errorBody["message"].(string)
	return text
}

// rules decodes a JSON spec's rules.
func rules(t *testing.T, text string) map[string]any {
	t.Helper()
	spec := map[string]any{}
	if err := json.Unmarshal([]byte(text), &spec); err != nil {
		t.Fatal(err)
	}
	return spec
}

// encode renders a decoded reply as compact JSON.
func encode(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

// text reads a string field of a reply.
func text(reply map[string]any, field string) string {
	value, _ := reply[field].(string)
	return value
}

// createInherited writes a folder policy and a project policy that inherits
// it and adds a value, and returns the project policy.
func createInherited(t *testing.T, w *world) map[string]any {
	t.Helper()
	folderPolicy := map[string]any{"name": testFolder + locations, "spec": rules(t, `{"rules":[{"values":{"allowedValues":["in:us-locations"]}}]}`)}
	created := w.send(adminToken, http.MethodPost, "/v2/"+testFolder+"/policies", folderPolicy, http.StatusOK)
	spec, _ := created["spec"].(map[string]any)
	if text(created, "name") != testFolder+locations || text(created, "etag") == "" || spec["updateTime"] == nil {
		t.Fatalf("created = %v", created)
	}
	projectPolicy := map[string]any{"name": "projects/" + testProject + locations,
		"spec": rules(t, `{"inheritFromParent":true,"rules":[{"values":{"allowedValues":["europe-west1"]}}]}`)}
	return w.send(adminToken, http.MethodPost, "/v2/projects/"+testProject+"/policies", projectPolicy, http.StatusOK)
}

func TestPolicyInheritance(t *testing.T) {
	w := newWorld(t)
	// By ID or number, the name is the number.
	if stored := createInherited(t, w); text(stored, "name") != "projects/"+testNumber+locations {
		t.Fatalf("project policy name = %v", stored["name"])
	}
	effective := w.send(viewerToken, http.MethodGet, "/v2/projects/"+testNumber+locations+":getEffectivePolicy", nil, http.StatusOK)
	if got := encode(t, effective); got != `{"name":"projects/702/policies/gcp.resourceLocations","spec":{"rules":[{"values":{"allowedValues":["in:us-locations","europe-west1"]}}]}}` {
		t.Fatalf("effective = %s", got)
	}
	listed := w.send(viewerToken, http.MethodGet, "/v2/projects/"+testProject+"/policies", nil, http.StatusOK)
	if items, _ := listed["policies"].([]any); len(items) != 1 {
		t.Fatalf("listed = %v", listed)
	}
}

func TestPolicyPatchAndDelete(t *testing.T) {
	w := newWorld(t)
	stored := createInherited(t, w)
	// A stale etag is ABORTED; an updateMask replaces only its field.
	path := "/v2/projects/" + testProject + locations
	stale := map[string]any{"etag": "stale", "dryRunSpec": rules(t, `{"reset":true}`)}
	if reply := w.send(adminToken, http.MethodPatch, path+"?updateMask=dryRunSpec", stale, http.StatusConflict); !strings.Contains(message(reply), "etag") {
		t.Fatalf("stale etag = %v", reply)
	}
	stale["etag"] = stored["etag"]
	patched := w.send(adminToken, http.MethodPatch, path+"?updateMask=policy.dry_run_spec", stale, http.StatusOK)
	if patched["spec"] == nil || patched["dryRunSpec"] == nil || text(patched, "etag") == text(stored, "etag") {
		t.Fatalf("patched = %v", patched)
	}
	w.send(adminToken, http.MethodDelete, path+"?etag="+text(patched, "etag"), nil, http.StatusOK)
	missing := w.send(viewerToken, http.MethodGet, path, nil, http.StatusNotFound)
	if message(missing) != "A Policy of constraint constraints/gcp.resourceLocations on resource projects/702 does not exist." {
		t.Fatalf("missing = %v", missing)
	}
	if listed := w.send(viewerToken, http.MethodGet, "/v2/projects/"+testProject+"/policies", nil, http.StatusOK); len(listed) != 0 {
		t.Fatalf("empty list = %v", listed)
	}
	// The folder's policy still applies below it, now without the project's value.
	effective := w.send(viewerToken, http.MethodGet, path+":getEffectivePolicy", nil, http.StatusOK)
	if got := encode(t, effective["spec"]); got != `{"rules":[{"values":{"allowedValues":["in:us-locations"]}}]}` {
		t.Fatalf("effective after delete = %s", got)
	}
}

func TestPolicyWriteRefusals(t *testing.T) {
	w := newWorld(t)
	base := "/v2/projects/" + testProject + "/policies"
	valid := map[string]any{"name": "projects/" + testProject + "/policies/storage.uniformBucketLevelAccess", "spec": rules(t, `{"rules":[{"enforce":true}]}`)}
	w.send(adminToken, http.MethodPost, base, valid, http.StatusOK)
	if reply := w.send(adminToken, http.MethodPost, base, valid, http.StatusConflict); !strings.Contains(message(reply), "already exists") {
		t.Fatalf("duplicate = %v", reply)
	}
	cases := []struct {
		body   map[string]any
		status int
		text   string
	}{
		{map[string]any{"name": testFolder + "/policies/storage.publicAccessPrevention", "spec": rules(t, `{"rules":[{"enforce":true}]}`)}, http.StatusBadRequest, "must name a policy on the parent"},
		{map[string]any{"name": "projects/" + testProject + "/policies/compute.vmExternalIpAccess", "spec": rules(t, `{"rules":[{"denyAll":true}]}`)}, http.StatusNotImplemented, "is not enforced locally"},
		{map[string]any{"name": "projects/" + testProject + locations, "spec": rules(t, `{"rules":[{"enforce":true}]}`)}, http.StatusBadRequest, "enforce applies only to boolean"},
		{map[string]any{"name": "projects/" + testProject + locations, "bogus": 1}, http.StatusBadRequest, "Unknown name"},
	}
	for _, test := range cases {
		if reply := w.send(adminToken, http.MethodPost, base, test.body, test.status); !strings.Contains(message(reply), test.text) {
			t.Errorf("%v: %v", test.body, reply)
		}
	}
	if reply := w.send(adminToken, http.MethodPatch, "/v2/projects/"+testProject+locations, valid, http.StatusBadRequest); !strings.Contains(message(reply), "must match the request path") {
		t.Fatalf("patch name mismatch = %v", reply)
	}
	if reply := w.send(adminToken, http.MethodPatch, "/v2/projects/"+testProject+"/policies/storage.uniformBucketLevelAccess?updateMask=alternate", valid, http.StatusNotImplemented); !strings.Contains(message(reply), "updateMask field alternate") {
		t.Fatalf("patch mask = %v", reply)
	}
	if reply := w.send(adminToken, http.MethodGet, "/v2/projects/"+testProject+"/policies/compute.requireOsLogin:getEffectivePolicy", nil, http.StatusNotImplemented); !strings.Contains(message(reply), "not enforced locally") {
		t.Fatalf("unenforced effective = %v", reply)
	}
}

func TestDenialsNameTheRecordedResources(t *testing.T) {
	w := newWorld(t)
	cases := map[string]string{
		"/v2/projects/" + testProject + "/policies":                        "Permission 'orgpolicy.policies.list' denied on resource '//cloudresourcemanager.googleapis.com/projects/op-project' (or it may not exist).",
		"/v2/" + testOrg + "/constraints":                                  "Permission 'orgpolicy.constraints.list' denied on resource '//cloudresourcemanager.googleapis.com/organizations/700' (or it may not exist).",
		"/v2/projects/" + testProject + locations + ":getEffectivePolicy":  "Permission 'orgpolicy.policy.get' denied on resource '//orgpolicy.googleapis.com/projects/op-project/policies/gcp.resourceLocations' (or it may not exist).",
		"/v2/projects/missing-project" + locations + ":getEffectivePolicy": "Permission 'orgpolicy.policy.get' denied on resource '//orgpolicy.googleapis.com/projects/missing-project/policies/gcp.resourceLocations' (or it may not exist).",
	}
	for path, want := range cases {
		if got := message(w.send(readerToken, http.MethodGet, path, nil, http.StatusForbidden)); got != want {
			t.Errorf("%s: %s", path, got)
		}
	}
	// The viewer reads; the viewer cannot write.
	w.send(viewerToken, http.MethodPost, "/v2/projects/"+testProject+"/policies",
		map[string]any{"name": "projects/" + testProject + locations, "spec": rules(t, `{"reset":true}`)}, http.StatusForbidden)
	constraints := w.send(viewerToken, http.MethodGet, "/v2/projects/"+testNumber+"/constraints?pageSize=1&pageToken=ignored", nil, http.StatusOK)
	items, _ := constraints["constraints"].([]any)
	first, _ := items[0].(map[string]any)
	if len(items) != 6 || first["name"] != "projects/702/constraints/gcp.resourceLocations" {
		t.Fatalf("constraints = %v", constraints)
	}
}

// client dials the world's gRPC server with the Go Organization Policy
// client as admin.
func (w *world) client() *orgpolicyapi.Client {
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
	client, err := orgpolicyapi.NewClient(context.Background(), option.WithGRPCConn(conn))
	if err != nil {
		w.t.Fatal(err)
	}
	return client
}

// TestGoClientOverGRPC writes and reads policies with the Go Organization
// Policy client, through the gateway's gRPC transcoding.
func TestGoClientOverGRPC(t *testing.T) {
	w := newWorld(t)
	ctx, client := context.Background(), w.client()
	name := testOrg + "/policies/storage.publicAccessPrevention"
	enforce := &orgpolicypb.PolicySpec{Rules: []*orgpolicypb.PolicySpec_PolicyRule{{Kind: &orgpolicypb.PolicySpec_PolicyRule_Enforce{Enforce: true}}}}
	if _, err := client.CreatePolicy(ctx, &orgpolicypb.CreatePolicyRequest{Parent: testOrg, Policy: &orgpolicypb.Policy{Name: name, Spec: enforce}}); err != nil {
		t.Fatal(err)
	}
	effective, err := client.GetEffectivePolicy(ctx, &orgpolicypb.GetEffectivePolicyRequest{Name: "projects/" + testProject + "/policies/storage.publicAccessPrevention"})
	if err != nil || !effective.GetSpec().GetRules()[0].GetEnforce() || effective.GetName() != "projects/702/policies/storage.publicAccessPrevention" {
		t.Fatalf("effective = %v, %v", effective, err)
	}
	if enforced, err := w.engine.OrgPolicyEnforced(nodes+"projects/"+testProject, "storage.publicAccessPrevention"); err != nil || !enforced {
		t.Fatalf("the engine does not enforce the written policy: %v %v", enforced, err)
	}
	if err := client.DeletePolicy(ctx, &orgpolicypb.DeletePolicyRequest{Name: name}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetPolicy(ctx, &orgpolicypb.GetPolicyRequest{Name: name}); status.Code(err) != codes.NotFound {
		t.Fatalf("deleted policy = %v", err)
	}
	if _, err := client.GetCustomConstraint(ctx, &orgpolicypb.GetCustomConstraintRequest{Name: testOrg + "/customConstraints/custom.x"}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("custom constraints = %v", err)
	}
}

// tokenCredentials sends a bearer token over plaintext loopback.
type tokenCredentials string

func (c tokenCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + string(c)}, nil
}

func (tokenCredentials) RequireTransportSecurity() bool { return false }

// TestConcurrentPolicyWrites races creates, patches, reads, and deletes of
// one policy; run it under -race.
func TestConcurrentPolicyWrites(t *testing.T) {
	w := newWorld(t)
	path := "/v2/projects/" + testProject + "/policies"
	body := map[string]any{"name": "projects/" + testProject + locations, "spec": map[string]any{"rules": []any{map[string]any{"allowAll": true}}}}
	providertest.Hammer(t, w.gateway, adminToken, 8, 20, func(int) []providertest.Request {
		return []providertest.Request{
			{Method: http.MethodPost, Path: path, Body: body, Want: []int{200, 409}},
			{Method: http.MethodPatch, Path: path + "/gcp.resourceLocations", Body: body, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: path + "/gcp.resourceLocations:getEffectivePolicy", Want: []int{200}},
			{Method: http.MethodGet, Path: path, Want: []int{200}},
			{Method: http.MethodDelete, Path: path + "/gcp.resourceLocations", Want: []int{200, 404}},
		}
	})
}
