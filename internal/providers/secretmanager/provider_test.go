package secretmanager

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// Test names and keys repeated across the lifecycle tests.
const (
	testProject    = "demo-secret"
	testSecret     = "tokens"
	testAccessor   = "accessor"
	testKeyData    = "data"
	testKeyPayload = "payload"
	testKeyLabels  = "labels"
)

// harness drives the provider through the gateway.
type harness struct {
	t    *testing.T
	gate *gateway.Gateway
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	registry := gateway.NewRegistry()
	index := state.NewIndex()
	engine, err := iam.New(index)
	if err != nil {
		t.Fatalf("build the iam engine: %v", err)
	}
	if err := index.Put("resourcemanager", state.Resource{
		Name: "//cloudresourcemanager.googleapis.com/projects/" + testProject, Service: "resourcemanager",
		Kind: "project", Number: "123456", Scope: state.Scope{Project: testProject, Location: state.Global},
	}); err != nil {
		t.Fatalf("register the project: %v", err)
	}
	if _, err := engine.SetPolicy("//cloudresourcemanager.googleapis.com/projects/"+testProject, iam.Policy{
		Bindings: []iam.Binding{{Role: "roles/secretmanager.admin", Members: []string{"serviceAccount:actor@" + testProject + ".iam.gserviceaccount.com"}}},
	}); err != nil {
		t.Fatalf("grant the project: %v", err)
	}
	provider, err := New(context.Background(), registry, backend.NewManager(time.Second, time.Second), index, engine)
	if err != nil {
		t.Fatalf("build the secretmanager provider: %v", err)
	}
	_ = provider
	g := gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:" + token + "@" + testProject + ".iam.gserviceaccount.com", Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})
	return &harness{t: t, gate: g}
}

// invoke issues one REST request as the named fixture principal.
func (h *harness) invoke(verb, path string, body any, actor string) *httptest.ResponseRecorder {
	h.t.Helper()
	var encoded []byte
	if body != nil {
		encoded, _ = json.Marshal(body) //nolint:errchkjson // plain map bodies
	}
	request, _ := http.NewRequestWithContext(context.Background(), verb, path, bytes.NewReader(encoded))
	if encoded != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Authorization", "Bearer "+actor)
	recorder := httptest.NewRecorder()
	h.gate.ServeHTTP(recorder, request)
	return recorder
}

// base64Of encodes one payload for the wire envelope.
func base64Of(payload string) string { return base64.StdEncoding.EncodeToString([]byte(payload)) }

// create posts the canonical create request and requires success.
func (h *harness) create(replication map[string]any) {
	recorder := h.invoke(http.MethodPost, "/v1/projects/"+testProject+"/secrets", map[string]any{
		"name":        "projects/" + testProject + "/secrets/" + testSecret,
		"replication": replication,
	}, "actor")
	if recorder.Code != 200 {
		h.t.Fatalf("secret create status = %d body %s", recorder.Code, recorder.Body.String())
	}
}

// replicationAuto is the automatic replication policy shape.
func replicationAuto() map[string]any {
	return map[string]any{"automatic": map[string]any{}}
}

// addVersion stores one payload version as the actor.
func (h *harness) addVersion(t *testing.T, payload string) {
	t.Helper()
	recorder := h.invoke(http.MethodPost, "/v1/projects/"+testProject+"/secrets/"+testSecret+":addVersion", map[string]any{
		testKeyPayload: map[string]any{testKeyData: base64Of(payload)},
	}, "actor")
	if recorder.Code != 200 {
		t.Fatalf("addVersion status = %d body %s", recorder.Code, recorder.Body.String())
	}
	var version struct {
		State string `json:"state"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &version); err != nil {
		t.Fatalf("decode version: %v", err)
	}
	if version.State != "ENABLED" {
		t.Fatalf("version state = %s", version.State)
	}
}

// assertAccessDeniedWithoutGrant requires a non-granted principal to be refused.
func assertAccessDeniedWithoutGrant(t *testing.T, h *harness) {
	t.Helper()
	recorder := h.invoke(http.MethodGet, "/v1/projects/"+testProject+"/secrets/"+testSecret+"/versions/1:access", nil, "outsider")
	if recorder.Code != 403 {
		t.Fatalf("access without grant status = %d body %s", recorder.Code, recorder.Body.String())
	}
}

// grantAccessor binds the secretAccessor role to the test accessor.
func grantAccessor(t *testing.T, h *harness) {
	t.Helper()
	recorder := h.invoke(http.MethodPost, "/v1/projects/"+testProject+"/secrets/"+testSecret+":setIamPolicy", map[string]any{
		"policy": map[string]any{"bindings": []any{map[string]any{
			"role":    "roles/secretmanager.secretAccessor",
			"members": []any{"serviceAccount:" + testAccessor + "@" + testProject + ".iam.gserviceaccount.com"},
		}}},
	}, "actor")
	if recorder.Code != 200 {
		t.Fatalf("setIamPolicy status = %d body %s", recorder.Code, recorder.Body.String())
	}
}

// assertPayloadAccess reads the payload with the granted accessor.
func assertPayloadAccess(t *testing.T, h *harness, want string) {
	t.Helper()
	recorder := h.invoke(http.MethodGet, "/v1/projects/"+testProject+"/secrets/"+testSecret+"/versions/1:access", nil, testAccessor)
	if recorder.Code != 200 {
		t.Fatalf("access with grant status = %d body %s", recorder.Code, recorder.Body.String())
	}
	var payload struct {
		P struct {
			Data string `json:"data"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	decoded, err := base64.StdEncoding.DecodeString(payload.P.Data)
	if err != nil || string(decoded) != want {
		t.Fatalf("payload = %q", payload.P.Data)
	}
}

// assertAccessBeforeVersionIsNotFound requires a missing version to be 404.
func assertAccessBeforeVersionIsNotFound(t *testing.T, h *harness) {
	t.Helper()
	recorder := h.invoke(http.MethodGet, "/v1/projects/"+testProject+"/secrets/"+testSecret+"/versions/latest:access", nil, "actor")
	if recorder.Code != 404 {
		t.Fatalf("access before version status = %d body %s", recorder.Code, recorder.Body.String())
	}
}

func TestSecretLifecycleVersionsAndPayloadAccess(t *testing.T) {
	h := newHarness(t)
	h.create(replicationAuto())
	assertAccessBeforeVersionIsNotFound(t, h)
	h.addVersion(t, "secret-payload")
	assertAccessDeniedWithoutGrant(t, h)
	grantAccessor(t, h)
	assertPayloadAccess(t, h, "secret-payload")
}

func TestDestroyBoundaryAndMetadataRead(t *testing.T) {
	h := newHarness(t)
	h.create(replicationAuto())
	h.invoke(http.MethodPost, "/v1/projects/"+testProject+"/secrets/"+testSecret+":addVersion", map[string]any{
		testKeyPayload: map[string]any{testKeyData: base64Of("destroy-me")},
	}, "actor")
	recorder := h.invoke(http.MethodPost, "/v1/projects/"+testProject+"/secrets/"+testSecret+"/versions/1:destroy", map[string]any{}, "actor")
	if recorder.Code != 200 {
		t.Fatalf("destroy status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodPost, "/v1/projects/"+testProject+"/secrets/"+testSecret+"/versions/1:destroy", map[string]any{}, "actor")
	if recorder.Code != 400 {
		t.Fatalf("double destroy status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodGet, "/v1/projects/"+testProject+"/secrets/"+testSecret+"/versions/1:access", nil, "actor")
	if recorder.Code != 400 {
		t.Fatalf("destroyed access status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodGet, "/v1/projects/"+testProject+"/secrets/"+testSecret+"/versions/1", nil, "actor")
	if recorder.Code != 200 {
		t.Fatalf("destroyed get status = %d", recorder.Code)
	}
	var version struct {
		State string `json:"state"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &version); err != nil {
		t.Fatalf("decode version: %v", err)
	}
	if version.State != "DESTROYED" {
		t.Fatalf("destroyed state = %s", version.State)
	}
}

func TestListUpdateAndDeleteLifecycle(t *testing.T) {
	h := newHarness(t)
	h.create(replicationAuto())
	recorder := h.invoke(http.MethodPatch, "/v1/projects/"+testProject+"/secrets/"+testSecret, map[string]any{
		testKeyLabels: map[string]any{"env": "synthetic"},
	}, "actor")
	if recorder.Code != 200 {
		t.Fatalf("update status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodGet, "/v1/projects/"+testProject+"/secrets", nil, "actor")
	if recorder.Code != 200 {
		t.Fatalf("list status = %d", recorder.Code)
	}
	var listing struct {
		Secrets []map[string]any `json:"secrets"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &listing); err != nil {
		t.Fatalf("decode listing: %v", err)
	}
	if len(listing.Secrets) != 1 {
		t.Fatalf("listing = %v", listing)
	}
	recorder = h.invoke(http.MethodDelete, "/v1/projects/"+testProject+"/secrets/"+testSecret, nil, "actor")
	if recorder.Code != 200 {
		t.Fatalf("delete status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodGet, "/v1/projects/"+testProject+"/secrets/"+testSecret, nil, "actor")
	if recorder.Code != 404 {
		t.Fatalf("deleted secret get status = %d", recorder.Code)
	}
}
