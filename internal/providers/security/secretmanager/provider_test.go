package secretmanager

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// Test names and keys repeated across the lifecycle tests.
const (
	testProject  = "demo-secret"
	testSecret   = "tokens"
	testAccessor = "accessor"
)

// harness drives the provider through the gateway.
type harness struct {
	t        *testing.T
	gate     *gateway.Gateway
	provider *Provider
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
	g := gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:" + token + "@" + testProject + ".iam.gserviceaccount.com", Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})
	return &harness{t: t, gate: g, provider: provider}
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
	if actor != "" {
		request.Header.Set("Authorization", "Bearer "+actor)
	}
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
		"payload": map[string]any{"data": base64Of(payload)},
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
		"payload": map[string]any{"data": base64Of("destroy-me")},
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
		"labels": map[string]any{"env": "synthetic"},
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

func TestVersionDisableBlocksAccessUntilReenabled(t *testing.T) {
	h := newHarness(t)
	h.create(replicationAuto())
	h.addVersion(t, "disable-me")
	grantAccessor(t, h)
	recorder := h.invoke(http.MethodPost, "/v1/projects/"+testProject+"/secrets/"+testSecret+"/versions/1:disable", map[string]any{}, "actor")
	if recorder.Code != 200 {
		t.Fatalf("disable status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodGet, "/v1/projects/"+testProject+"/secrets/"+testSecret+"/versions/1:access", nil, testAccessor)
	if recorder.Code != 400 {
		t.Fatalf("disabled access status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodPost, "/v1/projects/"+testProject+"/secrets/"+testSecret+"/versions/1:enable", map[string]any{}, "actor")
	if recorder.Code != 200 {
		t.Fatalf("enable status = %d body %s", recorder.Code, recorder.Body.String())
	}
	assertPayloadAccess(t, h, "disable-me")
}

func TestEveryRouteIsHandled(t *testing.T) {
	h := newHarness(t)
	h.create(replicationAuto())
	gap := "this method is not implemented"
	probe := func(verb, path string) {
		recorder := h.invoke(verb, path, nil, "actor")
		if recorder.Code == 501 && strings.Contains(recorder.Body.String(), gap) {
			t.Fatalf("%s %s hit the dispatch gap", verb, path)
		}
	}
	probe(http.MethodGet, "/v1/projects/"+testProject+"/secrets/"+testSecret)
	probe(http.MethodGet, "/v1/projects/"+testProject+"/secrets")
	probe(http.MethodGet, "/v1/projects/"+testProject+"/secrets/"+testSecret+":getIamPolicy")
	probe(http.MethodGet, "/v1/projects/"+testProject+"/secrets/"+testSecret+"/versions")
	probe(http.MethodGet, "/v1/projects/"+testProject+"/locations")
}

// TestRecordedWireContract pins the shapes the secretmanager-access-lifecycle
// recording observed: number-form names, labels, etags, CRC32C, the latest
// alias, the 64 KiB limit, and FAILED_PRECONDITION for destroyed access.
func TestRecordedWireContract(t *testing.T) {
	h := newHarness(t)
	secret := "/v1/projects/" + testProject + "/secrets/" + testSecret
	recorder := h.invoke(http.MethodPost, "/v1/projects/"+testProject+"/secrets?secretId="+testSecret, map[string]any{
		"replication": replicationAuto(), "labels": map[string]any{"managed-by": "gcp-local"},
	}, "actor")
	var created map[string]any
	_ = json.Unmarshal(recorder.Body.Bytes(), &created)
	if recorder.Code != 200 || created["name"] != "projects/123456/secrets/"+testSecret || created["etag"] == nil {
		t.Fatalf("create = %d %v", recorder.Code, created)
	}
	if labels, _ := created["labels"].(map[string]any); labels["managed-by"] != "gcp-local" {
		t.Fatalf("labels were not stored: %v", created)
	}
	h.addVersion(t, "hello")
	checkRecordedAccess(t, h, secret)
}

// checkRecordedAccess covers the access, limit, and destroy observations.
func checkRecordedAccess(t *testing.T, h *harness, secret string) {
	t.Helper()
	recorder := h.invoke(http.MethodGet, "/v1/projects/123456/secrets/"+testSecret+"/versions/latest:access", nil, "actor")
	var accessed struct {
		Name    string            `json:"name"`
		Payload map[string]string `json:"payload"`
	}
	_ = json.Unmarshal(recorder.Body.Bytes(), &accessed)
	// CRC32C (Castagnoli) of "hello".
	if recorder.Code != 200 || accessed.Name != "projects/123456/secrets/"+testSecret+"/versions/1" || accessed.Payload["dataCrc32c"] != "2591144780" {
		t.Fatalf("number-path latest access = %d %s", recorder.Code, recorder.Body.String())
	}
	oversize := h.invoke(http.MethodPost, secret+":addVersion", map[string]any{
		"payload": map[string]any{"data": base64Of(strings.Repeat("x", 64<<10+1))},
	}, "actor")
	if oversize.Code != 400 {
		t.Fatalf("oversize payload = %d", oversize.Code)
	}
	if code := h.invoke(http.MethodPost, secret+"/versions/1:destroy", nil, "actor").Code; code != 200 {
		t.Fatalf("destroy = %d", code)
	}
	destroyed := h.invoke(http.MethodGet, secret+"/versions/1:access", nil, "actor")
	if destroyed.Code != 400 || !strings.Contains(destroyed.Body.String(), "FAILED_PRECONDITION") {
		t.Fatalf("destroyed access = %d %s", destroyed.Code, destroyed.Body.String())
	}
}

// TestVersionsOrderNumerically pins numeric version order past nine
// versions: list is newest-first and latest is the highest enabled number.
func TestVersionsOrderNumerically(t *testing.T) {
	h := newHarness(t)
	h.create(replicationAuto())
	for range 11 {
		h.addVersion(t, "payload")
	}
	secret := "/v1/projects/" + testProject + "/secrets/" + testSecret
	recorder := h.invoke(http.MethodGet, secret+"/versions/latest", nil, "actor")
	if recorder.Code != 200 || !strings.HasSuffix(decodeName(t, recorder), "/versions/11") {
		t.Fatalf("latest = %d %s", recorder.Code, recorder.Body.String())
	}
	var listing struct {
		Versions []struct {
			Name string `json:"name"`
		} `json:"versions"`
	}
	if err := json.Unmarshal(h.invoke(http.MethodGet, secret+"/versions", nil, "actor").Body.Bytes(), &listing); err != nil {
		t.Fatalf("decode listing: %v", err)
	}
	if len(listing.Versions) != 11 || !strings.HasSuffix(listing.Versions[0].Name, "/versions/11") ||
		!strings.HasSuffix(listing.Versions[1].Name, "/versions/10") || !strings.HasSuffix(listing.Versions[10].Name, "/versions/1") {
		t.Fatalf("versions are not newest-first: %v", listing.Versions)
	}
}

// decodeName reads the name field of one response body.
func decodeName(t *testing.T, recorder *httptest.ResponseRecorder) string {
	t.Helper()
	var document struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &document); err != nil {
		t.Fatalf("decode name: %v", err)
	}
	return document.Name
}

// TestDeniedCallerLearnsNothing follows the secretmanager-access-lifecycle
// recording: a caller without permission gets 403 whether or not the secret
// or version exists, and only a permitted caller sees 404.
func TestDeniedCallerLearnsNothing(t *testing.T) {
	h := newHarness(t)
	secret := "/v1/projects/" + testProject + "/secrets/" + testSecret
	missing := "/v1/projects/" + testProject + "/secrets/absent"
	for _, path := range []string{missing, missing + "/versions/latest:access", missing + ":getIamPolicy"} {
		if code := h.invoke(http.MethodGet, path, nil, "outsider").Code; code != 403 {
			t.Fatalf("outsider %s = %d, want 403", path, code)
		}
		if code := h.invoke(http.MethodGet, path, nil, "actor").Code; code != 404 {
			t.Fatalf("actor %s = %d, want 404", path, code)
		}
	}
	h.create(replicationAuto())
	for _, path := range []string{secret, secret + "/versions/99:access"} {
		if code := h.invoke(http.MethodGet, path, nil, "outsider").Code; code != 403 {
			t.Fatalf("outsider %s = %d, want 403", path, code)
		}
	}
	if code := h.invoke(http.MethodGet, secret+"/versions/99:access", nil, "actor").Code; code != 404 {
		t.Fatalf("actor missing version = %d, want 404", code)
	}
}
