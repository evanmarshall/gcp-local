package cloudkms

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

// Names and keys repeated across the lifecycle tests.
const (
	testProject       = "demo-kms"
	testRing          = "ring-1"
	testKey           = "key-1"
	testKeyTwo        = "key-2"
	testVersionOne    = "1"
	testToken         = "actor"
	testRingsPath     = "/v1/projects/" + testProject + "/locations/us-central1/keyRings"
	testRingPath      = testRingsPath + "/" + testRing
	testKeyPath       = testRingPath + "/cryptoKeys"
	testActor         = "serviceAccount:actor@" + testProject + ".iam.gserviceaccount.com"
	testResourceAad   = "additionalAuthenticatedData"
	keyTestPlaintext  = "plaintext"
	keyTestCiphertext = "ciphertext"
)

// harness drives the provider through the gateway.
type harness struct {
	t     *testing.T
	gate  *gateway.Gateway
	index *state.Index
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
		Bindings: []iam.Binding{
			{Role: "roles/cloudkms.admin", Members: []string{testActor}},
		},
	}); err != nil {
		t.Fatalf("grant the project: %v", err)
	}
	_, err = New(context.Background(), registry, backend.NewManager(time.Second, time.Second), index, engine)
	if err != nil {
		t.Fatalf("build the kms provider: %v", err)
	}
	g := gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:" + token + "@" + testProject + ".iam.gserviceaccount.com", Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})
	return &harness{t: t, gate: g, index: index}
}

// invoke issues one REST request as the fixture principal.
func (h *harness) invoke(verb, path string, body any) *httptest.ResponseRecorder {
	h.t.Helper()
	var encoded []byte
	if body != nil {
		encoded, _ = json.Marshal(body) //nolint:errchkjson // plain map bodies
	}
	request, _ := http.NewRequestWithContext(context.Background(), verb, path, bytes.NewReader(encoded))
	if encoded != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Authorization", "Bearer "+testToken)
	recorder := httptest.NewRecorder()
	h.gate.ServeHTTP(recorder, request)
	return recorder
}

// ring creates the fixture key ring.
func (h *harness) ring() {
	h.t.Helper()
	recorder := h.invoke(http.MethodPost, testRingsPath+"?keyRingId="+testRing, map[string]any{})
	if recorder.Code != 200 {
		h.t.Fatalf("key ring create status = %d body %s", recorder.Code, recorder.Body.String())
	}
}

// key creates the fixture crypto key inside the fixture ring.
func (h *harness) key() {
	h.t.Helper()
	h.ring()
	recorder := h.invoke(http.MethodPost, testKeyPath+"?cryptoKeyId="+testKey,
		map[string]any{"purpose": purposeEncryptDecrypt})
	if recorder.Code != 200 {
		h.t.Fatalf("crypto key create status = %d body %s", recorder.Code, recorder.Body.String())
	}
}

func TestKeyRingAndCryptoKeyLifecycle(t *testing.T) {
	h := newHarness(t)
	h.key()
	recorder := h.invoke(http.MethodGet, testRingPath, nil)
	if recorder.Code != 200 {
		t.Fatalf("key ring get status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodGet, testRingPath+"/cryptoKeys/"+testKey, nil)
	if recorder.Code != 200 {
		t.Fatalf("crypto key get status = %d", recorder.Code)
	}
	var key struct {
		Primary struct {
			Name  string `json:"name"`
			State string `json:"state"`
		} `json:"primary"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &key); err != nil {
		t.Fatalf("decode key: %v", err)
	}
	if key.Primary.State != stateEnabled {
		t.Fatalf("primary = %+v", key.Primary)
	}
	// Duplicate creation is refused.
	recorder = h.invoke(http.MethodPost, testRingsPath+"?keyRingId="+testRing, map[string]any{})
	if recorder.Code != 409 {
		t.Fatalf("duplicate ring status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodGet, testRingPath, nil)
	if recorder.Code != 200 {
		t.Fatalf("ring list status = %d", recorder.Code)
	}
}

func TestEncryptDecryptRoundTrip(t *testing.T) {
	h := newHarness(t)
	h.key()
	plaintext := base64.StdEncoding.EncodeToString([]byte("payload-value"))
	recorder := h.invoke(http.MethodPost, testKeyPath+"/"+testKey+":encrypt",
		map[string]any{keyTestPlaintext: plaintext})
	if recorder.Code != 200 {
		t.Fatalf("encrypt status = %d body %s", recorder.Code, recorder.Body.String())
	}
	var sealed struct {
		Ciphertext string `json:"ciphertext"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &sealed); err != nil {
		t.Fatalf("decode ciphertext: %v", err)
	}
	recorder = h.invoke(http.MethodPost, testKeyPath+"/"+testKey+":decrypt",
		map[string]any{keyTestCiphertext: sealed.Ciphertext})
	if recorder.Code != 200 {
		t.Fatalf("decrypt status = %d body %s", recorder.Code, recorder.Body.String())
	}
	var opened struct {
		Plaintext string `json:"plaintext"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &opened); err != nil {
		t.Fatalf("decode plaintext: %v", err)
	}
	decoded, decodeErr := base64.StdEncoding.DecodeString(opened.Plaintext)
	if decodeErr != nil || string(decoded) != "payload-value" {
		t.Fatalf("round trip mismatch: %q %v", opened.Plaintext, decodeErr)
	}
	// AAD binding: encrypting with AAD and decrypting without fails closed.
	recorder = h.invoke(http.MethodPost, testKeyPath+"/"+testKey+":encrypt",
		map[string]any{keyTestPlaintext: plaintext, testResourceAad: "YQ=="})
	var bound struct {
		Ciphertext string `json:"ciphertext"`
	}
	_ = json.Unmarshal(recorder.Body.Bytes(), &bound)
	recorder = h.invoke(http.MethodPost, testKeyPath+"/"+testKey+":decrypt",
		map[string]any{keyTestCiphertext: bound.Ciphertext})
	if recorder.Code != 412 && recorder.Code != 400 {
		t.Fatalf("missing AAD decrypt status = %d", recorder.Code)
	}
}

func TestVersionStateTransitionsGateCrypto(t *testing.T) {
	h := newHarness(t)
	h.key()
	recorder := h.invoke(http.MethodPost, testKeyPath+"/"+testKey+"/cryptoKeyVersions/"+testVersionOne+":disable", map[string]any{})
	if recorder.Code != 200 {
		t.Fatalf("disable status = %d body %s", recorder.Code, recorder.Body.String())
	}
	// Encrypt with a disabled primary fails closed.
	recorder = h.invoke(http.MethodPost, testKeyPath+"/"+testKey+":encrypt",
		map[string]any{keyTestPlaintext: base64.StdEncoding.EncodeToString([]byte("x"))})
	if recorder.Code != 400 && recorder.Code != 412 {
		t.Fatalf("disabled primary encrypt status = %d", recorder.Code)
	}
	// A destroyed version is final.
	h.invoke(http.MethodPost, testKeyPath+"/"+testKey+"/cryptoKeyVersions/"+testVersionOne+":enable", map[string]any{})
	recorder = h.invoke(http.MethodPost, testKeyPath+"/"+testKey+"/cryptoKeyVersions/"+testVersionOne+":destroy", map[string]any{})
	if recorder.Code != 200 {
		t.Fatalf("destroy status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodPost, testKeyPath+"/"+testKey+"/cryptoKeyVersions/"+testVersionOne+":enable", map[string]any{})
	if recorder.Code != 400 {
		t.Fatalf("re-enable destroyed status = %d body %s", recorder.Code, recorder.Body.String())
	}
}

func TestVersionedEncryptDecryptAcrossVersions(t *testing.T) {
	h := newHarness(t)
	h.key()
	// Version 2 gets fresh material. The real surface keeps the primary
	// version until an explicit update, so move it before encrypting.
	recorder := h.invoke(http.MethodPost, testKeyPath+"/"+testKey+"/cryptoKeyVersions", map[string]any{})
	if recorder.Code != 200 {
		t.Fatalf("version create status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = h.invoke(http.MethodPatch, testKeyPath+"/"+testKey, map[string]any{"primary": "2"})
	if recorder.Code != 200 {
		t.Fatalf("primary update status = %d body %s", recorder.Code, recorder.Body.String())
	}
	plaintext := base64.StdEncoding.EncodeToString([]byte("second"))
	recorder = h.invoke(http.MethodPost, testKeyPath+"/"+testKey+":encrypt", map[string]any{keyTestPlaintext: plaintext})
	var sealed struct {
		Name       string `json:"name"`
		Ciphertext string `json:"ciphertext"`
	}
	_ = json.Unmarshal(recorder.Body.Bytes(), &sealed)
	if !strings.HasSuffix(sealed.Name, "/cryptoKeyVersions/2") {
		t.Fatalf("encrypt used %s", sealed.Name)
	}
	recorder = h.invoke(http.MethodPost, testKeyPath+"/"+testKey+":decrypt", map[string]any{keyTestCiphertext: sealed.Ciphertext})
	var opened struct {
		Used string `json:"usedCiphertextVersionName"`
	}
	_ = json.Unmarshal(recorder.Body.Bytes(), &opened)
	if !strings.HasSuffix(opened.Used, "/cryptoKeyVersions/2") {
		t.Fatalf("decrypt used %s", opened.Used)
	}
}

func TestKeyRingIamPolicy(t *testing.T) {
	h := newHarness(t)
	h.ring()
	recorder := h.invoke(http.MethodGet, testRingPath+":getIamPolicy", nil)
	if recorder.Code != 200 {
		t.Fatalf("getIam status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodPost, testRingPath+":setIamPolicy", map[string]any{
		"policy": iam.Policy{Version: 3, Bindings: []iam.Binding{
			{Role: "roles/cloudkms.cryptoKeyEncrypterDecrypter", Members: []string{"serviceAccount:app@other.iam.gserviceaccount.com"}},
		}},
	})
	if recorder.Code != 200 {
		t.Fatalf("setIam status = %d body %s", recorder.Code, recorder.Body.String())
	}
}

func TestEncryptRefusedWithoutPermission(t *testing.T) {
	h := newHarness(t)
	h.key()
	// A viewer-only principal can read but not use the key.
	recorder := h.invoke(http.MethodPost, testRingPath+":setIamPolicy", map[string]any{
		"policy": iam.Policy{Version: 3, Bindings: []iam.Binding{
			{Role: "roles/cloudkms.viewer", Members: []string{"serviceAccount:viewer@" + testProject + ".iam.gserviceaccount.com"}},
		}},
	})
	if recorder.Code != 200 {
		t.Fatalf("setIam status = %d", recorder.Code)
	}
	request, _ := http.NewRequestWithContext(context.Background(), http.MethodPost,
		testKeyPath+"/"+testKey+":encrypt", bytes.NewReader([]byte(`{"plaintext":"eA=="}`)))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer viewer")
	recorder = httptest.NewRecorder()
	h.gate.ServeHTTP(recorder, request)
	if recorder.Code != 403 {
		t.Fatalf("viewer encrypt status = %d body %s", recorder.Code, recorder.Body.String())
	}
}

func TestEveryRouteIsHandled(t *testing.T) {
	h := newHarness(t)
	h.key()
	gap := "this method is not implemented"
	probe := func(verb, path string) {
		recorder := h.invoke(verb, path, nil)
		if recorder.Code == 501 && strings.Contains(recorder.Body.String(), gap) {
			t.Fatalf("%s %s hit the dispatch gap", verb, path)
		}
	}
	probe(http.MethodGet, "/v1/projects/"+testProject+"/locations/us-central1")
	probe(http.MethodGet, testRingsPath)
	probe(http.MethodGet, testRingsPath)
	probe(http.MethodGet, testRingPath+":getIamPolicy")
	probe(http.MethodGet, testRingPath+"/cryptoKeys/"+testKey)
	probe(http.MethodGet, testKeyPath)
	probe(http.MethodGet, testRingPath+"/cryptoKeys/"+testKey+":getIamPolicy")
	probe(http.MethodGet, testRingPath+"/cryptoKeys/"+testKey+"/cryptoKeyVersions")
	probe(http.MethodGet, testRingPath+"/cryptoKeys/"+testKey+"/cryptoKeyVersions/"+testVersionOne)
}
