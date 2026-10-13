package iam

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
)

const (
	testProject = "demo-sa"
	testAccount = "worker"
	testToken   = "actor"
	testBase    = "/v1/projects/" + testProject + "/serviceAccounts"
	testActor   = "serviceAccount:actor@" + testProject + ".iam.gserviceaccount.com"
)

// harness drives the provider through the gateway.
type harness struct {
	t        *testing.T
	gate     *gateway.Gateway
	engine   *iam.Engine
	index    *state.Index
	provider *Provider
	// tokens resolves the access tokens IAM Credentials mints ("gl." handles).
	tokens   *identity.Service
	idTokens *identity.IDTokens
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	return buildHarness(t, true)
}

// buildHarness builds the provider with or without the credential issuers.
func buildHarness(t *testing.T, issuers bool) *harness {
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
			{Role: "roles/iam.serviceAccountAdmin", Members: []string{testActor}},
			// Key management sits in its own role in GCP.
			{Role: "roles/iam.serviceAccountKeyAdmin", Members: []string{testActor}},
		},
	}); err != nil {
		t.Fatalf("grant the project: %v", err)
	}
	tokens, err := identity.New(identity.Config{LifetimeExtension: []string{"extended@" + testProject + ".iam.gserviceaccount.com"}}, nil)
	if err != nil {
		t.Fatalf("build the identity service: %v", err)
	}
	idTokens, err := identity.NewIDTokens(engine, index)
	if err != nil {
		t.Fatalf("build the ID-token keys: %v", err)
	}
	credentials := Credentials{Tokens: tokens, IDTokens: idTokens}
	if !issuers {
		credentials = Credentials{}
	}
	p, err := New(context.Background(), registry, backend.NewManager(time.Second, time.Second), index, engine, credentials)
	if err != nil {
		t.Fatalf("build the provider: %v", err)
	}
	g := gateway.New(registry, func(ctx context.Context, token string) (gateway.Principal, error) {
		if strings.HasPrefix(token, "gl.") {
			return tokens.Resolve(ctx, token)
		}
		return gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:" + token + "@" + testProject + ".iam.gserviceaccount.com", Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})
	return &harness{t: t, gate: g, engine: engine, index: index, provider: p, tokens: tokens, idTokens: idTokens}
}

// expect issues one REST request and fails the test unless it returns want.
func (h *harness) expect(verb, path string, body any, want int) *httptest.ResponseRecorder {
	h.t.Helper()
	recorder := h.invoke(verb, path, body)
	if recorder.Code != want {
		h.t.Fatalf("%s %s status = %d, want %d: %s", verb, path, recorder.Code, want, recorder.Body.String())
	}
	return recorder
}

// invoke issues one REST request as the fixture principal.
func (h *harness) invoke(verb, path string, body any) *httptest.ResponseRecorder {
	h.t.Helper()
	return h.invokeAs(testToken, verb, path, body)
}

// invokeAs issues one REST request as the principal a token names.
func (h *harness) invokeAs(token, verb, path string, body any) *httptest.ResponseRecorder {
	h.t.Helper()
	var encoded []byte
	if body != nil {
		var err error
		if encoded, err = json.Marshal(body); err != nil {
			h.t.Fatalf("encode the request body: %v", err)
		}
	}
	request, _ := http.NewRequestWithContext(context.Background(), verb, path, bytes.NewReader(encoded))
	if encoded != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	h.gate.ServeHTTP(recorder, request)
	return recorder
}

func TestAccountAndKeyLifecycle(t *testing.T) {
	h := newHarness(t)
	h.expect(http.MethodPost, testBase, map[string]any{
		"accountId": testAccount, "serviceAccount": map[string]any{"displayName": "worker"},
	}, 200)
	h.expect(http.MethodGet, testBase+"/"+testAccount, nil, 200)
	// The projects/- wildcard names the account by email, as gcloud sends it.
	h.expect(http.MethodGet, "/v1/projects/-/serviceAccounts/"+accountEmail(testProject, testAccount), nil, 200)
	h.expect(http.MethodGet, testBase, nil, 200)
	h.expect(http.MethodPatch, testBase+"/"+testAccount, map[string]any{
		"serviceAccount": map[string]any{"displayName": "renamed", "description": "worker account"},
		"updateMask":     "displayName,description",
	}, 200)
	recorder := h.expect(http.MethodPost, testBase+"/"+testAccount+"/keys", map[string]any{}, 200)
	var created struct {
		Name           string `json:"name"`
		PrivateKeyData string `json:"privateKeyData"`
	}
	if json.Unmarshal(recorder.Body.Bytes(), &created) != nil || created.Name == "" || created.PrivateKeyData == "" {
		t.Fatalf("unexpected key body %s", recorder.Body.String())
	}
	h.expect(http.MethodGet, testBase+"/"+testAccount+"/keys", nil, 200)
	h.expect(http.MethodDelete, testBase+"/"+testAccount+"/keys/"+created.Name[strings.LastIndex(created.Name, "/")+1:], nil, 200)
	h.expect(http.MethodDelete, testBase+"/"+testAccount, nil, 200)
	h.expect(http.MethodGet, testBase+"/"+testAccount, nil, 404)
}

// TestCreatedAccountIndexesItsUniqueID proves a created account is published
// with its numeric identity, which ID-token minting requires as the subject.
func TestCreatedAccountIndexesItsUniqueID(t *testing.T) {
	h := newHarness(t)
	recorder := h.invoke(http.MethodPost, testBase, map[string]any{"accountId": testAccount})
	var created struct {
		UniqueID string `json:"uniqueId"`
	}
	if recorder.Code != 200 || json.Unmarshal(recorder.Body.Bytes(), &created) != nil {
		t.Fatalf("account create status = %d body %s", recorder.Code, recorder.Body.String())
	}
	resource, ok := h.index.Get(accountResource(testProject, accountEmail(testProject, testAccount)))
	if !ok || resource.Number == "" || resource.Number != created.UniqueID {
		t.Fatalf("indexed account = %+v, unique id %q", resource, created.UniqueID)
	}
}

func TestUnknownAndDeniedPaths(t *testing.T) {
	h := newHarness(t)
	recorder := h.invoke(http.MethodGet, testBase+"/missing", nil)
	if recorder.Code != 404 {
		t.Fatalf("unknown account get status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodGet, testBase+"/Bad_Upper", nil)
	if recorder.Code != 400 {
		t.Fatalf("invalid account id status = %d", recorder.Code)
	}
	recorder = h.invoke(http.MethodPost, testBase, map[string]any{"accountId": testAccount})
	if recorder.Code != 200 {
		t.Fatalf("create before role switch status = %d", recorder.Code)
	}
	if _, err := h.engine.SetPolicy("//cloudresourcemanager.googleapis.com/projects/"+testProject, iam.Policy{
		Bindings: []iam.Binding{{Role: "roles/iam.serviceAccountViewer", Members: []string{testActor}}},
	}); err != nil {
		t.Fatalf("switch the role: %v", err)
	}
	recorder = h.invoke(http.MethodPost, testBase, map[string]any{"accountId": "second"})
	if recorder.Code != 403 {
		t.Fatalf("viewer create status = %d", recorder.Code)
	}
}

// accountJSON is the part of an account document the tests read.
type accountJSON struct {
	Email    string `json:"email"`
	UniqueID string `json:"uniqueId"`
}

// createAccount creates one account and returns its document.
func (h *harness) createAccount(id string) accountJSON {
	h.t.Helper()
	recorder := h.expect(http.MethodPost, testBase, map[string]any{"accountId": id}, 200)
	return decodeInto[accountJSON](h.t, recorder.Body.Bytes())
}

// TestRecordedDenialsAndMissingAccounts pins "denied-read" (the account's
// unique-id name in the message and ErrorInfo) and "denied-missing": a
// missing account is 404 to every caller.
func TestRecordedDenialsAndMissingAccounts(t *testing.T) {
	h := newHarness(t)
	resource := "projects/-/serviceAccounts/" + h.createAccount(testAccount).UniqueID
	recorder := h.invokeAs("outsider", http.MethodGet, testBase+"/"+testAccount, nil)
	var body struct {
		Error struct {
			Message string           `json:"message"`
			Details []map[string]any `json:"details"`
		} `json:"error"`
	}
	if recorder.Code != 403 || json.Unmarshal(recorder.Body.Bytes(), &body) != nil {
		t.Fatalf("denied = %d %s", recorder.Code, recorder.Body.String())
	}
	if body.Error.Message != "Permission 'iam.serviceAccounts.get' denied on resource '//iam.googleapis.com/"+resource+"' (or it may not exist)." {
		t.Fatalf("message = %q", body.Error.Message)
	}
	want := []map[string]any{{"@type": "type.googleapis.com/google.rpc.ErrorInfo", "reason": "IAM_PERMISSION_DENIED",
		"domain": "iam.googleapis.com", "metadata": map[string]any{"permission": "iam.serviceAccounts.get", "resource": resource}}}
	if !reflect.DeepEqual(body.Error.Details, want) {
		t.Fatalf("details = %v", body.Error.Details)
	}
	for _, token := range []string{"outsider", testToken} {
		if got := h.invokeAs(token, http.MethodGet, testBase+"/missing", nil); got.Code != 404 || !strings.Contains(got.Body.String(), `"Unknown service account"`) {
			t.Fatalf("%s missing = %d %s", token, got.Code, got.Body.String())
		}
	}
}

// accountPage is one decoded accounts list page.
type accountPage struct {
	Accounts      []accountJSON `json:"accounts"`
	NextPageToken string        `json:"nextPageToken"`
}

// TestAccountListPagesInUniqueIDOrder pins "list-paged" (accounts list by
// unique id, which grows with creation here) and the recorded page refusals.
func TestAccountListPagesInUniqueIDOrder(t *testing.T) {
	h := newHarness(t)
	h.createAccount("zeta-account")
	h.createAccount("alpha-account")
	first := decodeInto[accountPage](t, h.expect(http.MethodGet, testBase+"?pageSize=1", nil, 200).Body.Bytes())
	if len(first.Accounts) != 1 || first.Accounts[0].Email != accountEmail(testProject, "zeta-account") || first.NextPageToken == "" {
		t.Fatalf("first page = %+v", first)
	}
	second := decodeInto[accountPage](t, h.expect(http.MethodGet, testBase+"?pageSize=1&pageToken="+first.NextPageToken, nil, 200).Body.Bytes())
	if len(second.Accounts) != 1 || second.NextPageToken != "" || second.Accounts[0].Email != accountEmail(testProject, "alpha-account") {
		t.Fatalf("second page = %+v", second)
	}
	h.wantError(http.MethodGet, testBase+"?pageToken=not-a-page-token", nil, 400, "Request contains an invalid argument.")
	for _, size := range []string{"-1", "101"} {
		h.wantError(http.MethodGet, testBase+"?pageSize="+size, nil, 400, "The field page_size is out of range. It must be between 0 and 100.")
	}
}

// TestStrictAccountBodies pins the recorded refusals of create bodies and
// the gaps for unmodeled key and policy shapes.
func TestStrictAccountBodies(t *testing.T) {
	h := newHarness(t)
	h.wantError(http.MethodPost, testBase, map[string]any{"accountId": testAccount, "unknownField": "x"}, 400,
		`Invalid JSON payload received. Unknown name "unknownField": Cannot find field.`)
	h.wantError(http.MethodPost, testBase, map[string]any{"accountId": testAccount, "displayName": "x"}, 400,
		`Invalid JSON payload received. Unknown name "displayName": Cannot find field.`)
	h.wantError(http.MethodPost, testBase, map[string]any{"accountId": testAccount, "serviceAccount": map[string]any{"displayName": []string{"x"}}}, 400,
		`Invalid JSON payload received. Unknown name "displayName" at 'service_account': Proto field is not repeating, cannot start list.`)
	h.createAccount(testAccount)
	keys := testBase + "/" + testAccount + "/keys"
	h.wantError(http.MethodPost, keys, map[string]any{"unknownField": "x"}, 400,
		`Invalid JSON payload received. Unknown name "unknownField": Cannot find field.`)
	h.wantError(http.MethodPost, keys, map[string]any{"privateKeyType": "TYPE_PKCS12_FILE"}, 501,
		"IAM coverage gap: privateKeyType TYPE_PKCS12_FILE is not modeled")
	h.wantError(http.MethodPost, keys, map[string]any{"keyAlgorithm": "KEY_ALG_RSA_1024"}, 501,
		"IAM coverage gap: keyAlgorithm KEY_ALG_RSA_1024 is not modeled")
	h.wantError(http.MethodPost, testBase+"/"+testAccount+":setIamPolicy", map[string]any{"policy": map[string]any{
		"auditConfigs": []any{map[string]any{"service": "allServices"}}}}, 501, "IAM coverage gap: policy.auditConfigs is not modeled")
}

// keyJSON is the part of a key document the tests read.
type keyJSON struct {
	Name          string `json:"name"`
	KeyType       string `json:"keyType"`
	PublicKeyData []byte `json:"publicKeyData"`
}

// keyTypesOf lists the key types one keys.list returns, in order.
func (h *harness) keyTypesOf(path string) []string {
	h.t.Helper()
	listed := decodeInto[struct {
		Keys []keyJSON `json:"keys"`
	}](h.t, h.expect(http.MethodGet, path, nil, 200).Body.Bytes())
	found := make([]string, 0, len(listed.Keys))
	for _, key := range listed.Keys {
		found = append(found, key.KeyType)
	}
	return found
}

// TestKeyTypes pins "keys-listed" (every account has a system-managed key)
// and the keyTypes filter.
func TestKeyTypes(t *testing.T) {
	h := newHarness(t)
	h.createAccount(testAccount)
	keys := testBase + "/" + testAccount + "/keys"
	for query, want := range map[string][]string{"": {keyTypeSystem}, "?keyTypes=USER_MANAGED": {}} {
		if got := h.keyTypesOf(keys + query); !reflect.DeepEqual(got, want) {
			t.Fatalf("fresh account keys%s = %v", query, got)
		}
	}
	created := decodeInto[keyJSON](t, h.expect(http.MethodPost, keys, map[string]any{
		"keyAlgorithm": "KEY_ALG_RSA_2048", "privateKeyType": "TYPE_GOOGLE_CREDENTIALS_FILE"}, 200).Body.Bytes())
	if created.Name != accountName(testProject, testAccount)+"/keys/"+provider.LastSegment(created.Name) {
		t.Fatalf("key name = %q", created.Name)
	}
	for query, want := range map[string]int{"?keyTypes=USER_MANAGED": 1, "?keyTypes=SYSTEM_MANAGED&keyTypes=USER_MANAGED": 2} {
		if got := h.keyTypesOf(keys + query); len(got) != want {
			t.Fatalf("keys%s = %v", query, got)
		}
	}
	h.wantError(http.MethodGet, keys+"?keyTypes=EVERY", nil, 400,
		`Invalid value at 'key_types[0]' (type.googleapis.com/google.iam.admin.v1.ListServiceAccountKeysRequest.KeyType), "EVERY"`)
}

// TestPublicKeyFormats covers keys.get publicKeyType: none by default, a
// self-signed certificate, or the DER public key.
func TestPublicKeyFormats(t *testing.T) {
	h := newHarness(t)
	h.createAccount(testAccount)
	keys := testBase + "/" + testAccount + "/keys"
	created := decodeInto[keyJSON](t, h.expect(http.MethodPost, keys, map[string]any{}, 200).Body.Bytes())
	key := keys + "/" + provider.LastSegment(created.Name)
	if plain := decodeInto[keyJSON](t, h.expect(http.MethodGet, key, nil, 200).Body.Bytes()); plain.PublicKeyData != nil {
		t.Fatalf("plain get = %+v", plain)
	}
	block, _ := pem.Decode(decodeInto[keyJSON](t, h.expect(http.MethodGet, key+"?publicKeyType=TYPE_X509_PEM_FILE", nil, 200).Body.Bytes()).PublicKeyData)
	if block == nil || block.Type != "CERTIFICATE" {
		t.Fatalf("pem = %v", block)
	}
	if _, err := x509.ParseCertificate(block.Bytes); err != nil {
		t.Fatal(err)
	}
	raw := decodeInto[keyJSON](t, h.expect(http.MethodGet, key+"?publicKeyType=TYPE_RAW_PUBLIC_KEY", nil, 200).Body.Bytes())
	if _, err := x509.ParsePKIXPublicKey(raw.PublicKeyData); err != nil {
		t.Fatal(err)
	}
	h.expect(http.MethodGet, key+"?publicKeyType=PNG", nil, 400)
}

// TestAccountConditionsSeeNoResourceAttributes pins "conditional-access": a
// condition on the account's resource.type and name grants nothing, one on
// request.time grants.
func TestAccountConditionsSeeNoResourceAttributes(t *testing.T) {
	h := newHarness(t)
	first := h.createAccount("first-account")
	h.createAccount("second-account")
	member := "serviceAccount:reader@" + testProject + ".iam.gserviceaccount.com"
	for id, expression := range map[string]string{
		"first-account":  `resource.type == "iam.googleapis.com/ServiceAccount" && resource.name == "projects/-/serviceAccounts/` + first.UniqueID + `"`,
		"second-account": `request.time < timestamp("2100-01-01T00:00:00Z")`,
	} {
		h.expect(http.MethodPost, testBase+"/"+id+":setIamPolicy", map[string]any{"policy": map[string]any{"version": 3, "bindings": []any{
			map[string]any{"role": "roles/iam.serviceAccountViewer", "members": []string{member},
				"condition": map[string]any{"title": id, "expression": expression}}}}}, 200)
	}
	h.expectAs("reader", testBase+"/first-account", 403)
	h.expectAs("reader", testBase+"/second-account", 200)
}
