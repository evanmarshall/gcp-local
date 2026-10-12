package iam

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"strings"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// TestCreatedKeysSignAndVerify checks that a created key is a usable key file
// whose signatures the engine verifies until the key or account is deleted.
func TestCreatedKeysSignAndVerify(t *testing.T) {
	h := newHarness(t)
	h.expect(http.MethodPost, testBase, map[string]any{"accountId": testAccount}, 200)
	file, key := createKey(t, h)
	email := accountEmail(testProject, testAccount)
	digest := sha256.Sum256([]byte("message"))
	signature, err := rsa.SignPKCS1v15(nil, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	if scope, ok := h.engine.VerifySignature(email, []byte("message"), signature); !ok || scope.Project != testProject {
		t.Fatal("created key does not verify")
	}
	if _, ok := h.engine.VerifySignature(email, []byte("other"), signature); ok {
		t.Fatal("signature verified another message")
	}
	h.expect(http.MethodDelete, testBase+"/"+testAccount+"/keys/"+file["private_key_id"], nil, 200)
	if _, ok := h.engine.VerifySignature(email, []byte("message"), signature); ok {
		t.Fatal("deleted key still verifies")
	}
}

// createKey creates one key for testAccount and checks its key file.
func createKey(t *testing.T, h *harness) (map[string]string, *rsa.PrivateKey) {
	t.Helper()
	var created struct {
		Name           string `json:"name"`
		PrivateKeyData string `json:"privateKeyData"`
		PrivateKeyType string `json:"privateKeyType"`
	}
	if err := json.Unmarshal(h.expect(http.MethodPost, testBase+"/"+testAccount+"/keys", map[string]any{}, 200).Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	file, key := decodeKeyFile(t, created.PrivateKeyData)
	want := map[string]string{"type": "service_account", "client_email": accountEmail(testProject, testAccount), "project_id": testProject}
	for field, value := range want {
		if file[field] != value {
			t.Fatalf("key file %s = %q", field, file[field])
		}
	}
	if created.PrivateKeyType != "TYPE_GOOGLE_CREDENTIALS_FILE" || !strings.HasSuffix(created.Name, "/"+file["private_key_id"]) ||
		!strings.HasSuffix(file["token_uri"], "/v1/token") {
		t.Fatalf("key = %+v, file = %v", created, file)
	}
	return file, key
}

// decodeKeyFile decodes privateKeyData into its JSON fields and RSA key.
func decodeKeyFile(t *testing.T, data string) (map[string]string, *rsa.PrivateKey) {
	t.Helper()
	raw, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		t.Fatal(err)
	}
	var file map[string]string
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode([]byte(file["private_key"]))
	if block == nil {
		t.Fatal("no PEM private key")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	key, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		t.Fatal("not an RSA key")
	}
	return file, key
}

// signBlob calls signBlob as the fixture principal and returns the signature.
func (h *harness) signBlob(account string, want int) []byte {
	h.t.Helper()
	recorder := h.expect(http.MethodPost, "/v1/projects/-/serviceAccounts/"+account+":signBlob",
		map[string]any{"payload": base64.StdEncoding.EncodeToString([]byte("blob"))}, want)
	var body struct {
		KeyID      string `json:"keyId"`
		SignedBlob []byte `json:"signedBlob"`
	}
	if want == 200 && (json.Unmarshal(recorder.Body.Bytes(), &body) != nil || body.KeyID == "") {
		h.t.Fatalf("signBlob body %s", recorder.Body.String())
	}
	return body.SignedBlob
}

// TestSignBlobNeedsTokenCreator covers signBlob for created and configured
// accounts, its permission, and its refusals.
func TestSignBlobNeedsTokenCreator(t *testing.T) {
	h := newHarness(t)
	h.expect(http.MethodPost, testBase, map[string]any{"accountId": testAccount}, 200)
	email := accountEmail(testProject, testAccount)
	h.signBlob(email, 403)
	if _, err := h.engine.SetPolicy(accountResource(testProject, email), iam.Policy{Bindings: []iam.Binding{
		{Role: "roles/iam.serviceAccountTokenCreator", Members: []string{testActor}}}}); err != nil {
		t.Fatal(err)
	}
	signature := h.signBlob(email, 200)
	if _, ok := h.engine.VerifySignature(email, []byte("blob"), signature); !ok {
		t.Fatal("signBlob signature does not verify")
	}
	// The same Google-managed key signs again.
	if _, ok := h.engine.VerifySignature(email, []byte("blob"), h.signBlob(email, 200)); !ok {
		t.Fatal("second signature does not verify")
	}
	h.expect(http.MethodPost, testBase+"/"+testAccount+":signBlob", map[string]any{"payload": "YQ=="}, 200)
	for _, body := range []map[string]any{{}, {"payload": "%%%"}, {"payload": "YQ==", "delegates": []string{"x"}}} {
		recorder := h.invoke(http.MethodPost, "/v1/projects/-/serviceAccounts/"+email+":signBlob", body)
		if recorder.Code != 400 && recorder.Code != 501 {
			t.Fatalf("signBlob %v status = %d", body, recorder.Code)
		}
	}
	// Deleting the account forgets its keys.
	h.expect(http.MethodDelete, testBase+"/"+testAccount, nil, 200)
	if _, ok := h.engine.VerifySignature(email, []byte("blob"), signature); ok {
		t.Fatal("deleted account's key still verifies")
	}
}

// TestSignBlobForConfiguredAccounts signs for an account declared only in the
// IAM configuration, and resetting the scope forgets its key.
func TestSignBlobForConfiguredAccounts(t *testing.T) {
	h := newHarness(t)
	// An account declared only in the IAM configuration signs too.
	configured := "configured@" + testProject + ".iam.gserviceaccount.com"
	if err := h.index.Put("iam", state.Resource{Name: accountResource(testProject, configured), Service: "iam", Kind: kindAccount,
		Parent: "//cloudresourcemanager.googleapis.com/projects/" + testProject, Scope: state.Scope{Project: testProject, Location: state.Global}}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.engine.SetPolicy("//cloudresourcemanager.googleapis.com/projects/"+testProject, iam.Policy{Bindings: []iam.Binding{
		{Role: "roles/iam.serviceAccountAdmin", Members: []string{testActor}},
		{Role: "roles/iam.serviceAccountTokenCreator", Members: []string{testActor}}}}); err != nil {
		t.Fatal(err)
	}
	h.signBlob("missing@"+testProject+".iam.gserviceaccount.com", 404)
	// Resetting the scope forgets the configured account's key.
	other := h.signBlob(configured, 200)
	h.engine.ClearScope(serviceName, state.Scope{Project: testProject, Location: state.Global})
	if _, ok := h.engine.VerifySignature(configured, []byte("blob"), other); ok {
		t.Fatal("cleared scope's key still verifies")
	}
}
