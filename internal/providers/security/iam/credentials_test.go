package iam

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"maps"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	iamcredentials "cloud.google.com/go/iam/credentials/apiv1"
	"cloud.google.com/go/iam/credentials/apiv1/credentialspb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	creatorRole   = "roles/iam.serviceAccountTokenCreator"
	openIDCreator = "roles/iam.serviceAccountOpenIdTokenCreator"
	viewer        = "roles/iam.serviceAccountViewer"
)

// credentialsPath is an IAM Credentials method on an account, by email.
func credentialsPath(email, verb string) string {
	return "/v1/projects/-/serviceAccounts/" + email + ":" + verb
}

// account creates one account and returns its email.
func (h *harness) account(id string) string {
	h.t.Helper()
	h.expect(http.MethodPost, testBase, map[string]any{"accountId": id}, 200)
	return accountEmail(testProject, id)
}

// grant replaces an account's policy with role/member pairs.
func (h *harness) grant(email string, bindings ...[2]string) {
	h.t.Helper()
	policy := iam.Policy{}
	for _, binding := range bindings {
		policy.Bindings = append(policy.Bindings, iam.Binding{Role: binding[0], Members: []string{binding[1]}})
	}
	if _, err := h.engine.SetPolicy(accountResource(testProject, email), policy); err != nil {
		h.t.Fatal(err)
	}
}

// accessToken calls generateAccessToken and returns the token on success.
func (h *harness) accessToken(email string, body map[string]any, want int) string {
	h.t.Helper()
	var response struct {
		AccessToken string `json:"accessToken"`
		ExpireTime  string `json:"expireTime"`
	}
	recorder := h.expect(http.MethodPost, credentialsPath(email, "generateAccessToken"), body, want)
	if want == 200 && (json.Unmarshal(recorder.Body.Bytes(), &response) != nil || response.AccessToken == "" || response.ExpireTime == "") {
		h.t.Fatalf("generateAccessToken body %s", recorder.Body.String())
	}
	return response.AccessToken
}

var cloudPlatform = map[string]any{"scope": []string{identity.CloudPlatform}}

// withDelegates adds a delegation chain to a request body.
func withDelegates(body map[string]any, delegates ...string) map[string]any {
	chained := map[string]any{"delegates": delegates}
	maps.Copy(chained, body)
	return chained
}

// TestAccessTokensAreBoundToTheTarget mints directly, then uses the token: it
// acts as the target everywhere, is introspectable, and is not the caller.
func TestAccessTokensAreBoundToTheTarget(t *testing.T) {
	h := newHarness(t)
	target, other := h.account("target"), h.account("other")
	h.accessToken(target, cloudPlatform, 403)
	h.grant(target, [2]string{creatorRole, testActor}, [2]string{viewer, "serviceAccount:" + target})
	token := h.accessToken(target, cloudPlatform, 200)
	principal, err := h.tokens.Resolve(context.Background(), token)
	if err != nil || principal.State != gateway.Authenticated || principal.Subject != "serviceAccount:"+target || principal.Project != testProject {
		t.Fatalf("minted token resolves to %+v, %v", principal, err)
	}
	if recorder := h.invokeAs(token, http.MethodGet, testBase+"/"+target, nil); recorder.Code != 200 {
		t.Fatalf("minted token read of the target = %d", recorder.Code)
	}
	if recorder := h.invokeAs(token, http.MethodGet, testBase+"/"+other, nil); recorder.Code != 403 {
		t.Fatalf("minted token read of another account = %d", recorder.Code)
	}
	h.accessToken("missing@"+testProject+".iam.gserviceaccount.com", cloudPlatform, 404)
}

// TestDelegationNeedsImplicitDelegationOnEachDelegate follows the recorded
// chain: a delegate on which the caller holds only the method's permission
// refuses, Token Creator on every hop allows, and the last delegate needs the
// method's permission on the target.
func TestDelegationNeedsImplicitDelegationOnEachDelegate(t *testing.T) {
	h := newHarness(t)
	middle, target := h.account("middle"), h.account("target")
	chain := "projects/-/serviceAccounts/" + middle
	h.grant(target, [2]string{creatorRole, "serviceAccount:" + middle})
	h.grant(middle, [2]string{openIDCreator, testActor})
	h.expect(http.MethodPost, credentialsPath(middle, "generateIdToken"), map[string]any{"audience": "aud"}, 200)
	refused := h.expect(http.MethodPost, credentialsPath(target, "generateIdToken"), map[string]any{"audience": "aud", "delegates": []string{chain}}, 403)
	if !strings.Contains(refused.Body.String(), "iam.serviceAccounts.implicitDelegation") {
		t.Fatalf("hop refusal %s", refused.Body.String())
	}
	h.accessToken(target, withDelegates(cloudPlatform, chain), 403)

	h.grant(middle, [2]string{creatorRole, testActor})
	token := h.accessToken(target, withDelegates(cloudPlatform, chain), 200)
	if principal, _ := h.tokens.Resolve(context.Background(), token); principal.Subject != "serviceAccount:"+target {
		t.Fatalf("delegated token resolves to %+v", principal)
	}
	h.accessToken(target, withDelegates(cloudPlatform, middle), 200)
	h.expect(http.MethodPost, credentialsPath(target, "signBlob"), map[string]any{"payload": "YQ==", "delegates": []string{chain}}, 200)

	// The last delegate needs the method's permission on the target.
	h.grant(target, [2]string{openIDCreator, "serviceAccount:" + middle})
	h.accessToken(target, withDelegates(cloudPlatform, chain), 403)
	h.accessToken(target, withDelegates(cloudPlatform, "projects/-/serviceAccounts/ghost@"+testProject+".iam.gserviceaccount.com"), 404)
	gap := h.invoke(http.MethodPost, credentialsPath(target, "generateAccessToken"), withDelegates(cloudPlatform, "projects/-/serviceAccounts/123456789"))
	if gap.Code != 501 || !strings.Contains(gap.Body.String(), "IAM Credentials coverage gap: delegates must name a user-managed service account by email") {
		t.Fatalf("unique-id delegate = %d %s", gap.Code, gap.Body.String())
	}
}

// TestAccessTokenLifetimeAndScopes enforces the one-hour limit, the
// configured extension to 12 hours, and required, known scopes.
func TestAccessTokenLifetimeAndScopes(t *testing.T) {
	h := newHarness(t)
	target, extended := h.account("target"), h.account("extended")
	for _, email := range []string{target, extended} {
		h.grant(email, [2]string{creatorRole, testActor})
	}
	recorder := h.expect(http.MethodPost, credentialsPath(target, "generateAccessToken"), withDelegates(map[string]any{"scope": []string{identity.CloudPlatform}, "lifetime": "600s"}), 200)
	var response struct {
		ExpireTime time.Time `json:"expireTime"`
	}
	if json.Unmarshal(recorder.Body.Bytes(), &response) != nil || time.Until(response.ExpireTime) > 601*time.Second || time.Until(response.ExpireTime) < 590*time.Second {
		t.Fatalf("600s token expires %s", recorder.Body.String())
	}
	for lifetime, want := range map[string]int{"3600s": 200, "3601s": 400, "0s": 400, "1h": 400, "43200s": 400} {
		h.accessToken(target, map[string]any{"scope": []string{identity.CloudPlatform}, "lifetime": lifetime}, want)
	}
	for lifetime, want := range map[string]int{"43200s": 200, "43201s": 400} {
		h.accessToken(extended, map[string]any{"scope": []string{identity.CloudPlatform}, "lifetime": lifetime}, want)
	}
	h.accessToken(target, map[string]any{}, 400)
	unknown := h.expect(http.MethodPost, credentialsPath(target, "generateAccessToken"), map[string]any{"scope": []string{"https://www.googleapis.com/auth/not-a-scope"}}, 400)
	if !strings.Contains(unknown.Body.String(), "not-a-scope") {
		t.Fatalf("unknown scope refusal %s", unknown.Body.String())
	}
}

// decodeJWT returns one base64url JWT segment decoded as JSON.
func decodeJWT(t *testing.T, token string, segment int) map[string]any {
	t.Helper()
	raw, err := base64.RawURLEncoding.DecodeString(strings.Split(token, ".")[segment])
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	return value
}

// TestIDTokensFollowTheRecordedClaims checks the claim sets with and without
// includeEmail, the one-hour lifetime, and that only the emailed token verifies.
func TestIDTokensFollowTheRecordedClaims(t *testing.T) {
	h := newHarness(t)
	target := h.account("target")
	h.grant(target, [2]string{creatorRole, testActor})
	for include, claims := range map[bool]int{true: 8, false: 6} {
		var response struct {
			Token string `json:"token"`
		}
		recorder := h.expect(http.MethodPost, credentialsPath(target, "generateIdToken"), map[string]any{"audience": "aud", "includeEmail": include}, 200)
		if json.Unmarshal(recorder.Body.Bytes(), &response) != nil {
			t.Fatal(recorder.Body.String())
		}
		decoded := decodeJWT(t, response.Token, 1)
		lifetime, _ := decoded["exp"].(float64)
		issued, _ := decoded["iat"].(float64)
		if len(decoded) != claims || decoded["azp"] != decoded["sub"] || lifetime-issued != 3600 {
			t.Fatalf("includeEmail=%v claims %v", include, decoded)
		}
		if _, err := h.idTokens.Verify(response.Token, "aud"); (err == nil) != include {
			t.Fatalf("includeEmail=%v verify = %v", include, err)
		}
	}
	h.expect(http.MethodPost, credentialsPath(target, "generateIdToken"), map[string]any{}, 400)
}

// TestSignedJWTsKeepThePayload checks the JWT header, that the managed key's
// signature verifies, and the refused payloads.
func TestSignedJWTsKeepThePayload(t *testing.T) {
	h := newHarness(t)
	target := h.account("target")
	h.grant(target, [2]string{creatorRole, testActor})
	payload := `{"sub":"x","exp":` + strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10) + `}`
	var signed struct {
		KeyID     string `json:"keyId"`
		SignedJwt string `json:"signedJwt"`
	}
	recorder := h.expect(http.MethodPost, credentialsPath(target, "signJwt"), map[string]any{"payload": payload}, 200)
	if json.Unmarshal(recorder.Body.Bytes(), &signed) != nil {
		t.Fatal(recorder.Body.String())
	}
	header := decodeJWT(t, signed.SignedJwt, 0)
	if header["alg"] != "RS256" || header["typ"] != "JWT" || header["kid"] != signed.KeyID || decodeJWT(t, signed.SignedJwt, 1)["sub"] != "x" {
		t.Fatalf("signed JWT header %v", header)
	}
	parts := strings.Split(signed.SignedJwt, ".")
	signature, _ := base64.RawURLEncoding.DecodeString(parts[2])
	if _, ok := h.engine.VerifySignature(target, []byte(parts[0]+"."+parts[1]), signature); !ok {
		t.Fatal("signed JWT does not verify")
	}
	late := `{"exp":` + strconv.FormatInt(time.Now().Add(13*time.Hour).Unix(), 10) + `}`
	for _, body := range []string{late, "not json", ""} {
		h.expect(http.MethodPost, credentialsPath(target, "signJwt"), map[string]any{"payload": body}, 400)
	}
}

// TestCredentialsWithoutIssuersAreGaps refuses minting when the core has no
// identity service or ID-token keys.
func TestCredentialsWithoutIssuersAreGaps(t *testing.T) {
	h := buildHarness(t, false)
	target := h.account("target")
	h.grant(target, [2]string{creatorRole, testActor})
	for verb, message := range map[string]string{
		"generateAccessToken": "IAM Credentials coverage gap: access tokens need the local identity service (-identity-config)",
		"generateIdToken":     "IAM Credentials coverage gap: ID tokens need the local ID-token keys",
	} {
		recorder := h.invoke(http.MethodPost, credentialsPath(target, verb), map[string]any{"scope": []string{identity.CloudPlatform}, "audience": "aud"})
		if recorder.Code != 501 || !strings.Contains(recorder.Body.String(), message) {
			t.Fatalf("%s = %d %s", verb, recorder.Code, recorder.Body.String())
		}
	}
}

// TestCredentialsClientOverGRPC drives cloud.google.com/go/iam/credentials
// through the gRPC gateway onto the REST provider methods.
func TestCredentialsClientOverGRPC(t *testing.T) {
	h := newHarness(t)
	middle, target := h.account("middle"), h.account("target")
	h.grant(middle, [2]string{creatorRole, testActor})
	h.grant(target, [2]string{creatorRole, "serviceAccount:" + middle})
	ctx := context.Background()
	client, err := iamcredentials.NewIamCredentialsClient(ctx, option.WithGRPCConn(h.serveGRPC(testToken)[0]))
	if err != nil {
		t.Fatal(err)
	}
	name := "projects/-/serviceAccounts/" + target
	delegates := []string{"projects/-/serviceAccounts/" + middle}
	token, err := client.GenerateAccessToken(ctx, &credentialspb.GenerateAccessTokenRequest{
		Name: name, Delegates: delegates, Scope: []string{identity.CloudPlatform}, Lifetime: durationpb.New(10 * time.Minute)})
	if err != nil || time.Until(token.GetExpireTime().AsTime()) > 601*time.Second {
		t.Fatalf("GenerateAccessToken = %v, %v", token, err)
	}
	if principal, _ := h.tokens.Resolve(ctx, token.GetAccessToken()); principal.Subject != "serviceAccount:"+target {
		t.Fatalf("gRPC token resolves to %+v", principal)
	}
	signed, err := client.SignBlob(ctx, &credentialspb.SignBlobRequest{Name: name, Delegates: delegates, Payload: []byte("blob")})
	nonEmpty(t, "SignBlob", string(signed.GetSignedBlob()), err)
	id, err := client.GenerateIdToken(ctx, &credentialspb.GenerateIdTokenRequest{Name: name, Delegates: delegates, Audience: "aud", IncludeEmail: true})
	nonEmpty(t, "GenerateIdToken", id.GetToken(), err)
	jwt, err := client.SignJwt(ctx, &credentialspb.SignJwtRequest{Name: name, Delegates: delegates, Payload: `{"sub":"x"}`})
	nonEmpty(t, "SignJwt", jwt.GetSignedJwt(), err)
	// The caller holds nothing on the target itself.
	_, err = client.GenerateAccessToken(ctx, &credentialspb.GenerateAccessTokenRequest{Name: name, Scope: []string{identity.CloudPlatform}})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("undelegated GenerateAccessToken = %v", err)
	}
}

// nonEmpty requires an RPC to succeed with a non-empty result.
func nonEmpty(t *testing.T, step, value string, err error) {
	t.Helper()
	if err != nil || value == "" {
		t.Fatalf("%s = %q, %v", step, value, err)
	}
}

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
