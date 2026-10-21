package identity

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
)

// testProjectName is the fixture project used by the identity service tests.
const testProjectName = "synthetic"

const subjectSecret = "gls.synthetic-secret-for-local-test-only-0123456789" //nolint:gosec // a published test fixture, not a credential
const appSubject = "serviceAccount:app@synthetic.iam.gserviceaccount.com"

// mustMarshal encodes a fixture value or fails the test.
func mustMarshal(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func service(t *testing.T) (*Service, *atomic.Int64) {
	t.Helper()
	clock := &atomic.Int64{}
	s, err := New(Config{TokenLifetimeSeconds: 600, Principals: []Enrollment{{Subject: appSubject, Project: testProjectName, SubjectToken: subjectSecret}, {Subject: "serviceAccount:other@peer.iam.gserviceaccount.com", Project: "peer", SubjectToken: subjectSecret + "-other"}}}, func() time.Time { return time.Unix(1700000000+clock.Load(), 0) })
	if err != nil {
		t.Fatal(err)
	}
	return s, clock
}
func form() url.Values {
	return url.Values{"grant_type": {"urn:ietf:params:oauth:grant-type:token-exchange"}, "audience": {Audience}, "requested_token_type": {AccessTokenType}, "subject_token_type": {AccessTokenType}, "subject_token": {subjectSecret}, "scope": {CloudPlatform}}
}
func exchange(t *testing.T, s *Service, values url.Values) (int, map[string]any) {
	t.Helper()
	return post(t, s, "/v1/token", values)
}

// post sends a form-encoded STS request with optional Authorization headers.
func post(t *testing.T, s *Service, path string, values url.Values, authorization ...string) (int, map[string]any) {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, path, strings.NewReader(values.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	for _, header := range authorization {
		request.Header.Add("Authorization", header)
	}
	s.ServeHTTP(recorder, request)
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	return recorder.Code, body
}

// accessToken extracts the access token from a token response body.
func accessToken(t *testing.T, body map[string]any) string {
	t.Helper()
	token, ok := body["access_token"].(string)
	if !ok {
		t.Fatalf("missing access token: %v", body)
	}
	return token
}

// requireAppPrincipal resolves token and requires the enrolled application
// principal with exactly the cloud-platform scope.
func requireAppPrincipal(t *testing.T, s *Service, token string) gateway.Principal {
	t.Helper()
	p, err := s.Resolve(context.Background(), token)
	if err != nil || p.State != gateway.Authenticated || p.Subject != appSubject || p.Project != testProjectName {
		t.Fatalf("%+v %v", p, err)
	}
	if len(p.Scopes) != 1 || p.Scopes[0] != CloudPlatform {
		t.Fatalf("scopes: %v", p.Scopes)
	}
	return p
}

// Real STS refuses an exchange whose scope is absent or empty with 400
// invalid_request "Scope(s) must be provided." (probed 2026-10-12); the Python
// auth library sends scope= when the caller set no scopes.
func TestExchangeRefusesMissingScope(t *testing.T) {
	s, _ := service(t)
	for _, scope := range []string{"", " "} {
		values := form()
		values.Set("scope", scope)
		code, body := exchange(t, s, values)
		requireOAuthError(t, code, body, 400, "invalid_request", "Scope(s) must be provided.")
	}
	values := form()
	values.Del("scope")
	code, body := exchange(t, s, values)
	requireOAuthError(t, code, body, 400, "invalid_request", "Scope(s) must be provided.")
}

// requireScopes resolves an exchange's token and requires exactly scopes.
func requireScopes(t *testing.T, s *Service, code int, body map[string]any, scopes ...string) {
	t.Helper()
	if code != http.StatusOK {
		t.Fatalf("exchange = %d %v", code, body)
	}
	p, err := s.Resolve(context.Background(), accessToken(t, body))
	slices.Sort(scopes)
	if err != nil || p.State != gateway.Authenticated || p.Subject != appSubject || !slices.Equal(p.Scopes, scopes) {
		t.Fatalf("%+v %v, want scopes %v", p, err, scopes)
	}
}

// requireOAuthError requires an OAuth error response with this code, error,
// and description.
func requireOAuthError(t *testing.T, code int, body map[string]any, wantCode int, name, description string) {
	t.Helper()
	if code != wantCode || body["error"] != name || body["error_description"] != description {
		t.Fatalf("got %d %v, want %d %s %q", code, body, wantCode, name, description)
	}
}

// Client libraries request their own default scopes; any allowlisted Google
// OAuth scope is issued, including gcloud's identity and service scopes (#52).
func TestExchangeIssuesAllowlistedScopes(t *testing.T) {
	s, _ := service(t)
	const auth = "https://www.googleapis.com/auth/"
	for _, scopes := range [][]string{
		{auth + "pubsub"},
		{auth + "bigquery", CloudPlatform},
		{auth + "spanner.data", auth + "spanner.admin", auth + "datastore", auth + "logging.write", auth + "cloud-platform.read-only", auth + "devstorage.read_write"},
		{"openid", auth + "userinfo.email", CloudPlatform, auth + "appengine.admin", auth + "sqlservice.login", auth + "compute"},
		// dbt-bigquery's default scopes.
		{auth + "bigquery", CloudPlatform, auth + "drive"},
	} {
		values := form()
		values.Set("scope", strings.Join(scopes, " "))
		code, body := exchange(t, s, values)
		requireScopes(t, s, code, body, scopes...)
	}
}

// An unknown scope is refused by name; an enrollment's own scopes restrict
// what its tokens may carry.
func TestExchangeRefusesUnknownAndUnenrolledScopes(t *testing.T) {
	s, _ := service(t)
	values := form()
	values.Set("scope", CloudPlatform+" https://www.googleapis.com/auth/unsupported")
	code, body := exchange(t, s, values)
	requireOAuthError(t, code, body, 400, "invalid_scope", `unsupported requested scope "https://www.googleapis.com/auth/unsupported": gcp-local issues only the Google OAuth scopes listed in docs/guides/local-identity.md`)

	readOnly := "https://www.googleapis.com/auth/devstorage.read_only"
	restricted, err := New(Config{Principals: []Enrollment{{Subject: appSubject, Project: testProjectName, SubjectToken: subjectSecret, Scopes: []string{readOnly}}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	code, body = exchange(t, restricted, form())
	requireOAuthError(t, code, body, 400, "invalid_scope", `scope is not enrolled for this principal "`+CloudPlatform+`"`)
	values = form()
	values.Set("scope", readOnly)
	code, body = exchange(t, restricted, values)
	requireScopes(t, restricted, code, body, readOnly)

	_, err = New(Config{Principals: []Enrollment{{Subject: appSubject, Project: testProjectName, SubjectToken: subjectSecret, Scopes: []string{"https://www.googleapis.com/auth/unsupported"}}}}, nil)
	if !errors.Is(err, errConfiguredScope) {
		t.Fatalf("configured unknown scope: %v", err)
	}
}

// gcloud sends its OAuth client as a Basic header (#53). Client
// authentication names the client only: the subject token picks the principal.
func TestExchangeAcceptsAndIgnoresClientAuthentication(t *testing.T) {
	s, _ := service(t)
	basic := "Basic " + base64.StdEncoding.EncodeToString([]byte("32555940559.apps.googleusercontent.com:not-a-secret"))
	code, body := post(t, s, "/v1/token", form(), basic)
	requireScopes(t, s, code, body, CloudPlatform)
	values := form()
	values.Set("client_id", "local-client")
	values.Set("client_secret", "not-a-secret")
	code, body = exchange(t, s, values)
	requireScopes(t, s, code, body, CloudPlatform)

	values = form()
	values.Set("subject_token", "secret-not-enrolled-but-client-authenticated")
	code, body = post(t, s, "/v1/token", values, basic)
	requireOAuthError(t, code, body, 400, "invalid_grant", "Unknown local subject credential")

	const refusal = "Send OAuth client authentication as one Basic authorization header or as client_id and client_secret form fields"
	withID := form()
	withID.Set("client_id", "local-client")
	secretOnly := form()
	secretOnly.Set("client_secret", "not-a-secret")
	for _, tc := range []struct {
		values  url.Values
		headers []string
	}{
		{form(), []string{"Bearer " + subjectSecret}},
		{form(), []string{basic, basic}},
		{form(), []string{"Basic not-base64!"}},
		{form(), []string{"Basic " + base64.StdEncoding.EncodeToString([]byte(":secret"))}},
		{withID, []string{basic}},
		{secretOnly, nil},
	} {
		code, body := post(t, s, "/v1/token", tc.values, tc.headers...)
		requireOAuthError(t, code, body, 401, "invalid_client", refusal)
	}
}

// gcloud introspects its token, with client authentication, to name the
// account of a non-impersonated external account.
func TestIntrospectionNamesTheTokenPrincipal(t *testing.T) {
	s, clock := service(t)
	_, body := exchange(t, s, form())
	token := accessToken(t, body)
	basic := "Basic " + base64.StdEncoding.EncodeToString([]byte("client:secret"))
	query := url.Values{"token": {token}, "token_type_hint": {AccessTokenType}}
	code, body := post(t, s, "/v1/introspect", query, basic)
	if code != 200 || body["active"] != true || body["username"] != "app@synthetic.iam.gserviceaccount.com" || body["scope"] != CloudPlatform {
		t.Fatalf("introspection = %d %v", code, body)
	}
	requireInactive(t, s, "unknown")
	requireInactive(t, s, subjectSecret)
	clock.Store(600)
	requireInactive(t, s, token)
}

// requireInactive requires introspection to report token inactive and nothing else.
func requireInactive(t *testing.T, s *Service, token string) {
	t.Helper()
	basic := "Basic " + base64.StdEncoding.EncodeToString([]byte("client:secret"))
	if code, body := post(t, s, "/v1/introspect", url.Values{"token": {token}}, basic); code != 200 || len(body) != 1 || body["active"] != false {
		t.Fatalf("%s: %d %v", token, code, body)
	}
}

// Introspection without client authentication or with unsupported fields is
// refused.
func TestIntrospectionRefusals(t *testing.T) {
	s, _ := service(t)
	_, body := exchange(t, s, form())
	token := accessToken(t, body)
	basic := "Basic " + base64.StdEncoding.EncodeToString([]byte("client:secret"))
	code, body := post(t, s, "/v1/introspect", url.Values{"token": {token}})
	requireOAuthError(t, code, body, 401, "invalid_client", "Token introspection requires OAuth client authentication")
	for _, tc := range []struct {
		values      url.Values
		code        int
		name, about string
	}{
		{url.Values{"token": {token}, "extra": {"x"}}, 400, "invalid_request", "Unsupported or repeated introspection parameter"},
		{url.Values{"token": {token}, "token_type_hint": {"refresh_token"}}, 400, "unsupported_token_type", "Only access tokens can be introspected"},
		{url.Values{"token_type_hint": {"access_token"}}, 400, "invalid_request", "Missing introspection token"},
	} {
		code, body := post(t, s, "/v1/introspect", tc.values, basic)
		requireOAuthError(t, code, body, tc.code, tc.name, tc.about)
	}
}

func TestExchangeExpiryAndPrincipalStates(t *testing.T) {
	s, clock := service(t)
	code, body := exchange(t, s, form())
	if code != 200 {
		t.Fatal(body)
	}
	token := accessToken(t, body)
	if !strings.HasPrefix(token, "gl.") || body["expires_in"] != float64(600) {
		t.Fatal("invalid access token contract")
	}
	p := requireAppPrincipal(t, s, token)
	p.Scopes[0] = "mutated"
	p, _ = s.Resolve(context.Background(), token)
	if p.Scopes[0] != CloudPlatform {
		t.Fatal("caller mutated token scopes")
	}
	clock.Store(600)
	p, _ = s.Resolve(context.Background(), token)
	if p.State != gateway.Expired || p.Subject != "" {
		t.Fatal(p)
	}
	_, next := exchange(t, s, form())
	if next["access_token"] == token {
		t.Fatal("refresh reused old handle")
	}
	p, _ = s.Resolve(context.Background(), accessToken(t, next))
	if p.State != gateway.Authenticated {
		t.Fatal(p)
	}
}
func TestResolveWithoutIssuedTokenHasNoSubject(t *testing.T) {
	s, _ := service(t)
	for input, want := range map[string]gateway.PrincipalState{"": gateway.Anonymous, "unknown": gateway.Unknown, subjectSecret: gateway.Unknown} {
		p, _ := s.Resolve(context.Background(), input)
		if p.State != want || p.Subject != "" {
			t.Fatalf("unexpected state: %+v", p)
		}
	}
}
func TestExchangeRejectsUnknownCredentialsAndUnsupportedFeatures(t *testing.T) {
	s, _ := service(t)
	for _, tc := range []struct{ field, value string }{{"subject_token", "secret-not-enrolled"}, {"audience", "//iam.googleapis.com/real"}, {"grant_type", "password"}, {"subject_token_type", "urn:ietf:params:oauth:token-type:jwt"}, {"options", `{"userProject":"another-project"}`}} {
		values := form()
		values.Set(tc.field, tc.value)
		code, body := exchange(t, s, values)
		if code != 400 {
			t.Fatalf("accepted %s", tc.field)
		}
		encoded := mustMarshal(t, body)
		if strings.Contains(string(encoded), tc.value) {
			t.Fatal("exchange echoed supplied credential or unsupported value")
		}
	}
	values := form()
	values.Add("subject_token", subjectSecret)
	if code, _ := exchange(t, s, values); code != 400 {
		t.Fatal("accepted repeated credential")
	}
}

// serveMetadata sends one GET to the metadata handler, with or without the
// Metadata-Flavor header.
func serveMetadata(handler http.Handler, path string, header bool) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
	if header {
		request.Header.Set("Metadata-Flavor", "Google")
	}
	handler.ServeHTTP(response, request)
	return response
}

// requireMetadataSubject requires a metadata token response minted for the
// application principal.
func requireMetadataSubject(t *testing.T, s *Service, response *httptest.ResponseRecorder) {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	p, _ := s.Resolve(context.Background(), accessToken(t, body))
	if p.Subject != appSubject {
		t.Fatal("metadata selected another account")
	}
}

func TestMetadataIsBoundToOnePrincipal(t *testing.T) {
	s, _ := service(t)
	handler, err := s.Metadata(appSubject)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Metadata("serviceAccount:missing@synthetic.iam.gserviceaccount.com"); err == nil {
		t.Fatal("bound unknown principal")
	}
	for _, tc := range []struct {
		path   string
		header bool
		status int
	}{
		{"/computeMetadata/v1/instance/service-accounts/default/token", true, 200},
		{"/computeMetadata/v1/instance/service-accounts/app@synthetic.iam.gserviceaccount.com/token", true, 200},
		{"/computeMetadata/v1/instance/service-accounts/other@peer.iam.gserviceaccount.com/token", true, 404},
		{"/computeMetadata/v1/instance/service-accounts/default/token?service_account=other", true, 400},
		{"/computeMetadata/v1/instance/service-accounts/default/token", false, 403},
		{"/computeMetadata/v1/instance/service-accounts/default/identity", true, 501},
	} {
		response := serveMetadata(handler, tc.path, tc.header)
		if response.Code != tc.status {
			t.Fatalf("%s: %d %s", tc.path, response.Code, response.Body.String())
		}
		if response.Code == 200 {
			requireMetadataSubject(t, s, response)
		}
	}
	token := "/computeMetadata/v1/instance/service-accounts/default/token?scopes="
	if response := serveMetadata(handler, token+"https://www.googleapis.com/auth/pubsub,openid", true); response.Code != 200 {
		t.Fatalf("allowlisted metadata scopes: %d %s", response.Code, response.Body.String())
	}
	if response := serveMetadata(handler, token+"https://www.googleapis.com/auth/unsupported", true); response.Code != 400 || response.Body.String() != "Unsupported metadata token scope or capacity\n" {
		t.Fatalf("unknown metadata scope: %d %s", response.Code, response.Body.String())
	}
	response := httptest.NewRecorder()
	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	request.Header.Set("Metadata-Flavor", "Google")
	request.Header.Set("X-Forwarded-For", "127.0.0.1")
	handler.ServeHTTP(response, request)
	if response.Code != 403 {
		t.Fatal("accepted forwarded metadata request")
	}
}
func TestInspectionDoesNotExposeTokensAndInvalidTokensStayUnauthenticated(t *testing.T) {
	s, clock := service(t)
	_, body := exchange(t, s, form())
	token := accessToken(t, body)
	for _, tc := range []struct {
		token  string
		status int
		state  gateway.PrincipalState
	}{{"", 200, gateway.Anonymous}, {"unknown", 401, gateway.Unknown}, {token, 200, gateway.Authenticated}} {
		response := httptest.NewRecorder()
		request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/_local/identity", nil)
		if tc.token != "" {
			request.Header.Set("Authorization", "Bearer "+tc.token)
		}
		s.ServeHTTP(response, request)
		var value map[string]any
		if err := json.Unmarshal(response.Body.Bytes(), &value); err != nil {
			t.Fatal(err)
		}
		if response.Code != tc.status || value["state"] != string(tc.state) {
			t.Fatal(value)
		}
		if strings.Contains(response.Body.String(), token) || strings.Contains(response.Body.String(), subjectSecret) {
			t.Fatal("inspection leaked credentials")
		}
	}
	clock.Store(600)
	response := httptest.NewRecorder()
	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/_local/identity", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	s.ServeHTTP(response, request)
	if response.Code != 401 {
		t.Fatal("expired token authenticated")
	}
}
func TestConfigurationRequiresExplicitPrivateEnrollment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "identity.json")
	if err := os.WriteFile(path, []byte(`{"principals":[]}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0644); err != nil { //nolint:gosec // the test asserts that a non-private config is rejected
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("read nonprivate credential config")
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	link := path + "-link"
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(link); err == nil {
		t.Fatal("read symlinked credential config")
	}
}
func TestEnrollmentRequiresLongSecretAndEmptyConfigHasNoPrincipal(t *testing.T) {
	if _, err := New(Config{Principals: []Enrollment{{Subject: appSubject, Project: testProjectName, SubjectToken: "short"}}}, nil); err == nil {
		t.Fatal("accepted short subject credential")
	}
	s, err := New(Config{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	p, _ := s.Resolve(context.Background(), "anything")
	if p.State != gateway.Unknown {
		t.Fatal("empty config created a principal")
	}
}

// TestImpersonationIssuesTokensForAnyServiceAccount binds a token to an
// unenrolled account for the requested lifetime and refuses users.
func TestImpersonationIssuesTokensForAnyServiceAccount(t *testing.T) {
	s, clock := service(t)
	token, expires, err := s.Impersonate("serviceAccount:worker@peer.iam.gserviceaccount.com", "peer", []string{CloudPlatform}, 90*time.Second)
	if err != nil || expires.Unix() != 1700000090 {
		t.Fatalf("impersonate = %v, %v", expires, err)
	}
	principal, _ := s.Resolve(context.Background(), token)
	if principal.State != gateway.Authenticated || principal.Subject != "serviceAccount:worker@peer.iam.gserviceaccount.com" || principal.Project != "peer" {
		t.Fatalf("impersonated principal %+v", principal)
	}
	clock.Store(90)
	if principal, _ := s.Resolve(context.Background(), token); principal.State != gateway.Expired {
		t.Fatalf("expired impersonated token %+v", principal)
	}
	if _, _, err := s.Impersonate("user:someone@example.com", "peer", []string{CloudPlatform}, 0); !errors.Is(err, errImpersonated) {
		t.Fatalf("user impersonation = %v", err)
	}
}

// TestLifetimeExtensionListsServiceAccounts limits impersonated lifetimes to
// one hour except for configured service accounts.
func TestLifetimeExtensionListsServiceAccounts(t *testing.T) {
	s, _ := service(t)
	if s.MaxLifetime("worker@peer.iam.gserviceaccount.com") != DefaultLifetime {
		t.Fatal("unlisted account may extend")
	}
	extended, err := New(Config{LifetimeExtension: []string{"long@peer.iam.gserviceaccount.com"}}, nil)
	if err != nil || extended.MaxLifetime("long@peer.iam.gserviceaccount.com") != ExtendedLifetime {
		t.Fatalf("extension = %v", err)
	}
	if _, err := New(Config{LifetimeExtension: []string{"user@example.com"}}, nil); !errors.Is(err, errExtensionAccount) {
		t.Fatalf("non-account extension = %v", err)
	}
}
