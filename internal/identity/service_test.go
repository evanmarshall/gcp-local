package identity

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
)

const subjectSecret = "gls.synthetic-secret-for-local-test-only-0123456789"
const appSubject = "serviceAccount:app@synthetic.iam.gserviceaccount.com"

func service(t *testing.T) (*Service, *atomic.Int64) {
	t.Helper()
	clock := &atomic.Int64{}
	s, err := New(Config{TokenLifetimeSeconds: 600, Principals: []Enrollment{{Subject: appSubject, Project: "synthetic", SubjectToken: subjectSecret}, {Subject: "serviceAccount:other@peer.iam.gserviceaccount.com", Project: "peer", SubjectToken: subjectSecret + "-other"}}}, func() time.Time { return time.Unix(1700000000+clock.Load(), 0) })
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
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/token", strings.NewReader(values.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	s.ServeHTTP(recorder, request)
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	return recorder.Code, body
}
func TestExchangeExpiryAndPrincipalStates(t *testing.T) {
	s, clock := service(t)
	code, body := exchange(t, s, form())
	if code != 200 {
		t.Fatal(body)
	}
	token := body["access_token"].(string)
	if !strings.HasPrefix(token, "gl.") || body["expires_in"] != float64(600) {
		t.Fatal("invalid access token contract")
	}
	p, err := s.Resolve(context.Background(), token)
	if err != nil || p.State != gateway.Authenticated || p.Subject != appSubject || p.Project != "synthetic" || len(p.Scopes) != 1 || p.Scopes[0] != CloudPlatform {
		t.Fatalf("%+v %v", p, err)
	}
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
	p, _ = s.Resolve(context.Background(), next["access_token"].(string))
	if p.State != gateway.Authenticated {
		t.Fatal(p)
	}
	for input, want := range map[string]gateway.PrincipalState{"": gateway.Anonymous, "unknown": gateway.Unknown, subjectSecret: gateway.Unknown} {
		p, _ = s.Resolve(context.Background(), input)
		if p.State != want || p.Subject != "" {
			t.Fatalf("unexpected state: %+v", p)
		}
	}
}
func TestExchangeRejectsUnknownCredentialsAndUnsupportedFeatures(t *testing.T) {
	s, _ := service(t)
	for _, tc := range []struct{ field, value string }{{"subject_token", "secret-not-enrolled"}, {"audience", "//iam.googleapis.com/real"}, {"grant_type", "password"}, {"subject_token_type", "urn:ietf:params:oauth:token-type:jwt"}, {"scope", "https://www.googleapis.com/auth/unsupported"}, {"options", `{"userProject":"another-project"}`}} {
		values := form()
		values.Set(tc.field, tc.value)
		code, body := exchange(t, s, values)
		if code != 400 {
			t.Fatalf("accepted %s", tc.field)
		}
		encoded, _ := json.Marshal(body)
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
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, tc.path, nil)
		if tc.header {
			request.Header.Set("Metadata-Flavor", "Google")
		}
		handler.ServeHTTP(response, request)
		if response.Code != tc.status {
			t.Fatalf("%s: %d %s", tc.path, response.Code, response.Body.String())
		}
		if response.Code == 200 {
			var body map[string]any
			json.Unmarshal(response.Body.Bytes(), &body)
			p, _ := s.Resolve(context.Background(), body["access_token"].(string))
			if p.Subject != appSubject {
				t.Fatal("metadata selected another account")
			}
		}
	}
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/", nil)
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
	token := body["access_token"].(string)
	for _, tc := range []struct {
		token  string
		status int
		state  gateway.PrincipalState
	}{{"", 200, gateway.Anonymous}, {"unknown", 401, gateway.Unknown}, {token, 200, gateway.Authenticated}} {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/_local/identity", nil)
		if tc.token != "" {
			request.Header.Set("Authorization", "Bearer "+tc.token)
		}
		s.ServeHTTP(response, request)
		var value map[string]any
		json.Unmarshal(response.Body.Bytes(), &value)
		if response.Code != tc.status || value["state"] != string(tc.state) {
			t.Fatal(value)
		}
		if strings.Contains(response.Body.String(), token) || strings.Contains(response.Body.String(), subjectSecret) {
			t.Fatal("inspection leaked credentials")
		}
	}
	clock.Store(600)
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/_local/identity", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	s.ServeHTTP(response, request)
	if response.Code != 401 {
		t.Fatal("expired token authenticated")
	}
}
func TestConfigurationRequiresExplicitPrivateEnrollment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "identity.json")
	os.WriteFile(path, []byte(`{"principals":[]}`), 0600)
	if _, err := Load(path); err != nil {
		t.Fatal(err)
	}
	os.Chmod(path, 0644)
	if _, err := Load(path); err == nil {
		t.Fatal("read nonprivate credential config")
	}
	os.Chmod(path, 0600)
	link := path + "-link"
	os.Symlink(path, link)
	if _, err := Load(link); err == nil {
		t.Fatal("read symlinked credential config")
	}
	if _, err := New(Config{Principals: []Enrollment{{Subject: appSubject, Project: "synthetic", SubjectToken: "short"}}}, nil); err == nil {
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
