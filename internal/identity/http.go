package identity

import (
	"crypto/sha256"
	"encoding/json"
	"mime"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/gateway"
)

func writeJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	// Marshal before committing a status so an encoding failure is still
	// reportable as a 500 instead of a silent truncated 200.
	encoded, err := json.Marshal(value)
	if err != nil {
		http.Error(w, `{"error":"response encoding failed"}`, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(code)
	_, _ = w.Write(encoded)
}
func oauthError(w http.ResponseWriter, code int, name, description string) {
	writeJSON(w, code, map[string]string{"error": name, "error_description": description})
}

// tokenTypeBearer is the OAuth token type of every issued access token.
const tokenTypeBearer = "Bearer"

// fieldAccessToken names the access token in token responses and is RFC
// 7662's token_type_hint for one.
const fieldAccessToken = "access_token"

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	sts := map[string]http.HandlerFunc{"/v1/token": s.exchange, "/v1/introspect": s.introspect}
	switch {
	case r.URL.Path == "/_local/identity/certs" && r.Method == http.MethodGet && s.IDTokens != nil:
		writeJSON(w, 200, s.IDTokens.Certificates())
	case sts[r.URL.Path] != nil && r.Method == http.MethodPost:
		sts[r.URL.Path](w, r)
	case r.URL.Path == "/_local/identity" && r.Method == http.MethodGet:
		s.inspect(w, r)
	default:
		http.NotFound(w, r)
	}
}

// parseOAuthForm reads a bounded form-encoded STS request and its optional
// client authentication, writing the OAuth error when it is refused.
func parseOAuthForm(w http.ResponseWriter, r *http.Request) bool {
	contentType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if contentType != "application/x-www-form-urlencoded" || r.URL.RawQuery != "" {
		oauthError(w, 400, "invalid_request", "Use a form-encoded local subject-token exchange")
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, 8192)
	if err := r.ParseForm(); err != nil {
		oauthError(w, 400, "invalid_request", "Malformed token exchange")
		return false
	}
	if !clientAuthentication(r) {
		oauthError(w, 401, "invalid_client", "Send OAuth client authentication as one Basic authorization header or as client_id and client_secret form fields")
		return false
	}
	return true
}

func (s *Service) exchange(w http.ResponseWriter, r *http.Request) {
	if !parseOAuthForm(w, r) {
		return
	}
	if code, detail, refused := exchangeForm(r); refused {
		oauthError(w, code, detail[0], detail[1])
		return
	}
	p, ok := s.subjects[sha256.Sum256([]byte(r.PostForm.Get("subject_token")))]
	if !ok {
		oauthError(w, 400, "invalid_grant", "Unknown local subject credential")
		return
	}
	value, lifetime, err := s.mint(p, requestedScopes(r, p.scopes))
	if err != nil {
		oauthError(w, 400, "invalid_scope", err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{fieldAccessToken: value, "issued_token_type": AccessTokenType, "token_type": tokenTypeBearer, "expires_in": lifetime})
}

// introspect answers RFC 7662 token introspection, which gcloud calls with its
// client authentication to name the account of a non-impersonated external
// account. Like STS, it requires client authentication; an unknown or expired
// token is inactive. Real STS introspection of a local principal is not
// recorded: username is the enrolled subject's email.
func (s *Service) introspect(w http.ResponseWriter, r *http.Request) {
	if !parseOAuthForm(w, r) {
		return
	}
	if code, detail, refused := introspectionForm(r); refused {
		oauthError(w, code, detail[0], detail[1])
		return
	}
	// A cancelled request resolves to no principal and reads as inactive.
	p, _ := s.Resolve(r.Context(), r.PostForm.Get("token"))
	if p.State != gateway.Authenticated {
		writeJSON(w, 200, map[string]any{"active": false})
		return
	}
	_, email, _ := strings.Cut(p.Subject, ":")
	writeJSON(w, 200, map[string]any{"active": true, "username": email, "scope": strings.Join(p.Scopes, " ")})
}

// introspectionForm validates an introspection request and reports the OAuth
// error to write when it is refused.
func introspectionForm(r *http.Request) (int, [2]string, bool) {
	if r.Header.Get("Authorization") == "" && r.PostForm.Get("client_id") == "" {
		return 401, [2]string{"invalid_client", "Token introspection requires OAuth client authentication"}, true
	}
	for field, values := range r.PostForm {
		if !slices.Contains([]string{"token", "token_type_hint", "client_id", "client_secret"}, field) || len(values) != 1 {
			return 400, [2]string{"invalid_request", "Unsupported or repeated introspection parameter"}, true
		}
	}
	if hint := r.PostForm.Get("token_type_hint"); hint != "" && hint != AccessTokenType && hint != fieldAccessToken {
		return 400, [2]string{"unsupported_token_type", "Only access tokens can be introspected"}, true
	}
	if r.PostForm.Get("token") == "" {
		return 400, [2]string{"invalid_request", "Missing introspection token"}, true
	}
	return 0, [2]string{}, false
}

// requestedScopes returns the exchange's scopes. Like the metadata server, an
// exchange without one gets the principal's configured scopes; client
// libraries that rely on default scopes omit it.
func requestedScopes(r *http.Request, configured []string) []string {
	if scopes := strings.Fields(r.PostForm.Get("scope")); len(scopes) > 0 {
		return scopes
	}
	return configured
}

// clientAuthentication accepts the OAuth client authentication an
// external-account credential may send (RFC 6749 section 2.3.1): one Basic
// header, or client_id with an optional client_secret in the form, never both.
// As at STS for an external account, it names the OAuth client only: the
// values grant nothing, and the subject token alone selects the principal.
func clientAuthentication(r *http.Request) bool {
	headers := r.Header.Values("Authorization")
	if len(headers) == 0 {
		return r.PostForm.Get("client_id") != "" || !r.PostForm.Has("client_secret")
	}
	if len(headers) > 1 || r.PostForm.Has("client_id") || r.PostForm.Has("client_secret") {
		return false
	}
	id, _, ok := r.BasicAuth()
	return ok && id != ""
}

// optionalFields are the exchange fields a client may omit.
var optionalFields = []string{"scope", "client_id", "client_secret"}

// unexpectedField reports a repeated field or one that is neither required nor
// optional.
func unexpectedField(form url.Values, required []string) bool {
	for field, values := range form {
		if (!slices.Contains(optionalFields, field) && !slices.Contains(required, field)) || len(values) != 1 {
			return true
		}
	}
	return false
}

// exchangeForm validates the token-exchange form and reports the OAuth error to
// write when it is refused. Only this audience, access-token subjects, and the
// token-exchange grant are implemented.
func exchangeForm(r *http.Request) (int, [2]string, bool) {
	refuse := func(code int, code_ string, detail string) (int, [2]string, bool) {
		return code, [2]string{code_, detail}, true
	}
	required := []string{"grant_type", "audience", "requested_token_type", "subject_token_type", "subject_token"}
	if unexpectedField(r.PostForm, required) {
		return refuse(400, "invalid_request", "Unsupported or repeated exchange parameter")
	}
	for _, field := range required {
		if r.PostForm.Get(field) == "" {
			return refuse(400, "invalid_request", "Missing exchange parameter")
		}
	}
	if r.PostForm.Get("grant_type") != "urn:ietf:params:oauth:grant-type:token-exchange" {
		return refuse(400, "unsupported_grant_type", "Only token exchange is implemented")
	}
	if r.PostForm.Get("audience") != Audience || r.PostForm.Get("requested_token_type") != AccessTokenType || r.PostForm.Get("subject_token_type") != AccessTokenType {
		return refuse(400, "invalid_target", "Select the local audience and access-token types")
	}
	return 0, [2]string{}, false
}

func (s *Service) inspect(w http.ResponseWriter, r *http.Request) {
	headers := r.Header.Values("Authorization")
	var bearer string
	if len(headers) > 0 {
		parts := strings.Fields(headers[0])
		if len(headers) != 1 || len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			oauthError(w, 401, "invalid_token", "Malformed authorization header")
			return
		}
		bearer = parts[1]
	}
	principal, err := s.Resolve(r.Context(), bearer)
	if err != nil {
		oauthError(w, 500, "server_error", "Unable to resolve local identity")
		return
	}
	code := 200
	if principal.State == gateway.Unknown || principal.State == gateway.Expired {
		code = 401
	}
	writeJSON(w, code, map[string]any{"state": principal.State, "subject": principal.Subject, "project": principal.Project, fieldScopes: principal.Scopes})
}
