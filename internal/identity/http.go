package identity

import (
	"crypto/sha256"
	"encoding/json"
	"mime"
	"net/http"
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
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/_local/identity/certs" && r.Method == http.MethodGet && s.IDTokens != nil:
		writeJSON(w, 200, s.IDTokens.Certificates())
	case r.URL.Path == "/v1/token" && r.Method == http.MethodPost:
		s.exchange(w, r)
	case r.URL.Path == "/_local/identity" && r.Method == http.MethodGet:
		s.inspect(w, r)
	default:
		http.NotFound(w, r)
	}
}
func (s *Service) exchange(w http.ResponseWriter, r *http.Request) {
	contentType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if contentType != "application/x-www-form-urlencoded" || r.URL.RawQuery != "" || len(r.Header.Values("Authorization")) != 0 {
		oauthError(w, 400, "invalid_request", "Use a form-encoded local subject-token exchange")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 8192)
	if err := r.ParseForm(); err != nil {
		oauthError(w, 400, "invalid_request", "Malformed token exchange")
		return
	}
	required := map[string]bool{"grant_type": true, "audience": true, "requested_token_type": true, "subject_token_type": true, "subject_token": true, "scope": true}
	for field, values := range r.PostForm {
		if !required[field] || len(values) != 1 {
			oauthError(w, 400, "invalid_request", "Unsupported or repeated exchange parameter")
			return
		}
	}
	for field := range required {
		if r.PostForm.Get(field) == "" {
			oauthError(w, 400, "invalid_request", "Missing exchange parameter")
			return
		}
	}
	if r.PostForm.Get("grant_type") != "urn:ietf:params:oauth:grant-type:token-exchange" {
		oauthError(w, 400, "unsupported_grant_type", "Only token exchange is implemented")
		return
	}
	if r.PostForm.Get("audience") != Audience || r.PostForm.Get("requested_token_type") != AccessTokenType || r.PostForm.Get("subject_token_type") != AccessTokenType {
		oauthError(w, 400, "invalid_target", "Select the local audience and access-token types")
		return
	}
	p, ok := s.subjects[sha256.Sum256([]byte(r.PostForm.Get("subject_token")))]
	if !ok {
		oauthError(w, 400, "invalid_grant", "Unknown local subject credential")
		return
	}
	value, lifetime, err := s.mint(p, strings.Fields(r.PostForm.Get("scope")))
	if err != nil {
		oauthError(w, 400, "invalid_scope", err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"access_token": value, "issued_token_type": AccessTokenType, "token_type": "Bearer", "expires_in": lifetime})
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
