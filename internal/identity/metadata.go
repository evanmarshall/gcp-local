package identity

import (
	"errors"
	"net/http"
	"strings"
)

// Metadata sentinels.
var (
	errMetadataSubject = errors.New("metadata requires an enrolled service account")
	errMetadataToken   = errors.New("unsupported metadata token parameter")
)

// fieldScopes is the metadata and identity field name for the granted scopes.
const fieldScopes = "scopes"

// Metadata binds a handler to one configured service account. The request URL,
// headers, and query never select a different principal. Workload orchestration
// must expose this handler only in that workload's private network (#13).
func (s *Service) Metadata(subject string) (http.Handler, error) {
	p, ok := s.principals[subject]
	if !ok || !strings.HasPrefix(subject, "serviceAccount:") {
		return nil, errMetadataSubject
	}
	email := strings.TrimPrefix(subject, "serviceAccount:")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.Header.Get("Metadata-Flavor") != "Google" || len(r.Header.Values("Metadata-Flavor")) != 1 || len(r.Header.Values("X-Forwarded-For")) != 0 {
			http.Error(w, "Metadata request denied", http.StatusForbidden)
			return
		}
		w.Header().Set("Metadata-Flavor", "Google")
		w.Header().Set("Cache-Control", "no-store")
		text := func(value string) { w.Header().Set("Content-Type", "text/plain"); _, _ = w.Write([]byte(value)) }
		switch r.URL.Path {
		case "/":
			text("")
			return
		case "/computeMetadata/v1/project/project-id":
			text(p.project)
			return
		case "/computeMetadata/v1/universe/universe-domain":
			text("googleapis.com")
			return
		case "/computeMetadata/v1/instance/service-accounts/":
			text("default/\n" + email + "/\n")
			return
		}
		path := strings.TrimPrefix(r.URL.Path, "/computeMetadata/v1/instance/service-accounts/")
		if path == r.URL.Path {
			http.NotFound(w, r)
			return
		}
		parts := strings.SplitN(path, "/", 2)
		if len(parts) != 2 || (parts[0] != "default" && parts[0] != email) {
			http.NotFound(w, r)
			return
		}
		switch parts[1] {
		case "":
			if r.URL.Query().Get("recursive") == "true" {
				writeJSON(w, 200, map[string]any{"email": email, "aliases": []string{"default"}, fieldScopes: p.scopes})
			} else {
				text("email\nscopes\ntoken\n")
			}
		case "email":
			text(email)
		case fieldScopes:
			text(strings.Join(p.scopes, "\n") + "\n")
		case "identity":
			http.Error(w, "Local ID tokens are unimplemented", http.StatusNotImplemented)
		case "token":
			query, err := parseMetadataQuery(r)
			if err != nil {
				http.Error(w, "Unsupported metadata token request", http.StatusBadRequest)
				return
			}
			scopes := p.scopes
			if query != "" {
				scopes = strings.Split(query, ",")
			}
			value, lifetime, err := s.mint(p, scopes)
			if err != nil {
				http.Error(w, "Unsupported metadata token scope or capacity", http.StatusBadRequest)
				return
			}
			writeJSON(w, 200, map[string]any{"access_token": value, "expires_in": lifetime, "token_type": "Bearer"})
		default:
			http.NotFound(w, r)
		}
	}), nil
}
func parseMetadataQuery(r *http.Request) (string, error) {
	if err := r.ParseForm(); err != nil {
		return "", err
	}
	for key, values := range r.Form {
		if key != fieldScopes || len(values) != 1 {
			return "", errMetadataToken
		}
	}
	return r.Form.Get("scopes"), nil
}
