package identity

import (
	"errors"
	"fmt"
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
	metadata := &metadataResponder{s: s, p: p, email: email}
	return http.HandlerFunc(metadata.serve), nil
}

// metadataResponder serves one enrolled service account's metadata tree.
type metadataResponder struct {
	s     *Service
	p     principal
	email string
}

// serve guards the request shape, then routes the metadata path.
func (m *metadataResponder) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet || r.Header.Get("Metadata-Flavor") != "Google" || len(r.Header.Values("Metadata-Flavor")) != 1 || len(r.Header.Values("X-Forwarded-For")) != 0 {
		http.Error(w, "Metadata request denied", http.StatusForbidden)
		return
	}
	w.Header().Set("Metadata-Flavor", "Google")
	w.Header().Set("Cache-Control", "no-store")
	m.route(w, r)
}

// route answers the fixed project paths and delegates the service-account tree.
func (m *metadataResponder) route(w http.ResponseWriter, r *http.Request) {
	text := func(value string) { w.Header().Set("Content-Type", "text/plain"); _, _ = w.Write([]byte(value)) }
	switch r.URL.Path {
	case "/":
		text("")
	case "/computeMetadata/v1/project/project-id":
		text(m.p.project)
	case "/computeMetadata/v1/universe/universe-domain":
		text("googleapis.com")
	case "/computeMetadata/v1/instance/service-accounts/":
		text("default/\n" + m.email + "/\n")
	default:
		m.serviceAccount(w, r, text)
	}
}

// serviceAccount resolves the account segment of the path and answers its leaf.
// Both the account's default alias and its own email address are accepted.
func (m *metadataResponder) serviceAccount(w http.ResponseWriter, r *http.Request, text func(string)) {
	path := strings.TrimPrefix(r.URL.Path, "/computeMetadata/v1/instance/service-accounts/")
	if path == r.URL.Path {
		http.NotFound(w, r)
		return
	}
	parts := strings.SplitN(path, "/", 2)
	if len(parts) != 2 || (parts[0] != "default" && parts[0] != m.email) {
		http.NotFound(w, r)
		return
	}
	m.serviceAccountLeaf(w, r, parts[1], text)
}

// serviceAccountLeaf answers one leaf below a resolved service account.
func (m *metadataResponder) serviceAccountLeaf(w http.ResponseWriter, r *http.Request, leaf string, text func(string)) {
	switch leaf {
	case "":
		if r.URL.Query().Get("recursive") == "true" {
			writeJSON(w, 200, map[string]any{"email": m.email, "aliases": []string{"default"}, fieldScopes: m.p.scopes})
		} else {
			text("email\nscopes\ntoken\n")
		}
	case "email":
		text(m.email)
	case fieldScopes:
		text(strings.Join(m.p.scopes, "\n") + "\n")
	case "identity":
		http.Error(w, "Local ID tokens are unimplemented", http.StatusNotImplemented)
	case "token":
		m.token(w, r)
	default:
		http.NotFound(w, r)
	}
}

// token mints one access token, optionally narrowed to requested scopes.
func (m *metadataResponder) token(w http.ResponseWriter, r *http.Request) {
	query, err := parseMetadataQuery(r)
	if err != nil {
		http.Error(w, "Unsupported metadata token request", http.StatusBadRequest)
		return
	}
	scopes := m.p.scopes
	if query != "" {
		scopes = strings.Split(query, ",")
	}
	value, lifetime, err := m.s.mint(m.p, scopes)
	if err != nil {
		http.Error(w, "Unsupported metadata token scope or capacity", http.StatusBadRequest)
		return
	}
	writeJSON(w, 200, map[string]any{"access_token": value, "expires_in": lifetime, "token_type": "Bearer"})
}

func parseMetadataQuery(r *http.Request) (string, error) {
	if err := r.ParseForm(); err != nil {
		return "", fmt.Errorf("parse metadata query: %w", err)
	}
	for key, values := range r.Form {
		if key != fieldScopes || len(values) != 1 {
			return "", errMetadataToken
		}
	}
	return r.Form.Get("scopes"), nil
}
