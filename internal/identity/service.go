// Package identity issues process-local tokens for explicitly enrolled callers.
package identity

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
)

const Audience = "//gcp-local/identity"
const AccessTokenType = "urn:ietf:params:oauth:token-type:access_token" //nolint:gosec // a public token-type URN, not a credential
const CloudPlatform = "https://www.googleapis.com/auth/cloud-platform"

// Enrollment configures one principal. Scopes, when set, are the only scopes
// its tokens may carry and the ones a scope-less request gets. Unset, a token
// may carry any scope in the iam scope allowlist and a scope-less request gets
// cloud-platform.
type Enrollment struct {
	Subject      string   `json:"subject"`
	Project      string   `json:"project"`
	SubjectToken string   `json:"subject_token"`
	Scopes       []string `json:"scopes,omitempty"`
}
type Config struct {
	TokenLifetimeSeconds int          `json:"token_lifetime_seconds"`
	Principals           []Enrollment `json:"principals"`
}
type principal struct {
	subject, project string
	// scopes are issued to a request that names none. A restricted principal
	// may request only these.
	scopes     []string
	restricted bool
}
type token struct {
	principal principal
	scopes    []string
	expires   time.Time
}

// Configuration and issuance sentinels.
var (
	errIdentityRead      = errors.New("unable to read identity configuration")
	errIdentityMode      = errors.New("identity configuration must be a regular owner-only file")
	errIdentityOpen      = errors.New("unable to open identity configuration")
	errIdentityInvalid   = errors.New("invalid identity configuration")
	errIdentityTrailing  = errors.New("identity configuration contains trailing data")
	errLifetimeRange     = errors.New("token lifetime must be between one second and one hour")
	errEnrollSubject     = errors.New("enroll an explicit user or service-account subject")
	errSubjectSecret     = errors.New("subject credentials must contain 32 to 256 non-whitespace characters")
	errDuplicateSecret   = errors.New("duplicate subject credential")
	errDuplicateEnrolled = errors.New("duplicate enrolled principal")
	errConfiguredScope   = errors.New("invalid configured oauth scope")
	errRequestedScope    = errors.New("unsupported requested scope")
	errScopeNotEnrolled  = errors.New("scope is not enrolled for this principal")
	errScopeRequired     = errors.New("at least one oauth scope is required")
	errIssueToken        = errors.New("unable to issue token")
	errTokenCapacity     = errors.New("local token capacity reached")
	errImpersonated      = errors.New("access tokens are issued only for an enrolled service account")
)

type Service struct {
	// Configure before serving. Public keys are readable; minting remains an
	// internal provider operation guarded by the shared IAM evaluator.
	IDTokens   *IDTokens
	mu         sync.Mutex
	subjects   map[[32]byte]principal
	principals map[string]principal
	tokens     map[[32]byte]token
	lifetime   time.Duration
	now        func() time.Time
}

func Load(path string) (Config, error) {
	var config Config
	info, err := os.Lstat(path)
	if err != nil {
		return config, errIdentityRead
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return config, errIdentityMode
	}
	file, err := os.Open(path) //nolint:gosec // the operator selects the config path explicitly
	if err != nil {
		return config, errIdentityOpen
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return Config{}, errIdentityInvalid
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		return Config{}, errIdentityTrailing
	}
	return config, nil
}
func New(config Config, now func() time.Time) (*Service, error) {
	if now == nil {
		now = time.Now
	}
	lifetime := config.TokenLifetimeSeconds
	if lifetime == 0 {
		lifetime = 3600
	}
	if lifetime < 1 || lifetime > 3600 {
		return nil, errLifetimeRange
	}
	s := &Service{subjects: map[[32]byte]principal{}, principals: map[string]principal{}, tokens: map[[32]byte]token{}, lifetime: time.Duration(lifetime) * time.Second, now: now}
	for _, entry := range config.Principals {
		if err := s.enroll(entry); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// enroll validates one configured principal and indexes it by subject and by
// the hash of its subject token, rejecting duplicates of either.
func (s *Service) enroll(entry Enrollment) error {
	if err := validateEnrollment(entry); err != nil {
		return err
	}
	hash := sha256.Sum256([]byte(entry.SubjectToken))
	if _, ok := s.subjects[hash]; ok {
		return errDuplicateSecret
	}
	if _, ok := s.principals[entry.Subject]; ok {
		return errDuplicateEnrolled
	}
	scopes := []string{CloudPlatform}
	if len(entry.Scopes) > 0 {
		scopes = entry.Scopes
	}
	for _, scope := range scopes {
		if !iam.KnownScope(scope) {
			return errConfiguredScope
		}
	}
	p := principal{entry.Subject, entry.Project, append([]string(nil), scopes...), len(entry.Scopes) > 0}
	s.subjects[hash] = p
	s.principals[p.subject] = p
	return nil
}

// validateEnrollment checks the subject, its project, and its token secret.
func validateEnrollment(entry Enrollment) error {
	if (!strings.HasPrefix(entry.Subject, "user:") && !strings.HasPrefix(entry.Subject, "serviceAccount:")) || !strings.Contains(entry.Subject, "@") || strings.ContainsAny(entry.Subject, "\r\n \t") {
		return errEnrollSubject
	}
	if err := (state.Scope{Project: entry.Project, Location: state.Global}).Validate(); err != nil {
		return fmt.Errorf("enrolled principal scope: %w", err)
	}
	if len(entry.SubjectToken) < 32 || len(entry.SubjectToken) > 256 || strings.ContainsAny(entry.SubjectToken, "\r\n \t") {
		return errSubjectSecret
	}
	return nil
}
func (s *Service) Resolve(ctx context.Context, accessToken string) (gateway.Principal, error) {
	if err := ctx.Err(); err != nil {
		return gateway.Principal{}, err
	}
	if accessToken == "" {
		return gateway.Principal{State: gateway.Anonymous}, nil
	}
	if len(accessToken) > 256 {
		return gateway.Principal{State: gateway.Unknown}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.tokens[sha256.Sum256([]byte(accessToken))]
	if !ok {
		return gateway.Principal{State: gateway.Unknown}, nil
	}
	if !s.now().Before(entry.expires) {
		return gateway.Principal{State: gateway.Expired}, nil
	}
	return gateway.Principal{State: gateway.Authenticated, Subject: entry.principal.subject, Project: entry.principal.project, Scopes: append([]string(nil), entry.scopes...)}, nil
}

// grantedScopes returns the sorted, de-duplicated requested scopes, refusing
// one outside the allowlist or, for a restricted principal, its enrollment.
func grantedScopes(p principal, requested []string) ([]string, error) {
	for _, scope := range requested {
		if !iam.KnownScope(scope) {
			return nil, fmt.Errorf("%w %.128q: gcp-local issues only the Google OAuth scopes listed in docs/local-identity.md", errRequestedScope, scope)
		}
		if p.restricted && !slices.Contains(p.scopes, scope) {
			return nil, fmt.Errorf("%w %.128q", errScopeNotEnrolled, scope)
		}
	}
	if len(requested) == 0 {
		return nil, errScopeRequired
	}
	scopes := slices.Clone(requested)
	slices.Sort(scopes)
	return slices.Compact(scopes), nil
}

// Impersonate issues an access token for an enrolled service account. It is
// an internal provider operation, never an HTTP endpoint, and it does not
// authorize: the calling provider must first authorize its service agent for
// iam.serviceAccounts.getAccessToken on the account with the IAM engine.
func (s *Service) Impersonate(subject string, scopes []string) (string, error) {
	p, ok := s.principals[subject]
	if !ok || !strings.HasPrefix(subject, "serviceAccount:") {
		return "", errImpersonated
	}
	token, _, err := s.mint(p, scopes)
	return token, err
}

func (s *Service) mint(p principal, requested []string) (string, int, error) {
	scopes, err := grantedScopes(p, requested)
	if err != nil {
		return "", 0, err
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", 0, errIssueToken
	}
	value := "gl." + base64.RawURLEncoding.EncodeToString(raw)
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	// Retain recent expired handles for explicit expiry diagnostics, bounded to
	// one hour beyond expiry. Older/pruned handles become unknown.
	for hash, entry := range s.tokens {
		if now.After(entry.expires.Add(time.Hour)) {
			delete(s.tokens, hash)
		}
	}
	if len(s.tokens) >= 10000 {
		return "", 0, errTokenCapacity
	}
	s.tokens[sha256.Sum256([]byte(value))] = token{principal: p, scopes: scopes, expires: now.Add(s.lifetime)}
	return value, int(s.lifetime / time.Second), nil
}
