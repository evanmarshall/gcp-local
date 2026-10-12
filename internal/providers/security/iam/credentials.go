package iam

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// userManaged matches a user-managed service-account email and captures its
// project. Delegates and targets named otherwise (unique IDs, Google-managed
// accounts) are a coverage gap.
var userManaged = regexp.MustCompile(`^[a-z][a-z0-9-]{1,28}[a-z0-9]@([a-z][a-z0-9-]{4,28}[a-z0-9])\.iam\.gserviceaccount\.com$`)

// maxJwtExpiry is how far in the future a signJwt exp claim may be (GCP: "no
// more than 12 hours in the future"; recorded as 400 beyond it).
const maxJwtExpiry = 12 * time.Hour

// credentialsGap reports an IAM Credentials request gcp-local does not serve.
func credentialsGap(what string) error {
	return status.Error(codes.Unimplemented, "IAM Credentials coverage gap: "+what)
}

// delegate authorizes every hop into a delegate: the caller on the first and
// each delegate on the next need iam.serviceAccounts.implicitDelegation. It
// returns the call to authorize on the target, acting as the last delegate,
// or the denial of the first hop that fails. Delegates are
// projects/-/serviceAccounts/EMAIL or a bare email, as GCP accepts both
// (iamcredentials-impersonation).
func (p *Provider) delegate(call gateway.Call, request *gateway.RESTRequest) (gateway.Call, *gateway.RESTResponse, error) {
	var body struct {
		Delegates []string `json:"delegates"`
	}
	if len(request.JSON) > 0 && json.Unmarshal(request.JSON, &body) != nil {
		return call, nil, provider.Invalid("Invalid IAM Credentials request body")
	}
	for _, name := range body.Delegates {
		email, project, resource, err := p.delegateAccount(name)
		if err != nil {
			return call, nil, err
		}
		hop := call
		hop.Method, hop.Resource = methodDelegate, resource
		decision, err := p.engine.Authorize(hop, iam.Target{Name: resource}, nil)
		if err != nil {
			if provider.Unverified(err) {
				return call, nil, fmt.Errorf("authorize the delegate %s: %w", email, err)
			}
			return call, provider.Denied(decision, resource), nil
		}
		// The delegate, not the caller, holds the next hop's permission. It
		// presents no token, so no OAuth scope narrows it.
		call.Principal = gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:" + email,
			Project: project, Scopes: []string{identity.CloudPlatform}}
	}
	return call, nil, nil
}

// delegateAccount resolves one delegates entry to an indexed account's email,
// project, and IAM resource name.
func (p *Provider) delegateAccount(name string) (string, string, string, error) {
	email := name
	if parts := strings.Split(name, "/"); len(parts) == 4 && parts[0] == "projects" && parts[2] == "serviceAccounts" {
		email = parts[3]
	}
	match := userManaged.FindStringSubmatch(email)
	if match == nil {
		return "", "", "", credentialsGap("delegates must name a user-managed service account by email")
	}
	resource := accountResource(match[1], email)
	if found, ok := p.index.Get(resource); !ok || found.Kind != kindAccount {
		return "", "", "", provider.NotFound(kindAccount, email)
	}
	return email, match[1], resource, nil
}

// generateAccessToken issues a local access token bound to the account. The
// identity service resolves it at every gcp-local endpoint and introspects it.
func (p *Provider) generateAccessToken(request *gateway.RESTRequest, scope state.Scope, email string) (*gateway.RESTResponse, error) {
	var body struct {
		Scope    []string `json:"scope"`
		Lifetime string   `json:"lifetime"`
	}
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, provider.Invalid("Invalid generateAccessToken request body")
	}
	if p.credentials.Tokens == nil {
		return nil, credentialsGap("access tokens need the local identity service (-identity-config)")
	}
	if len(body.Scope) == 0 {
		return nil, provider.Invalid("Scope required.")
	}
	limit := p.credentials.Tokens.MaxLifetime(email)
	lifetime := identity.DefaultLifetime
	if body.Lifetime != "" {
		parsed, ok := parseLifetime(body.Lifetime)
		if !ok || parsed <= 0 || parsed > limit {
			return nil, provider.Invalid(fmt.Sprintf("The lifetime of the access token must be greater than 0 seconds and at most %d seconds.", int(limit/time.Second)))
		}
		lifetime = parsed
	}
	token, expires, err := p.credentials.Tokens.Impersonate("serviceAccount:"+email, scope.Project, body.Scope, lifetime)
	if err != nil {
		return nil, provider.Invalid(err.Error())
	}
	return &gateway.RESTResponse{JSON: map[string]any{"accessToken": token, "expireTime": expires.UTC().Format(time.RFC3339)}}, nil
}

// parseLifetime reads a JSON google.protobuf.Duration: decimal seconds with
// an "s" suffix.
func parseLifetime(value string) (time.Duration, bool) {
	seconds, err := strconv.ParseFloat(strings.TrimSuffix(value, "s"), 64)
	if err != nil || !strings.HasSuffix(value, "s") || seconds > float64(identity.ExtendedLifetime/time.Second) {
		return 0, false
	}
	return time.Duration(seconds * float64(time.Second)), true
}

// generateIDToken issues a one-hour local ID token for the account, with its
// email only when includeEmail is set.
func (p *Provider) generateIDToken(request *gateway.RESTRequest, scope state.Scope, email string) (*gateway.RESTResponse, error) {
	var body struct {
		Audience     string `json:"audience"`
		IncludeEmail bool   `json:"includeEmail"`
	}
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, provider.Invalid("Invalid generateIdToken request body")
	}
	if p.credentials.IDTokens == nil {
		return nil, credentialsGap("ID tokens need the local ID-token keys")
	}
	if body.Audience == "" {
		return nil, provider.Invalid("Audience required.")
	}
	resource, ok := p.index.Get(accountResource(scope.Project, email))
	if !ok || resource.Number == "" {
		return nil, credentialsGap("ID tokens need an account with a numeric unique ID")
	}
	token, err := p.credentials.IDTokens.Issue(resource.Number, email, body.Audience, body.IncludeEmail)
	if err != nil {
		return nil, err //nolint:wrapcheck // the audience refusal is a wire status
	}
	return &gateway.RESTResponse{JSON: map[string]any{"token": token}}, nil
}

// signJwt signs a JSON claim set unchanged with the account's Google-managed
// key, under the header {"alg":"RS256","kid":KEY_ID,"typ":"JWT"}.
func (p *Provider) signJwt(request *gateway.RESTRequest, scope state.Scope, email string) (*gateway.RESTResponse, error) {
	var body struct {
		Payload string `json:"payload"`
	}
	var claims struct {
		Expiry *float64 `json:"exp"`
	}
	if json.Unmarshal(request.JSON, &body) != nil || len(body.Payload) > maxSignBlobPayload || json.Unmarshal([]byte(body.Payload), &claims) != nil {
		return nil, provider.Invalid("payload must be a JSON claim set of at most 64 KiB")
	}
	if claims.Expiry != nil && *claims.Expiry > float64(p.clock().Add(maxJwtExpiry).Unix()) {
		return nil, provider.Invalid("The exp claim must be no more than 12 hours in the future.")
	}
	key, err := p.systemKey(scope, email)
	if err != nil {
		return nil, err
	}
	header, err := json.Marshal(map[string]string{"alg": "RS256", "kid": key.ID, "typ": "JWT"})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode the JWT header: %v", err)
	}
	message := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString([]byte(body.Payload))
	digest := sha256.Sum256([]byte(message))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key.Private, crypto.SHA256, digest[:])
	if err != nil {
		return nil, status.Errorf(codes.Internal, "sign the JWT: %v", err)
	}
	return &gateway.RESTResponse{JSON: map[string]any{"keyId": key.ID, "signedJwt": message + "." + base64.RawURLEncoding.EncodeToString(signature)}}, nil
}

// maxSignBlobPayload bounds one signBlob payload; V4 string-to-sign values are
// a few hundred bytes.
const maxSignBlobPayload = 64 << 10

// newKeyID returns a 40-hex-digit key ID, the shape GCP assigns.
func newKeyID() (string, error) {
	raw := make([]byte, 20)
	if _, err := rand.Read(raw); err != nil {
		return "", status.Errorf(codes.Internal, "generate the key id: %v", err)
	}
	return hex.EncodeToString(raw), nil
}

// newRSAKey generates one 2048-bit signing key, the KEY_ALG_RSA_2048 shape.
func newRSAKey() (*rsa.PrivateKey, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "generate the key: %v", err)
	}
	return key, nil
}

// keyFile renders the TYPE_GOOGLE_CREDENTIALS_FILE document clients load with
// from_service_account_info or google.CredentialsFromJSON. token_uri names this
// emulator, so a client that tries to mint a token with the key never reaches
// Google; gcp-local does not accept JWT-bearer grants, so that exchange fails.
func keyFile(record *accountRecord, keyID string, key *rsa.PrivateKey, host string) ([]byte, error) {
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode the key: %v", err)
	}
	document, err := json.Marshal(map[string]string{
		"type":           "service_account",
		"project_id":     record.Project,
		"private_key_id": keyID,
		"private_key":    string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
		"client_email":   accountEmail(record.Project, record.ID),
		"client_id":      accountUniqueID(record),
		"token_uri":      "http://" + host + "/v1/token",
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode the key file: %v", err)
	}
	return document, nil
}

// signBlob signs a payload with the account's Google-managed key, creating the
// key on first use and publishing its public half for verifiers.
func (p *Provider) signBlob(request *gateway.RESTRequest, scope state.Scope, email string) (*gateway.RESTResponse, error) {
	var body struct {
		Payload []byte `json:"payload"`
	}
	if json.Unmarshal(request.JSON, &body) != nil || len(body.Payload) == 0 || len(body.Payload) > maxSignBlobPayload {
		return nil, status.Error(codes.InvalidArgument, "payload must be base64 data of at most 64 KiB")
	}
	key, err := p.systemKey(scope, email)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(body.Payload)
	signature, err := rsa.SignPKCS1v15(rand.Reader, key.Private, crypto.SHA256, digest[:])
	if err != nil {
		return nil, status.Errorf(codes.Internal, "sign the payload: %v", err)
	}
	return &gateway.RESTResponse{JSON: map[string]any{"keyId": key.ID, "signedBlob": base64.StdEncoding.EncodeToString(signature)}}, nil
}

// systemKey returns the account's Google-managed signing key, creating it once.
// A key generated by a losing concurrent caller is discarded. It returns a
// copy; the private key it shares is never modified after generation.
func (p *Provider) systemKey(scope state.Scope, email string) (systemKey, error) {
	var key systemKey
	_ = p.scopes.With(scope, func(s *scopeState) error {
		if stored := s.systemKeys[email]; stored != nil {
			key = *stored
		}
		return nil
	})
	if key.Private != nil {
		return key, nil
	}
	id, err := newKeyID()
	if err != nil {
		return key, err
	}
	private, err := newRSAKey()
	if err != nil {
		return key, err
	}
	_ = p.scopes.With(scope, func(s *scopeState) error {
		stored := s.systemKeys[email]
		if stored == nil {
			stored = &systemKey{ID: id, Private: private}
			s.systemKeys[email] = stored
			p.engine.PublishSigningKey(serviceName, scope, email, id, &private.PublicKey)
		}
		key = *stored
		return nil
	})
	return key, nil
}
