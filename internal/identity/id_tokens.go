package identity

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
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// IDTokenIssuer is deliberately local: tokens never claim Google's signing
// authority. Applications configure this issuer and the runtime's public key.
const IDTokenIssuer = "https://identity.gcp-local.invalid"    //nolint:gosec // a public local issuer URL, not a credential
const generateIDToken = "iam.serviceAccounts.generateIdToken" //nolint:gosec // an RPC method name, not a credential

type IDClaims struct {
	Issuer        string `json:"iss"`
	Subject       string `json:"sub"`
	Audience      string `json:"aud"`
	Email         string `json:"email"`
	EmailVerified bool   `json:"email_verified"`
	IssuedAt      int64  `json:"iat"`
	ExpiresAt     int64  `json:"exp"`
}
type IDTokens struct {
	key              *rsa.PrivateKey
	kid, certificate string
	engine           *iam.Engine
	index            *state.Index
	now              func() time.Time
}

func NewIDTokens(engine *iam.Engine, index *state.Index) (*IDTokens, error) {
	if engine == nil || index == nil {
		return nil, status.Error(codes.InvalidArgument, "ID tokens require IAM and an explicit account index")
	}
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("generate id token key: %w", err)
	}
	public, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("marshal id token public key: %w", err)
	}
	sum := sha256.Sum256(public)
	if err := engine.Register(generateIDToken, iam.Rule{Baseline: []string{"iam.serviceAccounts.getOpenIdToken"}}); err != nil {
		return nil, fmt.Errorf("register id token method: %w", err)
	}
	return &IDTokens{key: key, kid: hex.EncodeToString(sum[:16]), certificate: string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: public})), engine: engine, index: index, now: time.Now}, nil
}

func (s *IDTokens) Certificates() map[string]string { return map[string]string{s.kid: s.certificate} }

// Mint is an internal provider operation, never an unauthenticated HTTP mint
// endpoint. Every call evaluates the acting principal on the target account.
func (s *IDTokens) Mint(call gateway.Call, account, audience string) (string, error) {
	if call.Principal.State != gateway.Authenticated {
		return "", status.Error(codes.Unauthenticated, "ID-token minting requires an authenticated acting principal")
	}
	if audience == "" || len(audience) > 2048 || strings.ContainsAny(audience, "\r\n") {
		return "", status.Error(codes.InvalidArgument, "Invalid ID-token audience")
	}
	resource, ok := s.index.Get(account)
	if !ok || resource.Service != "iam" || resource.Kind != "serviceAccount" || resource.Number == "" {
		return "", status.Error(codes.Unimplemented, "Configure an explicit local service account before minting ID tokens")
	}
	email := strings.TrimPrefix(resource.Name, "//iam.googleapis.com/projects/"+resource.Scope.Project+"/serviceAccounts/")
	if email == resource.Name || !strings.HasSuffix(email, "@"+resource.Scope.Project+".iam.gserviceaccount.com") {
		return "", status.Error(codes.Unimplemented, "Invalid service account ownership")
	}
	call.Method, call.Project, call.Resource = generateIDToken, resource.Scope.Project, account
	if _, err := s.engine.Authorize(call, iam.Target{Name: account}, nil); err != nil {
		return "", fmt.Errorf("authorize id token request: %w", err)
	}
	now := s.now().Unix()
	header, err := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT", "kid": s.kid})
	if err != nil {
		return "", fmt.Errorf("encode id token header: %w", err)
	}
	claims, err := json.Marshal(IDClaims{Issuer: IDTokenIssuer, Subject: resource.Number, Audience: audience, Email: email, EmailVerified: true, IssuedAt: now, ExpiresAt: now + 300})
	if err != nil {
		return "", fmt.Errorf("encode id token claims: %w", err)
	}
	message := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(claims)
	digest := sha256.Sum256([]byte(message))
	signature, err := rsa.SignPKCS1v15(rand.Reader, s.key, crypto.SHA256, digest[:])
	if err != nil {
		return "", fmt.Errorf("sign id token: %w", err)
	}
	return message + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

// Verify validates only this runtime's key, issuer, audience, and token lifetime.
// It performs no discovery, Google key fetch, or principal authorization.
func (s *IDTokens) Verify(token, audience string) (IDClaims, error) {
	fail := func() (IDClaims, error) {
		return IDClaims{}, status.Error(codes.Unauthenticated, "Invalid local ID token")
	}
	if len(token) > 8192 || audience == "" {
		return fail()
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return fail()
	}
	header, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return fail()
	}
	var properties struct {
		Alg string `json:"alg"`
		Typ string `json:"typ"`
		Kid string `json:"kid"`
	}
	if json.Unmarshal(header, &properties) != nil || properties.Alg != "RS256" || properties.Typ != "JWT" || properties.Kid != s.kid {
		return fail()
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return fail()
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if rsa.VerifyPKCS1v15(&s.key.PublicKey, crypto.SHA256, digest[:], signature) != nil {
		return fail()
	}
	encoded, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return fail()
	}
	var claims IDClaims
	if json.Unmarshal(encoded, &claims) != nil {
		return fail()
	}
	now := s.now().Unix()
	if claims.Issuer != IDTokenIssuer || claims.Audience != audience || claims.Subject == "" || claims.Email == "" || !claims.EmailVerified || claims.IssuedAt > now || claims.ExpiresAt <= now || claims.ExpiresAt-claims.IssuedAt != 300 {
		return fail()
	}
	return claims, nil
}
