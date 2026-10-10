package cloudkms

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Provider owns the Cloud KMS control plane and key material state.
type Provider struct {
	index  *state.Index
	engine *iam.Engine
	scopes *state.Store[scopeState]
	clock  func() time.Time
	key    []byte
	mu     sync.Mutex
}

// New registers the Cloud KMS provider.
func New(_ context.Context, registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine) (*Provider, error) {
	if registry == nil || manager == nil || index == nil || engine == nil {
		return nil, errProviderShape
	}
	if err := registerPermissions(engine); err != nil {
		return nil, err
	}
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return nil, fmt.Errorf("generate the local key-encryption key: %w", err)
	}
	p := &Provider{
		index: index, engine: engine,
		scopes: state.NewStore(newScopeState, state.GlobalScope),
		clock:  time.Now, key: key,
	}
	if err := p.registerBackend(manager, engine, index); err != nil {
		return nil, err
	}
	for method := range permissions {
		if err := registry.Register(method, p.handle); err != nil {
			return nil, fmt.Errorf("register %s: %w", method, err)
		}
	}
	return p, nil
}

// registerBackend attaches the in-memory KMS backend and its reset hook.
func (p *Provider) registerBackend(manager *backend.Manager, engine *iam.Engine, index *state.Index) error {
	if err := manager.Register(serviceName, backend.Definition{
		Validate: state.GlobalScope,
		New:      func(state.Scope) (backend.Backend, error) { return emptyBackend{}, nil },
		AfterReset: func(scope state.Scope) error {
			if err := p.Reset(scope); err != nil {
				return err
			}
			engine.ClearScope(serviceName, scope)
			index.RemoveScope(serviceName, scope)
			return nil
		},
	}); err != nil {
		return fmt.Errorf("register the Cloud KMS backend: %w", err)
	}
	return nil
}

// emptyBackend satisfies the backend contract for an in-memory provider.
type emptyBackend struct{}

func (emptyBackend) Start(context.Context) error    { return nil }
func (emptyBackend) Ready(context.Context) error    { return nil }
func (emptyBackend) Stop(context.Context) error     { return nil }
func (emptyBackend) Reset(context.Context) error    { return nil }
func (emptyBackend) Snapshot(context.Context) error { return backend.ErrPersistenceUnsupported }
func (emptyBackend) Restore(context.Context) error  { return backend.ErrPersistenceUnsupported }

// Reset clears the scope's key rings, keys, and version material.
func (p *Provider) Reset(scope state.Scope) error {
	if err := p.scopes.With(scope, func(s *scopeState) error {
		s.reset()
		return nil
	}); err != nil {
		return fmt.Errorf("clear the cloudkms scope: %w", err)
	}
	return nil
}

// registerPermissions binds every method to its permission.
func registerPermissions(engine *iam.Engine) error {
	for method, permission := range permissions {
		if err := engine.Register(method, iam.Rule{Baseline: []string{permission}}); err != nil {
			return fmt.Errorf("register %s: %w", method, err)
		}
	}
	return nil
}

// handle is the gateway entry point for every Cloud KMS method.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Cloud KMS request")
	}
	scope, target, err := p.resolve(call, request)
	if err != nil {
		return nil, err
	}
	decision, authorizeErr := p.engine.Authorize(call, target, nil)
	if authorizeErr != nil {
		code := status.Code(authorizeErr)
		if code != codes.PermissionDenied && code != codes.Unauthenticated {
			return nil, fmt.Errorf("authorize %s: %w", call.Method, authorizeErr)
		}
		return p.denied(call, decision, target), nil
	}
	call.Resource = target.Name
	call.Location = scope.Location
	response, err := p.execute(call, request, scope, target)
	if err != nil {
		return nil, err
	}
	if response.Headers == nil {
		response.Headers = http.Header{}
	}
	response.Headers.Set("X-Gcp-Local-Decision-Id", decision.ID)
	return response, nil
}

// denied renders the KMS denial shape.
func (p *Provider) denied(_ gateway.Call, decision iam.Decision, target iam.Target) *gateway.RESTResponse {
	return wireError(403, "PERMISSION_DENIED", decision.Reason+" "+target.Name)
}

// resolve derives the scope and IAM target for one request.
func (p *Provider) resolve(call gateway.Call, request *gateway.RESTRequest) (state.Scope, iam.Target, error) {
	project := request.Path["project"]
	scope := scopeOf(project)
	target := iam.Target{Name: projectResource(project)}
	ring := request.Path["keyRing"]
	switch call.Method {
	case methodLocationsList, methodKeyRingsCreate, methodKeyRingsList:
		return scope, target, nil
	case methodKeyRingsGet, methodKeyRingsGetIam, methodKeyRingsSetIam:
		return p.ringTarget(scope, target, ring)
	case methodCryptoKeysCreate, methodCryptoKeysList, methodVersionsCreate, methodVersionsList:
		return p.keyCollectionTarget(scope, target, ring)
	case methodCryptoKeysGet, methodCryptoKeysUpdate, methodCryptoKeysGetIam,
		methodCryptoKeysSetIam, methodCryptoKeysEncrypt, methodCryptoKeysDecrypt,
		methodVersionsGet, methodVersionsDestroy, methodVersionsDisable, methodVersionsEnable:
		return p.keyTarget(scope, target, ring, request.Path["cryptoKey"])
	default:
		return scope, target, status.Errorf(codes.Internal, "Unhandled KMS method %s", call.Method)
	}
}

// ringTarget resolves the key ring resource targets.
func (p *Provider) ringTarget(scope state.Scope, target iam.Target, ring string) (state.Scope, iam.Target, error) {
	if !uid.MatchString(ring) {
		return scope, target, status.Errorf(codes.InvalidArgument, "Invalid key ring name %q", ring)
	}
	target.Name = keyRingResource(scope.Project, ring)
	return scope, target, nil
}

// keyCollectionTarget resolves the targets of ring-scoped collections.
func (p *Provider) keyCollectionTarget(scope state.Scope, target iam.Target, ring string) (state.Scope, iam.Target, error) {
	if !uid.MatchString(ring) {
		return scope, target, status.Errorf(codes.InvalidArgument, "Invalid key ring name %q", ring)
	}
	target.Name = keyRingResource(scope.Project, ring)
	return scope, target, nil
}

// keyTarget resolves the key resource targets; version methods inherit the
// key's policy, matching the real surface's permission evaluation.
func (p *Provider) keyTarget(scope state.Scope, target iam.Target, ring, key string) (state.Scope, iam.Target, error) {
	if !uid.MatchString(ring) || !uid.MatchString(key) {
		return scope, target, status.Errorf(codes.InvalidArgument, "Invalid crypto key name %q", key)
	}
	target.Name = cryptoKeyResource(scope.Project, ring, key)
	return scope, target, nil
}

// errSeal reports a local crypto failure; the caller adds the operation.
var errSeal = errors.New("local KMS crypto failed")

// readRandom fills one buffer from the local CSPRNG.
func readRandom(buffer []byte) (int, error) {
	n, err := rand.Read(buffer)
	if err != nil {
		return n, fmt.Errorf("%w: %w", errSeal, err)
	}
	return n, nil
}

// random fills one buffer with local randomness under the provider lock.
func (p *Provider) random(buffer []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, err := readRandom(buffer)
	return err
}

// seal encrypts one plaintext under the provider-held local key, prefixing
// the payload with the version number so decrypt reports the true origin.
func (p *Provider) seal(plaintext, aad []byte, version string) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	block, err := aes.NewCipher(p.key)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errSeal, err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errSeal, err)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("%w: %w", errSeal, err)
	}
	if len(version) > 255 {
		return nil, status.Error(codes.InvalidArgument, "Version label is too long")
	}
	marker := make([]byte, 1, 1+gcm.NonceSize())
	marker[0] = byte(len(version)) //nolint:gosec // the length is bounded above
	sealed := gcm.Seal(append(marker, nonce...), nonce, plaintext, append(aad, []byte(version)...))
	return append([]byte(version+"|"), sealed...), nil
}

// open reverses seal and returns the plaintext with the sealing version.
func (p *Provider) open(sealed, aad []byte) ([]byte, string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	block, err := aes.NewCipher(p.key)
	if err != nil {
		return nil, "", fmt.Errorf("%w: %w", errSeal, err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, "", fmt.Errorf("%w: %w", errSeal, err)
	}
	split := bytes.IndexByte(sealed, '|')
	if split < 0 || split > 8 {
		return nil, "", status.Error(codes.FailedPrecondition, "Ciphertext envelope is not from this key")
	}
	version := string(sealed[:split])
	rest := sealed[split+1:]
	if len(rest) < gcm.NonceSize()+1 {
		return nil, "", status.Error(codes.FailedPrecondition, "Ciphertext is truncated")
	}
	markerLength := int(rest[0])
	if markerLength != len(version) {
		return nil, "", status.Error(codes.FailedPrecondition, "Ciphertext envelope is corrupted")
	}
	nonce := rest[1 : 1+gcm.NonceSize()]
	plaintext, err := gcm.Open(nil, nonce, rest[1+gcm.NonceSize():], append(aad, version...))
	if err != nil {
		return nil, "", status.Error(codes.FailedPrecondition, "Decryption failed: the ciphertext is not sealed by this key")
	}
	return plaintext, version, nil
}
