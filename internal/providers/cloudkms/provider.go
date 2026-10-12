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
	"regexp"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
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
}

// New registers the Cloud KMS provider.
func New(_ context.Context, registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine) (*Provider, error) {
	if registry == nil || manager == nil || index == nil || engine == nil {
		return nil, errProviderShape
	}
	if err := provider.RegisterPermissions(engine, permissions); err != nil {
		return nil, fmt.Errorf("cloudkms permissions: %w", err)
	}
	p := &Provider{
		index: index, engine: engine,
		scopes: state.NewStore(newScopeState, state.GlobalScope),
		clock:  time.Now,
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
		New:      provider.NoBackend,
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

// handle is the gateway entry point for every Cloud KMS method.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Cloud KMS request")
	}
	scope, resource, target, err := p.resolve(call, request)
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
	response, err := p.execute(call, request, scope, resource)
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
	return provider.WireError(403, "PERMISSION_DENIED", decision.Reason+" "+target.Name)
}

// projectMethods and ringMethods are authorized on the project and key ring;
// every other method is authorized on its crypto key.
var (
	projectMethods = map[string]bool{methodLocationsList: true, methodKeyRingsCreate: true, methodKeyRingsList: true}
	ringMethods    = map[string]bool{
		methodKeyRingsGet: true, methodKeyRingsGetIam: true, methodKeyRingsSetIam: true,
		methodCryptoKeysCreate: true, methodCryptoKeysList: true,
	}
	versionNumber = regexp.MustCompile(`^[1-9][0-9]{0,8}$`)
)

// resolve derives the scope, resource reference, and IAM target of one
// request. A resource that does not exist yet is authorized on its nearest
// existing ancestor so the caller sees NOT_FOUND rather than an unverified
// target, matching the real surface's ancestor-inherited permissions.
func (p *Provider) resolve(call gateway.Call, request *gateway.RESTRequest) (state.Scope, ref, iam.Target, error) {
	resource := refOf(request)
	scope := provider.ProjectScope(resource.project)
	project := provider.ProjectResource(resource.project)
	if err := validateRef(resource); err != nil {
		return scope, resource, iam.Target{Name: project}, err
	}
	switch {
	case projectMethods[call.Method]:
		return scope, resource, iam.Target{Name: project}, nil
	case ringMethods[call.Method]:
		return scope, resource, p.anchored(resource.ringResource(), project), nil
	default:
		return scope, resource, p.anchored(resource.keyResource(), resource.ringResource(), project), nil
	}
}

// validateRef rejects malformed path segments before any state access.
func validateRef(resource ref) error {
	if !uid.MatchString(resource.location) {
		return status.Errorf(codes.InvalidArgument, "Invalid location %q", resource.location)
	}
	if resource.ring != "" && !uid.MatchString(resource.ring) {
		return status.Errorf(codes.InvalidArgument, "Invalid key ring name %q", resource.ring)
	}
	if resource.key != "" && !uid.MatchString(resource.key) {
		return status.Errorf(codes.InvalidArgument, "Invalid crypto key name %q", resource.key)
	}
	if resource.version != "" && !versionNumber.MatchString(resource.version) {
		return status.Errorf(codes.InvalidArgument, "Invalid crypto key version %q", resource.version)
	}
	return nil
}

// anchored returns the first indexed candidate, falling back to the last.
func (p *Provider) anchored(candidates ...string) iam.Target {
	for _, name := range candidates[:len(candidates)-1] {
		if _, ok := p.index.Get(name); ok {
			return iam.Target{Name: name}
		}
	}
	return iam.Target{Name: candidates[len(candidates)-1]}
}

// newGCM builds the AES-GCM cipher of one version's material.
func newGCM(material []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(material)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errSeal, err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errSeal, err)
	}
	return gcm, nil
}

// errSeal reports a local crypto failure; the caller adds the operation.
var errSeal = errors.New("local KMS crypto failed")

// errInvalidCiphertext is the real surface's answer to an unopenable payload.
var errInvalidCiphertext = status.Error(codes.InvalidArgument, "Decryption failed: the ciphertext is invalid.")

// readRandom fills one buffer from the local CSPRNG.
func readRandom(buffer []byte) error {
	if _, err := io.ReadFull(rand.Reader, buffer); err != nil {
		return fmt.Errorf("%w: %w", errSeal, err)
	}
	return nil
}

// seal encrypts one plaintext under a version's material. The envelope is
// "version|nonce||ciphertext"; the version label is authenticated with the AAD.
func seal(material, plaintext, aad []byte, version string) ([]byte, error) {
	gcm, err := newGCM(material)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("%w: %w", errSeal, err)
	}
	envelope := append([]byte(version+"|"), nonce...)
	return gcm.Seal(envelope, nonce, plaintext, boundAAD(version, aad)), nil
}

// envelopeVersion reads the sealing version label from one envelope.
func envelopeVersion(sealed []byte) (string, error) {
	split := bytes.IndexByte(sealed, '|')
	if split < 1 || !versionNumber.Match(sealed[:split]) {
		return "", errInvalidCiphertext
	}
	return string(sealed[:split]), nil
}

// open reverses seal with the named version's material.
func open(material, sealed, aad []byte, version string) ([]byte, error) {
	gcm, err := newGCM(material)
	if err != nil {
		return nil, err
	}
	rest := sealed[len(version)+1:]
	if len(rest) < gcm.NonceSize()+gcm.Overhead() {
		return nil, errInvalidCiphertext
	}
	nonce := rest[:gcm.NonceSize()]
	plaintext, err := gcm.Open(nil, nonce, rest[gcm.NonceSize():], boundAAD(version, aad))
	if err != nil {
		return nil, errInvalidCiphertext
	}
	return plaintext, nil
}

// boundAAD binds the caller's AAD to the version label.
func boundAAD(version string, aad []byte) []byte {
	return append([]byte(version+"|"), aad...)
}
