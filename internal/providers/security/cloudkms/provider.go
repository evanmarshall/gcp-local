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
	"regexp"
	"strings"
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
	index   *state.Index
	engine  *iam.Engine
	gate    provider.Gate
	manager *backend.Manager
	scopes  *state.Store[scopeState]
	clock   func() time.Time
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
		index: index, engine: engine, manager: manager,
		// Recorded: cloudkms-symmetric-key-lifecycle "denied-encrypt".
		gate: provider.Gate{Engine: engine, Index: index, Message: provider.PermissionDeniedOnResource,
			Domain: "cloudkms.googleapis.com", OmitResource: true, Attributes: attributes},
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

// handle is the gateway entry point for every Cloud KMS method. It runs in
// the manager session, which serializes it with a reset, and authorizes on
// the nearest existing resource under the scope lock, so a missing resource
// reads NOT_FOUND to a permitted caller and a denial otherwise.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Cloud KMS request")
	}
	resource := refOf(request)
	if err := validateRef(resource); err != nil {
		return nil, err
	}
	scope := provider.ProjectScope(resource.project)
	var response *gateway.RESTResponse
	err := p.manager.Use(call.Context, backend.Key{Service: serviceName, Scope: scope}, func(ctx context.Context, _ backend.Backend) error {
		call.Context = ctx //nolint:fatcontext // one request-scoped handoff of the manager session
		checked, candidates := checkedOn(call.Method, resource)
		decision, target, err := provider.Nearest(p.gate, p.scopes, scope, call, checked, candidates...)
		if err != nil {
			return err
		}
		call.Resource = target.Name
		call.Location = scope.Location
		executed, err := p.execute(call, request, scope, resource)
		if err != nil {
			return err
		}
		response = provider.WithDecision(executed, decision)
		return nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // wire statuses pass through
	}
	return response, nil
}

// Methods by the resource their permission is checked on; every other method
// is checked on its crypto key.
var (
	projectMethods = map[string]bool{
		methodLocationsGet: true, methodKeyRingsCreate: true, methodKeyRingsList: true,
	}
	ringMethods = map[string]bool{
		methodKeyRingsGet: true, methodKeyRingsGetIam: true, methodKeyRingsSetIam: true,
		methodCryptoKeysCreate: true, methodCryptoKeysList: true,
	}
	versionMethods = map[string]bool{
		methodVersionsGet: true, methodVersionsPatch: true, methodVersionsDestroy: true, methodVersionsRestore: true,
		methodAsymmetricSign: true, methodAsymmetricDecrypt: true, methodGetPublicKey: true,
		methodMacSign: true, methodMacVerify: true,
	}
	versionNumber = regexp.MustCompile(`^[1-9][0-9]{0,8}$`)
)

// checkedOn returns the relative name a method's permission is checked on,
// which a denial reports, and the IAM targets to authorize at, most specific
// first. Versions are not IAM resources; their key's policy applies.
func checkedOn(method string, resource ref) (string, []string) {
	project := provider.ProjectResource(resource.project)
	switch {
	case projectMethods[method]:
		return "projects/" + resource.project + "/locations/" + resource.location, []string{project}
	case ringMethods[method]:
		return resource.ringName(), []string{resource.ringResource(), project}
	case versionMethods[method]:
		return resource.versionName(resource.version), []string{resource.keyResource(), resource.ringResource(), project}
	default:
		return resource.keyName(), []string{resource.keyResource(), resource.ringResource(), project}
	}
}

// kinds names the KMS resource types by the collection that ends a name.
var kinds = map[string]string{"keyRings": "KeyRing", "cryptoKeys": "CryptoKey", "cryptoKeyVersions": "CryptoKeyVersion"}

// attributes are the IAM condition attributes of the resource a permission is
// checked on, whichever policy holder Nearest picked: a version method sees
// the version (recorded: cloudkms-symmetric-key-lifecycle "conditional-access",
// where a CryptoKey condition grants the key and its version list, not a
// version get). Locations carry none.
func attributes(_, resource string) iam.ResourceAttributes {
	segments := strings.Split(resource, "/")
	if len(segments) < 2 || kinds[segments[len(segments)-2]] == "" {
		return iam.ResourceAttributes{}
	}
	return iam.ResourceAttributes{Name: resource, Type: "cloudkms.googleapis.com/" + kinds[segments[len(segments)-2]],
		Service: "cloudkms.googleapis.com"}
}

// validateRef rejects malformed path segments before any state access.
func validateRef(resource ref) error {
	if resource.location != "" && !uid.MatchString(resource.location) {
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
