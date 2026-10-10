package cloudkms

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// execute dispatches one authorized KMS request.
func (p *Provider) execute(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, target iam.Target) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodLocationsList:
		return locationsList(scope)
	case methodKeyRingsCreate:
		return p.keyRingsCreate(request, scope)
	case methodKeyRingsGet:
		return p.keyRingsGet(request, scope)
	case methodKeyRingsList:
		return p.keyRingsList(scope)
	default:
		return p.executeKeys(call, request, scope, target)
	}
}

// executeVersions dispatches the crypto key version methods.
func (p *Provider) executeVersions(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodVersionsCreate:
		return p.versionsCreate(request, scope)
	case methodVersionsList:
		return p.versionsList(request, scope)
	case methodVersionsGet:
		return p.versionsGet(request, scope)
	case methodVersionsDestroy:
		return p.versionsState(request, scope, stateDestroyed)
	case methodVersionsDisable:
		return p.versionsState(request, scope, stateDisabled)
	default:
		return p.versionsState(request, scope, stateEnabled)
	}
}

// executeKeys dispatches the crypto key and version methods.
func (p *Provider) executeKeys(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, target iam.Target) (*gateway.RESTResponse, error) {
	if strings.HasPrefix(call.Method, "cloudkms.cryptoKeyVersions.") {
		return p.executeVersions(call, request, scope)
	}
	switch call.Method {
	case methodCryptoKeysGetIam, methodKeyRingsGetIam:
		return p.getIam(request, scope, target)
	case methodCryptoKeysSetIam, methodKeyRingsSetIam:
		return p.setIam(request, scope, target)
	case methodCryptoKeysCreate:
		return p.cryptoKeysCreate(request, scope)
	case methodCryptoKeysGet:
		return p.cryptoKeysGet(request, scope)
	case methodCryptoKeysList:
		return p.cryptoKeysList(request, scope)
	case methodCryptoKeysUpdate:
		return p.cryptoKeysUpdate(request, scope)
	case methodCryptoKeysEncrypt:
		return p.encrypt(request, scope)
	default:
		return p.decrypt(request, scope)
	}
}

// locationsList returns the modeled KMS locations.
func locationsList(scope state.Scope) (*gateway.RESTResponse, error) {
	return &gateway.RESTResponse{JSON: map[string]any{"locations": []any{
		map[string]any{"name": "projects/" + scope.Project + "/locations/" + testLocation,
			"locationId": testLocation, "displayName": testLocation},
	}}}, nil
}

// keyRingsCreate validates and stores one key ring.
func (p *Provider) keyRingsCreate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	ring := request.Query.Get("keyRingId")
	if !uid.MatchString(ring) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid key ring id %q", ring)
	}
	var created *keyRingRecord
	err := p.scopes.With(scope, func(s *scopeState) error {
		if s.keyRings[ring] != nil {
			return status.Errorf(codes.AlreadyExists, "The key ring %s already exists", ring)
		}
		created = &keyRingRecord{Name: ring, CreatedAt: p.clock()}
		s.keyRings[ring] = created
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("create the key ring: %w", err)
	}
	if indexErr := p.index.Put(serviceName, state.Resource{
		Name: keyRingResource(scope.Project, ring), Service: serviceName,
		Kind: kindKeyRing, Parent: projectResource(scope.Project), Scope: scope,
	}); indexErr != nil {
		return nil, gap("index the key ring")
	}
	return &gateway.RESTResponse{JSON: keyRingDocument(scope.Project, created)}, nil
}

// keyRingDocument renders the key ring wire shape.
func keyRingDocument(project string, record *keyRingRecord) map[string]any {
	return map[string]any{
		keyName:       fullName(project, ringLocation(record.Name), kindKeyRing, record.Name),
		keyCreateTime: stamp(record.CreatedAt),
	}
}

// keyRingsGet returns one key ring.
func (p *Provider) keyRingsGet(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	ring := request.Path["keyRing"]
	var record *keyRingRecord
	_ = p.scopes.With(scope, func(s *scopeState) error { record = s.keyRings[ring]; return nil })
	if record == nil {
		return nil, notFound(kindKeyRing, ring)
	}
	return &gateway.RESTResponse{JSON: keyRingDocument(scope.Project, record)}, nil
}

// keyRingsList returns the scope's key rings.
func (p *Provider) keyRingsList(scope state.Scope) (*gateway.RESTResponse, error) {
	items := []any{}
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for _, record := range s.keyRings {
			items = append(items, keyRingDocument(scope.Project, record))
		}
		return nil
	})
	return &gateway.RESTResponse{JSON: map[string]any{"keyRings": items}}, nil
}

// cryptoKeysCreate validates and stores one symmetric crypto key with version 1.
func (p *Provider) cryptoKeysCreate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	ring := request.Path["keyRing"]
	if p.ring(scope, ring) == nil {
		return nil, notFound(kindKeyRing, ring)
	}
	key := request.Query.Get("cryptoKeyId")
	if !uid.MatchString(key) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid crypto key id %q", key)
	}
	body, parseErr := parseCryptoKeyBody(request.JSON)
	if parseErr != nil {
		return nil, parseErr
	}
	var created *cryptoKeyRecord
	err := p.scopes.With(scope, func(s *scopeState) error {
		if s.keyRings[ring] == nil {
			return notFound(kindKeyRing, ring)
		}
		if s.cryptoKeys[key] != nil {
			return status.Errorf(codes.AlreadyExists, "The crypto key %s already exists", key)
		}
		material := make([]byte, 32)
		if err := p.random(material); err != nil {
			return err
		}
		version := &versionRecord{Number: "1", State: stateEnabled, Material: material, CreatedAt: p.clock()}
		created = &cryptoKeyRecord{
			Name: key, Purpose: body.Purpose, Algorithm: body.Algorithm,
			Labels: body.Labels, Rotation: body.RotationPeriod,
			NextRotation: body.NextRotation,
			Versions:     map[string]*versionRecord{"1": version},
			Primary:      "1", CreatedAt: p.clock(),
		}
		if body.SkipInitial {
			version.State = stateDestroyed
			version.Material = nil
			created.Primary = ""
		}
		s.cryptoKeys[key] = created
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("create the crypto key: %w", err)
	}
	if indexErr := p.index.Put(serviceName, state.Resource{
		Name: cryptoKeyResource(scope.Project, ring, key), Service: serviceName,
		Kind: kindCryptoKey, Parent: keyRingResource(scope.Project, ring), Scope: scope,
	}); indexErr != nil {
		return nil, gap("index the crypto key")
	}
	return &gateway.RESTResponse{JSON: p.cryptoKeyDocument(scope, ring, created)}, nil
}

// ring returns one key ring record.
func (p *Provider) ring(scope state.Scope, ring string) *keyRingRecord {
	var record *keyRingRecord
	_ = p.scopes.With(scope, func(s *scopeState) error { record = s.keyRings[ring]; return nil })
	return record
}

// cryptoKeyDocument renders the crypto key wire shape.
func (p *Provider) cryptoKeyDocument(scope state.Scope, ring string, record *cryptoKeyRecord) map[string]any {
	primary := map[string]any{}
	if record.Primary != "" {
		primary = map[string]any{
			keyName: cryptoKeyName(scope.Project, ring, record.Name) + "/cryptoKeyVersions/" + record.Primary,
			"state": record.Versions[record.Primary].State,
		}
	}
	labels := record.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	return map[string]any{
		keyName:            cryptoKeyName(scope.Project, ring, record.Name),
		keyCreateTime:      stamp(record.CreatedAt),
		"purpose":          record.Purpose,
		"versionTemplate":  map[string]any{"algorithm": record.Algorithm, "protectionLevel": "SOFTWARE"},
		"primary":          primary,
		"labels":           labels,
		"rotationPeriod":   record.Rotation,
		"nextRotationTime": record.NextRotation,
	}
}

// cryptoKeysGet returns one crypto key.
func (p *Provider) cryptoKeysGet(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	record := p.lookupKey(scope, request.Path["keyRing"], request.Path["cryptoKey"])
	if record == nil {
		return nil, notFound(kindCryptoKey, request.Path["cryptoKey"])
	}
	return &gateway.RESTResponse{JSON: p.cryptoKeyDocument(scope, request.Path["keyRing"], record)}, nil
}

// key returns one crypto key record.
func (p *Provider) lookupKey(scope state.Scope, ring, key string) *cryptoKeyRecord {
	var record *cryptoKeyRecord
	_ = p.scopes.With(scope, func(s *scopeState) error {
		if s.keyRings[ring] != nil {
			record = s.cryptoKeys[key]
		}
		return nil
	})
	return record
}

// cryptoKeysList returns the key ring's crypto keys.
func (p *Provider) cryptoKeysList(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	ring := request.Path["keyRing"]
	items := []any{}
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for _, record := range s.cryptoKeys {
			items = append(items, p.cryptoKeyDocument(scope, ring, record))
		}
		return nil
	})
	return &gateway.RESTResponse{JSON: map[string]any{"cryptoKeys": items}}, nil
}

// cryptoKeysUpdate replaces labels, rotation, or the primary version.
func (p *Provider) cryptoKeysUpdate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	ring, key := request.Path["keyRing"], request.Path["cryptoKey"]
	record := p.lookupKey(scope, ring, key)
	if record == nil {
		return nil, notFound(kindCryptoKey, key)
	}
	var body struct {
		Labels       map[string]string `json:"labels"`
		Rotation     string            `json:"rotationPeriod"`
		NextRotation string            `json:"nextRotationTime"`
		Primary      string            `json:"primary"`
		PrimaryState string            `json:"primaryState"`
	}
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid crypto key update")
	}
	if body.Primary != "" {
		if record.Versions[body.Primary] == nil {
			return nil, notFound("cryptoKeyVersion", body.Primary)
		}
		record.Primary = body.Primary
	}
	if body.PrimaryState != "" {
		return nil, gap("primary version state updates")
	}
	record.Labels = body.Labels
	record.Rotation = body.Rotation
	record.NextRotation = body.NextRotation
	return &gateway.RESTResponse{JSON: p.cryptoKeyDocument(scope, ring, record)}, nil
}

// encrypt seals one plaintext with the primary version's material.
func (p *Provider) encrypt(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	ring, key := request.Path["keyRing"], request.Path["cryptoKey"]
	record := p.lookupKey(scope, ring, key)
	if record == nil {
		return nil, notFound(kindCryptoKey, key)
	}
	var body struct {
		Plaintext string `json:"plaintext"`
		AAD       string `json:"additionalAuthenticatedData"`
	}
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid encrypt request")
	}
	plaintext, err := base64.StdEncoding.DecodeString(body.Plaintext)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "Plaintext must be base64")
	}
	if len(plaintext) == 0 || len(plaintext) > 64<<10 {
		return nil, status.Error(codes.InvalidArgument, "Plaintext is empty or oversized")
	}
	version := record.Versions[record.Primary]
	if version == nil || version.State != stateEnabled {
		return nil, status.Errorf(codes.FailedPrecondition, "The key has no enabled primary version")
	}
	aad, aadErr := base64.StdEncoding.DecodeString(body.AAD)
	if aadErr != nil {
		return nil, status.Error(codes.InvalidArgument, "AAD must be base64")
	}
	sealed, sealErr := p.seal(plaintext, aad, record.Primary)
	if sealErr != nil {
		return nil, status.Errorf(codes.Internal, "Local encryption failed: %v", sealErr)
	}
	return &gateway.RESTResponse{JSON: map[string]any{
		keyName:      cryptoKeyName(scope.Project, ring, key) + "/cryptoKeyVersions/" + record.Primary,
		"ciphertext": base64.StdEncoding.EncodeToString(sealed),
	}}, nil
}

// decrypt opens one sealed payload with an enabled version's material.
func (p *Provider) decrypt(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	ring, key := request.Path["keyRing"], request.Path["cryptoKey"]
	record := p.lookupKey(scope, ring, key)
	if record == nil {
		return nil, notFound(kindCryptoKey, key)
	}
	var body struct {
		Ciphertext string `json:"ciphertext"`
		AAD        string `json:"additionalAuthenticatedData"`
	}
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid decrypt request")
	}
	sealed, err := base64.StdEncoding.DecodeString(body.Ciphertext)
	if err != nil || len(sealed) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Ciphertext must be base64")
	}
	aad, aadErr := base64.StdEncoding.DecodeString(body.AAD)
	if aadErr != nil {
		return nil, status.Error(codes.InvalidArgument, "AAD must be base64")
	}
	// The seal envelope carries the sealing version number; decrypt fails
	// closed when that version no longer exists or is disabled.
	plaintext, usedVersion, openErr := p.open(sealed, aad)
	if openErr != nil {
		return nil, openErr
	}
	version := record.Versions[usedVersion]
	if version == nil || version.State != stateEnabled {
		return nil, status.Errorf(codes.FailedPrecondition, "The sealing version %s is not enabled", usedVersion)
	}
	return &gateway.RESTResponse{JSON: map[string]any{
		"plaintext":                 base64.StdEncoding.EncodeToString(plaintext),
		"usedCiphertextVersionName": cryptoKeyName(scope.Project, ring, key) + "/cryptoKeyVersions/" + version.Number,
	}}, nil
}

// versionsCreate appends one version with fresh material.
func (p *Provider) versionsCreate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	ring, key := request.Path["keyRing"], request.Path["cryptoKey"]
	record := p.lookupKey(scope, ring, key)
	if record == nil {
		return nil, notFound(kindCryptoKey, key)
	}
	material := make([]byte, 32)
	if err := p.random(material); err != nil {
		return nil, status.Errorf(codes.Internal, "Key generation failed: %v", err)
	}
	number := ""
	err := p.scopes.With(scope, func(_ *scopeState) error {
		next := len(record.Versions) + 1
		for record.Versions[strconv.Itoa(next)] != nil {
			next++
		}
		number = strconv.Itoa(next)
		record.Versions[number] = &versionRecord{Number: number, State: stateEnabled, Material: material, CreatedAt: p.clock()}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("create the crypto key version: %w", err)
	}
	if record.Primary == "" {
		record.Primary = number
	}
	return &gateway.RESTResponse{JSON: p.versionDocument(scope, ring, key, record.Versions[number])}, nil
}

// versionDocument renders one crypto key version wire shape.
func (p *Provider) versionDocument(scope state.Scope, ring, key string, record *versionRecord) map[string]any {
	return map[string]any{
		keyName:           cryptoKeyName(scope.Project, ring, key) + "/cryptoKeyVersions/" + record.Number,
		"state":           record.State,
		"algorithm":       "GOOGLE_SYMMETRIC_ENCRYPTION",
		"protectionLevel": "SOFTWARE",
		keyCreateTime:     stamp(record.CreatedAt),
	}
}

// versionsList returns the crypto key's versions.
func (p *Provider) versionsList(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	ring, key := request.Path["keyRing"], request.Path["cryptoKey"]
	record := p.lookupKey(scope, ring, key)
	if record == nil {
		return nil, notFound(kindCryptoKey, key)
	}
	items := []any{}
	for number := range record.Versions {
		items = append(items, p.versionDocument(scope, ring, key, record.Versions[number]))
	}
	return &gateway.RESTResponse{JSON: map[string]any{"cryptoKeyVersions": items}}, nil
}

// versionsGet returns one version.
func (p *Provider) versionsGet(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	ring, key, version := request.Path["keyRing"], request.Path["cryptoKey"], request.Path["version"]
	record := p.lookupKey(scope, ring, key)
	if record == nil || record.Versions[version] == nil {
		return nil, notFound("cryptoKeyVersion", version)
	}
	return &gateway.RESTResponse{JSON: p.versionDocument(scope, ring, key, record.Versions[version])}, nil
}

// versionsState applies one state transition to a version.
func (p *Provider) versionsState(request *gateway.RESTRequest, scope state.Scope, target string) (*gateway.RESTResponse, error) {
	ring, key, version := request.Path["keyRing"], request.Path["cryptoKey"], request.Path["version"]
	record := p.lookupKey(scope, ring, key)
	if record == nil || record.Versions[version] == nil {
		return nil, notFound("cryptoKeyVersion", version)
	}
	stored := record.Versions[version]
	if stored.State == stateDestroyed {
		return nil, status.Errorf(codes.FailedPrecondition, "Version %s is destroyed and its state is final", version)
	}
	if stored.State == target {
		return nil, status.Errorf(codes.FailedPrecondition, "Version %s is already %s", version, target)
	}
	stored.State = target
	if target == stateDestroyed {
		stored.Material = nil
		if record.Primary == version {
			record.Primary = ""
		}
	}
	return &gateway.RESTResponse{JSON: p.versionDocument(scope, ring, key, stored)}, nil
}

// getIam returns the stored policy of a key ring or crypto key.
func (p *Provider) getIam(_ *gateway.RESTRequest, _ state.Scope, target iam.Target) (*gateway.RESTResponse, error) {
	policy, err := p.engine.GetPolicy(target.Name)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read KMS policy: %v", err)
	}
	return &gateway.RESTResponse{JSON: policy}, nil
}

// setIam stores the policy of a key ring or crypto key.
func (p *Provider) setIam(request *gateway.RESTRequest, _ state.Scope, target iam.Target) (*gateway.RESTResponse, error) {
	var envelope struct {
		Policy iam.Policy `json:"policy"`
	}
	if json.Unmarshal(request.JSON, &envelope) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid IAM policy")
	}
	policy, err := p.engine.SetPolicy(target.Name, envelope.Policy)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "write KMS policy: %v", err)
	}
	return &gateway.RESTResponse{JSON: policy}, nil
}

// parseCryptoKeyBody reads and validates one crypto key create body.
func parseCryptoKeyBody(raw []byte) (cryptoKeyBody, error) {
	var body cryptoKeyBody
	if json.Unmarshal(raw, &body) != nil {
		return body, status.Error(codes.InvalidArgument, "Invalid crypto key body")
	}
	if body.Purpose != "" && body.Purpose != purposeEncryptDecrypt {
		return body, gap("only symmetric ENCRYPT_DECRYPT keys")
	}
	if body.Purpose == "" {
		body.Purpose = purposeEncryptDecrypt
	}
	if body.Algorithm == "" {
		body.Algorithm = "GOOGLE_SYMMETRIC_ENCRYPTION"
	}
	return body, nil
}
