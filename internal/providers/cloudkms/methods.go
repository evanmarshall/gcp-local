package cloudkms

import (
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// handler serves one authorized KMS method.
type handler func(p *Provider, request *gateway.RESTRequest, scope state.Scope, resource ref) (*gateway.RESTResponse, error)

// handlers maps every registered method to its implementation.
var handlers = map[string]handler{
	methodLocationsList:     (*Provider).locationsList,
	methodKeyRingsCreate:    (*Provider).keyRingsCreate,
	methodKeyRingsGet:       (*Provider).keyRingsGet,
	methodKeyRingsList:      (*Provider).keyRingsList,
	methodKeyRingsGetIam:    (*Provider).ringGetIam,
	methodKeyRingsSetIam:    (*Provider).ringSetIam,
	methodCryptoKeysCreate:  (*Provider).cryptoKeysCreate,
	methodCryptoKeysGet:     (*Provider).cryptoKeysGet,
	methodCryptoKeysList:    (*Provider).cryptoKeysList,
	methodCryptoKeysUpdate:  (*Provider).cryptoKeysUpdate,
	methodCryptoKeysPrimary: (*Provider).updatePrimaryVersion,
	methodCryptoKeysGetIam:  (*Provider).keyGetIam,
	methodCryptoKeysSetIam:  (*Provider).keySetIam,
	methodCryptoKeysEncrypt: (*Provider).encrypt,
	methodCryptoKeysDecrypt: (*Provider).decrypt,
	methodVersionsCreate:    (*Provider).versionsCreate,
	methodVersionsList:      (*Provider).versionsList,
	methodVersionsGet:       (*Provider).versionsGet,
	methodVersionsPatch:     (*Provider).versionsPatch,
	methodVersionsDestroy:   (*Provider).versionsDestroy,
}

// execute dispatches one authorized KMS request.
func (p *Provider) execute(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, resource ref) (*gateway.RESTResponse, error) {
	serve, ok := handlers[call.Method]
	if !ok {
		return nil, status.Errorf(codes.Internal, "Unhandled KMS method %s", call.Method)
	}
	return serve(p, request, scope, resource)
}

// respond wraps one JSON document.
func respond(document any) (*gateway.RESTResponse, error) {
	return &gateway.RESTResponse{JSON: document}, nil
}

// locationsList returns the modeled KMS locations.
func (p *Provider) locationsList(_ *gateway.RESTRequest, _ state.Scope, resource ref) (*gateway.RESTResponse, error) {
	return respond(map[string]any{"locations": []any{
		map[string]any{keyName: "projects/" + resource.project + "/locations/" + testLocation,
			"locationId": testLocation, "displayName": testLocation},
	}})
}

// keyRingsCreate validates and stores one key ring.
func (p *Provider) keyRingsCreate(request *gateway.RESTRequest, scope state.Scope, resource ref) (*gateway.RESTResponse, error) {
	resource.ring = request.Query.Get("keyRingId")
	if !uid.MatchString(resource.ring) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid key ring id %q", resource.ring)
	}
	var created *keyRingRecord
	err := p.scopes.With(scope, func(s *scopeState) error {
		if s.keyRings[resource.ringID()] != nil {
			return status.Errorf(codes.AlreadyExists, "KeyRing %s already exists.", resource.ringName())
		}
		created = &keyRingRecord{Name: resource.ring, CreatedAt: p.clock()}
		s.keyRings[resource.ringID()] = created
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("create the key ring: %w", err)
	}
	if indexErr := p.index.Put(serviceName, state.Resource{
		Name: resource.ringResource(), Service: serviceName,
		Kind: kindKeyRing, Parent: provider.ProjectResource(resource.project), Scope: scope,
	}); indexErr != nil {
		return nil, gap("index the key ring")
	}
	return respond(keyRingDocument(resource, created))
}

// keyRingDocument renders the key ring wire shape.
func keyRingDocument(resource ref, record *keyRingRecord) map[string]any {
	resource.ring = record.Name
	return map[string]any{keyName: resource.ringName(), keyCreateTime: stamp(record.CreatedAt)}
}

func (p *Provider) keyRingsGet(_ *gateway.RESTRequest, scope state.Scope, resource ref) (*gateway.RESTResponse, error) {
	var document map[string]any
	err := p.scopes.With(scope, func(s *scopeState) error {
		record := s.keyRings[resource.ringID()]
		if record == nil {
			return notFound("KeyRing", resource.ringName())
		}
		document = keyRingDocument(resource, record)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("read the key ring: %w", err)
	}
	return respond(document)
}

// keyRingsList returns the location's key rings in name order.
func (p *Provider) keyRingsList(_ *gateway.RESTRequest, scope state.Scope, resource ref) (*gateway.RESTResponse, error) {
	items := []any{}
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for _, id := range sortedKeys(s.keyRings, resource.location+"/") {
			items = append(items, keyRingDocument(resource, s.keyRings[id]))
		}
		return nil
	})
	return respond(map[string]any{"keyRings": items})
}

// sortedKeys lists one map's keys with a prefix in order.
func sortedKeys[T any](entries map[string]T, prefix string) []string {
	var names []string
	for name := range entries {
		if strings.HasPrefix(name, prefix) && !strings.Contains(name[len(prefix):], "/") {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

// cryptoKeysCreate validates and stores one symmetric crypto key with version 1
// unless skipInitialVersionCreation is set.
func (p *Provider) cryptoKeysCreate(request *gateway.RESTRequest, scope state.Scope, resource ref) (*gateway.RESTResponse, error) {
	resource.key = request.Query.Get("cryptoKeyId")
	if !uid.MatchString(resource.key) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid crypto key id %q", resource.key)
	}
	record, err := p.newCryptoKey(request, resource.key)
	if err != nil {
		return nil, err
	}
	err = p.scopes.With(scope, func(s *scopeState) error {
		if s.keyRings[resource.ringID()] == nil {
			return notFound("KeyRing", resource.ringName())
		}
		if s.cryptoKeys[resource.keyID()] != nil {
			return status.Errorf(codes.AlreadyExists, "CryptoKey %s already exists.", resource.keyName())
		}
		s.cryptoKeys[resource.keyID()] = record
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("create the crypto key: %w", err)
	}
	if indexErr := p.index.Put(serviceName, state.Resource{
		Name: resource.keyResource(), Service: serviceName,
		Kind: kindCryptoKey, Parent: resource.ringResource(), Scope: scope,
	}); indexErr != nil {
		return nil, gap("index the crypto key")
	}
	return respond(cryptoKeyDocument(resource, record))
}

// newCryptoKey builds one key record and its initial version from a request.
func (p *Provider) newCryptoKey(request *gateway.RESTRequest, name string) (*cryptoKeyRecord, error) {
	body, err := parseCryptoKeyBody(request.JSON)
	if err != nil {
		return nil, err
	}
	destroy, err := parseDestroyDuration(body.DestroyDuration)
	if err != nil {
		return nil, err
	}
	now := p.clock()
	record := &cryptoKeyRecord{
		Name: name, Purpose: body.Purpose, Algorithm: body.Template.Algorithm,
		Labels: maps.Clone(body.Labels), Rotation: body.RotationPeriod, NextRotation: body.NextRotation,
		DestroyDuration: destroy, DestroyText: body.DestroyDuration,
		Versions: map[string]*versionRecord{}, CreatedAt: now,
	}
	if request.Query.Get("skipInitialVersionCreation") == "true" {
		return record, nil
	}
	version, err := newVersion("1", now)
	if err != nil {
		return nil, err
	}
	record.Versions["1"], record.Primary = version, "1"
	return record, nil
}

// newVersion creates one enabled version with fresh material.
func newVersion(number string, now time.Time) (*versionRecord, error) {
	material := make([]byte, 32)
	if err := readRandom(material); err != nil {
		return nil, status.Errorf(codes.Internal, "Key generation failed: %v", err)
	}
	return &versionRecord{Number: number, State: stateEnabled, Material: material, CreatedAt: now}, nil
}

// cryptoKeyDocument renders the crypto key wire shape; unset optional fields
// are omitted, as the real surface does.
func cryptoKeyDocument(resource ref, record *cryptoKeyRecord) map[string]any {
	resource.key = record.Name
	document := map[string]any{
		keyName:                    resource.keyName(),
		keyCreateTime:              stamp(record.CreatedAt),
		"purpose":                  record.Purpose,
		"versionTemplate":          map[string]any{keyAlgorithm: record.Algorithm, keyProtectionLevel: protectionSoftware},
		"destroyScheduledDuration": record.DestroyText,
	}
	if version := record.Versions[record.Primary]; version != nil {
		document["primary"] = versionDocument(resource, version)
	}
	if len(record.Labels) > 0 {
		document["labels"] = record.Labels
	}
	if record.Rotation != "" {
		document["rotationPeriod"] = record.Rotation
	}
	if record.NextRotation != "" {
		document["nextRotationTime"] = record.NextRotation
	}
	return document
}

// withKey runs use on one existing crypto key under the scope lock.
func (p *Provider) withKey(scope state.Scope, resource ref, use func(*cryptoKeyRecord) error) error {
	err := p.scopes.With(scope, func(s *scopeState) error {
		if s.keyRings[resource.ringID()] == nil {
			return notFound("KeyRing", resource.ringName())
		}
		record := s.cryptoKeys[resource.keyID()]
		if record == nil {
			return notFound("CryptoKey", resource.keyName())
		}
		settle(record, p.clock())
		return use(record)
	})
	if err != nil {
		return fmt.Errorf("open the crypto key: %w", err)
	}
	return nil
}

// keyResponse runs use on one key and renders the key afterwards.
func (p *Provider) keyResponse(scope state.Scope, resource ref, use func(*cryptoKeyRecord) error) (*gateway.RESTResponse, error) {
	var document map[string]any
	err := p.withKey(scope, resource, func(record *cryptoKeyRecord) error {
		if err := use(record); err != nil {
			return err
		}
		document = cryptoKeyDocument(resource, record)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("crypto key %s: %w", resource.key, err)
	}
	return respond(document)
}

func (p *Provider) cryptoKeysGet(_ *gateway.RESTRequest, scope state.Scope, resource ref) (*gateway.RESTResponse, error) {
	return p.keyResponse(scope, resource, func(*cryptoKeyRecord) error { return nil })
}

// cryptoKeysList returns the key ring's crypto keys in name order.
func (p *Provider) cryptoKeysList(_ *gateway.RESTRequest, scope state.Scope, resource ref) (*gateway.RESTResponse, error) {
	items := []any{}
	err := p.scopes.With(scope, func(s *scopeState) error {
		if s.keyRings[resource.ringID()] == nil {
			return notFound("KeyRing", resource.ringName())
		}
		for _, id := range sortedKeys(s.cryptoKeys, resource.ringID()+"/") {
			settle(s.cryptoKeys[id], p.clock())
			items = append(items, cryptoKeyDocument(resource, s.cryptoKeys[id]))
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list crypto keys: %w", err)
	}
	return respond(map[string]any{"cryptoKeys": items})
}

// keyUpdateBody is one decoded crypto key PATCH body.
type keyUpdateBody struct {
	Labels       map[string]string `json:"labels"`
	Rotation     string            `json:"rotationPeriod"`
	NextRotation string            `json:"nextRotationTime"`
	Template     versionTemplate   `json:"versionTemplate"`
}

// cryptoKeysUpdate applies the fields named by updateMask.
func (p *Provider) cryptoKeysUpdate(request *gateway.RESTRequest, scope state.Scope, resource ref) (*gateway.RESTResponse, error) {
	paths, err := updateMask(request)
	if err != nil {
		return nil, err
	}
	var body keyUpdateBody
	if err := decodeBody(request.JSON, &body); err != nil {
		return nil, err
	}
	for _, path := range paths {
		if err := validateKeyPath(path, body); err != nil {
			return nil, err
		}
	}
	return p.keyResponse(scope, resource, func(record *cryptoKeyRecord) error {
		for _, path := range paths {
			applyKeyPath(record, path, body)
		}
		return nil
	})
}

// validateKeyPath accepts the modeled crypto key update mask paths.
func validateKeyPath(path string, body keyUpdateBody) error {
	switch path {
	case "labels", "rotationPeriod", "nextRotationTime":
		return nil
	case "versionTemplate.algorithm":
		if body.Template.Algorithm != algorithmSymmetric {
			return gap("only " + algorithmSymmetric + " version templates")
		}
		return nil
	default:
		return status.Errorf(codes.InvalidArgument, "Unsupported update mask path %q", path)
	}
}

// applyKeyPath writes one validated mask path.
func applyKeyPath(record *cryptoKeyRecord, path string, body keyUpdateBody) {
	switch path {
	case "labels":
		record.Labels = maps.Clone(body.Labels)
	case "rotationPeriod":
		record.Rotation = body.Rotation
	case "nextRotationTime":
		record.NextRotation = body.NextRotation
	default:
		record.Algorithm = body.Template.Algorithm
	}
}

// updatePrimaryVersion moves the primary to one enabled version.
func (p *Provider) updatePrimaryVersion(request *gateway.RESTRequest, scope state.Scope, resource ref) (*gateway.RESTResponse, error) {
	var body struct {
		Version string `json:"cryptoKeyVersionId"`
	}
	if err := decodeBody(request.JSON, &body); err != nil {
		return nil, err
	}
	if !versionNumber.MatchString(body.Version) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid cryptoKeyVersionId %q", body.Version)
	}
	return p.keyResponse(scope, resource, func(record *cryptoKeyRecord) error {
		version := record.Versions[body.Version]
		if version == nil {
			return notFound("CryptoKeyVersion", resource.versionName(body.Version))
		}
		if version.State != stateEnabled {
			return status.Errorf(codes.FailedPrecondition, "%s is not enabled.", resource.versionName(body.Version))
		}
		record.Primary = body.Version
		return nil
	})
}

// updateMask reads the comma-separated updateMask query.
func updateMask(request *gateway.RESTRequest) ([]string, error) {
	mask := request.Query.Get("updateMask")
	if mask == "" {
		return nil, status.Error(codes.InvalidArgument, "The request must include an update_mask.")
	}
	return strings.Split(mask, ","), nil
}

// decodeBody decodes an optional JSON body.
func decodeBody(raw []byte, target any) error {
	if len(raw) == 0 {
		return nil
	}
	if json.Unmarshal(raw, target) != nil {
		return status.Error(codes.InvalidArgument, "Invalid JSON payload")
	}
	return nil
}

// ringGetIam, ringSetIam, keyGetIam, and keySetIam serve resource policies.
func (p *Provider) ringGetIam(_ *gateway.RESTRequest, _ state.Scope, resource ref) (*gateway.RESTResponse, error) {
	return p.getIam(resource.ringResource(), "KeyRing", resource.ringName())
}

func (p *Provider) ringSetIam(request *gateway.RESTRequest, _ state.Scope, resource ref) (*gateway.RESTResponse, error) {
	return p.setIam(request, resource.ringResource(), "KeyRing", resource.ringName())
}

func (p *Provider) keyGetIam(_ *gateway.RESTRequest, _ state.Scope, resource ref) (*gateway.RESTResponse, error) {
	return p.getIam(resource.keyResource(), "CryptoKey", resource.keyName())
}

func (p *Provider) keySetIam(request *gateway.RESTRequest, _ state.Scope, resource ref) (*gateway.RESTResponse, error) {
	return p.setIam(request, resource.keyResource(), "CryptoKey", resource.keyName())
}

// getIam returns one resource's policy; an empty policy renders as its etag.
func (p *Provider) getIam(indexed, kind, name string) (*gateway.RESTResponse, error) {
	if _, ok := p.index.Get(indexed); !ok {
		return nil, notFound(kind, name)
	}
	policy, err := p.engine.GetPolicy(indexed)
	if err != nil {
		return nil, fmt.Errorf("read KMS policy: %w", err)
	}
	return respond(policy.Wire())
}

// setIam stores one resource's policy.
func (p *Provider) setIam(request *gateway.RESTRequest, indexed, kind, name string) (*gateway.RESTResponse, error) {
	if _, ok := p.index.Get(indexed); !ok {
		return nil, notFound(kind, name)
	}
	var envelope struct {
		Policy iam.Policy `json:"policy"`
	}
	if json.Unmarshal(request.JSON, &envelope) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid IAM policy")
	}
	policy, err := p.engine.SetPolicy(indexed, envelope.Policy)
	if err != nil {
		return nil, fmt.Errorf("write KMS policy: %w", err)
	}
	return respond(policy.Wire())
}

// parseCryptoKeyBody reads and validates one crypto key create body.
func parseCryptoKeyBody(raw []byte) (cryptoKeyBody, error) {
	var body cryptoKeyBody
	if err := decodeBody(raw, &body); err != nil {
		return body, err
	}
	if body.Purpose == "" {
		body.Purpose = purposeEncryptDecrypt
	}
	if body.Template.Algorithm == "" {
		body.Template.Algorithm = algorithmSymmetric
	}
	if body.DestroyDuration == "" {
		body.DestroyDuration = defaultDestroyDuration
	}
	switch {
	case body.Purpose != purposeEncryptDecrypt:
		return body, gap("only symmetric ENCRYPT_DECRYPT keys")
	case body.Template.Algorithm != algorithmSymmetric:
		return body, gap("only " + algorithmSymmetric + " keys")
	case body.Template.ProtectionLevel != "" && body.Template.ProtectionLevel != protectionSoftware:
		return body, gap("only SOFTWARE protection")
	case body.ImportOnly:
		return body, gap("import-only keys")
	}
	return body, nil
}

// parseDestroyDuration accepts the documented 1-to-120-day range.
func parseDestroyDuration(text string) (time.Duration, error) {
	duration, err := time.ParseDuration(text)
	if err != nil || !strings.HasSuffix(text, "s") || duration < 24*time.Hour || duration > 120*24*time.Hour {
		return 0, status.Errorf(codes.InvalidArgument, "Invalid destroyScheduledDuration %q", text)
	}
	return duration, nil
}
