package cloudkms

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// handler serves one authorized KMS method.
type handler func(p *Provider, request *gateway.RESTRequest, scope state.Scope, resource ref) (*gateway.RESTResponse, error)

// handlers maps every registered method to its implementation.
var handlers = map[string]handler{
	methodLocationsGet:      (*Provider).locationsGet,
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
	methodVersionsRestore:   (*Provider).versionsRestore,
	methodAsymmetricSign:    (*Provider).asymmetricSign,
	methodAsymmetricDecrypt: (*Provider).asymmetricDecrypt,
	methodGetPublicKey:      (*Provider).getPublicKey,
	methodMacSign:           (*Provider).macSign,
	methodMacVerify:         (*Provider).macVerify,
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

// paging is every KMS list's: GCP's recorded refusals of a page token it
// didn't issue and of a negative size (cloudkms-symmetric-key-lifecycle
// "versions-invalid-token", "versions-negative-size"). The default and
// maximum sizes are not recorded.
var paging = provider.Paging{Default: 1000, Max: 1000,
	InvalidToken: provider.Invalid("Invalid page token."), NegativeSize: provider.Invalid("Page size must be positive.")}

// listBody renders one page with the totalSize KMS lists report, omitted
// when zero as proto3 JSON does (recorded: "versions-paged").
func listBody[T any](field string, items []T, next string, total int) map[string]any {
	body := provider.ListBody(field, items, next)
	if total > 0 {
		body["totalSize"] = total
	}
	return body
}

// locationsGet returns one location; any location id is modeled.
func (p *Provider) locationsGet(_ *gateway.RESTRequest, _ state.Scope, resource ref) (*gateway.RESTResponse, error) {
	return respond(map[string]any{keyName: "projects/" + resource.project + "/locations/" + resource.location,
		"locationId": resource.location, "displayName": resource.location})
}

// decode strictly checks a body against the request message GCP's
// transcoder decodes it into, then reads it into body. root is the request
// field the route's body binds ("crypto_key"), "" for body "*".
func decode(raw []byte, root string, message proto.Message, body any, unmodeled ...string) error {
	if err := provider.DecodeProto(raw, root, message, gap, unmodeled...); err != nil {
		return err
	}
	return decodeBody(raw, body)
}

// keyRingsCreate validates and stores one key ring.
func (p *Provider) keyRingsCreate(request *gateway.RESTRequest, scope state.Scope, resource ref) (*gateway.RESTResponse, error) {
	resource.ring = request.Query.Get("keyRingId")
	if err := decode(request.JSON, "key_ring", &kmspb.KeyRing{}, &struct{}{}); err != nil {
		return nil, err
	}
	if !uid.MatchString(resource.ring) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid key ring id %q", resource.ring)
	}
	var document map[string]any
	err := p.scopes.With(scope, func(s *scopeState) error {
		if s.keyRings[resource.ringID()] != nil {
			return status.Errorf(codes.AlreadyExists, "KeyRing %s already exists.", resource.ringName())
		}
		if p.index.Put(serviceName, state.Resource{
			Name: resource.ringResource(), Service: serviceName,
			Kind: kindKeyRing, Parent: provider.ProjectResource(resource.project), Scope: scope,
		}) != nil {
			return gap("index the key ring")
		}
		record := &keyRingRecord{Name: resource.ring, CreatedAt: p.clock()}
		s.keyRings[resource.ringID()] = record
		document = keyRingDocument(resource, record)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("create the key ring: %w", err)
	}
	return respond(document)
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

// keyRingsList returns one page of the location's key rings in name order.
func (p *Provider) keyRingsList(request *gateway.RESTRequest, scope state.Scope, resource ref) (*gateway.RESTResponse, error) {
	var body map[string]any
	err := p.scopes.With(scope, func(s *scopeState) error {
		ids := sortedKeys(s.keyRings, resource.location+"/")
		page, next, err := provider.Page(ids, request.Query, paging, gap)
		if err != nil {
			return err
		}
		items := make([]any, 0, len(page))
		for _, id := range page {
			items = append(items, keyRingDocument(resource, s.keyRings[id]))
		}
		body = listBody("keyRings", items, next, len(ids))
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list key rings: %w", err)
	}
	return respond(body)
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

// cryptoKeysCreate validates and stores one crypto key with version 1 unless
// skipInitialVersionCreation is set.
func (p *Provider) cryptoKeysCreate(request *gateway.RESTRequest, scope state.Scope, resource ref) (*gateway.RESTResponse, error) {
	resource.key = request.Query.Get("cryptoKeyId")
	if !uid.MatchString(resource.key) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid crypto key id %q", resource.key)
	}
	record, err := p.newCryptoKey(request, resource.key)
	if err != nil {
		return nil, err
	}
	var document map[string]any
	err = p.scopes.With(scope, func(s *scopeState) error {
		if s.keyRings[resource.ringID()] == nil {
			return notFound("KeyRing", resource.ringName())
		}
		if s.cryptoKeys[resource.keyID()] != nil {
			return status.Errorf(codes.AlreadyExists, "CryptoKey %s already exists.", resource.keyName())
		}
		if p.index.Put(serviceName, state.Resource{
			Name: resource.keyResource(), Service: serviceName,
			Kind: kindCryptoKey, Parent: resource.ringResource(), Scope: scope,
		}) != nil {
			return gap("index the crypto key")
		}
		s.cryptoKeys[resource.keyID()] = record
		document = cryptoKeyDocument(resource, record)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("create the crypto key: %w", err)
	}
	return respond(document)
}

// newCryptoKey builds one key record and its initial version from a request.
// Only an ENCRYPT_DECRYPT key gets a primary version.
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
		Name: name, Purpose: string(body.Purpose), Algorithm: string(body.Template.Algorithm),
		Labels: maps.Clone(body.Labels), DestroyDuration: destroy, DestroyText: body.DestroyDuration,
		Versions: map[string]*versionRecord{}, CreatedAt: now,
	}
	if record.NextRotation, record.RotationPeriod, err = parseRotation(body.NextRotation, body.RotationPeriod); err != nil {
		return nil, err
	}
	if err := checkRotation(record); err != nil {
		return nil, err
	}
	if request.Query.Get("skipInitialVersionCreation") == "true" {
		return record, nil
	}
	version, err := newVersion("1", record.Algorithm, now)
	if err != nil {
		return nil, err
	}
	record.Versions["1"] = version
	if record.Purpose == purposeEncryptDecrypt {
		record.Primary = "1"
	}
	return record, nil
}

// newVersion creates one enabled version with fresh material for its
// algorithm, read from crypto/rand. GCP reports PENDING_GENERATION first for
// asymmetric versions; generation here is synchronous.
func newVersion(number, algorithm string, now time.Time) (*versionRecord, error) {
	spec := algorithms[algorithm]
	version := &versionRecord{Number: number, Algorithm: algorithm, State: stateEnabled, CreatedAt: now}
	var err error
	switch {
	case spec.curve != nil:
		version.Private, err = ecdsa.GenerateKey(spec.curve, rand.Reader)
	case spec.bits > 0:
		version.Private, err = rsa.GenerateKey(rand.Reader, spec.bits)
	default:
		version.Material = make([]byte, 32)
		err = readRandom(version.Material)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Key generation failed: %v", err)
	}
	return version, nil
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
		document["labels"] = maps.Clone(record.Labels)
	}
	if !record.NextRotation.IsZero() {
		// Recorded: a whole-second time reads back without fractional digits.
		document["nextRotationTime"] = protoStamp(record.NextRotation)
	}
	if record.RotationPeriod > 0 {
		document["rotationPeriod"] = strconv.FormatInt(int64(record.RotationPeriod/time.Second), 10) + "s"
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

// cryptoKeysList returns one page of the key ring's crypto keys in name
// order. versionView only adds HSM attestations, which SOFTWARE keys lack.
func (p *Provider) cryptoKeysList(request *gateway.RESTRequest, scope state.Scope, resource ref) (*gateway.RESTResponse, error) {
	var body map[string]any
	err := p.scopes.With(scope, func(s *scopeState) error {
		if s.keyRings[resource.ringID()] == nil {
			return notFound("KeyRing", resource.ringName())
		}
		ids := sortedKeys(s.cryptoKeys, resource.ringID()+"/")
		page, next, err := provider.Page(ids, request.Query, paging, gap)
		if err != nil {
			return err
		}
		items := make([]any, 0, len(page))
		for _, id := range page {
			settle(s.cryptoKeys[id], p.clock())
			items = append(items, cryptoKeyDocument(resource, s.cryptoKeys[id]))
		}
		body = listBody("cryptoKeys", items, next, len(ids))
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list crypto keys: %w", err)
	}
	return respond(body)
}

// keyUpdateBody is one decoded crypto key PATCH body.
type keyUpdateBody struct {
	Labels         map[string]string `json:"labels"`
	Template       versionTemplate   `json:"versionTemplate"`
	RotationPeriod string            `json:"rotationPeriod"`
	NextRotation   string            `json:"nextRotationTime"`
}

// unmodeledKeyFields are the CryptoKey fields a create or update refuses
// when set: import-only keys, external backends, and Key Access
// Justifications.
var unmodeledKeyFields = []string{"importOnly", "cryptoKeyBackend", "keyAccessJustificationsPolicy"}

// Rotation limits from the CryptoKey reference: a period of 24 to 876,000
// hours.
const (
	minRotationPeriod = 24 * time.Hour
	maxRotationPeriod = 876000 * time.Hour
)

// protoStamp renders a timestamp as proto3 JSON does: 0, 3, 6, or 9
// fractional digits.
func protoStamp(moment time.Time) string {
	text := moment.UTC().Format("2006-01-02T15:04:05")
	switch nanos := moment.Nanosecond(); {
	case nanos == 0:
	case nanos%1_000_000 == 0:
		text += fmt.Sprintf(".%03d", nanos/1_000_000)
	case nanos%1_000 == 0:
		text += fmt.Sprintf(".%06d", nanos/1_000)
	default:
		text += fmt.Sprintf(".%09d", nanos)
	}
	return text + "Z"
}

// parseRotation reads a rotation schedule; both texts may be empty.
func parseRotation(next, period string) (time.Time, time.Duration, error) {
	var at time.Time
	var every time.Duration
	var err error
	if next != "" {
		if at, err = time.Parse(time.RFC3339Nano, next); err != nil {
			return at, every, status.Errorf(codes.InvalidArgument, "Invalid nextRotationTime %q", next)
		}
	}
	if period != "" {
		every, err = time.ParseDuration(period)
		if err != nil || !strings.HasSuffix(period, "s") {
			return at, every, status.Errorf(codes.InvalidArgument, "Invalid rotationPeriod %q", period)
		}
		// Recorded: cloudkms-rotation-schedule "period-too-short"; the
		// maximum's wording is modeled on it.
		if every < minRotationPeriod {
			return at, every, status.Error(codes.InvalidArgument, "rotation_period must be at least 24h")
		}
		if every > maxRotationPeriod {
			return at, every, status.Error(codes.InvalidArgument, "rotation_period must be at most 876000h")
		}
	}
	return at, every, nil
}

// checkRotation applies the recorded rules to a key's schedule: only
// ENCRYPT_DECRYPT keys rotate automatically, and a period needs a next time
// (cloudkms-rotation-schedule "asymmetric-rotation", "period-without-next").
func checkRotation(record *cryptoKeyRecord) error {
	scheduled := !record.NextRotation.IsZero() || record.RotationPeriod > 0
	switch {
	case scheduled && record.Purpose != purposeEncryptDecrypt:
		return status.Errorf(codes.InvalidArgument, "Automatic rotation (via fields crypto_key.next_rotation_time and "+
			"crypto_key.rotation_schedule) is not available for keys with purpose %s.", record.Purpose)
	case record.RotationPeriod > 0 && record.NextRotation.IsZero():
		return status.Error(codes.InvalidArgument, "next_rotation_time is required if rotation_schedule is set.")
	}
	return nil
}

// cryptoKeysUpdate applies the fields named by updateMask.
func (p *Provider) cryptoKeysUpdate(request *gateway.RESTRequest, scope state.Scope, resource ref) (*gateway.RESTResponse, error) {
	paths, err := updateMask(request)
	if err != nil {
		return nil, err
	}
	var body keyUpdateBody
	if err := decode(request.JSON, "crypto_key", &kmspb.CryptoKey{}, &body, unmodeledKeyFields...); err != nil {
		return nil, err
	}
	next, period, err := parseRotation(body.NextRotation, body.RotationPeriod)
	if err != nil {
		return nil, err
	}
	return p.keyResponse(scope, resource, func(record *cryptoKeyRecord) error {
		for _, path := range paths {
			if err := validateKeyPath(record, path, body); err != nil {
				return err
			}
		}
		patched := *record
		for _, path := range paths {
			applyKeyPath(&patched, path, body, next, period)
		}
		if err := checkRotation(&patched); err != nil {
			return err
		}
		*record = patched
		return nil
	})
}

// validateKeyPath accepts the modeled crypto key update mask paths for one
// key, in camelCase or snake_case.
func validateKeyPath(record *cryptoKeyRecord, path string, body keyUpdateBody) error {
	switch path {
	case "labels":
		return nil
	case "rotationPeriod", "rotation_period", "nextRotationTime", "next_rotation_time":
		return nil
	case "versionTemplate.algorithm", "version_template.algorithm":
		return algorithmAllowed(record.Purpose, string(body.Template.Algorithm))
	default:
		return status.Errorf(codes.InvalidArgument, "Unsupported update mask path %q", path)
	}
}

// algorithmAllowed accepts an implemented algorithm of the key's purpose.
func algorithmAllowed(purpose, algorithm string) error {
	spec, ok := algorithms[algorithm]
	switch {
	case algorithm == "":
		return status.Errorf(codes.InvalidArgument, "versionTemplate.algorithm is required for purpose %s.", purpose)
	case !ok:
		return gap("algorithm " + algorithm)
	case spec.purpose != purpose:
		return status.Errorf(codes.InvalidArgument, "Algorithm %s is not compatible with purpose %s.", algorithm, purpose)
	}
	return nil
}

// applyKeyPath writes one validated mask path; an unset schedule field
// clears it, as the released Terraform provider does on destroy.
func applyKeyPath(record *cryptoKeyRecord, path string, body keyUpdateBody, next time.Time, period time.Duration) {
	switch path {
	case "labels":
		record.Labels = maps.Clone(body.Labels)
	case "versionTemplate.algorithm", "version_template.algorithm":
		record.Algorithm = string(body.Template.Algorithm)
	case "rotationPeriod", "rotation_period":
		record.RotationPeriod = period
	case "nextRotationTime", "next_rotation_time":
		record.NextRotation = next
	}
}

// updatePrimaryVersion moves the primary to one enabled version.
func (p *Provider) updatePrimaryVersion(request *gateway.RESTRequest, scope state.Scope, resource ref) (*gateway.RESTResponse, error) {
	var body struct {
		Version string `json:"cryptoKeyVersionId"`
	}
	if err := decode(request.JSON, "", &kmspb.UpdateCryptoKeyPrimaryVersionRequest{}, &body); err != nil {
		return nil, err
	}
	if !versionNumber.MatchString(body.Version) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid cryptoKeyVersionId %q", body.Version)
	}
	return p.keyResponse(scope, resource, func(record *cryptoKeyRecord) error {
		if record.Purpose != purposeEncryptDecrypt {
			return wrongPurpose(resource.keyName(), record.Purpose)
		}
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
func (p *Provider) ringGetIam(request *gateway.RESTRequest, _ state.Scope, resource ref) (*gateway.RESTResponse, error) {
	return p.getIam(request, resource.ringResource(), "KeyRing", resource.ringName())
}

func (p *Provider) ringSetIam(request *gateway.RESTRequest, _ state.Scope, resource ref) (*gateway.RESTResponse, error) {
	return p.setIam(request, resource.ringResource(), "KeyRing", resource.ringName())
}

func (p *Provider) keyGetIam(request *gateway.RESTRequest, _ state.Scope, resource ref) (*gateway.RESTResponse, error) {
	return p.getIam(request, resource.keyResource(), "CryptoKey", resource.keyName())
}

func (p *Provider) keySetIam(request *gateway.RESTRequest, _ state.Scope, resource ref) (*gateway.RESTResponse, error) {
	return p.setIam(request, resource.keyResource(), "CryptoKey", resource.keyName())
}

// getIam returns one resource's policy at the requested policy version; an
// empty policy renders as its etag.
func (p *Provider) getIam(request *gateway.RESTRequest, indexed, kind, name string) (*gateway.RESTResponse, error) {
	version, err := provider.RequestedPolicyVersion(request, "options.requestedPolicyVersion", gap)
	if err != nil {
		return nil, err //nolint:wrapcheck // wire statuses
	}
	if _, ok := p.index.Get(indexed); !ok {
		return nil, notFound(kind, name)
	}
	policy, err := p.engine.GetPolicy(indexed)
	if err != nil {
		return nil, fmt.Errorf("read KMS policy: %w", err)
	}
	return respond(policy.View(version).Wire())
}

// setIam stores one resource's policy.
func (p *Provider) setIam(request *gateway.RESTRequest, indexed, kind, name string) (*gateway.RESTResponse, error) {
	if _, ok := p.index.Get(indexed); !ok {
		return nil, notFound(kind, name)
	}
	decoded, err := provider.DecodePolicy(request.JSON, gap)
	if err != nil {
		return nil, err
	}
	policy, err := p.engine.SetPolicy(indexed, decoded)
	if err != nil {
		return nil, fmt.Errorf("write KMS policy: %w", err)
	}
	return respond(policy.Wire())
}

// parseCryptoKeyBody reads and validates one crypto key create body.
func parseCryptoKeyBody(raw []byte) (cryptoKeyBody, error) {
	var body cryptoKeyBody
	if err := decode(raw, "crypto_key", &kmspb.CryptoKey{}, &body, unmodeledKeyFields...); err != nil {
		return body, err
	}
	if body.Purpose == "" {
		// Recorded: cloudkms-rotation-schedule "purpose-missing".
		return body, provider.ErrorInfo(codes.InvalidArgument, "Request field crypto_key.purpose is required.",
			"MISSING_REQUIRED_FIELD", "cloudkms.googleapis.com", map[string]string{"field_name": "crypto_key.purpose"})
	}
	if body.Template.Algorithm == "" && body.Purpose == purposeEncryptDecrypt {
		body.Template.Algorithm = algorithmSymmetric
	}
	if body.DestroyDuration == "" {
		body.DestroyDuration = defaultDestroyDuration
	}
	return body, validateCryptoKeyBody(body)
}

// validateCryptoKeyBody refuses unimplemented and inconsistent key fields.
func validateCryptoKeyBody(body cryptoKeyBody) error {
	purpose := string(body.Purpose)
	if !purposes[purpose] {
		return gap("purpose " + purpose)
	}
	if err := algorithmAllowed(purpose, string(body.Template.Algorithm)); err != nil {
		return err
	}
	if body.Template.ProtectionLevel != "" && body.Template.ProtectionLevel != protectionSoftware {
		return gap("only SOFTWARE protection")
	}
	return nil
}

// parseDestroyDuration accepts the documented 1-to-120-day range.
func parseDestroyDuration(text string) (time.Duration, error) {
	duration, err := time.ParseDuration(text)
	if err != nil || !strings.HasSuffix(text, "s") || duration < 24*time.Hour || duration > 120*24*time.Hour {
		return 0, status.Errorf(codes.InvalidArgument, "Invalid destroyScheduledDuration %q", text)
	}
	return duration, nil
}
