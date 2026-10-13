package cloudkms

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash/crc32"
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
)

// castagnoli is the CRC32C table KMS integrity fields use.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// maxPayload bounds plaintext and AAD, as the real surface does.
const maxPayload = 64 << 10

// settle completes scheduled destruction whose time has passed.
func settle(record *cryptoKeyRecord, now time.Time) {
	for _, version := range record.Versions {
		if version.State == stateDestroyScheduled && !now.Before(version.DestroyAt) {
			version.State = stateDestroyed
			version.Material, version.Private = nil, nil
		}
	}
}

// versionDocument renders one crypto key version wire shape.
func versionDocument(resource ref, record *versionRecord) map[string]any {
	document := map[string]any{
		keyName:            resource.versionName(record.Number),
		keyState:           record.State,
		keyAlgorithm:       record.Algorithm,
		keyProtectionLevel: protectionSoftware,
		keyCreateTime:      stamp(record.CreatedAt),
		"generateTime":     stamp(record.CreatedAt),
	}
	if !record.DestroyAt.IsZero() {
		document["destroyTime"] = stamp(record.DestroyAt)
	}
	if record.State == stateDestroyed {
		document["destroyEventTime"] = stamp(record.DestroyAt)
	}
	return document
}

// withVersion runs use on one existing version and renders it afterwards.
func (p *Provider) withVersion(scope state.Scope, resource ref, use func(*cryptoKeyRecord, *versionRecord) error) (*gateway.RESTResponse, error) {
	var document map[string]any
	err := p.withKey(scope, resource, func(record *cryptoKeyRecord) error {
		version := record.Versions[resource.version]
		if version == nil {
			return notFound("CryptoKeyVersion", resource.versionName(resource.version))
		}
		if err := use(record, version); err != nil {
			return err
		}
		document = versionDocument(resource, version)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("crypto key version %s: %w", resource.version, err)
	}
	return respond(document)
}

// versionsCreate appends one enabled version with fresh material for the
// key's template algorithm. It does not move an existing primary; an
// ENCRYPT_DECRYPT key without one adopts the new version.
func (p *Provider) versionsCreate(request *gateway.RESTRequest, scope state.Scope, resource ref) (*gateway.RESTResponse, error) {
	if err := decode(request.JSON, "crypto_key_version", &kmspb.CryptoKeyVersion{}, &struct{}{}, "externalProtectionLevelOptions"); err != nil {
		return nil, err
	}
	var document map[string]any
	err := p.withKey(scope, resource, func(record *cryptoKeyRecord) error {
		number := strconv.Itoa(len(record.Versions) + 1)
		version, err := newVersion(number, record.Algorithm, p.clock())
		if err != nil {
			return err
		}
		record.Versions[number] = version
		if record.Primary == "" && record.Purpose == purposeEncryptDecrypt {
			record.Primary = number
		}
		document = versionDocument(resource, version)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("create the crypto key version: %w", err)
	}
	return respond(document)
}

// versionsList returns one page of the key's versions in number order
// (recorded: cloudkms-symmetric-key-lifecycle "versions-order"). view only
// adds HSM attestations, which SOFTWARE versions lack.
func (p *Provider) versionsList(request *gateway.RESTRequest, scope state.Scope, resource ref) (*gateway.RESTResponse, error) {
	var body map[string]any
	err := p.withKey(scope, resource, func(record *cryptoKeyRecord) error {
		numbers := make([]int, 0, len(record.Versions))
		for number := range record.Versions {
			value, _ := strconv.Atoi(number)
			numbers = append(numbers, value)
		}
		sort.Ints(numbers)
		page, next, err := provider.Page(numbers, request.Query, paging, gap)
		if err != nil {
			return err
		}
		items := make([]any, 0, len(page))
		for _, number := range page {
			items = append(items, versionDocument(resource, record.Versions[strconv.Itoa(number)]))
		}
		body = listBody("cryptoKeyVersions", items, next, len(numbers))
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list crypto key versions: %w", err)
	}
	return respond(body)
}

func (p *Provider) versionsGet(_ *gateway.RESTRequest, scope state.Scope, resource ref) (*gateway.RESTResponse, error) {
	return p.withVersion(scope, resource, func(*cryptoKeyRecord, *versionRecord) error { return nil })
}

// versionsPatch moves a version between ENABLED and DISABLED. The real API
// changes state only through PATCH with updateMask=state.
func (p *Provider) versionsPatch(request *gateway.RESTRequest, scope state.Scope, resource ref) (*gateway.RESTResponse, error) {
	paths, err := updateMask(request)
	if err != nil {
		return nil, err
	}
	if len(paths) != 1 || paths[0] != keyState {
		return nil, status.Errorf(codes.InvalidArgument, "Unsupported update mask %q", strings.Join(paths, ","))
	}
	var body struct {
		State stateEnum `json:"state"`
	}
	if err := decode(request.JSON, "crypto_key_version", &kmspb.CryptoKeyVersion{}, &body, "externalProtectionLevelOptions"); err != nil {
		return nil, err
	}
	if body.State != stateEnabled && body.State != stateDisabled {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid crypto key version state %q", body.State)
	}
	return p.withVersion(scope, resource, func(_ *cryptoKeyRecord, version *versionRecord) error {
		if version.State != stateEnabled && version.State != stateDisabled {
			return status.Errorf(codes.FailedPrecondition, "%s is in state %s.", resource.versionName(version.Number), version.State)
		}
		version.State = string(body.State)
		return nil
	})
}

// versionsDestroy schedules destruction after the key's destroy duration.
func (p *Provider) versionsDestroy(request *gateway.RESTRequest, scope state.Scope, resource ref) (*gateway.RESTResponse, error) {
	if err := decode(request.JSON, "", &kmspb.DestroyCryptoKeyVersionRequest{}, &struct{}{}); err != nil {
		return nil, err
	}
	return p.withVersion(scope, resource, func(record *cryptoKeyRecord, version *versionRecord) error {
		if version.State != stateEnabled && version.State != stateDisabled {
			return status.Errorf(codes.FailedPrecondition, "%s is in state %s.", resource.versionName(version.Number), version.State)
		}
		version.State = stateDestroyScheduled
		version.DestroyAt = p.clock().Add(record.DestroyDuration)
		return nil
	})
}

// versionsRestore returns a version scheduled for destruction to DISABLED.
func (p *Provider) versionsRestore(request *gateway.RESTRequest, scope state.Scope, resource ref) (*gateway.RESTResponse, error) {
	if err := decode(request.JSON, "", &kmspb.RestoreCryptoKeyVersionRequest{}, &struct{}{}); err != nil {
		return nil, err
	}
	return p.withVersion(scope, resource, func(_ *cryptoKeyRecord, version *versionRecord) error {
		if version.State != stateDestroyScheduled {
			return status.Errorf(codes.FailedPrecondition, "%s is in state %s.", resource.versionName(version.Number), version.State)
		}
		version.State, version.DestroyAt = stateDisabled, time.Time{}
		return nil
	})
}

// encryptBody is one decoded encrypt request.
type encryptBody struct {
	Plaintext    string          `json:"plaintext"`
	AAD          string          `json:"additionalAuthenticatedData"`
	PlaintextCRC json.RawMessage `json:"plaintextCrc32c"`
	AADCRC       json.RawMessage `json:"additionalAuthenticatedDataCrc32c"`
}

// encrypt seals one plaintext with the primary version's material.
func (p *Provider) encrypt(request *gateway.RESTRequest, scope state.Scope, resource ref) (*gateway.RESTResponse, error) {
	var body encryptBody
	if err := decode(request.JSON, "", &kmspb.EncryptRequest{}, &body); err != nil {
		return nil, err
	}
	plaintext, err := decodePayload(body.Plaintext, "plaintext", true)
	if err != nil {
		return nil, err
	}
	aad, err := decodePayload(body.AAD, "additionalAuthenticatedData", false)
	if err != nil {
		return nil, err
	}
	document := map[string]any{keyProtectionLevel: protectionSoftware}
	if err := verifyChecksums(document, crcCheck{plaintext, body.PlaintextCRC, "verifiedPlaintextCrc32c"},
		crcCheck{aad, body.AADCRC, "verifiedAdditionalAuthenticatedDataCrc32c"}); err != nil {
		return nil, err
	}
	number, material, err := p.primaryMaterial(scope, resource)
	if err != nil {
		return nil, err
	}
	sealed, err := seal(material, plaintext, aad, number)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Local encryption failed: %v", err)
	}
	document[keyName] = resource.versionName(number)
	document["ciphertext"] = base64.StdEncoding.EncodeToString(sealed)
	document["ciphertextCrc32c"] = checksum(sealed)
	return respond(document)
}

// primaryMaterial copies the enabled primary version's number and material.
func (p *Provider) primaryMaterial(scope state.Scope, resource ref) (string, []byte, error) {
	var number string
	var material []byte
	err := p.withKey(scope, resource, func(record *cryptoKeyRecord) error {
		if record.Purpose != purposeEncryptDecrypt {
			return wrongPurpose(resource.keyName(), record.Purpose)
		}
		version := record.Versions[record.Primary]
		if version == nil || version.State != stateEnabled {
			return status.Errorf(codes.FailedPrecondition, "%s has no enabled primary version.", resource.keyName())
		}
		number, material = version.Number, append([]byte(nil), version.Material...)
		return nil
	})
	if err != nil {
		return "", nil, fmt.Errorf("encrypt: %w", err)
	}
	return number, material, nil
}

// decryptBody is one decoded decrypt request.
type decryptBody struct {
	Ciphertext    string          `json:"ciphertext"`
	AAD           string          `json:"additionalAuthenticatedData"`
	CiphertextCRC json.RawMessage `json:"ciphertextCrc32c"`
	AADCRC        json.RawMessage `json:"additionalAuthenticatedDataCrc32c"`
}

// decrypt opens one sealed payload with its sealing version, which must be
// enabled. usedPrimary appears only when the primary version decrypted.
func (p *Provider) decrypt(request *gateway.RESTRequest, scope state.Scope, resource ref) (*gateway.RESTResponse, error) {
	var body decryptBody
	if err := decode(request.JSON, "", &kmspb.DecryptRequest{}, &body); err != nil {
		return nil, err
	}
	sealed, err := decodePayload(body.Ciphertext, "ciphertext", true)
	if err != nil {
		return nil, err
	}
	aad, err := decodePayload(body.AAD, "additionalAuthenticatedData", false)
	if err != nil {
		return nil, err
	}
	if err := verifyChecksums(map[string]any{}, crcCheck{sealed, body.CiphertextCRC, ""}, crcCheck{aad, body.AADCRC, ""}); err != nil {
		return nil, err
	}
	number, material, primary, err := p.sealingMaterial(scope, resource, sealed)
	if err != nil {
		return nil, err
	}
	plaintext, err := open(material, sealed, aad, number)
	if err != nil {
		return nil, err
	}
	document := map[string]any{
		"plaintext":        base64.StdEncoding.EncodeToString(plaintext),
		"plaintextCrc32c":  checksum(plaintext),
		keyProtectionLevel: protectionSoftware,
	}
	if primary {
		document["usedPrimary"] = true
	}
	return respond(document)
}

// sealingMaterial copies the material of the enabled version that sealed one
// envelope and reports whether it is the primary.
func (p *Provider) sealingMaterial(scope state.Scope, resource ref, sealed []byte) (string, []byte, bool, error) {
	var number string
	var material []byte
	var primary bool
	err := p.withKey(scope, resource, func(record *cryptoKeyRecord) error {
		if record.Purpose != purposeEncryptDecrypt {
			return wrongPurpose(resource.keyName(), record.Purpose)
		}
		var err error
		if number, err = envelopeVersion(sealed); err != nil {
			return err
		}
		version := record.Versions[number]
		if version == nil {
			return errInvalidCiphertext
		}
		if version.State != stateEnabled {
			return status.Errorf(codes.FailedPrecondition, "%s is not enabled, current state is: %s.", resource.versionName(number), version.State)
		}
		material, primary = append([]byte(nil), version.Material...), record.Primary == number
		return nil
	})
	if err != nil {
		return "", nil, false, fmt.Errorf("decrypt: %w", err)
	}
	return number, material, primary, nil
}

// decodePayload decodes one base64 payload field within the size bound. Proto3
// JSON accepts standard or URL-safe base64; gcloud (apitools) sends URL-safe.
func decodePayload(text, field string, required bool) ([]byte, error) {
	data, err := base64.StdEncoding.DecodeString(text)
	if err != nil {
		data, err = base64.URLEncoding.DecodeString(text)
	}
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid base64 in %s", field)
	}
	if (required && len(data) == 0) || len(data) > maxPayload {
		return nil, status.Errorf(codes.InvalidArgument, "%s is empty or exceeds %d bytes", field, maxPayload)
	}
	return data, nil
}

// crcCheck pairs one payload with its optional client checksum and the
// response field that confirms verification ("" when none is reported).
type crcCheck struct {
	data     []byte
	raw      json.RawMessage
	verified string
}

// verifyChecksums checks every supplied CRC32C and marks the verified ones.
func verifyChecksums(document map[string]any, checks ...crcCheck) error {
	for _, check := range checks {
		if len(check.raw) == 0 || string(check.raw) == "null" {
			continue
		}
		value, err := strconv.ParseUint(strings.Trim(string(check.raw), `"`), 10, 32)
		if err != nil {
			return status.Error(codes.InvalidArgument, "Invalid CRC32C checksum")
		}
		if uint32(value) != crc32.Checksum(check.data, castagnoli) {
			return status.Error(codes.InvalidArgument, "The checksum in the request did not match the data computed by the server.")
		}
		if check.verified != "" {
			document[check.verified] = true
		}
	}
	return nil
}

// checksum renders one CRC32C as the int64-as-string JSON wire form.
func checksum(data []byte) string {
	return strconv.FormatUint(uint64(crc32.Checksum(data, castagnoli)), 10)
}
