// Package cloudkms implements the Cloud KMS v1 control plane: key rings,
// crypto keys, versions, symmetric encrypt/decrypt, asymmetric sign and
// decrypt, and HMAC, with per-resource IAM. Symmetric material is AES-256-GCM;
// asymmetric and MAC material comes from Go's crypto packages. The
// cloudkms-symmetric-key-lifecycle recording pins the wire shapes and errors
// of the recorded symmetric methods; the asymmetric and MAC methods are
// modeled locally. See docs/services/security/cloudkms.md for the boundary.
package cloudkms

import (
	"crypto"
	"crypto/elliptic"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// serviceName owns the KMS backend registration.
const serviceName = "cloudkms"

// Method name constants; the generated route table uses the same identifiers.
const (
	methodLocationsGet      = "cloudkms.locations.get"
	methodKeyRingsCreate    = "cloudkms.keyRings.create"
	methodKeyRingsList      = "cloudkms.keyRings.list"
	methodKeyRingsGet       = "cloudkms.keyRings.get"
	methodKeyRingsGetIam    = "cloudkms.keyRings.getIamPolicy"
	methodKeyRingsSetIam    = "cloudkms.keyRings.setIamPolicy"
	methodCryptoKeysCreate  = "cloudkms.cryptoKeys.create"
	methodCryptoKeysList    = "cloudkms.cryptoKeys.list"
	methodCryptoKeysGet     = "cloudkms.cryptoKeys.get"
	methodCryptoKeysUpdate  = "cloudkms.cryptoKeys.update"
	methodCryptoKeysPrimary = "cloudkms.cryptoKeys.updatePrimaryVersion"
	methodCryptoKeysGetIam  = "cloudkms.cryptoKeys.getIamPolicy"
	methodCryptoKeysSetIam  = "cloudkms.cryptoKeys.setIamPolicy"
	methodCryptoKeysEncrypt = "cloudkms.cryptoKeys.encrypt"
	methodCryptoKeysDecrypt = "cloudkms.cryptoKeys.decrypt"
	methodVersionsCreate    = "cloudkms.cryptoKeyVersions.create"
	methodVersionsList      = "cloudkms.cryptoKeyVersions.list"
	methodVersionsGet       = "cloudkms.cryptoKeyVersions.get"
	methodVersionsPatch     = "cloudkms.cryptoKeyVersions.patch"
	methodVersionsDestroy   = "cloudkms.cryptoKeyVersions.destroy"
	methodVersionsRestore   = "cloudkms.cryptoKeyVersions.restore"
	methodAsymmetricSign    = "cloudkms.cryptoKeyVersions.asymmetricSign"
	methodAsymmetricDecrypt = "cloudkms.cryptoKeyVersions.asymmetricDecrypt"
	methodGetPublicKey      = "cloudkms.cryptoKeyVersions.getPublicKey"
	methodMacSign           = "cloudkms.cryptoKeyVersions.macSign"
	methodMacVerify         = "cloudkms.cryptoKeyVersions.macVerify"
)

// permissions follows the documented Cloud KMS access-control rows.
var permissions = map[string]string{
	methodLocationsGet:      "cloudkms.locations.get",
	methodKeyRingsCreate:    "cloudkms.keyRings.create",
	methodKeyRingsList:      "cloudkms.keyRings.list",
	methodKeyRingsGet:       "cloudkms.keyRings.get",
	methodKeyRingsGetIam:    "cloudkms.keyRings.getIamPolicy",
	methodKeyRingsSetIam:    "cloudkms.keyRings.setIamPolicy",
	methodCryptoKeysCreate:  "cloudkms.cryptoKeys.create",
	methodCryptoKeysList:    "cloudkms.cryptoKeys.list",
	methodCryptoKeysGet:     "cloudkms.cryptoKeys.get",
	methodCryptoKeysUpdate:  "cloudkms.cryptoKeys.update",
	methodCryptoKeysPrimary: "cloudkms.cryptoKeys.update",
	methodCryptoKeysGetIam:  "cloudkms.cryptoKeys.getIamPolicy",
	methodCryptoKeysSetIam:  "cloudkms.cryptoKeys.setIamPolicy",
	methodCryptoKeysEncrypt: "cloudkms.cryptoKeyVersions.useToEncrypt",
	methodCryptoKeysDecrypt: "cloudkms.cryptoKeyVersions.useToDecrypt",
	methodVersionsCreate:    "cloudkms.cryptoKeyVersions.create",
	methodVersionsList:      "cloudkms.cryptoKeyVersions.list",
	methodVersionsGet:       "cloudkms.cryptoKeyVersions.get",
	methodVersionsPatch:     "cloudkms.cryptoKeyVersions.update",
	methodVersionsDestroy:   "cloudkms.cryptoKeyVersions.destroy",
	methodVersionsRestore:   "cloudkms.cryptoKeyVersions.restore",
	// The method reference pages name these permissions; macSign shares
	// useToSign with asymmetricSign.
	methodAsymmetricSign:    "cloudkms.cryptoKeyVersions.useToSign",
	methodAsymmetricDecrypt: "cloudkms.cryptoKeyVersions.useToDecrypt",
	methodGetPublicKey:      "cloudkms.cryptoKeyVersions.viewPublicKey",
	methodMacSign:           "cloudkms.cryptoKeyVersions.useToSign",
	methodMacVerify:         "cloudkms.cryptoKeyVersions.useToVerify",
}

// Resource kinds, version states, and wire keys.
const (
	kindKeyRing   = "keyRing"
	kindCryptoKey = "cryptoKey"
	kindVersion   = "cryptoKeyVersion"
	uidPattern    = `^[a-zA-Z0-9_-]{1,63}$`
	// State names of a crypto key version.
	stateEnabled          = "ENABLED"
	stateDisabled         = "DISABLED"
	stateDestroyScheduled = "DESTROY_SCHEDULED"
	stateDestroyed        = "DESTROYED"
	// Wire document keys repeated across the response shapes.
	keyName            = "name"
	keyCreateTime      = "createTime"
	keyProtectionLevel = "protectionLevel"
	keyAlgorithm       = "algorithm"
	keyState           = "state"
	// The implemented purposes, the symmetric algorithm, and the only
	// protection level.
	purposeEncryptDecrypt    = "ENCRYPT_DECRYPT"
	purposeAsymmetricSign    = "ASYMMETRIC_SIGN"
	purposeAsymmetricDecrypt = "ASYMMETRIC_DECRYPT"
	purposeMAC               = "MAC"
	algorithmSymmetric       = "GOOGLE_SYMMETRIC_ENCRYPTION"
	protectionSoftware       = "SOFTWARE"
	// defaultDestroyDuration is the documented 30-day scheduled-destroy default.
	defaultDestroyDuration = "2592000s"
)

// uid is the shared resource-name matcher.
var uid = regexp.MustCompile(uidPattern)

// algorithmSpec describes one implemented version algorithm. curve marks
// ECDSA, bits marks RSA, and pss selects PSS over PKCS#1 v1.5 signing; an
// algorithm with neither curve nor bits holds 32 bytes of raw material.
type algorithmSpec struct {
	purpose string
	hash    crypto.Hash
	curve   elliptic.Curve
	bits    int
	pss     bool
}

// algorithms lists the implemented CryptoKeyVersionAlgorithm values.
var algorithms = map[string]algorithmSpec{
	algorithmSymmetric:             {purpose: purposeEncryptDecrypt},
	"EC_SIGN_P256_SHA256":          {purpose: purposeAsymmetricSign, hash: crypto.SHA256, curve: elliptic.P256()},
	"EC_SIGN_P384_SHA384":          {purpose: purposeAsymmetricSign, hash: crypto.SHA384, curve: elliptic.P384()},
	"RSA_SIGN_PSS_2048_SHA256":     {purpose: purposeAsymmetricSign, hash: crypto.SHA256, bits: 2048, pss: true},
	"RSA_SIGN_PKCS1_2048_SHA256":   {purpose: purposeAsymmetricSign, hash: crypto.SHA256, bits: 2048},
	"RSA_DECRYPT_OAEP_2048_SHA256": {purpose: purposeAsymmetricDecrypt, hash: crypto.SHA256, bits: 2048},
	"HMAC_SHA256":                  {purpose: purposeMAC, hash: crypto.SHA256},
}

// purposes lists the implemented CryptoKey purposes.
var purposes = map[string]bool{
	purposeEncryptDecrypt: true, purposeAsymmetricSign: true, purposeAsymmetricDecrypt: true, purposeMAC: true,
}

// wrongPurpose refuses a key use the key's purpose does not allow.
func wrongPurpose(name, purpose string) error {
	return status.Errorf(codes.FailedPrecondition, "%s has purpose %s, which does not support this operation.", name, purpose)
}

// The google.cloud.kms.v1 enum numbers of the implemented names. The REST
// SDKs send $alt=json;enum-encoding=int, so request enums arrive as numbers;
// responses keep names, which the SDKs also parse.
var (
	purposeNumbers   = map[int]string{1: purposeEncryptDecrypt, 5: purposeAsymmetricSign, 6: purposeAsymmetricDecrypt, 9: purposeMAC}
	protectionNumber = map[int]string{1: protectionSoftware}
	stateNumbers     = map[int]string{1: stateEnabled, 2: stateDisabled, 3: stateDestroyed, 4: stateDestroyScheduled}
	algorithmNumbers = map[int]string{
		1: algorithmSymmetric, 2: "RSA_SIGN_PSS_2048_SHA256", 5: "RSA_SIGN_PKCS1_2048_SHA256",
		8: "RSA_DECRYPT_OAEP_2048_SHA256", 12: "EC_SIGN_P256_SHA256", 13: "EC_SIGN_P384_SHA384", 32: "HMAC_SHA256",
	}
)

// purposeEnum, protectionEnum, stateEnum, and algorithmEnum decode one enum
// field by name or number.
type (
	purposeEnum    string
	protectionEnum string
	stateEnum      string
	algorithmEnum  string
)

func (e *purposeEnum) UnmarshalJSON(raw []byte) error {
	return decodeEnum(raw, purposeNumbers, (*string)(e))
}

func (e *protectionEnum) UnmarshalJSON(raw []byte) error {
	return decodeEnum(raw, protectionNumber, (*string)(e))
}

func (e *stateEnum) UnmarshalJSON(raw []byte) error {
	return decodeEnum(raw, stateNumbers, (*string)(e))
}

func (e *algorithmEnum) UnmarshalJSON(raw []byte) error {
	return decodeEnum(raw, algorithmNumbers, (*string)(e))
}

// decodeEnum reads a name, or a number mapped through names. A number outside
// the table stays numeric so validation refuses it as an unknown name.
func decodeEnum(raw []byte, names map[int]string, target *string) error {
	if string(raw) == "null" {
		return nil
	}
	if json.Unmarshal(raw, target) == nil {
		return nil
	}
	var number int
	if err := json.Unmarshal(raw, &number); err != nil {
		return fmt.Errorf("decode enum: %w", err)
	}
	if name, ok := names[number]; ok {
		*target = name
		return nil
	}
	*target = strconv.Itoa(number)
	return nil
}

// versionTemplate is the decoded version template of a key create body.
type versionTemplate struct {
	Algorithm       algorithmEnum  `json:"algorithm"`
	ProtectionLevel protectionEnum `json:"protectionLevel"`
}

// cryptoKeyBody is one decoded crypto key create body. Strict decoding
// (keyFields) refuses the fields it lacks.
type cryptoKeyBody struct {
	Purpose         purposeEnum       `json:"purpose"`
	Labels          map[string]string `json:"labels"`
	Template        versionTemplate   `json:"versionTemplate"`
	DestroyDuration string            `json:"destroyScheduledDuration"`
}

type keyRingRecord struct {
	Name      string
	CreatedAt time.Time
}

// cryptoKeyRecord is one stored crypto key. Primary is set only for
// ENCRYPT_DECRYPT keys, as in GCP.
type cryptoKeyRecord struct {
	Name            string
	Purpose         string
	Algorithm       string
	Labels          map[string]string
	DestroyDuration time.Duration
	DestroyText     string
	Versions        map[string]*versionRecord
	Primary         string
	CreatedAt       time.Time
}

// versionRecord is one stored crypto key version. Material holds symmetric
// and HMAC keys; Private holds an ECDSA or RSA key. Both stay in process
// memory and are cleared when destruction completes.
type versionRecord struct {
	Number    string
	Algorithm string
	State     string // ENABLED, DISABLED, DESTROY_SCHEDULED, DESTROYED
	Material  []byte
	Private   crypto.Signer
	CreatedAt time.Time
	DestroyAt time.Time
}

// scopeState is one project slice of provider state, keyed by ref IDs.
type scopeState struct {
	keyRings   map[string]*keyRingRecord
	cryptoKeys map[string]*cryptoKeyRecord
}

func newScopeState() scopeState {
	return scopeState{
		keyRings: map[string]*keyRingRecord{}, cryptoKeys: map[string]*cryptoKeyRecord{},
	}
}

func (s *scopeState) reset() {
	s.keyRings = map[string]*keyRingRecord{}
	s.cryptoKeys = map[string]*cryptoKeyRecord{}
}

// errProviderShape rejects an incomplete provider construction.
var errProviderShape = errors.New("cloudkms requires registry, manager, index, and iam")

// notFound uses KMS's "<kind> <name> not found." text, not the shared kind/name form.
func notFound(kind, name string) error {
	return status.Errorf(codes.NotFound, "%s %s not found.", kind, name)
}

// gap reports an unimplemented slice boundary.
var gap = provider.CoverageGap("Cloud KMS")

// ref addresses one KMS resource from the request path.
type ref struct {
	project, location, ring, key, version string
}

// refOf reads the resource reference from the route parameters.
func refOf(request *gateway.RESTRequest) ref {
	return ref{
		project: request.Path["project"], location: request.Path["location"],
		ring: request.Path["keyRing"], key: request.Path["cryptoKey"], version: request.Path["version"],
	}
}

// ringID and keyID are the scope-state map keys.
func (r ref) ringID() string { return r.location + "/" + r.ring }
func (r ref) keyID() string  { return r.ringID() + "/" + r.key }

// ringName, keyName, and versionName render canonical KMS resource names.
func (r ref) ringName() string {
	return "projects/" + r.project + "/locations/" + r.location + "/keyRings/" + r.ring
}
func (r ref) keyName() string { return r.ringName() + "/cryptoKeys/" + r.key }
func (r ref) versionName(number string) string {
	return r.keyName() + "/cryptoKeyVersions/" + number
}

// ringResource and keyResource name the IAM index entries.
func (r ref) ringResource() string { return "//cloudkms.googleapis.com/" + r.ringName() }
func (r ref) keyResource() string  { return "//cloudkms.googleapis.com/" + r.keyName() }

// stamp keeps nanosecond precision, which KMS returns where most APIs use microseconds.
func stamp(moment time.Time) string { return moment.UTC().Format("2006-01-02T15:04:05.000000000Z") }
