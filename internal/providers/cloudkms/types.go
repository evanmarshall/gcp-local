// Package cloudkms implements the Cloud KMS v1 control plane: key rings,
// symmetric crypto keys, versions, and encrypt/decrypt with per-resource IAM.
// The local crypto uses AES-256-GCM with per-version material. The
// cloudkms-symmetric-key-lifecycle recording pins the wire shapes and errors
// of the recorded methods; see docs/cloudkms.md for the boundary.
package cloudkms

import (
	"errors"
	"regexp"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// serviceName owns the KMS backend registration.
const serviceName = "cloudkms"

// Method name constants; the generated route table uses the same identifiers.
const (
	methodLocationsList     = "cloudkms.locations.list"
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
)

// permissions follows the documented Cloud KMS access-control rows.
var permissions = map[string]string{
	methodLocationsList:     "cloudkms.locations.list",
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
	testLocation       = "us-central1"
	// The only implemented purpose, algorithm, and protection level.
	purposeEncryptDecrypt = "ENCRYPT_DECRYPT"
	algorithmSymmetric    = "GOOGLE_SYMMETRIC_ENCRYPTION"
	protectionSoftware    = "SOFTWARE"
	// defaultDestroyDuration is the documented 30-day scheduled-destroy default.
	defaultDestroyDuration = "2592000s"
)

// uid is the shared resource-name matcher.
var uid = regexp.MustCompile(uidPattern)

// versionTemplate is the decoded version template of a key create body.
type versionTemplate struct {
	Algorithm       string `json:"algorithm"`
	ProtectionLevel string `json:"protectionLevel"`
}

// cryptoKeyBody is one decoded crypto key create body.
type cryptoKeyBody struct {
	Purpose         string            `json:"purpose"`
	Labels          map[string]string `json:"labels"`
	RotationPeriod  string            `json:"rotationPeriod"`
	NextRotation    string            `json:"nextRotationTime"`
	Template        versionTemplate   `json:"versionTemplate"`
	DestroyDuration string            `json:"destroyScheduledDuration"`
	ImportOnly      bool              `json:"importOnly"`
}

type keyRingRecord struct {
	Name      string
	CreatedAt time.Time
}

// cryptoKeyRecord is one stored symmetric crypto key.
type cryptoKeyRecord struct {
	Name            string
	Purpose         string
	Algorithm       string
	Labels          map[string]string
	Rotation        string // duration string like 86400s, empty disables
	NextRotation    string
	DestroyDuration time.Duration
	DestroyText     string
	Versions        map[string]*versionRecord
	Primary         string
	CreatedAt       time.Time
}

// versionRecord is one stored crypto key version.
type versionRecord struct {
	Number    string
	State     string // ENABLED, DISABLED, DESTROY_SCHEDULED, DESTROYED
	Material  []byte
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
func gap(message string) error {
	return status.Error(codes.Unimplemented, "Cloud KMS coverage gap: "+message)
}

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
