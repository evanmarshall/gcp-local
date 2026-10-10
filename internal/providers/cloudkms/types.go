// Package cloudkms implements the Cloud KMS v1 control plane: key rings,
// symmetric crypto keys, versions, and encrypt/decrypt with per-resource IAM.
// The local crypto uses a provider-held AES-GCM key; no cloud recording covers
// these methods yet, so nothing here claims recorded conformance.
package cloudkms

import (
	"errors"
	"regexp"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
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
	methodCryptoKeysGetIam  = "cloudkms.cryptoKeys.getIamPolicy"
	methodCryptoKeysSetIam  = "cloudkms.cryptoKeys.setIamPolicy"
	methodCryptoKeysEncrypt = "cloudkms.cryptoKeys.encrypt"
	methodCryptoKeysDecrypt = "cloudkms.cryptoKeys.decrypt"
	methodVersionsCreate    = "cloudkms.cryptoKeyVersions.create"
	methodVersionsList      = "cloudkms.cryptoKeyVersions.list"
	methodVersionsGet       = "cloudkms.cryptoKeyVersions.get"
	methodVersionsDestroy   = "cloudkms.cryptoKeyVersions.destroy"
	methodVersionsDisable   = "cloudkms.cryptoKeyVersions.disable"
	methodVersionsEnable    = "cloudkms.cryptoKeyVersions.enable"
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
	methodCryptoKeysGetIam:  "cloudkms.cryptoKeys.getIamPolicy",
	methodCryptoKeysSetIam:  "cloudkms.cryptoKeys.setIamPolicy",
	methodCryptoKeysEncrypt: "cloudkms.cryptoKeyVersions.useToEncrypt",
	methodCryptoKeysDecrypt: "cloudkms.cryptoKeyVersions.useToDecrypt",
	methodVersionsCreate:    "cloudkms.cryptoKeyVersions.create",
	methodVersionsList:      "cloudkms.cryptoKeyVersions.list",
	methodVersionsGet:       "cloudkms.cryptoKeyVersions.get",
	methodVersionsDestroy:   "cloudkms.cryptoKeyVersions.destroy",
	methodVersionsDisable:   "cloudkms.cryptoKeyVersions.update",
	methodVersionsEnable:    "cloudkms.cryptoKeyVersions.update",
}

// Resource-name helpers follow the real KMS resource namespace.
const (
	kindKeyRing   = "keyRing"
	kindCryptoKey = "cryptoKey"
	kindLocation  = "location"
	uidPattern    = `^[a-zA-Z0-9_-]{1,63}$`
	// State names of a crypto key version.
	stateEnabled   = "ENABLED"
	stateDisabled  = "DISABLED"
	stateDestroyed = "DESTROYED"
	// Wire document keys repeated across the response shapes.
	keyName       = "name"
	keyCreateTime = "createTime"
	testLocation  = "us-central1"
	// purposeEncryptDecrypt is the only implemented key purpose.

	purposeEncryptDecrypt = "ENCRYPT_DECRYPT"
)

// uid is the shared resource-name matcher.
var uid = regexp.MustCompile(uidPattern)

// cryptoKeyBody is one decoded crypto key create body.
type cryptoKeyBody struct {
	Purpose        string            `json:"purpose"`
	Algorithm      string            `json:"algorithm"`
	Labels         map[string]string `json:"labels"`
	RotationPeriod string            `json:"rotationPeriod"`
	NextRotation   string            `json:"nextRotationTime"`
	SkipInitial    bool              `json:"skipInitialVersionCreation"`
}

// keyRingRecord is one stored key ring.
type keyRingRecord struct {
	Name      string
	CreatedAt time.Time
}

// cryptoKeyRecord is one stored symmetric crypto key.
type cryptoKeyRecord struct {
	Name         string
	Purpose      string
	Algorithm    string
	Labels       map[string]string
	Rotation     string // duration string like 86400s, empty disables
	Versions     map[string]*versionRecord
	Primary      string
	NextRotation string
	CreatedAt    time.Time
}

// versionRecord is one stored crypto key version.
type versionRecord struct {
	Number    string
	State     string // ENABLED, DISABLED, DESTROYED
	Material  []byte
	CreatedAt time.Time
}

// scopeState is one project/location slice of provider state.
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

// wireError builds the KMS error body.
func wireError(code int, statusText, message string) *gateway.RESTResponse {
	return &gateway.RESTResponse{Status: code, JSON: map[string]any{
		"error": map[string]any{"code": code, "status": statusText, "message": message},
	}}
}

// notFound reports the KMS not-found status.
func notFound(kind, name string) error {
	return status.Errorf(codes.NotFound, "The resource '%s/%s' was not found", kind, name)
}

// gap reports an unimplemented slice boundary.
func gap(message string) error {
	return status.Error(codes.Unimplemented, "Cloud KMS coverage gap: "+message)
}

// scopeOf names one project/location scope. Regions are placement attributes
// (the storage precedent): every KMS resource lives in the project scope.
func scopeOf(project string) state.Scope {
	return state.Scope{Project: project, Location: state.Global}
}

// keyRingResource names one key ring in the IAM index.
func keyRingResource(project, ring string) string {
	return "//cloudkms.googleapis.com/projects/" + project + "/locations/" + ringLocation(ring) + "/keyRings/" + ring
}

// cryptoKeyResource names one crypto key in the IAM index.
func cryptoKeyResource(project, ring, key string) string {
	return keyRingResource(project, ring) + "/cryptoKeys/" + key
}

// ringLocation maps the request location; global locations keep their name.
func ringLocation(_ string) string { return testLocation }

// projectResource names the resource-manager project ancestor.
func projectResource(project string) string {
	return "//cloudresourcemanager.googleapis.com/projects/" + project
}

// stamp renders one creation timestamp.
func stamp(moment time.Time) string { return moment.UTC().Format("2006-01-02T15:04:05.000000Z") }

// cryptoKeyName renders the canonical crypto key name, which nests the ring.
func cryptoKeyName(project, ring, key string) string {
	return "projects/" + project + "/locations/" + ringLocation(ring) + "/keyRings/" + ring + "/cryptoKeys/" + key
}

// fullName renders the canonical KMS resource name.
func fullName(project, location, kind, id string) string {
	return "projects/" + project + "/locations/" + location + "/" + kind + "s/" + id
}
