package cloudkms

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// Names and keys repeated across the lifecycle tests. The identities mirror
// the recording: the fixture holds roles/cloudkms.admin on the project, while
// the allowed and denied callers hold nothing there.
const (
	testProject   = "gcp-local-test"
	testRing      = "gcp-local-conformance"
	testKey       = "gl-run"
	testFixture   = "fixture"
	testAllowed   = "allowed"
	testDenied    = "denied"
	testRingsPath = "/v1/projects/" + testProject + "/locations/us-central1/keyRings"
	testRingPath  = testRingsPath + "/" + testRing
	testKeysPath  = testRingPath + "/cryptoKeys"
	testKeyPath   = testKeysPath + "/" + testKey
	testVersions  = testKeyPath + "/cryptoKeyVersions"
	testEncrypter = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
	testPlaintext = "gcp-local synthetic plaintext"
	testAad       = "gcp-local-aad"
	testRotation  = "7776000s"
)

// harness drives the provider through the gateway.
type harness struct {
	t        *testing.T
	gate     *gateway.Gateway
	provider *Provider
}

// member names one test identity as an IAM member.
func member(actor string) string {
	return "serviceAccount:" + actor + "@" + testProject + ".iam.gserviceaccount.com"
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	registry := gateway.NewRegistry()
	index := state.NewIndex()
	engine, err := iam.New(index)
	if err != nil {
		t.Fatalf("build the iam engine: %v", err)
	}
	if err := index.Put("resourcemanager", state.Resource{
		Name: provider.ProjectResource(testProject), Service: "resourcemanager",
		Kind: "project", Number: "123456", Scope: state.Scope{Project: testProject, Location: state.Global},
	}); err != nil {
		t.Fatalf("register the project: %v", err)
	}
	if _, err := engine.SetPolicy(provider.ProjectResource(testProject), iam.Policy{
		Bindings: []iam.Binding{{Role: "roles/cloudkms.admin", Members: []string{member(testFixture)}}},
	}); err != nil {
		t.Fatalf("grant the project: %v", err)
	}
	provider, err := New(context.Background(), registry, backend.NewManager(time.Second, time.Second), index, engine)
	if err != nil {
		t.Fatalf("build the kms provider: %v", err)
	}
	g := gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: member(token), Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})
	return &harness{t: t, gate: g, provider: provider}
}

// as issues one REST request as the named actor.
func (h *harness) as(actor, verb, path string, body any) *httptest.ResponseRecorder {
	h.t.Helper()
	var encoded []byte
	if body != nil {
		encoded, _ = json.Marshal(body) //nolint:errchkjson // plain map bodies
	}
	request, _ := http.NewRequestWithContext(context.Background(), verb, path, bytes.NewReader(encoded))
	if encoded != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Authorization", "Bearer "+actor)
	recorder := httptest.NewRecorder()
	h.gate.ServeHTTP(recorder, request)
	return recorder
}

// invoke issues one REST request as the fixture.
func (h *harness) invoke(verb, path string, body any) *httptest.ResponseRecorder {
	h.t.Helper()
	return h.as(testFixture, verb, path, body)
}

// ok requires HTTP 200 and decodes the body.
func (h *harness) ok(recorder *httptest.ResponseRecorder, step string) map[string]any {
	h.t.Helper()
	if recorder.Code != 200 {
		h.t.Fatalf("%s status = %d body %s", step, recorder.Code, recorder.Body.String())
	}
	var document map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &document); err != nil {
		h.t.Fatalf("%s decode: %v", step, err)
	}
	return document
}

// fails requires one wire error: HTTP status, error.code, and error.status.
func (h *harness) fails(recorder *httptest.ResponseRecorder, step string, code int, statusText string) {
	h.t.Helper()
	var body struct {
		Error struct {
			Code   int    `json:"code"`
			Status string `json:"status"`
		} `json:"error"`
	}
	_ = json.Unmarshal(recorder.Body.Bytes(), &body)
	if recorder.Code != code || body.Error.Code != code || body.Error.Status != statusText {
		h.t.Fatalf("%s = %d %+v, want %d %s; body %s", step, recorder.Code, body.Error, code, statusText, recorder.Body.String())
	}
}

// failsWith is fails that also requires the error message.
func (h *harness) failsWith(recorder *httptest.ResponseRecorder, step string, code int, statusText, message string) {
	h.t.Helper()
	h.fails(recorder, step, code, statusText)
	var body struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(recorder.Body.Bytes(), &body) != nil || body.Error.Message != message {
		h.t.Fatalf("%s message = %s, want %q", step, recorder.Body.String(), message)
	}
}

// fields requires a document's sorted top-level field names.
func (h *harness) fields(document map[string]any, step string, want ...string) {
	h.t.Helper()
	got := make([]string, 0, len(document))
	for name := range document {
		got = append(got, name)
	}
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		h.t.Fatalf("%s fields = %v, want %v", step, got, want)
	}
}

// ring creates the fixture key ring as the recorded setup does.
func (h *harness) ring() {
	h.t.Helper()
	h.ok(h.invoke(http.MethodPost, testRingsPath+"?keyRingId="+testRing, map[string]any{}), "ring create")
}

// key creates the recorded labeled key and returns its document.
func (h *harness) key() map[string]any {
	h.t.Helper()
	h.ring()
	return h.ok(h.invoke(http.MethodPost, testKeysPath+"?cryptoKeyId="+testKey, map[string]any{
		"purpose": purposeEncryptDecrypt, "labels": map[string]string{"managed-by": "gcp-local", "gcp-local-run": "run"},
	}), "key create")
}

// grant gives the allowed caller encrypt/decrypt on the key.
func (h *harness) grant() {
	h.t.Helper()
	h.ok(h.invoke(http.MethodPost, testKeyPath+":setIamPolicy", map[string]any{"policy": map[string]any{
		"etag": "MA==", "bindings": []any{encrypterBinding()},
	}}), "grant")
}

// encrypt seals the recorded plaintext and AAD as actor.
func (h *harness) encrypt(actor string) *httptest.ResponseRecorder {
	h.t.Helper()
	return h.as(actor, http.MethodPost, testKeyPath+":encrypt", map[string]any{
		"plaintext": b64(testPlaintext), "additionalAuthenticatedData": b64(testAad)})
}

// decrypt opens one ciphertext as the allowed caller with the given AAD.
func (h *harness) decrypt(ciphertext, aad string) *httptest.ResponseRecorder {
	h.t.Helper()
	return h.as(testAllowed, http.MethodPost, testKeyPath+":decrypt", map[string]any{
		"ciphertext": ciphertext, "additionalAuthenticatedData": b64(aad)})
}

// encrypterBinding grants the allowed caller encrypt/decrypt.
func encrypterBinding() map[string]any {
	return map[string]any{"role": testEncrypter, "members": []string{member(testAllowed)}}
}

func b64(text string) string { return base64.StdEncoding.EncodeToString([]byte(text)) }

// versionOf returns the trailing version number of a version name.
func versionOf(name any) string {
	text, _ := name.(string)
	return text[strings.LastIndex(text, "/")+1:]
}

func TestMissingKeyRingReadsNotFoundThenCreates(t *testing.T) {
	h := newHarness(t)
	h.fails(h.invoke(http.MethodGet, testRingPath, nil), "missing ring get", 404, "NOT_FOUND")
	h.ring()
	ring := h.ok(h.invoke(http.MethodGet, testRingPath, nil), "ring get")
	h.fields(ring, "ring", keyCreateTime, keyName)
	h.fails(h.invoke(http.MethodPost, testRingsPath+"?keyRingId="+testRing, map[string]any{}), "duplicate ring", 409, "ALREADY_EXISTS")
}

func TestMissingResourcesNeverTouchAncestors(t *testing.T) {
	h := newHarness(t)
	h.fails(h.invoke(http.MethodGet, testRingPath+":getIamPolicy", nil), "missing ring policy", 404, "NOT_FOUND")
	h.fails(h.invoke(http.MethodPost, testRingPath+":setIamPolicy", map[string]any{"policy": map[string]any{}}), "missing ring set policy", 404, "NOT_FOUND")
	h.fails(h.invoke(http.MethodPost, testKeysPath+"?cryptoKeyId="+testKey, map[string]any{}), "key in missing ring", 404, "NOT_FOUND")
	h.ring()
	h.fails(h.invoke(http.MethodGet, testKeyPath, nil), "missing key", 404, "NOT_FOUND")
	h.fails(h.invoke(http.MethodPost, testKeyPath+":setIamPolicy", map[string]any{"policy": map[string]any{}}), "missing key set policy", 404, "NOT_FOUND")
	// Missing resources are authorized on their ancestor, so a caller with no
	// project grant still sees the denial rather than existence.
	h.fails(h.as(testDenied, http.MethodGet, testKeyPath, nil), "denied missing key", 403, "PERMISSION_DENIED")
}

func TestRecordedKeyCreateShape(t *testing.T) {
	h := newHarness(t)
	key := h.key()
	h.fields(key, "key-created", keyCreateTime, "destroyScheduledDuration", "labels", keyName, "primary", "purpose", "versionTemplate")
	if key[keyName] != "projects/"+testProject+"/locations/us-central1/keyRings/"+testRing+"/cryptoKeys/"+testKey || key["purpose"] != purposeEncryptDecrypt {
		t.Fatalf("key = %v", key)
	}
	primary, _ := key["primary"].(map[string]any)
	h.fields(primary, "primary", keyAlgorithm, keyCreateTime, "generateTime", keyName, keyProtectionLevel, keyState)
	if primary[keyState] != stateEnabled || versionOf(primary[keyName]) != "1" {
		t.Fatalf("primary = %v", primary)
	}
}

func TestRecordedPermissionsAndPolicy(t *testing.T) {
	h := newHarness(t)
	h.key()
	h.fails(h.encrypt(testDenied), "denied-encrypt", 403, "PERMISSION_DENIED")
	h.fails(h.encrypt(testFixture), "admin-cannot-encrypt", 403, "PERMISSION_DENIED")
	policy := h.ok(h.invoke(http.MethodGet, testKeyPath+":getIamPolicy", nil), "policy-read")
	h.fields(policy, "policy-read", "etag")
	granted := h.ok(h.invoke(http.MethodPost, testKeyPath+":setIamPolicy", map[string]any{"policy": map[string]any{
		"etag": policy["etag"], "bindings": []any{encrypterBinding()},
	}}), "encrypter-granted")
	h.fields(granted, "encrypter-granted", "bindings", "etag", "version")
	h.ok(h.encrypt(testAllowed), "allowed encrypt")
	h.fails(h.encrypt(testDenied), "still denied", 403, "PERMISSION_DENIED")
}

func TestRecordedEncryptDecryptShapes(t *testing.T) {
	h := newHarness(t)
	h.key()
	h.grant()
	sealed := h.ok(h.encrypt(testAllowed), "encrypted")
	h.fields(sealed, "encrypted", "ciphertext", "ciphertextCrc32c", keyName, keyProtectionLevel)
	if versionOf(sealed[keyName]) != "1" || sealed[keyProtectionLevel] != protectionSoftware {
		t.Fatalf("encrypted = %v", sealed)
	}
	ciphertext, _ := sealed["ciphertext"].(string)
	opened := h.ok(h.decrypt(ciphertext, testAad), "decrypted")
	h.fields(opened, "decrypted", "plaintext", "plaintextCrc32c", keyProtectionLevel, "usedPrimary")
	if opened["plaintext"] != b64(testPlaintext) || opened["usedPrimary"] != true {
		t.Fatalf("decrypted = %v", opened)
	}
	h.fails(h.decrypt(ciphertext, "other"), "wrong-aad", 400, "INVALID_ARGUMENT")
}

// Proto3 JSON accepts URL-safe base64, which gcloud (apitools) sends; other
// text is still refused by field name.
func TestDecryptAcceptsURLSafeBase64(t *testing.T) {
	h := newHarness(t)
	h.key()
	h.grant()
	sealed := h.ok(h.encrypt(testAllowed), "encrypted")
	ciphertext, _ := sealed["ciphertext"].(string)
	raw, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		t.Fatal(err)
	}
	opened := h.ok(h.decrypt(base64.URLEncoding.EncodeToString(raw), testAad), "url-safe")
	if opened["plaintext"] != b64(testPlaintext) {
		t.Fatalf("decrypted = %v", opened)
	}
	// 0xfb 0xff encodes as "+/8=" in standard and "-_8=" in URL-safe base64.
	if data, err := decodePayload("-_8=", "ciphertext", true); err != nil || !bytes.Equal(data, []byte{0xfb, 0xff}) {
		t.Fatalf("url-safe payload = %v, %v", data, err)
	}
	h.failsWith(h.decrypt("not base64!", testAad), "invalid base64", 400, "INVALID_ARGUMENT",
		`Invalid value at 'ciphertext' (TYPE_BYTES), "not base64!"`)
}

func TestRecordedVersionRotationLifecycle(t *testing.T) {
	h := newHarness(t)
	h.key()
	h.grant()
	sealed := h.ok(h.encrypt(testAllowed), "encrypted")
	ciphertext, _ := sealed["ciphertext"].(string)
	added := h.ok(h.invoke(http.MethodPost, testVersions, map[string]any{}), "version-added")
	h.fields(added, "version-added", keyAlgorithm, keyCreateTime, "generateTime", keyName, keyProtectionLevel, keyState)
	if versionOf(added[keyName]) != "2" || added[keyState] != stateEnabled || added[keyAlgorithm] != algorithmSymmetric {
		t.Fatalf("version-added = %v", added)
	}
	promoted := h.ok(h.invoke(http.MethodPost, testKeyPath+":updatePrimaryVersion", map[string]any{"cryptoKeyVersionId": "2"}), "primary-updated")
	primary, _ := promoted["primary"].(map[string]any)
	if versionOf(primary[keyName]) != "2" {
		t.Fatalf("primary-updated = %v", promoted)
	}
	second := h.ok(h.encrypt(testAllowed), "encrypted-v2")
	if versionOf(second[keyName]) != "2" {
		t.Fatalf("encrypted-v2 = %v", second)
	}
	old := h.ok(h.decrypt(ciphertext, testAad), "old-ciphertext-decrypts")
	if _, present := old["usedPrimary"]; present {
		t.Fatalf("old-ciphertext-decrypts = %v", old)
	}
	recordedDisableDestroy(h, ciphertext)
}

// recordedDisableDestroy continues the recorded lifecycle from the version-1
// ciphertext: disable, refused decrypt, scheduled destroy, and listing.
func recordedDisableDestroy(h *harness, ciphertext string) {
	h.t.Helper()
	disabled := h.ok(h.invoke(http.MethodPatch, testVersions+"/1?updateMask=state", map[string]any{keyState: stateDisabled}), "version-disabled")
	h.fields(disabled, "version-disabled", keyAlgorithm, keyCreateTime, "generateTime", keyName, keyProtectionLevel, keyState)
	if disabled[keyState] != stateDisabled {
		h.t.Fatalf("version-disabled = %v", disabled)
	}
	h.fails(h.decrypt(ciphertext, testAad), "disabled-decrypt", 400, "FAILED_PRECONDITION")
	destroyed := h.ok(h.invoke(http.MethodPost, testVersions+"/1:destroy", map[string]any{}), "version-destroyed")
	h.fields(destroyed, "version-destroyed", keyAlgorithm, keyCreateTime, "destroyTime", "generateTime", keyName, keyProtectionLevel, keyState)
	if destroyed[keyState] != stateDestroyScheduled {
		h.t.Fatalf("version-destroyed = %v", destroyed)
	}
	if got := h.versionStates(); !reflect.DeepEqual(got, []string{stateDestroyScheduled, stateEnabled}) {
		h.t.Fatalf("versions-listed = %v", got)
	}
}

// versionStates lists the key's version states in sorted order.
func (h *harness) versionStates() []string {
	h.t.Helper()
	var listed struct {
		Versions []struct {
			State string `json:"state"`
		} `json:"cryptoKeyVersions"`
	}
	recorder := h.invoke(http.MethodGet, testVersions, nil)
	if err := json.Unmarshal(recorder.Body.Bytes(), &listed); err != nil || recorder.Code != 200 {
		h.t.Fatalf("version list = %d %v", recorder.Code, err)
	}
	states := make([]string, 0, len(listed.Versions))
	for _, version := range listed.Versions {
		states = append(states, version.State)
	}
	sort.Strings(states)
	return states
}

func TestRecordedCleanupDestroysRemainingVersions(t *testing.T) {
	h := newHarness(t)
	h.key()
	h.ok(h.invoke(http.MethodPost, testVersions, map[string]any{}), "version 2")
	h.ok(h.invoke(http.MethodPost, testVersions+"/1:destroy", map[string]any{}), "destroy 1")
	h.ok(h.invoke(http.MethodPost, testVersions+"/2:destroy", map[string]any{}), "destroy 2")
	if got := h.versionStates(); !reflect.DeepEqual(got, []string{stateDestroyScheduled, stateDestroyScheduled}) {
		t.Fatalf("states = %v", got)
	}
	h.fails(h.invoke(http.MethodPost, testVersions+"/1:destroy", map[string]any{}), "destroy twice", 400, "FAILED_PRECONDITION")
	h.ok(h.invoke(http.MethodGet, testKeyPath, nil), "key read after destroy")
	// Scheduled destruction completes after the default 30-day duration.
	h.provider.clock = func() time.Time { return time.Now().Add(31 * 24 * time.Hour) }
	if got := h.versionStates(); !reflect.DeepEqual(got, []string{stateDestroyed, stateDestroyed}) {
		t.Fatalf("settled states = %v", got)
	}
}

func TestVersionPatchValidation(t *testing.T) {
	h := newHarness(t)
	h.key()
	h.fails(h.invoke(http.MethodPatch, testVersions+"/1", map[string]any{keyState: stateDisabled}), "no mask", 400, "INVALID_ARGUMENT")
	h.fails(h.invoke(http.MethodPatch, testVersions+"/1?updateMask=state", map[string]any{keyState: stateDestroyed}), "bad state", 400, "INVALID_ARGUMENT")
	h.fails(h.invoke(http.MethodPatch, testVersions+"/9?updateMask=state", map[string]any{keyState: stateDisabled}), "missing version", 404, "NOT_FOUND")
	h.ok(h.invoke(http.MethodPatch, testVersions+"/1?updateMask=state", map[string]any{keyState: stateDisabled}), "disable")
	h.fails(h.encrypt(testFixture), "admin still cannot encrypt", 403, "PERMISSION_DENIED")
	h.fails(h.invoke(http.MethodPost, testKeyPath+":updatePrimaryVersion", map[string]any{"cryptoKeyVersionId": "1"}), "disabled primary", 400, "FAILED_PRECONDITION")
	h.ok(h.invoke(http.MethodPatch, testVersions+"/1?updateMask=state", map[string]any{keyState: stateEnabled}), "enable")
	h.ok(h.invoke(http.MethodPost, testVersions+"/1:destroy", map[string]any{}), "destroy")
	h.fails(h.invoke(http.MethodPatch, testVersions+"/1?updateMask=state", map[string]any{keyState: stateEnabled}), "enable destroyed", 400, "FAILED_PRECONDITION")
	// The invented enable/disable verbs are not routed.
	if recorder := h.invoke(http.MethodPost, testVersions+"/1:disable", map[string]any{}); recorder.Code == 200 {
		t.Fatalf(":disable is still routed")
	}
}

func TestDisabledPrimaryRefusesEncrypt(t *testing.T) {
	h := newHarness(t)
	h.key()
	h.grant()
	h.ok(h.invoke(http.MethodPatch, testVersions+"/1?updateMask=state", map[string]any{keyState: stateDisabled}), "disable")
	h.fails(h.encrypt(testAllowed), "disabled primary encrypt", 400, "FAILED_PRECONDITION")
}

func TestCiphertextIsBoundToItsKey(t *testing.T) {
	h := newHarness(t)
	h.key()
	h.grant()
	sealed := h.ok(h.encrypt(testAllowed), "encrypt")
	other := testKeysPath + "/other"
	h.ok(h.invoke(http.MethodPost, testKeysPath+"?cryptoKeyId=other", map[string]any{}), "other key")
	h.ok(h.invoke(http.MethodPost, other+":setIamPolicy", map[string]any{"policy": map[string]any{
		"bindings": []any{encrypterBinding()},
	}}), "grant other")
	recorder := h.as(testAllowed, http.MethodPost, other+":decrypt", map[string]any{
		"ciphertext": sealed["ciphertext"], "additionalAuthenticatedData": b64(testAad)})
	h.fails(recorder, "cross-key decrypt", 400, "INVALID_ARGUMENT")
	h.fails(h.decrypt(b64("garbage"), testAad), "garbage", 400, "INVALID_ARGUMENT")
}

func TestEncryptChecksums(t *testing.T) {
	h := newHarness(t)
	h.key()
	h.grant()
	body := map[string]any{"plaintext": b64(testPlaintext), "plaintextCrc32c": checksum([]byte(testPlaintext))}
	sealed := h.ok(h.as(testAllowed, http.MethodPost, testKeyPath+":encrypt", body), "verified encrypt")
	if sealed["verifiedPlaintextCrc32c"] != true {
		t.Fatalf("encrypt = %v", sealed)
	}
	if _, present := sealed["verifiedAdditionalAuthenticatedDataCrc32c"]; present {
		t.Fatalf("unsent AAD checksum reported verified: %v", sealed)
	}
	body["plaintextCrc32c"] = "1"
	h.fails(h.as(testAllowed, http.MethodPost, testKeyPath+":encrypt", body), "bad checksum", 400, "INVALID_ARGUMENT")
}

// TestKeyUpdateMaskAndRotation: a rotation schedule is validated, stored,
// reported, patched, and cleared.
func TestKeyUpdateMaskAndRotation(t *testing.T) {
	h := newHarness(t)
	h.ring()
	h.failsWith(h.invoke(http.MethodPost, testKeysPath+"?cryptoKeyId="+testKey, map[string]any{
		"purpose": purposeEncryptDecrypt, "rotationPeriod": testRotation,
	}), "period alone", 400, "INVALID_ARGUMENT", "nextRotationTime must be set when rotationPeriod is set.")
	h.failsWith(h.invoke(http.MethodPost, testKeysPath+"?cryptoKeyId="+testKey, map[string]any{
		"purpose": purposeEncryptDecrypt, "rotationPeriod": "3600s", "nextRotationTime": "2030-01-01T00:00:00Z",
	}), "short period", 400, "INVALID_ARGUMENT", `Invalid rotationPeriod "3600s": it must be between 24 hours and 876000 hours.`)
	created := h.ok(h.invoke(http.MethodPost, testKeysPath+"?cryptoKeyId="+testKey, map[string]any{
		"purpose": purposeEncryptDecrypt}), "key")
	if created["destroyScheduledDuration"] != defaultDestroyDuration {
		t.Fatalf("created = %v", created)
	}
	if _, present := created["labels"]; present {
		t.Fatalf("empty labels rendered: %v", created)
	}
	h.fails(h.invoke(http.MethodPatch, testKeyPath, map[string]any{"labels": map[string]string{"a": "b"}}), "no mask", 400, "INVALID_ARGUMENT")
	labeled := h.ok(h.invoke(http.MethodPatch, testKeyPath+"?updateMask=labels", map[string]any{"labels": map[string]string{"a": "b"}}), "labels")
	if labeled["labels"] == nil {
		t.Fatalf("labeled = %v", labeled)
	}
	h.failsWith(h.invoke(http.MethodPatch, testKeyPath+"?updateMask=rotationPeriod",
		map[string]any{"rotationPeriod": testRotation}), "period without a time", 400, "INVALID_ARGUMENT",
		"nextRotationTime must be set when rotationPeriod is set.")
	scheduled := h.ok(h.invoke(http.MethodPatch, testKeyPath+"?updateMask=rotationPeriod,nextRotationTime",
		map[string]any{"rotationPeriod": testRotation, "nextRotationTime": "2030-01-01T00:00:00Z"}), "schedule rotation")
	if scheduled["rotationPeriod"] != testRotation || scheduled["nextRotationTime"] != "2030-01-01T00:00:00.000000000Z" {
		t.Fatalf("scheduled = %v", scheduled)
	}
	// The released Terraform provider clears rotation this way on destroy.
	cleared := h.ok(h.invoke(http.MethodPatch, testKeyPath+"?updateMask=rotationPeriod,nextRotationTime",
		map[string]any{"rotationPeriod": nil, "nextRotationTime": nil}), "clear rotation")
	if _, present := cleared["rotationPeriod"]; present || cleared["nextRotationTime"] != nil || cleared["labels"] == nil {
		t.Fatalf("cleared = %v", cleared)
	}
	h.fails(h.invoke(http.MethodPatch, testKeyPath+"?updateMask=primary", map[string]any{}), "invented primary path", 400, "INVALID_ARGUMENT")
}

// TestScheduledRotationCreatesPrimaryVersions: at each nextRotationTime the
// key gains a version that becomes primary, created at that time, and the
// schedule advances by rotationPeriod. Older versions still decrypt.
func TestScheduledRotationCreatesPrimaryVersions(t *testing.T) {
	h := newHarness(t)
	h.ring()
	start := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	h.provider.clock = func() time.Time { return start }
	h.ok(h.invoke(http.MethodPost, testKeysPath+"?cryptoKeyId="+testKey, map[string]any{"purpose": purposeEncryptDecrypt,
		"rotationPeriod": "86400s", "nextRotationTime": "2026-10-14T00:00:00Z"}), "key")
	h.grant()
	sealed := h.ok(h.encrypt(testAllowed), "encrypt with version 1")
	h.provider.clock = func() time.Time { return start.Add(37 * time.Hour) }
	key := h.ok(h.invoke(http.MethodGet, testKeyPath, nil), "rotated key")
	primary, _ := key["primary"].(map[string]any)
	if versionOf(primary[keyName]) != "3" || primary[keyCreateTime] != "2026-10-15T00:00:00.000000000Z" ||
		key["nextRotationTime"] != "2026-10-16T00:00:00.000000000Z" {
		t.Fatalf("key = %v", key)
	}
	if states := h.versionStates(); len(states) != 3 {
		t.Fatalf("versions = %v", states)
	}
	ciphertext, _ := sealed["ciphertext"].(string)
	opened := h.ok(h.decrypt(ciphertext, testAad), "decrypt with version 1")
	if opened["usedPrimary"] != nil {
		t.Fatalf("decrypt = %v", opened)
	}
	reencrypted := h.ok(h.encrypt(testAllowed), "encrypt with the new primary")
	if versionOf(reencrypted[keyName]) != "3" {
		t.Fatalf("encrypt = %v", reencrypted)
	}
}

func TestSkipInitialVersionCreation(t *testing.T) {
	h := newHarness(t)
	h.ring()
	key := h.ok(h.invoke(http.MethodPost, testKeysPath+"?cryptoKeyId="+testKey+"&skipInitialVersionCreation=true", map[string]any{}), "empty key")
	if _, present := key["primary"]; present {
		t.Fatalf("key = %v", key)
	}
	h.ok(h.invoke(http.MethodPost, testVersions, map[string]any{}), "first version")
	key = h.ok(h.invoke(http.MethodGet, testKeyPath, nil), "key")
	primary, _ := key["primary"].(map[string]any)
	if versionOf(primary[keyName]) != "1" {
		t.Fatalf("key = %v", key)
	}
}

func TestKeyRingIamPolicyAndLists(t *testing.T) {
	h := newHarness(t)
	h.key()
	h.fields(h.ok(h.invoke(http.MethodGet, testRingPath+":getIamPolicy", nil), "ring policy"), "ring policy", "etag")
	h.ok(h.invoke(http.MethodPost, testRingPath+":setIamPolicy", map[string]any{"policy": map[string]any{
		"bindings": []any{encrypterBinding()},
	}}), "ring grant")
	// A ring-level grant reaches the key through the resource hierarchy.
	h.ok(h.encrypt(testAllowed), "ring-granted encrypt")
	rings := h.ok(h.invoke(http.MethodGet, testRingsPath, nil), "ring list")
	keys := h.ok(h.invoke(http.MethodGet, testKeysPath, nil), "key list")
	if items, _ := rings["keyRings"].([]any); len(items) != 1 {
		t.Fatalf("rings = %v", rings)
	}
	if items, _ := keys["cryptoKeys"].([]any); len(items) != 1 {
		t.Fatalf("keys = %v", keys)
	}
}

func TestEveryRouteIsHandled(t *testing.T) {
	h := newHarness(t)
	h.key()
	probe := func(verb, path string, body any) {
		recorder := h.invoke(verb, path, body)
		if recorder.Code == 501 || recorder.Code == 500 || recorder.Code == 404 {
			t.Fatalf("%s %s = %d %s", verb, path, recorder.Code, recorder.Body.String())
		}
	}
	probe(http.MethodGet, "/v1/projects/"+testProject+"/locations/us-central1", nil)
	probe(http.MethodGet, testRingsPath, nil)
	probe(http.MethodGet, testRingPath, nil)
	probe(http.MethodGet, testRingPath+":getIamPolicy", nil)
	probe(http.MethodGet, testKeysPath, nil)
	probe(http.MethodGet, testKeyPath, nil)
	probe(http.MethodGet, testKeyPath+":getIamPolicy", nil)
	probe(http.MethodGet, testVersions, nil)
	probe(http.MethodGet, testVersions+"/1", nil)
	probe(http.MethodPost, testKeyPath+":updatePrimaryVersion", map[string]any{"cryptoKeyVersionId": "1"})
	probe(http.MethodPatch, testVersions+"/1?updateMask=state", map[string]any{keyState: stateEnabled})
	// The purpose-specific routes reach their handlers and refuse the symmetric key.
	probe(http.MethodGet, testVersions+"/1/publicKey", nil)
	for _, verb := range []string{":asymmetricSign", ":asymmetricDecrypt", ":macSign", ":macVerify", ":restore"} {
		probe(http.MethodPost, testVersions+"/1"+verb, map[string]any{"data": b64("x"), "ciphertext": b64("x"), "mac": b64("x")})
	}
}

// errorDetails decodes an error's message and its first ErrorInfo.
func errorDetails(t *testing.T, recorder *httptest.ResponseRecorder) (string, map[string]any) {
	t.Helper()
	var body struct {
		Error struct {
			Message string           `json:"message"`
			Details []map[string]any `json:"details"`
		} `json:"error"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode the error: %v", err)
	}
	for _, detail := range body.Error.Details {
		if detail["@type"] == "type.googleapis.com/google.rpc.ErrorInfo" {
			return body.Error.Message, detail
		}
	}
	return body.Error.Message, nil
}

// TestRecordedDenialDetails pins cloudkms-symmetric-key-lifecycle
// "denied-encrypt": the key named, ErrorInfo with only the permission.
func TestRecordedDenialDetails(t *testing.T) {
	h := newHarness(t)
	h.key()
	denied := h.encrypt(testDenied)
	h.fails(denied, "denied encrypt", 403, "PERMISSION_DENIED")
	message, info := errorDetails(t, denied)
	name := strings.TrimPrefix(testKeyPath, "/v1/")
	if message != "Permission 'cloudkms.cryptoKeyVersions.useToEncrypt' denied on resource '"+name+"' (or it may not exist)." {
		t.Fatalf("message = %q", message)
	}
	want := map[string]any{"@type": "type.googleapis.com/google.rpc.ErrorInfo", "reason": "IAM_PERMISSION_DENIED",
		"domain": "cloudkms.googleapis.com", "metadata": map[string]any{"permission": "cloudkms.cryptoKeyVersions.useToEncrypt"}}
	if !reflect.DeepEqual(info, want) {
		t.Fatalf("details = %v", info)
	}
	// A version method names the version.
	message, _ = errorDetails(t, h.as(testDenied, http.MethodGet, testVersions+"/1", nil))
	if message != "Permission 'cloudkms.cryptoKeyVersions.get' denied on resource '"+name+"/cryptoKeyVersions/1' (or it may not exist)." {
		t.Fatalf("version message = %q", message)
	}
}

// TestListPagesAndRefusals pins "versions-order", "versions-paged", and the
// recorded page refusals; filter and orderBy are coverage gaps.
func TestListPagesAndRefusals(t *testing.T) {
	h := newHarness(t)
	h.key()
	h.ok(h.invoke(http.MethodPost, testVersions, map[string]any{}), "version 2")
	first := h.ok(h.invoke(http.MethodGet, testVersions+"?pageSize=1", nil), "first page")
	token, _ := first["nextPageToken"].(string)
	if got := pageVersions(first); !reflect.DeepEqual(got, []string{"1"}) || token == "" || first["totalSize"] != float64(2) {
		t.Fatalf("first page = %v", first)
	}
	second := h.ok(h.invoke(http.MethodGet, testVersions+"?pageSize=1&pageToken="+token, nil), "second page")
	if _, more := second["nextPageToken"]; more || !reflect.DeepEqual(pageVersions(second), []string{"2"}) {
		t.Fatalf("second page = %v", second)
	}
	h.failsWith(h.invoke(http.MethodGet, testVersions+"?pageToken=not-a-page-token", nil), "bad token", 400, "INVALID_ARGUMENT", "Invalid page token.")
	h.failsWith(h.invoke(http.MethodGet, testVersions+"?pageSize=-1", nil), "negative size", 400, "INVALID_ARGUMENT", "Page size must be positive.")
	h.failsWith(h.invoke(http.MethodGet, testVersions+"?filter=state%3DENABLED", nil), "filter", 501, "UNIMPLEMENTED",
		"Cloud KMS coverage gap: list filter is not implemented")
	h.failsWith(h.invoke(http.MethodGet, testKeysPath+"?orderBy=name", nil), "orderBy", 501, "UNIMPLEMENTED",
		"Cloud KMS coverage gap: list orderBy is not implemented")
	keys := h.ok(h.invoke(http.MethodGet, testKeysPath+"?pageSize=1", nil), "keys")
	if _, more := keys["nextPageToken"]; more || keys["totalSize"] != float64(1) {
		t.Fatalf("keys = %v", keys)
	}
	rings := h.ok(h.invoke(http.MethodGet, testRingsPath, nil), "rings")
	if rings["totalSize"] != float64(1) {
		t.Fatalf("rings = %v", rings)
	}
}

// pageVersions lists the version numbers of one versions page.
func pageVersions(page map[string]any) []string {
	items, _ := page["cryptoKeyVersions"].([]any)
	numbers := make([]string, 0, len(items))
	for _, item := range items {
		if version, ok := item.(map[string]any); ok {
			numbers = append(numbers, versionOf(version[keyName]))
		}
	}
	return numbers
}

// TestStrictBodies pins the recorded refusals of unknown and mistyped fields.
func TestStrictBodies(t *testing.T) {
	h := newHarness(t)
	h.key()
	h.grant()
	h.failsWith(h.invoke(http.MethodPatch, testKeyPath+"?updateMask=labels", map[string]any{"labels": map[string]string{}, "unknownField": "x"}),
		"unknown field", 400, "INVALID_ARGUMENT", `Invalid JSON payload received. Unknown name "unknownField" at 'crypto_key': Cannot find field.`)
	h.failsWith(h.invoke(http.MethodPatch, testKeyPath+"?updateMask=versionTemplate.algorithm", map[string]any{
		"versionTemplate": map[string]any{keyAlgorithm: algorithmSymmetric, "unknownField": "x"}}),
		"unknown nested field", 400, "INVALID_ARGUMENT", `Invalid JSON payload received. Unknown name "unknownField" at 'crypto_key.version_template': Cannot find field.`)
	h.failsWith(h.as(testAllowed, http.MethodPost, testKeyPath+":encrypt", map[string]any{"plaintext": b64(testPlaintext), "plaintextCrc32c": "many"}),
		"mistyped field", 400, "INVALID_ARGUMENT", "Invalid value at 'plaintext_crc32c.value' (TYPE_INT64), ")
	h.failsWith(h.invoke(http.MethodPost, testRingsPath+"?keyRingId=other", map[string]any{"unknownField": "x"}),
		"unknown key ring field", 400, "INVALID_ARGUMENT", `Invalid JSON payload received. Unknown name "unknownField" at 'key_ring': Cannot find field.`)
	h.failsWith(h.invoke(http.MethodPost, testKeyPath+":setIamPolicy", map[string]any{"policy": map[string]any{
		"auditConfigs": []any{map[string]any{"service": "allServices"}}}}), "audit configs", 501, "UNIMPLEMENTED",
		"Cloud KMS coverage gap: policy.auditConfigs is not modeled")
}

// TestConditionalGrantSeesResourceAttributes pins "conditional-access": a
// condition on the CryptoKey type and name grants reads of the key and its
// version list, which are checked on the key, but not a version get.
func TestConditionalGrantSeesResourceAttributes(t *testing.T) {
	h := newHarness(t)
	h.key()
	name := strings.TrimPrefix(testKeyPath, "/v1/")
	h.ok(h.invoke(http.MethodPost, testKeyPath+":setIamPolicy", map[string]any{"policy": map[string]any{
		"version": 3, "bindings": []any{map[string]any{"role": "roles/cloudkms.viewer", "members": []string{member(testAllowed)},
			"condition": map[string]any{"title": "key-only", "expression": `resource.type == "cloudkms.googleapis.com/CryptoKey" && ` +
				`resource.service == "cloudkms.googleapis.com" && resource.name == "` + name + `"`}}},
	}}), "conditional grant")
	h.ok(h.as(testAllowed, http.MethodGet, testKeyPath, nil), "key get")
	h.ok(h.as(testAllowed, http.MethodGet, testVersions, nil), "version list")
	h.fails(h.as(testAllowed, http.MethodGet, testVersions+"/1", nil), "version get", 403, "PERMISSION_DENIED")
}

func TestLocationsGet(t *testing.T) {
	h := newHarness(t)
	got := h.ok(h.invoke(http.MethodGet, "/v1/projects/"+testProject+"/locations/us-east1", nil), "location")
	if got[keyName] != "projects/"+testProject+"/locations/us-east1" || got["locationId"] != "us-east1" {
		t.Fatalf("location = %v", got)
	}
}
