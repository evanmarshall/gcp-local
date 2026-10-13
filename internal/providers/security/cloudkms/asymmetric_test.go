package cloudkms

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
)

const (
	testSignerVerifier  = "roles/cloudkms.signerVerifier"
	testSigner          = "roles/cloudkms.signer"
	testPublicKeyViewer = "roles/cloudkms.publicKeyViewer"
	testDecrypter       = "roles/cloudkms.cryptoKeyDecrypter"
	testMessage         = "gcp-local synthetic message"
	algorithmP256       = "EC_SIGN_P256_SHA256"
	algorithmOAEP       = "RSA_DECRYPT_OAEP_2048_SHA256"
	algorithmHMAC       = "HMAC_SHA256"
)

// purposeKey creates one key of a purpose and algorithm and returns its path.
func (h *harness) purposeKey(id, purpose, algorithm string) string {
	h.t.Helper()
	created := h.ok(h.invoke(http.MethodPost, testKeysPath+"?cryptoKeyId="+id, map[string]any{
		"purpose": purpose, "versionTemplate": map[string]any{keyAlgorithm: algorithm},
	}), "create "+id)
	if _, present := created["primary"]; present && purpose != purposeEncryptDecrypt {
		h.t.Fatalf("%s key has a primary: %v", purpose, created)
	}
	return testKeysPath + "/" + id
}

// grantOn binds role to the allowed caller on one key.
func (h *harness) grantOn(path, role string) {
	h.t.Helper()
	h.ok(h.invoke(http.MethodPost, path+":setIamPolicy", map[string]any{"policy": map[string]any{
		"bindings": []any{map[string]any{"role": role, "members": []string{member(testAllowed)}}},
	}}), "grant "+role)
}

// publicKey reads and parses one version's PEM public key as the allowed caller.
func (h *harness) publicKey(path string) crypto.PublicKey {
	h.t.Helper()
	document := h.ok(h.as(testAllowed, http.MethodGet, path+"/cryptoKeyVersions/1/publicKey", nil), "public key")
	h.fields(document, "public key", "pem", keyAlgorithm, "pemCrc32c", keyName, keyProtectionLevel)
	text, _ := document["pem"].(string)
	if document["pemCrc32c"] != checksum([]byte(text)) {
		h.t.Fatalf("pemCrc32c = %v", document["pemCrc32c"])
	}
	block, _ := pem.Decode([]byte(text))
	if block == nil || block.Type != "PUBLIC KEY" {
		h.t.Fatalf("pem = %q", text)
	}
	key, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		h.t.Fatalf("parse public key: %v", err)
	}
	return key
}

// sign asks the allowed caller for one signature and returns its bytes.
func (h *harness) sign(path string, body map[string]any, step string) []byte {
	h.t.Helper()
	signed := h.ok(h.as(testAllowed, http.MethodPost, path+"/cryptoKeyVersions/1:asymmetricSign", body), step)
	signature, err := base64.StdEncoding.DecodeString(provider.Text(signed["signature"]))
	if err != nil {
		h.t.Fatalf("%s signature: %v", step, err)
	}
	if signed["signatureCrc32c"] != checksum(signature) || versionOf(signed[keyName]) != "1" {
		h.t.Fatalf("%s = %v", step, signed)
	}
	return signature
}

func TestAsymmetricSignVerifiesWithThePublicKey(t *testing.T) {
	h := newHarness(t)
	h.ring()
	for index, algorithm := range []string{algorithmP256, "EC_SIGN_P384_SHA384", "RSA_SIGN_PSS_2048_SHA256", "RSA_SIGN_PKCS1_2048_SHA256"} {
		spec := algorithms[algorithm]
		path := h.purposeKey("sign-"+strconv.Itoa(index), purposeAsymmetricSign, algorithm)
		h.grantOn(path, testSignerVerifier)
		public := h.publicKey(path)
		hash := spec.hash.New()
		hash.Write([]byte(testMessage))
		digest := hash.Sum(nil)
		byDigest := h.sign(path, map[string]any{
			"digest": map[string]any{digestFields[spec.hash]: base64.StdEncoding.EncodeToString(digest)}, "digestCrc32c": checksum(digest),
		}, algorithm+" digest")
		byData := h.sign(path, map[string]any{"data": b64(testMessage), "dataCrc32c": checksum([]byte(testMessage))}, algorithm+" data")
		for _, signature := range [][]byte{byDigest, byData} {
			if !verifies(spec, public, digest, signature) {
				t.Fatalf("%s signature does not verify", algorithm)
			}
		}
	}
}

// verifies checks one signature with Go's crypto.
func verifies(spec algorithmSpec, public crypto.PublicKey, digest, signature []byte) bool {
	switch key := public.(type) {
	case *ecdsa.PublicKey:
		return ecdsa.VerifyASN1(key, digest, signature)
	case *rsa.PublicKey:
		if spec.pss {
			return rsa.VerifyPSS(key, spec.hash, digest, signature, &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash}) == nil
		}
		return rsa.VerifyPKCS1v15(key, spec.hash, digest, signature) == nil
	}
	return false
}

func TestSignReportsVerifiedChecksums(t *testing.T) {
	h := newHarness(t)
	h.ring()
	path := h.purposeKey("sign", purposeAsymmetricSign, algorithmP256)
	h.grantOn(path, testSigner)
	digest := sha256.Sum256([]byte(testMessage))
	signed := h.ok(h.as(testAllowed, http.MethodPost, path+"/cryptoKeyVersions/1:asymmetricSign", map[string]any{
		"digest": map[string]any{"sha256": base64.StdEncoding.EncodeToString(digest[:])}, "digestCrc32c": checksum(digest[:]),
	}), "signed")
	h.fields(signed, "signed", keyName, keyProtectionLevel, "signature", "signatureCrc32c", "verifiedDigestCrc32c")
	unchecked := h.ok(h.as(testAllowed, http.MethodPost, path+"/cryptoKeyVersions/1:asymmetricSign", map[string]any{"data": b64(testMessage)}), "unchecked")
	h.fields(unchecked, "unchecked", keyName, keyProtectionLevel, "signature", "signatureCrc32c")
}

func TestAsymmetricDecryptOpensOAEPCiphertext(t *testing.T) {
	h := newHarness(t)
	h.ring()
	path := h.purposeKey("decrypt", purposeAsymmetricDecrypt, algorithmOAEP)
	h.grantOn(path, testPublicKeyViewer)
	public, _ := h.publicKey(path).(*rsa.PublicKey)
	ciphertext, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, public, []byte(testMessage), nil)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	body := map[string]any{"ciphertext": base64.StdEncoding.EncodeToString(ciphertext), "ciphertextCrc32c": checksum(ciphertext)}
	h.fails(h.as(testAllowed, http.MethodPost, path+"/cryptoKeyVersions/1:asymmetricDecrypt", body), "viewer cannot decrypt", 403, "PERMISSION_DENIED")
	h.grantOn(path, testDecrypter)
	opened := h.ok(h.as(testAllowed, http.MethodPost, path+"/cryptoKeyVersions/1:asymmetricDecrypt", body), "decrypted")
	h.fields(opened, "decrypted", "plaintext", "plaintextCrc32c", keyProtectionLevel, "verifiedCiphertextCrc32c")
	if opened["plaintext"] != b64(testMessage) || opened["plaintextCrc32c"] != checksum([]byte(testMessage)) {
		t.Fatalf("decrypted = %v", opened)
	}
	h.fails(h.as(testAllowed, http.MethodPost, path+"/cryptoKeyVersions/1:asymmetricDecrypt", map[string]any{
		"ciphertext": b64("not oaep")}), "garbage ciphertext", 400, "INVALID_ARGUMENT")
}

func TestMacSignAndVerify(t *testing.T) {
	h := newHarness(t)
	h.ring()
	path := h.purposeKey("mac", purposeMAC, algorithmHMAC)
	h.grantOn(path, testSignerVerifier)
	signed := h.ok(h.as(testAllowed, http.MethodPost, path+"/cryptoKeyVersions/1:macSign", map[string]any{
		"data": b64(testMessage), "dataCrc32c": checksum([]byte(testMessage))}), "mac signed")
	h.fields(signed, "mac signed", keyName, "mac", "macCrc32c", keyProtectionLevel, "verifiedDataCrc32c")
	tag, err := base64.StdEncoding.DecodeString(provider.Text(signed["mac"]))
	if err != nil || len(tag) != sha256.Size || signed["macCrc32c"] != checksum(tag) {
		t.Fatalf("mac signed = %v", signed)
	}
	verify := func(data string, mac []byte) map[string]any {
		return h.ok(h.as(testAllowed, http.MethodPost, path+"/cryptoKeyVersions/1:macVerify", map[string]any{
			"data": b64(data), "mac": base64.StdEncoding.EncodeToString(mac), "macCrc32c": checksum(mac),
		}), "mac verify")
	}
	h.fields(verify(testMessage, tag), "verified", keyName, keyProtectionLevel, "success", "verifiedMacCrc32c", "verifiedSuccessIntegrity")
	tampered := append([]byte(nil), tag...)
	tampered[0] ^= 1
	// A failed comparison is a 200 with success omitted, the proto3 form of false.
	h.fields(verify(testMessage, tampered), "tampered mac", keyName, keyProtectionLevel, "verifiedMacCrc32c")
	h.fields(verify("other", tag), "other data", keyName, keyProtectionLevel, "verifiedMacCrc32c")
	h.fails(h.as(testAllowed, http.MethodPost, path+"/cryptoKeyVersions/1:macVerify", map[string]any{"data": b64(testMessage)}),
		"verify without mac", 400, "INVALID_ARGUMENT")
}

func TestChecksumMismatchesAreRefused(t *testing.T) {
	h := newHarness(t)
	h.ring()
	sign := h.purposeKey("sign", purposeAsymmetricSign, algorithmP256)
	decrypt := h.purposeKey("decrypt", purposeAsymmetricDecrypt, algorithmOAEP)
	mac := h.purposeKey("mac", purposeMAC, algorithmHMAC)
	for _, path := range []string{sign, decrypt, mac} {
		h.grantOn(path, testSignerVerifier)
	}
	h.grantOn(decrypt, testDecrypter)
	digest := sha256.Sum256([]byte(testMessage))
	cases := []struct {
		step, path, method string
		body               map[string]any
	}{
		{"digest crc", sign, ":asymmetricSign", map[string]any{"digest": map[string]any{"sha256": base64.StdEncoding.EncodeToString(digest[:])}, "digestCrc32c": "1"}},
		{"data crc", sign, ":asymmetricSign", map[string]any{"data": b64(testMessage), "dataCrc32c": "1"}},
		{"ciphertext crc", decrypt, ":asymmetricDecrypt", map[string]any{"ciphertext": b64("x"), "ciphertextCrc32c": "1"}},
		{"mac data crc", mac, ":macSign", map[string]any{"data": b64(testMessage), "dataCrc32c": "1"}},
		{"mac crc", mac, ":macVerify", map[string]any{"data": b64(testMessage), "mac": b64("tag"), "macCrc32c": "1"}},
	}
	for _, c := range cases {
		h.fails(h.as(testAllowed, http.MethodPost, c.path+"/cryptoKeyVersions/1"+c.method, c.body), c.step, 400, "INVALID_ARGUMENT")
	}
}

func TestWrongPurposeIsRefused(t *testing.T) {
	h := newHarness(t)
	h.key()
	sign := h.purposeKey("sign", purposeAsymmetricSign, algorithmP256)
	decrypt := h.purposeKey("decrypt", purposeAsymmetricDecrypt, algorithmOAEP)
	mac := h.purposeKey("mac", purposeMAC, algorithmHMAC)
	for _, path := range []string{testKeyPath, sign, decrypt, mac} {
		h.ok(h.invoke(http.MethodPost, path+":setIamPolicy", map[string]any{"policy": map[string]any{"bindings": []any{
			map[string]any{"role": testSignerVerifier, "members": []string{member(testAllowed)}},
			map[string]any{"role": testEncrypter, "members": []string{member(testAllowed)}},
		}}}), "grant "+path)
	}
	digest := map[string]any{"digest": map[string]any{"sha256": base64.StdEncoding.EncodeToString(make([]byte, 32))}}
	data := map[string]any{"data": b64(testMessage)}
	cases := []struct {
		step, verb, path string
		body             map[string]any
	}{
		{"sign symmetric", http.MethodPost, testVersions + "/1:asymmetricSign", digest},
		{"public key of symmetric", http.MethodGet, testVersions + "/1/publicKey", nil},
		{"mac with symmetric", http.MethodPost, testVersions + "/1:macSign", data},
		{"sign with decrypt key", http.MethodPost, decrypt + "/cryptoKeyVersions/1:asymmetricSign", digest},
		{"decrypt with sign key", http.MethodPost, sign + "/cryptoKeyVersions/1:asymmetricDecrypt", map[string]any{"ciphertext": b64("x")}},
		{"verify with sign key", http.MethodPost, sign + "/cryptoKeyVersions/1:macVerify", map[string]any{"data": b64("x"), "mac": b64("x")}},
		{"public key of mac", http.MethodGet, mac + "/cryptoKeyVersions/1/publicKey", nil},
		{"encrypt with sign key", http.MethodPost, sign + ":encrypt", map[string]any{"plaintext": b64(testMessage)}},
		{"decrypt with mac key", http.MethodPost, mac + ":decrypt", map[string]any{"ciphertext": b64("1|x")}},
	}
	for _, c := range cases {
		h.fails(h.as(testAllowed, c.verb, c.path, c.body), c.step, 400, "FAILED_PRECONDITION")
	}
	h.fails(h.invoke(http.MethodPost, sign+":updatePrimaryVersion", map[string]any{"cryptoKeyVersionId": "1"}), "primary of sign key", 400, "FAILED_PRECONDITION")
}

func TestPurposeKeyCreateAndUpdateRefusals(t *testing.T) {
	h := newHarness(t)
	h.ring()
	create := func(body map[string]any) *httptest.ResponseRecorder {
		return h.invoke(http.MethodPost, testKeysPath+"?cryptoKeyId=refused", body)
	}
	h.fails(create(map[string]any{"purpose": purposeAsymmetricSign}), "no algorithm", 400, "INVALID_ARGUMENT")
	h.fails(create(map[string]any{"purpose": purposeMAC, "versionTemplate": map[string]any{keyAlgorithm: algorithmP256}}), "mismatched algorithm", 400, "INVALID_ARGUMENT")
	h.fails(create(map[string]any{"purpose": purposeAsymmetricSign, "versionTemplate": map[string]any{keyAlgorithm: "EC_SIGN_SECP256K1_SHA256"}}), "unimplemented algorithm", 501, "UNIMPLEMENTED")
	h.fails(create(map[string]any{"purpose": "RAW_ENCRYPT_DECRYPT"}), "unimplemented purpose", 501, "UNIMPLEMENTED")
	h.fails(create(map[string]any{"purpose": purposeAsymmetricSign, "rotationPeriod": testRotation,
		"versionTemplate": map[string]any{keyAlgorithm: algorithmP256}}), "asymmetric rotation", 400, "INVALID_ARGUMENT")
	h.fails(h.invoke(http.MethodGet, testKeysPath+"/refused", nil), "refused key absent", 404, "NOT_FOUND")
	path := h.purposeKey("sign", purposeAsymmetricSign, algorithmP256)
	h.failsWith(h.invoke(http.MethodPatch, path+"?updateMask=rotationPeriod,nextRotationTime", map[string]any{
		"rotationPeriod": testRotation, "nextRotationTime": "2030-01-01T00:00:00Z"}), "patch rotation", 400, "INVALID_ARGUMENT",
		"Automatic rotation (via fields crypto_key.next_rotation_time and crypto_key.rotation_schedule) is not available for keys with purpose ASYMMETRIC_SIGN.")
	h.fails(h.invoke(http.MethodPatch, path+"?updateMask=versionTemplate.algorithm,labels", map[string]any{
		"versionTemplate": map[string]any{keyAlgorithm: algorithmHMAC}, "labels": map[string]string{"a": "b"}}), "patch wrong algorithm", 400, "INVALID_ARGUMENT")
	if key := h.ok(h.invoke(http.MethodGet, path, nil), "unchanged key"); key["labels"] != nil {
		t.Fatalf("refused patch applied labels: %v", key)
	}
	h.ok(h.invoke(http.MethodPatch, path+"?updateMask=versionTemplate.algorithm", map[string]any{
		"versionTemplate": map[string]any{keyAlgorithm: "EC_SIGN_P384_SHA384"}}), "patch algorithm")
	added := h.ok(h.invoke(http.MethodPost, path+"/cryptoKeyVersions", map[string]any{}), "version 2")
	if added[keyAlgorithm] != "EC_SIGN_P384_SHA384" {
		t.Fatalf("version 2 = %v", added)
	}
	if key := h.ok(h.invoke(http.MethodGet, path, nil), "key"); key["primary"] != nil {
		t.Fatalf("sign key gained a primary: %v", key)
	}
}

func TestSignRequestValidation(t *testing.T) {
	h := newHarness(t)
	h.ring()
	path := h.purposeKey("sign", purposeAsymmetricSign, algorithmP256)
	h.grantOn(path, testSignerVerifier)
	sign := path + "/cryptoKeyVersions/1:asymmetricSign"
	digest := base64.StdEncoding.EncodeToString(make([]byte, 32))
	cases := []struct {
		step string
		body map[string]any
	}{
		{"digest and data", map[string]any{"digest": map[string]any{"sha256": digest}, "data": b64(testMessage)}},
		{"neither", map[string]any{}},
		{"two hashes", map[string]any{"digest": map[string]any{"sha256": digest, "sha384": digest}}},
		{"wrong hash", map[string]any{"digest": map[string]any{"sha384": base64.StdEncoding.EncodeToString(make([]byte, 48))}}},
		{"short digest", map[string]any{"digest": map[string]any{"sha256": b64("short")}}},
	}
	for _, c := range cases {
		h.fails(h.as(testAllowed, http.MethodPost, sign, c.body), c.step, 400, "INVALID_ARGUMENT")
	}
	h.fails(h.as(testAllowed, http.MethodGet, path+"/cryptoKeyVersions/1/publicKey?publicKeyFormat=PEM", nil), "public key format", 501, "UNIMPLEMENTED")
	h.fails(h.as(testAllowed, http.MethodPost, path+"/cryptoKeyVersions/9:asymmetricSign", map[string]any{"data": b64(testMessage)}), "missing version", 404, "NOT_FOUND")
}

func TestDisabledAndDestroyedVersionsRefuseUse(t *testing.T) {
	h := newHarness(t)
	h.ring()
	sign := h.purposeKey("sign", purposeAsymmetricSign, algorithmP256)
	mac := h.purposeKey("mac", purposeMAC, algorithmHMAC)
	h.grantOn(sign, testSignerVerifier)
	h.grantOn(mac, testSignerVerifier)
	use := func(step string) {
		h.t.Helper()
		h.fails(h.as(testAllowed, http.MethodPost, sign+"/cryptoKeyVersions/1:asymmetricSign", map[string]any{"data": b64(testMessage)}), step+" sign", 400, "FAILED_PRECONDITION")
		h.fails(h.as(testAllowed, http.MethodGet, sign+"/cryptoKeyVersions/1/publicKey", nil), step+" public key", 400, "FAILED_PRECONDITION")
		h.fails(h.as(testAllowed, http.MethodPost, mac+"/cryptoKeyVersions/1:macSign", map[string]any{"data": b64(testMessage)}), step+" mac", 400, "FAILED_PRECONDITION")
	}
	for _, path := range []string{sign, mac} {
		h.ok(h.invoke(http.MethodPatch, path+"/cryptoKeyVersions/1?updateMask=state", map[string]any{keyState: stateDisabled}), "disable")
	}
	use("disabled")
	for _, path := range []string{sign, mac} {
		h.ok(h.invoke(http.MethodPost, path+"/cryptoKeyVersions/1:destroy", map[string]any{}), "destroy")
	}
	use("destroy scheduled")
	restored := h.ok(h.invoke(http.MethodPost, sign+"/cryptoKeyVersions/1:restore", map[string]any{}), "restore")
	h.fields(restored, "restore", keyAlgorithm, keyCreateTime, "generateTime", keyName, keyProtectionLevel, keyState)
	if restored[keyState] != stateDisabled {
		t.Fatalf("restored = %v", restored)
	}
	h.fails(h.invoke(http.MethodPost, sign+"/cryptoKeyVersions/1:restore", map[string]any{}), "restore disabled", 400, "FAILED_PRECONDITION")
	h.ok(h.invoke(http.MethodPatch, sign+"/cryptoKeyVersions/1?updateMask=state", map[string]any{keyState: stateEnabled}), "enable")
	h.sign(sign, map[string]any{"data": b64(testMessage)}, "restored sign")
	// Completed destruction clears the material and cannot be restored.
	h.provider.clock = func() time.Time { return time.Now().Add(31 * 24 * time.Hour) }
	h.fails(h.invoke(http.MethodPost, mac+"/cryptoKeyVersions/1:restore", map[string]any{}), "restore destroyed", 400, "FAILED_PRECONDITION")
	h.ok(h.invoke(http.MethodPost, sign+"/cryptoKeyVersions/1:destroy", map[string]any{}), "destroy again")
	h.provider.clock = func() time.Time { return time.Now().Add(62 * 24 * time.Hour) }
	use("destroyed")
	_ = h.provider.scopes.With(state.Scope{Project: testProject, Location: state.Global}, func(s *scopeState) error {
		for id, record := range s.cryptoKeys {
			if version := record.Versions["1"]; version.Private != nil || version.Material != nil {
				t.Fatalf("%s kept material after destruction", id)
			}
		}
		return nil
	})
}

func TestKeyUseIsAuthorizedBeforeState(t *testing.T) {
	h := newHarness(t)
	h.ring()
	sign := h.purposeKey("sign", purposeAsymmetricSign, algorithmP256)
	mac := h.purposeKey("mac", purposeMAC, algorithmHMAC)
	signBody := map[string]any{"data": b64(testMessage)}
	h.fails(h.as(testDenied, http.MethodPost, sign+"/cryptoKeyVersions/1:asymmetricSign", signBody), "denied sign", 403, "PERMISSION_DENIED")
	h.fails(h.invoke(http.MethodPost, sign+"/cryptoKeyVersions/1:asymmetricSign", signBody), "admin cannot sign", 403, "PERMISSION_DENIED")
	h.fails(h.invoke(http.MethodGet, sign+"/cryptoKeyVersions/1/publicKey", nil), "admin cannot view public key", 403, "PERMISSION_DENIED")
	h.fails(h.invoke(http.MethodPost, mac+"/cryptoKeyVersions/1:macSign", signBody), "admin cannot mac", 403, "PERMISSION_DENIED")
	h.grantOn(sign, testPublicKeyViewer)
	h.publicKey(sign)
	h.fails(h.as(testAllowed, http.MethodPost, sign+"/cryptoKeyVersions/1:asymmetricSign", signBody), "viewer cannot sign", 403, "PERMISSION_DENIED")
	h.grantOn(mac, testSigner)
	tag := h.ok(h.as(testAllowed, http.MethodPost, mac+"/cryptoKeyVersions/1:macSign", signBody), "signer macs")
	h.fails(h.as(testAllowed, http.MethodPost, mac+"/cryptoKeyVersions/1:macVerify", map[string]any{"data": b64(testMessage), "mac": tag["mac"]}),
		"signer cannot verify", 403, "PERMISSION_DENIED")
	// Denial precedes the version's state and existence.
	h.ok(h.invoke(http.MethodPost, sign+"/cryptoKeyVersions/1:destroy", map[string]any{}), "destroy")
	h.fails(h.as(testDenied, http.MethodPost, sign+"/cryptoKeyVersions/1:asymmetricSign", signBody), "denied destroyed sign", 403, "PERMISSION_DENIED")
	h.fails(h.as(testDenied, http.MethodPost, sign+"/cryptoKeyVersions/7:asymmetricSign", signBody), "denied missing version", 403, "PERMISSION_DENIED")
	h.fails(h.as(testDenied, http.MethodPost, sign+"/cryptoKeyVersions/1:restore", map[string]any{}), "denied restore", 403, "PERMISSION_DENIED")
}

// The REST SDKs send enums as numbers ($alt=json;enum-encoding=int).
func TestNumericEnumsDecode(t *testing.T) {
	h := newHarness(t)
	h.ring()
	created := h.ok(h.invoke(http.MethodPost, testKeysPath+"?cryptoKeyId=sign", map[string]any{
		"purpose": 5, "versionTemplate": map[string]any{keyAlgorithm: 12, keyProtectionLevel: 1}}), "numeric create")
	if created["purpose"] != purposeAsymmetricSign {
		t.Fatalf("created = %v", created)
	}
	disabled := h.ok(h.invoke(http.MethodPatch, testKeysPath+"/sign/cryptoKeyVersions/1?updateMask=state", map[string]any{keyState: 2}), "numeric state")
	if disabled[keyState] != stateDisabled || disabled[keyAlgorithm] != algorithmP256 {
		t.Fatalf("disabled = %v", disabled)
	}
	h.fails(h.invoke(http.MethodPost, testKeysPath+"?cryptoKeyId=raw", map[string]any{"purpose": 7}), "unimplemented numeric purpose", 501, "UNIMPLEMENTED")
	h.fails(h.invoke(http.MethodPost, testKeysPath+"?cryptoKeyId=hsm", map[string]any{
		"purpose": 1, "versionTemplate": map[string]any{keyProtectionLevel: 2}}), "numeric HSM", 501, "UNIMPLEMENTED")
	h.fails(h.invoke(http.MethodPost, testKeysPath+"?cryptoKeyId=bad", map[string]any{"purpose": true}), "non-enum purpose", 400, "INVALID_ARGUMENT")
}
