package cloudkms

import (
	"crypto"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	// The hash registrations back crypto.Hash.New for SHA-256 and SHA-384.
	_ "crypto/sha256"
	_ "crypto/sha512"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"slices"

	"cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// digestFields names the Digest message field of each implemented hash.
var digestFields = map[crypto.Hash]string{crypto.SHA256: "sha256", crypto.SHA384: "sha384"}

// versionKey is a copy of one enabled version's algorithm and key material.
type versionKey struct {
	name      string
	algorithm string
	spec      algorithmSpec
	material  []byte
	private   crypto.Signer
}

// enabledVersion copies the key material of the addressed version. The
// version must exist, belong to a key with one of the allowed purposes, and
// be ENABLED.
func (p *Provider) enabledVersion(scope state.Scope, resource ref, allowed ...string) (versionKey, error) {
	var key versionKey
	err := p.withKey(scope, resource, func(record *cryptoKeyRecord) error {
		version := record.Versions[resource.version]
		name := resource.versionName(resource.version)
		if version == nil {
			return notFound("CryptoKeyVersion", name)
		}
		if !slices.Contains(allowed, record.Purpose) {
			return wrongPurpose(name, record.Purpose)
		}
		if version.State != stateEnabled {
			return status.Errorf(codes.FailedPrecondition, "%s is not enabled, current state is: %s.", name, version.State)
		}
		key = versionKey{
			name: name, algorithm: version.Algorithm, spec: algorithms[version.Algorithm],
			material: append([]byte(nil), version.Material...), private: version.Private,
		}
		return nil
	})
	if err != nil {
		return key, fmt.Errorf("use crypto key version %s: %w", resource.version, err)
	}
	return key, nil
}

// signBody is one decoded asymmetricSign request.
type signBody struct {
	Digest    map[string]string `json:"digest"`
	DigestCRC json.RawMessage   `json:"digestCrc32c"`
	Data      string            `json:"data"`
	DataCRC   json.RawMessage   `json:"dataCrc32c"`
}

// asymmetricSign signs a digest, or data hashed here with the algorithm's
// hash, with an ASYMMETRIC_SIGN version. EC signatures are ASN.1 DER and PSS
// uses a salt as long as the hash, as GCP documents.
func (p *Provider) asymmetricSign(request *gateway.RESTRequest, scope state.Scope, resource ref) (*gateway.RESTResponse, error) {
	var body signBody
	if err := decode(request.JSON, "", &kmspb.AsymmetricSignRequest{}, &body); err != nil {
		return nil, err
	}
	input, err := readSignInput(body)
	if err != nil {
		return nil, err
	}
	document := map[string]any{keyProtectionLevel: protectionSoftware}
	if err := verifyChecksums(document, crcCheck{input.digest, body.DigestCRC, "verifiedDigestCrc32c"},
		crcCheck{input.data, body.DataCRC, "verifiedDataCrc32c"}); err != nil {
		return nil, err
	}
	key, err := p.enabledVersion(scope, resource, purposeAsymmetricSign)
	if err != nil {
		return nil, err
	}
	digest, err := input.digestFor(key)
	if err != nil {
		return nil, err
	}
	var options crypto.SignerOpts = key.spec.hash
	if key.spec.pss {
		options = &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash, Hash: key.spec.hash}
	}
	signature, err := key.private.Sign(rand.Reader, digest, options)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Local signing failed: %v", err)
	}
	document[keyName] = key.name
	document["signature"] = base64.StdEncoding.EncodeToString(signature)
	document["signatureCrc32c"] = checksum(signature)
	return respond(document)
}

// signInput is the decoded payload of one asymmetricSign request: a digest
// named by its Digest field, or data.
type signInput struct {
	field        string
	digest, data []byte
}

// readSignInput requires exactly one of digest (with one hash) or data.
func readSignInput(body signBody) (signInput, error) {
	var input signInput
	if (len(body.Digest) == 0) == (body.Data == "") || len(body.Digest) > 1 {
		return input, status.Error(codes.InvalidArgument, "Exactly one of digest or data must be set, and digest must set one hash.")
	}
	if body.Data != "" {
		data, err := decodePayload(body.Data, "data", true)
		input.data = data
		return input, err
	}
	for name, text := range body.Digest {
		digest, err := decodePayload(text, "digest."+name, true)
		input.field, input.digest = name, digest
		if err != nil {
			return input, err
		}
	}
	return input, nil
}

// digestFor hashes data with the algorithm's hash, or checks that the
// supplied digest has the algorithm's hash type and length.
func (input signInput) digestFor(key versionKey) ([]byte, error) {
	if input.data != nil {
		hash := key.spec.hash.New()
		hash.Write(input.data)
		return hash.Sum(nil), nil
	}
	if input.field != digestFields[key.spec.hash] || len(input.digest) != key.spec.hash.Size() {
		return nil, status.Errorf(codes.InvalidArgument, "The digest type does not match algorithm %s.", key.algorithm)
	}
	return input.digest, nil
}

// asymmetricDecrypt opens RSA-OAEP ciphertext made with the version's public key.
func (p *Provider) asymmetricDecrypt(request *gateway.RESTRequest, scope state.Scope, resource ref) (*gateway.RESTResponse, error) {
	var body struct {
		Ciphertext    string          `json:"ciphertext"`
		CiphertextCRC json.RawMessage `json:"ciphertextCrc32c"`
	}
	if err := decode(request.JSON, "", &kmspb.AsymmetricDecryptRequest{}, &body); err != nil {
		return nil, err
	}
	ciphertext, err := decodePayload(body.Ciphertext, "ciphertext", true)
	if err != nil {
		return nil, err
	}
	document := map[string]any{keyProtectionLevel: protectionSoftware}
	if err := verifyChecksums(document, crcCheck{ciphertext, body.CiphertextCRC, "verifiedCiphertextCrc32c"}); err != nil {
		return nil, err
	}
	key, err := p.enabledVersion(scope, resource, purposeAsymmetricDecrypt)
	if err != nil {
		return nil, err
	}
	private, _ := key.private.(*rsa.PrivateKey)
	plaintext, err := rsa.DecryptOAEP(key.spec.hash.New(), rand.Reader, private, ciphertext, nil)
	if err != nil {
		return nil, errInvalidCiphertext
	}
	document["plaintext"] = base64.StdEncoding.EncodeToString(plaintext)
	document["plaintextCrc32c"] = checksum(plaintext)
	return respond(document)
}

// getPublicKey returns one asymmetric version's PEM public key.
func (p *Provider) getPublicKey(request *gateway.RESTRequest, scope state.Scope, resource ref) (*gateway.RESTResponse, error) {
	if request.Query.Get("publicKeyFormat") != "" {
		return nil, gap("publicKeyFormat")
	}
	key, err := p.enabledVersion(scope, resource, purposeAsymmetricSign, purposeAsymmetricDecrypt)
	if err != nil {
		return nil, err
	}
	der, err := x509.MarshalPKIXPublicKey(key.private.Public())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Public key encoding failed: %v", err)
	}
	text := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
	return respond(map[string]any{
		"pem": string(text), keyAlgorithm: key.algorithm, "pemCrc32c": checksum(text),
		keyName: key.name, keyProtectionLevel: protectionSoftware,
	})
}

// macBody is one decoded macSign or macVerify request.
type macBody struct {
	Data    string          `json:"data"`
	DataCRC json.RawMessage `json:"dataCrc32c"`
	Mac     string          `json:"mac"`
	MacCRC  json.RawMessage `json:"macCrc32c"`
}

// macTag reads the request data, and the caller's tag when verifying, and
// computes the data's tag with a MAC version.
func (p *Provider) macTag(request *gateway.RESTRequest, scope state.Scope, resource ref, document map[string]any, verifying bool) ([]byte, []byte, error) {
	var body macBody
	var message proto.Message = &kmspb.MacSignRequest{}
	if verifying {
		message = &kmspb.MacVerifyRequest{}
	}
	if err := decode(request.JSON, "", message, &body); err != nil {
		return nil, nil, err
	}
	data, err := decodePayload(body.Data, "data", true)
	if err != nil {
		return nil, nil, err
	}
	mac, err := decodePayload(body.Mac, "mac", verifying)
	if err != nil {
		return nil, nil, err
	}
	if err := verifyChecksums(document, crcCheck{data, body.DataCRC, "verifiedDataCrc32c"},
		crcCheck{mac, body.MacCRC, "verifiedMacCrc32c"}); err != nil {
		return nil, nil, err
	}
	key, err := p.enabledVersion(scope, resource, purposeMAC)
	if err != nil {
		return nil, nil, err
	}
	tag := hmac.New(key.spec.hash.New, key.material)
	tag.Write(data)
	document[keyName] = key.name
	return mac, tag.Sum(nil), nil
}

// macSign returns the HMAC tag of one payload.
func (p *Provider) macSign(request *gateway.RESTRequest, scope state.Scope, resource ref) (*gateway.RESTResponse, error) {
	document := map[string]any{keyProtectionLevel: protectionSoftware}
	_, tag, err := p.macTag(request, scope, resource, document, false)
	if err != nil {
		return nil, err
	}
	document["mac"] = base64.StdEncoding.EncodeToString(tag)
	document["macCrc32c"] = checksum(tag)
	return respond(document)
}

// macVerify compares one tag with the payload's HMAC. A mismatch is a 200
// with success and verifiedSuccessIntegrity omitted, the proto3 JSON form of
// false.
func (p *Provider) macVerify(request *gateway.RESTRequest, scope state.Scope, resource ref) (*gateway.RESTResponse, error) {
	document := map[string]any{keyProtectionLevel: protectionSoftware}
	given, tag, err := p.macTag(request, scope, resource, document, true)
	if err != nil {
		return nil, err
	}
	if hmac.Equal(given, tag) {
		document["success"] = true
		document["verifiedSuccessIntegrity"] = true
	}
	return respond(document)
}
