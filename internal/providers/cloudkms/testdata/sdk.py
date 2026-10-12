"""Drive google-cloud-kms over REST against the local provider with local ADC."""

import hashlib
import os
import sys

import google.auth
import google_crc32c
import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, padding
from google.api_core import exceptions
from google.auth.transport.requests import Request
from google.cloud import kms
from google.iam.v1 import policy_pb2

SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
PROJECT = "project-a"
MESSAGE = b"gcp-local synthetic message"


def crc32c(data: bytes) -> int:
    """Return the CRC32C KMS integrity fields use."""
    return int(google_crc32c.value(data))


def client_for(endpoint: str, filename: str | None = None) -> kms.KeyManagementServiceClient:
    """Build a REST client from local ADC, with no ambient proxy."""
    session = requests.Session()
    session.trust_env = False
    if filename:
        credentials, _ = google.auth.load_credentials_from_file(filename, scopes=SCOPES)
    else:
        credentials, _ = google.auth.default(scopes=SCOPES, request=Request(session=session))
    return kms.KeyManagementServiceClient(
        credentials=credentials, transport="rest", client_options={"api_endpoint": endpoint}
    )


def refused(call, wire_status: str) -> None:
    """Require one call to fail with the given google.rpc status name."""
    try:
        call()
    except exceptions.GoogleAPICallError as error:
        got = error.response.json()["error"]["status"]
        assert got == wire_status, (got, error)
    else:
        raise AssertionError("Expected " + wire_status)


def create_key(admin, ring: str, key_id: str, purpose, algorithm) -> str:
    """Create one key and grant the app identity use of it."""
    key = admin.create_crypto_key(
        parent=ring,
        crypto_key_id=key_id,
        crypto_key={"purpose": purpose, "version_template": {"algorithm": algorithm}},
    )
    assert not key.primary.name, key
    policy = admin.get_iam_policy(request={"resource": key.name})
    for role in ("roles/cloudkms.signerVerifier", "roles/cloudkms.cryptoKeyDecrypter"):
        policy.bindings.append(policy_pb2.Binding(role=role, members=[os.environ["GCP_LOCAL_APP"]]))
    admin.set_iam_policy(request={"resource": key.name, "policy": policy})
    return key.name + "/cryptoKeyVersions/1"


def public_key(app, version: str):
    """Read one public key and check its PEM checksum."""
    response = app.get_public_key(request={"name": version})
    assert response.pem_crc32c == crc32c(response.pem.encode()), response
    return serialization.load_pem_public_key(response.pem.encode())


def check_sign(admin, app, denied, ring: str) -> None:
    """Sign a digest, verify with the public key, and refuse an ungranted caller."""
    version = create_key(
        admin,
        ring,
        "sign",
        kms.CryptoKey.CryptoKeyPurpose.ASYMMETRIC_SIGN,
        kms.CryptoKeyVersion.CryptoKeyVersionAlgorithm.EC_SIGN_P256_SHA256,
    )
    digest = hashlib.sha256(MESSAGE).digest()
    signed = app.asymmetric_sign(
        request={"name": version, "digest": {"sha256": digest}, "digest_crc32c": crc32c(digest)}
    )
    assert signed.verified_digest_crc32c and signed.name == version, signed
    assert signed.signature_crc32c == crc32c(signed.signature), signed
    public_key(app, version).verify(signed.signature, MESSAGE, ec.ECDSA(hashes.SHA256()))
    refused(
        lambda: app.asymmetric_sign(
            request={"name": version, "digest": {"sha256": digest}, "digest_crc32c": 1}
        ),
        "INVALID_ARGUMENT",
    )
    refused(
        lambda: denied.asymmetric_sign(request={"name": version, "digest": {"sha256": digest}}),
        "PERMISSION_DENIED",
    )
    refused(
        lambda: app.asymmetric_decrypt(request={"name": version, "ciphertext": b"x"}),
        "FAILED_PRECONDITION",
    )


def check_decrypt(admin, app, ring: str) -> None:
    """Decrypt RSA-OAEP ciphertext produced with the public key."""
    version = create_key(
        admin,
        ring,
        "decrypt",
        kms.CryptoKey.CryptoKeyPurpose.ASYMMETRIC_DECRYPT,
        kms.CryptoKeyVersion.CryptoKeyVersionAlgorithm.RSA_DECRYPT_OAEP_2048_SHA256,
    )
    oaep = padding.OAEP(mgf=padding.MGF1(hashes.SHA256()), algorithm=hashes.SHA256(), label=None)
    ciphertext = public_key(app, version).encrypt(MESSAGE, oaep)
    opened = app.asymmetric_decrypt(
        request={"name": version, "ciphertext": ciphertext, "ciphertext_crc32c": crc32c(ciphertext)}
    )
    assert opened.plaintext == MESSAGE and opened.verified_ciphertext_crc32c, opened
    assert opened.plaintext_crc32c == crc32c(MESSAGE), opened


def check_mac(admin, app, ring: str) -> None:
    """Sign and verify an HMAC, then refuse a disabled version."""
    version = create_key(
        admin,
        ring,
        "mac",
        kms.CryptoKey.CryptoKeyPurpose.MAC,
        kms.CryptoKeyVersion.CryptoKeyVersionAlgorithm.HMAC_SHA256,
    )
    signed = app.mac_sign(
        request={"name": version, "data": MESSAGE, "data_crc32c": crc32c(MESSAGE)}
    )
    assert signed.verified_data_crc32c and signed.mac_crc32c == crc32c(signed.mac), signed
    verified = app.mac_verify(request={"name": version, "data": MESSAGE, "mac": signed.mac})
    assert verified.success and verified.verified_success_integrity, verified
    tampered = app.mac_verify(request={"name": version, "data": b"other", "mac": signed.mac})
    assert not tampered.success and not tampered.verified_success_integrity, tampered
    admin.update_crypto_key_version(
        request={
            "crypto_key_version": {
                "name": version,
                "state": kms.CryptoKeyVersion.CryptoKeyVersionState.DISABLED,
            },
            "update_mask": {"paths": ["state"]},
        }
    )
    refused(lambda: app.mac_sign(request={"name": version, "data": MESSAGE}), "FAILED_PRECONDITION")
    destroyed = admin.destroy_crypto_key_version(request={"name": version})
    assert destroyed.state == kms.CryptoKeyVersion.CryptoKeyVersionState.DESTROY_SCHEDULED, (
        destroyed
    )
    restored = admin.restore_crypto_key_version(request={"name": version})
    assert restored.state == kms.CryptoKeyVersion.CryptoKeyVersionState.DISABLED, restored


def main(endpoint: str) -> None:
    """Run the asymmetric and MAC flows as the admin, app, and denied identities."""
    admin = client_for(endpoint)
    app = client_for(endpoint, os.environ["GCP_LOCAL_APP_ADC"])
    denied = client_for(endpoint, os.environ["GCP_LOCAL_DENIED_ADC"])
    ring = admin.create_key_ring(
        parent=f"projects/{PROJECT}/locations/us-central1", key_ring_id="sdk", key_ring={}
    ).name
    check_sign(admin, app, denied, ring)
    check_decrypt(admin, app, ring)
    check_mac(admin, app, ring)
    print("google-cloud-kms asymmetric and MAC flows passed")


if __name__ == "__main__":
    main(sys.argv[1])
