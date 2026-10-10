# Cloud KMS (local slice)

Cloud KMS v1 control plane: key rings, symmetric crypto keys, versions, and encrypt/decrypt with per-resource IAM (`cloudkms.*` permission rows; `roles/cloudkms.admin`, `roles/cloudkms.cryptoKeyEncrypterDecrypter`, and `roles/cloudkms.viewer` snapshots pin the baselines).

## Local-modeled surfaces

- Key rings: create (with `keyRingId`), get, list; ring-level IAM get/set.
- Crypto keys: create (`cryptoKeyId`, symmetric `ENCRYPT_DECRYPT` only, initial version 1), get, list, update (labels, rotation, explicit primary move); key-level IAM get/set.
- Versions: create (fresh material), get, list, enable/disable/destroy; a destroyed version is final and clears the primary.
- Encrypt/decrypt: base64 plaintext/ciphertext plus optional additional-authenticated-data binding; encrypt uses the key's primary version and names it; decrypt reports the `usedCiphertextVersionName` from the seal envelope and fails closed if that version is gone or disabled.

The local crypto is provider-held AES-GCM keyed per process run: ciphertext from one process run does not decrypt in another. The real service's HSM-backed key material, envelope encryption, and regional replication are not modeled; this is a local stand-in, not a cryptographic boundary.

## Explicit gaps

- No cloud recording backs these methods; issue #39 tracks the recording and Terraform workflow pin. No supported claim is made.
- Only symmetric `ENCRYPT_DECRYPT` keys with `GOOGLE_SYMMETRIC_ENCRYPTION`; HSM protection levels, asymmetric keys, MAC/signing, and import jobs are unimplemented.
- Automatic rotation is stored (rotationPeriod/nextRotationTime) but not scheduled locally.
- Primary-version state moves only by explicit update.
- Key ring `cryptoKeys.create` requires an existing ring; rings cannot be deleted (matching the real surface's lack of ring delete).
