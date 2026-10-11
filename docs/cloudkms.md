# Cloud KMS (local slice)

Cloud KMS v1 control plane: key rings, symmetric crypto keys, versions, and encrypt/decrypt with per-resource IAM (`cloudkms.*` permission rows; recorded `roles/cloudkms.*` snapshots pin the baselines; as in GCP, `roles/cloudkms.admin` manages keys but cannot encrypt or decrypt).

## Recorded scenario

`cloudkms-symmetric-key-lifecycle` (`gcp_validation/cloudkms.py`, snapshot under `tests/conformance/recordings/`) was recorded against real GCP. The provider was changed to match it; unit tests in `internal/providers/cloudkms` replay each recorded step's selected fields and errors. The recording covers:

- Reading a missing key ring (404 `NOT_FOUND`), then creating it with `keyRingId` and an empty body.
- Key create shape: `createTime`, `destroyScheduledDuration`, `labels`, `name`, `primary`, `purpose`, `versionTemplate`; the primary version has `algorithm`, `createTime`, `generateTime`, `name`, `protectionLevel`, `state`.
- 403 for a caller without grants and for `roles/cloudkms.admin` on `:encrypt`; an empty key policy reads back as only `etag`; `:setIamPolicy` returns `bindings`, `etag`, `version`.
- `:encrypt` returns `ciphertext`, `ciphertextCrc32c`, `name`, `protectionLevel`; `verified*Crc32c` appear only for checksums the request sent. `:decrypt` returns `plaintext`, `plaintextCrc32c`, `protectionLevel`, and `usedPrimary` only when the primary version decrypted. Wrong AAD is 400 `INVALID_ARGUMENT`.
- Version create, `:updatePrimaryVersion`, `PATCH .../cryptoKeyVersions/N?updateMask=state` for ENABLED/DISABLED, 400 `FAILED_PRECONDITION` when decrypting with a disabled version, and `:destroy` to `DESTROY_SCHEDULED` with `destroyTime`.

## Local-modeled surfaces

- Key rings: create, get, list; ring-level IAM get/set. Rings and keys are keyed by location.
- Crypto keys: create (`cryptoKeyId`, optional `skipInitialVersionCreation` query, symmetric `ENCRYPT_DECRYPT` only), get, list, `PATCH` with `updateMask` (`labels`, `rotationPeriod`, `nextRotationTime`, `versionTemplate.algorithm`), `:updatePrimaryVersion` (permission `cloudkms.cryptoKeys.update`); key-level IAM get/set.
- Versions: create, get, list, `PATCH` state changes (permission `cloudkms.cryptoKeyVersions.update`), `:destroy`. A scheduled version becomes `DESTROYED` after the key's `destroyScheduledDuration` (default 30 days) and loses its material.
- A request for a resource that does not exist is authorized on its nearest existing ancestor, then returns `NOT_FOUND`.
- Optional CRC32C request checksums are verified; a mismatch is `INVALID_ARGUMENT`.

Each version has its own AES-256-GCM material held in memory. Ciphertext names its version, is bound to the caller's AAD, and does not decrypt under another key. It does not survive a restart. HSM-backed material, envelope formats, and regional replication are not modeled; this is a local stand-in, not a cryptographic boundary.

## Explicit gaps

- Only symmetric `ENCRYPT_DECRYPT` keys with `GOOGLE_SYMMETRIC_ENCRYPTION` and `SOFTWARE` protection; HSM/external protection, asymmetric keys, MAC/signing, import jobs, and `:restore` are unimplemented.
- Automatic rotation is stored but not scheduled locally.
- The `cloudkms.locations.list` route is served at the single-location path and returns only `us-central1`.
- Behavior beyond the recorded steps (pagination, list filters, key update fields, CRC handling) is local modeling without cloud evidence.
- Rings and keys cannot be deleted, matching the real surface.
