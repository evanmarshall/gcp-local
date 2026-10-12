# Cloud KMS

Cloud KMS v1 over REST: key rings, symmetric crypto keys, versions, and encrypt/decrypt, with per-resource IAM. Modeled locally; each version holds its own in-memory AES-256-GCM key. The scenario `cloudkms-symmetric-key-lifecycle` is recorded against GCP.

## Use

Select `-services ...,kms`. Routes are `/v1/projects/{project}/locations/{location}/keyRings/...` on port 4566. Ciphertext is bound to its version and AAD and does not survive a restart. This is a local stand-in, not a cryptographic boundary.

## Methods

| Resource | Methods | Notes |
| --- | --- | --- |
| Locations | `locations.list` at `GET .../locations/{location}` | Returns `us-central1` only |
| Key rings | `create`, `get`, `list`, `getIamPolicy`, `setIamPolicy` | No delete, as in GCP |
| Crypto keys | `create`, `get`, `list`, `patch`, `getIamPolicy`, `setIamPolicy` | `ENCRYPT_DECRYPT` only; `skipInitialVersionCreation`; `patch` takes `labels`, `rotationPeriod`, `nextRotationTime`, `versionTemplate.algorithm` |
| Crypto keys | `encrypt`, `decrypt`, `updatePrimaryVersion` | Optional CRC32C checksums verified; wrong AAD is 400 |
| Versions | `create`, `get`, `list`, `patch`, `destroy` | `patch` sets `ENABLED`/`DISABLED`; destroyed material is removed after `destroyScheduledDuration` (default 30 days) |

## IAM

- `cloudkms.cryptoKeyVersions.useToEncrypt` and `useToDecrypt` gate encrypt and decrypt. `roles/cloudkms.admin` manages keys but cannot encrypt or decrypt, as in GCP.
- Pinned roles: `roles/cloudkms.admin`, `.cryptoKeyEncrypter`, `.cryptoKeyEncrypterDecrypter`, `.viewer`.
- A missing resource is authorized on its nearest existing parent, then returns 404.

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `cloudkms-symmetric-key-lifecycle` | 2026-10-11 | Missing ring 404, ring and key create shapes, 403 without grants and for admin on encrypt, empty and set policies, encrypt/decrypt fields and CRC32C, wrong AAD, version create, primary change, disable, decrypt with a disabled version, destroy |

```sh
sh scripts/check_rest_recordings.sh
```

`TestAccKMSKeyRingCryptoKey` passes ([Terraform](terraform-compatibility.md)).

## Gaps

- Only `GOOGLE_SYMMETRIC_ENCRYPTION` with `SOFTWARE` protection. No HSM, external, asymmetric, MAC, signing, import jobs, or `restore`.
- Rotation is stored but not run.
- Pagination, list filters, and key update fields are not recorded.
