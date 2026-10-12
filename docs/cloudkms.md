# Cloud KMS

Cloud KMS v1 over REST and gRPC: key rings, crypto keys, versions, symmetric encrypt/decrypt, asymmetric sign and decrypt, and HMAC, with per-resource IAM. Recorded against GCP: symmetric keys (`cloudkms-symmetric-key-lifecycle`) and asymmetric sign and MAC keys (`cloudkms-asymmetric-mac-keys`, 2026-10-12).

## Use

Select `-services ...,kms`. REST routes are `/v1/projects/{project}/locations/{location}/keyRings/...` on port 4566; `google.cloud.kms.v1.KeyManagementService` is on the gRPC port 4567. Key material is generated with `crypto/rand`, lives only in process memory, and does not survive a restart. Only public keys leave the process. This is a local stand-in, not a cryptographic boundary.

The REST SDKs send enums as numbers (`$alt=json;enum-encoding=int`); requests accept names or numbers, and responses return names.

## Transport

The client libraries' default gRPC transport works; REST still works. The gateway transcodes each RPC to its REST route by the method's `google.api.http` binding, so both transports run the same provider code, IAM checks, and errors. Key ring and crypto key IAM policies use `google.iam.v1.IAMPolicy` on the same port.

Point the client at the gRPC endpoint with local channel credentials and local ADC:

```python
channel = secure_authorized_channel(credentials, request, "127.0.0.1:4567",
                                    ssl_credentials=grpc.local_channel_credentials())
client = kms.KeyManagementServiceClient(transport=KeyManagementServiceGrpcTransport(channel=channel))
```

```go
client, err := kms.NewKeyManagementClient(ctx, option.WithEndpoint("127.0.0.1:4567"),
	option.WithGRPCDialOption(grpc.WithTransportCredentials(local.NewCredentials())))
```

- Tested clients: Python `google-cloud-kms` 3.18.0 (REST and gRPC) and Go `cloud.google.com/go/kms` 1.35.0 (gRPC). Node and Java are not tested.
- RPCs without a REST method here (`RawEncrypt`, import jobs, `Decapsulate`, `GenerateRandomBytes`, deletes, retired resources), `Encrypt` with a version name, and `IAMPolicy.TestIamPermissions` return `UNIMPLEMENTED`. The location list is REST only.
- The clients retry only `UNAVAILABLE` and `DEADLINE_EXCEEDED`, within a 60-second deadline; no KMS refusal uses either code.

## Methods

| Resource | Methods | Notes |
| --- | --- | --- |
| Locations | `locations.list` at `GET .../locations/{location}` | Returns `us-central1` only |
| Key rings | `create`, `get`, `list`, `getIamPolicy`, `setIamPolicy` | No delete, as in GCP |
| Crypto keys | `create`, `get`, `list`, `patch`, `getIamPolicy`, `setIamPolicy` | Purposes and algorithms below; `skipInitialVersionCreation`; `patch` takes `labels`, `rotationPeriod`, `nextRotationTime`, `versionTemplate.algorithm` |
| Crypto keys | `encrypt`, `decrypt`, `updatePrimaryVersion` | `ENCRYPT_DECRYPT` only; optional CRC32C checksums verified; wrong AAD is 400 |
| Versions | `create`, `get`, `list`, `patch`, `destroy`, `restore` | `patch` sets `ENABLED`/`DISABLED`; `restore` returns a scheduled version to `DISABLED`; material is removed after `destroyScheduledDuration` (default 30 days) |
| Versions | `asymmetricSign`, `asymmetricDecrypt`, `getPublicKey` | `digest` or `data` (hashed with the algorithm's hash); EC signatures are DER; PSS salt equals the hash length; OAEP uses SHA-256 with no label; PEM with `pemCrc32c` |
| Versions | `macSign`, `macVerify` | A wrong tag is 200 with `success` omitted |

| Purpose | Algorithms |
| --- | --- |
| `ENCRYPT_DECRYPT` | `GOOGLE_SYMMETRIC_ENCRYPTION` |
| `ASYMMETRIC_SIGN` | `EC_SIGN_P256_SHA256`, `EC_SIGN_P384_SHA384`, `RSA_SIGN_PSS_2048_SHA256`, `RSA_SIGN_PKCS1_2048_SHA256` |
| `ASYMMETRIC_DECRYPT` | `RSA_DECRYPT_OAEP_2048_SHA256` |
| `MAC` | `HMAC_SHA256` |

Refusals, modeled locally:

- A key use that the key's purpose does not allow is 400 `FAILED_PRECONDITION`. So is any use of a version that is not `ENABLED`, and `updatePrimaryVersion` on a non-symmetric key.
- Missing `versionTemplate.algorithm` for a non-symmetric purpose, an algorithm of another purpose, rotation on a non-symmetric key, both or neither of `digest` and `data`, a digest of the wrong hash or length, and any CRC32C mismatch are 400 `INVALID_ARGUMENT`.
- Other purposes, algorithms, protection levels, and `publicKeyFormat` are 501 `UNIMPLEMENTED`.

## IAM

| Method | Permission |
| --- | --- |
| `encrypt` | `cloudkms.cryptoKeyVersions.useToEncrypt` |
| `decrypt`, `asymmetricDecrypt` | `cloudkms.cryptoKeyVersions.useToDecrypt` |
| `asymmetricSign`, `macSign` | `cloudkms.cryptoKeyVersions.useToSign` |
| `macVerify` | `cloudkms.cryptoKeyVersions.useToVerify` |
| `getPublicKey` | `cloudkms.cryptoKeyVersions.viewPublicKey` |
| `restore` | `cloudkms.cryptoKeyVersions.restore` |

- IAM is checked on the crypto key before any version state is read, so a denied caller gets 403 even for a disabled or missing version.
- `roles/cloudkms.admin` manages keys but cannot encrypt, decrypt, sign, verify, or read public keys, as in GCP.
- Pinned roles: `roles/cloudkms.admin`, `.cryptoKeyEncrypter`, `.cryptoKeyEncrypterDecrypter`, `.cryptoKeyDecrypter`, `.signer`, `.signerVerifier`, `.publicKeyViewer`, `.viewer`.
- A missing resource is authorized on its nearest existing parent, then returns 404.

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `cloudkms-symmetric-key-lifecycle` | 2026-10-11 | Missing ring 404, ring and key create shapes, 403 without grants and for admin on encrypt, empty and set policies, encrypt/decrypt fields and CRC32C, wrong AAD, version create, primary change, disable, decrypt with a disabled version, destroy |

```sh
sh scripts/check_rest_recordings.sh  # the symmetric recording
sh scripts/check_cloudkms.sh         # google-cloud-kms over REST and gRPC, and the Go client over gRPC, with local ADC
```

`cloudkms-asymmetric-mac-keys` was recorded and revalidated against GCP on 2026-10-12 and matches locally (`sh scripts/check_rest_recordings.sh`): sign by digest and data, public key PEM and CRC32C, HMAC sign and verify, CRC32C and digest refusals, wrong-purpose and disabled-version `FAILED_PRECONDITION`, destroy and restore. `TestAccKMSKeyRingCryptoKey` passes ([Terraform](terraform-compatibility.md)).

## Gaps

- `ASYMMETRIC_DECRYPT` (RSA-OAEP) is modeled locally, not recorded.
- Versions are `ENABLED` at once; GCP reports `PENDING_GENERATION` first for asymmetric keys.
- No HSM, external, raw, PQC, or key-encapsulation algorithms; no RSA 3072/4096, SHA-512, or other HMAC hashes; no import jobs.
- Rotation is stored but not run.
- Pagination, list filters, and key update fields are not recorded.
