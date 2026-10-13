# Secret Manager

Secret Manager v1 over REST and gRPC: secrets, versions, payload access, per-secret IAM, and location listing (REST only). Modeled locally in provider state. The scenario `secretmanager-access-lifecycle` is recorded against GCP over REST; gRPC is not recorded.

## Use

Select `-services ...,secretmanager`. REST routes are `/v1/projects/{project}/secrets/...` on port 4566. gRPC serves `google.cloud.secretmanager.v1.SecretManagerService` on port 4567, the transport the default client libraries use. Names use the project number (`projects/NUMBER/secrets/...`); callers may send either form.

Each gRPC method runs the REST method its `google.api.http` binding names, with the same IAM checks and errors. Connect with local ADC on a loopback channel, as for [Pub/Sub](../messaging/pubsub.md#use):

```python
from google.cloud import secretmanager
from google.cloud.secretmanager_v1.services.secret_manager_service.transports import (
    SecretManagerServiceGrpcTransport,
)

# channel = secure_authorized_channel(credentials, request, "127.0.0.1:4567",
#                                     ssl_credentials=grpc.local_channel_credentials())
client = secretmanager.SecretManagerServiceClient(
    transport=SecretManagerServiceGrpcTransport(channel=channel)
)
```

[`sdk.py`](../../../internal/providers/security/secretmanager/testdata/sdk.py) is the tested Python example; [`client_test.go`](../../../internal/providers/security/secretmanager/client_test.go) drives the Go client with `option.WithGRPCConn`.

## Methods

| Resource | Methods | Notes |
| --- | --- | --- |
| Secrets | `create`, `get`, `list`, `patch`, `delete` | Automatic replication; labels and annotations; `patch` requires `updateMask` and checks `etag`; `delete` checks an `etag` query |
| Secrets | `getIamPolicy`, `setIamPolicy` | Conditional bindings with policy version 3 |
| Versions | `addVersion`, `list`, `get` | Payloads up to 64 KiB; a client `dataCrc32c` is verified and reported as `clientSpecifiedPayloadChecksum`; metadata reads never return the payload |
| Versions | `access` | Returns `dataCrc32c`; `latest` is the newest enabled version |
| Versions | `disable`, `enable`, `destroy` | Check the body `etag`; access to a disabled or destroyed version is 400 `FAILED_PRECONDITION` |
| Locations | `list` | REST only |

Lists page with `pageSize` (up to 25000; a negative size reads as unset) and `pageToken`, and report `totalSize`. Secrets list by name, versions newest first. A `filter` is refused with `UNIMPLEMENTED`.

Bodies are decoded strictly: an unknown or mistyped field is GCP's 400 with a `BadRequest` violation, worded as GCP does.

gRPC `TestIamPermissions`, `RotateSecret`, `EnableManagedRotation`, and every regional name (`projects/*/locations/*/secrets/...`) return `UNIMPLEMENTED`. The client libraries do not retry that code. An anonymous gRPC call gets `UNAUTHENTICATED`; REST returns 401.

## IAM

- `secretmanager.versions.access` on the secret reads payloads (`roles/secretmanager.secretAccessor`).
- An accessor cannot read version metadata or add versions.
- A caller without permission gets 403 whether or not the secret or version exists; only a permitted caller gets 404. A missing secret is checked on its project.
- A 403 reads `Permission 'PERMISSION' denied on resource (or it may not exist).` with an `ErrorInfo` (`IAM_PERMISSION_DENIED`, domain `iam.googleapis.com`, metadata `permission`). GCP appends a per-call Troubleshooter link; gcp-local doesn't.
- IAM Conditions see `resource.type` (`secretmanager.googleapis.com/Secret` or `/SecretVersion`), `resource.service` (`secretmanager.googleapis.com`), and `resource.name` with the project number. Version methods (`get`, `access`, `disable`, `enable`, `destroy`) check the version as requested, so a condition on `versions/2` doesn't grant `versions/latest`; `addVersion` and version `list` check the secret.
- Pinned roles: `roles/secretmanager.admin`, `.secretAccessor`, `.viewer`.

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `secretmanager-access-lifecycle` | 2026-10-13 | Create and addVersion fields, number-form names, denied then granted access with denial messages and details, a denied caller's 403 on a missing version, `latest`, 64 KiB limit, disabled and destroyed access, double destroy; masked updates of labels and annotations, a missing mask, a stale etag, a replication change, an invalid annotation key; checksum mismatch and match; a stale version etag; unknown and mistyped body fields; conditional grants by resource type, service, and name on a second secret; version and secret order, paging, `totalSize`, a foreign page token, a negative page size; delete then 404, and a denied caller's 403 on the deleted secret |

```sh
sh scripts/check/recordings.sh
```

To re-record, get an approved scope (see `LiveScope` in `gcp_validation/services/security/secretmanager.py`); the fixture's standing `roles/secretmanager.admin` covers the run. It creates two secrets and six versions and deletes both secrets:

```sh
.venv-tools/bin/python -m pytest tests/conformance/security/secretmanager -q \
  --gcp-mode record --gcp-profile /absolute/path/to/.env \
  --gcp-secretmanager-scope /absolute/path/to/.gcp-local/secretmanager-scope.json \
  --gcp-allow-live --gcp-artifacts .artifacts/secretmanager-record
```

After an interrupted run, `python -m gcp_validation.services.security.secretmanager cleanup --profile ... --scope ... --manifest ... --allow-live` deletes only the secrets in the manifest.

## Gaps

- Not modeled, refused with `UNIMPLEMENTED` naming the field: user-managed replication, CMEK (`customerManagedEncryption`), `rotation`, `expireTime`, `ttl`, `topics`, `versionAliases`, and `versionDestroyTtl` ([#93](https://github.com/evanmarshall/gcp-local/issues/93)). Clearing one of them in a `patch` is a no-op. Resource Manager `tags` are an unknown field.
- No regional secrets: `projects/*/locations/*/secrets` routes are `UNIMPLEMENTED`.
- A `getIamPolicy` `requestedPolicyVersion` below 3 still returns conditional bindings.
- Locations, policy reads, and all of gRPC are not recorded. Annotation total size (16 KiB) is not checked. State is in memory.
