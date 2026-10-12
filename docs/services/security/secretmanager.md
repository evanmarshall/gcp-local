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
| Secrets | `create`, `get`, `list`, `patch`, `delete` | Automatic replication; `list` returns one page; a `filter` is refused with `UNIMPLEMENTED` |
| Secrets | `getIamPolicy`, `setIamPolicy` | |
| Versions | `addVersion`, `list`, `get` | Payloads up to 64 KiB; metadata reads never return the payload; a `filter` is refused |
| Versions | `access` | Returns `dataCrc32c`; `latest` is the newest enabled version |
| Versions | `disable`, `enable`, `destroy` | Access to a disabled or destroyed version is 400 `FAILED_PRECONDITION` |
| Locations | `list` | REST only |

gRPC `TestIamPermissions`, `RotateSecret`, `EnableManagedRotation`, and every regional name (`projects/*/locations/*/secrets/...`) return `UNIMPLEMENTED`. The client libraries do not retry that code. An anonymous gRPC call gets `UNAUTHENTICATED`; REST returns 401.

## IAM

- `secretmanager.versions.access` on the secret reads payloads (`roles/secretmanager.secretAccessor`).
- An accessor cannot read version metadata or add versions.
- Pinned roles: `roles/secretmanager.admin`, `.secretAccessor`, `.viewer`.

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `secretmanager-access-lifecycle` | 2026-10-11 | Create and addVersion fields, number-form names, denied then granted access, `latest`, 64 KiB limit, disabled and destroyed access, double destroy, delete then 404 |

```sh
sh scripts/check/recordings.sh
```

To re-record, get an approved `.gcp-local/secretmanager-scope.json` (see `LiveScope` in `gcp_validation/services/security/secretmanager.py`) and a temporary `roles/secretmanager.admin` on the fixture identity:

```sh
.venv-tools/bin/python -m pytest tests/conformance/security/secretmanager -q \
  --gcp-mode record --gcp-profile /absolute/path/to/.env \
  --gcp-secretmanager-scope /absolute/path/to/.gcp-local/secretmanager-scope.json \
  --gcp-allow-live --gcp-artifacts .artifacts/secretmanager-record
```

After an interrupted run, `python -m gcp_validation.services.security.secretmanager cleanup --profile ... --scope ... --manifest ... --allow-live` deletes only the secret in the manifest.

## Gaps

- `list`, `update`, `enable`, IAM policy reads, locations, and all of gRPC are not recorded.
- No user-managed replication, CMEK, rotation, expiration, topics, or version aliases. Those secret fields are accepted and ignored, as are `pageSize`, `updateMask`, version `etag`, and a client `dataCrc32c`.
- No regional secrets.
- Error messages are not compared. State is in memory.
