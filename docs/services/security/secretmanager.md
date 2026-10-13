# Secret Manager

Secret Manager v1 over REST and gRPC: secrets, versions, payload access, per-secret IAM, location listing (REST only), and the secret lifecycle policies: user-managed replication, rotation with Pub/Sub notifications, expiration, version aliases, and delayed destruction. Modeled locally in provider state. Three scenarios are recorded against GCP over REST (`secretmanager-access-lifecycle`, `secretmanager-secret-policies`, `secretmanager-rotation-events`); gRPC is not recorded.

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
| Secrets | `create`, `get`, `list`, `patch`, `delete` | Automatic or user-managed replication; labels, annotations, and the [lifecycle policies](#lifecycle); `patch` requires `updateMask` and checks `etag`; `delete` checks an `etag` query |
| Secrets | `getIamPolicy`, `setIamPolicy` | Conditional bindings with policy version 3 |
| Versions | `addVersion`, `list`, `get` | Payloads up to 64 KiB; a client `dataCrc32c` is verified and reported as `clientSpecifiedPayloadChecksum`; metadata reads never return the payload |
| Versions | `access` | Returns `dataCrc32c`; `latest` is the newest enabled version; `get` and `access` also take a version alias |
| Versions | `disable`, `enable`, `destroy` | Check the body `etag`; access to a disabled or destroyed version is 400 `FAILED_PRECONDITION`; with `versionDestroyTtl`, `destroy` schedules the destruction |
| Locations | `list` | REST only |

Lists page with `pageSize` (up to 25000; a negative size reads as unset) and `pageToken`, and report `totalSize`. Secrets list by name, versions newest first. A `filter` is refused with `UNIMPLEMENTED`.

Bodies are decoded strictly: an unknown or mistyped field is GCP's 400 with a `BadRequest` violation, worded as GCP does.

gRPC `TestIamPermissions`, `RotateSecret`, `EnableManagedRotation`, and every regional name (`projects/*/locations/*/secrets/...`) return `UNIMPLEMENTED`. The client libraries do not retry that code. An anonymous gRPC call gets `UNAUTHENTICATED`; REST returns 401.

## Lifecycle

Each policy is a `Secret` field, set on `create` or by `patch` with its mask path (camelCase or snake_case). Time-driven actions run in one worker per secret on the process clock. Refusals below are recorded unless marked modeled.

| Field | Behavior |
| --- | --- |
| `replication.userManaged.replicas[].location` | Stored and reported in request order, on the secret and in each version's `replicationStatus.userManaged.replicas`. At least one replica. Replication is immutable: a masked change is 400, and a replica count change names both counts |
| `topics[].name` | Up to 10 `projects/*/topics/*`. Each must exist, and the project's Secret Manager service agent (`service-NUMBER@gcp-sa-secretmanager.iam.gserviceaccount.com`) must hold `pubsub.topics.publish` on it (`roles/pubsub.publisher`), A missing topic is 404 `Topic [NAME] not found.`; a topic the agent can't publish to is 400 `FAILED_PRECONDITION` naming the agent |
| `rotation.nextRotationTime`, `rotation.rotationPeriod` | Needs a topic. The next time is at least 5 minutes ahead; the period is at least 1 hour and needs a next time (400 `FAILED_PRECONDITION`). The 100-year maximums are modeled. At the next time the service agent publishes `SECRET_ROTATE` with the secret after the time advances by the period; without a period the schedule ends, as recorded. Nothing changes the secret's value, as in GCP |
| `expireTime`, `ttl` | At least 1 minute ahead. `ttl` is input only and reads back as `expireTime`. At that time the secret, its versions, and its policy are deleted, and `SECRET_DELETE` is published with `deleteType` `EXPIRATION` |
| `versionAliases` | Up to 50 names (`[a-zA-Z][a-zA-Z0-9_-]{0,62}`, not `latest` or `NEW`), each naming an existing version; reads back as strings (`{"current": "1"}`). `versions.get` and `versions.access` resolve an alias; `disable`, `enable`, and `destroy` refuse any non-numeric id but `latest` as a malformed version id |
| `versionDestroyTtl` | At least 1 day (the 1000-day maximum is modeled). `destroy` disables the version and sets `scheduledDestroyTime`; a second `destroy` is 400 `FAILED_PRECONDITION`; `enable` cancels; at the scheduled time the version is destroyed |

With `topics` set, every change publishes a notification as the service agent through the Pub/Sub provider (select `pubsub`). Each newly configured topic first gets `TOPIC_CONFIGURED`, with no data and only an `eventType` attribute. Then: `SECRET_CREATE`, `SECRET_UPDATE`, `SECRET_DELETE` (`deleteType` `REQUESTED` or `EXPIRATION`), `SECRET_ROTATE`, `SECRET_VERSION_ADD`, `SECRET_VERSION_ENABLE`, `SECRET_VERSION_DISABLE`, `SECRET_VERSION_DESTROY`, and `SECRET_VERSION_DESTROY_SCHEDULED`. Attributes are `eventType`, `dataFormat` (`JSON_API_V1`), `secretId`, `timestamp`, and `versionId` for version events, with names in the project-number form; `data` is the secret or version as the API returns it. Delivery is best effort: a publish the agent is denied, or a deleted topic, drops the notification and changes nothing else.

## IAM

- `secretmanager.versions.access` on the secret reads payloads (`roles/secretmanager.secretAccessor`).
- An accessor cannot read version metadata or add versions.
- A caller without permission gets 403 whether or not the secret or version exists; only a permitted caller gets 404. A missing secret is checked on its project.
- A 403 reads `Permission 'PERMISSION' denied on resource (or it may not exist).` with an `ErrorInfo` (`IAM_PERMISSION_DENIED`, domain `iam.googleapis.com`, metadata `permission`). GCP appends a per-call Troubleshooter link; gcp-local doesn't.
- IAM Conditions see `resource.type` (`secretmanager.googleapis.com/Secret` or `/SecretVersion`), `resource.service` (`secretmanager.googleapis.com`), and `resource.name` with the project number. Version methods (`get`, `access`, `disable`, `enable`, `destroy`) check the version as requested, so a condition on `versions/2` doesn't grant `versions/latest`; `addVersion` and version `list` check the secret.
- Notifications and the topic check act as the Secret Manager service agent, never as the caller. Grant it `roles/pubsub.publisher` on each topic, as GCP requires.
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

The lifecycle scenarios are in `tests/conformance/security/secretmanager/policies/`, recorded and revalidated 2026-10-13, and in the weekly drift job:

| Recording | Observes |
| --- | --- |
| `secretmanager-secret-policies` | Topics without the agent's grant and a missing topic; rotation, expiration, alias, destroy-TTL, and replica refusals; user-managed replicas, topics, rotation, `ttl`, and `versionDestroyTtl` on create, get, and each version; alias set, get, access, and refusals; replication, expiration, topic, and rotation patches; delayed destroy, a second destroy, and cancel |
| `secretmanager-rotation-events` | A 120 s `ttl` secret's deletion and a rotation 310 s ahead; every notification on one subscription (types, attribute names, data fields); the rotation schedule after `SECRET_ROTATE` |

They need the sandbox's Secret Manager service agent (`gcloud beta services identity create --service=secretmanager.googleapis.com`); the run grants it `roles/pubsub.publisher` on its own topic only. Both run in about 6 minutes. Record with a `--gcp-live-scope-file` scope for service `secretmanager` (`LIMITS` in `gcp_validation/services/security/secretmanager_policies.py`).

## Gaps

- Not modeled, refused with `UNIMPLEMENTED` naming the field: CMEK (`customerManagedEncryption`, `replication.automatic.customerManagedEncryption`, `replication.userManaged.replicas.customerManagedEncryption`), because payloads are not sealed with a Cloud KMS key; and typed secrets (`secretType`). Clearing one in a `patch` is a no-op. Resource Manager `tags` are an unknown field.
- Not recorded: the maximums, a same-count replica change, `SECRET_UPDATE` and the version state-change notifications, delayed destruction completing, and the `timestamp` value's format. GCP delivers notifications seconds to minutes late; gcp-local publishes at once.
- A body with both `expireTime` and `ttl` is 400, from the `expiration` oneof. The Terraform provider 8.5.0 sends both when it updates a secret created with `ttl`, so that update fails here; GCP's answer is not recorded.
- Expiry, rotation, and scheduled destruction run on the process clock; restarting gcp-local loses them with the rest of the state.
- No regional secrets: `projects/*/locations/*/secrets` routes are `UNIMPLEMENTED`. Replica locations are checked only for a region-shaped name.
- A `getIamPolicy` `requestedPolicyVersion` below 3 still returns conditional bindings.
- Locations, policy reads, and all of gRPC are not recorded. Annotation total size (16 KiB) is not checked. State is in memory.
