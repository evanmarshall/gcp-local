# Cloud Tasks

Cloud Tasks over REST (v2beta3) and gRPC (v2): queues and HTTP-target tasks, dispatched by the provider with retries and optional OIDC tokens. Modeled locally. The scenario `cloudtasks-dispatch-retry` is recorded against GCP over REST; gRPC is not recorded.

## Use

Select `-services ...,cloudtasks`. REST routes are `/v2beta3/projects/{project}/locations/{location}/queues/...` on port 4566. gRPC serves `google.cloud.tasks.v2.CloudTasks` on port 4567, the version and transport the default client libraries use (`tasks_v2`, `cloudtasks/apiv2`). Task targets must be absolute `http` or `https` URLs that the gcp-local process can reach.

Each v2 gRPC method runs the v2beta3 REST method with the same path, so IAM checks and errors match. v2 fields that v2beta3 shares are honored; v2beta3-only response fields (`type`, `taskTtl`, `tombstoneTtl`) are dropped. Connect with local ADC on a loopback channel, as for [Pub/Sub](pubsub.md#use):

```python
from google.cloud import tasks_v2
from google.cloud.tasks_v2.services.cloud_tasks.transports import CloudTasksGrpcTransport

# channel = secure_authorized_channel(credentials, request, "127.0.0.1:4567",
#                                     ssl_credentials=grpc.local_channel_credentials())
client = tasks_v2.CloudTasksClient(transport=CloudTasksGrpcTransport(channel=channel))
```

[`sdk.py`](../internal/providers/cloudtasks/testdata/sdk.py) is the tested Python example; [`client_test.go`](../internal/providers/cloudtasks/client_test.go) drives the Go client with `option.WithGRPCConn`.

## Methods

| Resource | Methods | Notes |
| --- | --- | --- |
| Queues | `create`, `get`, `list`, `patch`, `delete` | `patch` merges `retryConfig` and `rateLimits`; `updateMask` ignored; `list` returns one page; a `filter` is refused with `UNIMPLEMENTED` |
| Queues | `pause`, `resume`, `purge` | A paused queue holds its tasks |
| Queues | `getIamPolicy`, `setIamPolicy` | |
| Tasks | `create`, `get`, `list`, `delete`, `run` | `httpRequest` only; body up to 1 MiB; BASIC view by default |
| Locations | `list` | REST only; returns `us-central1` only |

gRPC `TestIamPermissions`, `BatchCreateTasks`, `BatchDeleteTasks`, `GetCmekConfig`, and `UpdateCmekConfig` return `UNIMPLEMENTED`; the client libraries do not retry that code. An anonymous call gets `UNAUTHENTICATED` (REST 401).

Dispatch follows the queue's `retryConfig` (defaults: 100 attempts, 0.1 s min backoff, 3600 s max, 16 doublings). Each attempt sets `dispatchCount`, `firstAttempt`, and `lastAttempt`. A successful task is removed; a task that exhausts its attempts is removed 3 s later (local choice). With `oidcToken`, each attempt carries a local ID token for the service account, audience defaulting to the URL, minted by the project's Cloud Tasks service agent. When the token can't be minted, the attempt fails with the mint's status (for example `PERMISSION_DENIED`), nothing is sent, and the queue's retry policy applies.

## IAM

- Queue and task methods authorize on the queue, so a queue-level `roles/cloudtasks.enqueuer` allows task creation. A missing queue is authorized on the project, then returns 404.
- The FULL view (body and token) needs `cloudtasks.tasks.fullView`.
- A task with `oidcToken` needs `iam.serviceAccounts.actAs` on that account. The account must be a declared same-project local service account.
- The service agent, `serviceAccount:service-NUMBER@gcp-sa-cloudtasks.iam.gserviceaccount.com`, needs `iam.serviceAccounts.getOpenIdToken` on the account. GCP grants it `roles/cloudtasks.serviceAgent` when the API is enabled; that role isn't pinned here, so grant the agent `roles/iam.serviceAccountTokenCreator` on the account, as for [Pub/Sub push](pubsub.md#push).
- Pinned roles: `roles/cloudtasks.admin`, `roles/cloudtasks.enqueuer`.

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `cloudtasks-dispatch-retry` | 2026-10-11 | Queue create, denied enqueue, policy read, queue-level enqueuer grant, enqueuer cannot list, first attempt, removal after retries, OIDC without `actAs`, invalid URL, pause and resume, delete then 404 |

Rate limits, TTLs, and queue type are compared by presence only.

```sh
sh scripts/check_rest_recordings.sh
```

`TestAccCloudTasksQueueLifecycle` passes ([Terraform](terraform-compatibility.md)).

## Gaps

- No App Engine targets, OAuth tokens, `maxRetryDuration`, or rate-limit enforcement.
- Queue `httpTarget`, `appEngineRoutingOverride`, `stackdriverLoggingConfig`, and task-level `retryConfig` (v2) are accepted and ignored.
- gRPC is not recorded.
- Tasks and queues are in memory; restart drops pending tasks.
