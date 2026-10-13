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

[`sdk.py`](../../../internal/providers/messaging/cloudtasks/testdata/sdk.py) is the tested Python example; [`client_test.go`](../../../internal/providers/messaging/cloudtasks/client_test.go) drives the Go client with `option.WithGRPCConn`.

## Methods

| Resource | Methods | Notes |
| --- | --- | --- |
| Queues | `create`, `get`, `list`, `patch`, `delete` | `patch` replaces the queue, or only the fields `updateMask` names; `list` pages by name (default and maximum 9800); a `filter` or `readMask` returns `UNIMPLEMENTED` |
| Queues | `pause`, `resume`, `purge` | A paused queue holds its tasks; `purge` sets `purgeTime` |
| Queues | `getIamPolicy`, `setIamPolicy` | A conditional binding is 400 `Can't set conditional policy on this resource`, as in GCP |
| Tasks | `create`, `get`, `list`, `delete`, `run` | `httpRequest` only; body up to 1 MiB; BASIC view by default; `list` pages by name (default and maximum 1000) and, as GCP does, returns a next token after every full page |
| Locations | `list` | REST only; returns `us-central1` only |

Bodies decode strictly against the v2beta3 schema: an unknown name or a mistyped value is GCP's 400 with a `BadRequest` violation, and an unmodeled field set to a non-default value returns `UNIMPLEMENTED` naming it. A bad page token is 400 `Invalid page_token`; a negative page size is 400 `ListQueuesRequest.pageSize cannot be negative.` (or `ListTasksRequest`).

gRPC `TestIamPermissions`, `BatchCreateTasks`, `BatchDeleteTasks`, `GetCmekConfig`, and `UpdateCmekConfig` return `UNIMPLEMENTED`; the client libraries do not retry that code. An anonymous call gets `UNAUTHENTICATED` (REST 401).

Dispatch follows the queue's `retryConfig` (defaults: 100 attempts, 0.1 s min backoff, 3600 s max, 16 doublings, no `maxRetryDuration`). A failed task stops only once it has had `maxAttempts` attempts and, when set, `maxRetryDuration` has passed since its first attempt. Dispatches take tokens from a bucket of `maxBurstSize` (default 100) refilled at `maxDispatchesPerSecond` (default 500), and at most `maxConcurrentDispatches` (default 1000) run at once. Each attempt ends at the task's `dispatchDeadline` (default 600 s, 15 s to 30 min) with `DEADLINE_EXCEEDED`; an unreachable target is `UNKNOWN`. Each attempt sets `dispatchCount`, `firstAttempt`, and `lastAttempt`. A successful task is removed; a task that exhausts its retries is removed 3 s later (local choice). A deleted, completed, or purged task's name can't be reused for the queue's `tombstoneTtl` (default 1 h): create is 409 `The task cannot be created because a task with this name existed too recently. ...`. With `oidcToken`, each attempt carries a local ID token for the service account, audience defaulting to the URL, minted by the project's Cloud Tasks service agent. When the token can't be minted, the attempt fails with the mint's status (for example `PERMISSION_DENIED`), nothing is sent, and the queue's retry policy applies.

## IAM

- Queue and task methods authorize on the queue, so a queue-level `roles/cloudtasks.enqueuer` allows task creation. A missing queue is authorized on the project, then returns 404.
- A denial is 403 `The principal (user or service account) lacks IAM permission "PERMISSION" for the resource "NAME" (or the resource may not exist).` with no details, naming the task, the queue, or the location for queue create and list.
- No resource attributes reach IAM Conditions; a project-level condition on `resource.*` grants nothing. GCP's behavior there is not recorded.
- The FULL view (body and token) needs `cloudtasks.tasks.fullView`.
- A task with `oidcToken` needs `iam.serviceAccounts.actAs` on that account. The account must be a declared same-project local service account.
- The service agent, `serviceAccount:service-NUMBER@gcp-sa-cloudtasks.iam.gserviceaccount.com`, needs `iam.serviceAccounts.getOpenIdToken` on the account. GCP grants it `roles/cloudtasks.serviceAgent` when the API is enabled; that role isn't pinned here, so grant the agent `roles/iam.serviceAccountTokenCreator` on the account, as for [Pub/Sub push](pubsub.md#push).
- Pinned roles: `roles/cloudtasks.admin`, `roles/cloudtasks.enqueuer`.

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `cloudtasks-dispatch-retry` | 2026-10-13 | Queue create, denied enqueue, task get, and task list with messages, policy read, queue-level enqueuer grant, first attempt, removal after retries, OIDC without `actAs`, invalid URL, pause; on the paused queue: `dispatchDeadline` echo and bounds, task paging, bad page tokens and negative sizes for tasks and queues, a tombstoned name, a masked queue update with `maxRetryDuration`, a refused conditional policy; resume, delete then 404 |

Rate limits and TTLs are compared by presence only.

```sh
sh scripts/check/recordings.sh
```

`TestLocalCloudTasksQueueLifecycle` passes ([Terraform](../../contributing/terraform-compatibility.md)).

## Gaps

- `UNIMPLEMENTED` when set: App Engine queues and tasks, pull queues and tasks, OAuth tokens, queue `httpTarget`, `stackdriverLoggingConfig`, task-level `retryConfig`, a `taskTtl` other than 31 days, `readMask`, list `filter`, `setIamPolicy` `updateMask` and audit configs, and `patch` of a missing queue (GCP creates it).
- A v2 gRPC `appEngineRoutingOverride` is not in v2beta3 and is refused with 400.
- A deleted queue's name can be reused at once; GCP reserves it for 7 days.
- gRPC is not recorded.
- Tasks and queues are in memory; restart drops pending tasks.
