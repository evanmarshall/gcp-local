# Cloud Tasks

Cloud Tasks v2beta3 over REST: queues and HTTP-target tasks, dispatched by the provider with retries and optional OIDC tokens. Modeled locally. The scenario `cloudtasks-dispatch-retry` is recorded against GCP.

## Use

Select `-services ...,cloudtasks`. Routes are `/v2beta3/projects/{project}/locations/{location}/queues/...` on port 4566. Task targets must be absolute `http` or `https` URLs that the gcp-local process can reach.

## Methods

| Resource | Methods | Notes |
| --- | --- | --- |
| Queues | `create`, `get`, `list`, `patch`, `delete` | `patch` merges `retryConfig` and `rateLimits`; `updateMask` ignored |
| Queues | `pause`, `resume`, `purge` | A paused queue holds its tasks |
| Queues | `getIamPolicy`, `setIamPolicy` | |
| Tasks | `create`, `get`, `list`, `delete`, `run` | `httpRequest` only; body up to 1 MiB; BASIC view by default |
| Locations | `list` | Returns `us-central1` only |

Dispatch follows the queue's `retryConfig` (defaults: 100 attempts, 0.1 s min backoff, 3600 s max, 16 doublings). Each attempt sets `dispatchCount`, `firstAttempt`, and `lastAttempt`. A successful task is removed; a task that exhausts its attempts is removed 3 s later (local choice). With `oidcToken`, each request carries a local ID token for the service account, audience defaulting to the URL, as in [Pub/Sub push](pubsub.md#push).

## IAM

- Queue and task methods authorize on the queue, so a queue-level `roles/cloudtasks.enqueuer` allows task creation. A missing queue is authorized on the project, then returns 404.
- The FULL view (body and token) needs `cloudtasks.tasks.fullView`.
- A task with `oidcToken` needs `iam.serviceAccounts.actAs` on that account. The account must be a declared same-project local service account.
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
- Tasks and queues are in memory; restart drops pending tasks.
