# Cloud Scheduler

Cloud Scheduler v1 jobs over REST and gRPC: create, get, list, patch, delete, pause, resume, and run, with HTTP and Pub/Sub targets, unix-cron schedules in any tz database time zone, and retries. Jobs run in provider workers as the project's Cloud Scheduler service agent. The scenario `cloudscheduler-job-lifecycle` is recorded against GCP over REST; gRPC is not recorded.

## Use

Select `-services ...,cloudscheduler`, plus `pubsub` for Pub/Sub targets. REST routes are `/v1/projects/{project}/locations/{location}/jobs/...` on port 4566. gRPC serves `google.cloud.scheduler.v1.CloudScheduler` on port 4567, the transport the client libraries use; each gRPC method runs the REST method with the same path, so IAM checks and errors match.

```go
// conn: a loopback gRPC connection carrying local ADC (docs/contributing/clients.md).
client, _ := scheduler.NewCloudSchedulerClient(ctx, option.WithGRPCConn(conn))
job, _ := client.CreateJob(ctx, &schedulerpb.CreateJobRequest{Parent: "projects/p/locations/us-central1", Job: &schedulerpb.Job{
	Name: "projects/p/locations/us-central1/jobs/nightly", Schedule: "0 9 * * *", TimeZone: "America/New_York",
	Target: &schedulerpb.Job_HttpTarget{HttpTarget: &schedulerpb.HttpTarget{Uri: "http://127.0.0.1:8080/run"}},
}})
```

[`client_test.go`](../../../internal/providers/messaging/cloudscheduler/client_test.go) is the tested Go example.

## Methods

| Method | Notes |
| --- | --- |
| `create` | Unknown fields are refused. An empty name gets a generated numeric ID. Duplicate names are `ALREADY_EXISTS` |
| `get`, `delete` | Delete stops the job's worker |
| `list` | `pageSize` (default and maximum 500) and `pageToken` |
| `patch` | `updateMask` takes top-level and nested paths (`retryConfig.retryCount`); without a mask every field the body sets is updated. Output fields in the mask are `INVALID_ARGUMENT` |
| `pause`, `resume` | Pausing a paused job succeeds. A paused job has no `scheduleTime`; resume schedules the next time from now |
| `run` | Runs the job now, also when paused (not recorded); the response is the job before the run |

Locations, CMEK config, and operations methods return `UNIMPLEMENTED`. An anonymous call gets 401.

## Schedules and runs

- `schedule` is unix-cron: five fields, month and weekday names. English-like schedules (`every 5 minutes`) return `UNIMPLEMENTED`.
- `timeZone` is a tz database name or `utc`; empty is UTC. Daylight saving follows the zone.
- One execution, retries included, finishes before the next starts.
- `status` is `{"code": -1}` before the first attempt, `{}` after a success, and the attempt's code and message after a failure. `lastAttemptTime` is the start of the last attempt, reported when it completes, as GCP does.
- `retryConfig` retries a failed attempt `retryCount` times (0 to 5) or, with `maxRetryDuration`, until that long after the first attempt; with both, until both are reached. Backoff starts at `minBackoffDuration`, doubles `maxDoublings` times, then grows linearly to `maxBackoffDuration`. A supplied `retryConfig` echoes every field with the defaults (0, `0s`, `5s`, `3600s`, 5); an absent one stays absent. During a retry `scheduleTime` is the retry time.
- Pausing stops further retries.
- The provider's clock is injectable; the unit tests advance a manual clock.

## Targets

| Target | Delivery |
| --- | --- |
| `httpTarget` | `httpMethod` defaults to POST; a body needs POST, PUT, or PATCH. Adds `User-Agent: Google-Cloud-Scheduler`, `Content-Type: application/octet-stream` for a body without one, `X-Cloudscheduler: true`, and `X-Cloudscheduler-Jobname`. `attemptDeadline` defaults to 180 s and must be 15 s to 30 min; a timeout is `DEADLINE_EXCEEDED`, an unreachable target `UNAVAILABLE`. Non-2xx responses map to RPC codes as in [Cloud Tasks](cloudtasks.md) |
| `oidcToken` | A local ID token for the account, audience defaulting to the URI, verifiable as for [Pub/Sub push](pubsub.md#push) |
| `oauthToken` | A local access token for the account, scope defaulting to `cloud-platform`. The account must be enrolled in the identity configuration (`--workload` in the packaged environment); otherwise the attempt fails with `UNIMPLEMENTED` |
| `pubsubTarget` | Publishes `data` and `attributes` through the Pub/Sub provider. The topic must be in the job's project; `attemptDeadline` is dropped, as GCP does |
| `appEngineHttpTarget` | `UNIMPLEMENTED` |

## IAM

- Methods check `cloudscheduler.jobs.create`, `get`, `list`, `update`, `delete`, `pause`, `enable` (resume), and `run` on the job, whose parent is the project. A missing job is authorized on the project, then returns 404.
- A job with `oidcToken` or `oauthToken` needs `iam.serviceAccounts.actAs` on that account at create, and at patch when the update touches the target. The account must be a declared same-project local service account.
- Deliveries act as `serviceAccount:service-NUMBER@gcp-sa-cloudscheduler.iam.gserviceaccount.com`. Publishing needs its `pubsub.topics.publish`; OIDC and OAuth tokens need its `iam.serviceAccounts.getOpenIdToken` and `getAccessToken`. GCP grants it `roles/cloudscheduler.serviceAgent` on the project; the packaged environment writes that grant, and with `bin/gcp-local` you add it to the IAM configuration. Without it attempts fail with `PERMISSION_DENIED`.
- Pinned roles: `roles/cloudscheduler.admin`, `viewer`, `jobRunner`, `serviceAgent`.

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `cloudscheduler-job-lifecycle` | 2026-10-12 | Pub/Sub job create with retry defaults and a New York schedule, denied get, create, and run, list, pause (twice), resume, masked patch, run and its attempt status, an OIDC HTTP job's defaults, OIDC and OAuth without `actAs`, invalid schedule and time zone, delete, 404 |

```sh
sh scripts/check/recordings.sh
```

`TestAccCloudSchedulerJob` passes ([Terraform](../../contributing/terraform-compatibility.md)). The Go, Python, and Node.js clients pass the [client matrix](../../contributing/clients.md).

## Gaps

- No App Engine targets, English-like schedules, locations, CMEK, or operations methods.
- GCP answered overlapping mutations of one job with 409 `ABORTED` ("sync mutate calls cannot be queued") while recording; the scenario retries them. gcp-local applies them in order.
- `DISABLED` and `UPDATE_FAILED` states are not modeled.
- HTTP deliveries, OAuth tokens, retries, and run on a paused job are modeled locally, not recorded. gRPC is not recorded.
- Jobs are in memory; restart drops them.
