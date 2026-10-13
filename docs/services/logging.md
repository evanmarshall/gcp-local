# Cloud Logging

Cloud Logging v2 entries write and list, logs list, and logs delete over REST and gRPC. Entries are stored in memory per project. The scenarios `logging-entries-lifecycle`, `logging-filter-precedence`, and `logging-conditional-access` are recorded against GCP.

## Use

The Python client works over REST with local ADC:

```python
import google.auth
from google.cloud import logging

credentials, _ = google.auth.load_credentials_from_file(
    ".gcp-local/environment/identity/adc.json",
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
client = logging.Client(
    project="gcp-local-test",
    credentials=credentials,
    _use_grpc=False,
    client_options={"api_endpoint": "http://127.0.0.1:4566"},
)
client.logger("app").log_text("started", severity="INFO")
for entry in client.list_entries(filter_="severity>=INFO"):
    print(entry.payload)
```

For gRPC, build the generated client on a local channel that keeps ADC, as in [Pub/Sub](messaging/pubsub.md):

```python
import grpc
from google.auth.transport.grpc import secure_authorized_channel
from google.auth.transport.requests import Request
from google.cloud.logging_v2.services.logging_service_v2 import LoggingServiceV2Client
from google.cloud.logging_v2.services.logging_service_v2.transports import LoggingServiceV2GrpcTransport

channel = secure_authorized_channel(
    credentials, Request(), "127.0.0.1:4567", ssl_credentials=grpc.local_channel_credentials()
)
gapic = LoggingServiceV2Client(transport=LoggingServiceV2GrpcTransport(channel=channel))
```

The example scopes credentials to `cloud-platform`. The client's default Logging scopes are also [issued](../guides/local-identity.md#oauth-scopes).

## Methods

| REST | gRPC (`LoggingServiceV2`) | Permission on the project |
| --- | --- | --- |
| `POST /v2/entries:write` | `WriteLogEntries` | `logging.logEntries.create` |
| `POST /v2/entries:list` | `ListLogEntries` | `logging.logEntries.list` |
| `GET /v2/projects/{project}/logs` | `ListLogs` | `logging.logs.list` |
| `DELETE /v2/projects/{project}/logs/{log}` | `DeleteLog` | `logging.logs.delete` |

`roles/logging.logWriter` can write, `roles/logging.viewer` can read, and `roles/logging.admin` can do all four. A write that names several projects needs the permission on each; one denial rejects the whole request and stores nothing.

Denials are GCP's: a write or log delete is 403 `Permission '...' denied on resource '//logging.googleapis.com/projects/P/logs/LOG' (or it may not exist).` with ErrorInfo `IAM_PERMISSION_DENIED`, domain `iam.googleapis.com`; entries and logs list are 403 `Permission denied for all log views` with no details. A project-level IAM Condition sees `resource.service == "logging.googleapis.com"`; `resource.type` and `resource.name` are not supplied, so a condition on them grants nothing.

Write fills in the request's `logName`, `resource`, and `labels` (entry labels win), `timestamp` (now) when missing, `receiveTimestamp`, and `insertId`. Every entry needs a log name and a monitored resource type. `dryRun` validates and authorizes without storing. REST bodies are decoded strictly: an unknown field is GCP's 400 (`Unknown name "x" at 'entries[0]'`).

List takes `resourceNames` of the form `projects/{project}`, `orderBy` `timestamp asc` (default) or `timestamp desc` (anything else is 400 `order_by must be on timestamp`), and `pageSize` (default 50, max 1000) with `pageToken` (a token from another request is 400 `Invalid page_token`). Logs list returns the logs that hold entries, sorted by name, in pages of up to 1000. Deleting a log with no entries returns 404.

## Filters

| Form | Example |
| --- | --- |
| Comparison: `=`, `!=`, `>`, `>=`, `<`, `<=` | `severity>=WARNING`, `timestamp>="2026-10-11T00:00:00Z"` |
| Has (case-insensitive substring), present | `textPayload:"failed"`, `labels.env:*` |
| Regular expression | `textPayload=~"^disk"`, `!~` |
| Global text term (case-insensitive substring of payloads, labels, log name, insert ID) | `timeout`, `"disk full"` |
| `AND` (or a space), `OR`, `NOT` or `-`, parentheses | `severity=ERROR OR (resource.type=gce_instance AND -labels.env=prod)` |

Precedence is Cloud Logging's, not the usual one: `NOT`, then `OR`, then `AND` ([query language](https://cloud.google.com/logging/docs/view/logging-query-language#boolean_operators)). `a AND b OR c` means `a AND (b OR c)`, and `a OR b c` means `(a OR b) AND c`. Use parentheses to say otherwise.

Fields: `logName`, `severity` (name or number), `timestamp`, `receiveTimestamp`, `insertId`, `textPayload`, `trace`, `spanId`, `resource.type`, `resource.labels.KEY`, `labels.KEY`, and `jsonPayload.PATH`. Any other `LogEntry` field (`httpRequest.status`, `operation.id`, `protoPayload.x`) returns 501 naming it; a field `LogEntry` lacks is GCP's 400 `Field not found: 'x'.`

Functions return 501 naming the function: `log_id()`, `sample()`, `ip_in_net()`, `SEARCH()`, and `cast()`. GCP evaluates them; gcp-local never turns them into text searches. Any other function is GCP's 400 `Unsupported global function: 'x'.`

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `logging-entries-lifecycle` | 2026-10-13 | Write, list, severity, text, and JSON filters, descending pages, logs list, denials of write, list, logs list, and delete with their details, invalid filter, unknown function and field, bad page token, negative page size, unsupported order, unknown body fields, logs list bad token, delete |
| `logging-filter-precedence` | 2026-10-13 | `AND` with `OR`, `OR` with implicit `AND`, `NOT` with both, parentheses, and the query-language page's own example |
| `logging-conditional-access` | 2026-10-13 | Project-level conditions on `resource.service`: granted for Logging write and list, not for Artifact Registry or Vertex AI; time-condition controls for those two |

```sh
sh scripts/check/recordings.sh   # the recording
sh scripts/check/sdk/logging.sh           # Python client
```

`logging-conditional-access` writes the project policy: it needs `roles/resourcemanager.projectIamAdmin` on the fixture for the run, takes the project IAM lock, and runs alone in the live batch.

## Gaps

- Only `projects/` resource names. Organization, folder, billing account, and log bucket view names return 501.
- Sinks, exclusions, buckets, views, and log metrics aren't served (501), so no write is routed or excluded. Also not served: `TailLogEntries`, `ListMonitoredResourceDescriptors`, `entries.copy`.
- `protoPayload` entries return 501. `partialSuccess` is accepted; with an invalid entry it returns 501 (GCP writes the valid ones).
- 10,000 entries per project; the oldest are dropped. No retention window. No default 24-hour list window (the Python client adds its own).
- Filter functions, fields outside the list above, and private log entry permissions are not modeled.
- 401 wording and filter edge cases beyond the recorded scenarios have not been compared with Google Cloud.
