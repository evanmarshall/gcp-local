# Cloud Logging

Cloud Logging v2 entries write and list, logs list, and logs delete over REST and gRPC. Entries are stored in memory per project. The scenario `logging-entries-lifecycle` is recorded against GCP.

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

For gRPC, build the generated client on a local channel that keeps ADC, as in [Pub/Sub](pubsub.md):

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

Scope credentials to `cloud-platform`. The client's default Logging scopes are not issued by the local token service.

## Methods

| REST | gRPC (`LoggingServiceV2`) | Permission on the project |
| --- | --- | --- |
| `POST /v2/entries:write` | `WriteLogEntries` | `logging.logEntries.create` |
| `POST /v2/entries:list` | `ListLogEntries` | `logging.logEntries.list` |
| `GET /v2/projects/{project}/logs` | `ListLogs` | `logging.logs.list` |
| `DELETE /v2/projects/{project}/logs/{log}` | `DeleteLog` | `logging.logs.delete` |

`roles/logging.logWriter` can write, `roles/logging.viewer` can read, and `roles/logging.admin` can do all four. A write that names several projects needs the permission on each; one denial rejects the whole request and stores nothing.

Write fills in the request's `logName`, `resource`, and `labels` (entry labels win), `timestamp` (now) when missing, `receiveTimestamp`, and `insertId`. Every entry needs a log name and a monitored resource type. `dryRun` validates and authorizes without storing.

List takes `resourceNames` of the form `projects/{project}`, `orderBy` `timestamp asc` (default) or `timestamp desc`, and `pageSize` (default 50, max 1000) with `pageToken`. Logs list returns the logs that hold entries, sorted by name. Deleting a log with no entries returns 404.

## Filters

| Form | Example |
| --- | --- |
| Comparison: `=`, `!=`, `>`, `>=`, `<`, `<=` | `severity>=WARNING`, `timestamp>="2026-10-11T00:00:00Z"` |
| Has (case-insensitive substring), present | `textPayload:"failed"`, `labels.env:*` |
| Regular expression | `textPayload=~"^disk"`, `!~` |
| Global text term (case-insensitive substring of payloads, labels, log name, insert ID) | `timeout`, `"disk full"` |
| `AND` (or a space), `OR`, `NOT` or `-`, parentheses | `severity=ERROR OR (resource.type=gce_instance AND -labels.env=prod)` |

Fields: `logName`, `severity` (name or number), `timestamp`, `receiveTimestamp`, `insertId`, `textPayload`, `trace`, `spanId`, `resource.type`, `resource.labels.KEY`, `labels.KEY`, and `jsonPayload.PATH`. Any other field returns 400.

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `logging-entries-lifecycle` | 2026-10-12 | Write, list, severity, text, and JSON filters, descending pages, logs list, denied write and list, invalid filter, delete |

```sh
sh scripts/check_rest_recordings.sh   # the recording
sh scripts/check_logging.sh           # Python client
```

## Gaps

- Only `projects/` resource names. Organization, folder, billing account, and log bucket view names return 501.
- No `TailLogEntries`, `ListMonitoredResourceDescriptors`, sinks, buckets, views, exclusions, metrics, or `entries.copy`.
- `protoPayload` entries are rejected unless the payload type is linked into the binary (audit logs are not).
- 10,000 entries per project; the oldest are dropped. No retention window. No default 24-hour list window (the Python client adds its own).
- Filter functions (`sample`, `log_id`, `ip_in_net`), `SEARCH`, numeric comparison on `httpRequest`, and private log entry permissions are not modeled.
- Error messages and filter edge cases beyond the recorded scenario have not been compared with Google Cloud.
