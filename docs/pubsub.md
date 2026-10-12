# Pub/Sub

Topics, subscriptions, publish, pull, StreamingPull, authenticated local push, ordering, filters, dead-letter topics, retry backoff, exactly-once delivery, snapshots, and seek, with IAM enforced before the call reaches Google's official emulator. Three scenarios are recorded against GCP ([verification](#verification)); everything else is tested locally. Each project gets its own emulator JVM, started on first use.

## Use

The [packaged environment](quickstart.md) includes Java 17 and the emulator. To run it manually, install Java 17, run `python3 scripts/install_emulator_jar.py pubsub`, create [local ADC](local-identity.md), and grant your local principal `roles/pubsub.admin` in the [IAM file](iam.md):

```sh
sh scripts/go.sh build -o bin/gcp-local ./cmd/gcp-local
bin/gcp-local -services pubsub \
  -identity-config /absolute/private/server.json \
  -iam-config /absolute/private/iam.json \
  -pubsub-java "$(command -v java)" \
  -pubsub-jar "$PWD/.tools/pubsub/emulator.jar"
```

gRPC listens on `127.0.0.1:4567` and REST on `127.0.0.1:4566`. Keep `PUBSUB_EMULATOR_HOST` unset; it switches the SDK to anonymous credentials. With `requirements-pubsub.txt`, `GOOGLE_APPLICATION_CREDENTIALS` set to the local profile, and `GOOGLE_CLOUD_PROJECT` set to its project:

```python
import google.auth, grpc, requests
from google.auth.transport.grpc import secure_authorized_channel
from google.auth.transport.requests import Request
from google.cloud import pubsub_v1
from google.pubsub_v1.services.publisher.transports.grpc import PublisherGrpcTransport

session = requests.Session()
session.trust_env = False
request = Request(session=session)
credentials, project = google.auth.default(
    scopes=["https://www.googleapis.com/auth/cloud-platform"], request=request
)
channel = secure_authorized_channel(
    credentials, request, "127.0.0.1:4567", ssl_credentials=grpc.local_channel_credentials()
)
publisher = pubsub_v1.PublisherClient(transport=PublisherGrpcTransport(channel=channel))
```

Use `SubscriberGrpcTransport(channel=channel)` for subscribers. `local_channel_credentials` does not encrypt and works only on loopback. [`sdk.py`](../internal/providers/pubsub/testdata/sdk.py) is the tested example.

## Methods

| Surface | Methods | Notes |
| --- | --- | --- |
| Publisher (gRPC) | `CreateTopic`, `GetTopic`, `ListTopics`, `DeleteTopic`, `Publish` | Labels at create; 1 MiB per publish request |
| Subscriber (gRPC) | `CreateSubscription`, `GetSubscription`, `ListSubscriptions`, `DeleteSubscription`, `Pull`, `Acknowledge`, `ModifyAckDeadline`, `StreamingPull`, `Seek`, `CreateSnapshot`, `GetSnapshot`, `ListSnapshots`, `DeleteSnapshot` | Same-project only; delivery settings below are fixed at create; `ModifyAckDeadline=0` redelivers. StreamingPull `protocol_version` 0 or 1 (the Go client sends 1) on the first request only; with 1 the emulator answers keepalive requests. Other versions return `UNIMPLEMENTED` |
| IAMPolicy (gRPC) | `GetIamPolicy`, `SetIamPolicy` | Topics and subscriptions; version-1 policies with etag checks |
| REST | Topic and subscription `create`, `get`, `list`, `delete`, `getIamPolicy`, `setIamPolicy`; `topics.publish`, `subscriptions.pull`, `subscriptions.acknowledge`, `subscriptions.modifyAckDeadline` | The data-plane methods run the gRPC twins' validation, permissions, and delivery features, and answer in the REST reference's JSON (base64 `data`, `messageIds`, `receivedMessages` with `ackId`, `message`, and `deliveryAttempt`). A body may repeat the path's topic or subscription; a different one returns 400. No REST StreamingPull, snapshots, or seek. Unknown body fields return 400; known unsupported fields return 501 |
| Push | Authenticated HTTP push to a numeric loopback URL | Modeled locally, not recorded |

## Delivery features

Ordering, filters, a repeated exactly-once ack, and dead-lettering with and without the service agent's grants are recorded (`pubsub-delivery-features`, 2026-10-12). Retry backoff timing, snapshots and seek, and push are modeled locally, not recorded. Field rules follow the [`Subscription` reference](https://cloud.google.com/pubsub/docs/reference/rpc/google.pubsub.v1#subscription). "Emulator" is what Google's emulator 0.8.36 does on its own; `check_pubsub_backend.sh` fails if that changes.

| Feature | Emulator | gcp-local |
| --- | --- | --- |
| Dead-letter topic (`dead_letter_policy`) | Counts `delivery_attempt` and forwards after `max_delivery_attempts` (5 to 100; 0 means 5), adding the `CloudPubSubDeadLetter*` attributes. No IAM | Forwards to the dead-letter topic only while the service agent holds both grants (below). Without them, as in GCP, `delivery_attempt` still counts 1, 2, 3, and so on, the message is redelivered past `max_delivery_attempts`, and nothing is forwarded; the provider counts these attempts itself. Push bodies carry `deliveryAttempt` |
| Retry policy (`retry_policy`) | Stored, but redelivers a nack immediately | A nack waits `minimum_backoff`, doubling per consecutive nack of the message up to `maximum_backoff` (defaults 10 s and 600 s). Ack-deadline expiry redelivers with no extra backoff |
| Ordering (`enable_message_ordering`, `ordering_key`) | Delivers in key order, one batch per key, but re-queues a nacked message behind later ones | A nack on an ordered subscription becomes a 1-second deadline; the emulator's expiry path redelivers that message and every later one for the key, in order |
| Filter (`filter`) | Filters on attributes; an unparsable filter fails `UNKNOWN` | Returns `INVALID_ARGUMENT` for an unparsable filter; refuses filters over 256 bytes |
| Exactly-once (`enable_exactly_once_delivery`) | Unary `Acknowledge` and `ModifyAckDeadline` fail with `ErrorInfo` `EXACTLY_ONCE_ACKID_FAILURE` for an acked or unknown ack ID; streams report `exactly_once_delivery_enabled` | A repeated unary `Acknowledge` of an ack ID already acknowledged returns OK, as in GCP. Unknown ack IDs and `ModifyAckDeadline` keep the emulator's failure. Refused on push subscriptions |
| Retention, snapshots, seek (`retain_acked_messages`, `message_retention_duration`) | Seek to a time or snapshot; snapshot create, get, list, delete | Authorizes each call and keeps snapshot labels, which the emulator drops |

### Dead-letter grants

Grant the project's Pub/Sub service agent, `serviceAccount:service-NUMBER@gcp-sa-pubsub.iam.gserviceaccount.com`, `roles/pubsub.publisher` on the dead-letter topic and `roles/pubsub.subscriber` on the source subscription. The provider re-checks both before every pull, nack, stream request, and push delivery, so a grant or revocation takes effect at the next one.

### Push

The provider runs push delivery; the emulator only sees a pull subscription. Set `push_config.push_endpoint` to a numeric loopback HTTP URL and `push_config.oidc_token.service_account_email` to a service account declared in the IAM file:

```json
"service_accounts": [{
  "project": "project-a",
  "email": "push-worker@project-a.iam.gserviceaccount.com",
  "unique_id": "123456789012345678901"
}]
```

On `//iam.googleapis.com/projects/project-a/serviceAccounts/push-worker@project-a.iam.gserviceaccount.com`, grant `roles/iam.serviceAccountUser` to the subscription creator and `roles/iam.serviceAccountTokenCreator` to `serviceAccount:service-NUMBER@gcp-sa-pubsub.iam.gserviceaccount.com`, where `NUMBER` is the project number.

- Each request carries a wrapped JSON message and an RS256 ID token: five-minute expiry, issuer `https://identity.gcp-local.invalid`, audience defaulting to the endpoint. Verify it with the keys at `GET /_local/identity/certs`. It is not Google-signed.
- 200, 201, 202, and 204 acknowledge. Anything else is retried after one second. The HTTP timeout is three seconds; redirects are not followed.
- At most 32 push subscriptions per project, one outstanding message per subscription.
- Pull or StreamingPull on a push subscription fails.

## IAM

Permissions follow [Google's access-control table](https://docs.cloud.google.com/pubsub/docs/access-control); role definitions for Admin, Publisher, Subscriber, and Viewer were pinned 2026-10-05.

- `pubsub.topics.publish` to publish. `pubsub.subscriptions.consume` for pull, ack, modify-deadline, and StreamingPull.
- Creating a subscription needs `pubsub.subscriptions.create` on the project and `pubsub.topics.attachSubscription` on the topic.
- `CreateSnapshot` needs `pubsub.snapshots.create` on the project and `pubsub.subscriptions.consume` on the source. `Seek` needs `pubsub.subscriptions.consume`, plus `pubsub.snapshots.seek` on the snapshot when seeking to one. `GetSnapshot` uses `pubsub.snapshots.get`, which the access-control table omits; the Viewer role carries it.
- Push creation needs `iam.serviceAccounts.actAs`; each delivery needs the service agent's `iam.serviceAccounts.getOpenIdToken`. Without it messages stay pending.
- An open stream rechecks access before each delivery and ack, so revoking stops delivery.

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `pubsub-pull-authorization` | 2026-10-09 | Topic-scoped Publisher and subscription-scoped Subscriber grants, denied publish and pull, message identity, ack, cleanup |
| `pubsub-delivery-features` | 2026-10-12 | Delivery settings echoed at create, denied pull, key order before and after a nack, filtered delivery, an invalid filter, a repeated exactly-once ack, dead-letter attempts and forwarding with and without the service agent's grants, cleanup |
| `pubsub-rest-data-plane` | 2026-10-12 | REST publish, pull, nack (`modifyAckDeadline` 0), redelivery, and acknowledge in the shapes gcloud 588.0.0 sends; denied publish, pull, and acknowledge; a topic-scoped publisher that cannot pull; an out-of-range deadline; an empty pull; a pull without `maxMessages`; a publish body that repeats the topic; cleanup |

`check_pubsub.sh` compares the first two locally; `check_rest_recordings.sh` compares `pubsub-rest-data-plane` (`tests/conformance/pubsubrest`, live scope service `pubsub-rest`). Re-recording `pubsub-delivery-features` (`tests/conformance/pubsub_delivery`) needs its own approved scope (`python3 scripts/plan_pubsub.py --scenario delivery ...`).

Run in Linux or moo after installing Java 17, the emulator JAR, and `requirements-services.txt`:

```sh
sh scripts/check_pubsub.sh          # provider, SDK, push, delivery features, and the recording
sh scripts/check_pubsub_backend.sh  # the bare emulator protocol and the delivery behaviors listed above
```

To re-record, generate a scope file (it starts `approved: false`; get the owner's approval before setting it), then record and revalidate:

```sh
python3 scripts/plan_pubsub.py --profile /abs/.env --setup-account EMAIL \
  --scope-output .gcp-local/pubsub-scope.json
.venv-tools/bin/python -m pytest tests/conformance/pubsub -q \
  --gcp-mode record --gcp-profile /abs/.env \
  --gcp-pubsub-scope /abs/.gcp-local/pubsub-scope.json \
  --gcp-allow-live --gcp-artifacts .artifacts/pubsub-record
```

A run creates one topic and one subscription and makes at most ten publish attempts of at most 1 KiB. It writes an intent manifest under `~/.local/state/gcp-local/PROJECT/pubsub/` before creating anything. After an interrupted run, clean up with `python -m gcp_validation.pubsub cleanup --profile ... --scope ... --manifest RUN_ID.json --allow-live`. See [validation](validation.md) for promotion.

## Gaps

- Not recorded: StreamingPull, push, retry backoff, snapshots, and seek. IAM policy calls are recorded only as the REST grants in `pubsub-rest-data-plane`.
- GCP accepted a publish sent right after its topic was deleted (2026-10-12); locally it returns 404.
- Dead-letter attempts counted before the service agent's grants exist restart at 1 once both grants are held.
- No schemas (`SchemaService`, topic `schema_settings`), KMS, topic retention, subscription expiration TTL, BigQuery or Cloud Storage export subscriptions, `UpdateTopic`, `UpdateSubscription`, `UpdateSnapshot`, `ModifyPushConfig`, `DetachSubscription`, or `ListTopicSnapshots`/`ListTopicSubscriptions`. Subscriptions never expire.
- Exactly-once: an ack after the deadline expires still succeeds (GCP rejects it), and acks sent on a stream get no `acknowledge_confirmation`. The Python client acks over unary calls and is unaffected.
- Retry backoff applies to nacks only; ordered redelivery after a nack waits at least 1 second.
- The dead-letter grants are re-checked at delivery decisions; an ack-deadline expiry between them uses the last checked state.
- No snapshot IAM policies, cross-project dead-letter topics, or cross-project snapshots.
- No `testIamPermissions`, conditional policies, cross-project subscriptions, or anonymous, remote, or HTTPS push.
- Messages are in memory; restart loses them.
