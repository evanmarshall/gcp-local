# Pub/Sub

Issue [#12](https://github.com/evanmarshall/gcp-local/issues/12) implements the initial local slice. A gRPC provider wraps the official emulator with local ADC, basic IAM, pull/stream delivery, and authenticated local HTTP push. One Pub/Sub scenario, the [bounded pull and authorization recording](pubsub-validation.md), was captured and revalidated against real GCP on 2026-10-09 UTC, and the local provider matches it in comparison mode. Broader Pub/Sub fidelity remains tracked in [#29](https://github.com/evanmarshall/gcp-local/issues/29). The [packaged environment](quickstart.md) carries a pinned Java 17 runtime and the emulator JAR, selects every implemented provider, and runs the full [push workflow](workflow.md#run-it-in-the-packaged-environment).

## Supported local slice

| Surface | Local coverage |
| --- | --- |
| Topics | Create/get/list/delete; labels at creation; publish data and attributes |
| Subscriptions | Same-project create/get/list/delete; labels and ack deadline at creation; pull, acknowledge, modify ack deadline |
| StreamingPull | Initial request, data/ack relay, half-close, cancellation, reset/shutdown cleanup; subscription switching rejected |
| IAM | Project inheritance and resource grants; get/set unconditional v1 policies with etag checks; separate subscription-create and topic-attachment checks |
| Python SDK | Pinned 2.42.0, grpcio 1.84.0; local ADC refresh, publish/pull/ack, streaming callback/cancel, denial, cleanup |
| Retry | Local nack (`ModifyAckDeadline=0`) redelivers the same message ID through the official backend; no retry timing or exactly-once claim |
| Authenticated push | Same-project service account; caller `actAs`; service-agent ID-token permission; signed local token; wrapped JSON body; HTTP success acknowledgement, retry after failure, delete/reset/shutdown cancellation |

Permission plans follow [Google's access-control table](https://docs.cloud.google.com/pubsub/docs/access-control). Pub/Sub Admin, Publisher, Subscriber, and Viewer role definitions were read and pinned on 2026-10-05; reading roles made no grants or service resources. Local tests show denied calls never reach or start the backend. An existing stream checks authorization again before forwarding acknowledgements or delivering messages, so revocation prevents subsequent delivery. These are implementation tests, not recorded GCP authorization evidence.

Each resource project owns one lazy JVM restricted to that project. The emulator owns configurations and queues; the shared index owns resource references; IAM owns policies. Control-plane writes publish index changes only after backend success. An uncertain create/delete is reconciled by reading the backend; an unresolved result blocks the scope until reset. Reset cancels sessions, stops the child, then clears only that project's Pub/Sub resources and policies. Project bootstrap policies survive. Sessions allow a publisher to proceed while a pull/stream waits.

Anonymous push, remote/HTTPS push endpoints, cross-project subscriptions or push accounts, ordering, exactly-once delivery, schemas, KMS, filters, dead-letter policies, configurable retries, retention/expiry fidelity, snapshots/seek, updates, and unknown protobuf fields return explicit coverage errors. Topic/subscription `testIamPermissions` and REST transport are not registered. Only named local callers are supported. Publication requests have a 1 MiB local limit. JVMs use a 256 MiB heap and additional native memory; the runtime is process-local and does not persist messages.

## Authenticated local push

The provider owns push configuration and workers. The emulator receives a **pull-only** subscription and never sees the push URL. Create a subscription with `push_config.push_endpoint` set to an explicit numeric loopback HTTP URL and `push_config.oidc_token.service_account_email` set to a declared local service account. Optional `audience` defaults to the endpoint. A pull or streaming request on this push subscription fails rather than competing with its delivery worker. Get/list responses restore the provider-owned push configuration.

Declare the account in the private IAM bootstrap (alongside the existing project resource, whose `number` is required):

```json
"service_accounts": [{
  "project": "project-a",
  "email": "push-worker@project-a.iam.gserviceaccount.com",
  "unique_id": "123456789012345678901"
}]
```

Attach a `roles/iam.serviceAccountUser` binding for the subscription creator and a `roles/iam.serviceAccountTokenCreator` binding for `serviceAccount:service-123456@gcp-sa-pubsub.iam.gserviceaccount.com` to the account's canonical policy resource `//iam.googleapis.com/projects/project-a/serviceAccounts/push-worker@project-a.iam.gserviceaccount.com`. Replace `123456` with the declared project number. These are explicit **local** grants; this configuration does not change GCP. The creator separately needs the ordinary subscription-create and topic-attachment permissions.

Creation checks `iam.serviceAccounts.actAs`. Each delivery checks the service agent's `iam.serviceAccounts.getOpenIdToken` on the configured account. A missing/revoked agent grant leaves messages pending and sends no HTTP request. Tokens are signed with the runtime's ephemeral RS256 key, carry the account email/unique ID and requested audience, expire after five minutes, and use `https://identity.gcp-local.invalid` as issuer. Fetch the runtime's public PEM key map from `GET /_local/identity/certs`; validate signature, expiry, audience, issuer, and expected email. The integration runner additionally verifies the token with Google's Python JWT verifier. Tokens cannot authenticate to real GCP and are not claimed to be Google-signed.

Workers send wrapped Pub/Sub JSON, acknowledge final HTTP responses 200/201/202/204, and nack failures for redelivery. Receivers that send only an informational 102 response are outside this slice: the Go HTTP client waits for a final response, so that case times out and retries. The local implementation retries after one second with a three-second HTTP timeout, one outstanding message per worker, and at most 32 push subscriptions per project. This bounds local work; it does not reproduce GCP's adaptive push backoff, token reuse, delivery timing, or retry-policy semantics. Redirects and ambient proxies are disabled. Delete/reset cancels and drains workers and HTTP requests before returning. Reset preserves separately owned local service accounts and their IAM policies.

The split between caller and service-agent checks follows [Google's push authentication documentation](https://docs.cloud.google.com/pubsub/docs/authenticate-push-subscriptions); acknowledgement behavior follows [the push delivery guide](https://docs.cloud.google.com/pubsub/docs/push). It remains locally tested, without recorded GCP push evidence.

## Run the provider

Use `python3 scripts/local_env.py up` (or the `gcp-local` CLI) for the [packaged environment](quickstart.md), or follow the manual instructions below for a private runtime checkout or Moo machine.

The manual path uses Java 17, the pinned Go toolchain, and `python3 scripts/install_emulator_jar.py pubsub`. Generate an identity bundle with `scripts/create_local_identity.py`; configure the corresponding project and a `roles/pubsub.admin` grant to that named local principal in an owner-only IAM file, following [the bootstrap format](iam.md). Then:

```sh
sh scripts/go.sh build -o bin/gcp-local ./cmd/gcp-local
bin/gcp-local -services pubsub \
  -identity-config /absolute/private/server.json \
  -iam-config /absolute/private/iam.json \
  -pubsub-java "$(command -v java)" \
  -pubsub-jar "$PWD/.tools/pubsub/emulator.jar"
```

Select `-services storage,pubsub` to enable both. HTTP identity/diagnostics uses loopback port 4566 and gRPC uses loopback port 4567; the packaged environment publishes the same pair on host loopback through its HAProxy entry point and grants `roles/pubsub.admin` (among the other packaged roles) to the generated developer identity. Emulator ports are random and private implementation details. A process running on the same host can access loopback; run untrusted applications in a separate network namespace/container that exposes only the gateway. The child inherits no ADC, cloud credentials, proxies, or push endpoints. Core process shutdown removes its temporary directories and processes. Reset is currently an internal lifecycle operation; restarting the core resets all its in-memory state.

For Python install `requirements-pubsub.txt` and use an explicit local ADC file. Keep `PUBSUB_EMULATOR_HOST` unset: that mode selects anonymous credentials. The verified transport configuration is:

```python
import google.auth
import grpc
import requests
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

Use `SubscriberGrpcTransport(channel=channel)` for the subscriber. Set `GOOGLE_APPLICATION_CREDENTIALS` to the generated local profile and `GOOGLE_CLOUD_PROJECT` to its synthetic project. The local gRPC credential API is [experimental and supports local TCP call credentials](https://grpc.github.io/grpc/python/grpc.html#grpc.local_channel_credentials); it does not encrypt TCP. This verified setup is limited to numeric loopback connections, not a general remote TLS configuration. The [actual SDK test](../internal/providers/pubsub/testdata/sdk.py) also confines credential refresh and API traffic to loopback and bounds waits.

## Provider verification

```sh
.venv-tools/bin/python -m pip install -r requirements-pubsub.txt
sh scripts/check_pubsub.sh
```

The race-enabled Go suite and real Python SDK run with only loopback available. Tests exercise resource grants and stale policy etags, grant revocation during an active stream, project isolation, nack/redelivery, half-close, client cancellation, reset, shutdown, and uncertain control-plane responses. Push tests verify `actAs` denial before backend access, separate service-agent grants/revocation, independently verified JWTs, retry message identity, redirect rejection, and cancellation of an active receiver. The runner writes `.artifacts/pubsub-provider/results.json` with test outcomes, source/backend hashes, SDK versions, and an explicit **unverified** cloud-fidelity status. CI also reruns the existing Storage recordings and broader core tests. Project provisioning already existed; the 2026-10-09 pull/authorization session enabled `pubsub.googleapis.com`, added one temporary fixture grant, recorded and revalidated, then removed that grant. No other real-GCP Pub/Sub execution has occurred.

## Backend verification

Install Java 17 in the task machine, then run from its synced checkout:

```sh
python3 scripts/install_emulator_jar.py pubsub
sh scripts/check_pubsub_backend.sh
```

The installer downloads Google's emulator component directly, verifies the archive and JAR hashes in [the pin](../toolchains/pubsub.json), and installs only the JAR. It does not install or authenticate gcloud. The runner requires Linux network/PID namespaces and runs with loopback only; it needs no GCP credentials. CI installs dependencies before blocking network access.

The probe exercises topic/subscription creation, publication, pull, acknowledgement, bidirectional StreamingPull, half-close, cancellation, shutdown, and restart. It demonstrates that invalid bearer credentials are accepted upstream and that IAM policy RPCs return `UNIMPLEMENTED`; the provider must supply authorization before forwarding. Restart loses resources, so this backend has no persistence claim. A JSON artifact includes the exact emulator hash, Java version, protocol results, and startup/RSS/shutdown samples.

The first successful Moo run on 2026-10-05 used emulator 0.8.36 and OpenJDK 17.0.20.1 in a 2 CPU/2 GiB task machine. Two warm starts took 0.257 and 0.254 seconds; RSS after the probes was 129868 and 114468 KiB; shutdown took 0.008 and 0.034 seconds. These are individual development measurements, not throughput or performance guarantees. Guest-internal and [host-forwarded gRPC probes](moo-runtime.md) both passed, including streaming, half-close, cancellation, and restart state loss.

Google documents emulator gaps including IAM, retention/expiration, and some update operations in [the emulator guide](https://docs.cloud.google.com/pubsub/docs/emulator). Backend success establishes no GCP permission fidelity. Unsupported configurations remain explicit; push delivery and identity belong to the provider rather than the unauthenticated backend.
