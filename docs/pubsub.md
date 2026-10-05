# Pub/Sub

Issue [#12](https://github.com/evanmarshall/gcp-local/issues/12) is in progress. A gRPC provider now wraps the official emulator with local ADC, basic IAM, pull delivery, and managed streams. No Pub/Sub scenario has been recorded against GCP. Authenticated push remains pending; the packaged Storage environment does not yet include Java or Pub/Sub.

## Supported local slice

| Surface | Local coverage |
| --- | --- |
| Topics | Create/get/list/delete; labels at creation; publish data and attributes |
| Subscriptions | Same-project create/get/list/delete; labels and ack deadline at creation; pull, acknowledge, modify ack deadline |
| StreamingPull | Initial request, data/ack relay, half-close, cancellation, reset/shutdown cleanup; subscription switching rejected |
| IAM | Project inheritance and resource grants; get/set unconditional v1 policies with etag checks; separate subscription-create and topic-attachment checks |
| Python SDK | Pinned 2.42.0, grpcio 1.84.0; local ADC refresh, publish/pull/ack, streaming callback/cancel, denial, cleanup |
| Retry | Local nack (`ModifyAckDeadline=0`) redelivers the same message ID through the official backend; no retry timing or exactly-once claim |

Permission plans follow [Google's access-control table](https://docs.cloud.google.com/pubsub/docs/access-control). Pub/Sub Admin, Publisher, Subscriber, and Viewer role definitions were read and pinned on 2026-10-05; reading roles made no grants or service resources. Local tests show denied calls never reach or start the backend. An existing stream checks authorization again before forwarding acknowledgements or delivering messages, so revocation prevents subsequent delivery. These are implementation tests, not recorded GCP authorization evidence.

Each resource project owns one lazy JVM restricted to that project. The emulator owns configurations and queues; the shared index owns resource references; IAM owns policies. Control-plane writes publish index changes only after backend success. An uncertain create/delete is reconciled by reading the backend; an unresolved result blocks the scope until reset. Reset cancels sessions, stops the child, then clears only that project's Pub/Sub resources and policies. Project bootstrap policies survive. Sessions allow a publisher to proceed while a pull/stream waits.

Push, cross-project subscriptions, ordering, exactly-once delivery, schemas, KMS, filters, dead-letter policies, configurable retries, retention/expiry fidelity, snapshots/seek, updates, and unknown protobuf fields return explicit coverage errors. Topic/subscription `testIamPermissions` and REST transport are not registered. Only named local callers are supported. Publication requests have a 1 MiB local limit. JVMs use a 256 MiB heap and additional native memory; the runtime is process-local and does not persist messages.

## Run the provider

Use a private runtime checkout or Moo machine with Java 17, the pinned Go toolchain, and `python3 scripts/install_pubsub.py`. Generate an identity bundle with `scripts/create_local_identity.py`; configure the corresponding project and a `roles/pubsub.admin` grant to that named local principal in an owner-only IAM file, following [the bootstrap format](iam.md). Then:

```sh
sh scripts/go.sh build -o bin/gcp-local ./cmd/gcp-local
bin/gcp-local -services pubsub \
  -identity-config /absolute/private/server.json \
  -iam-config /absolute/private/iam.json \
  -pubsub-java "$(command -v java)" \
  -pubsub-jar "$PWD/.tools/pubsub/emulator.jar"
```

Select `-services storage,pubsub` to enable both. HTTP identity/diagnostics uses loopback port 4566 and gRPC uses loopback port 4567. Emulator ports are random and private implementation details. A process running on the same host can access loopback; run untrusted applications in a separate network namespace/container that exposes only the gateway. The child inherits no ADC, cloud credentials, proxies, or push endpoints. Core process shutdown removes its temporary directories and processes. Reset is currently an internal lifecycle operation; restarting the core resets all its in-memory state.

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
    scopes=["https://www.googleapis.com/auth/cloud-platform"], request=request)
channel = secure_authorized_channel(
    credentials, request, "127.0.0.1:4567",
    ssl_credentials=grpc.local_channel_credentials())
publisher = pubsub_v1.PublisherClient(transport=PublisherGrpcTransport(channel=channel))
```

Use `SubscriberGrpcTransport(channel=channel)` for the subscriber. Set `GOOGLE_APPLICATION_CREDENTIALS` to the generated local profile and `GOOGLE_CLOUD_PROJECT` to its synthetic project. The local gRPC credential API is [experimental and supports local TCP call credentials](https://grpc.github.io/grpc/python/grpc.html#grpc.local_channel_credentials); it does not encrypt TCP. This verified setup is limited to numeric loopback connections, not a general remote TLS configuration. The [actual SDK test](../internal/providers/pubsub/testdata/sdk.py) also confines credential refresh and API traffic to loopback and bounds waits.

## Provider verification

```sh
.venv-tools/bin/python -m pip install -r requirements-pubsub.txt
sh scripts/check_pubsub.sh
```

The race-enabled Go suite and real Python SDK run with only loopback available. Tests exercise resource grants and stale policy etags, grant revocation during an active stream, project isolation, nack/redelivery, half-close, client cancellation, reset, shutdown, and uncertain control-plane responses. The runner writes `.artifacts/pubsub-provider/results.json` with test outcomes, source/backend hashes, SDK versions, and an explicit **unverified** cloud-fidelity status. CI also reruns the existing Storage recordings and broader core tests. No Pub/Sub project provisioning, API enablement, grants, or real-GCP service execution has occurred.

## Backend verification

Install Java 17 in the task machine, then run from its synced checkout:

```sh
python3 scripts/install_pubsub.py
sh scripts/check_pubsub_backend.sh
```

The installer downloads Google's emulator component directly, verifies the archive and JAR hashes in [the pin](../toolchains/pubsub.json), and installs only the JAR. It does not install or authenticate gcloud. The runner requires Linux network/PID namespaces and runs with loopback only; it needs no GCP credentials. CI installs dependencies before blocking network access.

The probe exercises topic/subscription creation, publication, pull, acknowledgement, bidirectional StreamingPull, half-close, cancellation, shutdown, and restart. It demonstrates that invalid bearer credentials are accepted upstream and that IAM policy RPCs return `UNIMPLEMENTED`; the provider must supply authorization before forwarding. Restart loses resources, so this backend has no persistence claim. A JSON artifact includes the exact emulator hash, Java version, protocol results, and startup/RSS/shutdown samples.

The first successful Moo run on 2026-10-05 used emulator 0.8.36 and OpenJDK 17.0.20.1 in a 2 CPU/2 GiB task machine. Two warm starts took 0.257 and 0.254 seconds; RSS after the probes was 129868 and 114468 KiB; shutdown took 0.008 and 0.034 seconds. These are individual development measurements, not throughput or performance guarantees. Guest-internal gRPC passed; host-forwarded gRPC remains part of [#25](https://github.com/evanmarshall/gcp-local/issues/25).

Google documents emulator gaps including IAM, retention/expiration, and some update operations in [the emulator guide](https://docs.cloud.google.com/pubsub/docs/emulator). Backend success establishes no GCP permission fidelity. Unsupported configurations must remain explicit when the provider is added; push delivery and identity belong to the provider rather than the unauthenticated backend.
