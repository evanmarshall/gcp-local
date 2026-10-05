# Pub/Sub

Issue [#12](https://github.com/evanmarshall/gcp-local/issues/12) is in progress. The official emulator is pinned and tested as a backend; the runtime does not yet register a Pub/Sub provider. No Pub/Sub scenario has been recorded against GCP.

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
