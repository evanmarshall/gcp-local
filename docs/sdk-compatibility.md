# Python SDK compatibility

## Storage conformance

The first upstream slice uses `google-cloud-storage==3.10.1` at [Google's source revision fce6a6b](https://github.com/googleapis/python-storage/tree/fce6a6bf4561eabc939c972bb92282be86fb6aec), under Apache-2.0. The [vendored manifest](../third_party/python-storage/manifest.json) pins the complete original file hash, unchanged selected function bodies, license, dependencies, and fixture adaptations. Full upstream fixtures are not loaded: their ambient `Client()` construction, versioned/shared buckets, larger payloads, and other services exceed this slice's explicit scope.

| Upstream test ID (`tests/system/test_blob.py::`) | Behavior |
| --- | --- |
| `test_blob_direct_write_and_read_into_file` | Multipart upload, metadata reload, file download, exact payload |
| `test_blob_download_w_generation_match` | Failed generation precondition, successful generation/metageneration download |
| `test_blob_crud_w_etag_match` | Metadata/media etag match and not-match, HTTP 412 and 304 |

The wrapper additionally lists two objects with page size one, deletes them with generation guards, and confirms an empty list. It uses one private bucket, two names mapped to `mybuffer` and `smallfile`, and payloads of 11 and 34 bytes. The bucket proxy records names in the interrupted-run manifest **before** returning native SDK Blob objects. The allowed caller has the scenario's bucket Object User grant; the fixture administrator owns cleanup. No JSON key or upstream service-account fixture is required.

The selected assertions were re-recorded and revalidated against real GCP on 2026-10-05 UTC. Their reviewed recording includes source/adapter hashes, exact SDK versions, selected IDs, stable object metadata, and cleanup. This establishes client behavior for this slice. The actual [Storage provider](storage.md) now matches this recording inside a loopback-only namespace. IAM fidelity comes from the separate authorization baseline. Other languages, resumable uploads, retries under transient failures, ACLs, retention, and the rest of Google's system suite are not certified. Storage testbench does not replace the real IAM oracle.

## Explicit targets

Install `requirements-sdk.txt` plus `requirements-tools.txt`. Use the common [validation modes](validation.md) for `tests/conformance/test_storage_sdk.py`. Live execution requires the explicitly selected sandbox profile and `--gcp-allow-live`; the SDK uses the allowed profile's typed keyless credentials and fixed Google Storage endpoint. Ambient ADC, environment project discovery, proxies, and redirects are not used. Request paths/origins are checked before sending credentials.

For local SDK conformance, run the provider and test process together inside a Linux network namespace with only loopback. `scripts/run_isolated.py` starts the selected provider, waits up to 20 seconds for its explicit loopback readiness URL, runs the test command with a deadline, and terminates both process groups. It strips ambient cloud credential/proxy variables and provides no networked fallback. Missing local methods fail without reaching Google. Subprocesses inherit the namespace; the smoke test verifies IPv4/IPv6 egress denial, working loopback, and timeout termination. This guards accidental egress from selected, inspected tests; it is not a sandbox for hostile privileged code.

The launcher requires Linux, `unshare`, `iproute2`, and permission to create a network namespace. Moo's root guest supports this; Linux CI uses `sudo` with the selected virtualenv. macOS runs must use moo instead of weakening isolation. `sh scripts/check_storage.sh` builds the production binary and a credential-fixture binary, runs real local ADC operations, and compares the provider with all committed Storage scenarios. The credential fixture uses the production gateway/IAM/provider/backend; only token resolution adapts the runner identities.

Additional runner-only checks:

```sh
moo run issue-23 -- 'cd /srv/app && .venv-tools/bin/python scripts/smoke_isolation.py'
moo run issue-23 -- 'cd /srv/app && .venv-tools/bin/python scripts/run_isolated.py -- .venv-tools/bin/python -m pytest tests/validation/test_sdk_runner.py -q'
```

These checks exercise a controlled protocol stub and deliberately corrupt download/precondition responses. They test the runner and upstream assertions; they do not certify an emulator. Ordinary CI runs both these checks and the complete offline suite. Live CI currently selects the original lifecycle/authorization cases; run the SDK scenario explicitly until it is added to that manual allowlist.

The SDK lockfile includes Apache-2.0 Google packages, Apache-2.0 OpenTelemetry, BSD-3-Clause protobuf, and PSF-2.0 typing-extensions. All source/license provenance stays separate from the emulator implementation.

## Pub/Sub local integration

The [Pub/Sub provider](pubsub.md) adds a separate pinned `requirements-pubsub.txt` and `scripts/check_pubsub.sh` runner. It executes the actual Python PublisherClient/SubscriberClient with local ADC refresh over a numeric loopback gRPC channel, tests denied and anonymous calls, publish/pull/ack, streaming callbacks/cancellation, and cleanup. These are repository-owned integration tests, not vendored upstream acceptance cases or GCP recordings. `PUBSUB_EMULATOR_HOST` stays unset because the SDK otherwise selects anonymous credentials. See the provider guide for the verified transport configuration and explicit gaps.
