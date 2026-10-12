# Validation

Conformance scenarios run once against a real GCP sandbox to record behavior, then run against gcp-local in CI to check it still matches. The runner is a pytest plugin in `gcp_validation/` built on `localstack-snapshot`. Recordings live in `tests/conformance/recordings/<id>/`; [coverage](coverage.md) lists them per service.

## Modes

| Mode | Required flags | What it does |
| --- | --- | --- |
| `offline` (default) | None | Runs harness tests only. Cloud scenarios don't execute. Python sockets are blocked except in `harness_loopback` tests. |
| `local` | `--gcp-project`, `--gcp-endpoint` | Runs scenarios against a loopback endpoint and compares with the reviewed recording. Fails if the recording is missing or stale. Accepts no real credentials. |
| `record` | `--gcp-profile` (or `--gcp-project`, `--gcp-credentials-file`, `--gcp-live-scope`), `--gcp-allow-live` | Runs against real GCP and writes an unreviewed candidate after tests and cleanup succeed. |
| `revalidate` | Same as `record` | Runs against real GCP and compares with the reviewed recording. Never rewrites it. |

Live modes need an authorized sandbox and cost scope ([sandbox setup](gcp-sandbox.md)). `--gcp-allow-live` is not a spending cap. Credentials are selected explicitly; the runner never reads ambient ADC, environment project settings, or `.env` on its own.

## Run

Offline harness:

```sh
.venv-tools/bin/python -m pytest
```

Local comparison against the recordings, inside a Linux network namespace with only loopback:

```sh
sh scripts/check_storage.sh          # Storage scenarios and the Storage SDK slice
sh scripts/check_rest_recordings.sh  # every other REST recording
```

Both build `tools/storage-conformance`, a test-only binary that maps the runner's synthetic `gcp-local-test:fixture`, `:allowed`, and `:denied` bearer tokens to principals. Everything else in it is the production gateway, IAM, provider, and backend. The production binary rejects those tokens. Pub/Sub has its own runner ([Pub/Sub](pubsub.md)).

Results go to `.artifacts/validation/junit.xml` and `coverage.json`; use `--gcp-artifacts PATH` to separate concurrent runs. Outcomes are pass, divergence, unsupported, unverified, skipped, inconclusive, and error. Only a reviewed GCP comparison counts as verified.

Record and revalidate, after authorization:

```sh
.venv-tools/bin/python -m pip install -r requirements-cloud.txt
.venv-tools/bin/python -m gcp_validation.sandbox doctor --profile /absolute/path/to/.env
.venv-tools/bin/python -m pytest tests/conformance/test_storage.py -q \
  --gcp-mode record --gcp-profile /absolute/path/to/.env --gcp-allow-live \
  --gcp-artifacts .artifacts/storage-record
```

Repeat with `--gcp-mode revalidate` and a fresh artifacts directory.

## Network isolation

`scripts/run_isolated.py` runs a server and a test command in a Linux network namespace with only loopback. It waits up to 20 seconds for the readiness URL, enforces a deadline, kills both process groups, and strips cloud credential and proxy variables. Missing local methods fail; nothing falls back to Google. It needs Linux, `unshare`, `iproute2`, and root or namespace permission. On macOS use moo. `pytest-socket` alone is not isolation: subprocesses, Go programs, and containers bypass it.

## Write a scenario

1. Mark the test `gcp_scenario(id=..., service=..., transport=..., versions={"sdk": ..., "provider": ..., "backend": ...})`. IDs are unique, lowercase, and hyphenated. Use `not-applicable` for versions that don't apply.
2. List repository files that shape the request or normalization in `contract_files`. The runner hashes them with the test module; changing either invalidates the recording.
3. Configure the client from `gcp_target`. Model setup and cleanup as fixtures that depend on `gcp_snapshot`.
4. Pass selected public fields, as JSON values, to `gcp_snapshot.match("operation-name", observation)`. Never pass whole SDK or HTTP objects. The adapter rejects credential fields, bearer tokens, and private keys, but it is not a sensitive-data classifier.
5. Assert etags, lifecycle, and side effects in the test too; normalization must not hide them.
6. Raise `Unsupported`, `Unverified`, or `Inconclusive` from `gcp_validation.evidence` when they apply. A mismatch raises `Divergence`. Failed cleanup is an error and publishes no candidate.

The plugin compares or persists only after every teardown succeeds. Don't call snapshot finalization yourself. It ignores the upstream `SNAPSHOT_UPDATE` and `SNAPSHOT_RAW` flags.

## Promote a recording

A `record` run writes `case.snapshot.json` and `provenance.json` with `"reviewed": false` under the artifacts directory. Inspect the behavior and sanitization, copy both files to `tests/conformance/recordings/<id>/`, and set only `reviewed` to `true`. Never edit expected values to make the emulator pass. Synthetic evidence is labeled `source: synthetic` and never counts.

## Interrupted live runs

Each live Storage scenario takes a per-project lock and writes an owner-readable manifest under `~/.local/state/gcp-local/PROJECT_ID/` before it mutates anything. To clean up after a killed run:

```sh
.venv-tools/bin/python -m gcp_validation.storage cleanup \
  --profile /absolute/path/to/.env --manifest /absolute/path/to/RUN_ID.json --allow-live
```

Cleanup touches only resources named in the manifest. The lock is per host; coordinate the one-bucket sandbox across machines yourself.

## Storage SDK slice

`tests/conformance/test_storage_sdk.py` runs three upstream tests from `google-cloud-storage==3.10.1` at [revision fce6a6b](https://github.com/googleapis/python-storage/tree/fce6a6bf4561eabc939c972bb92282be86fb6aec). The [vendored manifest](../third_party/python-storage/manifest.json) pins file hashes and lists fixture adaptations.

| Upstream ID (`tests/system/test_blob.py::`) | Behavior |
| --- | --- |
| `test_blob_direct_write_and_read_into_file` | Multipart upload, metadata reload, download |
| `test_blob_download_w_generation_match` | Generation preconditions |
| `test_blob_crud_w_etag_match` | Etag match and not-match (412, 304) |

The wrapper also lists with page size one and deletes with generation guards. `requirements-sdk.txt` is a contract file for this recording; changing it invalidates the recording. Other languages, resumable uploads, retries, ACLs, and retention are not covered.

## Limits

- The manual cloud workflow (`cloud-conformance.yml`) runs only `test_storage.py` and `test_storage_authorization.py`. Run other live scenarios by hand.
- Permission minimization uses a separate runner ([permission discovery](permission-discovery.md)).
- Keep recordings unchanged during dependency upgrades; revalidation is a separate live task.
