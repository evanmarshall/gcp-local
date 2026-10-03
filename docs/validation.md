# Validation runners

The foundation uses pytest and the same snapshot package pinned by archived LocalStack. Python is the test toolchain, not a choice of emulator implementation language. The bounded Storage lifecycle, same-project authorization, and selected Python SDK scenarios have real-GCP baselines recorded and revalidated on 2026-10-03 UTC. See [the authorization matrix](authorization-baseline.md) for exact coverage and the pending cross-project recording. Ordinary offline checks still report that scenario as unverified because they do not execute it; harness passes do not count as service coverage.

## Run offline checks

```sh
python3 -m venv .venv-tools
.venv-tools/bin/python -m pip install -r requirements-tools.txt -r requirements-validation.txt
.venv-tools/bin/python -m pytest
```

Inside an existing moo task machine, run the dependency install and pytest commands from `/srv/app`. The host owns source edits and Git checks. New dependencies need installing in existing machines; they do not update from a changed lockfile automatically.

Results are written to `.artifacts/validation/junit.xml` and `coverage.json`. Use `--gcp-artifacts PATH` for separate concurrent runs. The JSON report separates harness tests from conformance, retains each outcome, and counts only reviewed GCP comparisons as verified. It includes current scenario SDK/provider/backend declarations and recorded baseline versions. Statuses are pass, divergence, unsupported, unverified, skipped, inconclusive, and error. Unknown behavior, skips, expected failures, and fresh recording candidates never become verified passes.

## Explicit execution modes

| Mode | Required target configuration | Behavior |
| --- | --- | --- |
| `offline` (default) | None | Run harness checks; cloud scenarios are unverified and do not execute. Python sockets are disabled except explicitly marked synthetic loopback tests. |
| `local` | `--gcp-project`, `--gcp-endpoint` | Permit loopback TCP targets only; compare against reviewed GCP evidence and fail if it is absent or stale. No real credentials accepted. |
| `record` | `--gcp-project`, `--gcp-credentials-file`, `--gcp-live-scope`, `--gcp-allow-live` | Allow an explicitly authorized live scenario; write a new, unreviewed candidate only after all tests and cleanup succeed. |
| `revalidate` | Same explicit live inputs | Compare against a reviewed recording; report a successful cloud validation date without silently rewriting the baseline. |

The live credential file is selected explicitly. The Storage adapter loads only the identities in the selected sandbox profile. No project or credential discovery uses environment variables or metadata services. `.env` is for [sandbox setup inputs](gcp-sandbox.md), not an automatic mode switch. The Storage adapter selects the fixed Google Storage endpoint in live modes and the explicit loopback endpoint in local mode. GitHub WIF and reusable project provisioning remain in #21.

Example local invocation, once a matching service and baseline exist:

```sh
.venv-tools/bin/python -m pytest tests/conformance \
  --gcp-mode local --gcp-project gcp-local-test \
  --gcp-endpoint http://127.0.0.1:4566
```

Live invocations add `--gcp-mode record` or `revalidate`, the dedicated sandbox project, an explicit ADC file, a description of the approved resource/cost scope, and `--gcp-allow-live`. A flag is not a spending cap. Do not run live until the sandbox scope is authorized. Credentials stay out of the moo baseline, test parameters, logs, and Git.

`pytest-socket` guards Python socket use, including collection and test setup, in this runner. It is not a security sandbox: subprocesses, Go programs, native networking, and containers require their own egress isolation before upstream suites are enabled. Restrict live recording jobs to trusted code. Network override flags/markers are rejected; `harness_loopback` is only for synthetic harness tests.

## Write a scenario

Mark cloud scenarios with `gcp_scenario(id=..., service=..., transport=..., versions=...)`. Supply `versions` with `sdk`, `provider`, and `backend` string values; use `not-applicable` where appropriate. IDs are unique lowercase hyphenated names. The runner hashes the source module and any repository-relative `contract_files` as the scenario revision, so changed adapter/normalization behavior requires a reviewed baseline.

Use `gcp_target` to configure the service client explicitly. Send selected public fields to `gcp_snapshot.match("operation-name", observation)`. Assert lifecycle, etag, and side effects in the test as well. Model resource setup/cleanup with pytest fixtures; service fixtures should depend on `gcp_snapshot` when they need the evidence preflight. The plugin compares or persists only after every teardown succeeds. Do not call snapshot finalization yourself.

Do not pass whole SDK/HTTP objects or raw request/response dumps. The adapter accepts JSON values and rejects common credential fields, bearer tokens, private keys, and nonfinite numbers before buffering them. This check supplements scenario-specific field selection; it is not a general sensitive-data classifier. Request-shape normalization and service-specific redaction belong in the recorder (#9).

Reviewed baselines live under `tests/conformance/recordings/SCENARIO_ID/` with `case.snapshot.json` and `provenance.json`. Recording produces unique candidate directories in the artifacts folder, with `reviewed: false`, source, versions, time, cleanup status, and a snapshot hash. After inspecting the observed behavior and sanitization, copy the pair to the baseline directory and set only the review flag. Never edit expected response values to make a local implementation pass. The adapter rejects synthetic evidence in cloud scenarios and does not honor upstream `SNAPSHOT_UPDATE`/`SNAPSHOT_RAW` environment flags.

Raise `Unsupported`, `Unverified`, or `Inconclusive` from `gcp_validation.evidence` when those outcomes apply. They fail an explicitly selected suite and retain their classification in the report. Snapshot mismatches raise `Divergence`. Failed cleanup is an error and publishes no candidate. Synthetic fixture tests always label their provenance `source: synthetic` and cannot establish real-cloud verification.

## Open-source components and next adapters

| Component | Use and license | Status |
| --- | --- | --- |
| [pytest](https://github.com/pytest-dev/pytest) 9.0.2 | Test lifecycle and JUnit; MIT | Installed |
| [localstack-snapshot](https://github.com/localstack/localstack-snapshot) 0.3.3 | Snapshot comparison/persistence; Apache-2.0 | Installed through a small adapter, without its AWS-oriented pytest plugin |
| [pytest-socket](https://github.com/miketheman/pytest-socket) 0.8.1 | Python network guards; MIT | Installed |
| [Google SDK suites](https://github.com/googleapis/python-storage/tree/main/tests/system) | Selected client/transport tests; Apache-2.0 | Allowlist and pin source in [#23](https://github.com/evanmarshall/gcp-local/issues/23) |
| [terraform-provider-google](https://github.com/hashicorp/terraform-provider-google) | Existing Go acceptance runner; MPL-2.0 | Pin source/provider/test IDs in [#10](https://github.com/evanmarshall/gcp-local/issues/10) |
| [Terratest](https://github.com/gruntwork-io/terratest) | Optional cross-service orchestration; Apache-2.0 | Evaluate for #14 when orchestration is needed |
| [Schemathesis](https://github.com/schemathesis/schemathesis) | Local REST property tests; MIT | Deferred until a supported OpenAPI contract exists; not a Discovery/gRPC/IAM oracle |

All installed dependencies, including transitives, are pinned in `requirements-validation.txt`. Transitive licenses: cachebox, deepdiff, iniconfig, orderly-set, and pluggy are MIT; jsonpath-ng is Apache-2.0; packaging is Apache-2.0 OR BSD-2-Clause; Pygments is BSD-2-Clause. PyYAML in the contributor requirements is MIT. No upstream suite is vendored or claimed as running yet.

The runner versions follow [the archived LocalStack dependency list](https://github.com/localstack/localstack/blob/8b9a79f05846835cf4dff63ab7eefdde9df83783/requirements-test.txt). Update pins in reviewed changes, run dependency checks and the complete synthetic harness on Python 3.11/3.12, and explicitly review the private SnapshotSession methods used by the adapter. Keep GCP baselines unchanged during dependency upgrades; revalidation is a separate live task.


## Bounded Storage lifecycle

The scenario in `tests/conformance/test_storage.py` creates one private, regional Standard bucket with soft delete and versioning disabled. A separate fixture identity manages the bucket and grants Object User to the allowed caller. The scenario uploads a fixed synthetic payload, reads and lists it, checks a failed conditional overwrite preserves content/generation/etag, checks a denied upload leaves the object list unchanged, deletes the object, and verifies absence. Cleanup removes the owned bucket even when the scenario fails.

Install the optional, pinned Google authentication dependencies for live runs:

```sh
.venv-tools/bin/python -m pip install -r requirements-cloud.txt
.venv-tools/bin/python -m gcp_validation.sandbox doctor --profile /absolute/path/to/.env
.venv-tools/bin/python -m pytest tests/conformance/test_storage.py -q \
  --gcp-mode record --gcp-profile /absolute/path/to/.env --gcp-allow-live \
  --gcp-artifacts .artifacts/storage-record
```

`--gcp-profile` supplies the explicit project, allowed credential path, and owner-provided scope; conflicting target overrides fail before resource creation. The initial local profile contract requires separate fixture, allowed, and denied ADC files from the dedicated project. Region and resource bounds are recorded in provenance, together with source-file and snapshot integrity hashes. The free-text authorization description and actual project/identity names are not persisted in snapshots.

Inspect the new candidate pair before promoting it using the review procedure above. Then run the same command with `--gcp-mode revalidate` and a separate artifacts directory. Revalidation compares the same contract without rewriting the baseline. Local comparison uses the earlier local-mode command and the committed recording; it cannot load the live profile, use ambient proxies, or follow redirects to cloud endpoints.

The initial local adapter sends explicit synthetic `gcp-local-test:fixture`, `:allowed`, and `:denied` bearer identities. The controlled protocol server used by offline harness tests recognizes these identities; the actual emulator identity protocol remains #6. This is not a working Storage emulator or SDK compatibility certification. The controlled-server recordings are always labeled synthetic and never count as GCP evidence.

Each live scenario holds a per-project process lock and rejects an already occupied sandbox before creating another bucket. An owner-readable manifest is written under `~/.local/state/gcp-local/PROJECT_ID/` before mutations. Normal cleanup verifies both the numeric project and unique bucket ownership label. For an interrupted run, explicitly select its manifest:

```sh
.venv-tools/bin/python -m gcp_validation.storage cleanup \
  --profile /absolute/path/to/.env --manifest /absolute/path/to/RUN_ID.json --allow-live
```

Cleanup only visits manifest-owned object names in the matching bucket. It does not sweep unrelated buckets or repair ownership mismatches. A failed cleanup leaves the manifest pending and prevents candidate publication. Different developer machines must still coordinate use of the one-bucket sandbox; the local process lock is not a distributed lock.

The authentication lane reuses [google-auth](https://github.com/googleapis/google-auth-library-python) 2.56.2 (Apache-2.0), with all transitive versions pinned in `requirements-cloud.txt`. Its additional dependency licenses are: Requests Apache-2.0; urllib3 and charset-normalizer MIT; cffi MIT-0; cryptography Apache-2.0 OR BSD-3-Clause; certifi MPL-2.0; idna BSD-3-Clause; pyasn1 and pyasn1-modules BSD-2-Clause; pycparser BSD-3-Clause. Update these pins with the offline suite and explicit live revalidation. Cloud CLI bundles are not imported by the repository runner.


The first reviewed [Storage recording](../tests/conformance/recordings/storage-object-lifecycle/provenance.json) was revalidated successfully on 2026-10-03 UTC. It covers only this JSON API lifecycle and one denied-create case. There are no emulator, upstream SDK-suite, Terraform, broader IAM, or cross-project coverage claims. The controlled-server comparison is a runner test, even when it matches the GCP baseline; its temporary report is not published as emulator coverage.


## Selected upstream SDK suite

Install `requirements-sdk.txt` to run the pinned Google Python Storage slice; see [SDK compatibility](sdk-compatibility.md) for the source pin, exact upstream IDs, fixture adaptations, and coverage limits. The default harness CI installs these dependencies. Real SDK execution selects `tests/conformance/test_storage_sdk.py` through the same explicit profile and live flag as other scenarios. It uses the same owned Storage fixture and resource limits.

Local upstream tests require process-level network isolation. Run the future provider and its tests together in the Linux namespace launcher described in that guide. A Python socket guard alone is not accepted for local SDK conformance. This also isolates helper subprocesses. No fallback to a networked runner or real GCP exists when local methods are absent.

The [Terraform acceptance selection](terraform-compatibility.md) pins candidate provider tests and records the remaining adapter work. It is not yet executable or certified.
