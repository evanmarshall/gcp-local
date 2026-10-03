# Validation runners

The foundation uses pytest and the same snapshot package pinned by archived LocalStack. Python is the test toolchain, not a choice of emulator implementation language. There are currently no real-GCP recordings or supported service scenarios. Passing harness tests establishes that the tooling works; the coverage report correctly shows zero verified cloud scenarios.

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

The live credential file is selected explicitly and is never read by the foundation itself; the service adapter must load exactly that file. No project or credential discovery uses environment variables or metadata services. `.env` is for [sandbox setup inputs](gcp-sandbox.md), not an automatic mode switch. Real service adapters, endpoint selection, authentication, and cloud verification are follow-on work in #9/#21.

Example local invocation, once a matching service and baseline exist:

```sh
.venv-tools/bin/python -m pytest tests/conformance \
  --gcp-mode local --gcp-project gcp-local-test \
  --gcp-endpoint http://127.0.0.1:4566
```

Live invocations add `--gcp-mode record` or `revalidate`, the dedicated sandbox project, an explicit ADC file, a description of the approved resource/cost scope, and `--gcp-allow-live`. A flag is not a spending cap. Do not run live until the sandbox scope is authorized. Credentials stay out of the moo baseline, test parameters, logs, and Git.

`pytest-socket` guards Python socket use, including collection and test setup, in this runner. It is not a security sandbox: subprocesses, Go programs, native networking, and containers require their own egress isolation before upstream suites are enabled. Restrict live recording jobs to trusted code. Network override flags/markers are rejected; `harness_loopback` is only for synthetic harness tests.

## Write a scenario

Mark cloud scenarios with `gcp_scenario(id=..., service=..., transport=..., versions=...)`. Supply `versions` with `sdk`, `provider`, and `backend` string values; use `not-applicable` where appropriate. IDs are unique lowercase hyphenated names. The runner hashes the source module as the scenario revision so a changed contract requires a reviewed baseline.

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
