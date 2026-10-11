# Terraform acceptance adapter audit

The runner uses `terraform-plugin-testing v1.15.0` and the unmodified, checksum-pinned Google provider **8.5.0** executable. It extracts eight exact functions from source revision `70539b4b7bb9ea096987422d9596186417b73f51`; source hashes, the original function text, and MPL-2.0 attribution are committed in `third_party/terraform-provider-google`. `scripts/generate_terraform_tests.py --check` checks the reproducible adaptation. Terraform CLI **1.16.5** and provider release archives are pinned for Linux arm64/amd64 and macOS arm64. The CLI is source-available under BUSL-1.1; the acceptance framework and Google provider are MPL-2.0. Executables are downloaded to ignored `.tools`, never vendored.

## Executed helpers and changes

| Upstream helper/path | Local execution |
| --- | --- |
| `acctest.VcrTest` | `resource.Test` with external released provider; no VCR, HTTP logging, sweepers, or package-wide TestMain |
| `ProtoV5ProviderFactories` | `ExternalProviders` selects exactly `hashicorp/google =8.5.0` from the installed filesystem mirror |
| `AccTestPreCheck`, envvar project lookup | Explicit synthetic project, fixed test-only credential, namespace and configuration checks; ambient cloud configuration is removed |
| `TestBucketName`, `RandInt`, `t.Parallel` | Cryptographically unique owned name, manifest before mutations, serial tests, one bucket at a time |
| Bucket HCL constructors | Explicit resource project, us-central1, uniform access, enforced public access prevention, zero soft-delete duration, ownership default label, short timeouts |
| Bare-name import | Project-qualified import to avoid the provider's auxiliary Compute project lookup; import/state equality checks remain |
| Basic test's multi-region RPO assertion | `TestCheckNoResourceAttr`: the recorded single-region GCP response omits RPO and Terraform omits the state attribute |
| Temporary source file and CRC32C helper | Private test directory; original checksum algorithm and expected content assertions retained |
| Destroy/check clients | Explicit loopback Storage client with no proxy/redirects, bounded response/time limits, and actual 404 verification |
| Post-apply lifecycle | Additional refresh and unchanged plan checks after each configuration, including label removals and file replacement |

The original bucket/object attribute checks remain except for the documented regional RPO assertion.
The locally adapted `TestAccSpannerInstanceAndDatabase` is not an extracted upstream function: it drives `google_spanner_instance` and `google_spanner_database` with the same pinned provider through `spanner_custom_endpoint`, fixture credentials, and the modeled regional configs. Its assertions (names, processing units, post-destroy 404) and destroy probe follow the same owned-name and isolation rules.
 Provider `terraform_labels` is excluded from import comparison because the adapter adds an ownership default label; effective labels remain checked. Import tests cover qualified IDs only. These are adapted upstream cases, not an unmodified provider-wide certification.

## Endpoint audit

`storage_custom_endpoint` supplies `/storage/v1/`; generated Google clients derive the upload path from it. The provider and direct helper clients share the same authenticated gateway. All provider requests remain inside the network namespace.

Provider configuration ordinarily calls Google's user-info endpoint even with a static `access_token`. Explicit `universe_domain = "gcp-local.invalid"` disables that introspection in this pinned provider version. Explicit resource project and project-qualified import avoid the auxiliary Compute project-number lookup. Other endpoints are unsupported and cannot be reached through the isolated network. This configuration is local-only.

Bucket destruction also requests `anywhereCaches/`. The emulator returns explicit unsupported (501), which this provider version intentionally ignores, before listing live objects and deleting the bucket. No Anywhere Cache support or conformance is claimed. Actual lifecycle rules, versioned objects, asynchronous jobs, and polling are outside this slice; selected Storage mutations are synchronous. Stale metageneration and failed-update nonmutation are covered by the separate real-GCP bucket-update recording.

## Isolation and cleanup

The Go test binary, Terraform CLI, provider plugin, direct helper clients, and emulator run inside one Linux network and PID namespace with only loopback. Dependencies and executables are installed before isolation. The CLI has only a filesystem mirror and no direct download fallback. No real ADC or credentials are supplied. Detached subprocesses disappear with the PID namespace after timeout; the isolation smoke verifies this separately from ordinary process-group cleanup.

Cleanup manifests precede resource mutation and persist after failure. Cleanup requires the synthetic project number, exact generated name, and ownership label. It permits at most ten objects and the one selected upstream object name, deletes with generation preconditions, and verifies bucket absence. A failed or skipped test is never reported as passed; machine-readable results identify every selected ID, provider/backend/CLI versions, and explicitly state that Terraform did not run against real GCP.

The negative control injects an incorrect RPO only in the build-tagged test fixture and requires the original acceptance framework to fail on that specific attribute assertion. It runs in a fresh namespace and reports the expected failure separately from the positive suite.
