# Terraform acceptance selection

The first candidate slice is pinned to [terraform-provider-google v8.5.0, revision 70539b4](https://github.com/hashicorp/terraform-provider-google/tree/70539b4b7bb9ea096987422d9596186417b73f51). Its source uses MPL-2.0, Go 1.26.0, and `terraform-plugin-testing v1.15.0`. The [selection manifest](../tests/terraform/selection.json) records exact IDs, inspected source hashes, required APIs, and remaining adapter work. Nothing in this selection launches Terraform or contacts a cloud endpoint.

| Exact test ID in `./google/services/storage` | Why selected |
| --- | --- |
| `TestAccStorageBucket_basic` | Bucket create/read, project number/default metadata, import by name and project/name, destroy |
| `TestAccStorageBucket_labels` | Bucket label updates and removals, repeated import/state checks |
| `TestAccStorageObject_recreate` | Small file upload, CRC32C checks, replacement after content changes, destroy |

The provider's own Go acceptance runner remains the test framework. A passing unchanged second plan must be added explicitly; selecting these upstream tests is not evidence that our provider already produces a clean plan. Local execution, refresh, polling, stale-etag coverage, and results remain pending #8. These tests do not require adding a separate Terratest framework.

## Endpoint and identity contract

The provider exposes `storage_custom_endpoint`; the future local adapter must set the JSON API base explicitly, including `/storage/v1/`, for both provider factories and direct clients used by destroy checks. `access_token` supplies the synthetic local principal (`gcp-local-test:fixture` for resource setup). Set a synthetic project explicitly. Do not use local ambient ADC or service-account impersonation, which can make real token requests even when the Storage endpoint is overridden. For live runs, select the existing explicit keyless fixture profile; do not insert real access tokens into committed HCL, logs, or test fixtures.

Upstream factories construct muxed SDK/framework providers, and helpers can obtain separate clients. Their transitive credential/configuration path still needs an adapter audit before execution. A Storage endpoint override alone is insufficient to authorize this runner. The Linux namespace launcher from #23 must contain Terraform, the Go test process, direct-client helpers, and the local provider. Download and checksum-pin the Terraform executable and module/build dependencies **before** entering the namespace; the test phase must not fetch providers or modules from the internet. The Terraform CLI version is not selected yet.

## Fixture changes required before live use

The selected upstream HCL uses the multi-region `US`, omits our cleanup-related bucket settings, and calls `t.Parallel()`. Do not run it unmodified against the approved sandbox. Adapt those fixture constructors to `us-central1`, uniform bucket access, enforced public access prevention, soft-delete duration zero, no retention/versioning, and run the exact allowlist with `-parallel=1`. Track owned resource intent before mutation and retain a cleanup manifest if a child exits or is killed. Global sweepers, retention-lock tests, Autoclass, large-object, KMS, ACL, and additional-service cases are outside this slice.

Preserve upstream assertions while adding an explicit unchanged second plan, bounded cleanup, and machine-readable outcomes. Keep the provider's `VCR_MODE`/`VCR_PATH` unset: raw upstream cassettes are not our sanitized evidence format. Source inspection establishes an implementation plan, not compatibility or IAM fidelity. Additional error/default/operation observations must be recorded through the approved GCP harness; the current Storage lifecycle/authorization baselines remain separate evidence.
