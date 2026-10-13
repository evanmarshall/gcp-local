# Terraform compatibility

18 Terraform acceptance cases pass against gcp-local across 14 services, with the released google provider 8.5.0, Terraform 1.16.5, and terraform-plugin-testing 1.15.0. Only 3 are extracted from the provider's upstream tests; the other 15 are written in gcp-local. The [selection manifest](../../tests/terraform/selection.json) lists both sets and pins the upstream source hashes; the [adapter audit](../../tests/terraform/audit.md) lists every change to the upstream cases.

## Extracted from upstream (3)

Extracted by `scripts/dev/generate_terraform_tests.py` from `hashicorp/terraform-provider-google` at `70539b4b`, with the original text in [`third_party/terraform-provider-google`](../../third_party/terraform-provider-google/manifest.json).

| Test ID | Upstream file | What passes locally |
| --- | --- | --- |
| `TestAccStorageBucket_basic` | `google/services/storage/resource_storage_bucket_test.go` | Create, read, project-number and timestamp fields, regional defaults, `project/bucket` import, refresh, empty second plan, destroy |
| `TestAccStorageBucket_labels` | `google/services/storage/resource_storage_bucket_test.go` | Add, change, and remove labels with import checks and an empty plan after each step |
| `TestAccStorageObject_recreate` | `google/services/storage/resource_storage_bucket_object_test.go` | Small multipart upload, CRC32C checks, replacement on content change |

## Written locally (15)

These have no upstream counterpart. Their configurations and assertions are gcp-local's own, in `tests/terraform/runner`.

| Test ID | What passes locally |
| --- | --- |
| `TestLocalKMSKeyRingCryptoKey` | Key ring; crypto key with primary version and a 90-day `rotation_period`; key ring and key IAM members; rotation cleared and versions destroyed with the key |
| `TestLocalSecretManagerSecretAndVersion` | Pub/Sub topic with the Secret Manager service agent as publisher; secret with replication policy, `topics`, and `rotation`; version with payload; a second secret with `ttl`; `version_aliases` added in a second step; 404 after destroy |
| `TestLocalCloudTasksQueueLifecycle` | Queue with rate limits, PATCH update, delete, 404 after destroy |
| `TestLocalCloudSchedulerJob` | Pub/Sub target job with a time zone and retry config, PATCH of description and schedule, pause, empty plan, 404 after destroy |
| `TestLocalFirestoreDocument` | Document create, refresh, empty plan, 404 after destroy |
| `TestLocalBigQueryDatasetAndTable` | Dataset and table with a schema, destroy |
| `TestLocalBigQueryPartitionsAndAccess` | Day-partitioned and clustered table with `require_partition_filter`, range-partitioned table, dataset default partition expiration, inline `access` blocks with a group, `google_bigquery_dataset_access` for a user and an authorized dataset, removing the user entry, empty plans, destroy |
| `TestLocalSpannerInstanceAndDatabase` | Instance with processing units, database with DDL, empty plan, 404 after destroy |
| `TestLocalPubSubTopicAndSubscription` | Topic and pull subscription over REST with retention, empty plan, 404 after destroy |
| `TestLocalComputeNetworkAndSubnetwork` | Custom network and regional subnetwork, empty plan, 404 after destroy |
| `TestLocalArtifactRegistryRepository` | Docker repository through a long-running operation, empty plan, 404 after destroy |
| `TestLocalServiceAccount` | Service account create, read, empty plan, destroy |
| `TestLocalProjectIamCustomRole` | Project custom role and a project binding to it, title and permission update, empty plan, role reads back `deleted` after destroy |
| `TestLocalCloudRunV2Service` | Cloud Run v2 service create, a traffic split with a tag and scaling, a traffic-only update to internal ingress, 404 after destroy |
| `TestLocalFolderAndFolderIam` | Folder create through a polled operation under a seeded folder, folder IAM member, rename, import, empty plan, `DELETE_REQUESTED` after destroy |
| `TestLocalProjectIamPolicy` | Project IAM policy replace; runs last because it replaces the whole policy |

These cases prove the released provider accepts gcp-local's responses. They are not recorded from real GCP and never run against it; the API outcomes they rely on are recorded separately ([coverage](../coverage.md)).

## Run the suite

On Linux or in moo, with permission to create network and PID namespaces:

```sh
python3 scripts/install/go.py
python3 scripts/install/terraform.py
sh scripts/check/terraform.sh
```

The installer verifies checksums and keeps Terraform and the provider in `.tools/terraform`, served from a filesystem-only mirror. Every case runs in a loopback-only namespace. Results go to `.artifacts/terraform/results.json`; a skipped or unrun case counts as a failure.

The check also runs a negative control: a fixture with a wrong bucket RPO must fail the upstream `TestAccStorageBucket_basic` assertion (results in `.artifacts/terraform-fault`).

## Provider configuration

The suite authenticates with a build-tagged test token that the production binary rejects; it needs no ADC or GCP account. Each case sets its service's custom endpoint, for example:

```hcl
provider "google" {
  project                         = "gcp-local-test"
  access_token                    = "gcp-local-test:fixture" # test fixture only
  storage_custom_endpoint         = "http://127.0.0.1:4566/storage/v1/"
  universe_domain                 = "gcp-local.invalid"
  request_timeout                 = "2s"
  add_terraform_attribution_label = false
}
```

Don't copy the test token into application configuration. For your own Terraform, use local ADC as in the [workflow](../guides/workflow.md#terraform-authentication).

## Limits

- Storage: only private us-central1 STANDARD buckets with uniform access, enforced public access prevention, no soft delete, and small objects. Labels are the only bucket update tested.
- Imports need `project/bucket`; bare bucket names are not supported.
- Operation polling is tested only by the Artifact Registry and folder cases; other selected mutations are synchronous.
- Not tested: other provider versions, lifecycle rules, object versioning, resumable uploads, Anywhere Cache, and resources not in the table.
