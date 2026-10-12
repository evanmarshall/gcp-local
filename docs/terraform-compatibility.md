# Terraform compatibility

15 upstream google provider acceptance cases, adapted to gcp-local, pass across 13 services. They use the released google provider 8.5.0, Terraform 1.16.5, and terraform-plugin-testing 1.15.0. The [selection manifest](../tests/terraform/selection.json) pins the test IDs and source hashes; the [adapter audit](../tests/terraform/audit.md) lists every change from upstream.

| Upstream test ID | What passes locally |
| --- | --- |
| `TestAccStorageBucket_basic` | Create, read, project-number and timestamp fields, regional defaults, `project/bucket` import, refresh, empty second plan, destroy |
| `TestAccStorageBucket_labels` | Add, change, and remove labels with import checks and an empty plan after each step |
| `TestAccStorageObject_recreate` | Small multipart upload, CRC32C checks, replacement on content change |
| `TestAccKMSKeyRingCryptoKey` | Key ring; crypto key with rotation period and primary version; versions destroyed with the key |
| `TestAccSecretManagerSecretAndVersion` | Secret with replication policy, version with payload, enable/destroy, 404 after destroy |
| `TestAccCloudTasksQueueLifecycle` | Queue with rate limits, PATCH update, delete, 404 after destroy |
| `TestAccFirestoreDocument` | Document create, refresh, empty plan, 404 after destroy |
| `TestAccBigQueryDatasetAndTable` | Dataset and table with a schema, destroy |
| `TestAccSpannerInstanceAndDatabase` | Instance with processing units, database with DDL, empty plan, 404 after destroy |
| `TestAccPubSubTopicAndSubscription` | Topic and pull subscription over REST with retention, empty plan, 404 after destroy |
| `TestAccComputeNetworkAndSubnetwork` | Custom network and regional subnetwork, empty plan, 404 after destroy |
| `TestAccArtifactRegistryRepository` | Docker repository through a long-running operation, empty plan, 404 after destroy |
| `TestAccServiceAccount` | Service account create, read, empty plan, destroy |
| `TestAccCloudRunV2Service` | Cloud Run v2 service create, read, 404 after destroy |
| `TestAccProjectIamPolicy` | Project IAM policy replace; runs last because it replaces the whole policy |

These cases prove the released provider accepts gcp-local's responses. They are not recorded from real GCP and never run against it; the API outcomes they rely on are recorded separately ([coverage](coverage.md)).

## Run the suite

On Linux or in moo, with permission to create network and PID namespaces:

```sh
python3 scripts/install_go.py
python3 scripts/install_terraform.py
sh scripts/check_terraform.sh
```

The installer verifies checksums and keeps Terraform and the provider in `.tools/terraform`, served from a filesystem-only mirror. Every case runs in a loopback-only namespace. Results go to `.artifacts/terraform/results.json`; a skipped or unrun case counts as a failure.

The check also runs a negative control: a fixture with a wrong bucket RPO must fail the upstream assertion (results in `.artifacts/terraform-fault`).

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

Don't copy the test token into application configuration. For your own Terraform, use local ADC as in the [workflow](workflow.md#terraform-authentication).

## Limits

- Storage: only private us-central1 STANDARD buckets with uniform access, enforced public access prevention, no soft delete, and small objects. Labels are the only bucket update tested.
- Imports need `project/bucket`; bare bucket names are not supported.
- Selected mutations are synchronous; async polling is not tested.
- Not tested: other provider versions, lifecycle rules, object versioning, resumable uploads, Anywhere Cache, and resources not in the table.
