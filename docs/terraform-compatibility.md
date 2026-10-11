# Terraform compatibility

Eleven adapted upstream Google provider acceptance cases now pass across eight services: authenticated Storage, Cloud KMS, Secret Manager, Cloud Tasks, Firestore documents, Spanner instances/databases, Pub/Sub topics/subscriptions, and Compute networks/subnetworks. They exercise the released **google 8.5.0** plugin with **Terraform 1.16.5** and **terraform-plugin-testing 1.15.0**. The [selection manifest](../tests/terraform/selection.json) pins exact IDs, source hashes, and limits; the [adapter audit](../tests/terraform/audit.md) accounts for fixture, credential, helper, endpoint, and assertion changes.

| Upstream test ID | Passing local coverage |
| --- | --- |
| `TestAccStorageBucket_basic` | Create/read, numeric project number and timestamps, regional defaults, project-qualified import, refresh, unchanged plan, destroy |
| `TestAccStorageBucket_labels` | Add/change/remove labels, import/state checks, refresh, unchanged plan after each configuration, destroy |
| `TestAccStorageObject_recreate` | Small multipart upload, upstream CRC32C checks, replacement after file-content change, refresh, unchanged plan, destroy |
| `TestAccKMSKeyRingCryptoKey` | Key ring create/read, crypto key with rotation period and primary version, version destroy on key destroy, post-destroy verification |
| `TestAccSecretManagerSecretAndVersion` | Secret create with replication policy, version add with payload access, enable/destroy cycle, destroy with 404 confirmation |
| `TestAccCloudTasksQueueLifecycle` | Queue create with rate limits, PATCH update to new limits, delete, post-destroy 404 |
| `TestAccFirestoreDocument` | Document create in a collection, refresh, unchanged plan, destroy with 404 confirmation |
| `TestAccSpannerInstanceAndDatabase` | Instance create with processing units, database with DDL, refresh, unchanged plan, destroy with post-destroy 404 |
| `TestAccPubSubTopicAndSubscription` | Topic and pull subscription over the JSON REST surface with retention duration, refresh, unchanged plan, destroy with 404 |
| `TestAccComputeNetworkAndSubnetwork` | Custom network and regional subnetwork over the Compute v1 endpoint, refresh, unchanged plan, destroy with 404 |

The adapter retains the upstream checks except for the documented single-region RPO expectation and import handling for the injected ownership default label. These are serial, owned fixtures with bounded cleanup. No Terraform execution against real GCP is claimed. Seven separate recorded Storage scenarios cover the relevant API outcomes, including multipart request shapes, label updates, stale metageneration rejection, and nonmutation after denied updates. The KMS, Secret Manager, Cloud Tasks, Firestore, Spanner, Pub/Sub, and Compute slices are locally modeled without cloud recordings (their issues track the recording work); the Terraform cases verify each control-plane contract with the released provider, not recorded cloud behavior. Selected mutations are synchronous; asynchronous polling and the remaining services remain unsupported.

## Run the suite

Use moo or Linux with permission to create network/PID namespaces. Install tools and warm the nested module before entering isolation:

```sh
python3 scripts/install_go.py
python3 scripts/install_terraform.py
sh scripts/go.sh -C tests/terraform/runner test -c -o "$PWD/bin/terraform-tests" .
sh scripts/check_terraform.sh
```

The installer verifies release checksums and keeps Terraform plus the Google plugin in ignored `.tools/terraform`. No global Terraform installation is needed. Tests use a filesystem-only provider mirror with no registry fallback. CI runs the same suite and publishes `.artifacts/terraform/results.json` and owned-resource cleanup manifests. Every selected ID has a result; skipped, failed, and unrun cases cannot count as passing.

The check also starts a fresh fixture with deliberately incorrect bucket RPO. It must fail on that upstream attribute assertion. This expected failure is recorded separately under `.artifacts/terraform-fault`; the check fails if the corruption is missed or a different failure masks the assertion.

## Endpoint and identity configuration

The acceptance runner needs **no ADC or GCP account**. It uses an explicit synthetic project and a build-tagged credential fixture; the production gateway, IAM evaluator, Storage provider, and backend execute every API request. The fixed test token is rejected by the production binary. Production opaque local ADC is separately exercised by `scripts/check_storage.sh`; native Terraform ADC and refresh are not certified by these acceptance cases.

The adapter supplies this provider configuration automatically:

```hcl
provider "google" {
  project                          = "gcp-local-test"
  access_token                     = "gcp-local-test:fixture" # test fixture only
  storage_custom_endpoint          = "http://127.0.0.1:4566/storage/v1/"
  universe_domain                  = "gcp-local.invalid"
  request_timeout                  = "2s"
  add_terraform_attribution_label   = false
}
```

Each bucket also declares its project explicitly. Imports use `project/bucket`. These choices avoid an auxiliary Compute project lookup; bare-name imports are not supported by this slice. The universe setting prevents this provider version's user-info call during static-token configuration. A Storage endpoint override by itself is insufficient to keep the provider local. Provider factories, direct cleanup clients, the CLI, and plugins all run inside a loopback-only namespace as an independent egress boundary.

For application SDKs, use the private [local ADC bundle](local-identity.md), explicit project, and Storage endpoint. Do not copy the test-only token into application configuration, or point the local runner at real ADC. An ordinary production Terraform setup using local ADC remains a separate integration step; these checks are an acceptance harness for the emulator.

## Limits

Only private us-central1 STANDARD buckets with uniform access, enforced public access prevention, disabled soft delete, and small objects are covered. Labels are the supported bucket update. IAM resources, Compute lookups, other provider versions, persistence, actual lifecycle rules, object versioning, large/resumable uploads, and other GCP resources are not certified. Bucket destruction's optional Anywhere Cache lookup receives explicit unsupported; the pinned provider intentionally ignores that error before normal object/bucket cleanup. No Anywhere Cache support is implied.

The runner preserves MPL-2.0 source attribution for the Google provider tests and uses the existing Go acceptance framework. Terraform CLI itself is BUSL-1.1, downloaded as a development tool; no runtime architecture or second test framework is introduced.
