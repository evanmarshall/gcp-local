# Service coverage

What each service supports and how it is verified. Every service authenticates callers and checks IAM on every request.

- **Methods served**: methods that run. All others return `UNIMPLEMENTED`, except Storage, which returns 400 ([why](storage.md#gaps)).
- **API total**: methods in Google's discovery document for that API version.
- **Data plane**: what an application can do with a resource once it exists.
- **Recordings**: scenarios recorded against real Google Cloud that the emulator matches ([validation](validation.md)). Each covers only its own steps.
- **Terraform**: pinned google provider acceptance cases ([Terraform compatibility](terraform-compatibility.md)).

## Summary

| Key | Service | Backend | Methods served | API total | Data plane for applications | Recordings | Terraform |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `storage` | [Cloud Storage](storage.md) | fake-gcs-server, in-process | 17 | 88 | Yes: object read, write, list, delete, metadata patch, resumable uploads, V4 signed URLs, rewrite; rewrite and XML paths other than signed URLs not recorded | storage-authorization, storage-bucket-policy-only, storage-bucket-updates, storage-cross-project, storage-go-client-projection, storage-iam-conditions, storage-multipart-query-name, storage-object-lifecycle, storage-object-replacement, storage-python-sdk, storage-resumable-signed-url | TestAccStorageBucket_basic, TestAccStorageBucket_labels, TestAccStorageObject_recreate |
| `pubsub` | [Pub/Sub](pubsub.md) | Official emulator | 18 | 46 | Yes: publish, pull, streaming pull, push, ordering, filters, dead-letter topics, retry backoff, exactly-once, snapshots and seek; streaming pull, push, retry backoff, snapshots and seek not recorded | pubsub-delivery-features, pubsub-pull-authorization | TestAccPubSubTopicAndSubscription |
| `firestore` | [Firestore](firestore.md) | Official emulator | 16 | 60 | Yes: documents, subcollections, queries, aggregations, transactions, batch writes, listeners over REST and gRPC; batch writes and listeners not recorded | firestore-document-lifecycle, firestore-query-transaction | TestAccFirestoreDocument |
| `spanner` | [Spanner](spanner.md) | Official emulator | 36 | 99 | Yes: sessions, SQL queries, DML, reads, mutations, transactions over gRPC (unary methods also over REST) | spanner-instance-database-admin, spanner-data-plane-rest | TestAccSpannerInstanceAndDatabase |
| `bigtable` | [Bigtable](bigtable.md) | Official emulator code (bttest) | 24 | 101 | Yes, modeled locally, not recorded: row reads, scans, and mutations over gRPC | bigtable-instance-table-admin | none |
| `cloudsql` | [Cloud SQL for PostgreSQL](cloudsql.md) | Real PostgreSQL per instance | 14 | 85 | Yes: PostgreSQL on a published `127.0.0.1` port with a user password; no TLS | cloudsql-postgres-instance-admin | none |
| `redis` | [Memorystore for Redis](memorystore.md) | Real Redis per instance | 8 | 47 | Yes: Redis on the reported `127.0.0.1` port, with AUTH; no TLS | memorystore-redis-instance-admin | none |
| `bigquery` | [BigQuery](bigquery.md) | gcp-local | 20 | 47 | Partial: streaming inserts and single-table SELECT with an equality filter, through `jobs.query` or query jobs | bigquery-dataset-rows-query, bigquery-jobs-insert-query | TestAccBigQueryDatasetAndTable |
| `secretmanager` | [Secret Manager](secretmanager.md) | gcp-local | 15 | 36 | Yes: secret versions and payload access over REST and gRPC; gRPC not recorded | secretmanager-access-lifecycle | TestAccSecretManagerSecretAndVersion |
| `cloudkms` | [Cloud KMS](cloudkms.md) | gcp-local | 26 | 87 | Yes: symmetric encrypt and decrypt; asymmetric sign, HMAC, and (modeled locally) asymmetric decrypt | cloudkms-symmetric-key-lifecycle, cloudkms-asymmetric-mac-keys | TestAccKMSKeyRingCryptoKey |
| `cloudtasks` | [Cloud Tasks](cloudtasks.md) | gcp-local | 16 | 24 | Yes: HTTP-target dispatch with OIDC and retries; tasks created over REST (v2beta3) or gRPC (v2); gRPC not recorded | cloudtasks-dispatch-retry | TestAccCloudTasksQueueLifecycle |
| `logging` | [Cloud Logging](logging.md) | gcp-local | 4 | 254 | Yes: write, filter, page, and delete log entries | logging-entries-lifecycle | none |
| `artifactregistry` | [Artifact Registry](artifactregistry.md) | gcp-local | 7 | 69 | No: repositories only; no image push or pull | artifactregistry-repository-lifecycle | TestAccArtifactRegistryRepository |
| `iam` | [IAM service accounts](serviceaccounts.md) | gcp-local | 12 | 137 | Partial: accounts, policies, keys and `signBlob` that sign Storage URLs; keys mint no tokens; keys and `signBlob` not recorded | serviceaccount-lifecycle-authorization | TestAccServiceAccount |
| `resourcemanager` | [Project IAM](project-iam.md) | gcp-local | 2 | 38 | Yes: project IAM policy | project-iam-policy-binding | TestAccProjectIamPolicy |
| `compute` | [VPC networking and Connectivity Tests](networking.md) | gcp-local | 38 | 1066 | Analysis only: verdicts, no real traffic | vpc-firewall-connectivity, vm-connectivity-nat-peering | TestAccComputeNetworkAndSubnetwork |
| `run` | [Cloud Run](cloudrun.md) | Local processes or Docker | 13 | 59 | Partial: your own images with `--cloud-run-docker`, workload identity, ID-token invocation; no egress, jobs, or volumes | none | TestAccCloudRunV2Service |
| `vertexai` | [Vertex AI](vertexai.md) | Deterministic stub or Ollama | 5 | 1128 | Partial: generate, stream, count tokens, embed; answers come from the stub or Ollama, not Gemini | vertexai-inference-authorization | none |

Notes on counts:

- API totals use these discovery documents: storage v1, pubsub v1, firestore v1, spanner v1, bigtableadmin v2 plus bigtable v2, sqladmin v1beta4, redis v1, bigquery v2, secretmanager v1, cloudkms v1, cloudtasks v2beta3, artifactregistry v1, iam v1, cloudresourcemanager v1, compute v1 plus networkmanagement v1, run v2, logging v2, aiplatform v1.
- Pub/Sub counts gRPC methods. 12 REST routes serve topic and subscription create, get, list, delete, and IAM policy; snapshots and seek are gRPC only.
- Spanner: 21 admin and 15 data-plane session methods.
- Bigtable: 17 admin methods (bigtableadmin v2) and 7 data methods (bigtable v2, gRPC).
- Firestore: 14 document methods (all on gRPC; all but Listen on REST) and 2 Admin field methods on REST.
- Logging serves its 4 methods over both REST and gRPC.
- IAM counts `signBlob` from IAM Service Account Credentials v1 with the 11 IAM v1 methods.
- Secret Manager also serves 14 of its 15 methods over gRPC `google.cloud.secretmanager.v1`; locations list is REST only.
- Cloud Tasks also serves 15 of its 16 methods over gRPC `google.cloud.tasks.v2`; locations list is REST only. REST is v2beta3.
- Cloud KMS serves 25 of its 26 methods over gRPC as well; only the location list is REST only ([transport](cloudkms.md#transport)).
- Cloud Run serves its 13 methods over both REST and gRPC (`google.cloud.run.v2.Services`, `google.cloud.run.v2.Revisions`, and `google.longrunning.Operations` for Cloud Run operation names).

## Client compatibility

A recording proves only the requests it sends. This table lists the real clients a test drives against the emulator; a client not listed is untested, even for a recorded service. Rows testing `tests/clients/*` come from the [client matrix](clients.md): one workflow per service at the client's default transport, with a denied call by an ungranted identity.

| Service | Client | Transport | Test |
| --- | --- | --- | --- |
| storage | Python `google-cloud-storage` 3.10.1 | JSON REST | `tests/conformance/test_storage_sdk.py` |
| storage | Python `google-cloud-storage` 3.10.1: resumable upload, V4 signed URLs | JSON REST, XML | `scripts/smoke_storage.py` |
| storage | Go `cloud.google.com/go/storage` 1.65.0, default settings: bucket and object CRUD, object metadata `Update`, resumable upload, XML reads, copy, signed URLs | JSON REST, XML | `internal/providers/storage/sdk_test.go` |
| pubsub | Python `google-cloud-pubsub` 2.42.0 | gRPC | `internal/providers/pubsub/testdata/sdk.py` |
| pubsub (ordering, `resume_publish`, dead-letter, filters, exactly-once) | Python `google-cloud-pubsub` 2.42.0 | gRPC | `internal/providers/pubsub/testdata/sdk_delivery.py` |
| firestore | Python `google-cloud-firestore` 2.34.1 | gRPC | `internal/providers/firestore/testdata/sdk.py` |
| spanner | Go `cloud.google.com/go/spanner` 1.95.1 | gRPC | `internal/providers/spanner/data_integration_test.go` |
| bigtable | Go `cloud.google.com/go/bigtable` 1.58.0 | gRPC | `internal/providers/bigtable/data_test.go` |
| logging | Python `google-cloud-logging` 3.17.0 | REST and gRPC | `internal/providers/logging/testdata/sdk.py` |
| cloudkms | Python `google-cloud-kms` 3.18.0 | REST and gRPC | `internal/providers/cloudkms/testdata/sdk.py` |
| cloudkms | Go `cloud.google.com/go/kms` 1.35.0 | gRPC | `internal/providers/cloudkms/grpc_test.go` |
| cloudkms | Go `cloud.google.com/go/kms` 1.35.0 | gRPC | `tests/clients/go` |
| cloudkms | Python `google-cloud-kms` 3.18.0 | gRPC | `tests/clients/python` |
| cloudkms | Node `@google-cloud/kms` 6.2.1 | gRPC | `tests/clients/node` |
| cloudkms | `gcloud` 588.0.0 | REST | `tests/clients/cli` |
| secretmanager | Python `google-cloud-secret-manager` 2.31.0 | gRPC | `internal/providers/secretmanager/testdata/sdk.py` |
| secretmanager | Go `cloud.google.com/go/secretmanager` 1.22.0 | gRPC | `internal/providers/secretmanager/client_test.go` |
| cloudtasks | Python `google-cloud-tasks` 2.26.0 (`tasks_v2`) | gRPC | `internal/providers/cloudtasks/testdata/sdk.py` |
| cloudtasks | Go `cloud.google.com/go/cloudtasks` 1.20.0 (`apiv2`) | gRPC | `internal/providers/cloudtasks/client_test.go` |
| run | Go `cloud.google.com/go/run/apiv2` 1.23.0 | gRPC | `internal/providers/run/grpc_test.go` |
| run | Python `google-cloud-run` 0.16.1 | gRPC | `internal/providers/run/testdata/sdk.py` |
| vertexai | Python `google-genai` 2.29.0 | REST | `internal/providers/vertexai/testdata/sdk.py` |
| identity | Python `google-auth` 2.56.2 (ADC, metadata) | HTTP | `internal/identity/testdata/sdk.py` |
| Terraform cases above | `hashicorp/google` 8.5.0 | REST | `tests/terraform/runner` |
| storage | Go `cloud.google.com/go/storage` 1.65.0 | JSON REST | `tests/clients/go` |
| storage | Node `@google-cloud/storage` 8.4.0 | JSON REST | `tests/clients/node` |
| storage | Python `google-cloud-storage` 3.10.1 | JSON REST | `tests/clients/python` |
| pubsub | Go `cloud.google.com/go/pubsub/v2` 2.7.0 | gRPC | `tests/clients/go` |
| pubsub | Node `@google-cloud/pubsub` 6.2.0 | gRPC | `tests/clients/node` |
| pubsub | Python `google-cloud-pubsub` 2.42.0 | gRPC | `tests/clients/python` |
| firestore | Go `cloud.google.com/go/firestore` 1.22.0 | gRPC | `tests/clients/go` |
| firestore | Python `google-cloud-firestore` 2.34.1 | gRPC | `tests/clients/python` |
| firestore | Node `@google-cloud/firestore` 9.3.1 | gRPC | `tests/clients/node` |
| secretmanager | Go `cloud.google.com/go/secretmanager` 1.22.0 | gRPC | `tests/clients/go` |
| secretmanager | Python `google-cloud-secret-manager` 2.31.0 | gRPC | `tests/clients/python` |
| secretmanager | Node `@google-cloud/secret-manager` 7.1.1 | gRPC | `tests/clients/node` |
| secretmanager | `gcloud` 588.0.0 | REST | `tests/clients/cli` |
| cloudtasks | Go `cloud.google.com/go/cloudtasks` 1.20.0 | gRPC | `tests/clients/go` |
| cloudtasks | Python `google-cloud-tasks` 2.26.0 | gRPC | `tests/clients/python` |
| cloudtasks | Node `@google-cloud/tasks` 7.2.1 | gRPC | `tests/clients/node` |
| run | Go `cloud.google.com/go/run` 1.23.0 | gRPC | `tests/clients/go` |
| run | Python `google-cloud-run` 0.16.1 | gRPC | `tests/clients/python` |
| bigquery | Go `cloud.google.com/go/bigquery` 1.85.0 | REST | `tests/clients/go` |
| bigquery | Node `@google-cloud/bigquery` 9.2.0 | REST | `tests/clients/node` |
| bigquery | Python `google-cloud-bigquery` 3.46.1 | REST | `tests/clients/python` |
| logging | Go `cloud.google.com/go/logging` 1.18.0 | gRPC | `tests/clients/go` |
| logging | Node `@google-cloud/logging` 12.2.0 | gRPC | `tests/clients/node` |
| logging | Python `google-cloud-logging` 3.17.0 | gRPC | `tests/clients/python` |
| iam | Node `@googleapis/iam` 42.1.0 | REST | `tests/clients/node` |

### Known client failures

These matrix pairs fail today. `sh scripts/check_clients.sh` fails if one starts passing or fails differently, so this table stays current.

| Service | Client | Transport | Fails because | Issue |
| --- | --- | --- | --- | --- |
| storage | `gcloud` 588.0.0 | JSON REST | bucket create refuses the enableObjectRetention query option | [#65](https://github.com/evanmarshall/gcp-local/issues/65) |
| storage | `rclone` 1.75.2 | JSON REST | bucket create without publicAccessPrevention and softDeletePolicy (the GCP defaults) is refused | [#64](https://github.com/evanmarshall/gcp-local/issues/64) |
| pubsub | `gcloud` 588.0.0 | REST | REST publish and pull are not served | [#66](https://github.com/evanmarshall/gcp-local/issues/66) |
| spanner | Go `cloud.google.com/go/spanner` 1.95.1 | gRPC | no admin gRPC listener | [#61](https://github.com/evanmarshall/gcp-local/issues/61) |
| spanner | Node `@google-cloud/spanner` 9.0.0 | gRPC | no admin gRPC listener | [#61](https://github.com/evanmarshall/gcp-local/issues/61) |
| spanner | Python `google-cloud-spanner` 3.71.0 | gRPC | no admin gRPC listener | [#61](https://github.com/evanmarshall/gcp-local/issues/61) |
| bigtable | Go `cloud.google.com/go/bigtable` 1.58.0 | gRPC | no admin gRPC listener | [#62](https://github.com/evanmarshall/gcp-local/issues/62) |
| bigtable | Node `@google-cloud/bigtable` 7.4.0 | gRPC | no admin gRPC listener | [#62](https://github.com/evanmarshall/gcp-local/issues/62) |
| bigtable | Python `google-cloud-bigtable` 2.50.0 | gRPC | no admin gRPC listener | [#62](https://github.com/evanmarshall/gcp-local/issues/62) |
| iam | Go `cloud.google.com/go/iam` 1.11.0 | gRPC | no gRPC listener | [#63](https://github.com/evanmarshall/gcp-local/issues/63) |
| iam | Python `google-cloud-iam` 2.26.0 | gRPC | no gRPC listener | [#63](https://github.com/evanmarshall/gcp-local/issues/63) |

Untested: Java clients, `gsutil`, the Node.js and gcloud clients of Cloud Run, and every client of Artifact Registry, Cloud SQL, Memorystore, and Compute.

## Cross-service behavior

| Capability | Status |
| --- | --- |
| Local credentials (ADC, STS, metadata server) | Supported ([local identity](local-identity.md)) |
| IAM allow policies with predefined roles recorded from Google Cloud | Supported on every service ([IAM](iam.md)) |
| IAM conditions (`resource.name`, `resource.type`, `request.time`) | Recorded for Storage; elsewhere a conditional binding grants nothing |
| IAM deny policies, principal access boundaries | Local model only, not recorded |
| Organization policy, VPC Service Controls | Dry-run signals only, not recorded |
| Permission discovery against real Google Cloud | One conclusive probe: `secretmanager-versions-access` ([permission discovery](permission-discovery.md)) |
| Persistence across restarts | Not supported; state is in memory |
| Quotas and rate limits | Not modeled |

## Not integrated

Cloud Functions, GKE, Dataflow, Workflows, Eventarc, Cloud Scheduler, Monitoring, and Datastore mode have no provider. Requests to them get no route.
