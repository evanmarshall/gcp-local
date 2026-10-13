# Service coverage

What each service supports and how it is verified. Every service authenticates callers and checks IAM on every request.

- **Methods served**: methods that run. All others return `UNIMPLEMENTED`, except Storage, which returns 400 ([why](services/storage.md#gaps)).
- **API total**: methods in the discovery document committed under [`api/discovery/`](../api/discovery/) (Storage, Memorystore for Redis, Cloud SQL, Cloud Scheduler). Other services show —: their discovery document isn't committed, so the total can't be checked.
- **Data plane**: what an application can do with a resource once it exists.
- **Recordings**: scenarios recorded against real Google Cloud whose compared fields (status codes, allow/deny outcomes, and selected response fields) the emulator reproduces ([validation](contributing/validation.md#what-a-recording-proves)). Each covers only its own steps.
- **Terraform**: acceptance cases run with the pinned google provider. `TestAcc*` cases are extracted from the provider's upstream tests; `TestLocal*` cases are written in gcp-local ([Terraform compatibility](contributing/terraform-compatibility.md)).

## Summary

| Key | Service | Backend | Methods served | API total | Data plane for applications | Recordings | Terraform |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `storage` | [Cloud Storage](services/storage.md) | fake-gcs-server, in-process | 25 | 88 | Yes: object read, write, list (delimiter, offsets, paging), delete, metadata patch, resumable uploads, V4 signed URLs, rewrite, object versioning, lifecycle rules (configuration recorded, timing modeled), retention policies, Bucket Lock, holds, object retention, Pub/Sub notifications and hierarchical-namespace folders (not recorded), soft delete and restore, public access prevention, bucket list paging; rewrite with a request body, public members under inherited prevention, and XML paths other than signed URLs not recorded | storage-authorization, storage-bucket-defaults, storage-bucket-lock, storage-bucket-policy-only, storage-bucket-updates, storage-cross-project, storage-gcloud-bucket-defaults, storage-gcloud-object-requests, storage-go-client-projection, storage-iam-conditions, storage-iam-directory, storage-iam-groups, storage-lifecycle-rules, storage-list-paging, storage-multipart-query-name, storage-object-lifecycle, storage-object-replacement, storage-object-retention, storage-object-versioning, storage-public-access-enforced, storage-python-sdk, storage-rclone-object-requests, storage-resumable-signed-url, storage-retention-holds, storage-versioning-soft-delete | TestAccStorageBucket_basic, TestAccStorageBucket_labels, TestAccStorageObject_recreate |
| `pubsub` | [Pub/Sub](services/messaging/pubsub.md) | Official emulator; Protocol Buffer schemas in gcp-local | 28 | — | Yes: publish, pull, streaming pull, push, ordering, filters, dead-letter topics, retry backoff, exactly-once, snapshots and seek, Avro and Protocol Buffer schemas with publish-time validation; streaming pull, push, retry backoff, snapshots and seek not recorded | pubsub-delivery-features, pubsub-pull-authorization, pubsub-rest-data-plane, pubsub-schemas | TestLocalPubSubTopicAndSubscription |
| `firestore` | [Firestore](services/databases/firestore.md) | Official emulator | 18 | — | Yes: documents, subcollections, queries, aggregations, transactions, batch writes, listeners over REST and gRPC, TTL deletion; batch writes, listeners, and TTL deletion not recorded | firestore-document-lifecycle, firestore-query-transaction | TestLocalFirestoreDocument |
| `spanner` | [Spanner](services/databases/spanner.md) | Official emulator | 36 | — | Yes: sessions, SQL queries, DML, reads, mutations, transactions over gRPC (unary methods also over REST) | spanner-instance-database-admin, spanner-data-plane-rest | TestLocalSpannerInstanceAndDatabase |
| `bigtable` | [Bigtable](services/databases/bigtable.md) | Official emulator code (bttest) | 28 | — | Yes, modeled locally, not recorded: row reads, scans, and mutations over gRPC | bigtable-instance-table-admin | none |
| `cloudsql` | [Cloud SQL for PostgreSQL and MySQL](services/databases/cloudsql.md) | Real PostgreSQL or MySQL per instance | 15 | 85 | Yes: PostgreSQL or MySQL on a published `127.0.0.1` port with a user password; no TLS | cloudsql-postgres-instance-admin, cloudsql-mysql-instance-admin | none |
| `redis` | [Memorystore for Redis](services/databases/memorystore.md) | Real Redis per instance | 10 | 47 | Yes: Redis on the reported `127.0.0.1` port, with AUTH; no TLS | memorystore-redis-instance-admin | none |
| `bigquery` | [BigQuery](services/databases/bigquery.md) | GoogleSQL engine (googlesqlite), in-process | 21 | — | Yes: GoogleSQL queries with joins, aggregates, CTEs, parameters, DML, DDL, and scripts, authorized per table; streaming inserts; no load, extract, or copy jobs | bigquery-api-surface, bigquery-dataset-rows-query, bigquery-jobs-insert-query, bigquery-sql-engine | TestLocalBigQueryDatasetAndTable |
| `secretmanager` | [Secret Manager](services/security/secretmanager.md) | gcp-local | 15 | — | Yes: secret versions and payload access over REST and gRPC; gRPC not recorded; rotation, expiration, aliases, topics, user-managed replication, CMEK, and regional secrets are `UNIMPLEMENTED` | secretmanager-access-lifecycle | TestLocalSecretManagerSecretAndVersion |
| `cloudkms` | [Cloud KMS](services/security/cloudkms.md) | gcp-local | 26 | — | Yes: symmetric encrypt and decrypt; asymmetric sign, HMAC, and (modeled locally) asymmetric decrypt; scheduled rotation and list filters are `UNIMPLEMENTED` | cloudkms-symmetric-key-lifecycle, cloudkms-asymmetric-mac-keys | TestLocalKMSKeyRingCryptoKey |
| `cloudtasks` | [Cloud Tasks](services/messaging/cloudtasks.md) | gcp-local | 16 | — | Yes: HTTP-target dispatch with OIDC and retries; tasks created over REST (v2beta3) or gRPC (v2); gRPC not recorded | cloudtasks-dispatch-retry | TestLocalCloudTasksQueueLifecycle |
| `cloudscheduler` | [Cloud Scheduler](services/messaging/cloudscheduler.md) | gcp-local | 8 | 16 | Yes: cron jobs in any time zone that call HTTP targets (OIDC or OAuth) or publish to Pub/Sub as the service agent, with retries; HTTP delivery and retries not recorded | cloudscheduler-job-lifecycle | TestLocalCloudSchedulerJob |
| `logging` | [Cloud Logging](services/logging.md) | gcp-local | 4 | — | Yes: write, filter, page, and delete log entries | logging-entries-lifecycle, logging-filter-precedence | none |
| `artifactregistry` | [Artifact Registry](services/artifactregistry.md) | gcp-local | 7 | — | No: repositories only; no image push or pull | artifactregistry-repository-lifecycle | TestLocalArtifactRegistryRepository |
| `iam` | [IAM service accounts](services/security/serviceaccounts.md) and [roles](services/security/iam-roles.md) | gcp-local | 29 | — | Partial: accounts, policies, keys (user-managed keys not recorded); IAM Credentials impersonation (`generateAccessToken`, `generateIdToken`, `signJwt`, `signBlob`) with delegation and lifetime limits, tokens usable at every service; keys mint no tokens; project and organization custom roles that grant in any allow policy; organization roles not recorded | serviceaccount-lifecycle-authorization, iamcredentials-impersonation, iam-custom-role-lifecycle | TestLocalServiceAccount, TestLocalProjectIamCustomRole |
| `resourcemanager` | [Resource Manager](services/security/resourcemanager.md) and [project IAM](services/security/project-iam.md) | gcp-local | 29 | — | Yes: folders (create, rename, move, soft delete, undelete), project get, search, and move, organization reads, and IAM on all three, inherited down the hierarchy; success paths that need organization roles not recorded | project-iam-policy-binding, resourcemanager-hierarchy-visibility | TestLocalFolderAndFolderIam, TestLocalProjectIamPolicy |
| `compute` | [VPC networking and Connectivity Tests](services/networking.md) | gcp-local | 38 | — | Analysis only: verdicts, no real traffic | vpc-firewall-connectivity, vm-connectivity-nat-peering, vm-firewall-service-accounts | TestLocalComputeNetworkAndSubnetwork |
| `run` | [Cloud Run](services/cloudrun.md) | Local processes or Docker | 13 | — | Partial: your own images with `--cloud-run-docker`, workload identity, ID-token invocation; no egress, jobs, or volumes | none | TestLocalCloudRunV2Service |
| `vertexai` | [Vertex AI](services/vertexai.md) | Deterministic stub or Ollama | 5 | — | Partial: generate, stream, count tokens, embed; answers come from the stub or Ollama, not Gemini | vertexai-inference-authorization | none |

Notes on counts:

- Pub/Sub counts gRPC methods. 28 REST routes serve topic and subscription create, get, list, delete, and IAM policy, publish, pull, acknowledge, and modifyAckDeadline, and all 12 `projects.schemas` methods; StreamingPull, snapshots, and seek are gRPC only.
- Spanner: 21 admin and 15 data-plane session methods. All 21 admin methods are also served over gRPC (`google.spanner.admin.instance.v1.InstanceAdmin`, `google.spanner.admin.database.v1.DatabaseAdmin`, and `google.longrunning.Operations` GetOperation for Spanner operation names).
- Bigtable: 17 admin methods and 4 operations methods (bigtableadmin v2) and 7 data methods (bigtable v2, gRPC). The admin and operations methods are also served over gRPC (`google.bigtable.admin.v2`, `google.longrunning.Operations`).
- Firestore: 14 document methods (all on gRPC; all but Listen on REST), 2 Admin field methods and operations `get` and `list` on REST.
- Logging serves its 4 methods over both REST and gRPC.
- IAM counts 11 IAM v1 account methods, 14 role methods (6 each for `projects.roles` and `organizations.roles`, plus `roles.get` and `roles.list`), and the 4 IAM Service Account Credentials v1 methods. 10 account methods and 13 role methods are also served over gRPC `google.iam.admin.v1.IAM`, and all 4 credentials methods over `google.iam.credentials.v1.IAMCredentials`; key create and `ListRoles` without a parent are REST only.
- Secret Manager also serves 14 of its 15 methods over gRPC `google.cloud.secretmanager.v1`; locations list is REST only.
- Cloud Tasks also serves 15 of its 16 methods over gRPC `google.cloud.tasks.v2`; locations list is REST only. REST is v2beta3.
- Cloud Scheduler serves its 8 job methods over REST and gRPC `google.cloud.scheduler.v1.CloudScheduler`.
- Cloud KMS serves 25 of its 26 methods over gRPC as well; only the location read is REST only ([transport](services/security/cloudkms.md#transport)).
- Resource Manager: 5 v1 methods (project get, IAM policy get, set, and test, operations get) and 24 v3 methods, all 24 also over gRPC (`google.cloud.resourcemanager.v3` and `google.longrunning.Operations` for `operations/*`).
- Cloud Run serves its 13 methods over both REST and gRPC (`google.cloud.run.v2.Services`, `google.cloud.run.v2.Revisions`, and `google.longrunning.Operations` for Cloud Run operation names).

## Client compatibility

A recording proves only the requests it sends. This table lists the real clients a test drives against the emulator; a client not listed is untested, even for a recorded service. Rows testing `tests/clients/*` come from the [client matrix](contributing/clients.md): one workflow per service at the client's default transport, with a denied call by an ungranted identity.

| Service | Client | Transport | Test |
| --- | --- | --- | --- |
| storage | Python `google-cloud-storage` 3.10.1 | JSON REST | `tests/conformance/storage/clients/test_storage_sdk.py` |
| storage | Python `google-cloud-storage` 3.10.1: resumable upload, V4 signed URLs | JSON REST, XML | `scripts/smoke/storage.py` |
| storage | Go `cloud.google.com/go/storage` 1.65.0, default settings: bucket and object CRUD, bucket list with `ReturnPartialSuccess`, delimited object listing across pages, object metadata `Update`, resumable upload, XML reads (a missing bucket fails at once), copy, signed URLs | JSON REST, XML | `internal/providers/storage/sdk_test.go` |
| pubsub | Python `google-cloud-pubsub` 2.42.0 | gRPC | `internal/providers/messaging/pubsub/testdata/sdk.py` |
| pubsub (ordering, `resume_publish`, dead-letter, filters, exactly-once) | Python `google-cloud-pubsub` 2.42.0 | gRPC | `internal/providers/messaging/pubsub/testdata/sdk_delivery.py` |
| pubsub (`SchemaServiceClient`, topics with Avro and Protocol Buffer schemas) | Python `google-cloud-pubsub` 2.42.0 | gRPC | `internal/providers/messaging/pubsub/testdata/sdk_schemas.py` |
| firestore | Python `google-cloud-firestore` 2.34.1 | gRPC | `internal/providers/databases/firestore/testdata/sdk.py` |
| spanner | Go `cloud.google.com/go/spanner` 1.95.1 | gRPC | `internal/providers/databases/spanner/integration_test.go` |
| spanner | Go `cloud.google.com/go/spanner` 1.95.1 admin clients | gRPC | `internal/providers/databases/spanner/admin_grpc_test.go` |
| bigtable | Go `cloud.google.com/go/bigtable` 1.58.0 | gRPC | `internal/providers/databases/bigtable/data_test.go` |
| bigtable | Go `cloud.google.com/go/bigtable` 1.58.0 admin clients | gRPC | `internal/providers/databases/bigtable/admin_grpc_test.go` |
| iam | Go `cloud.google.com/go/iam` 1.11.0 (`admin/apiv1`) | gRPC | `internal/providers/security/iam/grpc_test.go` |
| iamcredentials | Go `cloud.google.com/go/iam` 1.11.0 (`credentials/apiv1`) | gRPC | `internal/providers/security/iam/credentials_test.go` |
| resourcemanager | Go `cloud.google.com/go/resourcemanager` 1.17.0 (`apiv3`): folder operations, project move and search, permission tests | gRPC | `internal/providers/security/resourcemanager/grpc_test.go` |
| logging | Python `google-cloud-logging` 3.17.0 | REST and gRPC | `internal/providers/logging/testdata/sdk.py` |
| cloudkms | Python `google-cloud-kms` 3.18.0 | REST and gRPC | `internal/providers/security/cloudkms/testdata/sdk.py` |
| cloudkms | Go `cloud.google.com/go/kms` 1.35.0 | gRPC | `internal/providers/security/cloudkms/grpc_test.go` |
| cloudkms | Go `cloud.google.com/go/kms` 1.35.0 | gRPC | `tests/clients/go` |
| cloudkms | Python `google-cloud-kms` 3.18.0 | gRPC | `tests/clients/python` |
| cloudkms | Node `@google-cloud/kms` 6.2.1 | gRPC | `tests/clients/node` |
| cloudkms | `gcloud` 588.0.0 | REST | `tests/clients/cli` |
| secretmanager | Python `google-cloud-secret-manager` 2.31.0 | gRPC | `internal/providers/security/secretmanager/testdata/sdk.py` |
| secretmanager | Go `cloud.google.com/go/secretmanager` 1.22.0 | gRPC | `internal/providers/security/secretmanager/client_test.go` |
| cloudtasks | Python `google-cloud-tasks` 2.26.0 (`tasks_v2`) | gRPC | `internal/providers/messaging/cloudtasks/testdata/sdk.py` |
| cloudtasks | Go `cloud.google.com/go/cloudtasks` 1.20.0 (`apiv2`) | gRPC | `internal/providers/messaging/cloudtasks/client_test.go` |
| cloudscheduler | Go `cloud.google.com/go/scheduler` 1.17.0 (`apiv1`) | gRPC | `internal/providers/messaging/cloudscheduler/client_test.go` |
| run | Go `cloud.google.com/go/run/apiv2` 1.23.0 | gRPC | `internal/providers/run/grpc_test.go` |
| run | Python `google-cloud-run` 0.16.1 | gRPC | `internal/providers/run/testdata/sdk.py` |
| vertexai | Python `google-genai` 2.29.0 | REST | `internal/providers/vertexai/testdata/sdk.py` |
| identity | Python `google-auth` 2.56.2 (ADC, metadata) | HTTP | `internal/identity/testdata/sdk.py` |
| Terraform cases above | `hashicorp/google` 8.5.0 | REST | `tests/terraform/runner` |
| storage | Go `cloud.google.com/go/storage` 1.65.0 | JSON REST | `tests/clients/go` |
| storage | Node `@google-cloud/storage` 8.4.0 | JSON REST | `tests/clients/node` |
| storage | Python `google-cloud-storage` 3.10.1 | JSON REST | `tests/clients/python` |
| storage | `gcloud` 588.0.0 | JSON REST | `tests/clients/cli` |
| storage | `rclone` 1.75.2 | JSON REST | `tests/clients/cli` |
| pubsub | Go `cloud.google.com/go/pubsub/v2` 2.7.0 | gRPC | `tests/clients/go` |
| pubsub | Node `@google-cloud/pubsub` 6.2.0 | gRPC | `tests/clients/node` |
| pubsub | Python `google-cloud-pubsub` 2.42.0 | gRPC | `tests/clients/python` |
| pubsub | `gcloud` 588.0.0 | REST | `tests/clients/cli` |
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
| cloudscheduler | Go `cloud.google.com/go/scheduler` 1.17.0 | gRPC | `tests/clients/go` |
| cloudscheduler | Python `google-cloud-scheduler` 2.20.0 | gRPC | `tests/clients/python` |
| cloudscheduler | Node `@google-cloud/scheduler` 6.1.0 | gRPC | `tests/clients/node` |
| run | Go `cloud.google.com/go/run` 1.23.0 | gRPC | `tests/clients/go` |
| run | Python `google-cloud-run` 0.16.1 | gRPC | `tests/clients/python` |
| bigquery | Go `cloud.google.com/go/bigquery` 1.85.0 | REST | `tests/clients/go` |
| bigquery | Node `@google-cloud/bigquery` 9.2.0 | REST | `tests/clients/node` |
| bigquery | Python `google-cloud-bigquery` 3.46.1 | REST | `tests/clients/python` |
| logging | Go `cloud.google.com/go/logging` 1.18.0 | gRPC | `tests/clients/go` |
| logging | Node `@google-cloud/logging` 12.2.0 | gRPC | `tests/clients/node` |
| logging | Python `google-cloud-logging` 3.17.0 | gRPC | `tests/clients/python` |
| spanner | Go `cloud.google.com/go/spanner` 1.95.1 | gRPC | `tests/clients/go` |
| spanner | Node `@google-cloud/spanner` 9.0.0 | gRPC | `tests/clients/node` |
| spanner | Python `google-cloud-spanner` 3.71.0 | gRPC | `tests/clients/python` |
| bigtable | Go `cloud.google.com/go/bigtable` 1.58.0 | gRPC | `tests/clients/go` |
| bigtable | Node `@google-cloud/bigtable` 7.4.0 | gRPC | `tests/clients/node` |
| bigtable | Python `google-cloud-bigtable` 2.50.0 | gRPC | `tests/clients/python` |
| iam | Go `cloud.google.com/go/iam` 1.11.0 | gRPC | `tests/clients/go` |
| iam | Node `@googleapis/iam` 42.1.0 | REST | `tests/clients/node` |
| iam | Python `google-cloud-iam` 2.26.0 | gRPC | `tests/clients/python` |
| iamcredentials | Go `google.golang.org/api` 0.293.0 | REST | `tests/clients/go` |
| iamcredentials | Python `google-auth` 2.56.2 | REST | `tests/clients/python` |
| iamcredentials | `gcloud` 588.0.0 | REST | `tests/clients/cli` |

### Known client failures

No matrix pair fails today. A pair that fails is listed here as `xfail` in `tests/clients/matrix.json` with its issue; `sh scripts/check/clients.sh` fails if one starts passing or fails differently.

Untested: Java clients, `gsutil`, the Node.js and gcloud clients of Cloud Run, and every client of Artifact Registry, Cloud SQL, Memorystore, and Compute.

The Terraform google provider's `impersonate_service_account` doesn't reach gcp-local: provider 8.5.0 calls `https://iamcredentials.UNIVERSE_DOMAIN` and takes no endpoint for it ([IAM service accounts](services/security/serviceaccounts.md#gaps)).

## Cross-service behavior

| Capability | Status |
| --- | --- |
| Local credentials (ADC, STS, metadata server) | Supported ([local identity](guides/local-identity.md)) |
| IAM allow policies with predefined roles recorded from Google Cloud | Supported on every service ([IAM](architecture/iam.md)) |
| IAM custom roles (`projects/P/roles/R`, `organizations/O/roles/R`) | Granted in allow policies on every service; soft delete stops the grant at once. Project roles recorded on a service account (`iam-custom-role-lifecycle`); organization roles modeled locally ([IAM roles](services/security/iam-roles.md)) |
| IAM conditions (`resource.name`, `resource.type`, `request.time`) | Evaluated where the provider supplies attributes (Storage, Cloud SQL, Spanner, Firestore, Memorystore, Secret Manager, Cloud KMS); recorded for Storage, Spanner, Cloud SQL, Memorystore, Secret Manager, and Cloud KMS. Pub/Sub supplies none, as GCP's attribute reference lists only Pub/Sub Lite; IAM service accounts supply none, as recorded: GCP ignores their resource attributes. A condition that reads a missing attribute grants nothing ([IAM](architecture/iam.md)) |
| IAM `group:`, `principalSet://goog/group/`, and `domain:` members | Resolved from a local directory in the IAM config, transitively, in allow bindings, conditions, and deny rules; refusal of unknown groups and domains (`storage-iam-directory`) and bucket reads granted through a real group, a nested group, and a conditional group binding (`storage-iam-groups`) recorded for Storage; `domain:` grants and group deny rules not recorded ([IAM](architecture/iam.md#groups-and-domains)) |
| IAM deny policies, principal access boundaries | Enforced locally (per-rule exceptions, conditions, public principals; boundaries for service accounts); not recorded, because recording needs organization-level setup ([IAM](architecture/iam.md)) |
| Organization policy, VPC Service Controls | Dry-run signals only, not recorded |
| Permission discovery against real Google Cloud | One conclusive probe: `secretmanager-versions-access` ([permission discovery](contributing/permission-discovery.md)) |
| Persistence across restarts | Not supported; state is in memory |
| Quotas and rate limits | Not modeled |

## Not integrated

Cloud Functions, GKE, Dataflow, Workflows, Eventarc, Monitoring, and Datastore mode have no provider. Requests to them get no route.
