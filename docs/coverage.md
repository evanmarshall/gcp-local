# Service coverage

What each service supports and how it is verified. Every service authenticates callers and checks IAM on every request.

- **Methods served**: methods that run. Every other method returns `UNIMPLEMENTED`.
- **API total**: methods in Google's discovery document for that API version.
- **Data plane**: what an application can do with the resource once it exists.
- **Recordings**: scenarios recorded against real Google Cloud that the emulator must match ([validation](validation.md)). Each covers only its own steps.
- **Terraform**: pinned google provider acceptance cases ([Terraform compatibility](terraform-compatibility.md)).

`scripts/check_repo.py` fails if a packaged service, recording, or Terraform case is missing here. It does not check method counts.

## Summary

| Key | Service | Backend | Methods served | API total | Data plane for applications | Recordings | Terraform |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `storage` | [Cloud Storage](storage.md) | fake-gcs-server, in-process | 15 | 88 | Yes: object read, write, list, delete | storage-authorization, storage-bucket-updates, storage-cross-project, storage-iam-conditions, storage-multipart-query-name, storage-object-lifecycle, storage-object-replacement, storage-python-sdk | TestAccStorageBucket_basic, TestAccStorageBucket_labels, TestAccStorageObject_recreate |
| `pubsub` | [Pub/Sub](pubsub.md) | Official emulator | 13 | 46 | Yes: publish, pull, streaming pull, push | pubsub-pull-authorization | TestAccPubSubTopicAndSubscription |
| `firestore` | [Firestore](firestore.md) | Official emulator | 7 | 60 | Partial: single documents only; no queries, transactions, or batch writes | firestore-document-lifecycle | TestAccFirestoreDocument |
| `spanner` | [Spanner](spanner.md) | Official emulator | 21 | 99 | No: admin and schema only; SQL reads and writes are not routed | spanner-instance-database-admin | TestAccSpannerInstanceAndDatabase |
| `bigtable` | [Bigtable](bigtable.md) | Official emulator code (bttest) | 17 | 82 | No: instance and table admin only; row reads and writes are not routed | bigtable-instance-table-admin | none |
| `cloudsql` | [Cloud SQL for PostgreSQL](cloudsql.md) | Real PostgreSQL per instance | 14 | 85 | Yes: PostgreSQL on a published `127.0.0.1` port with a user password; no TLS | cloudsql-postgres-instance-admin | none |
| `redis` | [Memorystore for Redis](memorystore.md) | Real Redis per instance | 8 | 47 | Yes: Redis on the reported `127.0.0.1` port, with AUTH; no TLS | memorystore-redis-instance-admin | none |
| `bigquery` | [BigQuery](bigquery.md) | gcp-local | 18 | 47 | Partial: streaming inserts and single-table SELECT with an equality filter | bigquery-dataset-rows-query | TestAccBigQueryDatasetAndTable |
| `secretmanager` | [Secret Manager](secretmanager.md) | gcp-local | 15 | 36 | Yes: secret versions and payload access | secretmanager-access-lifecycle | TestAccSecretManagerSecretAndVersion |
| `cloudkms` | [Cloud KMS](cloudkms.md) | gcp-local | 20 | 87 | Yes: symmetric encrypt and decrypt | cloudkms-symmetric-key-lifecycle | TestAccKMSKeyRingCryptoKey |
| `cloudtasks` | [Cloud Tasks](cloudtasks.md) | gcp-local | 16 | 24 | Yes: HTTP-target dispatch with OIDC and retries | cloudtasks-dispatch-retry | TestAccCloudTasksQueueLifecycle |
| `artifactregistry` | [Artifact Registry](artifactregistry.md) | gcp-local | 7 | 69 | No: repositories only; no image push or pull | artifactregistry-repository-lifecycle | TestAccArtifactRegistryRepository |
| `iam` | [IAM service accounts](serviceaccounts.md) | gcp-local | 11 | 137 | Partial: accounts and policies; keys are synthesized and sign nothing | serviceaccount-lifecycle-authorization | TestAccServiceAccount |
| `resourcemanager` | [Project IAM](project-iam.md) | gcp-local | 2 | 38 | Yes: project IAM policy | project-iam-policy-binding | TestAccProjectIamPolicy |
| `compute` | [VPC networking and Connectivity Tests](networking.md) | gcp-local | 38 | 1066 | Analysis only: verdicts, no real traffic | vpc-firewall-connectivity, vm-connectivity-nat-peering | TestAccComputeNetworkAndSubnetwork |
| `run` | [Cloud Run](cloudrun.md) | Local processes or Docker | 13 | 59 | Partial: invocation works; the packaged environment runs only the bundled worker | none | TestAccCloudRunV2Service |

API totals come from these discovery documents: storage v1, pubsub v1, firestore v1, spanner v1,
bigtableadmin v2, sqladmin v1beta4, redis v1, bigquery v2, secretmanager v1, cloudkms v1, cloudtasks
v2beta3, artifactregistry v1, iam v1, cloudresourcemanager v1, compute v1 plus networkmanagement v1,
and run v2. Pub/Sub counts the gRPC methods with permission plans; the same operations are also
served over 12 REST routes.

## Cross-service behavior

| Capability | Status |
| --- | --- |
| Local credentials (ADC, STS, metadata server) | Supported ([local identity](local-identity.md)) |
| IAM allow policies with predefined roles recorded from Google Cloud | Supported on every service ([IAM](iam.md)) |
| IAM conditions (`resource.name`, `resource.type`, `request.time`) | Recorded for Storage; other providers fail closed until they supply resource attributes |
| IAM deny policies, principal access boundaries | Local model only, not recorded |
| Organization policy, VPC Service Controls | Dry-run signals only, not recorded |
| Permission discovery against real Google Cloud | One conclusive probe: `secretmanager-versions-access` ([permission discovery](permission-discovery.md)) |
| Persistence across restarts | Not supported; state is in memory |
| Quotas and rate limits | Not modeled |

## Not yet integrated

Services with no provider include Cloud Functions, GKE, Dataflow, Workflows, Eventarc, Cloud
Scheduler, Logging, Monitoring, and Datastore mode. Requests to them get no route.
