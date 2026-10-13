# gcp-local

**A local Google Cloud emulator that enforces IAM.** It runs Cloud Storage, Pub/Sub, Firestore, Spanner, BigQuery, Cloud SQL, Cloud Run, Cloud Logging, Vertex AI, and more on your machine. Use the real client libraries and Terraform, and catch missing IAM grants before you deploy: a call your identity isn't granted is denied locally, with the status code real GCP returned in the recorded scenarios.

gcp-local issues local credentials and evaluates IAM policies on every request. For every service except Cloud Run, selected parts of its behavior (status codes, allow/deny decisions, and chosen response fields) are checked against scenarios recorded from real Google Cloud; [what that proves and doesn't](docs/contributing/validation.md#what-a-recording-proves).

> **Status: pre-1.0.** Nothing is published to PyPI or a container registry yet; run it from source. Each service implements part of its API ([coverage](docs/coverage.md)).

## Quick start

You need Docker with Compose and Python 3.11+. No Google Cloud account or credentials are used.

```sh
git clone https://github.com/evanmarshall/gcp-local.git
cd gcp-local
python3 scripts/dev/local_env.py up      # builds and starts the environment
python3 scripts/dev/local_env.py env     # prints the endpoint, project, and credential file
```

HTTP is on `http://127.0.0.1:4566` and gRPC on `127.0.0.1:4567` (TLS on `127.0.0.1:4584`). The project is `gcp-local-test`, and a generated developer identity holds explicit IAM grants. `reset` wipes all data, `down` stops everything, and `logs` shows recent output.

Point a client library at it with the generated credential file:

```python
import google.auth
from google.cloud import storage

credentials, _ = google.auth.load_credentials_from_file(
    ".gcp-local/environment/identity/adc.json",
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
client = storage.Client(
    project="gcp-local-test",
    credentials=credentials,
    client_options={"api_endpoint": "http://127.0.0.1:4566"},
)
bucket = client.bucket("my-bucket")
# Uniform bucket-level access is required. The other two settings are
# optional; without them the bucket gets GCP's defaults.
bucket.iam_configuration.uniform_bucket_level_access_enabled = True
bucket.iam_configuration.public_access_prevention = "enforced"
bucket.soft_delete_policy.retention_duration_seconds = 0
client.create_bucket(bucket, location="us-central1")
bucket.blob("hello.txt").upload_from_string("Hello from local GCP.")
```

More examples are in [`examples/`](examples/): Pub/Sub over gRPC, and a Terraform-provisioned Pub/Sub → Cloud Run → Cloud Storage [workflow](docs/guides/workflow.md). The [quickstart](docs/guides/quickstart.md) covers Terraform credentials, multiple environments, and configuration. [Client setup](docs/contributing/clients.md) has the endpoint and transport settings for Go, Python, Node.js, gcloud, and rclone.

## Supported services

[Service coverage](docs/coverage.md) lists methods served, data-plane support, recordings, and Terraform tests per service.

| Service | Backend | Application use |
| --- | --- | --- |
| [Cloud Storage](docs/services/storage.md) | fake-gcs-server | Objects: read, write, list, delete, copy, metadata updates; resumable uploads; V4 signed URLs |
| [Pub/Sub](docs/services/messaging/pubsub.md) | Official Google emulator | Publish, pull, streaming pull, push, ordering, filters, dead-letter topics, retry backoff, exactly-once, snapshots and seek, Avro and Protocol Buffer schemas |
| [Secret Manager](docs/services/security/secretmanager.md) | gcp-local | Secret versions and access over REST and gRPC |
| [Cloud KMS](docs/services/security/cloudkms.md) | gcp-local | Symmetric encrypt and decrypt, asymmetric sign and decrypt, HMAC over REST and gRPC |
| [Cloud Tasks](docs/services/messaging/cloudtasks.md) | gcp-local | HTTP dispatch with OIDC and retries over REST and gRPC |
| [Cloud Scheduler](docs/services/messaging/cloudscheduler.md) | gcp-local | Cron jobs that call HTTP targets or publish to Pub/Sub, over REST and gRPC |
| [Cloud Logging](docs/services/logging.md) | gcp-local | Write, filter, and delete log entries over REST and gRPC |
| [Vertex AI](docs/services/vertexai.md) | Deterministic stub or Ollama | Generate, stream, count tokens, embed; answers are not from Gemini |
| [Firestore](docs/services/databases/firestore.md) | Official Google emulator | Documents, subcollections, queries, transactions, batch writes, listeners |
| [BigQuery](docs/services/databases/bigquery.md) | GoogleSQL engine | GoogleSQL queries, DML, DDL, parameters, streaming inserts |
| [Cloud SQL for PostgreSQL and MySQL](docs/services/databases/cloudsql.md) | Real PostgreSQL or MySQL | PostgreSQL and MySQL clients on `127.0.0.1` with a user password |
| [Memorystore for Redis](docs/services/databases/memorystore.md) | Real Redis | Redis clients on the reported port, with AUTH |
| [Cloud Run](docs/services/cloudrun.md) | Local processes or Docker | Your own images with `--cloud-run-docker`; ID-token invocation; admin API over REST and gRPC |
| [Spanner](docs/services/databases/spanner.md) | Official Google emulator | SQL queries, DML, reads, mutations, transactions |
| [Bigtable](docs/services/databases/bigtable.md) | Official Google emulator code | Row reads, scans, mutations over gRPC; not recorded |
| [Artifact Registry](docs/services/artifactregistry.md) | gcp-local | Repositories only; no push or pull |
| [IAM](docs/services/security/serviceaccounts.md) and [project IAM](docs/services/security/project-iam.md) | gcp-local | Service accounts and policies; impersonation (`generateAccessToken`, `generateIdToken`, `signJwt`, `signBlob`) |
| [Resource Manager](docs/services/security/resourcemanager.md) | gcp-local | Folders, project moves, and organization and folder IAM inherited by everything under them |
| [VPC networking](docs/services/networking.md) | gcp-local | Connectivity verdicts; no real traffic |

IAM allow policies use predefined roles recorded from Google Cloud. IAM conditions are recorded for Storage and also evaluated for Cloud SQL; on other services a condition that reads `resource.*` grants nothing. Deny rules and principal access boundaries for service accounts are enforced locally but not recorded; organization policy and VPC Service Controls are not enforced ([IAM](docs/architecture/iam.md)).

## How it works

A single gateway routes requests to per-service providers, following [LocalStack](https://github.com/localstack/localstack)'s architecture. Providers authenticate and authorize before touching state, and reuse real backends (Google's emulators, PostgreSQL, MySQL, Redis) where they exist. gRPC stubs are generated from Google's published protobuf descriptors. REST routes are generated from Google's discovery documents for the Storage JSON API, Memorystore for Redis, Cloud SQL, and Cloud Scheduler, and from route tables written by hand for the other services ([api/README.md](api/README.md), [design](docs/design.md), [LocalStack comparison](docs/architecture/localstack-comparison.md)).

Scenarios are recorded once against a sandbox Google Cloud project and replayed against the emulator in CI, alongside Terraform acceptance cases run with the pinned google provider (3 extracted from the provider's upstream tests, the rest written locally; [Terraform](docs/contributing/terraform-compatibility.md)) and client-library tests ([validation](docs/contributing/validation.md)).

## Limitations

- Unsupported methods and options return `UNIMPLEMENTED`.
- State is in memory; `reset` and `down` discard it.
- Storage buckets need uniform bucket-level access; ACLs, versioning, and object retention are not modeled. Public access prevention and soft delete work, but deleted buckets are not soft-deleted and organization policies are not applied ([Storage](docs/services/storage.md#gaps)).
- Packaged Cloud Run runs your own images only with `gcp-local --cloud-run-docker up`, which mounts the Docker socket (root-equivalent on the host). Revisions have no internet access ([Cloud Run](docs/services/cloudrun.md)).
- Redis and Cloud SQL listen on `127.0.0.1` ports 4568-4583 by default, not 6379, 5432, or 3306; read the port from the API. Neither accepts TLS.
- The Spanner emulator runs on Linux and in the container image, not natively on macOS.
- Networking verdicts are analytical, not packet routing.
- Vertex AI answers come from a stub or local Ollama, not Gemini.

## Contributing

See [CONTRIBUTING.md](.github/CONTRIBUTING.md) and the [issue tracker](https://github.com/evanmarshall/gcp-local/issues).

## License

MIT. See [LICENSE](LICENSE). Vendored Google API discovery documents under `api/discovery/` keep their Apache-2.0 license.
