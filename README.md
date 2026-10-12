# gcp-local

**A local Google Cloud emulator that enforces IAM.** It runs Cloud Storage, Pub/Sub, Firestore, Spanner, BigQuery, Cloud SQL, Cloud Run, Cloud Logging, Vertex AI, and more on your machine. Use the real client libraries and Terraform, and get the same permission errors as production.

gcp-local issues local credentials and evaluates IAM policies on every request. Each service's behavior is checked against recordings from real Google Cloud.

> **Status: pre-1.0.** Nothing is published to PyPI or a container registry yet; run it from source. Each service implements part of its API ([coverage](docs/coverage.md)).

## Quick start

You need Docker with Compose and Python 3.11+. No Google Cloud account or credentials are used.

```sh
git clone https://github.com/evanmarshall/gcp-local.git
cd gcp-local
python3 scripts/local_env.py up      # builds and starts the environment
python3 scripts/local_env.py env     # prints the endpoint, project, and credential file
```

HTTP is on `http://127.0.0.1:4566` and gRPC on `127.0.0.1:4567`. The project is `gcp-local-test`, and a generated developer identity holds explicit IAM grants. `reset` wipes all data, `down` stops everything, and `logs` shows recent output.

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
# The local Storage provider currently requires these recommended settings.
bucket.iam_configuration.uniform_bucket_level_access_enabled = True
bucket.iam_configuration.public_access_prevention = "enforced"
bucket.soft_delete_policy.retention_duration_seconds = 0
client.create_bucket(bucket, location="us-central1")
bucket.blob("hello.txt").upload_from_string("Hello from local GCP.")
```

More examples are in [`examples/`](examples/): Pub/Sub over gRPC, and a Terraform-provisioned Pub/Sub → Cloud Run → Cloud Storage [workflow](docs/workflow.md). The [quickstart](docs/quickstart.md) covers Terraform credentials, multiple environments, and configuration.

## Supported services

[Service coverage](docs/coverage.md) lists methods served, data-plane support, recordings, and Terraform tests per service.

| Service | Backend | Application use |
| --- | --- | --- |
| [Cloud Storage](docs/storage.md) | fake-gcs-server | Objects: read, write, list, delete |
| [Pub/Sub](docs/pubsub.md) | Official Google emulator | Publish, pull, streaming pull, push |
| [Secret Manager](docs/secretmanager.md) | gcp-local | Secret versions and access |
| [Cloud KMS](docs/cloudkms.md) | gcp-local | Symmetric encrypt and decrypt |
| [Cloud Tasks](docs/cloudtasks.md) | gcp-local | HTTP dispatch with OIDC and retries |
| [Cloud Logging](docs/logging.md) | gcp-local | Write, filter, and delete log entries over REST and gRPC |
| [Vertex AI](docs/vertexai.md) | Deterministic stub or Ollama | Generate, stream, count tokens, embed; answers are not from Gemini |
| [Firestore](docs/firestore.md) | Official Google emulator | Documents, subcollections, queries, transactions, batch writes, listeners |
| [BigQuery](docs/bigquery.md) | gcp-local | Streaming inserts, simple SELECT |
| [Cloud SQL for PostgreSQL](docs/cloudsql.md) | Real PostgreSQL | PostgreSQL clients on `127.0.0.1` with a user password |
| [Memorystore for Redis](docs/memorystore.md) | Real Redis | Redis clients on the reported port, with AUTH |
| [Cloud Run](docs/cloudrun.md) | Local processes or Docker | Invocation; the packaged environment runs only the bundled worker |
| [Spanner](docs/spanner.md) | Official Google emulator | SQL queries, DML, reads, mutations, transactions |
| [Bigtable](docs/bigtable.md) | Official Google emulator code | Row reads, scans, mutations over gRPC; not recorded |
| [Artifact Registry](docs/artifactregistry.md) | gcp-local | Repositories only; no push or pull |
| [IAM](docs/serviceaccounts.md) and [project IAM](docs/project-iam.md) | gcp-local | Service accounts and policies |
| [VPC networking](docs/networking.md) | gcp-local | Connectivity verdicts; no real traffic |

IAM allow policies use predefined roles recorded from Google Cloud. IAM conditions are recorded for Storage; on other services a conditional binding grants nothing. Deny policies and principal access boundaries are modeled locally but not recorded; organization policy and VPC Service Controls are not enforced ([IAM](docs/iam.md)).

## How it works

A single gateway routes requests to per-service providers, following [LocalStack](https://github.com/localstack/localstack)'s architecture. Providers authenticate and authorize before touching state, and reuse real backends (Google's emulators, PostgreSQL, Redis) where they exist. API routes are generated from Google's published API definitions ([design](docs/design.md), [LocalStack comparison](docs/localstack-comparison.md)).

Scenarios are recorded once against a sandbox Google Cloud project and replayed against the emulator in CI, alongside pinned Terraform provider acceptance tests and client-library tests ([validation](docs/validation.md)).

## Limitations

- Unsupported methods and options return `UNIMPLEMENTED`.
- State is in memory; `reset` and `down` discard it.
- Storage buckets need uniform bucket-level access, enforced public access prevention, and no soft-delete retention.
- Packaged Cloud Run runs only the bundled example worker; your own images need the Docker runtime ([Cloud Run](docs/cloudrun.md)).
- Redis and Cloud SQL listen on `127.0.0.1` ports 4568-4583 by default, not 6379 or 5432; read the port from the API. Neither accepts TLS.
- The Spanner emulator runs on Linux and in the container image, not natively on macOS.
- Networking verdicts are analytical, not packet routing.
- Vertex AI answers come from a stub or local Ollama, not Gemini.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) and the [issue tracker](https://github.com/evanmarshall/gcp-local/issues).

## License

Apache-2.0. See [LICENSE](LICENSE).
