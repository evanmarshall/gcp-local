# gcp-local

**A local emulator for Google Cloud that enforces IAM.** Run Cloud Storage, Pub/Sub, Cloud Run, Firestore, Spanner, BigQuery, Cloud SQL, and more on your machine, with the real client libraries and Terraform, and get the same permission errors you would get in production.

Most cloud emulators skip authentication entirely, so code that works locally can still fail in production on a missing grant. gcp-local issues local credentials, evaluates IAM policies on every request, and denies calls the way Google Cloud does. Each service's behavior is checked against recordings captured from real Google Cloud.

> **Status: early.** gcp-local is pre-1.0 and not yet published to PyPI or a container registry; run it from source today (see below). Coverage is a curated slice of each API, listed in [Supported services](#supported-services).

## Quick start

You need Docker (with Compose) and Python 3.11+. No Google Cloud account or credentials are used.

```sh
git clone https://github.com/evanmarshall/gcp-local.git
cd gcp-local
python3 scripts/local_env.py up      # builds and starts the environment
python3 scripts/local_env.py env     # prints the endpoint, project, and credential file
```

This starts every service on `http://127.0.0.1:4566` (HTTP) and `127.0.0.1:4567` (gRPC), with a local project `gcp-local-test` and a generated developer identity that holds explicit IAM grants. `reset` wipes all data, `down` stops everything, and `logs` shows recent output.

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

Full, runnable examples are in [`examples/`](examples/), including Pub/Sub over gRPC and a Terraform-provisioned Pub/Sub → Cloud Run → Cloud Storage [workflow](docs/workflow.md). The [quickstart guide](docs/quickstart.md) covers configuration, Terraform credentials, multiple environments, and the upcoming `pip install gcp-local` and `docker run` options.

## Supported services

"Recorded" means the emulator's responses are compared against a scenario captured from real Google Cloud; each linked page lists exactly which operations are covered and which are not.

| Service | Backend | Verification |
| --- | --- | --- |
| [Cloud Storage](docs/storage.md) | fake-gcs-server, in-process | Recorded, including IAM conditions; Terraform tests |
| [Pub/Sub](docs/pubsub.md) | Official Google emulator | Recorded (pull and IAM); push is modeled locally; Terraform tests |
| [Firestore](docs/firestore.md) | Official Google emulator | Recorded; Terraform tests |
| [Spanner](docs/spanner.md) | Official Google emulator | Recorded; Terraform tests |
| [Bigtable](docs/bigtable.md) | Official Google emulator code | Recorded |
| [Cloud SQL for PostgreSQL](docs/cloudsql.md) | Real PostgreSQL per instance | Recorded |
| [Memorystore for Redis](docs/memorystore.md) | Real Redis per instance | Recorded |
| [BigQuery](docs/bigquery.md) | gcp-local | Recorded (datasets, tables, rows, simple queries); Terraform tests |
| [Secret Manager](docs/secretmanager.md) | gcp-local | Recorded; Terraform tests |
| [Cloud KMS](docs/cloudkms.md) | gcp-local | Recorded; Terraform tests |
| [Cloud Tasks](docs/cloudtasks.md) | gcp-local | Recorded, including dispatch and retry; Terraform tests |
| [Artifact Registry](docs/artifactregistry.md) | gcp-local | Recorded (repositories only); Terraform tests |
| [IAM service accounts](docs/serviceaccounts.md) and [project IAM](docs/project-iam.md) | gcp-local | Recorded; Terraform tests |
| [VPC networking and Connectivity Tests](docs/networking.md) | gcp-local | Recorded, including firewall, peering, and Cloud NAT verdicts; Terraform tests |
| [Cloud Run](docs/cloudrun.md) | Local processes or Docker | Modeled locally, not yet recorded; Terraform tests |

Every service evaluates IAM on each request through one shared policy engine with predefined role definitions recorded from Google Cloud. IAM conditions are supported; deny policies, principal access boundaries, organization policy, and VPC Service Controls are not yet ([IAM details](docs/iam.md)).

## How it works

gcp-local follows the architecture of the archived open-source [LocalStack](https://github.com/localstack/localstack): a single gateway routes requests to per-service providers, providers authenticate and authorize before touching state, and real backends (Google's own emulators, PostgreSQL, Redis) are reused wherever they exist. API routes are generated from Google's published API definitions. See the [design](docs/design.md) and the [comparison with LocalStack](docs/localstack-comparison.md).

Fidelity is tested, not assumed: scenarios are recorded once against a dedicated Google Cloud project, reviewed, and then replayed against the emulator in CI, alongside pinned Terraform provider acceptance tests and client-library tests ([validation](docs/validation.md)).

## Limitations

- Each service implements a slice of its API. Unsupported methods and options return an explicit `UNIMPLEMENTED` error instead of silently succeeding.
- State lives in memory; `reset` and `down` discard it.
- Cloud Storage buckets must use uniform bucket-level access, enforced public access prevention, and no soft-delete retention.
- Cloud Run in the packaged environment runs only the bundled example worker; running your own images requires the Docker runtime ([details](docs/cloudrun.md)).
- The Spanner emulator runs on Linux and in the container image, not natively on macOS.
- Networking verdicts are analytical (like Connectivity Tests), not real packet routing.

## Contributing

Issues and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, checks, and the recording workflow, and the [issue tracker](https://github.com/evanmarshall/gcp-local/issues) for planned work.
