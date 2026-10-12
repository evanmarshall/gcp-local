# Quickstart

`gcp-local up` starts every service in one Docker container with a generated local identity and IAM policy. You need Docker with Compose (`up --wait` support) and Python 3.11+. No GCP account, gcloud login, Java, or real credentials are used; `.env`, gcloud settings, and ambient ADC are never read.

## Start

From a checkout (builds the image locally; the first build downloads pinned tools):

```sh
python3 scripts/dev/local_env.py up
python3 scripts/dev/local_env.py status
python3 scripts/dev/local_env.py env
```

After the first release, the same commands come from the package:

```sh
pipx install gcp-local        # or: python3 -m pip install gcp-local
gcp-local up
gcp-local env
```

The packaged CLI pulls `ghcr.io/evanmarshall/gcp-local:<its version>`. `--image` or `GCP_LOCAL_IMAGE` selects another image; pin a digest for reproducible CI. `python3 -m gcp_local` is equivalent to `gcp-local`. Nothing has been published yet ([Releases](#releases)).

| Command | Does |
| --- | --- |
| `up` | Creates the environment directory if needed, starts the container, waits for health |
| `status` | Prints JSON: version, providers, backend state |
| `env` | Prints JSON with the variables below |
| `reset` | Deletes all resources, policies, Cloud Run revisions, and issued tokens |
| `down` | Removes the containers (Cloud Run revisions included) and networks; keeps credentials |
| `logs` | Prints the last 100 log lines |

Options go before the command: `--directory`, `--port`, `--data-ports FIRST-LAST`, `--project`, `--workload ACCOUNT_ID` (repeatable), `--image`, `--cloud-run-docker`. `up` sets `--image` and `--cloud-run-docker` each time; `reset` keeps them. The directory fixes its ports, project, and workloads on first `up`; use a new directory to change them.

## Endpoints and variables

| `env` key | Value |
| --- | --- |
| `GCP_LOCAL_ENDPOINT` | `http://127.0.0.1:4566` (all REST APIs) |
| `GCP_LOCAL_GRPC_ENDPOINT` | `127.0.0.1:4567` (Pub/Sub, Bigtable and Spanner data and admin, Firestore, Cloud Logging, Secret Manager v1, Cloud Tasks v2, Cloud Scheduler v1, Cloud KMS, Cloud Run v2, IAM service accounts) |
| `GCP_LOCAL_GRPC_TLS_ENDPOINT` | `127.0.0.1:4584`: the same gRPC services over TLS, for Node.js and Python's Spanner, Bigtable, and Logging clients ([gRPC over TLS](../contributing/clients.md#grpc-over-tls)) |
| `GCP_LOCAL_GRPC_TLS_CA` | `<directory>/tls/ca.pem`, the environment's own CA certificate |
| `GCP_LOCAL_DATA_PORTS` | `4568-4583`: one port per Redis or public-IP Cloud SQL instance |
| `GOOGLE_APPLICATION_CREDENTIALS` | `<directory>/identity/adc.json` (default directory `.gcp-local/environment`) |
| `GOOGLE_CLOUD_PROJECT` | `gcp-local-test` |
| `GCP_LOCAL_PROJECT_NUMBER` | A stable 12-digit number derived from the project ID |
| `GCP_LOCAL_TERRAFORM_CREDENTIALS` | `<directory>/identity/terraform-adc.json`, the ADC marked for Terraform |

The data-port range follows `--port`: `--port 14566` gives 14568-14583, and the gRPC TLS port is the next one (14584). A second environment needs a base port at least 19 away. `--data-ports` sets up to 64 ports. Connect to Redis and Cloud SQL on the port the API reports ([Memorystore](../services/databases/memorystore.md#endpoint-and-auth-semantics), [Cloud SQL](../services/databases/cloudsql.md#connect-from-the-host)).

## Use it from an application

Set the `env` values in your test process, set each client's API endpoint explicitly, and keep the SDK's normal credential loading. Don't use anonymous emulator switches; they skip IAM. Point gRPC clients at the gRPC endpoint.

```sh
python3 -m venv .venv-tools
.venv-tools/bin/python -m pip install -r requirements/sdk.txt -r requirements/pubsub.txt
.venv-tools/bin/python examples/storage.py   # prints "Hello from local GCP."
.venv-tools/bin/python examples/pubsub.py    # publishes and pulls one message over gRPC
```

Both examples clean up after themselves and accept `--directory`. Storage buckets need an explicit location and uniform access ([Storage](../services/storage.md)). To set up an application repository with an agent, see [agent setup](agent-setup.md).

## Terraform

Use the file in `GCP_LOCAL_TERRAFORM_CREDENTIALS` with `universe_domain = "gcp-local.invalid"`, the `cloud-platform` scope, and per-service endpoint overrides. [The workflow](workflow.md#terraform-authentication) has the full provider block.

## Developer roles

The generated developer identity gets these project-level grants. An identity without grants is refused, and a request without credentials never becomes an administrator.

| Service | Roles |
| --- | --- |
| Cloud Storage | `roles/storage.admin` |
| Pub/Sub | `roles/pubsub.admin` |
| Secret Manager | `roles/secretmanager.admin` |
| IAM service accounts | `roles/iam.serviceAccountAdmin`, `roles/iam.serviceAccountUser` |
| Project IAM | `roles/resourcemanager.projectIamAdmin` |
| Artifact Registry | `roles/artifactregistry.admin` |
| Firestore | `roles/datastore.user`, `roles/datastore.indexAdmin` |
| BigQuery | `roles/bigquery.dataOwner`, `roles/bigquery.jobUser` |
| Cloud Tasks | `roles/cloudtasks.admin` |
| Cloud Scheduler | `roles/cloudscheduler.admin`; the project's Cloud Scheduler service agent holds `roles/cloudscheduler.serviceAgent`, as GCP grants it |
| Cloud Logging | `roles/logging.admin` |
| Vertex AI | `roles/aiplatform.user` |
| Cloud KMS | `roles/cloudkms.admin`, `roles/cloudkms.cryptoKeyEncrypterDecrypter` |
| Spanner | `roles/spanner.admin` |
| Bigtable | `roles/bigtable.admin` |
| Cloud SQL | `roles/cloudsql.admin` |
| Memorystore for Redis | `roles/redis.admin` |
| Compute networking, Connectivity Tests | `roles/compute.admin`, `roles/networkmanagement.admin` |
| Cloud Run | `roles/run.admin` |

## Cloud Run in the packaged environment

By default, revisions run as processes inside the container. Only the image `gcp-local/workflow:latest` runs; it maps to the bundled [workflow worker](../../examples/workflow/worker.py), and other images are refused.

To run your own images, start with `gcp-local --cloud-run-docker up`. Each revision then runs as a Docker container on the environment's network. This mounts the host's Docker socket, which is root-equivalent access to the host; it is off by default. See [run your own image](../services/cloudrun.md#run-your-own-image) for what the container gets and which images are pulled.

Enroll workload identities on first `up` (`--workload worker`); grant them roles through IAM.

## Multiple environments

Give each application or CI job its own directory and port:

```sh
gcp-local --directory /absolute/app/.gcp-local/environment --port 14566 up
gcp-local --directory /absolute/app/.gcp-local/environment down
```

Keep `.gcp-local/` out of Git. The CLI refuses a directory with changed ownership, a moved manifest, or a concurrent command. Directories from an earlier version are refused except for `down`; delete them and start fresh.

## Run with docker run

Without Python or Compose (after the first release):

```sh
mkdir -m 700 -p .gcp-local/docker
docker run --detach --name gcp-local \
  --user "$(id -u):$(id -g)" --read-only --tmpfs /tmp:size=256m,mode=1777 \
  --cap-drop ALL --security-opt no-new-privileges:true \
  --publish 127.0.0.1:4566:4566 --publish 127.0.0.1:4567:4567 \
  --publish 127.0.0.1:4568-4583:4568-4583 --publish 127.0.0.1:4584:4584 \
  --env GCP_LOCAL_PORT=4566 \
  --env GCP_LOCAL_HOST_DIRECTORY="$(pwd -P)/.gcp-local/docker" \
  --volume "$(pwd -P)/.gcp-local/docker:/state" \
  ghcr.io/evanmarshall/gcp-local:VERSION
```

- Port mappings must use the same number on both sides. The data range must match `GCP_LOCAL_DATA_PORTS` (default: the 16 ports after `GCP_LOCAL_PORT + 1`) and the TLS port `GCP_LOCAL_GRPC_TLS_PORT` (default `GCP_LOCAL_PORT + 18`).
- Optional variables: `GCP_LOCAL_DATA_PORTS`, `GCP_LOCAL_GRPC_TLS_PORT`, `GCP_LOCAL_PROJECT` (default `gcp-local-test`), `GCP_LOCAL_WORKLOADS` (comma-separated, e.g. `worker`).
- Wait for `docker inspect --format '{{.State.Health.Status}}' gcp-local` to print `healthy`.
- `gcp-local --directory .gcp-local/docker status|env` and the examples work. `reset`, `down`, and `logs` don't; use `docker restart` (clears state) and `docker rm --force`.
- The container is on Docker's default bridge network, not an internal backend network.
- Cloud Run runs only the bundled worker unless you set up [Docker mode](../services/cloudrun.md#run-your-own-image) by hand.

## Limits

- State is in memory. There are no volumes.
- The container runs as your UID/GID with a read-only filesystem and no capabilities. Backends sit on an internal Docker network; a TCP proxy publishes only the loopback ports.
- A data port with no instance accepts and immediately closes connections.
- Your application processes on the host are not network-isolated by Docker.

## Releases

Pushing a `vMAJOR.MINOR.PATCH` tag equal to `gcp_local.__version__` runs `.github/workflows/release.yml`. It publishes the wheel and sdist to PyPI with `SHA256SUMS`, and a `linux/amd64` + `linux/arm64` image to GHCR with provenance, an SBOM, and an attestation. No tag has been pushed.

Before the first tag, the owner must give Actions package write access for `ghcr.io/evanmarshall/gcp-local`, register a PyPI trusted publisher (project `gcp-local`, workflow `release.yml`, environment `pypi`), and make the repository public.

Verify a release:

```sh
gh attestation verify --owner evanmarshall oci://ghcr.io/evanmarshall/gcp-local:<version>
sha256sum --check SHA256SUMS
```
