# Run gcp-local locally

You need a running Docker engine with Compose supporting `up --wait`, and Python 3.11 or newer for the CLI. No GCP project, account, API key, gcloud login, Java install, or production credentials are required: the image carries the static core, a pinned Java 17 runtime, the checksum-pinned Pub/Sub and Firestore emulator JARs, the Spanner emulator binary, and a pinned Python interpreter for its launcher and bundled Cloud Run worker.

There are three ways to start the same packaged environment. All of them create a private directory with a generated local identity and IAM policy, and none of them load `.env`, gcloud settings, or ambient ADC.

## Install with pip (after the first release)

No image or package has been published yet; this path works once the owner completes the [release setup](#releases) and pushes a version tag.

```sh
pipx install gcp-local        # or: python3 -m pip install gcp-local
gcp-local up
gcp-local status
gcp-local env
```

The installed CLI starts `ghcr.io/evanmarshall/gcp-local:<its own version>`. Select another reference with `--image` on `up` or the `GCP_LOCAL_IMAGE` variable; pin a digest (`ghcr.io/evanmarshall/gcp-local@sha256:...`) for reproducible CI. The wheel carries the Compose definition, so no checkout is needed. `gcp-local --help` lists `up`, `status`, `env`, `reset`, `down`, and `logs`, and `python3 -m gcp_local` is equivalent.

## Run the image with docker run (after the first release)

Without Python or Compose, run the image directly against a private state directory. The core listens on the published port, so use the same number on both sides of the HTTP mapping:

```sh
mkdir -m 700 -p .gcp-local/docker
docker run --detach --name gcp-local \
  --user "$(id -u):$(id -g)" --read-only --tmpfs /tmp:size=256m,mode=1777 \
  --cap-drop ALL --security-opt no-new-privileges:true \
  --publish 127.0.0.1:4566:4566 --publish 127.0.0.1:4567:4567 \
  --publish 127.0.0.1:4568-4583:4568-4583 \
  --env GCP_LOCAL_PORT=4566 \
  --env GCP_LOCAL_HOST_DIRECTORY="$(pwd -P)/.gcp-local/docker" \
  --volume "$(pwd -P)/.gcp-local/docker:/state" \
  ghcr.io/evanmarshall/gcp-local:VERSION
```

On first start the container writes the environment into `/state` (environment manifest, identity, IAM, and an ADC file whose paths name the host directory); later starts reuse it. The third mapping publishes the Redis and Cloud SQL ports; it must use the same numbers on both sides and match `GCP_LOCAL_DATA_PORTS` (default: the 16 ports after `GCP_LOCAL_PORT + 1`). Optional variables: `GCP_LOCAL_DATA_PORTS` (`FIRST-LAST`), `GCP_LOCAL_PROJECT` (default `gcp-local-test`), and `GCP_LOCAL_WORKLOADS` (comma-separated Cloud Run workload accounts, for example `worker`). Wait for `docker inspect --format '{{.State.Health.Status}}' gcp-local` to report `healthy`. The SDK examples and `gcp-local --directory .gcp-local/docker status|env` accept the directory; `reset`, `down`, and `logs` refuse it, so use `docker restart` (which discards in-memory state) and `docker rm --force`. Unlike the Compose environment, this container is on Docker's default bridge network rather than an internal backend network.

## Build from source

From a fresh checkout, the same CLI builds the image locally instead of pulling it:

```sh
python3 scripts/local_env.py up
python3 scripts/local_env.py status
python3 scripts/local_env.py env
```

`scripts/local_env.py` is a thin wrapper around the `gcp_local` package. An editable install (`pip install -e .`) also builds from the checkout; `--image` or `GCP_LOCAL_IMAGE` selects an image instead. The first build downloads pinned tools, the JRE, and Go modules; later starts reuse the build cache.

## What starts

`up` waits for the container and the published endpoints to become healthy. The HTTP endpoint is `http://127.0.0.1:4566` and the gRPC gateway (Pub/Sub, Bigtable, Spanner, Firestore) is `127.0.0.1:4567`. Memorystore and Cloud SQL instances get ports from `127.0.0.1:4568-4583`, one per instance; connect to the port the API reports ([Memorystore](memorystore.md#endpoint-and-auth-semantics), [Cloud SQL](cloudsql.md#connect-from-the-host)). The range follows the HTTP port (`--port 14566` gives 14568-14583), so a second environment needs a base port at least 18 away; `up --data-ports FIRST-LAST` sets up to 64 ports. `status` returns JSON with the version, registered providers, and lazy backend state. If startup fails, the command exits nonzero; `logs` prints the last 100 lines and `down` removes the environment.

The command creates `.gcp-local/environment/` with mode 0700 and owner-only identity/IAM files. The synthetic project is `gcp-local-test`, and its project number is a stable 12-digit value derived from the project ID (the Pub/Sub push contract requires a real-shaped number). `env` prints JSON with the ADC path, project, project number, HTTP and gRPC endpoints, the data-port range (`GCP_LOCAL_DATA_PORTS`), and the Terraform credential file.

Every implemented provider is selected. The generated developer identity receives explicit project-level grants, evaluated per request by the [IAM engine](iam.md) like any other binding; an enrolled identity without grants is refused, and missing credentials never become an administrator.

| Service | Developer roles |
| --- | --- |
| Cloud Storage | Storage Admin |
| Pub/Sub | Pub/Sub Admin |
| Secret Manager | Secret Manager Admin |
| IAM service accounts | Service Account Admin, Service Account User |
| Project IAM (Resource Manager) | Project IAM Admin |
| Artifact Registry | Artifact Registry Admin |
| Firestore | Datastore User, Datastore Index Admin |
| BigQuery | BigQuery Data Owner, BigQuery Job User |
| Cloud Tasks | Cloud Tasks Admin |
| Cloud KMS | Cloud KMS Admin, CryptoKey Encrypter/Decrypter |
| Spanner | Spanner Admin |
| Bigtable | Bigtable Admin |
| Cloud SQL for PostgreSQL | Cloud SQL Admin |
| Memorystore for Redis | Redis Admin |
| Compute networking, Connectivity Tests | Compute Admin, Network Management Admin |
| Cloud Run | Cloud Run Admin |

What each service supports and how it is verified is in [service coverage](coverage.md).

### Cloud Run in the packaged environment

Revisions run through the process runtime inside the core container: the image `gcp-local/workflow:latest` maps to the bundled [workflow worker](../examples/workflow/worker.py), and every other image is refused at launch. Running arbitrary application images would need container-in-container execution, which the packaged environment does not do. `compose.run.yaml` is a manual Compose override that mounts the host Docker socket for the [container runtime](cloudrun.md); the CLI never applies it. Workload identities must be enrolled when the directory is created, for example `up --workload worker`; the grants come from IAM (Terraform or the API), not from enrollment.

### Terraform

`env` names `GCP_LOCAL_TERRAFORM_CREDENTIALS`, the same local external-account ADC with `"universe_domain": "gcp-local.invalid"`, which the google provider needs alongside its `universe_domain` override and the `cloud-platform` scope ([details](workflow.md#terraform-authentication)). The [workflow acceptance](workflow.md) provisions its whole chain this way against the packaged environment with `scripts/smoke_workflow.py --packaged`.

## Try the SDK

Install the pinned client dependencies in a project tooling environment, then run the examples:

```sh
python3 -m venv .venv-tools
.venv-tools/bin/python -m pip install -r requirements-sdk.txt -r requirements-pubsub.txt
.venv-tools/bin/python examples/storage.py
.venv-tools/bin/python examples/pubsub.py
```

The Storage example prints `Hello from local GCP.` after an authenticated upload/download, then removes its private bucket and object. The Pub/Sub example creates one topic and one pull subscription, publishes and pulls one message through the published gRPC gateway with the same local ADC, then removes both. Both examples confine token refresh and SDK requests to the selected local environment and accept `--directory`.

For an application, apply the values from `env` to that test process, configure the client's API endpoint explicitly, and point gRPC clients at the printed gRPC endpoint with loopback call credentials. Preserve the SDK's normal credentials; anonymous emulator switches bypass the identity path. Buckets currently require an explicit location, uniform bucket access, enforced public access prevention, and zero soft-delete duration. See [Storage coverage](storage.md) and the [portable agent setup](agent-setup.md).

## Reset and stop

```sh
gcp-local reset    # or: python3 scripts/local_env.py reset
gcp-local down
```

Reset recreates this environment: every service's resources and policies, Cloud Run revision processes, issued access tokens, and decision history disappear. Bootstrap project/IAM configuration and private enrollment files remain. Recreate SDK clients after reset. `down` removes only the directory's Compose containers and networks; it retains local credentials and shared image/build caches. There are no data volumes or persistence claims.

For a second application or concurrent CI task, choose a separate private directory and unused port:

```sh
gcp-local --directory /absolute/app/.gcp-local/environment --port 14566 up
gcp-local --directory /absolute/app/.gcp-local/environment status
gcp-local --directory /absolute/app/.gcp-local/environment down
```

The directory fixes its ports, synthetic project, and workload enrollments; use a new directory to change them. Its absolute path determines the Compose project name, preventing ordinary worktrees from sharing service state. Keep application `.gcp-local/` directories ignored by Git. The controller refuses changed ownership, moved manifests, and concurrent commands on the same environment. Directories created by an earlier version (before every service was packaged) are refused except for `down`; remove them and start a new one.

## Packaging and verification

The [component manifest](../packaging/manifest.json) pins the build image, the Java runtime image, and the HAProxy image by digest; Go, API contracts, role data, backend modules, emulator artifacts, and cloud fixtures retain their existing source/checksum pins. The Python interpreter is copied from the digest-pinned build image. The image runs as the creating user's UID/GID, with a read-only filesystem and no capabilities; the JVM's temporary directory is a tmpfs. Backends start lazily inside their authorized providers. Only Redis and public-IP Cloud SQL instances expose ports, through the data-port range; Redis AUTH and PostgreSQL passwords still apply.

The Compose core joins only an internal Docker network. A small HAProxy TCP entry point bridges the published loopback ports (HTTP, gRPC, and the data-port range) to the core; it does not interpret API paths, protocols, or credentials. A data port with no instance behind it accepts and immediately closes connections. It exists because a container on an internal-only network did not publish its requested host port during Docker 29.4.3 testing. See Docker's [network isolation](https://docs.docker.com/reference/compose-file/networks/#internal) and [port publishing](https://docs.docker.com/engine/network/port-publishing/) behavior.

- `.venv-tools/bin/python scripts/smoke_environment.py` verifies two independent environments, authenticated Storage, both examples, one read per packaged service allowed for the developer and refused for an ungranted identity and an anonymous caller, Spanner row writes and reads over REST and gRPC and Bigtable row writes and reads over gRPC (refused for the ungranted identity), Redis (with and without AUTH) and Cloud SQL (password SCRAM) used from the host with delete closing their ports, reset isolation, blocked backend IPv4/IPv6 egress, a visible port-conflict failure, agent readiness diagnostics, a standalone `docker run` with both examples, and complete teardown.
- `.venv-tools/bin/python scripts/smoke_workflow.py --packaged` runs the Terraform workflow against an environment started with `scripts/local_env.py up`, then requires `reset` to leave no buckets, topics, subscriptions, Cloud Run services, service accounts, or worker processes, and `down` to remove its containers.
- `.venv-tools/bin/python scripts/smoke_package.py` builds the wheel, installs it in a fresh virtual environment, and drives the installed `gcp-local` script outside the checkout against a selected image.

CI runs all three on Linux; they were also run on macOS Docker. Host application tests still need their own endpoint and network controls; Docker's backend network does not isolate those processes.

## Releases

`.github/workflows/release.yml` runs only for a pushed `vMAJOR.MINOR.PATCH` tag equal to `gcp_local.__version__`. It builds the sdist and wheel with build provenance and `SHA256SUMS`, tests the wheel's CLI against a locally built image, pushes a `linux/amd64` + `linux/arm64` image to GHCR with BuildKit provenance, an SBOM, and a GitHub build attestation, publishes the distributions to PyPI through trusted publishing, and creates a GitHub release with generated notes, the checksums, and the image digest. No tag has been pushed and nothing has been published.

Before the first tag, the repository owner must:

1. Allow the workflow to publish `ghcr.io/evanmarshall/gcp-local` (Actions workflow permissions with package write; after the first push, link the package to this repository and make it public for anonymous pulls).
2. Register a PyPI trusted publisher for project `gcp-local`, owner `evanmarshall`, repository `gcp-local`, workflow `release.yml`, environment `pypi`. The `pypi` environment exists; add required reviewers to it if you want a manual gate.
3. Make the repository public, or the README's clone instructions and the GHCR image link will not work for other people.
4. Bump `gcp_local/__init__.py` and tag that commit `v<version>`.

Verify a release with `gh attestation verify --owner evanmarshall oci://ghcr.io/evanmarshall/gcp-local:<version>` and `sha256sum --check SHA256SUMS`.
