# Run local Cloud Storage and Pub/Sub

You need Python 3.11 or newer and a running Docker engine with Compose supporting `up --wait`. No GCP project, account, API key, gcloud login, Java install, or production credentials are required: the image carries a pinned Java 17 runtime and the checksum-pinned emulator JAR. The first build downloads pinned tools, the JRE, and Go modules; subsequent starts reuse them.

From a fresh checkout:

```sh
python3 scripts/local_env.py up
python3 scripts/local_env.py status
python3 scripts/local_env.py env
```

`up` waits for the container and the published endpoints to become healthy. Storage is available at `http://127.0.0.1:4566` and the Pub/Sub gRPC gateway at `127.0.0.1:4567`; a second environment takes the next port. `status` returns JSON with the version, registered providers, and lazy backend state. If startup fails, the command exits nonzero; use `python3 scripts/local_env.py logs` for the last 100 log lines and `down` to remove that environment.

The command creates `.gcp-local/environment/` with mode 0700 and local identity/IAM files with mode 0600. The synthetic project is `gcp-local-test`; the generated developer identity has explicit local Storage Admin and Pub/Sub Admin grants. `env` prints the ADC path, project, HTTP endpoint, and gRPC endpoint as JSON. Existing `.env`, gcloud settings, and ambient ADC are not loaded or changed.

## Try the SDK

Install the pinned client dependencies in a project tooling environment, then run the example:

```sh
python3 -m venv .venv-tools
.venv-tools/bin/python -m pip install -r requirements-sdk.txt -r requirements-pubsub.txt
.venv-tools/bin/python examples/storage.py
.venv-tools/bin/python examples/pubsub.py
```

The Storage example prints `Hello from local GCP.` after an authenticated upload/download, then removes its private bucket and object. The Pub/Sub example creates one topic and one pull subscription, publishes and pulls one message through the published gRPC gateway with the same local ADC, then removes both. Install `requirements-pubsub.txt` for the second. Both examples confine token refresh and SDK requests to the selected local environment and accept `--directory`.

For an application, apply the values from `env` to that test process, configure the Storage client's `api_endpoint` explicitly, and point a Pub/Sub client's gRPC channel at the printed gRPC endpoint with loopback call credentials. Preserve the SDK's normal credentials; anonymous emulator switches bypass the identity path. Buckets currently require an explicit location, uniform bucket access, enforced public access prevention, and zero soft-delete duration. See [Storage coverage](storage.md) and the [portable agent setup](agent-setup.md).

## Reset and stop

```sh
python3 scripts/local_env.py reset
python3 scripts/local_env.py down
```

Reset recreates this environment: bucket/object data, bucket policies, issued access tokens, and decision history disappear. Bootstrap project/IAM configuration and private enrollment files remain. Recreate SDK clients after reset. `down` removes only the directory's Compose containers and networks; it retains local credentials and shared image/build caches. There are no data volumes or persistence claims.

For a second application or concurrent CI task, choose a separate private directory and unused port:

```sh
python3 scripts/local_env.py --directory /absolute/app/.gcp-local/environment --port 14566 up
python3 scripts/local_env.py --directory /absolute/app/.gcp-local/environment status
python3 scripts/local_env.py --directory /absolute/app/.gcp-local/environment down
```

The directory fixes its port and synthetic project; use a new directory to change either. Its absolute path determines the Compose project name, preventing ordinary worktrees from sharing service state. Keep application `.gcp-local/` directories ignored by Git. The controller refuses changed ownership, moved manifests, and concurrent commands on the same environment.

## Packaging and verification

The [component manifest](../packaging/manifest.json) pins the build image, the Java runtime image, and the HAProxy image by digest; Go, API contracts, role data, backend modules, the emulator JAR, and cloud fixtures retain their existing source/checksum pins. The final emulator image contains its static Go binary, a Java 17 JRE, and the pinned emulator JAR, and reads two explicit private configuration mounts. It runs as the creating user's UID/GID, with a read-only filesystem and no capabilities; the JVM's temporary directory is a Compose tmpfs. The fake-gcs-server library, the Bigtable emulator library (bttest, with `-services bigtable`), and the Pub/Sub JVM start lazily inside their authorized providers and have no separately exposed backend ports.

The core joins only an internal Docker network. A small HAProxy TCP entry point bridges the published loopback port to the fixed gateway address; it does not interpret API paths or credentials. This is needed because a container on an internal-only network did not publish its requested host port during Docker 29.4.3 testing. It preserves the gateway/provider boundary and avoids introducing a custom proxy implementation. See Docker's [network isolation](https://docs.docker.com/reference/compose-file/networks/#internal) and [port publishing](https://docs.docker.com/engine/network/port-publishing/) behavior.

Run `.venv-tools/bin/python scripts/smoke_environment.py` to verify two independent environments, authenticated Storage, the Pub/Sub example, reset isolation, blocked backend IPv4/IPv6 egress, a visible port-conflict failure, agent readiness diagnostics, and complete teardown. CI runs this on Linux; the same workflow is tested with macOS Docker. Images are built locally, not published to a registry. Host application tests still need their own endpoint and network controls; Docker's backend network does not isolate those processes.

The recorded Storage scenarios and the recorded Pub/Sub pull scenario are ready. One real-GCP recording each also backs Secret Manager, IAM service accounts, project IAM, Artifact Registry, Firestore, BigQuery, Cloud Tasks, Cloud KMS, Spanner, Bigtable, and Compute networking with Connectivity Tests, and the local providers match them in comparison mode; see each service doc and [iam.md](iam.md). These services run in the conformance fixture, but the packaged environment above still grants its developer identity only Storage and Pub/Sub. The Cloud Run control plane, and the Pub/Sub push to Cloud Run to Storage workflow are implemented and locally modeled without recordings (see [cloudrun.md](cloudrun.md), [workflow.md](workflow.md)); networking beyond its recording is described in [networking.md](networking.md). Pub/Sub push/streaming fidelity and persistence also remain pending. Terraform with local external-account ADC works only with the adjustments in [workflow.md](workflow.md#terraform-authentication); the packaged environment does not generate that variant.
