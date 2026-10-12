# Cloud Run

Cloud Run v2 services, revisions, and operations over REST and gRPC, with containers run by Docker or as local processes and invocation gated by ID token and `run.invoker`. Modeled locally; nothing is recorded against GCP.

## Run your own image

Start the packaged environment in Docker mode, then deploy any image your Docker engine has or can pull anonymously:

```sh
docker build -t my-app:dev .
gcp-local --cloud-run-docker up      # or: python3 scripts/local_env.py --cloud-run-docker up
gcp-local env                        # GCP_LOCAL_RUN_ENDPOINT is http://gcp-local:<port>
```

**`--cloud-run-docker` mounts the host's Docker socket into the core container. Anything with that socket controls the host's Docker, which is root-equivalent on the host.** It is off by default. Without it, the packaged environment runs only the bundled `gcp-local/workflow:latest` worker as a process and refuses other images.

Deploy with Terraform or the API as usual (`image = "my-app:dev"`). Each revision is a container that:

- receives only `K_SERVICE`, `K_REVISION`, `K_CONFIGURATION`, `PORT`, `GCE_METADATA_HOST`, and the service's plain-text `env` (plus the image's own `ENV`). No host credentials, files, or Docker socket are mounted.
- runs as `template.serviceAccount` through its own metadata server at `GCE_METADATA_HOST`, which answers only that container ([local identity](local-identity.md)). A revision without a service account gets no metadata server.
- joins the environment's internal network. It reaches gcp-local at `GCP_LOCAL_RUN_ENDPOINT` (REST, e.g. `http://gcp-local:4566/storage/v1`) and `GCP_LOCAL_RUN_GRPC_ENDPOINT` (`gcp-local:4567`); pass these through `env`. It has no internet access, so it cannot reach real Google APIs or your cloud credentials.
- is labeled `gcp-local.environment=<compose project>`. `reset` and `down` remove every container with this environment's label and no others; the core also removes them when it starts and stops.

Images:

- A local image (built or tagged with `docker`) runs as is, whatever its name.
- A missing image is pulled anonymously by the engine during `create`. Neither the host's nor the container's registry credentials are sent. For a private image, `docker pull` it yourself first.
- Missing `gcr.io`, `*.gcr.io`, and `*.pkg.dev` images are refused: Artifact Registry here stores [repository metadata only](artifactregistry.md), and gcp-local never pulls from Google. `docker tag my-app:dev us-docker.pkg.dev/p/r/app:1` makes such a name local.
- Missing `gcp-local/...` images are refused; build the bundled worker with `docker build -t gcp-local/workflow:latest examples/workflow`.

Without the CLI, add Docker mode to the [`docker run` command](quickstart.md#run-with-docker-run):

```sh
docker network create --internal gcp-local-run
# Replace `docker run --detach` with `docker create` and add:
#   --volume /var/run/docker.sock:/var/run/docker.sock --group-add SOCKET_GID
#   --env GCP_LOCAL_RUN_DOCKER_SOCKET=/var/run/docker.sock
#   --env GCP_LOCAL_RUN_DOCKER_NETWORK=gcp-local-run --env GCP_LOCAL_RUN_ENVIRONMENT=standalone
docker network connect gcp-local-run gcp-local   # before the first start
docker start gcp-local
```

`SOCKET_GID` is the socket's group inside the engine (`0` on Docker Desktop). Revisions reach the core at `http://gcp-local:<GCP_LOCAL_PORT>`. `docker stop gcp-local` removes the revisions; after `docker rm --force`, run `docker rm -f $(docker ps -aq --filter label=gcp-local.environment=standalone)`.

## Invoke

Call `http://127.0.0.1:4566/_run/{service}-{project}.{location}.run.app/...` with a local ID token whose audience is `https://{service}-{project}.{location}.run.app`. Pub/Sub push and Cloud Tasks mint these tokens; there is no public ID-token endpoint. The gateway checks the audience and `run.routes.invoke` before forwarding, and the container never sees the caller's `Authorization` header.

## Core flags

```sh
bin/gcp-local -services run -identity-config ... -iam-config ... \
  -run-docker-socket /var/run/docker.sock
```

- `-run-docker-socket` runs one container per revision and publishes its port on the default bridge, for a core on the Docker host. Without a socket or a process mapping, every launch is refused.
- `-run-docker-network NAME` and `-run-docker-environment LABEL` attach revisions to a network the core shares and label them; the packaged Docker mode sets both. Either without a socket is refused, and a network needs a label.
- `-run-process IMAGE=COMMAND` runs a mapped image as a local process in its own process group. It can't be combined with `-run-docker-socket`. The revision is ready only once a process in that group listens on its `PORT` (checked through `/proc` on Linux and `lsof` elsewhere), so another program that takes the port first can't make it look ready.

## Methods

Routes are `/v2/projects/{project}/locations/{location}/...`.

| Resource | Methods | Notes |
| --- | --- | --- |
| Services | `create`, `get`, `list`, `patch`, `delete` | `serviceId` query; one container; port from `ports[0].containerPort` (default 8080) |
| Services | `getIamPolicy`, `setIamPolicy` | `allUsers` with `roles/run.invoker` allows anonymous calls |
| Revisions | `get`, `list`, `delete` | Stopped after 5 minutes idle; the next call cold-starts |
| Operations | `get`, `list`, `delete` | Create and update operations finish when the container is ready; an early exit fails them. A finished operation never changes. Deletes return a finished operation |

Containers launch outside the provider's state lock. An update that loses a race with another update returns 409 `ABORTED` and starts nothing; a service deleted while its create launches finishes that rollout with `ABORTED`. Requests racing a delete read 404.

The same 13 methods run over gRPC on port 4567 (`google.cloud.run.v2.Services`, `google.cloud.run.v2.Revisions`, and `google.longrunning.Operations`), with the same permissions. Operation metadata and response are the `Service` (or the deleted `Revision`), so client LRO helpers work. Point the client at the gRPC endpoint with local ADC on a plaintext local channel:

```python
import google.auth, grpc
from google.auth.transport.grpc import secure_authorized_channel
from google.auth.transport.requests import Request
from google.cloud import run_v2
from google.cloud.run_v2.services.services.transports import ServicesGrpcTransport

credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
channel = secure_authorized_channel(credentials, Request(), "127.0.0.1:4567",
                                    ssl_credentials=grpc.local_channel_credentials())
services = run_v2.ServicesClient(transport=ServicesGrpcTransport(channel=channel))
service = services.create_service(parent="projects/my-project/locations/us-central1",
                                  service_id="app", service=run_v2.Service(...)).result()
```

Tested clients are listed in [coverage](coverage.md#client-compatibility).

## IAM

Permissions are `run.services.*`, `run.revisions.*`, `run.operations.*`, and `run.routes.invoke`. Pinned roles: `roles/run.admin`, `.developer`, `.invoker`, `.viewer`.

Every create and update deploys a revision, and needs `iam.serviceAccounts.actAs` on its runtime service account, as in GCP ([service identity](https://cloud.google.com/run/docs/configuring/services/service-identity)). Grant `roles/iam.serviceAccountUser` on the account (or the project). Without it the call is 403 `Permission 'iam.serviceaccounts.actAs' denied on service account EMAIL (or it may not exist).` and nothing is stored or launched; the same goes for an account that isn't declared. A template without `serviceAccount` deploys as the default compute account, `NUMBER-compute@developer.gserviceaccount.com`: the check uses that account's policy when the environment declares it, and the project's otherwise. The revision still gets no metadata server. An account from another project is refused with `UNIMPLEMENTED`.

A deleted service's policy goes with it; a service recreated with the same name starts empty.

## Verification

```sh
sh scripts/go.sh test -race ./internal/providers/run ./cmd/gcp-local      # includes the Go client over gRPC
sh scripts/check_cloudrun_sdk.sh                                          # Linux: google-cloud-run over gRPC
.venv-tools/bin/python scripts/smoke_workflow.py --packaged --cloud-run-docker   # Docker
.venv-tools/bin/python scripts/smoke_cloudrun.py                                 # Linux with Docker
```

The Docker-mode smoke builds `examples/workflow` as a user image, deploys it with Terraform, delivers Pub/Sub pushes with ID tokens, checks that the worker writes to Storage as its own service account, and checks that one environment's `reset` and `down` leave another environment's revision running. `TestAccCloudRunV2Service` passes ([Terraform](terraform-compatibility.md)).

## Gaps

- No recordings.
- Revision containers have no internet egress.
- `ingress` is stored but not enforced; token and `run.invoker` are the only boundary. VPC egress and Direct VPC are not modeled, and the [networking analyzer](networking.md) has no Cloud Run endpoints.
- No jobs, volumes (including `/cloudsql`), or `valueSource` environment variables.
- A pull happens inside `create` and can take up to 5 minutes.
- State is in memory.
- `TestIamPermissions`, `CancelOperation`, and `WaitOperation` return `UNIMPLEMENTED`. So do the request options `validateOnly`, `allowMissing`, `showDeleted`, `updateMask`, `etag`, `pageSize`, `pageToken`, and `filter` when set; lists return everything on one page.
- Service fields other than the template's one container (image, port, plain `env`), `serviceAccount`, `revision`, `ingress`, and `traffic` are accepted and ignored.
- A service's rollout operations share one name; a later rollout replaces the earlier one.
