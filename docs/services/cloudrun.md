# Cloud Run

Cloud Run v2 services, revisions, and operations over REST and gRPC, with containers run by Docker or as local processes, traffic splits and tag URLs, ingress, and scaling, and invocation gated by ID token and `run.invoker`. The scenarios `run-service-lifecycle` and `run-traffic` are recorded against GCP over REST; gRPC is not recorded.

## Run your own image

Start the packaged environment in Docker mode, then deploy any image your Docker engine has or can pull anonymously:

```sh
docker build -t my-app:dev .
gcp-local --cloud-run-docker up      # or: python3 scripts/dev/local_env.py --cloud-run-docker up
gcp-local env                        # GCP_LOCAL_RUN_ENDPOINT is http://gcp-local:<port>
```

**`--cloud-run-docker` mounts the host's Docker socket into the core container. Anything with that socket controls the host's Docker, which is root-equivalent on the host.** It is off by default. Without it, the packaged environment runs only the bundled `gcp-local/workflow:latest` worker as a process and refuses other images.

Deploy with Terraform or the API as usual (`image = "my-app:dev"`). Each revision is a container that:

- receives only `K_SERVICE`, `K_REVISION`, `K_CONFIGURATION`, `PORT`, `GCE_METADATA_HOST`, and the service's plain-text `env` (plus the image's own `ENV`). No host credentials, files, or Docker socket are mounted.
- runs as `template.serviceAccount` through its own metadata server at `GCE_METADATA_HOST`, which answers only that container ([local identity](../guides/local-identity.md)). A revision without a service account gets no metadata server.
- joins the environment's internal network. It reaches gcp-local at `GCP_LOCAL_RUN_ENDPOINT` (REST, e.g. `http://gcp-local:4566/storage/v1`) and `GCP_LOCAL_RUN_GRPC_ENDPOINT` (`gcp-local:4567`); pass these through `env`. It has no internet access, so it cannot reach real Google APIs or your cloud credentials.
- is labeled `gcp-local.environment=<compose project>`. `reset` and `down` remove every container with this environment's label and no others; the core also removes them when it starts and stops.

Images:

- A local image (built or tagged with `docker`) runs as is, whatever its name.
- A missing image is pulled anonymously by the engine during `create`. Neither the host's nor the container's registry credentials are sent. For a private image, `docker pull` it yourself first.
- Missing `gcr.io`, `*.gcr.io`, and `*.pkg.dev` images are refused: Artifact Registry here stores [repository metadata only](artifactregistry.md), and gcp-local never pulls from Google. `docker tag my-app:dev us-docker.pkg.dev/p/r/app:1` makes such a name local.
- Missing `gcp-local/...` images are refused; build the bundled worker with `docker build -t gcp-local/workflow:latest examples/workflow`.

Without the CLI, add Docker mode to the [`docker run` command](../guides/quickstart.md#run-with-docker-run):

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

Call the service's `uri`, `http://127.0.0.1:4566/_run/{service}-{project}.{location}.run.app/...`, with a local ID token whose audience is that `uri` or `https://{service}-{project}.{location}.run.app`. Pub/Sub push, Cloud Tasks, Cloud Scheduler, and IAM Credentials `generateIdToken` mint these tokens. The gateway checks ingress, the audience, and `run.routes.invoke` before it cold-starts or forwards anything, and the container never sees the caller's `Authorization` header. The path and query reach the container as sent.

### Traffic and tag URLs

`traffic` takes `LATEST` and `REVISION` targets with percents and tags, as in GCP:

- A tagged target is served at `http://127.0.0.1:4566/_run/{tag}---{service}-{project}.{location}.run.app/...`, GCP's `https://{tag}---{service}-{hash}.a.run.app`. `trafficStatuses[].uri` names it. A tag URL takes a token for the service `uri`, not the tag URL: GCP answers 401 to that.
- Each request to the service URL goes to one revision by a weighted choice over the percents in effect. The choice is a seeded PRNG (`Config.TrafficSeed`, default 0), so one seed gives one sequence of choices; GCP's choice is probabilistic and not recorded.
- Percents must sum to 100 (`traffic[].percent: traffic percentage adds to 90, should be 100`) and tags must be unique (`spec.traffic: may not contain a duplicate tag.`), both 400 with a `BadRequest` violation. A target naming a missing revision is stored, and its rollout fails with code 9: `Revision 'NAME' does not exist or is deleted. Update all traffic to route to currently available revisions.`
- `trafficStatuses` is the routing in effect. `LATEST` resolves to the latest ready revision; targets of one revision merge into one entry; entries sort by revision. A failed rollout leaves it unchanged.
- A change to traffic, ingress, or metadata alone creates no revision. Revisions are numbered by how many were created (`svc-00003` can be generation 6).
- `revisions.delete` refuses the latest created revision (`The latest created Revision "NAME" cannot be directly deleted.`) and one with a percent (`Revision "NAME" cannot be directly deleted because it is actively serving.`), both 400 `FAILED_PRECONDITION`. A revision that is only tagged deletes; its targets leave the traffic block and the generation moves on.

### Ingress

`INGRESS_TRAFFIC_INTERNAL_ONLY` and `INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER` answer external callers 404, before authentication, as GCP does. Internal callers are gcp-local's own Pub/Sub push, Cloud Tasks, and Cloud Scheduler deliveries to a service in the same project, matching [GCP's list](https://cloud.google.com/run/docs/securing/ingress). Each dispatcher marks such a request with a header signed by a key that never leaves the process and bound to the project, method, host, and path; the marked request is internal only if the project matches the service's. Everyone else is external: the host, other machines, and revision containers, which have no VPC egress. No load balancer exists locally, so both internal settings admit the same callers.

### Scaling

`template.scaling.minInstanceCount`, `maxInstanceCount`, and `template.maxInstanceRequestConcurrency` (default 80, at most 1000) are stored, validated with GCP's messages, and echoed on the template and the revision. One process stands in for every instance of a revision:

- A revision serves at most `maxInstanceCount` (default 100) × concurrency requests at once. One more is 429 `The request was aborted because there was no available instance.`, GCP's [documented](https://cloud.google.com/run/docs/troubleshooting) answer; GCP's queueing before that 429 is not modeled or recorded.
- `minInstanceCount` of 1 or more keeps a revision that receives traffic from the idle stop.

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
| Services | `create`, `get`, `list`, `patch`, `delete` | `serviceId` query; one container; port from `ports[0].containerPort` (default 8080); `list` pages by name |
| Services | `getIamPolicy`, `setIamPolicy` | `allUsers` with `roles/run.invoker` allows anonymous calls; conditional bindings are stored |
| Revisions | `get`, `list`, `delete` | `list` pages newest first. `delete` refuses the latest and serving revisions ([traffic](#traffic-and-tag-urls)) |
| Operations | `get`, `list`, `delete`, `wait` | One operation per rollout and per delete; `list` pages newest first. A rollout finishes when the container is ready; an early exit fails it. A rollout or delete reads `done` no sooner than 2 s after it starts. A finished operation never changes |

Bodies decode strictly: an unknown name or a mistyped value is GCP's 400 with a `BadRequest` violation. A page token gcp-local didn't issue is GCP's 500 `Internal error encountered.`; a negative `pageSize` is the default (100; at most 1000, not recorded). Revision names are `SERVICE-00001` without GCP's random suffix. Every revision container stops after 5 minutes without a request and cold-starts on the next one.

Containers launch outside the provider's state lock. An update that loses a race with another update returns 409 `ABORTED` and starts nothing; a service deleted while its create launches finishes that rollout with `ABORTED`. Requests racing a delete read 404.

The same 14 methods run over gRPC on port 4567 (`google.cloud.run.v2.Services`, `google.cloud.run.v2.Revisions`, and `google.longrunning.Operations`), with the same permissions. Operation metadata and response are the `Service` (or the deleted `Revision`), so client LRO helpers work. Point the client at the gRPC endpoint with local ADC on a plaintext local channel:

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

Tested clients are listed in [coverage](../coverage.md#client-compatibility).

## IAM

Permissions are `run.services.*`, `run.revisions.*`, `run.operations.*`, and `run.routes.invoke`. Pinned roles: `roles/run.admin`, `.developer`, `.invoker`, `.viewer`.

Every create and update deploys a revision, and needs `iam.serviceAccounts.actAs` on its runtime service account, as in GCP ([service identity](https://cloud.google.com/run/docs/configuring/services/service-identity)). Grant `roles/iam.serviceAccountUser` on the account (or the project). Without it the call is 403 `Permission 'iam.serviceaccounts.actAs' denied on service account EMAIL (or it may not exist).` and nothing is stored or launched; the same goes for an account that isn't declared. A template without `serviceAccount` deploys as the default compute account, `NUMBER-compute@developer.gserviceaccount.com`: the check uses that account's policy when the environment declares it, and the project's otherwise. The revision still gets no metadata server. An account from another project is refused with `UNIMPLEMENTED`.

A deleted service's policy goes with it; a service recreated with the same name starts empty.

A denial is 403 `Permission 'PERMISSION' denied on resource 'NAME' (or resource may not exist).` with no details, naming the service (also for create), `.../services` for list, or the operation. Anonymous invocation of a private service is 403; an invalid token is 401.

Conditions on a service binding see `request.time` but no resource attributes, as in GCP: a condition on `resource.name`, `resource.type`, or `resource.service` grants nothing (recorded). Project-level conditions on Cloud Run resources are not recorded and get no resource attributes either.

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `run-service-lifecycle` | 2026-10-13 | Create and wait (operation shape, `reconciling`), service fields (traffic, `trafficStatuses`, revisions, conditions, ingress, launch stage), anonymous and invalid-token invocation, four denials with messages, list order and paging, bad page token, negative page size, a rollout with its own operation, revisions and operations newest first, a denied operation read, a `request.time` and a resource-attribute conditional binding, unknown and mistyped fields, delete operation, operation delete |
| `run-traffic` | 2026-10-13 | A 50/50 split with a tag (`traffic`, `trafficStatuses`, tag URL), a traffic-only update (no revision, merged statuses), invocation with an ID token of the service URL and through the tag URL with each audience and anonymously, which revision answered, refused deletes of the latest and a serving revision, a tagged revision's delete, the sum, duplicate-tag, and missing-revision errors, scaling and concurrency errors and echo, revision numbering, `INTERNAL_ONLY` and `INTERNAL_LOAD_BALANCER` answering external callers 404 |

```sh
sh scripts/go.sh test -race ./internal/providers/run ./cmd/gcp-local      # includes the Go client over gRPC
sh scripts/check/sdk/cloudrun.sh                                          # Linux: google-cloud-run over gRPC
.venv-tools/bin/python scripts/smoke/workflow.py --packaged --cloud-run-docker   # Docker
.venv-tools/bin/python scripts/smoke/cloudrun.py                                 # Linux with Docker
```

`tests/clients/python/run.py` deploys a split with `google-cloud-run`, mints an ID token through IAM Credentials, and invokes the tag URL.

The Docker-mode smoke builds `examples/workflow` as a user image, deploys it with Terraform, delivers Pub/Sub pushes with ID tokens, checks that the worker writes to Storage as its own service account, and checks that one environment's `reset` and `down` leave another environment's revision running. `TestLocalCloudRunV2Service` deploys a split with a tag and scaling, then moves all traffic behind internal ingress ([Terraform](../contributing/terraform-compatibility.md)).

## Gaps

- Recorded over REST only.
- Revision containers have no internet egress, and requests they send to other services are external.
- One process serves each revision: `maxInstanceCount` scales its capacity, not its process count, and GCP's request queueing and autoscaling latency are not modeled. The 429 at capacity is from GCP's documentation, not recorded.
- `ingress` `INGRESS_TRAFFIC_NONE` returns `UNIMPLEMENTED`. Requests from inside a VPC, a VPC Service Controls perimeter, Eventarc, and Workflows can't be internal: none exist locally.
- A shape error in one traffic target (no `type`, `LATEST` naming a revision, a `REVISION` target without one, a percent over 100, a malformed tag) is a 400 in gcp-local's wording; GCP's isn't recorded. A second tag on one revision gets its own status entry; GCP's merge of that is not recorded.
- Every other unmodeled service field returns `UNIMPLEMENTED` naming it when set: service-level `scaling`, `template.vpcAccess`, `template.timeout`, `template.containers.resources` (limits), probes, `template.encryptionKey`, `binaryAuthorization`, `template.volumes`, `template.labels`, `valueSource` env, `etag`, and the rest of the v2 schema. So do more than one container and a port named other than `http1`.
- VPC egress and Direct VPC are not modeled, and the [networking analyzer](networking.md) has no Cloud Run endpoints. No jobs.
- A pull happens inside `create` and can take up to 5 minutes. An update's launch failure is a synchronous 500, not a failed operation.
- State is in memory.
- `TestIamPermissions` returns `UNIMPLEMENTED`. GCP serves no operations `cancel` for Cloud Run (404); gcp-local returns `UNIMPLEMENTED`. The request options `validateOnly`, `allowMissing`, `showDeleted`, `updateMask`, and `etag` return `UNIMPLEMENTED` when set, and so do `filter` on operations list and `updateMask` or audit configs on `setIamPolicy`.
- A `wait` whose timeout passes returns the operation not done, as GCP does; GCP's not-done metadata is a partial `Service`, gcp-local's the full one.
