# Cloud Run

Cloud Run v2 services, revisions, and operations over REST, with containers run by Docker or as local processes and invocation gated by ID token and `run.invoker`. Modeled locally; nothing is recorded against GCP.

## Use

```sh
bin/gcp-local -services run -identity-config ... -iam-config ... \
  -run-docker-socket /var/run/docker.sock
```

- `-run-docker-socket` runs one container per revision. Without a socket, every launch is refused.
- `-run-process IMAGE=COMMAND` runs a mapped image as a local process. The [packaged environment](quickstart.md#cloud-run-in-the-packaged-environment) uses it only for the bundled `gcp-local/workflow:latest` worker and refuses other images.

Invoke a service at `http://127.0.0.1:4566/_run/{service}-{project}.{location}.run.app/...` with a local ID token whose audience is the service URI. The container receives `K_SERVICE`, `K_REVISION`, `K_CONFIGURATION`, `PORT`, and the service's plain-text `env`; it never sees the caller's `Authorization` header. Workload metadata is bound to `template.serviceAccount` ([local identity](local-identity.md)).

## Methods

Routes are `/v2/projects/{project}/locations/{location}/...`.

| Resource | Methods | Notes |
| --- | --- | --- |
| Services | `create`, `get`, `list`, `patch`, `delete` | `serviceId` query; one container; port from `ports[0].containerPort` (default 8080) |
| Services | `getIamPolicy`, `setIamPolicy` | `allUsers` with `roles/run.invoker` allows anonymous calls |
| Revisions | `get`, `list`, `delete` | Stopped after 5 minutes idle; the next call cold-starts |
| Operations | `get`, `list`, `delete` | Create and update operations finish when the container is ready; an early exit fails them |

## IAM

Permissions are `run.services.*`, `run.revisions.*`, `run.operations.*`, and `run.routes.invoke`. Pinned roles: `roles/run.admin`, `.developer`, `.invoker`, `.viewer`. Invocation checks the token audience and `run.routes.invoke` before forwarding.

## Verification

```sh
sh scripts/go.sh test -race ./internal/providers/run
.venv-tools/bin/python scripts/smoke_cloudrun.py   # Linux with Docker
```

`TestAccCloudRunV2Service` passes ([Terraform](terraform-compatibility.md)). The [workflow example](workflow.md) deploys a service with an invoker grant.

## Gaps

- No recordings.
- `ingress` is stored but not enforced; token and `run.invoker` are the only boundary. VPC egress and Direct VPC are not modeled, and the [networking analyzer](networking.md) has no Cloud Run endpoints.
- No jobs, volumes (including `/cloudsql`), or `valueSource` environment variables.
- State is in memory.
