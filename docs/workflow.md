# The workflow: Terraform -> Pub/Sub push -> Cloud Run worker -> Storage

The acceptance test is `scripts/smoke_workflow.py` (CI job `workflow-acceptance`).
The example sources live in `examples/workflow/`. Terraform provisions every
resource with the released google 8.5.0 provider against a private local core;
nothing in the chain is created through the SDKs.

## Run it

From a fresh checkout on Linux or in moo (Java 17 and `requirements-pubsub.txt`
installed; Docker is not needed):

```sh
GCP_LOCAL_JAVA=$(readlink -f "$(command -v java)") .venv-tools/bin/python scripts/smoke_workflow.py
```

The script installs the pinned Go toolchain, Pub/Sub emulator jar, Terraform
1.16.5 and the google 8.5.0 plugin (`scripts/install_terraform.py`; Terraform
reads it from a filesystem-only mirror), builds the core, and starts it on
`127.0.0.1:9298` with Storage, Pub/Sub, Cloud Run, and IAM. Revisions run
through the process runtime (`--run-process`), so the worker is
`examples/workflow/worker.py` running as a local process. Container execution
of revisions is covered separately by `scripts/smoke_cloudrun.py`.

Once the tools are installed, the whole run needs no network egress; in moo
it also passes inside the loopback-only namespace:

```sh
GCP_LOCAL_JAVA=$(readlink -f "$(command -v java)") python3 scripts/run_isolated.py --timeout 900 -- \
  .venv-tools/bin/python scripts/smoke_workflow.py
```

## Run it in the packaged environment

The same workflow runs against the [packaged environment](quickstart.md) on
host Docker (Linux or macOS; Python requirements and Terraform as above, no
Java or Go on the host):

```sh
.venv-tools/bin/python scripts/smoke_workflow.py --packaged
```

It starts a private environment with the documented
`scripts/local_env.py up --project demo-run --workload worker`, reads `env` for
the endpoints, project number, and Terraform credential file, and runs the
apply, delivery, both denial boundaries, and the empty second plan below. The
worker runs as a process inside the core container: the packaged process
runtime maps `gcp-local/workflow:latest` to the bundled worker, and the core
listens on the published port so the service URI is the same on the host and
inside the container. Instead of `terraform destroy`, it runs `reset` and
requires the local APIs to list no buckets, topics, subscriptions, Cloud Run
services, or service accounts and the container to run no worker process; then
`down` must leave no container.

## What Terraform creates

`examples/workflow/main.tf`: the worker and push service accounts, the bucket,
the worker's `roles/storage.objectUser` bucket grant, the topic, the Cloud Run
v2 worker (`google_cloud_run_v2_service`, running as the worker account), the
push account's `roles/run.invoker` grant on the service, the Pub/Sub service
agent's `roles/iam.serviceAccountTokenCreator` grant on the push account, and
the push subscription (OIDC token for the push account, audience the service's
`https://worker-PROJECT.REGION.run.app` host).

The developer identity holds only project-level Storage Admin, Pub/Sub Admin,
Cloud Run Admin, Service Account Admin, and Service Account User. There is no
`allUsers` grant. The worker's workload metadata enrollment stays in the
private identity configuration; Terraform creates the account record that the
grants and push ID tokens resolve.

## Keeping Terraform local

- `var.endpoint` must be a numeric loopback `http://` URL with a port.
- Storage, Pub/Sub, Cloud Run v2 (`cloud_run_v2_custom_endpoint`; the v1
  `cloud_run_custom_endpoint` does not redirect v2 resources), and IAM have
  explicit endpoint overrides; `universe_domain = "gcp-local.invalid"` sends any
  other API to a reserved domain instead of googleapis.com.
- `var.credentials_file` must be an `external_account` file whose `token_url`
  is a loopback `/v1/token` and whose `universe_domain` is `gcp-local.invalid`.
- The smoke runs Terraform with a minimal environment (no ambient ADC or gcloud
  configuration) and a dead `HTTPS_PROXY`/`HTTP_PROXY`, so any non-loopback
  request fails. It also checks that variable validation refuses
  `https://storage.googleapis.com` and the ADC file without the local universe.

## Terraform authentication

The provider authenticates with local external-account ADC: it exchanges the
private subject token at the local STS endpoint and refreshes it itself. Two
adjustments to the SDK ADC file are required, and both are proven only by this
workflow:

- The provider refuses `universe_domain = "gcp-local.invalid"` unless the
  credential names the same universe, so Terraform reads a copy of the ADC file
  with `"universe_domain": "gcp-local.invalid"` added. The Python SDKs keep the
  unmarked file.
- The provider requests `cloud-platform` and `userinfo.email` by default; the
  local issuer grants `cloud-platform` only, so the example sets
  `scopes = ["https://www.googleapis.com/auth/cloud-platform"]`.

`scripts/create_local_identity.py` and the packaged environment write this
variant as `terraform-adc.json` beside `adc.json`; the packaged `env` command
names it `GCP_LOCAL_TERRAFORM_CREDENTIALS`.

## Acceptance sequence

1. `terraform init`, then the guard checks above, then `terraform apply`.
2. Publishing a message (Pub/Sub SDK over the local gRPC gateway) makes the
   push agent mint an ID token for the push account, invocation verify the
   audience and `run.invoker`, and the worker store `<messageId>.txt` with its
   own metadata token. The smoke reads the object back.
3. Storage boundary: `terraform apply -var grant_worker_storage=false` removes
   the worker's bucket grant. A published message is not stored for the whole
   8-second window (the worker's write is refused and the push is nacked).
   Re-applying with the grant stores the redelivered message and a new one.
4. Invocation boundary: `terraform apply -var grant_push_invoker=false` removes
   the push account's invoker grant. With the worker's Storage grant intact, a
   message is not stored for 8 seconds; re-applying the grant delivers it. The
   smoke infers the boundary from that state: only the invoker grant changed,
   and a delivery that reached the worker would have been stored. It does not
   observe the invocation response directly.
5. A second `terraform plan -detailed-exitcode` reports no changes.
6. `terraform destroy`, then listing buckets, topics, subscriptions, Cloud Run
   services, and service accounts through the local API returns none, and no
   worker process remains.

## Recorded coverage and approximations

- Storage bucket/object semantics: cloud-recorded
  ([storage recordings](../tests/conformance/recordings/), e.g.
  `storage-authorization`, `storage-object-lifecycle`; see [storage.md](storage.md)).
- Pub/Sub pull authorization: cloud-recorded
  ([pubsub-pull-authorization](../tests/conformance/recordings/pubsub-pull-authorization);
  see [pubsub-validation.md](pubsub-validation.md)).
- Service accounts: cloud-recorded
  ([serviceaccount-lifecycle-authorization](../tests/conformance/recordings/serviceaccount-lifecycle-authorization)).
- **Push delivery has no real-GCP recording.** The push agent, its ID-token
  minting, the service agent token-creator requirement, and redelivery timing
  are local-modeled.
- **Cloud Run has no real-GCP recording** (#13): services, revisions,
  operations, invocation, and the invoker check are local-modeled.
- The Terraform cases in `tests/terraform` and this workflow show that the
  released provider accepts the local responses; they are not recorded provider
  call sequences from GCP.
- Locally the service `uri` is the loopback invocation URL
  (`http://127.0.0.1:PORT/_run/HOST`) and the push audience is
  `https://HOST`. On GCP both are the service URL; the example derives the
  audience from the local `uri` shape.
- The process runtime does not stop revision processes when the core exits
  abnormally; the smoke kills leftover workers after a failed run.
