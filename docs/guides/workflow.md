# Workflow: Terraform → Pub/Sub push → Cloud Run → Storage

Terraform (google provider 8.5.0) provisions a Pub/Sub push subscription that invokes a Cloud Run worker, which writes each message to a Storage bucket. Everything runs locally under IAM. Sources are in [`examples/workflow/`](../../examples/workflow/); `scripts/smoke/workflow.py` runs it end to end.

## Run it in the packaged environment

On Linux or macOS with Docker, after installing `requirements/pubsub.txt` and `python3 scripts/install/terraform.py`:

```sh
.venv-tools/bin/python scripts/smoke/workflow.py --packaged
```

It runs `scripts/dev/local_env.py up --project demo-run --workload worker`, reads `env`, applies the Terraform, checks delivery and both denial boundaries, then runs `reset` and `down` and requires no resources, worker processes, or containers to remain.

Add `--cloud-run-docker` to build `examples/workflow` as a user image and run it as a revision container ([Cloud Run](../services/cloudrun.md#run-your-own-image)). Terraform sets `worker_storage_endpoint` from `GCP_LOCAL_RUN_ENDPOINT`. A second Docker-mode environment runs its own revision, and the first environment's `reset` and `down` must leave it running.

## Run it without Docker

On Linux with Java 17 and `requirements/pubsub.txt`:

```sh
GCP_LOCAL_JAVA=$(readlink -f "$(command -v java)") .venv-tools/bin/python scripts/smoke/workflow.py
```

The script installs Go, the Pub/Sub emulator, Terraform 1.16.5, and the google 8.5.0 plugin, then starts the core on `127.0.0.1:9298` with Storage, Pub/Sub, Cloud Run, and IAM. The worker runs as a local process (`--run-process`). After installation it needs no network; to prove that, wrap it in `python3 scripts/run/isolated.py --timeout 900 --`.

## What Terraform creates

[`main.tf`](../../examples/workflow/main.tf) creates:

- worker and push service accounts
- a bucket, with `roles/storage.objectUser` for the worker
- a topic
- a Cloud Run v2 service running as the worker account
- `roles/run.invoker` on the service for the push account
- `roles/iam.serviceAccountTokenCreator` on the push account for the Pub/Sub service agent
- a push subscription with an OIDC token for the push account

The developer identity has only project-level Storage Admin, Pub/Sub Admin, Cloud Run Admin, Service Account Admin, and Service Account User. There is no `allUsers` grant.

## Terraform authentication

Use the `GCP_LOCAL_TERRAFORM_CREDENTIALS` file from `env` (`terraform-adc.json`). It is the local external-account ADC with `"universe_domain": "gcp-local.invalid"` added, which the provider requires when its `universe_domain` is overridden. The verified configuration sets `scopes` to `cloud-platform`.

```hcl
provider "google" {
  project                         = var.project
  credentials                     = file(var.credentials_file)
  scopes                          = ["https://www.googleapis.com/auth/cloud-platform"]
  universe_domain                 = "gcp-local.invalid"
  storage_custom_endpoint         = "${var.endpoint}/storage/v1/"
  pubsub_custom_endpoint          = "${var.endpoint}/v1/"
  cloud_run_v2_custom_endpoint    = "${var.endpoint}/v2/"
  iam_custom_endpoint             = "${var.endpoint}/v1/"
  iam_beta_custom_endpoint        = "${var.endpoint}/v1/"
  add_terraform_attribution_label = false
}
```

- Give every API you use a custom endpoint. `universe_domain` sends any API without one to a reserved `.invalid` domain instead of googleapis.com.
- Cloud Run v2 resources need `cloud_run_v2_custom_endpoint`; `cloud_run_custom_endpoint` does not apply to them.
- The example's variable validation refuses a non-loopback endpoint and any credential file that isn't a local external-account file.

This workflow is the only test of Terraform with local ADC.

## What the test checks

1. `terraform apply` succeeds.
2. A published message is stored as `<messageId>.txt` by the worker.
3. Removing the worker's bucket grant stops storage for 8 seconds; restoring it stores the redelivered message.
4. Removing the push account's invoker grant stops delivery for 8 seconds; restoring it delivers the message.
5. A second `terraform plan` shows no changes.
6. `terraform destroy` (or `reset` in packaged mode) leaves no buckets, topics, subscriptions, services, service accounts, or worker processes.

## Limits

- Push delivery, push ID tokens, and redelivery timing are modeled locally, not recorded.
- Cloud Run is modeled locally, not recorded.
- Storage, Pub/Sub pull authorization, and service accounts are recorded ([Storage](../services/storage.md), [Pub/Sub](../services/messaging/pubsub.md), [service accounts](../services/security/serviceaccounts.md)).
- Step 4 infers the invoker denial from no object being stored; it doesn't observe the invocation response.
- The local service `uri` is `http://127.0.0.1:PORT/_run/HOST` and the push audience is `https://HOST`. On GCP both are the service URL.
- If the core exits abnormally, revision processes keep running.
