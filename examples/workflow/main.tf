# The workflow example provisions every resource of the Pub/Sub push ->
# Cloud Run worker -> Storage chain against a local gcp-local endpoint:
# the worker and push service accounts, the bucket and the worker's object
# grant, the topic, the Cloud Run v2 worker, its invoker grant, and the push
# subscription. scripts/smoke/workflow.py drives it end to end.
#
# This configuration can only reach a loopback endpoint: var.endpoint must be
# a numeric loopback HTTP URL, every used API has an explicit endpoint
# override, and universe_domain sends any API without an override to a
# reserved .invalid domain instead of googleapis.com.
terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "8.5.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
  # Local external-account ADC: the provider exchanges the private subject
  # token at the local STS endpoint and refreshes it itself. The local issuer
  # grants cloud-platform only, so the default userinfo.email scope is dropped.
  credentials                     = file(var.credentials_file)
  scopes                          = ["https://www.googleapis.com/auth/cloud-platform"]
  universe_domain                 = "gcp-local.invalid"
  storage_custom_endpoint         = "${var.endpoint}/storage/v1/"
  pubsub_custom_endpoint          = "${var.endpoint}/v1/"
  cloud_run_v2_custom_endpoint    = "${var.endpoint}/v2/"
  iam_custom_endpoint             = "${var.endpoint}/v1/"
  iam_beta_custom_endpoint        = "${var.endpoint}/v1/"
  request_timeout                 = "30s"
  add_terraform_attribution_label = false
}

variable "endpoint" {
  type        = string
  description = "Local gcp-local HTTP endpoint, for example http://127.0.0.1:4566."
  validation {
    condition     = can(regex("^http://(127\\.0\\.0\\.1|\\[::1\\]):[0-9]{2,5}$", var.endpoint))
    error_message = "The endpoint must be a numeric loopback http URL with a port; this example never targets real GCP."
  }
}

variable "credentials_file" {
  type        = string
  description = "Local external-account ADC file carrying universe_domain = gcp-local.invalid."
  validation {
    # Only a local external-account file passes: its token exchange must go to
    # a loopback STS and it must name the local universe. Service-account keys,
    # user credentials, and real federation configs are refused.
    condition = try(
      jsondecode(file(var.credentials_file)).type == "external_account"
      && can(regex("^http://(127\\.0\\.0\\.1|\\[::1\\]):[0-9]{2,5}/v1/token$", jsondecode(file(var.credentials_file)).token_url))
      && jsondecode(file(var.credentials_file)).universe_domain == "gcp-local.invalid",
      false
    )
    error_message = "Use a local gcp-local ADC file (loopback token_url, universe_domain gcp-local.invalid)."
  }
}

variable "project" {
  type = string
}

variable "project_number" {
  type        = string
  description = "Numeric project number; names the Pub/Sub service agent."
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "bucket" {
  type = string
}

variable "topic" {
  type    = string
  default = "workflow"
}

variable "subscription" {
  type    = string
  default = "workflow-push"
}

variable "service" {
  type    = string
  default = "worker"
}

variable "worker_image" {
  type    = string
  default = "gcp-local/workflow:latest"
}

variable "worker_storage_endpoint" {
  type        = string
  description = "Storage JSON API base the worker dials, as seen from its runtime."
}

variable "grant_worker_storage" {
  type        = bool
  default     = true
  description = "Toggle the worker's object grant; false exercises the Storage denial."
}

variable "grant_push_invoker" {
  type        = bool
  default     = true
  description = "Toggle the push identity's run.invoker grant; false exercises the invocation denial."
}

# The worker's runtime identity. The workload metadata enrollment for this
# email stays in the private local identity configuration.
resource "google_service_account" "worker" {
  account_id   = "worker"
  display_name = "Workflow worker"
}

# The identity Pub/Sub presents to the worker on each push.
resource "google_service_account" "push" {
  account_id   = "workflow-push"
  display_name = "Workflow push invoker"
}

resource "google_storage_bucket" "workflow" {
  name                        = var.bucket
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  # The worker's objects are workflow output; destroy removes them.
  force_destroy = true
  soft_delete_policy {
    retention_duration_seconds = 0
  }
}

resource "google_storage_bucket_iam_member" "worker_objects" {
  count  = var.grant_worker_storage ? 1 : 0
  bucket = google_storage_bucket.workflow.name
  role   = "roles/storage.objectUser"
  member = google_service_account.worker.member
}

resource "google_pubsub_topic" "workflow" {
  name = var.topic
}

resource "google_cloud_run_v2_service" "worker" {
  name                = var.service
  location            = var.region
  deletion_protection = false
  ingress             = "INGRESS_TRAFFIC_ALL"
  template {
    service_account = google_service_account.worker.email
    containers {
      image = var.worker_image
      ports {
        container_port = 8080
      }
      env {
        name  = "GCP_LOCAL_STORAGE_ENDPOINT"
        value = var.worker_storage_endpoint
      }
    }
  }
}

resource "google_cloud_run_v2_service_iam_member" "push_invoker" {
  count    = var.grant_push_invoker ? 1 : 0
  project  = google_cloud_run_v2_service.worker.project
  location = google_cloud_run_v2_service.worker.location
  name     = google_cloud_run_v2_service.worker.name
  role     = "roles/run.invoker"
  member   = google_service_account.push.member
}

# The Pub/Sub service agent mints the push identity's OIDC token.
resource "google_service_account_iam_member" "pubsub_token_creator" {
  service_account_id = google_service_account.push.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:service-${var.project_number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

locals {
  # The service's canonical run.app host. Locally `uri` is the loopback
  # invocation URL (endpoint/_run/HOST); the invoker verifies tokens against
  # https://HOST. On GCP both values are the service URL.
  worker_audience = "https://${trimprefix(google_cloud_run_v2_service.worker.uri, "${var.endpoint}/_run/")}"
}

resource "google_pubsub_subscription" "workflow" {
  name                 = var.subscription
  topic                = google_pubsub_topic.workflow.id
  ack_deadline_seconds = 10

  push_config {
    push_endpoint = google_cloud_run_v2_service.worker.uri
    oidc_token {
      service_account_email = google_service_account.push.email
      audience              = local.worker_audience
    }
  }

  depends_on = [google_service_account_iam_member.pubsub_token_creator]
}

output "worker_uri" {
  value = google_cloud_run_v2_service.worker.uri
}
