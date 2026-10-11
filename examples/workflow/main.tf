# The workflow example provisions the durable bucket plus the Pub/Sub topic
# and push subscription through Terraform. The resource contracts are pinned
# by the acceptance runner (TestAccPubSubTopicAndSubscription over the JSON
# REST surface). The Cloud Run worker is still created with the same REST
# surfaces the SDKs use; its Terraform resource is parked (see docs/workflow.md
# and issue #13).
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "8.5.0"
    }
  }
}

provider "google" {
  credentials = var.credentials_file
  project     = var.project
  region      = var.region
}

variable "project" {
  type = string
}

variable "region" {
  type = string
}

variable "endpoint" {
  type = string
}

variable "credentials_file" {
  type = string
}

variable "bucket" {
  type = string
}

variable "topic" {
  type = string
}

variable "subscription" {
  type = string
}

variable "push_endpoint" {
  type = string
}

variable "push_audience" {
  type = string
}

variable "worker_service_account" {
  type = string
}

resource "google_storage_bucket" "workflow" {
  name     = var.bucket
  location = var.region
}

resource "google_pubsub_topic" "workflow" {
  name = var.topic
}

resource "google_pubsub_subscription" "workflow" {
  name  = var.subscription
  topic = google_pubsub_topic.workflow.id

  push_config {
    push_endpoint = var.push_endpoint
    oidc_token {
      service_account_email = var.worker_service_account
      audience              = var.push_audience
    }
  }
}
