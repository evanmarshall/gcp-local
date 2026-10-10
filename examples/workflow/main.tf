# The workflow example provisions the durable bucket through Terraform. The
# Pub/Sub push subscription and the Cloud Run worker are created with the same
# REST surfaces the SDKs use; their Terraform resources are not covered by a
# recorded local contract yet (see docs/workflow.md).
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

resource "google_storage_bucket" "workflow" {
  name     = var.bucket
  location = var.region
}
