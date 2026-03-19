variable "project_id" {
  type        = string
  description = "GCP project ID"
}

variable "project_number" {
  type        = string
  description = "GCP project number"
}

variable "region" {
  type        = string
  description = "GCP region"
}

variable "artifact_repo" {
  type        = string
  description = "Artifact Registry repository name"
}

variable "staging_bucket" {
  type        = string
  description = "GCS staging bucket"
}

variable "temp_bucket" {
  type        = string
  description = "GCS temp bucket"
}

variable "bq_datasets" {
  description = "List of BigQuery datasets and IAM bindings"
  type = list(object({
    dataset_id      = string
    dataflow_roles  = list(string)
    developer_roles = list(string)
  }))
}

variable "bq_dataset_id" {
  type        = string
  description = "BigQuery dataset ID"
}

variable "bq_dataset_friendly_name" {
  type        = string
  description = "BigQuery dataset friendly name"
}

variable "bq_storage_billing_model" {
  type    = string
  default = "LOGICAL"
}

variable "developer_email" {
  type        = string
  description = "Email of the developer user"
}

variable "vpc_net" {
  type        = string
  description = "VPC Network"
}

variable "vpc_subnet" {
  type        = string
  description = "VPC Subnetwork"
}

variable "cloudbuild_bucket" {
  type        = string
  description = "Cloud Build bucket for storing source code"
}

variable "zone" {
  description = "The GCP zone for the Workbench instance (e.g., us-central1-a). Must be in the chosen region."
  type        = string
}

variable "instance_name" {
  description = "The unique name for the Vertex AI Workbench instance."
  type        = string
}

variable "machine_type" {
  description = "The GCE machine type for the instance."
  type        = string
  default     = "e2-standard-4"
}

variable "idle_timeout_seconds" {
  description = "Idle timeout for the notebook in seconds (e.g., 3 hours = 10800 seconds). Set to 0 to disable."
  type        = number
  default     = 10800
}

variable "dataflow_service_account_id" {
  description = "The ID for the Dataflow service account (the portion before the @)"
  type        = string
}


