variable "project_id" {}
variable "sa_cloudbuild_name" { default = "sa-cloudbuild" }
variable "sa_dataflow_name" { default = "sa-dataflow-worker" }
variable "sa_notebook_name" { default = "vertex-notebook-sa" }

resource "google_service_account" "cloudbuild" {
  project      = var.project_id
  account_id   = var.sa_cloudbuild_name
  display_name = "Custom Cloud Build Service Account"
}

resource "google_service_account" "dataflow" {
  project      = var.project_id
  account_id   = var.sa_dataflow_name
  display_name = "Dataflow Worker Service Account"
}

resource "google_service_account" "notebook_sa" {
  account_id   = var.sa_notebook_name
  display_name = "Vertex AI Notebook Service Account"
  project      = var.project_id
}

output "cloudbuild_sa_email" { value = google_service_account.cloudbuild.email }
output "dataflow_sa_email" { value = google_service_account.dataflow.email }
output "notebook_sa_email" { value = google_service_account.notebook_sa.email }