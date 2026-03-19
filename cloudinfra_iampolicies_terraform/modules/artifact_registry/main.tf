variable "project_id" {}
variable "region" {}
variable "repo" {}
variable "cloudbuild_sa_email" {}
variable "dataflow_sa_email" {}

resource "google_artifact_registry_repository_iam_member" "artifact_writer_cloudbuild" {
  project    = var.project_id
  location   = var.region
  repository = var.repo
  role       = "roles/artifactregistry.writer"
  member     = "serviceAccount:${var.cloudbuild_sa_email}"
}

resource "google_artifact_registry_repository_iam_member" "artifact_reader_dataflow" {
  project    = var.project_id
  location   = var.region
  repository = var.repo
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${var.dataflow_sa_email}"
}
