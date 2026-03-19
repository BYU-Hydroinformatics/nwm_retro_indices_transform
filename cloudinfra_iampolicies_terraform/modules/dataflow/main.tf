variable "project_id" {}
variable "dataflow_sa_email" {}
variable "cloudbuild_sa_email" {}
variable "developer_email" {}



resource "google_project_iam_member" "dataflow_developer" {
  project = var.project_id
  role    = "roles/dataflow.developer"
  member  = "user:${var.developer_email}"
}

resource "google_service_account_iam_member" "developer_act_as_worker_sa" {
  service_account_id = "projects/${var.project_id}/serviceAccounts/${var.dataflow_sa_email}"
  role               = "roles/iam.serviceAccountUser"
  member             = "user:${var.developer_email}"
}

resource "google_project_iam_member" "dataflow_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${var.dataflow_sa_email}"
}

resource "google_project_iam_member" "dataflow_logging" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${var.dataflow_sa_email}"
}

resource "google_project_iam_member" "dataflow_metrics" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${var.dataflow_sa_email}"
}

resource "google_project_iam_member" "dataflow_storage_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${var.dataflow_sa_email}"
}

resource "google_project_iam_member" "cloudbuild_logging" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${var.cloudbuild_sa_email}"
}

resource "google_project_iam_member" "cloudbuild_artifact_writer" {
  project = var.project_id
  role    = "roles/artifactregistry.writer"
  member  = "serviceAccount:${var.cloudbuild_sa_email}"
}


