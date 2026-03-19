variable "project_id" {}
variable "dataflow_sa_email" {}
variable "notebook_sa_email" {}
variable "developer_email" {}
variable "cloudbuild_sa_email" {}


resource "google_project_iam_member" "nb_sa_dataflow_dev" {
  project = var.project_id
  role    = "roles/dataflow.developer"
  member  = "serviceAccount:${var.notebook_sa_email}"
}

resource "google_service_account_iam_member" "nb_sa_act_as_dataflow_sa" {
  service_account_id = "projects/${var.project_id}/serviceAccounts/${var.dataflow_sa_email}"
  role               = "roles/iam.serviceAccountUser"
  member  = "serviceAccount:${var.notebook_sa_email}"
}

resource "google_project_iam_member" "nb_sa_storage_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${var.notebook_sa_email}"
}

resource "google_project_iam_member" "nb_sa_bq_user" {
  project = var.project_id
  role    = "roles/bigquery.user"
  member  = "serviceAccount:${var.notebook_sa_email}"
}

resource "google_project_iam_member" "cloudbuild_storage_viewer" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${var.cloudbuild_sa_email}"
}





