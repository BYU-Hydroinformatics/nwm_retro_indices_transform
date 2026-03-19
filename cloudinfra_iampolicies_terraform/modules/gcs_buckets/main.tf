variable "staging_bucket" {}
variable "temp_bucket" {}
variable "project_id" {}
variable "dataflow_sa_email" {}
variable "notebook_sa_email" {}
variable "cloudbuild_sa_email" {}

resource "google_storage_bucket_iam_member" "staging_admin" {
  bucket = var.staging_bucket
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${var.dataflow_sa_email}"
}

resource "google_storage_bucket_iam_member" "staging_admin_nb" {
  bucket = var.staging_bucket
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${var.notebook_sa_email}"
}

resource "google_storage_bucket_iam_member" "temp_admin" {
  bucket = var.temp_bucket
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${var.dataflow_sa_email}"
}

resource "google_storage_bucket_iam_member" "temp_admin_nb" {
  bucket = var.temp_bucket
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${var.notebook_sa_email}"
}

resource "google_storage_bucket_iam_member" "staging_admin_nb_direct" {
  bucket = var.staging_bucket
  role   = "roles/storage.admin"
  member = "serviceAccount:${var.notebook_sa_email}"
}

resource "google_storage_bucket_iam_member" "temp_admin_nb_direct" {
  bucket = var.temp_bucket
  role   = "roles/storage.admin"
  member = "serviceAccount:${var.notebook_sa_email}"
}

resource "google_project_iam_member" "cloudbuild_storage_viewer" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${var.cloudbuild_sa_email}"
}
