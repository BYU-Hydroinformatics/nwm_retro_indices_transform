variable "project_id" {}
variable "dataflow_sa_email" {}
variable "notebook_sa_email" {}
variable "developer_email" {}
variable "bq_datasets" {
  type = list(object({
    dataset_id      = string
    dataflow_roles  = list(string)
    developer_roles = list(string)
  }))
}

# ------------------------------
# Dataflow roles per dataset
# ------------------------------
locals {
  dataflow_bindings = flatten([
    for ds in var.bq_datasets : [
      for role in ds.dataflow_roles : {
        dataset_id = ds.dataset_id
        role       = role
      }
    ]
  ])

  developer_bindings = flatten([
    for ds in var.bq_datasets : [
      for role in ds.developer_roles : {
        dataset_id = ds.dataset_id
        role       = role
      }
    ]
  ])
}

resource "google_bigquery_dataset_iam_member" "dataflow_roles" {
  for_each = { for b in local.dataflow_bindings : "${b.dataset_id}|${b.role}" => b }

  project    = var.project_id
  dataset_id = each.value.dataset_id
  role       = each.value.role
  member     = "serviceAccount:${var.dataflow_sa_email}"
}

resource "google_bigquery_dataset_iam_member" "notebook_roles" {
  for_each = { for b in local.dataflow_bindings : "${b.dataset_id}|${b.role}" => b }

  project    = var.project_id
  dataset_id = each.value.dataset_id
  role       = each.value.role
  member     = "serviceAccount:${var.notebook_sa_email}"
}

resource "google_bigquery_dataset_iam_member" "developer_roles" {
  for_each = { for b in local.developer_bindings : "${b.dataset_id}|${b.role}" => b }

  project    = var.project_id
  dataset_id = each.value.dataset_id
  role       = each.value.role
  member     = "user:${var.developer_email}"
}

resource "google_project_iam_member" "bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${var.dataflow_sa_email}"
}

resource "google_project_iam_member" "bigquery_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${var.dataflow_sa_email}"
}