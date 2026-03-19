locals {
  apis = [
    "cloudbuild.googleapis.com",
    "artifactregistry.googleapis.com",
    "notebooks.googleapis.com",
    "dataflow.googleapis.com",
    "compute.googleapis.com"
  ]
}

locals {
  bucket_names = [
    var.staging_bucket,
    var.temp_bucket,
    var.cloudbuild_bucket,
  ]
}

resource "google_project_service" "enabled_apis" {
  for_each = toset(local.apis)
  project  = var.project_id
  service  = each.value
}

module "service_accounts" {
  source      = "./modules/service_accounts"
  project_id  = var.project_id
}

module "artifact_registry" {
  source              = "./modules/artifact_registry"
  project_id          = var.project_id
  region              = var.region
  repo                = var.artifact_repo
  cloudbuild_sa_email = module.service_accounts.cloudbuild_sa_email
  dataflow_sa_email   = module.service_accounts.dataflow_sa_email
}

resource "google_storage_bucket" "multiple_buckets" {
  for_each = toset(local.bucket_names)

  name          = each.key
  location      = var.region
  project       = var.project_id
  storage_class = "STANDARD"
  uniform_bucket_level_access = true
}

module "gcs_buckets" {
  source             = "./modules/gcs_buckets"
  project_id         = var.project_id
  staging_bucket     = var.staging_bucket
  temp_bucket        = var.temp_bucket
  dataflow_sa_email  = module.service_accounts.dataflow_sa_email
  notebook_sa_email  = module.service_accounts.notebook_sa_email
  cloudbuild_sa_email = module.service_accounts.cloudbuild_sa_email
  depends_on = [google_storage_bucket.multiple_buckets]
}


resource "google_bigquery_dataset" "national_water_model" {
  project        = var.project_id
  dataset_id     = var.bq_dataset_id
  location       = var.region

  friendly_name  = var.bq_dataset_friendly_name
  description    = "BigQuery Dataset for NWM Retrospective Data-Based Products"
  delete_contents_on_destroy = true
  storage_billing_model = var.bq_storage_billing_model
}

module "bigquery" {
  source            = "./modules/bigquery"
  project_id        = var.project_id
  dataflow_sa_email = module.service_accounts.dataflow_sa_email
  notebook_sa_email = module.service_accounts.notebook_sa_email
  developer_email   = var.developer_email
  bq_datasets       = var.bq_datasets
  depends_on = [
    google_bigquery_dataset.national_water_model
  ]
}


module "dataflow" {
  source             = "./modules/dataflow"
  project_id         = var.project_id
  developer_email    = var.developer_email
  dataflow_sa_email  = module.service_accounts.dataflow_sa_email
  cloudbuild_sa_email = module.service_accounts.cloudbuild_sa_email
}

module "notebook" {
  source             = "./modules/notebook"
  project_id         = var.project_id
  developer_email    = var.developer_email
  notebook_sa_email  = module.service_accounts.notebook_sa_email
  dataflow_sa_email  = module.service_accounts.dataflow_sa_email
  cloudbuild_sa_email = module.service_accounts.cloudbuild_sa_email
}

# Allow developer to submit Cloud Build jobs
resource "google_project_iam_member" "cloudbuild_editor" {
  project = var.project_id
  role    = "roles/cloudbuild.builds.editor"
  member  = "user:${var.developer_email}"
}

# Allow developer to use services (serviceusage.services.use)
resource "google_project_iam_member" "serviceusage_consumer" {
  project = var.project_id
  role    = "roles/serviceusage.serviceUsageConsumer"
  member  = "user:${var.developer_email}"
}


# Allow developer to access Cloud Build bucket
resource "google_storage_bucket_iam_member" "cloudbuild_bucket_admin" {
  bucket = var.cloudbuild_bucket
  role   = "roles/storage.objectAdmin"
  member = "user:${var.developer_email}"
  depends_on = [
    google_storage_bucket.multiple_buckets
  ]
}

resource "google_service_account_iam_member" "allow_user_cloudbuild" {
  service_account_id = "projects/${var.project_id}/serviceAccounts/${module.service_accounts.cloudbuild_sa_email}"
  role               = "roles/iam.serviceAccountUser"
  member             = "user:${var.developer_email}"
}

resource "google_storage_bucket_iam_member" "cloudbuild_sa_bucket_access" {
  bucket = var.cloudbuild_bucket
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${module.service_accounts.cloudbuild_sa_email}"
  depends_on = [
    google_storage_bucket.multiple_buckets
  ]
}

module "gcloud_build" {
  source                = "terraform-google-modules/gcloud/google"
  version               = "4.0.0"
  skip_download         = true
  gcloud_download_url   = ""
  create_cmd_entrypoint = "gcloud"
  create_cmd_body       = "builds submit --config=container/cloudbuild.yaml --project=${var.project_id} ."
  
  module_depends_on = tolist([
    google_storage_bucket_iam_member.cloudbuild_sa_bucket_access.id,
    google_service_account_iam_member.allow_user_cloudbuild.id,
    google_project_iam_member.cloudbuild_editor.id,
    google_project_iam_member.serviceusage_consumer.id,
    google_storage_bucket_iam_member.cloudbuild_bucket_admin.id
  ])
}

resource "google_project_iam_member" "workbench_user_iam" {
  project = var.project_id
  role   = "roles/aiplatform.user" 
  member = "user:${var.developer_email}" 
}

resource "google_service_account_iam_member" "workbench_owner_act_as" {
  service_account_id = "projects/${var.project_id}/serviceAccounts/${module.service_accounts.notebook_sa_email}"
  role               = "roles/iam.serviceAccountUser" 
  member             = "user:${var.developer_email}"
  depends_on = [
    module.service_accounts.notebook_sa_email
  ]
}

resource "google_workbench_instance" "vertex_workbench" {
  name     = var.instance_name
  location = var.zone
  instance_owners = ["${var.developer_email}"]
  gce_setup {
    machine_type = var.machine_type
    boot_disk {
      disk_size_gb = 150
      disk_type    = "PD_SSD"
    }
    data_disks {
      disk_size_gb = 50
      disk_type    = "PD_SSD"
    }
    vm_image {
      project = "cloud-notebooks-managed"
      family  = "workbench-instances"
    }

    metadata = {
      "idle-timeout-seconds" = var.idle_timeout_seconds
      "terraform"            = "true"
  }
  service_accounts {
    email = module.service_accounts.notebook_sa_email
  }
  network_interfaces {
    network = "projects/${var.project_id}/global/networks/${var.vpc_net}"
    subnet  = "projects/${var.project_id}/regions/${var.region}/subnetworks/${vpc_subnet}"
  }

  }

  depends_on = [
    google_service_account_iam_member.workbench_owner_act_as,
    google_project_iam_member.workbench_user_iam
  ]
}


resource "google_notebooks_instance_iam_member" "notebook_start_permission" {
  project  = var.project_id
  location = var.zone
  instance_name = google_workbench_instance.vertex_workbench.name

  role   = "roles/notebooks.admin"
  member = "user:${var.developer_email}"
}

resource "google_project_iam_member" "notebook_instance_viewer" {
  project = var.project_id
  role    = "roles/notebooks.viewer"
  member  = "user:${var.developer_email}"
}

resource "google_project_iam_member" "aiplatform_viewer" {
  project = var.project_id
  role    = "roles/aiplatform.viewer"
  member  = "user:${var.developer_email}"
}

resource "google_project_iam_member" "user_notebook_runner" {
  project = var.project_id
  role    = "roles/notebooks.runner"
  member  = "user:${var.developer_email}"
}

resource "google_project_iam_member" "user_aiplatform_user" {
  project = var.project_id
  role    = "roles/aiplatform.user"
  member  = "user:${var.developer_email}"
}


resource "google_artifact_registry_repository" "repo" {
  repository_id = var.artifact_repo
  location      = var.region
  description   = "Artifact Registry Repo for Apache Beam Docker Images"
  format        = "docker"
  depends_on = [
    google_project_service.enabled_apis
  ]
}



