project_id      = <PROJECT_ID>
project_number  = <PROJECT_NUMBER>
region          = "us-central1"
artifact_repo   = "beam-images"
staging_bucket  = "nwm-retro-staging"
temp_bucket     = "nwm-retro-temp"
cloudbuild_bucket   = "<PROJECT_ID>_cloudbuild"
developer_email = <"DEVELOPER_EMAIL">
vpc_net         = "base-network" #if custom otherwise use default
vpc_subnet      = "base-network" #if custom otherwise use default

bq_dataset_id       = "national_water_model"
bq_dataset_friendly_name = "nwm_retrospective_data_based_products"
bq_datasets = [
  {
    dataset_id      = "national_water_model"
    dataflow_roles  = ["roles/bigquery.dataEditor","roles/bigquery.metadataViewer"]
    developer_roles = ["roles/bigquery.metadataViewer"]
  },
]
bq_storage_billing_model = "PHYSICAL"

zone                 = "us-central1-a"
instance_name        = "dataflow-launcher-notebook"
machine_type         = "n2-standard-8"
idle_timeout_seconds = 1800
dataflow_service_account_id = "sa-dataflow-worker"
