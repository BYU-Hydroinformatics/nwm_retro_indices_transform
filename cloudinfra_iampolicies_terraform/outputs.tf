output "cloudbuild_sa_email" { value = module.service_accounts.cloudbuild_sa_email }
output "dataflow_sa_email" { value = module.service_accounts.dataflow_sa_email }
output "notebook_sa_email" { value = module.service_accounts.notebook_sa_email }

output "bigquery_dataset_id" {
  value = google_bigquery_dataset.national_water_model.id
}

output "notebook_instance_name" {
  value = google_workbench_instance.vertex_workbench.name
}

