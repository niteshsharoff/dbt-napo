module "airflow" {
  source       = "../modules/airflow"
  project_id   = local.project_id
  region       = local.region
  host_network = local.shared_vpc["host_network"]
}
