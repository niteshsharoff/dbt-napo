# Airflow remote logging bucket
resource "google_storage_bucket" "airflow_bucket" {
  name          = format("%s-airflow", var.project_id)
  project       = var.project_id
  location      = local.location
  force_destroy = false

  versioning {
    enabled = false
  }
}

# Airflow Database
module "airflow_db" {
  source = "git@github.com:CharlieNapo/infrastructure.git//terraform/modules/cloud_sql?ref=master"

  project_id  = var.project_id
  owner       = var.owner
  region      = var.region
  environment = "production"

  private_network = var.host_network

  name              = format("%s-airflow-db", var.project_id)
  engine            = "POSTGRES_14"
  machine_type      = "db-f1-micro"
  num_read_replicas = 0
}