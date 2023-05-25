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
  source = "git@github.com:CharlieNapo/infrastructure.git//terraform/modules/cloud_sql?ref=v1.0.0"

  project_id  = var.project_id
  owner       = var.owner
  region      = var.region
  environment = "production"

  private_network = var.host_network

  name              = "airflow"
  engine            = "POSTGRES_14"
  machine_type      = "db-f1-micro"
  num_read_replicas = 0
}

# Airflow master node pool
module "airflow_master_node_pool" {
  source = "git@github.com:CharlieNapo/infrastructure.git//terraform/modules/gke_node_pool?ref=v1.0.0"
  region = var.region

  name         = "airflow-master"
  cluster_name = var.cluster_name
  project_id   = var.project_id

  node = {
    machine_type  = var.node_pool.machine_type
    disk_size     = var.node_pool.disk_size
    default_count = var.node_pool.default_count
    min_count     = var.node_pool.min_count
    max_count     = var.node_pool.max_count

    labels = {
      cluster = var.cluster_name
    }

    oauth_scope = [
      "https://www.googleapis.com/auth/monitoring",
      "https://www.googleapis.com/auth/logging.write",
      "https://www.googleapis.com/auth/projecthosting",
      "https://www.googleapis.com/auth/devstorage.full_control",
      "https://www.googleapis.com/auth/compute",
      "https://www.googleapis.com/auth/cloud-platform",
      "https://www.googleapis.com/auth/pubsub",
      "https://www.googleapis.com/auth/bigquery",
    ]
  }
}