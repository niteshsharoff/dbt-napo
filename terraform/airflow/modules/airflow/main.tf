# Airflow remote logging bucket
resource "google_storage_bucket" "airflow_bucket" {
  name          = format("%s-airflow", var.service_project.id)
  project       = var.service_project.id
  location      = local.location
  force_destroy = false

  versioning {
    enabled = false
  }
}

locals {
  location = "EU"
}

# Airflow Database
module "airflow_db" {
  source = "git@github.com:CharlieNapo/infrastructure.git//terraform/modules/cloud_sql?ref=v1.0.0"

  project_id  = var.service_project.id
  owner       = var.owner
  region      = var.region
  environment = var.environment

  private_network = var.host_project.network

  name              = var.db_config.name
  engine            = var.db_config.engine
  machine_type      = var.db_config.machine_type
  num_read_replicas = 0
}

# Airflow GKE cluster
module "gke_cluster" {
  source = "git@github.com:CharlieNapo/infrastructure.git//terraform/modules/gke_cluster?ref=v1.0.0"
  region = var.region

  # Update for multi node deployment
  node_locations = [
    format("%s-a", var.region)
  ]

  host_project = {
    id      = var.host_project.id
    network = var.host_project.network
  }

  service_project = {
    id     = var.service_project.id
    subnet = var.service_project.subnet
  }

  cluster = {
    name                 = var.cluster_config.name
    master_ip_cidr_range = var.cluster_config.master_ip_cidr_range
    pod_ip_cidr_range    = var.cluster_config.pod_ip_cidr_range
    svc_ip_cidr_range    = var.cluster_config.svc_ip_cidr_range
    enable_dataplane_v2  = true
  }

  bastion_ip_cidr_range = format("%s/32", var.bastion_host_ip)
}

# Airflow master node pool
module "airflow_master_node_pool" {
  source = "git@github.com:CharlieNapo/infrastructure.git//terraform/modules/gke_node_pool?ref=v1.0.0"
  region = var.region

  name         = "airflow-master"
  cluster_name = var.cluster_config.name
  project_id   = var.service_project.id

  node = {
    machine_type  = var.master_node_pool.machine_type
    disk_size     = var.master_node_pool.disk_size
    default_count = var.master_node_pool.default_count
    min_count     = var.master_node_pool.min_count
    max_count     = var.master_node_pool.max_count

    labels = {
      cluster = var.cluster_config.name
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