data "google_storage_bucket_object_content" "shared_vpc_network_manifest" {
  bucket = "ae32-vpcservice-prod-terraform-bucket"
  name   = "core-infrastructure/ae32-vpc-host/networking"
}

locals {
  shared_vpc = jsondecode(base64decode(data.google_storage_bucket_object_content.shared_vpc_network_manifest.content))
  project_id = "ae32-vpcservice-prod"
  region     = "europe-west2"
  cluster = {
    name                 = "data"
    master_ip_cidr_range = "172.19.0.16/28"
  }
}

module "gke_cluster" {
  source = "git@github.com:CharlieNapo/infrastructure.git//terraform/modules/gke_cluster?ref=v1.0.0"
  region = local.region

  node_locations = [
    format("%s-a", local.region)
  ]

  host_project = {
    id      = local.shared_vpc["host_project_id"]
    network = local.shared_vpc["host_network"]
  }

  service_project = {
    id     = local.project_id
    subnet = local.shared_vpc["production_subnet"]["self_link"]
  }

  cluster = {
    name                 = local.cluster.name
    master_ip_cidr_range = local.cluster.master_ip_cidr_range
    pod_ip_cidr_range    = local.shared_vpc["production_subnet"]["pod_ip_cidr_range_name"]
    svc_ip_cidr_range    = local.shared_vpc["production_subnet"]["svc_ip_cidr_range_name"]
    enable_dataplane_v2  = true
  }

  bastion_ip_cidr_range = format("%s/32", local.shared_vpc["bastion_host"]["ip"])
}

module "airflow" {
  source       = "../modules/airflow"
  project_id   = local.project_id
  region       = local.region
  host_network = local.shared_vpc["host_network"]
  cluster_name = local.cluster.name

  node_pool = {
    machine_type  = "n2-standard-4"
    disk_size     = 40
    default_count = 1
    min_count     = 1
    max_count     = 1
  }

  depends_on = [module.gke_cluster]
}