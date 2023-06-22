data "google_storage_bucket_object_content" "shared_vpc_network_manifest" {
  bucket = "ae32-vpcservice-prod-terraform-bucket"
  name   = "core-infrastructure/ae32-vpc-host/networking"
}

locals {
  shared_vpc  = jsondecode(base64decode(data.google_storage_bucket_object_content.shared_vpc_network_manifest.content))
  project_id  = "ae32-vpcservice-dev"
  region      = "europe-west2"
  environment = "development"
  cluster = {
    name                 = "data"
    master_ip_cidr_range = "172.19.0.16/28"
  }
}

module "airflow" {
  source      = "../modules/airflow"
  region      = local.region
  environment = local.environment

  bastion_host_ip = local.shared_vpc.bastion_host.ip

  host_project = {
    id      = local.shared_vpc.host_project_id
    network = local.shared_vpc.host_network
  }

  service_project = {
    id     = local.project_id
    subnet = local.shared_vpc.development_subnet.self_link
  }

  db_config = {
    name         = "airflow"
    engine       = "POSTGRES_14"
    machine_type = "db-f1-micro"
  }

  cluster_config = {
    name                 = local.cluster.name
    master_ip_cidr_range = local.cluster.master_ip_cidr_range
    pod_ip_cidr_range    = local.shared_vpc.development_subnet.pod_ip_cidr_range_name
    svc_ip_cidr_range    = local.shared_vpc.development_subnet.svc_ip_cidr_range_name
  }

  master_node_pool = {
    machine_type  = "n2-standard-4"
    disk_size     = 40
    default_count = 1
    min_count     = 1
    max_count     = 1
  }
}