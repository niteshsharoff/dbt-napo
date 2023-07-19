variable "owner" {
  type        = string
  description = "Organisational owner of the Cloud SQL instances"
  default     = ""
}

variable "environment" {
  type        = string
  description = "Deployment environment."
}

variable "region" {
  type        = string
  description = "Region where Airflow resources are going to be deployed."
  default     = "europe-west2"
}

variable "host_project" {
  type = object({
    id      = string
    network = string
  })
}

variable "service_project" {
  type = object({
    id     = string
    subnet = string
  })
}

variable "db_config" {
  type = object({
    name         = string
    engine       = string
    machine_type = string
  })
  description = "Airflow DB configuration."
}

variable "cluster_config" {
  type = object({
    name                 = string
    master_ip_cidr_range = string
    pod_ip_cidr_range    = string
    svc_ip_cidr_range    = string
  })
  description = "Airflow GKE cluster configuration."
}

variable "master_node_pool" {
  type = object({
    machine_type  = string
    disk_size     = number
    default_count = number
    min_count     = number
    max_count     = number
  })
  description = "Airflow master node pool configuration."
}

variable "bastion_host_ip" {
  type        = string
  description = "Bastion box private IP address."
}