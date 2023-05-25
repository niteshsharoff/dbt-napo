variable "owner" {
  type        = string
  description = "Organisational owner of the Cloud SQL instances"
  default     = ""
}

variable "project_id" {
  type        = string
  description = "ID of the project where the Airflow resources are going to be deployed."
}

variable "region" {
  type        = string
  description = "Region where Airflow resources are going to be deployed."
  default     = "europe-west2"
}

variable "host_network" {
  type        = string
  description = "Resource link of the VPC network from which instance is accessible via private IP."
}

variable "cluster_name" {
  type        = string
  description = "The GKE cluster name that the Airflow node pools will be added to"
}

variable "node_pool" {
  type = object({
    machine_type  = string
    disk_size     = number
    default_count = number
    min_count     = number
    max_count     = number
  })
  description = "Node pool configuration"
}