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