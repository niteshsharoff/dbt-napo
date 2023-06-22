terraform {
  required_version = "~> 1.4.6"

  required_providers {
    google = "~> 4.70.0"
  }

  backend "gcs" {
    bucket = "ae32-vpcservice-prod-terraform-bucket"
    prefix = "data-infrastructure/airflow/terraform/state"
  }
}

provider "google" {
  project = local.project_id
  region  = local.region
}
