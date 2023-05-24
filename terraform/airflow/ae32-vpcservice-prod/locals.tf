data "google_storage_bucket_object_content" "shared_vpc_network_config" {
  bucket = "ae32-vpcservice-prod-terraform-bucket"
  name   = "core-infrastructure/ae32-vpc-host/networking"
}

locals {
  project_id = "ae32-vpcservice-prod"
  region     = "europe-west2"
  shared_vpc = jsondecode(base64decode(data.google_storage_bucket_object_content.shared_vpc_network_config.content))
}