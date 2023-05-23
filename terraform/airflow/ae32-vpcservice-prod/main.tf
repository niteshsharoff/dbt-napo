resource "google_storage_bucket" "airflow" {
  name          = "${local.project_id}-airflow"
  project       = local.project_id
  location      = "EU"
  force_destroy = false
  versioning {
    enabled = false
  }
}
