provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

resource "google_storage_bucket" "f1_data_bucket" {
  name     = "${var.project}-f1-data-bucket"
  location = var.region
  force_destroy = true
}

resource "google_bigquery_dataset" "f1_dataset" {
  dataset_id = "f1_data"
  location   = var.region
}