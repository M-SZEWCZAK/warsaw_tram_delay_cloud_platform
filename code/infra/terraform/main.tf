# infra/terraform/main.tf
# Creates all GCP resources for the Warsaw Tram Delay Prediction Platform.
# Run: terraform init && terraform apply

terraform {
  required_version = ">= 1.7"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ── Variables ─────────────────────────────────────────────────────────────────

variable "project_id" {
    type = string
    }
variable "region"     {
    type = string
      default = "europe-central2"
      }
variable "bq_dataset" {
    type = string
    default = "trams_warsaw"
     }
variable "warsaw_api_key" {
  type      = string
  sensitive = true
}
variable "prediction_api_key" {
  type      = string
  sensitive = true
}
variable "images_ready" {
  type    = bool
  default = true
  description = "Set to true after Docker images have been pushed to Artifact Registry"
}

locals {
  image_prefix = "${var.region}-docker.pkg.dev/${var.project_id}/tram-platform"
}

# ── Artifact Registry ─────────────────────────────────────────────────────────

resource "google_artifact_registry_repository" "tram_platform" {
  location      = var.region
  repository_id = "tram-platform"
  format        = "DOCKER"
}

# ── BigQuery ──────────────────────────────────────────────────────────────────

resource "google_bigquery_dataset" "trams_warsaw" {
  dataset_id = var.bq_dataset
  location   = var.region
}

resource "google_bigquery_table" "timetable" {
  dataset_id          = google_bigquery_dataset.trams_warsaw.dataset_id
  table_id            = "timetable"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "load_date"
  }

  schema = jsonencode([
    { name = "stop_id",             type = "STRING",    mode = "REQUIRED" },
    { name = "stop_name",           type = "STRING",    mode = "NULLABLE" },
    { name = "stop_nr",             type = "STRING",    mode = "NULLABLE" },
    { name = "lat",                 type = "FLOAT64",   mode = "NULLABLE" },
    { name = "lon",                 type = "FLOAT64",   mode = "NULLABLE" },
    { name = "line",                type = "STRING",    mode = "REQUIRED" },
    { name = "brigade",             type = "STRING",    mode = "REQUIRED" },
    { name = "scheduled_departure", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "load_date",           type = "DATE",      mode = "REQUIRED" },
  ])
}

resource "google_bigquery_table" "positions_raw" {
  dataset_id          = google_bigquery_dataset.trams_warsaw.dataset_id
  table_id            = "positions_raw"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "ingested_at"
  }

  schema = jsonencode([
    { name = "vehicle_number", type = "STRING",    mode = "REQUIRED" },
    { name = "line",           type = "STRING",    mode = "NULLABLE" },
    { name = "brigade",        type = "STRING",    mode = "NULLABLE" },
    { name = "lat",            type = "FLOAT64",   mode = "NULLABLE" },
    { name = "lon",            type = "FLOAT64",   mode = "NULLABLE" },
    { name = "gps_time",       type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "ingested_at",    type = "TIMESTAMP", mode = "REQUIRED" },
  ])
}

resource "google_bigquery_table" "positions_enriched" {
  dataset_id          = google_bigquery_dataset.trams_warsaw.dataset_id
  table_id            = "positions_enriched"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "gps_time"
  }

  clustering = ["line"]

  schema = jsonencode([
    { name = "vehicle_number",      type = "STRING",    mode = "REQUIRED" },
    { name = "line",                type = "STRING",    mode = "NULLABLE" },
    { name = "brigade",             type = "STRING",    mode = "NULLABLE" },
    { name = "lat",                 type = "FLOAT64",   mode = "NULLABLE" },
    { name = "lon",                 type = "FLOAT64",   mode = "NULLABLE" },
    { name = "gps_time",            type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "ingested_at",         type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "matched_stop_id",     type = "STRING",    mode = "NULLABLE" },
    { name = "scheduled_departure", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "delay_s",             type = "INT64",     mode = "NULLABLE" },
    { name = "precip_mm",           type = "FLOAT64",   mode = "NULLABLE" },
    { name = "temp_c",              type = "FLOAT64",   mode = "NULLABLE" },
  ])
}

resource "google_bigquery_table" "features" {
  dataset_id          = google_bigquery_dataset.trams_warsaw.dataset_id
  table_id            = "features"
  deletion_protection = false

  clustering = ["line", "hour_of_day"]

  schema = jsonencode([
    { name = "line",             type = "STRING",    mode = "REQUIRED" },
    { name = "brigade",          type = "STRING",    mode = "REQUIRED" },
    { name = "stop_id",          type = "STRING",    mode = "REQUIRED" },
    { name = "hour_of_day",      type = "INT64",     mode = "REQUIRED" },
    { name = "mean_delay_s",     type = "FLOAT64",   mode = "NULLABLE" },
    { name = "stddev_delay_s",   type = "FLOAT64",   mode = "NULLABLE" },
    { name = "rain_delay_corr",  type = "FLOAT64",   mode = "NULLABLE" },
    { name = "peak_hour_flag",   type = "BOOL",      mode = "NULLABLE" },
    { name = "sample_count",     type = "INT64",     mode = "NULLABLE" },
    { name = "window_start",     type = "DATE",      mode = "REQUIRED" },
    { name = "window_end",       type = "DATE",      mode = "REQUIRED" },
    { name = "computed_at",      type = "TIMESTAMP", mode = "REQUIRED" },
  ])
}

# ── Cloud Storage ─────────────────────────────────────────────────────────────

resource "google_storage_bucket" "raw_archive" {
  name                        = "${var.project_id}-trams-raw-archive"
  location                    = var.region
  force_destroy               = false
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition { age = 60 }
    action    { type = "Delete" }
  }
}

resource "google_storage_bucket" "ml_artefacts" {
  name                        = "${var.project_id}-trams-ml-artefacts"
  location                    = var.region
  force_destroy               = false
  uniform_bucket_level_access = true
  versioning                  { enabled = true }
}

# ── Pub/Sub ───────────────────────────────────────────────────────────────────

resource "google_pubsub_topic" "positions" {
  name = "tram-positions"
  message_retention_duration = "604800s"  # 7 days
}

resource "google_pubsub_subscription" "positions_sub" {
  name  = "tram-positions-sub"
  topic = google_pubsub_topic.positions.name
  message_retention_duration = "604800s"
  ack_deadline_seconds = 60
}

resource "google_pubsub_topic" "weather" {
  name = "tram-weather"
  message_retention_duration = "604800s"
}

resource "google_pubsub_subscription" "weather_sub" {
  name  = "tram-weather-sub"
  topic = google_pubsub_topic.weather.name
  message_retention_duration = "604800s"
  ack_deadline_seconds = 60
}

# ── Secret Manager ────────────────────────────────────────────────────────────

resource "google_secret_manager_secret" "warsaw_api_key" {
  secret_id = "warsaw-api-key"
  replication {
      auto {}
       }
}

resource "google_secret_manager_secret_version" "warsaw_api_key" {
  secret      = google_secret_manager_secret.warsaw_api_key.id
  secret_data = var.warsaw_api_key
}

resource "google_secret_manager_secret" "prediction_api_key" {
  secret_id = "prediction-api-key"
  replication {
      auto {}
       }
}

resource "google_secret_manager_secret_version" "prediction_api_key" {
  secret      = google_secret_manager_secret.prediction_api_key.id
  secret_data = var.prediction_api_key
}

# ── Cloud Run — Timetable Loader (Job) ────────────────────────────────────────

resource "google_cloud_run_v2_job" "timetable_loader" {
  name     = "timetable-loader"
  location = var.region

  template {
    template {
      timeout = "3600s"  # 1 hour max
      max_retries = 3
      containers {
        image = var.images_ready ? "${local.image_prefix}/timetable-loader:latest" : "gcr.io/cloudrun/placeholder"
        env {
          name  = "GCP_PROJECT_ID"
          value = var.project_id
        }
        env {
          name = "WARSAW_API_KEY"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.warsaw_api_key.secret_id
              version = "latest"
            }
          }
        }
        resources {
          limits = { cpu = "2", memory = "2Gi" }
        }
      }
    }
  }
}

# ── Cloud Run — Position Ingestor (Job) ───────────────────────────────────────

resource "google_cloud_run_v2_job" "position_ingestor" {
  name     = "position-ingestor"
  location = var.region

  template {
    template {
      timeout     = "50s"
      max_retries = 1
      containers {
        image = var.images_ready ? "${local.image_prefix}/position-ingestor:latest" : "gcr.io/cloudrun/placeholder"
        env {
          name  = "GCP_PROJECT_ID"
          value = var.project_id
        }
        env {
          name  = "PUBSUB_POSITIONS_TOPIC"
          value = google_pubsub_topic.positions.name
        }
        env {
          name = "WARSAW_API_KEY"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.warsaw_api_key.secret_id
              version = "latest"
            }
          }
        }
        resources {
          limits = { cpu = "1", memory = "512Mi" }
        }
      }
    }
  }
}

# ── Cloud Run — Weather Ingestor (Job) ────────────────────────────────────────

resource "google_cloud_run_v2_job" "weather_ingestor" {
  name     = "weather-ingestor"
  location = var.region

  template {
    template {
      timeout     = "30s"
      max_retries = 1
      containers {
        image = var.images_ready ? "${local.image_prefix}/weather-ingestor:latest" : "gcr.io/cloudrun/placeholder"
        env {
          name  = "GCP_PROJECT_ID"
          value = var.project_id
        }
        env {
          name  = "PUBSUB_WEATHER_TOPIC"
          value = google_pubsub_topic.weather.name
        }
        env {
          name = "WARSAW_API_KEY"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.warsaw_api_key.secret_id
              version = "latest"
            }
          }
        }
        resources {
          limits = { cpu = "1", memory = "512Mi" }
        }
      }
    }
  }
}

# ── Cloud Run — Prediction API (Service) ─────────────────────────────────────

resource "google_cloud_run_v2_service" "prediction_api" {
  name     = "prediction-api"
  location = var.region

  template {
    scaling {
      min_instance_count = 0
      max_instance_count = 10
    }
    containers {
      image = var.images_ready ? "${local.image_prefix}/prediction-api:latest" : "gcr.io/cloudrun/placeholder"
      ports { container_port = 8080 }
      env {
        name  = "GCP_PROJECT_ID"
        value = var.project_id
      }
      env {
        name  = "GCP_REGION"
        value = var.region
      }
      env {
        name = "PREDICTION_API_KEY"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.prediction_api_key.secret_id
            version = "latest"
          }
        }
      }
      resources {
        limits = { cpu = "2", memory = "2Gi" }
      }
    }
  }
}
