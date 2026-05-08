variable "project_id" { type = string }
variable "region"     { default = "europe-central2" } # Warsaw
variable "zone"       { default = "europe-central2-a" }

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

# ============================================================
# Service Accounts
# ============================================================

resource "google_service_account" "ingestor_sa" {
  account_id   = "tram-ingestor-job-sa"
  display_name = "SA for Ingestor Cloud Run Jobs"
}

resource "google_service_account" "dataflow_sa" {
  account_id   = "tram-stream-processor-sa"
  display_name = "SA for Dataflow Stream Processor"
}

resource "google_service_account" "prediction_sa" {
  account_id   = "tram-prediction-api-sa"
  display_name = "SA for Prediction API & Vertex AI"
}

# FIX: Added SA for Feature Builder scheduled queries
resource "google_service_account" "feature_builder_sa" {
  account_id   = "tram-feature-builder-sa"
  display_name = "SA for Feature Builder BQ Scheduled Queries"
}

# ============================================================
# IAM Bindings
# ============================================================

resource "google_project_iam_member" "ingestor_pubsub" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.ingestor_sa.email}"
}

resource "google_project_iam_member" "ingestor_bq" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.ingestor_sa.email}"
}

resource "google_project_iam_member" "ingestor_run_invoker" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.ingestor_sa.email}"
}

resource "google_project_iam_member" "dataflow_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "dataflow_bq" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "dataflow_firestore" {
  project = var.project_id
  role    = "roles/datastore.user"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "dataflow_storage" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "dataflow_pubsub" {
  project = var.project_id
  role    = "roles/pubsub.subscriber"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "prediction_vertex" {
  project = var.project_id
  role    = "roles/aiplatform.user"
  member  = "serviceAccount:${google_service_account.prediction_sa.email}"
}

resource "google_project_iam_member" "prediction_firestore" {
  project = var.project_id
  role    = "roles/datastore.user"
  member  = "serviceAccount:${google_service_account.prediction_sa.email}"
}

resource "google_project_iam_member" "feature_builder_bq" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.feature_builder_sa.email}"
}

resource "google_project_iam_member" "feature_builder_bq_job" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.feature_builder_sa.email}"
}

resource "google_project_iam_member" "feature_builder_storage" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.feature_builder_sa.email}"
}

# ============================================================
# BigQuery
# ============================================================

resource "google_bigquery_dataset" "trams_ds" {
  dataset_id = "trams_warsaw"
  location   = "EU"
}

resource "google_bigquery_table" "timetable" {
  dataset_id          = google_bigquery_dataset.trams_ds.dataset_id
  table_id            = "timetable"
  deletion_protection = false
}

resource "google_bigquery_table" "positions_raw" {
  dataset_id          = google_bigquery_dataset.trams_ds.dataset_id
  table_id            = "positions_raw"
  deletion_protection = false
  time_partitioning { type = "DAY" }
}

resource "google_bigquery_table" "positions_enriched" {
  dataset_id          = google_bigquery_dataset.trams_ds.dataset_id
  table_id            = "positions_enriched"
  deletion_protection = false
  # FIX: Added missing type field to time_partitioning
  time_partitioning {
    type  = "DAY"
    field = "gps_time"
  }
  schema = <<EOF
[
  {"name": "vehicle_number",       "type": "STRING"},
  {"name": "line",                 "type": "STRING"},
  {"name": "brigade",              "type": "STRING"},
  {"name": "lat",                  "type": "FLOAT"},
  {"name": "lon",                  "type": "FLOAT"},
  {"name": "matched_stop_id",      "type": "STRING"},
  {"name": "scheduled_departure",  "type": "TIMESTAMP"},
  {"name": "delay_s",              "type": "INTEGER"},
  {"name": "precip_mm",            "type": "FLOAT"},
  {"name": "temp_c",               "type": "FLOAT"},
  {"name": "gps_time",             "type": "TIMESTAMP"}
]
EOF
}

resource "google_bigquery_table" "ml_features" {
  dataset_id          = google_bigquery_dataset.trams_ds.dataset_id
  table_id            = "ml_features"
  deletion_protection = false
  time_partitioning {
    type  = "DAY"
    field = "gps_time"
  }
}

resource "google_bigquery_data_transfer_config" "feature_builder" {
  display_name           = "feature-builder"
  location               = "EU"
  data_source_id         = "scheduled_query"
  schedule               = "every 24 hours"
  service_account_name   = google_service_account.feature_builder_sa.email
  params = {
    query = <<SQL
SELECT
  vehicle_number, line, brigade,
  AVG(delay_s)    AS avg_delay_s,
  AVG(precip_mm)  AS avg_precip_mm,
  AVG(temp_c)     AS avg_temp_c,
  DATE(gps_time)  AS gps_time
FROM `${var.project_id}.trams_warsaw.positions_enriched`
WHERE DATE(gps_time) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
GROUP BY 1, 2, 3, 7
SQL
    destination_table_name_template = "${var.project_id}:trams_warsaw.ml_features"
    write_disposition               = "WRITE_APPEND"
  }
}

# ============================================================
# Firestore
# ============================================================

resource "google_firestore_database" "database" {
  name        = "(default)"
  location_id = "eur3"
  type        = "FIRESTORE_NATIVE"
}

# ============================================================
# Cloud Storage
# ============================================================

resource "google_storage_bucket" "archive" {
  name     = "${var.project_id}-trams-raw-archive"
  location = "EU"
  lifecycle_rule {
    action { type = "Delete" }
    condition { age = 60 }
  }
}

resource "google_storage_bucket" "ml_artefacts" {
  name     = "${var.project_id}-trams-ml-artefacts"
  location = "EU"
  lifecycle_rule {
    action { type = "Delete" }
    condition { age = 365 }
  }
}

# FIX: Added templates bucket for Dataflow Flex Template spec
resource "google_storage_bucket" "dataflow_templates" {
  name     = "${var.project_id}-templates"
  location = "EU"
}

# ============================================================
# Pub/Sub Topics & Subscriptions
# ============================================================

resource "google_pubsub_topic" "positions" { name = "tram-positions" }
resource "google_pubsub_topic" "weather"   { name = "weather-data" }

resource "google_pubsub_subscription" "positions_dataflow" {
  name  = "tram-positions-dataflow-sub"
  topic = google_pubsub_topic.positions.name

  ack_deadline_seconds = 60

  expiration_policy {
    ttl = "" # never expires
  }
}

# ============================================================
# Cloud Run Jobs: Ingestors
# ============================================================

resource "google_cloud_run_v2_job" "loader" {
  name     = "timetable-loader"
  location = var.region
  template {
    template {
      service_account = google_service_account.ingestor_sa.email
      containers { image = "gcr.io/${var.project_id}/timetable-loader:latest" }
    }
  }
}

resource "google_cloud_scheduler_job" "loader_cron" {
  name     = "cron-timetable-loader"
  schedule = "0 3 * * *"
  region   = var.region
  http_target {
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.loader.name}:run"
    http_method = "POST"
    oauth_token { service_account_email = google_service_account.ingestor_sa.email }
  }
}

resource "google_cloud_run_v2_job" "pos_ingestor" {
  name     = "position-ingestor"
  location = var.region
  template {
    template {
      service_account = google_service_account.ingestor_sa.email
      containers { image = "gcr.io/${var.project_id}/position-ingestor:latest" }
    }
  }
}

resource "google_cloud_scheduler_job" "pos_cron_day" {
  name     = "cron-position-ingestor-day"
  schedule = "* 4-23 * * *" # 04:00–23:59
  region   = var.region      
  http_target {
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.pos_ingestor.name}:run"
    http_method = "POST"
    oauth_token { service_account_email = google_service_account.ingestor_sa.email }
  }
}

resource "google_cloud_scheduler_job" "pos_cron_night" {
  name     = "cron-position-ingestor-night"
  schedule = "* 0-1 * * *" # 00:00–01:59
  region   = var.region
  http_target {
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.pos_ingestor.name}:run"
    http_method = "POST"
    oauth_token { service_account_email = google_service_account.ingestor_sa.email }
  }
}

resource "google_cloud_run_v2_job" "weather_ingestor" {
  name     = "weather-ingestor"
  location = var.region
  template {
    template {
      service_account = google_service_account.ingestor_sa.email
      containers { image = "gcr.io/${var.project_id}/weather-ingestor:latest" }
    }
  }
}

resource "google_cloud_scheduler_job" "weather_cron" {
  name     = "cron-weather-ingestor"
  schedule = "*/30 * * * *" # every 30 min — adjust to your API rate limit
  region   = var.region
  http_target {
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.weather_ingestor.name}:run"
    http_method = "POST"
    oauth_token { service_account_email = google_service_account.ingestor_sa.email }
  }
}

# ============================================================
# Dataflow Stream Processor
# ============================================================

resource "google_dataflow_flex_template_job" "stream_processor" {
  provider                = google-beta
  name                    = "tram-stream-processor"
  container_spec_gcs_path = "gs://${google_storage_bucket.dataflow_templates.name}/stream-processor.json"
  parameters = {
    inputSubscription = google_pubsub_subscription.positions_dataflow.id
    bqTable           = "${var.project_id}:${google_bigquery_dataset.trams_ds.dataset_id}.${google_bigquery_table.positions_enriched.table_id}"
    gcsBucket         = google_storage_bucket.archive.name
  }
  service_account_email = google_service_account.dataflow_sa.email
}

# ============================================================
# Vertex AI & Prediction API
# ============================================================

resource "google_vertex_ai_endpoint" "endpoint" {
  name         = "tram-delay-endpoint"
  display_name = "Tram Delay Prediction"
  location     = var.region
}

resource "google_cloud_run_v2_service" "prediction_api" {
  name     = "prediction-api"
  location = var.region
  template {
    service_account = google_service_account.prediction_sa.email
    containers {
      image = "gcr.io/${var.project_id}/prediction-api:latest"
      env {
        name  = "VERTEX_ENDPOINT"
        value = google_vertex_ai_endpoint.endpoint.id
      }
      env {
        name  = "GCP_PROJECT"
        value = var.project_id
      }
    }
  }
}

# Uses a custom container submitted to Vertex AI Training
resource "google_vertex_ai_dataset" "training_dataset" {
  display_name = "tram-delay-training-dataset"
  metadata_schema_uri = "gs://google-cloud-aiplatform/schema/dataset/metadata/tabular_1.0.0.yaml"
  region = var.region
}

resource "google_cloudbuild_trigger" "training_job" {
  name        = "tram-training-job-trigger"
  description = "Triggers a Vertex AI custom training job on demand or schedule"
  location    = var.region

  # Trigger manually or wire to a Pub/Sub / scheduler as needed
  webhook_config {
    secret = "projects/${var.project_id}/secrets/training-trigger-secret/versions/latest"
  }

  build {
    step {
      name = "gcr.io/google.com/cloudsdktool/cloud-sdk"
      args = [
        "gcloud", "ai", "custom-jobs", "create",
        "--region=${var.region}",
        "--display-name=tram-delay-training",
        "--worker-pool-spec=machine-type=n1-standard-4,replica-count=1,container-image-uri=gcr.io/${var.project_id}/training-job:latest",
        "--service-account=${google_service_account.feature_builder_sa.email}"
      ]
    }
    options { logging = "CLOUD_LOGGING_ONLY" }
  }
}
