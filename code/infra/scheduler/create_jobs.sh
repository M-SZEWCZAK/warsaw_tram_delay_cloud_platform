#!/usr/bin/env bash
# infra/scheduler/create_jobs.sh
# Creates all Cloud Scheduler jobs.
# Run once after `terraform apply`.
set -euo pipefail

: "${GCP_PROJECT_ID:?}" "${GCP_REGION:?}"

SA="tram-scheduler@${GCP_PROJECT_ID}.iam.gserviceaccount.com"

echo "==> Creating Cloud Scheduler jobs…"

# ── Timetable Loader — 03:00 daily ───────────────────────────────────────────
gcloud scheduler jobs create http timetable-loader-daily \
  --project="${GCP_PROJECT_ID}" \
  --location="${GCP_REGION}" \
  --schedule="0 3 * * *" \
  --time-zone="Europe/Warsaw" \
  --uri="https://${GCP_REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${GCP_PROJECT_ID}/jobs/timetable-loader:run" \
  --http-method=POST \
  --oauth-service-account-email="${SA}" \
  --description="Triggers timetable-loader Cloud Run Job daily at 03:00 Warsaw time"

# ── Position Ingestor — every 60 s, 04:00–01:00 ──────────────────────────────
# Cloud Scheduler minimum granularity is 1 minute.
# We create a single per-minute job; the job itself exits quickly.
gcloud scheduler jobs create http position-ingestor-every-minute \
  --project="${GCP_PROJECT_ID}" \
  --location="${GCP_REGION}" \
  --schedule="* 4-23,0 * * *" \
  --time-zone="Europe/Warsaw" \
  --uri="https://${GCP_REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${GCP_PROJECT_ID}/jobs/position-ingestor:run" \
  --http-method=POST \
  --oauth-service-account-email="${SA}" \
  --description="Polls tram positions every minute during operating hours"

# ── Weather Ingestor — every 10 minutes ──────────────────────────────────────
gcloud scheduler jobs create http weather-ingestor-every-10min \
  --project="${GCP_PROJECT_ID}" \
  --location="${GCP_REGION}" \
  --schedule="*/10 * * * *" \
  --time-zone="Europe/Warsaw" \
  --uri="https://${GCP_REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${GCP_PROJECT_ID}/jobs/weather-ingestor:run" \
  --http-method=POST \
  --oauth-service-account-email="${SA}" \
  --description="Polls Warsaw weather every 10 minutes"

# ── Prediction API warm-up: scale to 1 at 03:55 ──────────────────────────────
# Keeps one replica warm before tram service starts to avoid cold-start breaches.
gcloud scheduler jobs create http prediction-api-warmup \
  --project="${GCP_PROJECT_ID}" \
  --location="${GCP_REGION}" \
  --schedule="55 3 * * *" \
  --time-zone="Europe/Warsaw" \
  --uri="https://prediction-api-${GCP_PROJECT_ID}.${GCP_REGION}.run.app/health" \
  --http-method=GET \
  --description="Warm-up ping so Prediction API is ready by 04:00"

echo "==> All scheduler jobs created."
