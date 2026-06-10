#!/usr/bin/env bash
# build_and_deploy.sh
# Builds all Docker images and pushes them to Artifact Registry.
# Run from the repository root after `terraform apply`.
set -euo pipefail

GCP_PROJECT_ID="${GCP_PROJECT_ID:-warsaw-tram-platform}"
GCP_REGION="${GCP_REGION:-europe-central2}"

REGISTRY="${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/tram-platform"

gcloud auth configure-docker "${GCP_REGION}-docker.pkg.dev" --quiet

build_push() {
  local service=$1
  local tag="${REGISTRY}/${service}:latest"
  echo "==> Building ${service}…"
  docker build -t "${tag}" -f "${service}/Dockerfile" .
  docker push "${tag}"
  echo "    Pushed ${tag}"
}

build_push timetable_loader
build_push position_ingestor
build_push weather_ingestor
build_push prediction_api

# Update Cloud Run services / jobs to the new images
gcloud run jobs update timetable-loader  --image="${REGISTRY}/timetable_loader:latest"  --region="${GCP_REGION}" --project="${GCP_PROJECT_ID}"
gcloud run jobs update position-ingestor --image="${REGISTRY}/position_ingestor:latest" --region="${GCP_REGION}" --project="${GCP_PROJECT_ID}"
gcloud run jobs update weather-ingestor  --image="${REGISTRY}/weather_ingestor:latest"  --region="${GCP_REGION}" --project="${GCP_PROJECT_ID}"
gcloud run services update prediction-api --image="${REGISTRY}/prediction_api:latest"   --region="${GCP_REGION}" --project="${GCP_PROJECT_ID}"

echo "==> All services updated."
