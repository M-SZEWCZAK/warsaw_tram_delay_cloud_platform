#!/usr/bin/env bash
# stream_processor/deploy.sh
# Builds a Dataflow Flex Template and launches the streaming job.
# Run from the repo root: bash stream_processor/deploy.sh
set -euo pipefail

PROJECT=warsaw-tram-platform
REGION=europe-central2
REGISTRY="europe-central2-docker.pkg.dev/${PROJECT}/tram-platform"
IMAGE="${REGISTRY}/stream-processor:latest"
BUCKET="warsaw-tram-platform-trams-raw-archive"
TEMPLATE_PATH="gs://${BUCKET}/dataflow/flex-template.json"

echo "==> Step 1: Build and push stream processor image"
gcloud builds submit \
  --tag "${IMAGE}" \
  --file stream_processor/Dockerfile \
  .

echo "==> Step 2: Upload Flex Template metadata to GCS"
gsutil cp stream_processor/flex_template_metadata.json \
  "gs://${BUCKET}/dataflow/flex_template_metadata.json"

echo "==> Step 3: Build Flex Template spec"
gcloud dataflow flex-template build "${TEMPLATE_PATH}" \
  --image="${IMAGE}" \
  --sdk-language=PYTHON \
  --metadata-file=stream_processor/flex_template_metadata.json \
  --project="${PROJECT}"

echo "==> Step 4: Launch Flex Template job"
gcloud dataflow flex-template run tram-stream-processor \
  --template-file-gcs-location="${TEMPLATE_PATH}" \
  --region="${REGION}" \
  --project="${PROJECT}" \
  --parameters="^|^project=${PROJECT}|region=${REGION}" \
  --additional-experiments=enable_prime \
  --worker-machine-type=n1-standard-2 \
  --num-workers=1 \
  --max-workers=3

echo "==> Stream processor deployed. Monitor at:"
echo "    https://console.cloud.google.com/dataflow/jobs?project=${PROJECT}"
