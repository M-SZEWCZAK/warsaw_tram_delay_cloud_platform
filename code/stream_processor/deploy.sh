#!/usr/bin/env bash
# stream_processor/deploy.sh
# Submit the Beam pipeline to Dataflow.
# Usage: ./stream_processor/deploy.sh
set -euo pipefail

: "${GCP_PROJECT_ID:?}" "${GCP_REGION:?}" "${GCS_ARCHIVE_BUCKET:?}"
: "${PUBSUB_POSITIONS_TOPIC:?}" "${PUBSUB_WEATHER_TOPIC:?}"

python -m stream_processor.pipeline \
  --runner=DataflowRunner \
  --project="${GCP_PROJECT_ID}" \
  --region="${GCP_REGION}" \
  --temp_location="gs://${GCS_ARCHIVE_BUCKET}/tmp" \
  --staging_location="gs://${GCS_ARCHIVE_BUCKET}/staging" \
  --streaming \
  --job_name="tram-stream-processor" \
  --num_workers=2 \
  --max_num_workers=10 \
  --autoscaling_algorithm=THROUGHPUT_BASED \
  --worker_machine_type=n1-standard-2 \
  --save_main_session
