"""
training_job/submit.py
Submits the training job as a Vertex AI Custom Job.
Run this locally or from Cloud Shell to trigger training on-demand.

Usage:
  python -m training_job.submit

For weekly scheduled runs, set up a Cloud Scheduler job that calls this
script via a Cloud Run trigger or Cloud Function.
"""
import sys
sys.path.insert(0, ".")
from shared.config import GCP_PROJECT_ID, GCP_REGION, GCS_ML_BUCKET

from google.cloud import aiplatform

def submit():
    aiplatform.init(project=GCP_PROJECT_ID, location=GCP_REGION)

    image_uri = (
        f"{GCP_REGION}-docker.pkg.dev/{GCP_PROJECT_ID}"
        f"/tram-platform/training-job:latest"
    )

    job = aiplatform.CustomContainerTrainingJob(
        display_name="tram-delay-training",
        container_uri=image_uri,
    )

    job.run(
        replica_count=1,
        machine_type="n1-standard-4",
        base_output_dir=f"gs://{GCS_ML_BUCKET}/vertex_output",
        environment_variables={
            "GCP_PROJECT_ID": GCP_PROJECT_ID,
            "GCP_REGION":     GCP_REGION,
            "GCS_ML_BUCKET":  GCS_ML_BUCKET,
        },
        sync=True,  # wait for completion
    )

    print("Training job complete.")

if __name__ == "__main__":
    submit()
