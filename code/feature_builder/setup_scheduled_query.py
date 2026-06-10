"""
feature_builder/setup_scheduled_query.py
One-time setup: creates (or replaces) the BigQuery Scheduled Query for feature
materialisation.  Run this once during deployment — not as a regular job.

Usage:
  python -m feature_builder.setup_scheduled_query
"""
import os
import sys
sys.path.insert(0, ".")
from shared.config import GCP_PROJECT_ID, GCP_REGION, BQ_DATASET

from google.cloud import bigquery_datatransfer_v1
from google.protobuf.field_mask_pb2 import FieldMask


def main():
    client = bigquery_datatransfer_v1.DataTransferServiceClient()
    parent = f"projects/{GCP_PROJECT_ID}/locations/{GCP_REGION}"

    # Read the SQL template and substitute the project placeholder
    sql_path = os.path.join(os.path.dirname(__file__), "feature_query.sql")
    with open(sql_path) as f:
        query = f.read().replace("@GCP_PROJECT_ID", GCP_PROJECT_ID)

    config = bigquery_datatransfer_v1.TransferConfig(
        display_name="tram_feature_builder",
        data_source_id="scheduled_query",
        destination_dataset_id=BQ_DATASET,
        schedule="every day 02:00",
        params={
            "query": query,
            "destination_table_name_template": "features",
            "write_disposition": "WRITE_TRUNCATE",
        },
    )

    response = client.create_transfer_config(
        parent=parent,
        transfer_config=config,
    )
    print(f"Scheduled query created: {response.name}")


if __name__ == "__main__":
    main()
