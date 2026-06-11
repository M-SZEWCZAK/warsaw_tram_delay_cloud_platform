"""
training_job/train.py
Vertex AI Custom Training Job.

Pipeline:
  1. Read features from BigQuery (trams_warsaw.features)
  2. Read recent positions_enriched for labels
  3. Train an XGBoost regressor to predict delay_s
  4. Evaluate on a hold-out split
  5. Save model artefact to GCS  (trams-ml-artefacts/models/<run_id>/)
  6. Register + deploy to Vertex AI Endpoint
     - If no endpoint exists yet: create one
     - If one exists: deploy new model version, shift traffic

Run locally:
  GCP_PROJECT_ID=warsaw-tram-platform \
  GCP_REGION=europe-central2 \
  GCS_ML_BUCKET=warsaw-tram-platform-trams-ml-artefacts \
  python -m training_job.train

Deployed as Vertex AI Custom Job via training_job/submit.py.
"""
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone

import joblib
import numpy as np
import pandas as pd
from google.cloud import bigquery, storage
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from xgboost import XGBRegressor

sys.path.insert(0, ".")
from shared.config import (
    GCP_PROJECT_ID,
    GCP_REGION,
    BQ_TABLE_FEATURES,
    BQ_TABLE_POSITIONS_ENRICHED,
    GCS_ML_BUCKET,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

RUN_ID     = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
MODEL_DIR  = f"models/{RUN_ID}"
MODEL_FILE = "model.joblib"
META_FILE  = "metadata.json"

FEATURE_COLS = [
    "hour_of_day",
    "peak_hour_flag",
    "mean_delay_s",
    "stddev_delay_s",
    "rain_delay_corr",
    "line_enc",
    "stop_id_enc",
]
TARGET_COL = "delay_s"

# ── Data loading ──────────────────────────────────────────────────────────────

def load_training_data() -> pd.DataFrame:
    """
    Join positions_enriched (labels) with features (aggregates) from BigQuery.
    Returns a flat DataFrame ready for training.
    """
    client = bigquery.Client(project=GCP_PROJECT_ID)

    query = f"""
    SELECT
        e.line,
        e.matched_stop_id                                       AS stop_id,
        e.brigade,
        EXTRACT(HOUR FROM e.gps_time AT TIME ZONE 'Europe/Warsaw') AS hour_of_day,
        e.delay_s,
        e.precip_mm,
        e.temp_c,
        f.mean_delay_s,
        f.stddev_delay_s,
        f.rain_delay_corr,
        f.peak_hour_flag
    FROM `{BQ_TABLE_POSITIONS_ENRICHED}` e
    LEFT JOIN `{BQ_TABLE_FEATURES}` f
        ON  e.line            = f.line
        AND e.matched_stop_id = f.stop_id
        AND e.brigade         = f.brigade
        AND EXTRACT(HOUR FROM e.gps_time AT TIME ZONE 'Europe/Warsaw') = f.hour_of_day
    WHERE
        e.delay_s        IS NOT NULL
        AND e.matched_stop_id IS NOT NULL
        AND ABS(e.delay_s) < 1800   -- discard outliers > 30 min
    ORDER BY RAND()
    LIMIT 500000
    """

    log.info("Loading training data from BigQuery…")
    df = client.query(query).to_dataframe()
    log.info("Loaded %d rows", len(df))
    return df


# ── Feature engineering ───────────────────────────────────────────────────────

def engineer_features(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Label-encode categoricals and fill nulls. Returns df + encoder map."""
    encoders = {}

    for col in ["line", "stop_id"]:
        le = LabelEncoder()
        df[f"{col}_enc"] = le.fit_transform(df[col].fillna("UNKNOWN").astype(str))
        encoders[col] = le

    df["peak_hour_flag"] = df["peak_hour_flag"].fillna(False).astype(int)
    df["mean_delay_s"]   = df["mean_delay_s"].fillna(0.0)
    df["stddev_delay_s"] = df["stddev_delay_s"].fillna(0.0)
    df["rain_delay_corr"]= df["rain_delay_corr"].fillna(0.0)

    return df, encoders


# ── Training ──────────────────────────────────────────────────────────────────

def train_model(df: pd.DataFrame) -> tuple:
    X = df[FEATURE_COLS]
    y = df[TARGET_COL]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.15, random_state=42
    )

    model = XGBRegressor(
        n_estimators=400,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        tree_method="hist",
        random_state=42,
        n_jobs=-1,
    )

    log.info("Training XGBoost on %d samples…", len(X_train))
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=50,
    )

    y_pred = model.predict(X_test)
    mae  = mean_absolute_error(y_test, y_pred)
    r2   = r2_score(y_test, y_pred)
    log.info("Evaluation — MAE: %.1f s  R²: %.3f", mae, r2)

    return model, mae, r2, X_train.shape[0]


# ── GCS upload ────────────────────────────────────────────────────────────────

def upload_to_gcs(model, encoders: dict, mae: float, r2: float, n_train: int) -> str:
    """Save model + metadata to GCS. Returns gs:// URI of the model directory."""
    import tempfile, os as _os

    client = storage.Client(project=GCP_PROJECT_ID)
    bucket = client.bucket(GCS_ML_BUCKET)

    with tempfile.TemporaryDirectory() as tmp:
        # Model
        model_path = _os.path.join(tmp, MODEL_FILE)
        joblib.dump({"model": model, "encoders": encoders, "features": FEATURE_COLS}, model_path)
        bucket.blob(f"{MODEL_DIR}/{MODEL_FILE}").upload_from_filename(model_path)

        # Metadata
        meta = {
            "run_id":      RUN_ID,
            "trained_at":  datetime.now(timezone.utc).isoformat(),
            "mae_seconds": round(mae, 2),
            "r2":          round(r2, 4),
            "n_train":     n_train,
            "features":    FEATURE_COLS,
            "model_type":  "XGBRegressor",
        }
        meta_path = _os.path.join(tmp, META_FILE)
        with open(meta_path, "w") as f:
            json.dump(meta, f, indent=2)
        bucket.blob(f"{MODEL_DIR}/{META_FILE}").upload_from_filename(meta_path)

    gcs_uri = f"gs://{GCS_ML_BUCKET}/{MODEL_DIR}"
    log.info("Model uploaded to %s", gcs_uri)
    return gcs_uri


# ── Vertex AI registration & deployment ──────────────────────────────────────

def deploy_to_vertex(gcs_uri: str, mae: float) -> str:
    """
    Upload model to Vertex AI Model Registry and deploy to an Endpoint.
    Returns the endpoint resource name.
    """
    from google.cloud import aiplatform

    aiplatform.init(project=GCP_PROJECT_ID, location=GCP_REGION)

    # 1. Upload model
    log.info("Uploading model to Vertex AI Model Registry…")
    model = aiplatform.Model.upload(
        display_name=f"tram-delay-model-{RUN_ID}",
        artifact_uri=gcs_uri,
        serving_container_image_uri=(
            f"{GCP_REGION}-docker.pkg.dev/{GCP_PROJECT_ID}/tram-platform/prediction-api:latest"
        ),
        serving_container_predict_route="/v1/delay",
        serving_container_health_route="/health",
        labels={"mae_s": str(int(mae)), "run_id": RUN_ID},
    )
    log.info("Model registered: %s", model.resource_name)

    # 2. Find or create endpoint
    endpoints = aiplatform.Endpoint.list(
        filter='display_name="tram-delay-endpoint"',
        order_by="create_time desc",
    )

    if endpoints:
        endpoint = endpoints[0]
        log.info("Using existing endpoint: %s", endpoint.resource_name)
    else:
        log.info("Creating new Vertex AI Endpoint…")
        endpoint = aiplatform.Endpoint.create(
            display_name="tram-delay-endpoint",
            labels={"project": "warsaw-tram"},
        )
        log.info("Endpoint created: %s", endpoint.resource_name)

    # 3. Deploy model to endpoint (shift 100% traffic to new model)
    log.info("Deploying model to endpoint…")
    endpoint.deploy(
        model=model,
        deployed_model_display_name=f"tram-delay-{RUN_ID}",
        machine_type="n1-standard-2",
        min_replica_count=0,
        max_replica_count=2,
        traffic_percentage=100,
    )
    log.info("Deployment complete: %s", endpoint.resource_name)
    return endpoint.resource_name


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    try:
        df = load_training_data()

        if len(df) < 1000:
            log.warning(
                "Only %d training rows available. Need more data — "
                "run the pipeline for a few days before training.",
                len(df),
            )
            # Still train with what we have for testing purposes
            if len(df) == 0:
                log.error("No data at all — aborting.")
                sys.exit(1)

        df, encoders = engineer_features(df)
        model, mae, r2, n_train = train_model(df)
        gcs_uri = upload_to_gcs(model, encoders, mae, r2, n_train)
        endpoint_name = deploy_to_vertex(gcs_uri, mae)

        log.info("Training job complete.")
        log.info("  GCS artefact : %s", gcs_uri)
        log.info("  Endpoint     : %s", endpoint_name)
        log.info("  MAE          : %.1f s", mae)
        log.info("  R²           : %.3f", r2)

    except Exception:
        log.exception("Training job failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
