"""
prediction_api/main.py
Cloud Run service — serves REST predictions via Vertex AI Endpoint.

Endpoints:
  GET  /health         — liveness probe (no auth)
  POST /v1/delay       — predict delay for a tram at a stop (API key auth)

The service scales to zero between requests.
To mitigate cold-start latency (25–45 s), a min-replica Cloud Scheduler
toggle keeps one replica warm during 04:00–01:00 (see infra/scheduler/).
"""
import json
import logging
import os
from datetime import datetime

from fastapi import FastAPI, HTTPException, Security, status
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel, Field
import uvicorn

import sys
sys.path.insert(0, ".")
from shared.config import (
    GCP_PROJECT_ID,
    GCP_REGION,
    VERTEX_ENDPOINT_ID,
    BQ_TABLE_FEATURES,
    PREDICTION_API_PORT,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = FastAPI(title="Warsaw Tram Delay Prediction API", version="1.0.0")

# ── Auth ──────────────────────────────────────────────────────────────────────
API_KEY_NAME = "X-API-Key"
_API_KEY = os.environ.get("PREDICTION_API_KEY", "")
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=True)


def verify_api_key(api_key: str = Security(api_key_header)) -> str:
    if not _API_KEY or api_key != _API_KEY:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid or missing API key",
        )
    return api_key


# ── Request / Response schemas ────────────────────────────────────────────────

class DelayRequest(BaseModel):
    line:      str = Field(..., example="3")
    stop_id:   str = Field(..., example="1001")
    brigade:   str = Field(..., example="1")
    timestamp: str = Field(..., example="2025-09-01T08:15:00Z")


class DelayResponse(BaseModel):
    delay_seconds: int
    confidence:    float
    model_version: str
    features_used: list[str]


# ── Feature lookup ────────────────────────────────────────────────────────────

def fetch_features(line: str, stop_id: str, brigade: str, hour: int) -> dict:
    """
    Pull pre-materialised features from BigQuery features table.
    Falls back to zeros if no matching row exists.
    """
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT_ID)
    query = f"""
        SELECT mean_delay_s, stddev_delay_s, rain_delay_corr, peak_hour_flag, sample_count
        FROM `{BQ_TABLE_FEATURES}`
        WHERE line = @line
          AND stop_id = @stop_id
          AND brigade = @brigade
          AND hour_of_day = @hour
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("line",    "STRING",  line),
            bigquery.ScalarQueryParameter("stop_id", "STRING",  stop_id),
            bigquery.ScalarQueryParameter("brigade", "STRING",  brigade),
            bigquery.ScalarQueryParameter("hour",    "INTEGER", hour),
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())
    if rows:
        return dict(rows[0])
    return {
        "mean_delay_s":    0.0,
        "stddev_delay_s":  0.0,
        "rain_delay_corr": 0.0,
        "peak_hour_flag":  False,
        "sample_count":    0,
    }


# ── Vertex AI prediction ──────────────────────────────────────────────────────

def call_vertex_endpoint(instances: list[dict]) -> dict:
    """
    Send a prediction request to the deployed Vertex AI Endpoint.
    Returns the first prediction result dict.
    """
    from google.cloud import aiplatform

    aiplatform.init(project=GCP_PROJECT_ID, location=GCP_REGION)
    endpoint = aiplatform.Endpoint(endpoint_name=VERTEX_ENDPOINT_ID)
    response = endpoint.predict(instances=instances)
    if not response.predictions:
        raise RuntimeError("Vertex AI returned no predictions")
    return response.predictions[0]


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health", status_code=200)
def health():
    return {"status": "ok"}


@app.post("/v1/delay", response_model=DelayResponse)
def predict_delay(
    req: DelayRequest,
    _: str = Security(verify_api_key),
) -> DelayResponse:
    try:
        ts = datetime.fromisoformat(req.timestamp.replace("Z", "+00:00"))
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid timestamp format. Use ISO 8601.")

    hour = ts.hour

    # 1. Fetch features from BQ
    feats = fetch_features(req.line, req.stop_id, req.brigade, hour)

    # 2. Build instance for Vertex AI
    instance = {
        "line":              req.line,
        "stop_id":           req.stop_id,
        "brigade":           req.brigade,
        "hour_of_day":       hour,
        "mean_delay_s":      feats.get("mean_delay_s", 0.0),
        "stddev_delay_s":    feats.get("stddev_delay_s", 0.0),
        "rain_delay_corr":   feats.get("rain_delay_corr", 0.0),
        "peak_hour_flag":    int(feats.get("peak_hour_flag", False)),
    }

    # 3. Call Vertex AI
    prediction = call_vertex_endpoint([instance])

    return DelayResponse(
        delay_seconds=int(prediction.get("delay_seconds", 0)),
        confidence=float(prediction.get("confidence", 0.5)),
        model_version=str(prediction.get("model_version", "unknown")),
        features_used=list(instance.keys()),
    )


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run("prediction_api.main:app", host="0.0.0.0", port=PREDICTION_API_PORT)
