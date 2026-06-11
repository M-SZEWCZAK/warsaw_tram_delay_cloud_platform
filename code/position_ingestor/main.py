"""
position_ingestor/main.py
Cloud Run Job — triggered by Cloud Scheduler every 60 seconds, 04:00–01:00.

Each invocation:
  1. GETs live tram positions from the Warsaw API
  2. Publishes all positions as a batch to Pub/Sub
  3. Exits (Cloud Run Job lifecycle; Cloud Scheduler re-triggers it)
"""
import json
import logging
import sys
from datetime import datetime, timezone

import requests
from google.cloud import pubsub_v1

sys.path.insert(0, ".")
from shared.config import (
    WARSAW_API_KEY,
    WARSAW_API_BASE,
    RESOURCE_TRAM_POSITIONS,
    GCP_PROJECT_ID,
    PUBSUB_POSITIONS_TOPIC,
    BQ_TABLE_POSITIONS_RAW,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

INGESTED_AT = datetime.now(timezone.utc).isoformat()


# ── Fetch positions ───────────────────────────────────────────────────────────

# def fetch_tram_positions() -> list[dict]:
#     """
#     GET /api/action/wsstore_get — returns all active tram positions.
#     Response fields: VehicleNumber, Lines, Brigade, Lat, Lon, Time.
#     """
#     resp = requests.get(
#         f"{WARSAW_API_BASE}/wsstore_get",
#         params={
#             "id":     RESOURCE_TRAM_POSITIONS,
#             "type":   "2",
#             "apikey": WARSAW_API_KEY,
#         },
#         timeout=(15, 60),
#     )
#     resp.raise_for_status()
#     data = resp.json()
#     return data.get("result", [])

def fetch_tram_positions() -> list[dict]:
    """
    POST /api/action/get_ztm_lokalizacja_pojazdow — returns all active tram positions.
    Response fields: VehicleNumber, Lines, Brigade, Lat, Lon, Time.
    """
    url = f"{WARSAW_API_BASE}/get_ztm_lokalizacja_pojazdow"
    api_key = WARSAW_API_KEY.strip()
    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json"
    }

    payload = {
        "type": 2  # 1 for buses, 2 for trams
    }

    # Using a shorter timeout for local testing so you don't hang forever if it fails
    resp = requests.post(
        url,
        json=payload,
        headers=headers,
        timeout=(15, 60),
    )

    resp.raise_for_status()
    data = resp.json()

    return data if isinstance(data, list) else []

# ── Publish to Pub/Sub ────────────────────────────────────────────────────────

def publish_positions(positions: list[dict]) -> None:
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_POSITIONS_TOPIC)

    futures = []
    for pos in positions:
        message = {
            "vehicle_number": str(pos.get("VehicleNumber", "")),
            "line":           str(pos.get("Lines", "")),
            "brigade":        str(pos.get("Brigade", "")),
            "lat":            float(pos.get("Lat", 0)),
            "lon":            float(pos.get("Lon", 0)),
            "gps_time":       pos.get("Time", INGESTED_AT),
            "ingested_at":    INGESTED_AT,
        }
        data = json.dumps(message).encode("utf-8")
        futures.append(
            publisher.publish(
                topic_path,
                data,
                source="position_ingestor",   # message attribute for routing
            )
        )

    # Wait for all publishes to complete
    for future in futures:
        future.result(timeout=20)

    log.info("Published %d position messages to %s", len(futures), PUBSUB_POSITIONS_TOPIC)


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    try:
        positions = fetch_tram_positions()
        log.info("Fetched %d tram positions from Warsaw API", len(positions))

        if not positions:
            log.warning("No positions returned — tram service may be outside operating hours.")
            return

        publish_positions(positions)

    except Exception:
        log.exception("Position ingestor failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
