"""
weather_ingestor/main.py
Cloud Run Job — triggered by Cloud Scheduler every 10 minutes.

Fetches current Warsaw weather from the city API and publishes a single
message to Pub/Sub.  The Stream Processor side-joins this against GPS pings.
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
    RESOURCE_WEATHER,
    GCP_PROJECT_ID,
    PUBSUB_WEATHER_TOPIC,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

FETCHED_AT = datetime.now(timezone.utc).isoformat()


# ── Fetch weather ─────────────────────────────────────────────────────────────

def fetch_weather() -> dict:
    """
    POST /api/action/get_zom_pogoda — returns current Warsaw weather observations.
    Relevant fields: temperatura, opad, predkosc_wiatru, widzialnosc.
    """
    url = f"{WARSAW_API_BASE}/get_zom_pogoda"
    api_key = WARSAW_API_KEY.strip()
    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json"
    }

    resp = requests.post(
        url,
        headers=headers,
        timeout=(45, 180),
    )
    resp.raise_for_status()
    data = resp.json()

    # Extract the nested result key safely
    obs=data[0]

    def safe_float(val) -> float | None:
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    return {
        "temp_c":          safe_float(obs.get("temp_pow")),
        "precip_mm":       safe_float(obs.get("int_opadu")),
        "wind_ms":         safe_float(obs.get("pred_wiatru")),
        "fetched_at":      FETCHED_AT,
    }


# ── Publish ───────────────────────────────────────────────────────────────────

def publish_weather(weather: dict) -> None:
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_WEATHER_TOPIC)

    data = json.dumps(weather).encode("utf-8")
    future = publisher.publish(
        topic_path,
        data,
        source="weather_ingestor",
    )
    future.result(timeout=10)
    log.info("Published weather snapshot to %s: %s", PUBSUB_WEATHER_TOPIC, weather)


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    try:
        weather = fetch_weather()
        publish_weather(weather)
    except Exception:
        log.exception("Weather ingestor failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
