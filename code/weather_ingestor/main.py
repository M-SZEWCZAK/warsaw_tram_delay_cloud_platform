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
    GET /api/action/imgw_get — returns current Warsaw weather observations.
    Relevant fields: temperatura, opad, predkosc_wiatru, widzialnosc.
    """
    resp = requests.get(
        f"{WARSAW_API_BASE}/imgw_get",
        params={
            "id":     RESOURCE_WEATHER,
            "apikey": WARSAW_API_KEY,
        },
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    result = data.get("result", [{}])
    # The API returns a list; take the first (most recent) observation
    obs = result[0] if result else {}

    def safe_float(val) -> float | None:
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    return {
        "temp_c":          safe_float(obs.get("temperatura")),
        "precip_mm":       safe_float(obs.get("opad")),
        "wind_ms":         safe_float(obs.get("predkosc_wiatru")),
        "visibility_km":   safe_float(obs.get("widzialnosc")),
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
