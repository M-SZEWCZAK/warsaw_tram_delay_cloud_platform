"""
shared/config.py
Central configuration pulled from environment variables.
All services import from here so only one place needs updating.
"""
import os

# ── Warsaw Open Data API ──────────────────────────────────────────────────────
WARSAW_API_KEY: str = os.environ["WARSAW_API_KEY"]
WARSAW_API_BASE: str = "https://api.um.warszawa.pl/api/action"

# Resource IDs (constants published by the city; do not change without checking
# the catalogue at https://dane.um.warszawa.pl)
RESOURCE_STOPS: str = "ab75c33d-3a26-4342-b36a-6e5fef0a3ac3"      # all tram stops
RESOURCE_LINES_AT_STOP: str = "88cd555f-6f31-43ca-9de4-66c479ad5942"  # lines per stop
RESOURCE_BRIGADE_SCHEDULE: str = "e923fa0e-d96c-43f9-ae6e-60518c9f3238"  # brigade timetable
RESOURCE_TRAM_POSITIONS: str = "c7238cfe-8b1f-4c38-bb4a-de386db7e776"  # live positions
RESOURCE_WEATHER: str = "adaa4f0a-4ac1-4429-857c-4e6f46705c93"        # Warsaw weather

# ── Google Cloud Platform ─────────────────────────────────────────────────────
GCP_PROJECT_ID: str = os.environ["GCP_PROJECT_ID"]
GCP_REGION: str = os.environ.get("GCP_REGION", "europe-central2")

# BigQuery
BQ_DATASET: str = os.environ.get("BQ_DATASET", "trams_warsaw")
BQ_TABLE_TIMETABLE: str = f"{GCP_PROJECT_ID}.{BQ_DATASET}.timetable"
BQ_TABLE_POSITIONS_RAW: str = f"{GCP_PROJECT_ID}.{BQ_DATASET}.positions_raw"
BQ_TABLE_POSITIONS_ENRICHED: str = f"{GCP_PROJECT_ID}.{BQ_DATASET}.positions_enriched"
BQ_TABLE_FEATURES: str = f"{GCP_PROJECT_ID}.{BQ_DATASET}.features"

# Pub/Sub
PUBSUB_POSITIONS_TOPIC: str = os.environ.get("PUBSUB_POSITIONS_TOPIC", "tram-positions")
PUBSUB_WEATHER_TOPIC: str = os.environ.get("PUBSUB_WEATHER_TOPIC", "tram-weather")

# Firestore
FIRESTORE_COLLECTION: str = os.environ.get("FIRESTORE_COLLECTION", "vehicles")
FIRESTORE_VEHICLE_TTL_SECONDS: int = 7200  # 2 hours

# Cloud Storage
GCS_ARCHIVE_BUCKET: str = os.environ.get("GCS_ARCHIVE_BUCKET", "trams-raw-archive")
GCS_ML_BUCKET: str = os.environ.get("GCS_ML_BUCKET", "trams-ml-artefacts")

# Vertex AI
VERTEX_ENDPOINT_ID: str = os.environ.get("VERTEX_ENDPOINT_ID", "")
PREDICTION_API_PORT: int = int(os.environ.get("PORT", "8080"))

# ── Operational window ────────────────────────────────────────────────────────
OPERATING_WINDOW_START_HOUR: int = 4   # 04:00
OPERATING_WINDOW_END_HOUR: int = 1     # 01:00 (next day)

# ── Timetable loader tuning ───────────────────────────────────────────────────
TIMETABLE_ASYNC_CONCURRENCY: int = 40  # bounded async pool size
TIMETABLE_REQUEST_TIMEOUT_S: int = 10  # per-request HTTP timeout
