"""
stream_processor/pipeline.py
Apache Beam streaming pipeline — runs on Google Dataflow.

Reads:
  - Pub/Sub tram-positions topic
  - Pub/Sub tram-weather topic

Writes:
  - BigQuery positions_raw       (every message)
  - BigQuery positions_enriched  (with delay_s, matched stop, weather)
  - Firestore vehicles/{vehicle_number}   (upsert, TTL 2 h)

Side inputs are loaded directly inside DoFn workers rather than as Beam
PCollection side inputs.  This avoids all window-alignment problems:
  - Timetable  →  fetched from BigQuery once at setup(), refreshed daily
                  after 04:00 Warsaw time (safe: the 03:00 job has finished).
  - Weather    →  maintained as a small in-process deque; the weather
                  Pub/Sub subscription is consumed by a separate ParDo that
                  writes into a shared Firestore document so every position
                  worker can read it without cross-stream joins.

Run locally (DirectRunner):
  python -m stream_processor.pipeline \
    --runner=DirectRunner \
    --project=$GCP_PROJECT_ID \
    --region=$GCP_REGION

Deploy to Dataflow:
  python -m stream_processor.pipeline \
    --runner=DataflowRunner \
    --project=$GCP_PROJECT_ID \
    --region=$GCP_REGION \
    --temp_location=gs://$GCS_ARCHIVE_BUCKET/tmp \
    --staging_location=gs://$GCS_ARCHIVE_BUCKET/staging \
    --streaming
"""
import json
import logging
import math
import os
from collections import deque
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import Any

import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions,SetupOptions

from shared.config import (
    GCP_PROJECT_ID,
    BQ_TABLE_POSITIONS_RAW,
    BQ_TABLE_POSITIONS_ENRICHED,
    PUBSUB_POSITIONS_TOPIC,
    PUBSUB_WEATHER_TOPIC,
    FIRESTORE_COLLECTION,
    BQ_TABLE_TIMETABLE,
    BQ_DATASET,
)
from shared.bq_schemas import POSITIONS_RAW_SCHEMA, POSITIONS_ENRICHED_SCHEMA

log = logging.getLogger(__name__)

STOP_MATCH_RADIUS_M = 150   # metres
WEATHER_TTL_S       = 600   # keep weather snapshots for 10 minutes
WARSAW_TZ           = ZoneInfo("Europe/Warsaw")

# Firestore document that holds the latest weather snapshot written by
# CacheWeatherInFirestore.  All EnrichPosition workers read from it.
WEATHER_DOC_ID = "_latest_weather"


# ── Geo helper ────────────────────────────────────────────────────────────────

def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    )
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ── Parse DoFns ───────────────────────────────────────────────────────────────

class ParsePosition(beam.DoFn):
    def process(self, element: bytes):
        try:
            yield json.loads(element.decode("utf-8"))
        except Exception as exc:
            log.warning("Failed to parse position message: %s", exc)


class ParseWeather(beam.DoFn):
    def process(self, element: bytes):
        try:
            yield json.loads(element.decode("utf-8"))
        except Exception as exc:
            log.warning("Failed to parse weather message: %s", exc)


# ── Weather side-path: write latest snapshot to Firestore ─────────────────────

class CacheWeatherInFirestore(beam.DoFn):
    """
    Receives parsed weather dicts and upserts a single well-known Firestore
    document (WEATHER_DOC_ID) with the latest values.  EnrichPosition workers
    poll this document instead of receiving weather as a Beam side input,
    which eliminates all cross-stream windowing issues.
    """

    def __init__(self, project_id):
        self.project_id = project_id  # serialized with the DoFn
    def setup(self):
        from google.cloud import firestore
        self._db = firestore.Client(project=self.project_id)
        self._col = self._db.collection(FIRESTORE_COLLECTION)

    def process(self, element: dict):
        element["fetched_at"] = element.get(
            "fetched_at", datetime.now(timezone.utc).isoformat()
        )
        self._col.document(WEATHER_DOC_ID).set(element)
        yield element


# ── Enrichment DoFn ───────────────────────────────────────────────────────────

class EnrichPosition(beam.DoFn):
    """
    Enriches a position dict with:
      - timetable match  (loaded from BigQuery in setup(), refreshed daily)
      - weather snapshot (fetched from Firestore, cached locally for 60 s)

    No Beam side inputs are used, so there are no window-alignment problems.
    """

    # ── Timetable ─────────────────────────────────────────────────────────────
    def __init__(self, project_id):
        self.project_id = project_id
    def setup(self):
        self._timetable: list[dict] = []
        self._timetable_loaded_date = None   # datetime.date
        self._weather_cache: dict | None = None
        self._weather_cache_ts: float = 0.0  # time.monotonic() stamp

        from google.cloud import firestore
        self._db = firestore.Client(project=self.project_id)

        self._load_timetable()

    def _load_timetable(self):
        from google.cloud import bigquery
        client = bigquery.Client(project=self.project_id)
        query = f"""
            SELECT stop_id, stop_name, lat, lon, line, brigade,
                   CAST(scheduled_departure AS STRING) AS scheduled_departure
            FROM `{BQ_TABLE_TIMETABLE}`
            WHERE load_date = CURRENT_DATE('Europe/Warsaw')
        """
        try:
            rows = list(client.query(query).result())
            self._timetable = [dict(r) for r in rows]
            self._timetable_loaded_date = datetime.now(WARSAW_TZ).date()
            log.info("Timetable loaded: %d rows", len(self._timetable))
        except Exception as exc:
            log.error("Failed to load timetable: %s", exc)

    def _maybe_refresh_timetable(self):
        """
        Reload the timetable once per day, but only after 04:00 Warsaw time
        (the nightly load job runs at 03:00 and is done well before then).
        Between 01:00–04:00 there are no tram positions anyway, so the stale
        timetable from yesterday is fine for the handful of late-night messages.
        """
        now_warsaw = datetime.now(WARSAW_TZ)
        today = now_warsaw.date()

        if (
            self._timetable_loaded_date != today
            and now_warsaw.hour >= 4
        ):
            log.info("Refreshing timetable for %s", today)
            self._load_timetable()

    # ── Weather ───────────────────────────────────────────────────────────────

    def _get_weather(self) -> dict:
        """
        Return a weather dict, using a short in-process cache (60 s) so we
        don't hit Firestore on every message.
        """
        import time
        now = time.monotonic()
        if now - self._weather_cache_ts < 60 and self._weather_cache is not None:
            return self._weather_cache

        try:
            doc = (
                self._db
                .collection(FIRESTORE_COLLECTION)
                .document(WEATHER_DOC_ID)
                .get()
            )
            self._weather_cache = doc.to_dict() or {}
        except Exception as exc:
            log.warning("Could not fetch weather from Firestore: %s", exc)
            self._weather_cache = self._weather_cache or {}

        self._weather_cache_ts = now
        return self._weather_cache

    # ── Main ──────────────────────────────────────────────────────────────────

    def process(self, position: dict):
        self._maybe_refresh_timetable()

        lat      = position.get("lat", 0.0)
        lon      = position.get("lon", 0.0)
        line     = position.get("line", "")
        brigade  = position.get("brigade", "")
        gps_time = position.get("gps_time", "")

        try:
            gps_dt = datetime.fromisoformat(gps_time.replace("Z", "+00:00"))
        except Exception:
            gps_dt = datetime.now(timezone.utc)

        # ── Nearest stop ──────────────────────────────────────────────────────
        best_stop = None
        best_dist = float("inf")
        for entry in self._timetable:
            if entry.get("line") != line or entry.get("brigade") != brigade:
                continue
            d = haversine_m(lat, lon, entry["lat"], entry["lon"])
            if d < best_dist:
                best_dist = d
                best_stop = entry

        matched_stop_id     = None
        scheduled_departure = None
        delay_s             = None

        if best_stop and best_dist <= STOP_MATCH_RADIUS_M:
            matched_stop_id     = best_stop["stop_id"]
            scheduled_departure = best_stop.get("scheduled_departure")
            if scheduled_departure:
                try:
                    sched_dt = datetime.fromisoformat(
                        scheduled_departure.replace("Z", "+00:00")
                    )
                    delay_s = int((gps_dt - sched_dt).total_seconds())
                except Exception:
                    pass

        # ── Weather ───────────────────────────────────────────────────────────
        weather   = self._get_weather()
        precip_mm = weather.get("precip_mm")
        temp_c    = weather.get("temp_c")

        yield {
            **position,
            "matched_stop_id":     matched_stop_id,
            "scheduled_departure": scheduled_departure,
            "delay_s":             delay_s,
            "precip_mm":           precip_mm,
            "temp_c":              temp_c,
        }


# ── Vehicle state DoFn ────────────────────────────────────────────────────────

class WriteToFirestore(beam.DoFn):
    """Upsert live vehicle state into Firestore vehicles/{vehicle_number}."""

    def __init__(self, project_id):
        self.project_id = project_id  # serialized with the DoFn
    def setup(self):
        from google.cloud import firestore
        self._db = firestore.Client(project=self.project_id)

    def process(self, element: dict):
        vehicle_number = element.get("vehicle_number", "unknown")
        self._db.collection(FIRESTORE_COLLECTION).document(vehicle_number).set({
            "vehicle_number": vehicle_number,
            "line":           element.get("line"),
            "brigade":        element.get("brigade"),
            "lat":            element.get("lat"),
            "lon":            element.get("lon"),
            "delay_s":        element.get("delay_s"),
            "next_stop_id":   element.get("matched_stop_id"),
            "updated":        int(datetime.now(timezone.utc).timestamp()),
        })
        yield element


# ── BQ schema helper ──────────────────────────────────────────────────────────

def _bq_schema(fields):
    return {"fields": [
        {"name": f.name, "type": f.field_type, "mode": f.mode}
        for f in fields
    ]}


# ── Pipeline ──────────────────────────────────────────────────────────────────

def run(argv=None):
    import sys
    raw_args = argv if argv is not None else sys.argv[1:]
    cleaned_args = [a for a in raw_args if not a.startswith("--streaming")]

    options = PipelineOptions(cleaned_args)
    options.view_as(StandardOptions).streaming = True
    options.view_as(SetupOptions).requirements_file = "/app/stream_processor/requirements.txt"

    weather_sub = "projects/warsaw-tram-platform/subscriptions/tram-weather-sub"
    positions_sub = "projects/warsaw-tram-platform/subscriptions/tram-positions-sub"
    project_id = os.environ.get("GCP_PROJECT_ID")
    with beam.Pipeline(options=options) as p:

        # ── Weather path: parse → cache in Firestore ──────────────────────────
        # No windowing, no side-input AsList — just write the latest snapshot
        # to a known Firestore document so EnrichPosition can read it cheaply.
        _ = (
            p
            | "ReadWeather"           >> ReadFromPubSub(subscription=weather_sub)
            | "ParseWeather"          >> beam.ParDo(ParseWeather())
            | "CacheWeatherFirestore" >> beam.ParDo(CacheWeatherInFirestore(project_id))
        )

        # ── Position path ─────────────────────────────────────────────────────
        # No windowing needed — we're not using PCollection side inputs at all.
        positions = (
            p
            | "ReadPositions"  >> ReadFromPubSub(subscription=positions_sub)
            | "ParsePositions" >> beam.ParDo(ParsePosition())
        )

        # Write raw positions to BigQuery immediately
        _ = (
            positions
            | "WriteRawToBQ" >> WriteToBigQuery(
                table=BQ_TABLE_POSITIONS_RAW,
                schema=_bq_schema(POSITIONS_RAW_SCHEMA),
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

        # Enrich (timetable + weather looked up inside the DoFn worker)
        enriched = (
            positions
            | "EnrichPositions" >> beam.ParDo(EnrichPosition(project_id))
        )

        # Write enriched positions to BigQuery
        _ = (
            enriched
            | "WriteEnrichedToBQ" >> WriteToBigQuery(
                table=BQ_TABLE_POSITIONS_ENRICHED,
                schema=_bq_schema(POSITIONS_ENRICHED_SCHEMA),
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

        # Write live vehicle state to Firestore
        _ = (
            enriched
            | "WriteToFirestore" >> beam.ParDo(WriteToFirestore(project_id))
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()