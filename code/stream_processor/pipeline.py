"""
stream_processor/pipeline.py
Apache Beam streaming pipeline — runs on Google Dataflow.

Reads:
  - Pub/Sub tram-positions topic
  - Pub/Sub tram-weather topic (as a slow-changing side input)
  - BigQuery timetable table (as a bounded side input, loaded once at startup)

Writes:
  - BigQuery positions_raw       (every message)
  - BigQuery positions_enriched  (with delay_s, matched stop, weather)
  - Firestore vehicles/{vehicle_number}   (upsert, TTL 2 h)

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
from datetime import datetime, timezone
from typing import Any

import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.util import BatchElements

# Allow running from repo root
import sys
sys.path.insert(0, ".")
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

# ── Constants ─────────────────────────────────────────────────────────────────
STOP_MATCH_RADIUS_M = 150   # max metres from GPS ping to count as "at stop"
WEATHER_WINDOW_S    = 600   # weather snapshot valid for 10 minutes


# ── Geo helper ────────────────────────────────────────────────────────────────

def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance in metres between two lat/lon points."""
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi  = math.radians(lat2 - lat1)
    dlam  = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ── DoFns ─────────────────────────────────────────────────────────────────────

class ParsePosition(beam.DoFn):
    """Parse a raw Pub/Sub bytes message into a position dict."""

    def process(self, element: bytes):
        try:
            msg = json.loads(element.decode("utf-8"))
            yield msg
        except Exception as exc:
            log.warning("Failed to parse position message: %s", exc)


class ParseWeather(beam.DoFn):
    """Parse a raw Pub/Sub bytes weather message."""

    def process(self, element: bytes):
        try:
            yield json.loads(element.decode("utf-8"))
        except Exception as exc:
            log.warning("Failed to parse weather message: %s", exc)


class EnrichPosition(beam.DoFn):
    """
    Join a position message against:
      - timetable side input  →  matched_stop_id, scheduled_departure, delay_s
      - weather side input    →  precip_mm, temp_c
    """

    def process(
        self,
        position: dict,
        timetable_side: list[dict],
        weather_side: list[dict],
    ):
        lat = position.get("lat", 0.0)
        lon = position.get("lon", 0.0)
        line = position.get("line", "")
        brigade = position.get("brigade", "")
        gps_time_str = position.get("gps_time", "")

        try:
            gps_dt = datetime.fromisoformat(gps_time_str.replace("Z", "+00:00"))
        except Exception:
            gps_dt = datetime.now(timezone.utc)

        # ── Nearest stop match ────────────────────────────────────────────────
        best_stop = None
        best_dist = float("inf")
        for entry in timetable_side:
            if entry.get("line") != line or entry.get("brigade") != brigade:
                continue
            d = haversine_m(lat, lon, entry["lat"], entry["lon"])
            if d < best_dist:
                best_dist = d
                best_stop = entry

        matched_stop_id    = None
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

        # ── Latest weather snapshot ───────────────────────────────────────────
        precip_mm = None
        temp_c    = None
        if weather_side:
            latest = max(
                weather_side,
                key=lambda w: w.get("fetched_at", ""),
                default=None,
            )
            if latest:
                precip_mm = latest.get("precip_mm")
                temp_c    = latest.get("temp_c")

        enriched = {
            **position,
            "matched_stop_id":     matched_stop_id,
            "scheduled_departure": scheduled_departure,
            "delay_s":             delay_s,
            "precip_mm":           precip_mm,
            "temp_c":              temp_c,
        }
        yield enriched


class WriteToFirestore(beam.DoFn):
    """
    Upsert each enriched position into Firestore vehicles/{vehicle_number}.
    Initialises the Firestore client lazily (once per worker process).
    """

    def setup(self):
        from google.cloud import firestore
        self._db = firestore.Client(project=GCP_PROJECT_ID)

    def process(self, element: dict):
        vehicle_number = element.get("vehicle_number", "unknown")
        doc_ref = self._db.collection(FIRESTORE_COLLECTION).document(vehicle_number)
        doc_ref.set({
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


# ── Side-input loaders ────────────────────────────────────────────────────────

def load_timetable_from_bq() -> list[dict]:
    """
    Load today's timetable from BigQuery as a list of dicts.
    Called once at pipeline startup to create a bounded side input.
    """
    from google.cloud import bigquery
    from datetime import date

    client = bigquery.Client(project=GCP_PROJECT_ID)
    query = f"""
        SELECT stop_id, stop_name, lat, lon, line, brigade, scheduled_departure
        FROM `{BQ_TABLE_TIMETABLE}`
        WHERE load_date = CURRENT_DATE('Europe/Warsaw')
    """
    rows = list(client.query(query).result())
    return [dict(row) for row in rows]


# ── Pipeline definition ───────────────────────────────────────────────────────

def run(argv=None):
    options = PipelineOptions(argv)
    options.view_as(StandardOptions).streaming = True

    positions_sub = f"projects/{GCP_PROJECT_ID}/subscriptions/{PUBSUB_POSITIONS_TOPIC}-sub"
    weather_sub   = f"projects/{GCP_PROJECT_ID}/subscriptions/{PUBSUB_WEATHER_TOPIC}-sub"

    with beam.Pipeline(options=options) as p:

        # ── Side inputs ───────────────────────────────────────────────────────
        # Timetable: loaded once via BigQuery Storage Read API (Dataflow built-in)
        timetable_side = (
            p
            | "ReadTimetable" >> beam.io.ReadFromBigQuery(
                query=f"""
                    SELECT stop_id, stop_name, lat, lon, line, brigade,
                           CAST(scheduled_departure AS STRING) AS scheduled_departure
                    FROM `{BQ_TABLE_TIMETABLE}`
                    WHERE load_date = CURRENT_DATE('Europe/Warsaw')
                """,
                use_standard_sql=True,
                project=GCP_PROJECT_ID,
            )
        )
        timetable_view = beam.pvalue.AsList(timetable_side)

        # Weather: windowed global latest (slow-changing)
        weather_stream = (
            p
            | "ReadWeather"  >> ReadFromPubSub(subscription=weather_sub)
            | "ParseWeather" >> beam.ParDo(ParseWeather())
        )
        weather_view = beam.pvalue.AsList(weather_stream)

        # ── Main stream ───────────────────────────────────────────────────────
        positions = (
            p
            | "ReadPositions"  >> ReadFromPubSub(subscription=positions_sub)
            | "ParsePositions" >> beam.ParDo(ParsePosition())
        )

        # Write raw positions to BQ immediately
        _ = (
            positions
            | "WriteRawToBQ" >> WriteToBigQuery(
                table=BQ_TABLE_POSITIONS_RAW,
                schema={"fields": [
                    {"name": f.name, "type": f.field_type, "mode": f.mode}
                    for f in POSITIONS_RAW_SCHEMA
                ]},
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

        # Enrich with timetable match + weather
        enriched = (
            positions
            | "EnrichPositions" >> beam.ParDo(
                EnrichPosition(),
                timetable_side=timetable_view,
                weather_side=weather_view,
            )
        )

        # Write enriched to BQ
        _ = (
            enriched
            | "WriteEnrichedToBQ" >> WriteToBigQuery(
                table=BQ_TABLE_POSITIONS_ENRICHED,
                schema={"fields": [
                    {"name": f.name, "type": f.field_type, "mode": f.mode}
                    for f in POSITIONS_ENRICHED_SCHEMA
                ]},
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

        # Write enriched to Firestore (live vehicle state)
        _ = (
            enriched
            | "WriteToFirestore" >> beam.ParDo(WriteToFirestore())
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
