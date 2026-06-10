"""
timetable_loader/main.py
Cloud Run Job — triggered by Cloud Scheduler at 03:00 daily.

Hierarchy:
  1. GET all tram stops
  2. For each stop  →  GET lines at that stop          (parallel, pool=40)
  3. For each stop+line → GET brigade schedules         (parallel, pool=40)
  4. Write everything to BigQuery timetable table
  5. Write load_status document to Firestore
"""
import asyncio
import logging
import sys
from datetime import date, datetime, timezone

import aiohttp
from google.cloud import bigquery, firestore

# Allow running from repo root: python -m timetable_loader.main
sys.path.insert(0, ".")
from shared.config import (
    WARSAW_API_KEY,
    WARSAW_API_BASE,
    RESOURCE_STOPS,
    RESOURCE_LINES_AT_STOP,
    RESOURCE_BRIGADE_SCHEDULE,
    GCP_PROJECT_ID,
    BQ_DATASET,
    BQ_TABLE_TIMETABLE,
    TIMETABLE_ASYNC_CONCURRENCY,
    TIMETABLE_REQUEST_TIMEOUT_S,
    FIRESTORE_COLLECTION,
)
from shared.bq_schemas import TIMETABLE_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TODAY = date.today().isoformat()
LOAD_TIMESTAMP = datetime.now(timezone.utc).isoformat()


# ── Warsaw API helpers ────────────────────────────────────────────────────────

async def fetch_json(session: aiohttp.ClientSession, url: str, params: dict) -> dict:
    """Single GET call with retries (up to 3) on transient errors."""
    params["apikey"] = WARSAW_API_KEY
    for attempt in range(3):
        try:
            async with session.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=TIMETABLE_REQUEST_TIMEOUT_S)
            ) as resp:
                resp.raise_for_status()
                return await resp.json(content_type=None)
        except Exception as exc:
            if attempt == 2:
                raise
            await asyncio.sleep(1.5 ** attempt)
    return {}


async def get_all_stops(session: aiohttp.ClientSession) -> list[dict]:
    """Step 1 — download all tram stops."""
    data = await fetch_json(
        session,
        f"{WARSAW_API_BASE}/dbstore_get",
        {"id": RESOURCE_STOPS},
    )
    return data.get("result", [])


async def get_lines_for_stop(
    session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, stop: dict
) -> tuple[dict, list[str]]:
    """Step 2 — download tram lines for a single stop."""
    async with semaphore:
        data = await fetch_json(
            session,
            f"{WARSAW_API_BASE}/dbtimetable_get",
            {
                "id": RESOURCE_LINES_AT_STOP,
                "busstopId": stop["zespol"],
                "busstopNr": stop["slupek"],
            },
        )
    lines = [row["values"][0]["value"] for row in data.get("result", [])]
    return stop, lines


async def get_brigades_for_stop_line(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    stop: dict,
    line: str,
) -> list[dict]:
    """Step 3 — brigade schedules for one (stop, line) pair."""
    async with semaphore:
        data = await fetch_json(
            session,
            f"{WARSAW_API_BASE}/dbtimetable_get",
            {
                "id": RESOURCE_BRIGADE_SCHEDULE,
                "busstopId": stop["zespol"],
                "busstopNr": stop["slupek"],
                "line": line,
            },
        )

    rows = []
    for entry in data.get("result", []):
        values = {v["key"]: v["value"] for v in entry.get("values", [])}
        brigade = values.get("brygada")
        time_str = values.get("czas")  # format: "HH:MM:SS" (may exceed 23 for night)
        if not brigade or not time_str:
            continue

        # Convert "HH:MM:SS" (ZTM uses >24 h notation for night services)
        try:
            h, m, s = map(int, time_str.split(":"))
            # Normalise hours > 23 to next calendar day
            extra_days = h // 24
            h = h % 24
            departure_dt = datetime.combine(
                date.fromisoformat(TODAY), __import__("datetime").time(h, m, s)
            )
            if extra_days:
                departure_dt = departure_dt.replace(
                    day=departure_dt.day + extra_days
                )
            departure_iso = departure_dt.isoformat()
        except Exception:
            departure_iso = None

        rows.append({
            "stop_id":             stop["zespol"],
            "stop_name":           stop.get("nazwa_zespolu", ""),
            "stop_nr":             stop["slupek"],
            "lat":                 float(stop.get("szer_geo", 0) or 0),
            "lon":                 float(stop.get("dlug_geo", 0) or 0),
            "line":                line,
            "brigade":             brigade,
            "scheduled_departure": departure_iso,
            "load_date":           TODAY,
        })
    return rows


# ── BigQuery writer ───────────────────────────────────────────────────────────

def ensure_bq_table(client: bigquery.Client) -> None:
    """Create or confirm the timetable table with correct schema."""
    dataset_ref = bigquery.DatasetReference(GCP_PROJECT_ID, BQ_DATASET)
    table_ref = dataset_ref.table("timetable")
    table = bigquery.Table(table_ref, schema=TIMETABLE_SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(field="load_date")
    try:
        client.create_table(table, exists_ok=True)
    except Exception as exc:
        log.warning("Table creation check failed: %s", exc)


def write_to_bigquery(rows: list[dict]) -> None:
    client = bigquery.Client(project=GCP_PROJECT_ID)
    ensure_bq_table(client)

    BATCH = 1000
    errors = []
    for i in range(0, len(rows), BATCH):
        chunk = rows[i : i + BATCH]
        errs = client.insert_rows_json(BQ_TABLE_TIMETABLE, chunk)
        if errs:
            errors.extend(errs)

    if errors:
        log.error("BQ insert errors (sample): %s", errors[:5])
        raise RuntimeError(f"{len(errors)} BQ insert errors")

    log.info("Wrote %d rows to %s", len(rows), BQ_TABLE_TIMETABLE)


# ── Firestore status doc ──────────────────────────────────────────────────────

def write_load_status(row_count: int, success: bool) -> None:
    db = firestore.Client(project=GCP_PROJECT_ID)
    db.collection("_meta").document("timetable_load_status").set({
        "load_date":       TODAY,
        "loaded_at":       LOAD_TIMESTAMP,
        "row_count":       row_count,
        "success":         success,
    })
    log.info("Firestore load_status written (success=%s, rows=%d)", success, row_count)


# ── Orchestrator ──────────────────────────────────────────────────────────────

async def run() -> None:
    semaphore = asyncio.Semaphore(TIMETABLE_ASYNC_CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=TIMETABLE_ASYNC_CONCURRENCY)

    async with aiohttp.ClientSession(connector=connector) as session:
        # 1. Stops
        log.info("Fetching all tram stops…")
        stops = await get_all_stops(session)
        log.info("Found %d stops", len(stops))

        # 2. Lines per stop (parallel)
        log.info("Fetching lines for each stop (concurrency=%d)…", TIMETABLE_ASYNC_CONCURRENCY)
        stop_lines_tasks = [
            get_lines_for_stop(session, semaphore, stop) for stop in stops
        ]
        stop_lines_results = await asyncio.gather(*stop_lines_tasks, return_exceptions=True)

        # 3. Brigades per (stop, line) (parallel)
        brigade_tasks = []
        for result in stop_lines_results:
            if isinstance(result, Exception):
                log.warning("stop/lines fetch error: %s", result)
                continue
            stop, lines = result
            for line in lines:
                brigade_tasks.append(
                    get_brigades_for_stop_line(session, semaphore, stop, line)
                )

        log.info("Fetching brigade schedules for %d (stop, line) pairs…", len(brigade_tasks))
        brigade_results = await asyncio.gather(*brigade_tasks, return_exceptions=True)

    # Flatten
    all_rows: list[dict] = []
    for res in brigade_results:
        if isinstance(res, Exception):
            log.warning("Brigade fetch error: %s", res)
        else:
            all_rows.extend(res)

    log.info("Total timetable rows assembled: %d", len(all_rows))

    # 4. Write to BigQuery
    write_to_bigquery(all_rows)

    # 5. Signal completion
    write_load_status(row_count=len(all_rows), success=True)
    log.info("Timetable load complete.")


def main() -> None:
    try:
        asyncio.run(run())
    except Exception:
        log.exception("Timetable loader failed")
        write_load_status(row_count=0, success=False)
        sys.exit(1)


if __name__ == "__main__":
    main()
