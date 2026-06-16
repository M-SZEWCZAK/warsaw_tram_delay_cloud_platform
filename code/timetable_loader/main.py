"""
timetable_loader/main.py
Cloud Run Job — triggered by Cloud Scheduler at 03:00 daily.
Optimized for Tram-only data with anti-throttling mechanisms.
"""
import asyncio
import logging
import sys
from datetime import date, datetime, timezone

import aiohttp
from google.cloud import bigquery, firestore

# Allow running from repo root
sys.path.insert(0, ".")
from shared.config import (
    WARSAW_API_KEY,
    WARSAW_API_BASE,
    GCP_PROJECT_ID,
    BQ_DATASET,
    BQ_TABLE_TIMETABLE,
    TIMETABLE_ASYNC_CONCURRENCY,  # Used for connection pool limits
    TIMETABLE_REQUEST_TIMEOUT_S,
    FIRESTORE_COLLECTION,
)
from shared.bq_schemas import TIMETABLE_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TODAY = date.today().isoformat()
LOAD_TIMESTAMP = datetime.now(timezone.utc).isoformat()

# ── Safe Pacing Configurations ────────────────────────────────────────────────
BATCH_CHUNK_SIZE = 100  # Maximum concurrent tasks allowed to fire in a wave
THROTTLE_DELAY_S = 0.08  # Pacing sleep (80ms) inside individual requests
BATCH_BREATHER_S = 1.5  # Resting period (1.5s) between major database waves


# ── Warsaw API Helpers ────────────────────────────────────────────────────────

def unpack_api_row(row: list | dict) -> dict:
    if isinstance(row, list):
        return {item["key"]: item["value"] for item in row if isinstance(item, dict) and "key" in item}
    if isinstance(row, dict) and "values" in row and isinstance(row["values"], list):
        return {item["key"]: item["value"] for item in row["values"] if "key" in item}
    return row if isinstance(row, dict) else {}


def is_tram_line(line: str) -> bool:
    """Returns True if the line string is a valid tram number (strictly 1-99, no letters)."""
    if not line:
        return False
    line_clean = line.strip()
    return line_clean.isdigit() and (1 <= int(line_clean) <= 99)


async def fetch_json(session: aiohttp.ClientSession, url: str, payload: dict = None) -> list | dict:
    """Wysyła zapytanie POST z wykładniczym backoffem, tłumieniem ruchu i bezpieczną obsługą błędów."""
    headers = {
        "Authorization": WARSAW_API_KEY.strip(),
        "Content-Type": "application/json"
    }

    kwargs = {"headers": headers, "timeout": aiohttp.ClientTimeout(total=TIMETABLE_REQUEST_TIMEOUT_S)}
    if payload is not None:
        kwargs["json"] = payload

    for attempt in range(3):
        try:
            async with session.post(url, **kwargs) as resp:
                resp.raise_for_status()

                # Active Throttling: Stop the loop from moving instantly to the next request
                await asyncio.sleep(THROTTLE_DELAY_S)

                return await resp.json(content_type=None)
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            if attempt == 2:
                log.warning("Skipping payload due to API failure after 3 attempts. Payload: %s. Error: %s", payload,
                            exc)
                return []

            backoff = 2.0 * (attempt + 1)
            log.info("Temporary API issue. Backing off for %.1fs before retry...", backoff)
            await asyncio.sleep(backoff)

    return []


async def get_all_stops(session: aiohttp.ClientSession) -> list[dict]:
    url = f"{WARSAW_API_BASE}/get_ztm_przystanki_komunikacji_miejskiej"
    data = await fetch_json(session, url, payload={})
    if isinstance(data, dict):
        return data.get("result", [])
    return data if isinstance(data, list) else []


async def get_lines_for_stop(session: aiohttp.ClientSession, stop: dict) -> tuple[dict, list[str]]:
    url = f"{WARSAW_API_BASE}/get_ztm_lista_linii_na_przystanku"

    flat_stop = unpack_api_row(stop)
    stop_id = flat_stop.get("zespol")
    stop_nr = flat_stop.get("slupek")

    payload = {"busstopId": stop_id, "busstopNr": stop_nr}
    data = await fetch_json(session, url, payload)

    raw_rows = data.get("result", []) if isinstance(data, dict) else data
    if not isinstance(raw_rows, list):
        raw_rows = []

    lines = []
    for row in raw_rows:
        flat_row = unpack_api_row(row)
        line_num = flat_row.get("linia")

        # Filter for TRAM lines only (numbers 1-99, no alphabetical suffixes)
        if line_num and is_tram_line(str(line_num)):
            lines.append(str(line_num))

    return stop, lines


async def get_brigades_for_stop_line(session: aiohttp.ClientSession, stop: dict, line: str) -> list[dict]:
    url = f"{WARSAW_API_BASE}/get_ztm_odjazdy_linii_z_przystanku"

    flat_stop = unpack_api_row(stop)
    stop_id = flat_stop.get("zespol")
    stop_nr = flat_stop.get("slupek")
    stop_name = flat_stop.get("nazwa_zespolu", "Unknown")
    lat_val = flat_stop.get("szer_geo", 0)
    lon_val = flat_stop.get("dlug_geo", 0)

    payload = {"busstopId": stop_id, "busstopNr": stop_nr, "line": line}
    data = await fetch_json(session, url, payload)

    raw_rows = data.get("result", []) if isinstance(data, dict) else data
    if not isinstance(raw_rows, list):
        raw_rows = []

    rows = []
    for entry in raw_rows:
        flat_entry = unpack_api_row(entry)
        brigade = flat_entry.get("brygada")
        time_str = flat_entry.get("czas")

        if not brigade or not time_str:
            continue

        try:
            h, m, s = map(int, time_str.split(":"))
            extra_days = h // 24
            h = h % 24
            departure_dt = datetime.combine(
                date.fromisoformat(TODAY), datetime.min.time().replace(hour=h, minute=m, second=s)
            )
            if extra_days:
                departure_dt = departure_dt + __import__("datetime").timedelta(days=extra_days)
            departure_iso = departure_dt.isoformat()
        except Exception:
            departure_iso = None

        rows.append({
            "stop_id": stop_id,
            "stop_name": stop_name,
            "stop_nr": stop_nr,
            "lat": float(lat_val or 0),
            "lon": float(lon_val or 0),
            "line": line,
            "brigade": str(brigade),
            "scheduled_departure": departure_iso,
            "load_date": TODAY,
        })
    return rows


# ── BigQuery & Firestore Writers ──────────────────────────────────────────────

def ensure_bq_table(client: bigquery.Client) -> None:
    dataset_ref = bigquery.DatasetReference(GCP_PROJECT_ID, BQ_DATASET)
    table_ref = dataset_ref.table(BQ_TABLE_TIMETABLE.split(".")[-1])
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
        chunk = rows[i: i + BATCH]
        errs = client.insert_rows_json(BQ_TABLE_TIMETABLE, chunk)
        if errs:
            errors.extend(errs)

    if errors:
        log.error("BQ insert errors (sample): %s", errors[:5])
        raise RuntimeError(f"{len(errors)} BQ insert errors")
    log.info("Wrote %d rows to %s", len(rows), BQ_TABLE_TIMETABLE)


# ── Orchestrator Main Loop ──────────────────────────────────────────────────

async def main():
    log.info("Starting Warsaw Tram Timetable scraper job...")

    # Configure connection limits explicitly using your shared config parameters
    connector = aiohttp.TCPConnector(limit=TIMETABLE_ASYNC_CONCURRENCY)

    async with aiohttp.ClientSession(connector=connector) as session:
        # Step 1: Fetch all public transit stops
        log.info("Fetching master list of transit stops...")
        all_stops = await get_all_stops(session)
        if not all_stops:
            log.error("Zero stops found. Exiting.")
            return

        log.info("Found %d total stops. Scanning for active tram lines...", len(all_stops))

        # Step 2: Extract which lines run on which stops (chunked to stay under limits)
        stops_with_trams = []
        for i in range(0, len(all_stops), BATCH_CHUNK_SIZE):
            chunk = all_stops[i: i + BATCH_CHUNK_SIZE]

            # Fire an intentional wave of concurrent connections up to BATCH_CHUNK_SIZE
            tasks = [get_lines_for_stop(session, stop) for stop in chunk]
            results = await asyncio.gather(*tasks)

            # Keep only elements where a tram line is actively running
            for stop_data, lines in results:
                if lines:
                    stops_with_trams.append((stop_data, lines))

            # Prevent API abuse between waves
            await asyncio.sleep(BATCH_BREATHER_S)

        log.info("Identified %d stops with active tram schedules.", len(stops_with_trams))

        # Step 3: Fetch departure timetable info for each validated tram line
        all_timetable_rows = []

        # Unroll stops and lines into individual queryable units
        unrolled_queries = []
        for stop, lines in stops_with_trams:
            for line in lines:
                unrolled_queries.append((stop, line))

        log.info("Processing %d distinct stop-line combinations...", len(unrolled_queries))

        for i in range(0, len(unrolled_queries), BATCH_CHUNK_SIZE):
            chunk = unrolled_queries[i: i + BATCH_CHUNK_SIZE]

            tasks = [get_brigades_for_stop_line(session, stop, line) for stop, line in chunk]
            results = await asyncio.gather(*tasks)

            for rows in results:
                if rows:
                    all_timetable_rows.extend(rows)

            await asyncio.sleep(BATCH_BREATHER_S)

        # Step 4: Write off-loaded data to BigQuery
        if all_timetable_rows:
            log.info("Persisting %d data points into BigQuery...", len(all_timetable_rows))
            write_to_bigquery(all_timetable_rows)
        else:
            log.warning("Pipeline completed but found no active timetable rows to write.")

    log.info("Cloud Run Job executed successfully.")


if __name__ == "__main__":
    asyncio.run(main())