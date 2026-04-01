"""
Oura Ring ingestion script — pulls from Oura API v2 and writes to raw zone in Postgres.

Raw zone design: one JSONB row per record, append-only, deduped by id.
Each table mirrors one Oura endpoint exactly as returned by the API.

Usage:
    python3 ingest_oura.py                          # yesterday only
    python3 ingest_oura.py --start 2024-04-01       # backfill from date to today
    python3 ingest_oura.py --start 2024-04-01 --end 2025-06-30
"""

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

OURA_TOKEN = os.getenv("OURA_ACCESS_TOKEN")
DB_DSN = os.getenv("DATABASE_URL", "postgresql://user:password@localhost/health_db")

OURA_BASE = "https://api.ouraring.com/v2/usercollection"

# Each endpoint maps to a raw table and the field that holds the record list
ENDPOINTS = {
    "sleep":             "raw.oura_sleep",
    "daily_sleep":       "raw.oura_daily_sleep",
    "daily_readiness":   "raw.oura_daily_readiness",
    "daily_stress":      "raw.oura_daily_stress",
    "daily_activity":    "raw.oura_daily_activity",
    "workout":           "raw.oura_workout",
}

# Heartrate has no 'id' field — uses timestamp as unique key
HEARTRATE_TABLE = "raw.oura_heartrate"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            os.path.join(os.path.dirname(__file__), "ingest_oura.log")
        ),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Oura API
# ---------------------------------------------------------------------------

def fetch_endpoint(endpoint: str, start_date: str, end_date: str) -> list[dict]:
    """Fetch all pages for a single Oura endpoint and date range."""
    headers = {"Authorization": f"Bearer {OURA_TOKEN}"}
    params = {"start_date": start_date, "end_date": end_date}
    records = []

    url = f"{OURA_BASE}/{endpoint}"
    while url:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code != 200:
            log.error("  %s returned %s: %s", endpoint, resp.status_code, resp.text[:200])
            return records

        body = resp.json()
        page_records = body.get("data", [])
        records.extend(page_records)

        # Pagination: next_token means there are more pages
        next_token = body.get("next_token")
        if next_token:
            url = f"{OURA_BASE}/{endpoint}"
            params = {"next_token": next_token}
        else:
            url = None

    return records

# ---------------------------------------------------------------------------
# Postgres
# ---------------------------------------------------------------------------

def upsert_records(conn, table: str, records: list[dict]) -> int:
    """Insert records into raw table, skipping any that already exist (by id)."""
    if not records:
        return 0

    inserted = 0
    with conn.cursor() as cur:
        for record in records:
            record_id = record.get("id")
            if not record_id:
                log.warning("  Record missing 'id', skipping: %s", str(record)[:100])
                continue

            cur.execute(
                f"""
                INSERT INTO {table} (id, raw_data, pulled_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (id) DO NOTHING
                """,
                (record_id, json.dumps(record)),
            )
            if cur.rowcount > 0:
                inserted += 1

    conn.commit()
    return inserted

# ---------------------------------------------------------------------------
# Heart rate (timestamp-keyed, no 'id' field)
# ---------------------------------------------------------------------------

def upsert_heartrate(conn, records: list[dict]) -> int:
    if not records:
        return 0
    inserted = 0
    with conn.cursor() as cur:
        for record in records:
            ts = record.get("timestamp")
            if not ts:
                continue
            cur.execute(
                f"""
                INSERT INTO {HEARTRATE_TABLE} (timestamp, raw_data, pulled_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (timestamp) DO NOTHING
                """,
                (ts, json.dumps(record)),
            )
            if cur.rowcount > 0:
                inserted += 1
    conn.commit()
    return inserted

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    parser = argparse.ArgumentParser(description="Ingest Oura data into Postgres raw zone")
    parser.add_argument("--start", default=yesterday, help="Start date YYYY-MM-DD (default: yesterday)")
    parser.add_argument("--end",   default=date.today().isoformat(), help="End date YYYY-MM-DD (default: today)")
    return parser.parse_args()


def main():
    args = parse_args()
    log.info("Oura ingestion: %s → %s", args.start, args.end)

    if not OURA_TOKEN:
        log.error("OURA_ACCESS_TOKEN not set — check .env file")
        sys.exit(1)

    conn = psycopg2.connect(DB_DSN)
    log.info("Connected to Postgres")

    total_inserted = 0
    for endpoint, table in ENDPOINTS.items():
        log.info("Fetching %s ...", endpoint)
        records = fetch_endpoint(endpoint, args.start, args.end)
        log.info("  Got %d records from API", len(records))

        n = upsert_records(conn, table, records)
        log.info("  Inserted %d new rows into %s", n, table)
        total_inserted += n

    log.info("Fetching heartrate ...")
    hr_records = fetch_endpoint("heartrate", args.start, args.end)
    log.info("  Got %d heartrate records from API", len(hr_records))
    n = upsert_heartrate(conn, hr_records)
    log.info("  Inserted %d new rows into %s", n, HEARTRATE_TABLE)
    total_inserted += n

    conn.close()
    log.info("Done. Total new rows inserted: %d", total_inserted)


if __name__ == "__main__":
    main()
