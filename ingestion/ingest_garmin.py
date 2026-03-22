"""
Garmin Connect ingestion script — pulls activities and daily steps into Postgres raw zone.

Usage:
    python3 ingest_garmin.py                     # last 7 days
    python3 ingest_garmin.py --days 30           # last N days
    python3 ingest_garmin.py --start 2022-06-01  # from date to today
"""

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from garminconnect import (
    Garmin,
    GarminConnectAuthenticationError,
    GarminConnectConnectionError,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

GARMIN_EMAIL    = os.getenv("GARMIN_EMAIL")
GARMIN_PASSWORD = os.getenv("GARMIN_PASSWORD")
DB_DSN          = os.getenv("DATABASE_URL", "postgresql://user:password@localhost/health_db")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(os.path.dirname(__file__), "ingest_garmin.log")),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Garmin client
# ---------------------------------------------------------------------------

def garmin_login() -> Garmin:
    if not GARMIN_EMAIL or not GARMIN_PASSWORD:
        log.error("GARMIN_EMAIL or GARMIN_PASSWORD not set — check .env")
        sys.exit(1)
    try:
        client = Garmin(GARMIN_EMAIL, GARMIN_PASSWORD)
        client.login()
        log.info("Garmin login successful")
        return client
    except (GarminConnectConnectionError, GarminConnectAuthenticationError) as e:
        log.error("Garmin login failed: %s", e)
        sys.exit(1)

# ---------------------------------------------------------------------------
# Activities
# ---------------------------------------------------------------------------

def ingest_activities(client: Garmin, conn, start_date: str, end_date: str):
    log.info("Fetching activities %s → %s", start_date, end_date)
    activities = client.get_activities_by_date(start_date, end_date)

    if not activities:
        log.info("  No activities returned")
        return

    inserted = 0
    with conn.cursor() as cur:
        for activity in activities:
            activity_id = str(activity.get("activityId", ""))
            if not activity_id:
                continue
            cur.execute(
                """
                INSERT INTO raw.garmin_activity (id, raw_data, pulled_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (id) DO NOTHING
                """,
                (activity_id, json.dumps(activity)),
            )
            if cur.rowcount > 0:
                inserted += 1

    conn.commit()
    log.info("  Inserted %d new activities", inserted)

# ---------------------------------------------------------------------------
# Daily steps
# ---------------------------------------------------------------------------

def ingest_steps(client: Garmin, conn, start_date: str, end_date: str):
    log.info("Fetching daily steps %s → %s", start_date, end_date)

    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end   = datetime.strptime(end_date,   "%Y-%m-%d").date()
    current = start
    inserted = 0

    with conn.cursor() as cur:
        while current <= end:
            date_str = current.isoformat()
            try:
                day_data = client.get_steps_data(date_str)
                if isinstance(day_data, list) and day_data:
                    steps = sum(entry.get("steps", 0) for entry in day_data)
                    cur.execute(
                        """
                        INSERT INTO raw.garmin_daily_steps (date, steps, pulled_at)
                        VALUES (%s, %s, NOW())
                        ON CONFLICT (date) DO UPDATE SET steps = EXCLUDED.steps, pulled_at = NOW()
                        """,
                        (date_str, steps),
                    )
                    if cur.rowcount > 0:
                        inserted += 1
            except Exception as e:
                log.warning("  Steps fetch failed for %s: %s", date_str, e)

            current += timedelta(days=1)

    conn.commit()
    log.info("  Upserted %d step records", inserted)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Ingest Garmin data into Postgres raw zone")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--days",  type=int, default=7,   help="Number of past days to sync (default: 7)")
    group.add_argument("--start", type=str,              help="Start date YYYY-MM-DD (syncs to today)")
    parser.add_argument("--end",  type=str, default=date.today().isoformat(), help="End date (default: today)")
    return parser.parse_args()


def main():
    args = parse_args()

    if args.start:
        start_date = args.start
    else:
        start_date = (date.today() - timedelta(days=args.days)).isoformat()

    end_date = args.end
    log.info("Garmin ingestion: %s → %s", start_date, end_date)

    client = garmin_login()
    conn   = psycopg2.connect(DB_DSN)
    log.info("Connected to Postgres")

    ingest_activities(client, conn, start_date, end_date)
    ingest_steps(client, conn, start_date, end_date)

    conn.close()
    log.info("Done")


if __name__ == "__main__":
    main()
