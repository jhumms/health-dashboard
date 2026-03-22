"""
Daylio ingestion script — processes CSV exports dropped to the daylio_exports folder
and loads them into Postgres raw zone.

The cron already handles moving the CSV from Google Drive via rclone at 23:00.
This script runs after, finds any CSVs in the drop folder, validates schema,
inserts to Postgres, and removes the file.

Usage:
    python3 ingest_daylio.py
"""

import csv
import json
import logging
import os
import sys

import psycopg2
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

DB_DSN   = os.getenv("DATABASE_URL", "postgresql://user:password@localhost/health_db")
DROP_DIR = os.getenv("DAYLIO_DROP_DIR", "/home/jhumms/health_workflow/ingestion/drops/daylio")

EXPECTED_COLUMNS = {"full_date", "date", "weekday", "time", "mood", "activities", "note_title", "note"}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(os.path.dirname(__file__), "ingest_daylio.log")),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------

def process_csv(conn, csv_path: str) -> int:
    inserted = 0
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        actual_cols = set(reader.fieldnames or [])
        if not EXPECTED_COLUMNS.issubset(actual_cols):
            missing = EXPECTED_COLUMNS - actual_cols
            log.warning("  Skipping %s — missing columns: %s", os.path.basename(csv_path), missing)
            return 0

        with conn.cursor() as cur:
            for row in reader:
                full_date = row.get("full_date", "").strip()
                if not full_date:
                    continue
                cur.execute(
                    """
                    INSERT INTO raw.daylio_logs (full_date, raw_data, pulled_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (full_date) DO UPDATE
                        SET raw_data = EXCLUDED.raw_data,
                            pulled_at = NOW()
                    """,
                    (full_date, json.dumps(dict(row))),
                )
                if cur.rowcount > 0:
                    inserted += 1

    conn.commit()
    return inserted


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    csv_files = sorted(
        f for f in os.listdir(DROP_DIR) if f.endswith(".csv")
    )

    if not csv_files:
        log.info("No CSV files found in %s", DROP_DIR)
        return

    conn = psycopg2.connect(DB_DSN)
    log.info("Connected to Postgres")

    total = 0
    for filename in csv_files:
        csv_path = os.path.join(DROP_DIR, filename)
        log.info("Processing %s", filename)
        n = process_csv(conn, csv_path)
        log.info("  Upserted %d rows", n)
        total += n
        os.remove(csv_path)
        log.info("  Deleted %s", filename)

    conn.close()
    log.info("Done. Total rows upserted: %d", total)


if __name__ == "__main__":
    main()
