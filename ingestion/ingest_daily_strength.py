"""
Daily Strength ingestion script — syncs zip export from Google Drive and loads
WorkoutSession records into Postgres raw zone.

The zip export contains the full app database as JSON files. Each WorkoutSession
is stored as a single JSONB blob (it already contains nested exercises and sets).

Usage:
    python3 ingest_daily_strength.py              # sync from Drive, process latest zip
    python3 ingest_daily_strength.py --zip /path/to/file.zip  # load a specific zip
"""

import argparse
import glob
import json
import logging
import os
import sys
import zipfile
from datetime import datetime

import psycopg2
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

DB_DSN     = os.getenv("DATABASE_URL", "postgresql://user:password@localhost/health_db")
LOCAL_DIR  = os.getenv("DAILY_STRENGTH_DIR", "/home/jhumms/health_workflow/ingestion/drops/daily_strength")
GDRIVE_PATH = os.getenv("DAILY_STRENGTH_GDRIVE", "gdrive:Phone/DailyStrength")
EXTRACT_DIR = os.path.join(LOCAL_DIR, "extracted")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(os.path.dirname(__file__), "ingest_daily_strength.log")),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def sync_from_drive():
    log.info("Syncing from Google Drive: %s → %s", GDRIVE_PATH, LOCAL_DIR)
    ret = os.system(f"rclone copy '{GDRIVE_PATH}' '{LOCAL_DIR}' --drive-skip-gdocs")
    if ret != 0:
        log.warning("rclone exited with code %d", ret)


def find_latest_zip() -> str | None:
    zips = sorted(
        glob.glob(os.path.join(LOCAL_DIR, "*.zip")),
        key=os.path.getmtime,
        reverse=True,
    )
    return zips[0] if zips else None


def extract_zip(zip_path: str) -> str:
    os.makedirs(EXTRACT_DIR, exist_ok=True)
    for f in os.listdir(EXTRACT_DIR):
        fp = os.path.join(EXTRACT_DIR, f)
        if os.path.isfile(fp):
            os.remove(fp)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(EXTRACT_DIR)
    log.info("Extracted %s", zip_path)
    return EXTRACT_DIR


def load_json(path: str) -> list:
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------

def ingest_sessions(conn, extract_dir: str):
    sessions = load_json(os.path.join(extract_dir, "WorkoutSession.json"))
    log.info("Found %d workout sessions", len(sessions))

    inserted = 0
    with conn.cursor() as cur:
        for session in sessions:
            session_id = session.get("id")
            if not session_id:
                continue
            cur.execute(
                """
                INSERT INTO raw.daily_strength_session (id, raw_data, pulled_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (id) DO NOTHING
                """,
                (session_id, json.dumps(session)),
            )
            if cur.rowcount > 0:
                inserted += 1

    conn.commit()
    log.info("Inserted %d new sessions", inserted)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Ingest Daily Strength data into Postgres raw zone")
    parser.add_argument("--zip", type=str, help="Path to a specific zip file to load (skips Drive sync)")
    parser.add_argument("--no-sync", action="store_true", help="Skip Google Drive sync, use existing local zips")
    return parser.parse_args()


def main():
    args = parse_args()

    if args.zip:
        zip_path = args.zip
        log.info("Using specified zip: %s", zip_path)
    else:
        if not args.no_sync:
            sync_from_drive()
        zip_path = find_latest_zip()
        if not zip_path:
            log.error("No zip file found in %s", LOCAL_DIR)
            sys.exit(1)
        log.info("Using latest zip: %s", zip_path)

    extract_dir = extract_zip(zip_path)

    conn = psycopg2.connect(DB_DSN)
    log.info("Connected to Postgres")

    ingest_sessions(conn, extract_dir)

    conn.close()
    log.info("Done")


if __name__ == "__main__":
    main()
