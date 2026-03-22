"""
Weather ingestion script — pulls daily + hourly forecast/historical data from
Open-Meteo and stores it in raw.weather_daily.

Location is resolved automatically via IP geolocation (home Pi location).
Hourly breakdowns (AM vs PM rain/temp) are stored so the AI insights layer
can make time-of-day recommendations.

Usage:
    python3 ingest_weather.py                        # today + 7-day forecast
    python3 ingest_weather.py --days 7               # last 7 days (historical)
    python3 ingest_weather.py --start 2024-04-01     # backfill from date to today
"""

import argparse
import json
import logging
import os
import sys
from datetime import date, timedelta
from urllib.request import urlopen
from urllib.parse import urlencode
from urllib.error import URLError

import psycopg2
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

DB_DSN = os.getenv("DATABASE_URL", "postgresql://jhumms:health2026@localhost/health_db")

# Open-Meteo endpoints (no API key required)
FORECAST_URL  = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_URL   = "https://archive-api.open-meteo.com/v1/archive"
IPGEO_URL     = "https://ipapi.co/json/"

# WMO weather code descriptions (subset — covers all codes Open-Meteo returns)
WMO_CODES = {
    0: "Clear sky",
    1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Icy fog",
    51: "Light drizzle", 53: "Drizzle", 55: "Heavy drizzle",
    61: "Light rain", 63: "Rain", 65: "Heavy rain",
    71: "Light snow", 73: "Snow", 75: "Heavy snow",
    77: "Snow grains",
    80: "Light showers", 81: "Showers", 82: "Heavy showers",
    85: "Snow showers", 86: "Heavy snow showers",
    95: "Thunderstorm",
    96: "Thunderstorm with hail", 99: "Thunderstorm with heavy hail",
}

HOURLY_VARS = "temperature_2m,precipitation_probability,precipitation,weathercode,windspeed_10m"
DAILY_VARS  = "temperature_2m_max,temperature_2m_min,precipitation_sum,precipitation_probability_max,weathercode,sunrise,sunset"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(os.path.dirname(__file__), "ingest_weather.log")),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fetch_json(url: str) -> dict:
    with urlopen(url, timeout=15) as resp:
        return json.loads(resp.read().decode())


def get_location() -> dict:
    """Resolve lat/lon from Pi's public IP via ipapi.co."""
    try:
        data = fetch_json(IPGEO_URL)
        lat  = data.get("latitude")
        lon  = data.get("longitude")
        city = data.get("city", "unknown")
        if lat and lon:
            log.info("Location resolved: %s (%.4f, %.4f)", city, lat, lon)
            return {"latitude": lat, "longitude": lon, "city": city, "source": "ip_geo"}
    except (URLError, Exception) as e:
        log.warning("IP geolocation failed: %s", e)

    # Hard fallback — update this if ip_geo consistently fails
    lat, lon = float(os.getenv("HOME_LAT", "0")), float(os.getenv("HOME_LON", "0"))
    log.warning("Using fallback coords from env: %.4f, %.4f", lat, lon)
    return {"latitude": lat, "longitude": lon, "city": "home", "source": "env_fallback"}


def build_url(base: str, lat: float, lon: float, start: str, end: str) -> str:
    params = {
        "latitude":    lat,
        "longitude":   lon,
        "hourly":      HOURLY_VARS,
        "daily":       DAILY_VARS,
        "timezone":    "auto",
        "start_date":  start,
        "end_date":    end,
    }
    return f"{base}?{urlencode(params)}"


def parse_day(date_str: str, api_resp: dict, location: dict) -> dict:
    """Extract one day's data from the API response into a flat record."""
    hourly = api_resp.get("hourly", {})
    daily  = api_resp.get("daily",  {})

    # Find index of this date in the daily arrays
    daily_times = daily.get("time", [])
    if date_str not in daily_times:
        return None
    di = daily_times.index(date_str)

    # Hourly data — filter to hours belonging to this date
    hourly_times = hourly.get("time", [])
    hour_indices = [i for i, t in enumerate(hourly_times) if t.startswith(date_str)]

    def hourly_slice(key):
        vals = hourly.get(key, [])
        return [vals[i] for i in hour_indices if i < len(vals)]

    def hours_avg(key, start_h, end_h):
        """Average of hourly values between start_h and end_h (inclusive)."""
        vals = hourly.get(key, [])
        times = hourly_times
        subset = [
            vals[i] for i in hour_indices
            if i < len(vals) and vals[i] is not None
            and start_h <= int(times[i][11:13]) <= end_h
        ]
        return round(sum(subset) / len(subset), 1) if subset else None

    weathercode = daily.get("weathercode", [None])[di] if di < len(daily.get("weathercode", [])) else None

    record = {
        "date":            date_str,
        "latitude":        location["latitude"],
        "longitude":       location["longitude"],
        "city":            location["city"],
        "location_source": location["source"],

        # Daily summary
        "temp_max_c":      daily.get("temperature_2m_max", [None])[di],
        "temp_min_c":      daily.get("temperature_2m_min", [None])[di],
        "precip_sum_mm":   daily.get("precipitation_sum", [None])[di],
        "precip_prob_max": daily.get("precipitation_probability_max", [None])[di],
        "weathercode":     weathercode,
        "weather_desc":    WMO_CODES.get(weathercode, "Unknown"),
        "sunrise":         daily.get("sunrise", [None])[di],
        "sunset":          daily.get("sunset", [None])[di],

        # Time-of-day breakdowns (for AI recommendations)
        "morning_temp_c":      hours_avg("temperature_2m", 6, 9),
        "afternoon_temp_c":    hours_avg("temperature_2m", 12, 15),
        "evening_temp_c":      hours_avg("temperature_2m", 17, 20),
        "morning_precip_prob": hours_avg("precipitation_probability", 6, 10),
        "afternoon_precip_prob": hours_avg("precipitation_probability", 11, 15),
        "evening_precip_prob": hours_avg("precipitation_probability", 16, 22),

        # Full hourly arrays (stored for completeness)
        "hourly": {
            "time":                   [hourly_times[i] for i in hour_indices],
            "temperature_2m":         hourly_slice("temperature_2m"),
            "precipitation_probability": hourly_slice("precipitation_probability"),
            "precipitation":          hourly_slice("precipitation"),
            "weathercode":            hourly_slice("weathercode"),
            "windspeed_10m":          hourly_slice("windspeed_10m"),
        },
    }
    return record


def upsert_day(conn, record: dict):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw.weather_daily (date, raw_data, pulled_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (date) DO UPDATE
                SET raw_data  = EXCLUDED.raw_data,
                    pulled_at = NOW()
            """,
            (record["date"], json.dumps(record)),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Main fetch logic
# ---------------------------------------------------------------------------

def ingest_range(conn, location: dict, start_date: str, end_date: str):
    today     = date.today().isoformat()
    yesterday = (date.today() - timedelta(days=1)).isoformat()

    # If range spans the archive/forecast boundary, split and recurse once each
    if start_date < today and end_date >= today:
        ingest_range(conn, location, start_date, yesterday)
        ingest_range(conn, location, today, end_date)
        return

    use_archive = end_date < today
    base_url = ARCHIVE_URL if use_archive else FORECAST_URL

    log.info("Fetching %s weather %s → %s", "historical" if use_archive else "forecast", start_date, end_date)
    url = build_url(base_url, location["latitude"], location["longitude"], start_date, end_date)

    try:
        api_resp = fetch_json(url)
    except Exception as e:
        log.error("Open-Meteo request failed: %s", e)
        return

    dates = api_resp.get("daily", {}).get("time", [])
    inserted = 0
    for d in dates:
        record = parse_day(d, api_resp, location)
        if record:
            upsert_day(conn, record)
            inserted += 1

    log.info("Upserted %d weather records", inserted)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Ingest weather data into raw.weather_daily")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--days",  type=int, help="Pull last N days of historical data")
    group.add_argument("--start", type=str, help="Start date YYYY-MM-DD for historical backfill")
    parser.add_argument("--end",  type=str, default=None, help="End date (default: today + 7 for forecast, today for historical)")
    parser.add_argument("--forecast-only", action="store_true", help="Pull only the 7-day forecast")
    return parser.parse_args()


def main():
    args = parse_args()
    today = date.today()

    if args.forecast_only or (not args.days and not args.start):
        start_date = today.isoformat()
        end_date   = (today + timedelta(days=7)).isoformat()
    elif args.start:
        start_date = args.start
        end_date   = args.end or (today + timedelta(days=7)).isoformat()
    else:
        start_date = (today - timedelta(days=args.days)).isoformat()
        end_date   = args.end or (today + timedelta(days=7)).isoformat()

    location = get_location()
    conn = psycopg2.connect(DB_DSN)
    log.info("Connected to Postgres")

    ingest_range(conn, location, start_date, end_date)

    conn.close()
    log.info("Done")


if __name__ == "__main__":
    main()
