"""
rag.py — Retrieval layer for the health dashboard chat

Provides Claude with SQL-backed tools to query historical health data.
All queries are read-only and fully parameterized — no raw SQL from user input.

Tools exposed to Claude:
  - get_period_stats    → aggregate stats (avg/min/max/stddev) for any metric
  - get_daily_records   → raw daily rows for trend/timeline questions
  - get_top_days        → best or worst N days for any metric
  - get_workout_history → workout-specific rollups
"""

import logging
import os
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import psycopg2
import psycopg2.extras

import context_notes

log = logging.getLogger(__name__)

DB_DSN = os.getenv("DATABASE_URL", "postgresql://user:password@localhost/health_db")

# ---------------------------------------------------------------------------
# Allowed metrics — whitelist prevents any injection via metric names
# ---------------------------------------------------------------------------

NUMERIC_METRICS = {
    "sleep_score", "deep_sleep_score", "rem_sleep_score", "restfulness_score",
    "readiness_score", "hrv_balance_score", "recovery_index_score", "temperature_deviation",
    "activity_score", "preferred_steps", "active_calories", "oura_steps",
    "run_distance_miles", "run_duration_minutes", "run_pace_min_per_mile",
    "oura_workout_count", "oura_workout_minutes", "oura_workout_calories",
    "oura_run_count", "oura_run_minutes",
    "high_activity_time_s", "medium_activity_time_s", "sedentary_time_s",
    "mood_score",
    "workout_count", "total_exercises", "total_workout_minutes",
    "temp_max_f", "temp_min_f",
    "precip_sum_mm", "precip_prob_max",
    "morning_precip_prob", "afternoon_precip_prob", "evening_precip_prob",
    "morning_temp_f", "afternoon_temp_f", "evening_temp_f",
}

METRIC_LABELS = {
    "sleep_score": "Sleep Score",
    "deep_sleep_score": "Deep Sleep Score",
    "rem_sleep_score": "REM Sleep Score",
    "restfulness_score": "Restfulness Score",
    "readiness_score": "Readiness Score",
    "hrv_balance_score": "HRV Balance Score",
    "recovery_index_score": "Recovery Index Score",
    "temperature_deviation": "Temp Deviation (°C)",
    "activity_score": "Activity Score",
    "preferred_steps": "Steps",
    "active_calories": "Active Calories",
    "run_distance_miles": "Run Distance (miles)",
    "run_duration_minutes": "Run Duration (min)",
    "run_pace_min_per_mile": "Run Pace (min/mile)",
    "oura_workout_count": "Oura Workouts",
    "oura_workout_minutes": "Oura Workout Minutes",
    "oura_workout_calories": "Oura Workout Calories",
    "oura_run_count": "Oura Runs",
    "oura_run_minutes": "Oura Run Minutes",
    "mood_score": "Mood Score",
    "workout_count": "Workouts",
    "total_exercises": "Total Exercises",
    "total_workout_minutes": "Workout Minutes",
    "temp_max_f": "Max Temp (°F)",
    "temp_min_f": "Min Temp (°F)",
    "morning_temp_f": "Morning Temp (°F)",
    "afternoon_temp_f": "Afternoon Temp (°F)",
    "evening_temp_f": "Evening Temp (°F)",
}


def _clean(v):
    """Convert Decimal and date to JSON-safe types."""
    if isinstance(v, Decimal):
        return float(v)
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return v


def _clean_row(row: dict) -> dict:
    return {k: _clean(v) for k, v in row.items()}


def _connect():
    return psycopg2.connect(os.getenv("DATABASE_URL", DB_DSN))


def _resolve_dates(start_date: str | None, end_date: str | None) -> tuple[str, str]:
    """Fill in sensible defaults and clamp to available data range."""
    today = date.today()
    end   = date.fromisoformat(end_date)   if end_date   else today
    start = date.fromisoformat(start_date) if start_date else end - timedelta(days=29)
    # Never exceed today
    end = min(end, today)
    # Cap at 2 years of history
    earliest = today - timedelta(days=730)
    start = max(start, earliest)
    return start.isoformat(), end.isoformat()


# ---------------------------------------------------------------------------
# Tool 1: get_period_stats
# ---------------------------------------------------------------------------

def get_period_stats(metric: str, start_date: str | None = None, end_date: str | None = None) -> dict:
    """
    Return aggregate statistics for one health metric over a date range.
    Includes: count, avg, min, max, stddev, and a 7-day rolling avg for the most recent week.
    """
    if metric not in NUMERIC_METRICS:
        return {"error": f"Unknown metric '{metric}'. Valid metrics: {sorted(NUMERIC_METRICS)}"}

    start, end = _resolve_dates(start_date, end_date)

    sql = f"""
        SELECT
            count({metric})                   AS count_with_data,
            round(avg({metric})::numeric, 1)  AS avg,
            min({metric})                     AS min,
            max({metric})                     AS max,
            round(stddev({metric})::numeric, 1) AS stddev,
            count(*)                          AS total_days
        FROM staging_marts.daily_summary
        WHERE date BETWEEN %(start)s AND %(end)s
          AND {metric} IS NOT NULL
    """

    # 7-day rolling avg for the most recent 7 days in range
    sql_7d = f"""
        SELECT round(avg({metric})::numeric, 1) AS avg_last_7
        FROM staging_marts.daily_summary
        WHERE date BETWEEN %(start7)s AND %(end)s
          AND {metric} IS NOT NULL
    """

    try:
        conn = _connect()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, {"start": start, "end": end})
            stats = _clean_row(dict(cur.fetchone()))

            end_dt = date.fromisoformat(end)
            start_7 = (end_dt - timedelta(days=6)).isoformat()
            cur.execute(sql_7d, {"start7": start_7, "end": end})
            row7 = cur.fetchone()
            stats["avg_last_7_days"] = _clean(row7["avg_last_7"]) if row7 else None

        conn.close()
        label = METRIC_LABELS.get(metric, metric)
        return {
            "metric": metric,
            "label": label,
            "period": f"{start} to {end}",
            "stats": stats,
        }
    except Exception as e:
        log.error("get_period_stats error: %s", e)
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# Tool 2: get_daily_records
# ---------------------------------------------------------------------------

ALLOWED_DAILY_COLUMNS = NUMERIC_METRICS | {
    "date", "mood", "daylio_activities", "workout_names",
    "weather_desc", "has_oura_data", "has_workout", "has_mood_log", "has_weather",
    "run_notes", "has_run",
}


def get_daily_records(
    metrics: list[str],
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict:
    """
    Return day-by-day records for a list of metrics over a date range.
    Good for trend questions, timeline views, and correlation analysis.
    Always includes 'date'. Max 90 days per call.
    """
    invalid = [m for m in metrics if m not in ALLOWED_DAILY_COLUMNS]
    if invalid:
        return {"error": f"Unknown columns: {invalid}. Valid: {sorted(ALLOWED_DAILY_COLUMNS)}"}

    start, end = _resolve_dates(start_date, end_date)

    # Enforce a max window to keep responses manageable
    start_dt = date.fromisoformat(start)
    end_dt   = date.fromisoformat(end)
    if (end_dt - start_dt).days > 90:
        start_dt = end_dt - timedelta(days=89)
        start = start_dt.isoformat()

    cols = ", ".join(["date"] + [m for m in metrics if m != "date"])

    sql = f"""
        SELECT {cols}
        FROM staging_marts.daily_summary
        WHERE date BETWEEN %(start)s AND %(end)s
        ORDER BY date ASC
    """

    try:
        conn = _connect()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, {"start": start, "end": end})
            rows = [_clean_row(dict(r)) for r in cur.fetchall()]
        conn.close()

        # Compute simple correlations if 2 numeric metrics requested
        correlations = {}
        numeric_cols = [m for m in metrics if m in NUMERIC_METRICS and m != "date"]
        if len(numeric_cols) == 2:
            a_vals = [r[numeric_cols[0]] for r in rows if r.get(numeric_cols[0]) is not None and r.get(numeric_cols[1]) is not None]
            b_vals = [r[numeric_cols[1]] for r in rows if r.get(numeric_cols[0]) is not None and r.get(numeric_cols[1]) is not None]
            if len(a_vals) >= 5:
                correlations[f"{numeric_cols[0]}_vs_{numeric_cols[1]}"] = _pearson(a_vals, b_vals)

        return {
            "metrics": metrics,
            "period": f"{start} to {end}",
            "row_count": len(rows),
            "records": rows,
            **({"correlations": correlations} if correlations else {}),
        }
    except Exception as e:
        log.error("get_daily_records error: %s", e)
        return {"error": str(e)}


def _pearson(x: list, y: list) -> float:
    n = len(x)
    if n < 2:
        return 0.0
    mx, my = sum(x) / n, sum(y) / n
    num = sum((a - mx) * (b - my) for a, b in zip(x, y))
    den = (sum((a - mx) ** 2 for a in x) * sum((b - my) ** 2 for b in y)) ** 0.5
    return round(num / den, 3) if den else 0.0


# ---------------------------------------------------------------------------
# Tool 3: get_top_days
# ---------------------------------------------------------------------------

def get_top_days(
    metric: str,
    start_date: str | None = None,
    end_date: str | None = None,
    order: str = "desc",
    limit: int = 10,
) -> dict:
    """
    Return the best (order='desc') or worst (order='asc') days for a metric.
    Great for answering "when did I sleep best?", "what was my worst HRV week?", etc.
    """
    if metric not in NUMERIC_METRICS:
        return {"error": f"Unknown metric '{metric}'."}
    if order not in ("asc", "desc"):
        return {"error": "order must be 'asc' or 'desc'"}
    limit = max(1, min(limit, 30))

    start, end = _resolve_dates(start_date, end_date)

    sql = f"""
        SELECT
            date,
            {metric},
            sleep_score,
            readiness_score,
            mood,
            mood_score,
            has_workout,
            workout_names
        FROM staging_marts.daily_summary
        WHERE date BETWEEN %(start)s AND %(end)s
          AND {metric} IS NOT NULL
        ORDER BY {metric} {order.upper()}
        LIMIT %(limit)s
    """

    try:
        conn = _connect()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, {"start": start, "end": end, "limit": limit})
            rows = [_clean_row(dict(r)) for r in cur.fetchall()]
        conn.close()
        return {
            "metric": metric,
            "label": METRIC_LABELS.get(metric, metric),
            "period": f"{start} to {end}",
            "order": "best" if order == "desc" else "worst",
            "results": rows,
        }
    except Exception as e:
        log.error("get_top_days error: %s", e)
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# Tool 4: get_workout_history
# ---------------------------------------------------------------------------

def get_workout_history(start_date: str | None = None, end_date: str | None = None) -> dict:
    """
    Return a workout-focused rollup: total sessions, total minutes, exercises,
    workout type breakdown, and per-week counts.
    """
    start, end = _resolve_dates(start_date, end_date)

    sql_summary = """
        SELECT
            count(*)                                  AS workout_days,
            sum(workout_count)                        AS total_sessions,
            round(sum(total_workout_minutes)::numeric, 0)  AS total_minutes,
            round(avg(total_workout_minutes)::numeric, 1)  AS avg_session_minutes,
            sum(total_exercises)                      AS total_exercises
        FROM staging_marts.daily_summary
        WHERE date BETWEEN %(start)s AND %(end)s
          AND has_workout = true
    """

    sql_weekly = """
        SELECT
            date_trunc('week', date)::date            AS week_start,
            count(*)                                  AS workout_days,
            sum(workout_count)                        AS sessions,
            round(sum(total_workout_minutes)::numeric, 0) AS minutes
        FROM staging_marts.daily_summary
        WHERE date BETWEEN %(start)s AND %(end)s
          AND has_workout = true
        GROUP BY 1
        ORDER BY 1 ASC
    """

    sql_names = """
        SELECT workout_names, count(*) AS occurrences
        FROM staging_marts.daily_summary
        WHERE date BETWEEN %(start)s AND %(end)s
          AND workout_names IS NOT NULL
        GROUP BY workout_names
        ORDER BY occurrences DESC
        LIMIT 15
    """

    sql_rest_vs_workout = """
        SELECT
            has_workout,
            count(*)                                  AS days,
            round(avg(sleep_score)::numeric, 1)       AS avg_sleep,
            round(avg(readiness_score)::numeric, 1)   AS avg_readiness,
            round(avg(hrv_balance_score)::numeric, 1) AS avg_hrv,
            round(avg(mood_score)::numeric, 1)        AS avg_mood
        FROM staging_marts.daily_summary
        WHERE date BETWEEN %(start)s AND %(end)s
          AND has_oura_data = true
        GROUP BY has_workout
        ORDER BY has_workout DESC
    """

    sql_runs = """
        SELECT
            id,
            date,
            distance_miles,
            round(duration_seconds / 60.0, 2)                              AS duration_minutes,
            round((duration_seconds / 60.0) / nullif(distance_miles, 0), 2) AS pace_min_per_mile,
            notes
        FROM raw.manual_runs
        WHERE date BETWEEN %(start)s AND %(end)s
        ORDER BY date ASC, logged_at ASC
    """

    try:
        conn = _connect()
        params = {"start": start, "end": end}
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql_summary, params)
            summary = _clean_row(dict(cur.fetchone()))

            cur.execute(sql_weekly, params)
            weekly = [_clean_row(dict(r)) for r in cur.fetchall()]

            cur.execute(sql_names, params)
            workout_types = [_clean_row(dict(r)) for r in cur.fetchall()]

            cur.execute(sql_rest_vs_workout, params)
            rest_vs_workout = [_clean_row(dict(r)) for r in cur.fetchall()]

            cur.execute(sql_runs, params)
            runs = [_clean_row(dict(r)) for r in cur.fetchall()]

        conn.close()

        total_days = (date.fromisoformat(end) - date.fromisoformat(start)).days + 1
        summary["total_days_in_period"] = total_days
        summary["rest_days"] = total_days - (summary.get("workout_days") or 0)

        run_distances = [r["distance_miles"] for r in runs if r.get("distance_miles")]
        run_paces = [r["pace_min_per_mile"] for r in runs if r.get("pace_min_per_mile")]
        run_summary = {
            "total_runs": len(runs),
            "total_miles": round(sum(run_distances), 2) if run_distances else 0,
            "avg_pace_min_per_mile": round(sum(run_paces) / len(run_paces), 2) if run_paces else None,
        }

        return {
            "period": f"{start} to {end}",
            "summary": summary,
            "weekly_breakdown": weekly,
            "workout_types": workout_types,
            "rest_vs_workout_day_comparison": rest_vs_workout,
            "run_summary": run_summary,
            "runs": runs,
        }
    except Exception as e:
        log.error("get_workout_history error: %s", e)
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# Tool 5: log_run
# ---------------------------------------------------------------------------

def log_run(
    distance_miles: float,
    duration_seconds: int,
    date: str | None = None,
    notes: str | None = None,
) -> dict:
    """Save a manually logged run to raw.manual_runs."""
    run_date = date or _today_str()
    pace = round((duration_seconds / 60.0) / distance_miles, 2) if distance_miles else None

    try:
        conn = _connect()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO raw.manual_runs (date, distance_miles, duration_seconds, notes)
                VALUES (%s, %s, %s, %s)
                """,
                (run_date, distance_miles, duration_seconds, notes),
            )
        conn.commit()
        conn.close()
        minutes, secs = divmod(duration_seconds, 60)
        return {
            "saved": True,
            "date": run_date,
            "distance_miles": distance_miles,
            "duration": f"{minutes}:{secs:02d}",
            "pace_min_per_mile": pace,
        }
    except Exception as e:
        log.error("log_run error: %s", e)
        return {"error": str(e)}


def _today_str() -> str:
    return date.today().isoformat()


# ---------------------------------------------------------------------------
# Tool registry — used by chat_server to dispatch tool calls
# ---------------------------------------------------------------------------

TOOL_DEFINITIONS = [
    {
        "name": "get_period_stats",
        "description": (
            "Get aggregate statistics (count, average, min, max, std deviation) for a single "
            "health metric over a date range. Use for questions like 'what was my average sleep "
            "score last month?' or 'how did my HRV look in February?'"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "metric": {
                    "type": "string",
                    "description": (
                        "The metric to aggregate. One of: sleep_score, readiness_score, "
                        "hrv_balance_score, deep_sleep_score, rem_sleep_score, restfulness_score, "
                        "recovery_index_score, temperature_deviation, activity_score, "
                        "preferred_steps, active_calories, mood_score, workout_count, "
                        "total_workout_minutes, temp_max_f, temp_min_f."
                    ),
                },
                "start_date": {
                    "type": "string",
                    "description": "Start date in YYYY-MM-DD format. Defaults to 30 days ago.",
                },
                "end_date": {
                    "type": "string",
                    "description": "End date in YYYY-MM-DD format. Defaults to today.",
                },
            },
            "required": ["metric"],
        },
    },
    {
        "name": "get_daily_records",
        "description": (
            "Fetch day-by-day records for one or more health metrics over a date range (max 90 days). "
            "Use for trend questions, timeline analysis, or correlation questions like 'does my mood "
            "follow my sleep?' Returns raw daily rows plus a Pearson correlation if exactly 2 numeric "
            "metrics are requested."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "metrics": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        "List of column names to fetch. Any combination of: sleep_score, "
                        "readiness_score, hrv_balance_score, mood_score, preferred_steps, "
                        "active_calories, workout_count, total_workout_minutes, mood, "
                        "workout_names, weather_desc, temp_max_f, and other daily_summary columns."
                    ),
                },
                "start_date": {
                    "type": "string",
                    "description": "Start date YYYY-MM-DD. Defaults to 30 days ago.",
                },
                "end_date": {
                    "type": "string",
                    "description": "End date YYYY-MM-DD. Defaults to today.",
                },
            },
            "required": ["metrics"],
        },
    },
    {
        "name": "get_top_days",
        "description": (
            "Find the best or worst days for a health metric in a given period. "
            "Use for questions like 'when did I sleep best this year?', 'what were my "
            "worst readiness days?', or 'show me my top step days last month'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "metric": {
                    "type": "string",
                    "description": "The metric to rank days by (e.g. sleep_score, hrv_balance_score, mood_score).",
                },
                "start_date": {"type": "string", "description": "Start date YYYY-MM-DD."},
                "end_date":   {"type": "string", "description": "End date YYYY-MM-DD."},
                "order": {
                    "type": "string",
                    "enum": ["desc", "asc"],
                    "description": "'desc' for best days (highest values), 'asc' for worst days.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of days to return (1–30). Default 10.",
                },
            },
            "required": ["metric"],
        },
    },
    {
        "name": "save_context_note",
        "description": (
            "Save a short-term health context note that will automatically expire after a given number of days. "
            "Use this when the user mentions a temporary condition that will affect their health data — "
            "such as jetlag, illness, travel, stress, medication, or injury. "
            "Estimate a realistic recovery duration based on the condition. "
            "The note will be injected into all future AI health analysis until it expires."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "note": {
                    "type": "string",
                    "description": (
                        "A concise description of the temporary condition, written in third person. "
                        "E.g. 'Jetlagged after Tokyo trip, circadian rhythm off' or "
                        "'Recovering from a cold, energy and HRV likely suppressed'."
                    ),
                },
                "expires_days": {
                    "type": "integer",
                    "description": (
                        "Number of days until this note expires. Base on typical recovery: "
                        "jetlag ~5-7 days, mild cold ~7 days, flu ~10-14 days, "
                        "travel fatigue ~3 days, minor injury ~14 days."
                    ),
                },
            },
            "required": ["note", "expires_days"],
        },
    },
    {
        "name": "get_workout_history",
        "description": (
            "Get a comprehensive activity analysis for a date range. Covers strength workouts (sessions, "
            "minutes, exercises, weekly breakdown) AND runs (every run with date, distance in miles, "
            "duration, pace in min/mile, plus summary totals). Also compares health metrics on workout "
            "days vs rest days. Use for ANY question about runs, pace, mileage, training load, "
            "consistency, or recovery patterns."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "start_date": {"type": "string", "description": "Start date YYYY-MM-DD."},
                "end_date":   {"type": "string", "description": "End date YYYY-MM-DD."},
            },
            "required": [],
        },
    },
]


def execute_tool(name: str, tool_input: dict) -> Any:
    """Dispatch a tool call by name and return the result."""
    if name == "get_period_stats":
        return get_period_stats(**tool_input)
    if name == "get_daily_records":
        return get_daily_records(**tool_input)
    if name == "get_top_days":
        return get_top_days(**tool_input)
    if name == "get_workout_history":
        return get_workout_history(**tool_input)
    if name == "save_context_note":
        return context_notes.save_note(**tool_input)
    return {"error": f"Unknown tool: {name}"}
