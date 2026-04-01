"""
generate_dashboard.py — nightly dashboard generator

Queries staging_marts.daily_summary, calls Claude Haiku for daily insights,
and renders a static dashboard.html with D3.js charts + AI recommendations.

Usage:
    python3 generate_dashboard.py              # generates today's dashboard
    python3 generate_dashboard.py --dry-run   # skips Claude call, uses placeholder text
"""

import argparse
import json
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import anthropic
import psycopg2
import psycopg2.extras
from decimal import Decimal
from dotenv import load_dotenv
from jinja2 import Template

import context_notes
import llm_logging


class HealthJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if hasattr(obj, "isoformat"):
            return obj.isoformat()
        return super().default(obj)


def dumps(obj):
    return json.dumps(obj, cls=HealthJSONEncoder)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).parent
load_dotenv(SCRIPT_DIR.parent / "ingestion" / ".env")

DB_DSN = os.getenv("DATABASE_URL", "postgresql://user:password@localhost/health_db")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
OUTPUT_PATH = SCRIPT_DIR / "output" / "dashboard.html"
PERSONAL_CONTEXT_PATH = SCRIPT_DIR / "personal_context.json"
INSIGHTS_CACHE_DIR = SCRIPT_DIR / "output"

# Haiku for nightly routine insight generation (cost-efficient)
INSIGHTS_MODEL = "claude-haiku-4-5"
LOOKBACK_DAYS = 30

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(SCRIPT_DIR / "generate_dashboard.log"),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Database queries
# ---------------------------------------------------------------------------

TREND_QUERY = """
SELECT
    date,
    sleep_score,
    deep_sleep_score,
    rem_sleep_score,
    readiness_score,
    hrv_balance_score,
    recovery_index_score,
    temperature_deviation,
    resting_heart_rate,
    resting_heart_rate_score,
    average_hrv,
    activity_score,
    preferred_steps,
    active_calories,
    high_activity_time_s,
    mood,
    mood_score,
    daylio_activities,
    workout_count,
    total_exercises,
    total_workout_minutes,
    workout_names,
    oura_workout_count,
    oura_workout_minutes,
    oura_workout_types,
    run_distance_miles,
    run_duration_minutes,
    run_pace_min_per_mile,
    has_oura_data,
    has_mood_log,
    has_workout,
    has_run
FROM staging_marts.daily_summary
WHERE date >= %(start_date)s
ORDER BY date ASC
"""

TODAY_QUERY = """
SELECT
    date,
    sleep_score,
    deep_sleep_score,
    rem_sleep_score,
    restfulness_score,
    readiness_score,
    hrv_balance_score,
    recovery_index_score,
    temperature_deviation,
    resting_heart_rate,
    resting_heart_rate_score,
    average_hrv,
    activity_score,
    preferred_steps,
    active_calories,
    oura_steps,
    high_activity_time_s,
    medium_activity_time_s,
    sedentary_time_s,
    mood,
    mood_score,
    daylio_activities,
    note_title,
    workout_count,
    total_exercises,
    total_workout_minutes,
    workout_names,
    weather_city,
    temp_max_f,
    temp_min_f,
    temp_max_c,
    temp_min_c,
    precip_sum_mm,
    precip_prob_max,
    weather_desc,
    sunrise,
    sunset,
    morning_temp_c,
    afternoon_temp_c,
    evening_temp_c,
    morning_temp_f,
    afternoon_temp_f,
    evening_temp_f,
    morning_precip_prob,
    afternoon_precip_prob,
    evening_precip_prob,
    likely_rain,
    better_in_morning,
    hot_day,
    cold_day,
    has_oura_data,
    has_mood_log,
    has_workout,
    has_run,
    has_weather
FROM staging_marts.daily_summary
WHERE date = %(today)s
"""


def fetch_data(conn):
    today = date.today()
    start = today - timedelta(days=LOOKBACK_DAYS)

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(TODAY_QUERY, {"today": today.isoformat()})
        today_row = cur.fetchone()

        cur.execute(TREND_QUERY, {"start_date": start.isoformat()})
        trend_rows = cur.fetchall()

    return today_row, trend_rows


# ---------------------------------------------------------------------------
# Claude insights
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are a personal health coach for {name}. You have complete access to their health metrics, weather, and personal situation.

Personal context:
{personal_context}
{active_notes}
Your tone is always upbeat, encouraging, and positive — celebrate progress, frame challenges as opportunities, and keep the energy high. Be concise and direct. Acknowledge the reality of new parenthood — sleep may be fragmented and that's okay. Focus on what {name} can control today: sleep quality, steps, and running toward their 5k goal.

Step goal: 7,500 steps/day average. Running goal: 5k in under 24:30 (that's ~7:54/mile pace for 3.1 miles).

Always prioritize in this order: (1) sleep/recovery recap, (2) activity level recommendation, (3) running update and coaching, (4) steps progress, (5) weather for any outdoor activity."""


def build_insight_prompt(today: dict, trends: list, personal_context: dict, active_notes: list | None = None) -> str:
    """Build a structured prompt for today's health insights."""
    if not today:
        today = {}

    # Recent trend averages (last 7 days with data)
    recent = [r for r in trends[-7:] if r.get("has_oura_data")]
    avg_sleep = round(sum(r["sleep_score"] for r in recent if r["sleep_score"]) / max(len(recent), 1), 1) if recent else None
    avg_readiness = round(sum(r["readiness_score"] for r in recent if r["readiness_score"]) / max(len(recent), 1), 1) if recent else None
    avg_hrv = round(sum(r["hrv_balance_score"] for r in recent if r["hrv_balance_score"]) / max(len(recent), 1), 1) if recent else None
    avg_rhr = round(sum(r["resting_heart_rate"] for r in recent if r.get("resting_heart_rate")) / max(sum(1 for r in recent if r.get("resting_heart_rate")), 1), 1) if recent else None
    avg_hrv_ms = round(sum(r["average_hrv"] for r in recent if r.get("average_hrv")) / max(sum(1 for r in recent if r.get("average_hrv")), 1), 1) if recent else None

    # Steps (yesterday + 7-day average, goal: 7500)
    step_rows = [r for r in trends[-7:] if r.get("preferred_steps")]
    avg_steps = round(sum(r["preferred_steps"] for r in step_rows) / len(step_rows)) if step_rows else None
    yesterday_steps_row = next((r for r in reversed(trends) if r.get("preferred_steps")), None)
    yesterday_steps = yesterday_steps_row.get("preferred_steps") if yesterday_steps_row else None
    yesterday_steps_date = yesterday_steps_row.get("date") if yesterday_steps_row else None

    recent_workouts = sum(1 for r in trends[-7:] if r.get("has_workout"))

    # Run data: 14-day window for coaching context
    run_rows_14d = [r for r in trends[-14:] if r.get("has_run") and r.get("run_distance_miles")]
    recent_run_rows = [r for r in trends[-7:] if r.get("has_run") and r.get("run_distance_miles")]
    recent_run_miles = round(sum(r["run_distance_miles"] for r in run_rows_14d), 2) if run_rows_14d else 0
    recent_run_paces = [r["run_pace_min_per_mile"] for r in run_rows_14d if r.get("run_pace_min_per_mile")]
    avg_run_pace = round(sum(recent_run_paces) / len(recent_run_paces), 2) if recent_run_paces else None

    sections = []

    sections.append(f"=== TODAY'S DATE: {today.get('date')} ===")
    # Today's scores
    sections.append("=== TODAY'S METRICS ===")
    if today.get("has_oura_data"):
        sections.append(f"Sleep score: {today.get('sleep_score')} (deep: {today.get('deep_sleep_score')}, REM: {today.get('rem_sleep_score')}, restfulness: {today.get('restfulness_score')})")
        sections.append(f"Readiness: {today.get('readiness_score')} | HRV balance score: {today.get('hrv_balance_score')} | Recovery index: {today.get('recovery_index_score')}")
        sections.append(f"Resting heart rate: {today.get('resting_heart_rate')} bpm | HRV (ms): {today.get('average_hrv')} | Temp deviation: {today.get('temperature_deviation')}°C")
    else:
        sections.append("Oura data: not yet available")

    if yesterday_steps:
        steps_vs_goal = yesterday_steps - 7500
        steps_note = f" (+{steps_vs_goal:,} over goal)" if steps_vs_goal >= 0 else f" ({abs(steps_vs_goal):,} short of 7,500 goal)"
        sections.append(f"Steps yesterday ({yesterday_steps_date}): {yesterday_steps:,}{steps_note}")

    if today.get("has_mood_log"):
        state = today.get("mood_state") or ""
        tags  = ", ".join(today.get("mood_tags") or [])
        sections.append(f"Mood (yesterday): {today.get('mood')} ({today.get('mood_score')}/5) | State: {state}" + (f" | Tags: {tags}" if tags else ""))

    if today.get("has_workout"):
        sections.append(f"Workout today: {today.get('workout_names')} ({today.get('total_workout_minutes', 0):.0f} min)")

    # 7-day trends
    sections.append("\n=== 7-DAY TRENDS ===")
    sections.append(f"Avg sleep score: {avg_sleep} | Avg readiness: {avg_readiness} | Avg HRV balance score: {avg_hrv}")
    sections.append(f"Avg resting HR: {avg_rhr} bpm | Avg HRV: {avg_hrv_ms} ms")
    if avg_steps is not None:
        steps_diff = avg_steps - 7500
        steps_trend = f"+{steps_diff:,} above" if steps_diff >= 0 else f"{abs(steps_diff):,} below"
        sections.append(f"Avg steps (7d): {avg_steps:,} ({steps_trend} 7,500 goal)")
    sections.append(f"Strength workouts in last 7 days: {recent_workouts}")

    # 14-day run history
    if run_rows_14d:
        pace_str = f" | Avg pace: {avg_run_pace} min/mile" if avg_run_pace else ""
        sections.append(f"\nRuns in last 14 days: {len(run_rows_14d)} ({recent_run_miles} miles total{pace_str})")
        sections.append("5k goal pace: 7:54/mile (24:30 finish)")
        for r in run_rows_14d:
            pace = f" @ {r['run_pace_min_per_mile']} min/mile" if r.get("run_pace_min_per_mile") else ""
            dist = r['run_distance_miles']
            sections.append(f"  - {r['date']}: {dist} mi{pace}")
    else:
        sections.append("\nRuns in last 14 days: 0")

    # Weather
    sections.append("\n=== TODAY'S WEATHER ===")
    if today.get("has_weather"):
        sections.append(f"{today.get('weather_desc')} | High: {today.get('temp_max_f')}°F | Low: {today.get('temp_min_f')}°F")
        sections.append(f"Rain probability: {today.get('precip_prob_max')}% | Likely rain: {today.get('likely_rain')}")
        sections.append(f"Morning: {today.get('morning_temp_f')}°F, {today.get('morning_precip_prob')}% rain")
        sections.append(f"Afternoon: {today.get('afternoon_temp_f')}°F, {today.get('afternoon_precip_prob')}% rain")
        sections.append(f"Evening: {today.get('evening_temp_f')}°F, {today.get('evening_precip_prob')}% rain")
        sections.append(f"Better to exercise in morning: {today.get('better_in_morning')} | Hot day: {today.get('hot_day')} | Cold day: {today.get('cold_day')}")
        sections.append(f"Sunrise: {today.get('sunrise')} | Sunset: {today.get('sunset')}")
    else:
        sections.append("Weather data not available")

    health_context = "\n".join(sections)

    notes_section = ""
    if active_notes:
        notes_section = "\n\n" + context_notes.format_for_prompt(active_notes)

    return f"""{health_context}{notes_section}

Based on all of this, provide Joshua with a daily coaching summary. Be upbeat and positive throughout.

Structure your response as JSON with these exact keys:

"status_summary": 2-3 sentences covering:
  (a) sleep quality and what it means for today — classify today as LIGHT, MODERATE, or HARD effort day based on sleep/readiness score (light = poor sleep/low readiness, moderate = average, hard = great sleep/high readiness)
  (b) brief weather note for outdoor activity if relevant

"recommendations": a JSON array of exactly 3 plain strings (no nested objects, no keys — just 3 strings) in this order:
  1. Running coaching string: the overall goal is a 5k in sub-24:30 (7:54/mile). Look at the past 14 days of runs — their frequency, distances, and paces — and give ONE specific training suggestion for today based on where they are in their training. Pick from: easy recovery run, tempo run (sustained 7:30–8:00/mile effort), interval sprints (e.g. 6x400m fast), long slow run (build aerobic base), or rest/cross-train. Choose based on how many days since the last run and what type of run they haven't done recently. Be specific: tell them what to do and why it helps the 5k goal. Keep it to 2 sentences max.
  2. Steps string: reference yesterday's step count vs the 7,500/day goal, and how the 7-day average is trending — give one positive nudge for today.
  3. Activity/recovery string: a positive, achievable tip based on the effort classification for today.

"watchout": one constructive, positive heads-up (not a warning — frame it as something to be aware of that helps them succeed)

Format as JSON only. No markdown. Keep each item concise (1-2 sentences)."""


def get_insights(today: dict, trends: list, personal_context: dict, dry_run: bool = False) -> dict:
    today_str = date.today().isoformat()
    cache_path = INSIGHTS_CACHE_DIR / f"insights_{today_str}.json"

    # Return cached insights if already generated today (avoids double-billing)
    if not dry_run and cache_path.exists():
        try:
            cached = json.loads(cache_path.read_text())
            log.info("Using cached insights for %s (no new Claude call)", today_str)
            return cached
        except Exception:
            pass  # Fall through to regenerate

    if dry_run:
        return {
            "status_summary": "Dashboard running in dry-run mode — Claude insights disabled.",
            "recommendations": [
                "Run generate_dashboard.py without --dry-run to get real AI insights.",
                "Ensure ANTHROPIC_API_KEY is set in your .env file.",
            ],
            "watchout": "Dry-run mode active — no Claude API calls made.",
        }

    if not ANTHROPIC_API_KEY:
        log.warning("ANTHROPIC_API_KEY not set — skipping insights")
        return {
            "status_summary": "AI insights unavailable: ANTHROPIC_API_KEY not configured.",
            "recommendations": ["Add ANTHROPIC_API_KEY to your .env file to enable AI insights."],
            "watchout": "Set up your Anthropic API key to unlock personalized recommendations.",
        }

    active = context_notes.get_active_notes()
    notes_block = ("\n" + context_notes.format_for_prompt(active) + "\n") if active else ""

    prompt = build_insight_prompt(today, trends, personal_context, active_notes=active)
    personal_str = json.dumps(personal_context, indent=2)
    system = SYSTEM_PROMPT.format(
        name=personal_context.get("name", "Joshua"),
        personal_context=personal_str,
        active_notes=notes_block,
    )

    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        response = client.messages.create(
            model=INSIGHTS_MODEL,
            max_tokens=1024,
            system=system,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text.strip()

        # Parse JSON from response
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text:
            text = text.split("```")[1].split("```")[0].strip()

        insights = json.loads(text)
        log.info("Got insights from Claude (input: %d tokens, output: %d tokens)",
                 response.usage.input_tokens, response.usage.output_tokens)
        _, _, total_cost = llm_logging.log_llm_call(
            "dashboard", INSIGHTS_MODEL,
            response.usage.input_tokens, response.usage.output_tokens,
        )
        llm_logging.log_daily_insights(
            today_str, insights,
            response.usage.input_tokens, response.usage.output_tokens, total_cost,
        )
        try:
            cache_path.write_text(json.dumps(insights))
        except Exception as e:
            log.warning("Could not cache insights: %s", e)
        return insights

    except json.JSONDecodeError as e:
        log.error("Failed to parse Claude response as JSON: %s", e)
        return {
            "status_summary": text if "text" in dir() else "Error parsing AI response.",
            "recommendations": [],
            "watchout": "AI response was not valid JSON — check logs.",
        }
    except anthropic.APIError as e:
        log.error("Claude API error: %s", e)
        return {
            "status_summary": "AI insights temporarily unavailable.",
            "recommendations": [],
            "watchout": f"API error: {e}",
        }


# ---------------------------------------------------------------------------
# HTML Template
# ---------------------------------------------------------------------------

DASHBOARD_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Joshua's Health Dashboard — {{ today.date }}</title>
  <script src="https://d3js.org/d3.v7.min.js"></script>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    :root {
      --bg: #F2EDE3;
      --surface: #EBE5D8;
      --surface2: #E0D9CB;
      --border: #BBBBBB;
      --text: #111111;
      --text-dim: #777777;
      --accent: #CC2200;
      --accent2: #FFCC00;
      --accent3: #1155BB;
      --accent4: #111111;
      --good: #007700;
      --warn: #CC6600;
      --bad: #CC2200;
      --radius: 0px;
    }

    body {
      background: var(--bg);
      color: var(--text);
      font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif;
      line-height: 1.5;
      min-height: 100vh;
    }

    header {
      background: var(--accent);
      border-bottom: 4px solid #000;
      padding: 1rem 2rem;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    header h1 { font-size: 1.1rem; color: #fff; font-weight: 900; letter-spacing: 0.12em; text-transform: uppercase; }
    header .date { color: rgba(255,255,255,0.75); font-size: 0.85rem; letter-spacing: 0.05em; }

    main { max-width: 1400px; margin: 0 auto; padding: 1.5rem 2rem; }

    .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 1.5rem; margin-bottom: 1.5rem; }
    .grid-3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 1.5rem; margin-bottom: 1.5rem; }
    .grid-4 { display: grid; grid-template-columns: repeat(4, 1fr); gap: 1rem; margin-bottom: 1.5rem; }
    @media (max-width: 900px) {
      .grid-2, .grid-3, .grid-4 { grid-template-columns: 1fr; }
    }
    @media (max-width: 1200px) {
      .grid-4 { grid-template-columns: 1fr 1fr; }
    }

    .card {
      background: var(--surface);
      border: 2px solid var(--border);
      border-radius: var(--radius);
      padding: 1.25rem;
    }

    .card-title {
      font-size: 0.68rem;
      text-transform: uppercase;
      letter-spacing: 0.15em;
      color: var(--text-dim);
      margin-bottom: 0.75rem;
      font-weight: 700;
    }

    .section-title {
      font-size: 0.72rem;
      font-weight: 900;
      color: var(--text);
      margin-bottom: 1rem;
      padding-bottom: 0.5rem;
      border-bottom: 4px solid var(--accent);
      text-transform: uppercase;
      letter-spacing: 0.2em;
    }

    /* Stat tiles */
    .stat-tile {
      background: var(--surface);
      border: 2px solid var(--border);
      border-radius: var(--radius);
      padding: 1rem 1.25rem;
      display: flex;
      flex-direction: column;
      gap: 0.25rem;
      position: relative;
    }
    .stat-tile::before {
      content: '';
      position: absolute;
      top: 0; left: 0;
      width: 4px; height: 100%;
      background: var(--accent3);
    }
    .stat-tile .label { font-size: 0.65rem; color: var(--text-dim); text-transform: uppercase; letter-spacing: 0.12em; font-weight: 700; }
    .stat-tile .value { font-size: 2.2rem; font-weight: 900; line-height: 1; }
    .stat-tile .subtext { font-size: 0.75rem; color: var(--text-dim); }
    .score-good { color: var(--good); }
    .score-warn { color: var(--warn); }
    .score-bad  { color: var(--bad); }
    .score-na   { color: var(--text-dim); }

    /* Charts */
    .chart-container { width: 100%; }
    .chart-container svg { display: block; }
    .tick text { fill: var(--text-dim); font-size: 11px; }
    .tick line, .domain { stroke: var(--border); }
    .grid line { stroke: var(--border); stroke-opacity: 0.5; }
    .tooltip {
      position: absolute;
      background: var(--surface2);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 0.5rem 0.75rem;
      font-size: 0.82rem;
      pointer-events: none;
      opacity: 0;
      transition: opacity 0.15s;
    }

    /* Weather */
    .weather-row { display: flex; gap: 0.75rem; flex-wrap: wrap; margin-top: 0.5rem; }
    .weather-pill {
      background: var(--surface2);
      border: 2px solid var(--border);
      border-radius: var(--radius);
      padding: 0.3rem 0.75rem;
      font-size: 0.78rem;
      display: flex;
      align-items: center;
      gap: 0.4rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }
    .weather-pill.rain { color: var(--accent3); border-color: var(--accent3); }
    .weather-pill.hot  { color: var(--bad); border-color: var(--bad); }
    .weather-pill.cold { color: #90caf9; border-color: #90caf9; }
    .weather-pill.good { color: var(--good); border-color: var(--good); }
    .time-of-day {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 0;
      margin-top: 0.75rem;
      border: 2px solid var(--border);
    }
    .tod-block {
      background: var(--surface2);
      padding: 0.75rem;
      border-right: 2px solid var(--border);
    }
    .tod-block:last-child { border-right: none; }
    .tod-block .tod-label { font-size: 0.62rem; color: var(--text-dim); text-transform: uppercase; letter-spacing: 0.1em; font-weight: 700; }
    .tod-block .tod-temp { font-size: 1.3rem; font-weight: 900; }
    .tod-block .tod-rain { font-size: 0.78rem; color: var(--accent3); font-weight: 700; }

    /* AI insights */
    .insight-card {
      background: var(--surface);
      border: 2px solid var(--border);
      border-radius: var(--radius);
      border-left: 6px solid var(--accent2);
      padding: 1.5rem;
      margin-bottom: 1.5rem;
    }
    .insight-summary {
      font-size: 0.93rem;
      color: var(--text);
      line-height: 1.75;
      margin-bottom: 1.25rem;
    }
    .recommendations { list-style: none; }
    .recommendations li {
      display: flex;
      gap: 0.75rem;
      padding: 0.65rem 0;
      border-top: 1px solid var(--border);
      font-size: 0.88rem;
      line-height: 1.5;
    }
    .recommendations li .icon {
      font-size: 0.7rem;
      flex-shrink: 0;
      color: var(--accent2);
      font-weight: 900;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      margin-top: 0.2rem;
    }
    .watchout-box {
      background: rgba(204, 34, 0, 0.08);
      border: 2px solid var(--accent);
      border-radius: var(--radius);
      padding: 0.75rem 1rem;
      margin-top: 1rem;
      font-size: 0.85rem;
      color: ##ba2611;
      display: flex;
      gap: 0.5rem;
      align-items: flex-start;
    }

    /* Chat */
    #chat-section {
      background: var(--surface);
      border: 2px solid var(--border);
      border-radius: var(--radius);
      overflow: hidden;
      margin-bottom: 2rem;
    }
    #chat-header {
      background: var(--accent3);
      padding: 0.75rem 1.25rem;
      border-bottom: 2px solid #000;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    #chat-header h2 { font-size: 0.75rem; font-weight: 900; color: #fff; text-transform: uppercase; letter-spacing: 0.15em; }
    #chat-messages {
      height: 320px;
      overflow-y: auto;
      padding: 1rem 1.25rem;
      display: flex;
      flex-direction: column;
      gap: 0.75rem;
    }
    .msg {
      max-width: 80%;
      padding: 0.6rem 0.9rem;
      border-radius: 12px;
      font-size: 0.88rem;
      line-height: 1.55;
      white-space: pre-wrap;
    }
    .msg.user {
      background: var(--accent);
      color: #fff;
      align-self: flex-end;
      border-radius: var(--radius);
      font-weight: 600;
    }
    .msg.assistant {
      background: var(--surface2);
      color: var(--text);
      align-self: flex-start;
      border-radius: var(--radius);
      border-left: 3px solid var(--accent3);
    }
    .msg.typing { color: var(--text-dim); font-style: italic; }
    #chat-form {
      display: flex;
      border-top: 1px solid var(--border);
    }
    #chat-input {
      flex: 1;
      background: transparent;
      border: none;
      padding: 0.85rem 1.25rem;
      color: var(--text);
      font-size: 0.9rem;
      outline: none;
    }
    #chat-input::placeholder { color: var(--text-dim); }
    #chat-send {
      background: var(--accent);
      color: #fff;
      border: none;
      border-left: 2px solid #000;
      padding: 0 1.5rem;
      font-size: 0.72rem;
      font-weight: 900;
      text-transform: uppercase;
      letter-spacing: 0.1em;
      cursor: pointer;
      transition: background 0.1s;
    }
    #chat-send:hover { background: #aa1c00; }
    #chat-send:disabled { background: var(--border); cursor: not-allowed; }

    .no-data { color: var(--text-dim); font-style: italic; font-size: 0.85rem; }

    /* LLM Usage */
    .llm-usage-row {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 0.45rem 0;
      border-top: 1px solid var(--border);
      font-size: 0.82rem;
      gap: 1rem;
      flex-wrap: wrap;
    }
    .llm-usage-model { font-weight: 700; text-transform: uppercase; letter-spacing: 0.05em; }
    .llm-usage-tokens { color: var(--text-dim); font-size: 0.78rem; flex: 1; }
    .llm-usage-cost { font-weight: 900; color: var(--accent3); white-space: nowrap; }
    #session-usage {
      margin-top: 1rem;
      padding-top: 0.75rem;
      border-top: 2px solid var(--border);
      font-size: 0.78rem;
      display: none;
    }

    footer {
      text-align: center;
      padding: 1.5rem;
      color: var(--text-dim);
      font-size: 0.72rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      border-top: 4px solid var(--border);
    }
  </style>
</head>
<body>

<header>
  <h1>⚡ Health Dashboard</h1>
  <span class="date">{{ today.date or '' }} &nbsp;·&nbsp; Generated {{ generated_at }}</span>
</header>

<div class="tooltip" id="tooltip"></div>

<main>

  <!-- ===== SECTION 1: HOW I'M DOING ===== -->
  <div style="display:flex; align-items:center; justify-content:space-between; flex-wrap:wrap; gap:0.5rem; margin-bottom:0.5rem;">
    <div class="section-title" style="margin-bottom:0;">How I'm Doing</div>
    <div style="display:flex; align-items:center; gap:0.5rem;">
      <label for="date-picker" style="font-size:0.78rem; color:var(--text-dim); text-transform:uppercase; letter-spacing:0.06em;">Date</label>
      <input type="date" id="date-picker" value="{{ today.date or '' }}"
             min="{{ trend_dates.0 }}" max="{{ today.date or '' }}"
             style="background:var(--card-bg); color:var(--text); border:1px solid var(--border); border-radius:6px; padding:0.3rem 0.6rem; font-size:0.82rem; cursor:pointer;">
    </div>
  </div>

  <!-- Today's key scores -->
  <div class="grid-4">
    <div class="stat-tile">
      <span class="label">Sleep Score</span>
      <span class="value" id="tile-sleep-value">{{ today.sleep_score or '—' }}</span>
      <span class="subtext" id="tile-sleep-sub">Deep: {{ today.deep_sleep_score or '—' }} &nbsp;|&nbsp; REM: {{ today.rem_sleep_score or '—' }}</span>
    </div>
    <div class="stat-tile">
      <span class="label">Readiness</span>
      <span class="value" id="tile-readiness-value">{{ today.readiness_score or '—' }}</span>
      <span class="subtext" id="tile-readiness-sub">HRV balance: {{ today.hrv_balance_score or '—' }}</span>
    </div>
    <div class="stat-tile">
      <span class="label">Activity <span style="font-weight:400;font-size:0.68rem;opacity:0.6;">(yesterday)</span></span>
      <span class="value" id="tile-activity-value">{{ today.activity_score or '—' }}</span>
      <span class="subtext" id="tile-activity-sub">Steps: {{ '{:,}'.format(today.preferred_steps) if today.preferred_steps else '—' }}</span>
    </div>
    <div class="stat-tile">
      <span class="label">Mood <span style="font-weight:400;font-size:0.68rem;opacity:0.6;">(yesterday)</span></span>
      <span class="value" id="tile-mood-value">{{ today.mood or '—' }}</span>
      <span class="subtext" id="tile-mood-sub">{% if today.mood_state %}{{ today.mood_state }}{% else %}No log today{% endif %}</span>
      <div id="tile-mood-tags" style="margin-top:0.4rem; display:flex; flex-wrap:wrap; gap:0.3rem;">
        {% for tag in today.mood_tags %}
        <span style="background:rgba(255,255,255,0.08); border:1px solid var(--border); border-radius:999px; padding:0.1rem 0.55rem; font-size:0.72rem; color:var(--text-dim);">{{ tag }}</span>
        {% endfor %}
      </div>
    </div>
  </div>

  <!-- Charts row -->
  <div class="grid-2">
    <div class="card">
      <div class="card-title">Sleep &amp; Readiness — 30 Days &nbsp;<span style="font-weight:400;font-size:0.75rem;"><span style="color:#CC2200;">●</span> Sleep &nbsp;<span style="color:#FFCC00;">●</span> Readiness</span></div>
      <div class="chart-container" id="chart-sleep"></div>
    </div>
    <div class="card">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0.5rem;">
        <div class="card-title" style="margin-bottom:0;">Heart Rate &amp; HRV — 30 Days &nbsp;<span style="font-weight:400;font-size:0.75rem;" id="hrv-legend-raw"><span style="color:#CC2200;">●</span> Resting HR &nbsp;<span style="color:#1155BB;">●</span> HRV</span><span style="font-weight:400;font-size:0.75rem;display:none;" id="hrv-legend-score"><span style="color:#1155BB;">●</span> HRV Score &nbsp;<span style="color:#CC2200;">●</span> HR Score</span></div>
        <div style="display:flex;gap:0;border:1px solid var(--border);border-radius:6px;overflow:hidden;font-size:0.72rem;">
          <button id="hrv-btn-raw" onclick="showHrvRaw()" style="padding:0.2rem 0.6rem;background:var(--accent2);color:#000;border:none;cursor:pointer;font-size:0.72rem;">Raw</button>
          <button id="hrv-btn-score" onclick="showHrvScore()" style="padding:0.2rem 0.6rem;background:none;color:var(--text-dim);border:none;cursor:pointer;font-size:0.72rem;">Oura Scores</button>
        </div>
      </div>
      <div class="chart-container" id="chart-hrv-raw"></div>
      <div class="chart-container" id="chart-hrv-score" style="display:none;"></div>
    </div>
  </div>

  <div class="grid-2">
    <div class="card">
      <div class="card-title">Steps — 30 Days</div>
      <div class="chart-container" id="chart-steps"></div>
    </div>
    <div class="card">
      <div class="card-title">Mood — 30 Days</div>
      <div class="chart-container" id="chart-mood"></div>
    </div>
  </div>

  <!-- Workout summary -->
  <div class="card" style="margin-bottom:1.5rem">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0.5rem;">
      <div class="card-title" style="margin-bottom:0;">Workouts — 30 Days</div>
      <div style="font-size:0.72rem;color:var(--text-dim);display:flex;gap:0.75rem;">
        <span><span style="color:#1155BB;">●</span> Strength</span>
        <span><span style="color:#22AA66;">●</span> Oura (runs/walks)</span>
        <span><span style="color:#AA44FF;">●</span> Both</span>
      </div>
    </div>
    <div class="chart-container" id="chart-workouts"></div>
  </div>

  <!-- ===== SECTION 2: WHAT I CAN DO TODAY ===== -->
  <div class="section-title" style="margin-top:1rem">What I Can Do Today</div>

  <!-- Weather card -->
  {% if today.has_weather %}
  <div class="card" style="margin-bottom:1.5rem">
    <div class="card-title">Today's Weather — {{ today.weather_city }}</div>
    <div style="font-size:1.1rem;font-weight:600;margin-bottom:0.5rem">
      {{ today.weather_desc }} &nbsp;·&nbsp; {{ today.temp_min_f }}–{{ today.temp_max_f }}°F
      &nbsp;·&nbsp; Sunrise {{ today.sunrise.split('T')[-1] if today.sunrise else '—' }} | Sunset {{ today.sunset.split('T')[-1] if today.sunset else '—' }}
    </div>
    <div class="weather-row">
      {% if today.likely_rain %}
      <span class="weather-pill rain">🌧 Rain likely ({{ today.precip_prob_max }}%)</span>
      {% endif %}
      {% if today.better_in_morning %}
      <span class="weather-pill good">☀️ Better to exercise in the morning</span>
      {% endif %}
      {% if today.hot_day %}
      <span class="weather-pill hot">🌡 Hot day — stay hydrated</span>
      {% endif %}
      {% if today.cold_day %}
      <span class="weather-pill cold">🧊 Cold day — layer up</span>
      {% endif %}
      {% if not today.likely_rain and not today.better_in_morning and not today.hot_day and not today.cold_day %}
      <span class="weather-pill good">✅ Good conditions for outdoor activity</span>
      {% endif %}
    </div>
    <div class="time-of-day">
      <div class="tod-block">
        <div class="tod-label">Morning (6–9am)</div>
        <div class="tod-temp">{{ today.morning_temp_f }}°F</div>
        <div class="tod-rain">{{ today.morning_precip_prob|int }}% rain</div>
      </div>
      <div class="tod-block">
        <div class="tod-label">Afternoon (12–3pm)</div>
        <div class="tod-temp">{{ today.afternoon_temp_f }}°F</div>
        <div class="tod-rain">{{ today.afternoon_precip_prob|int }}% rain</div>
      </div>
      <div class="tod-block">
        <div class="tod-label">Evening (5–10pm)</div>
        <div class="tod-temp">{{ today.evening_temp_f }}°F</div>
        <div class="tod-rain">{{ today.evening_precip_prob|int }}% rain</div>
      </div>
    </div>
  </div>
  {% endif %}

  <!-- AI insights -->
  <div class="insight-card">
    <div style="display:flex;align-items:center;gap:0.75rem;margin-bottom:1rem">
      <span style="font-size:0.62rem;font-weight:900;color:var(--accent2);text-transform:uppercase;letter-spacing:0.18em;border:2px solid var(--accent2);padding:0.2rem 0.5rem">AI · Haiku</span>
    </div>
    <div class="insight-summary">{{ insights.status_summary }}</div>
    <ul class="recommendations">
      {% for rec in insights.recommendations %}
      <li>
        <span class="icon">→</span>
        <span>{{ rec }}</span>
      </li>
      {% endfor %}
    </ul>
    {% if insights.watchout %}
    <div class="watchout-box">
      <span>⚠️</span>
      <span>{{ insights.watchout }}</span>
    </div>
    {% endif %}
  </div>

  <!-- ===== SECTION 3: CHAT ===== -->
  <div class="section-title">Ask Claude About Your Health</div>

  <div id="chat-section">
    <div id="chat-header">
      <h2>💬 Health Chat · Claude Sonnet</h2>
      <span style="font-size:0.78rem;color:var(--text-dim)">Full health context sent with every message</span>
    </div>
    <div id="chat-messages"></div>
    <form id="chat-form" onsubmit="sendMessage(event)">
      <input id="chat-input" type="text" placeholder="How's my HRV trend looking? Should I rest today?" autocomplete="off">
      <button id="chat-send" type="submit">Send</button>
    </form>
  </div>

  <!-- ===== CHAT HISTORY ===== -->
  <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:0.5rem;margin-top:2rem;">
    <div class="section-title" style="margin-bottom:0;">Chat History</div>
    <button onclick="clearHistory()" style="background:none;border:1px solid var(--border);color:var(--text-dim);border-radius:6px;padding:0.2rem 0.7rem;font-size:0.75rem;cursor:pointer;">Clear All</button>
  </div>
  <div id="chat-history-list" style="margin-bottom:2rem;"></div>

  <!-- ===== LLM USAGE ===== -->
  <div class="section-title" style="margin-top:2rem;">LLM Usage</div>
  <div class="card" style="margin-bottom:2rem;">
    <div style="display:flex; gap:2.5rem; flex-wrap:wrap; align-items:flex-start;">
      <div style="min-width:160px;">
        <div class="card-title">{{ llm_stats.month }} Total</div>
        <div style="font-size:2rem; font-weight:900; color:var(--accent3);">${{ "%.4f"|format(llm_stats.totals.total_cost) }}</div>
        <div style="font-size:0.75rem; color:var(--text-dim); margin-top:0.2rem;">
          {{ llm_stats.totals.total_calls }} calls &nbsp;·&nbsp;
          {{ "{:,}".format(llm_stats.totals.total_input_tokens) }} in /
          {{ "{:,}".format(llm_stats.totals.total_output_tokens) }} out tokens
        </div>
      </div>
      <div style="flex:1; min-width:280px;">
        {% for r in llm_stats.by_model %}
        <div class="llm-usage-row">
          <span class="llm-usage-model">{{ r.model }}</span>
          <span class="llm-usage-tokens">{{ r.calls }} calls &nbsp;·&nbsp; {{ "{:,}".format(r.input_tokens) }} in / {{ "{:,}".format(r.output_tokens) }} out</span>
          <span class="llm-usage-cost">${{ "%.4f"|format(r.total_cost) }}</span>
        </div>
        {% endfor %}
        {% if not llm_stats.by_model %}
        <div class="no-data">No LLM calls recorded this month yet.</div>
        {% endif %}
      </div>
    </div>
    <div id="session-usage">
      <span style="text-transform:uppercase; font-weight:700; letter-spacing:0.08em; color:var(--text);">This Session</span>
      <span id="session-stats" style="margin-left:1rem; color:var(--text-dim);"></span>
    </div>
  </div>

</main>

<footer>
  Generated {{ generated_at }} · Health data from Oura, Garmin, Daily Strength, Daylio, Open-Meteo · AI by Claude
</footer>

<!-- ===== DATA + CHARTS ===== -->
<script>
// Embedded health data
const TREND_DATA = {{ trend_json }};
const TODAY_DATA = {{ today_json }};
const HEALTH_CONTEXT = {{ health_context_json }};

// Build a quick date→row lookup from trend data
const TREND_BY_DATE = {};
TREND_DATA.forEach(r => { if (r.date) TREND_BY_DATE[r.date] = r; });

// Score colouring (mirrors Python score_class / mood_class)
function scoreClass(v)    { return v == null ? "score-na" : v >= 75 ? "score-good" : v >= 55 ? "score-warn" : "score-bad"; }
function moodClass(v)     { return v == null ? "score-na" : v >= 4  ? "score-good" : v >= 3  ? "score-warn" : "score-bad"; }

function fmt(v, fallback) { return (v != null && v !== "") ? v : (fallback || "—"); }
function fmtSteps(v)      { return v != null ? Number(v).toLocaleString() : "—"; }

function renderTiles(row) {
  // Sleep
  const sv = document.getElementById("tile-sleep-value");
  const ss = document.getElementById("tile-sleep-sub");
  if (sv) { sv.textContent = fmt(row.sleep_score); sv.className = "value " + scoreClass(row.sleep_score); }
  if (ss) ss.textContent = "Deep: " + fmt(row.deep_sleep_score) + "  |  REM: " + fmt(row.rem_sleep_score);

  // Readiness
  const rv = document.getElementById("tile-readiness-value");
  const rs = document.getElementById("tile-readiness-sub");
  if (rv) { rv.textContent = fmt(row.readiness_score); rv.className = "value " + scoreClass(row.readiness_score); }
  if (rs) rs.textContent = "HRV balance: " + fmt(row.hrv_balance_score);

  // Activity
  const av = document.getElementById("tile-activity-value");
  const as_ = document.getElementById("tile-activity-sub");
  if (av) { av.textContent = fmt(row.activity_score); av.className = "value " + scoreClass(row.activity_score); }
  if (as_) as_.textContent = "Steps: " + fmtSteps(row.preferred_steps);

  // Mood
  const mv  = document.getElementById("tile-mood-value");
  const ms  = document.getElementById("tile-mood-sub");
  const mt  = document.getElementById("tile-mood-tags");
  if (mv) { mv.textContent = fmt(row.mood); mv.className = "value " + moodClass(row.mood_score); }
  if (ms) ms.textContent = row.mood_state || (row.has_mood_log ? "" : "No log today");
  if (mt) {
    const tags = row.mood_tags || [];
    mt.innerHTML = tags.map(t =>
      `<span style="background:rgba(255,255,255,0.08);border:1px solid var(--border);border-radius:999px;padding:0.1rem 0.55rem;font-size:0.72rem;color:var(--text-dim);">${t}</span>`
    ).join("");
  }
}

// Date picker wiring
document.addEventListener("DOMContentLoaded", () => {
  const picker = document.getElementById("date-picker");
  if (!picker) return;
  picker.addEventListener("change", () => {
    const row = TREND_BY_DATE[picker.value];
    if (row) renderTiles(row);
    else {
      // Date not in trend window — reset to today
      picker.value = TODAY_DATA.date || "";
      renderTiles(TODAY_DATA);
    }
  });

  // Apply initial score classes to today tiles (Jinja doesn't set them dynamically)
  renderTiles(TODAY_DATA);
});

// Tooltip helper
const tooltip = document.getElementById("tooltip");
function showTooltip(event, html) {
  tooltip.innerHTML = html;
  tooltip.style.opacity = 1;
  tooltip.style.left = (event.pageX + 12) + "px";
  tooltip.style.top  = (event.pageY - 20) + "px";
}
function hideTooltip() { tooltip.style.opacity = 0; }

// Score color helper (JS side)
function scoreColor(v) {
  if (v == null) return "#8b93c4";
  if (v >= 75) return "#4ecca3";
  if (v >= 55) return "#ffd166";
  return "#ff6b6b";
}
function moodColor(v) {
  if (v == null) return "#8b93c4";
  if (v >= 4) return "#4ecca3";
  if (v >= 3) return "#ffd166";
  return "#ff6b6b";
}

// Generic line chart factory
function lineChart(containerId, data, lines, opts = {}) {
  const el = document.getElementById(containerId);
  if (!el) return;
  const W = el.clientWidth || 500, H = opts.height || 160;
  const margin = { top: 12, right: 24, bottom: 28, left: 36 };
  const w = W - margin.left - margin.right;
  const h = H - margin.top - margin.bottom;

  const svg = d3.select(el).append("svg")
    .attr("width", W).attr("height", H);
  const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

  const parseDate = d3.timeParse("%Y-%m-%d");
  const fmt = d3.timeFormat("%b %d");

  const x = d3.scaleTime()
    .domain(d3.extent(data, d => parseDate(d.date)))
    .range([0, w]);

  // Determine y domain across all lines
  const allVals = lines.flatMap(l => data.map(d => d[l.key]).filter(v => v != null));
  const yMin = opts.yMin != null ? opts.yMin : Math.max(0, d3.min(allVals) - 5);
  const yMax = opts.yMax != null ? opts.yMax : d3.max(allVals) + 5;

  const y = d3.scaleLinear().domain([yMin, yMax]).range([h, 0]);

  // Grid lines
  g.append("g").attr("class", "grid")
    .call(d3.axisLeft(y).tickSize(-w).tickFormat("").ticks(4));

  // Axes
  g.append("g").attr("transform", `translate(0,${h})`)
    .call(d3.axisBottom(x).ticks(6).tickFormat(fmt));
  g.append("g").call(d3.axisLeft(y).ticks(4));

  // Lines + dots
  for (const line of lines) {
    const valid = data.filter(d => d[line.key] != null);
    if (valid.length === 0) continue;

    const lineFn = d3.line()
      .defined(d => d[line.key] != null)
      .x(d => x(parseDate(d.date)))
      .y(d => y(d[line.key]))
      .curve(d3.curveMonotoneX);

    g.append("path")
      .datum(data)
      .attr("fill", "none")
      .attr("stroke", line.color || "#7c83ff")
      .attr("stroke-width", 2)
      .attr("d", lineFn);

    // Dots (only show last 14 days to avoid clutter)
    const recent = valid.slice(-14);
    g.selectAll(`.dot-${line.key}`)
      .data(recent)
      .join("circle")
      .attr("class", `dot-${line.key}`)
      .attr("cx", d => x(parseDate(d.date)))
      .attr("cy", d => y(d[line.key]))
      .attr("r", 3)
      .attr("fill", line.color || "#7c83ff")
      .on("mouseover", (event, d) => {
        showTooltip(event, `<b>${fmt(parseDate(d.date))}</b><br>${line.label}: <b>${d[line.key]}</b>`);
      })
      .on("mouseout", hideTooltip);
  }

}

// Dual-axis line chart (left axis = line1, right axis = line2)
function dualAxisChart(containerId, data, line1, line2) {
  const el = document.getElementById(containerId);
  if (!el) return;
  const W = el.clientWidth || 500, H = 160;
  const margin = { top: 12, right: 48, bottom: 28, left: 36 };
  const w = W - margin.left - margin.right;
  const h = H - margin.top - margin.bottom;

  const svg = d3.select(el).append("svg").attr("width", W).attr("height", H);
  const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

  const parseDate = d3.timeParse("%Y-%m-%d");
  const fmt = d3.timeFormat("%b %d");

  const x = d3.scaleTime()
    .domain(d3.extent(data, d => parseDate(d.date)))
    .range([0, w]);

  const vals1 = data.map(d => d[line1.key]).filter(v => v != null);
  const vals2 = data.map(d => d[line2.key]).filter(v => v != null);

  const yL = d3.scaleLinear()
    .domain([d3.min(vals1) - 3, d3.max(vals1) + 3])
    .range([h, 0]);
  const yR = d3.scaleLinear()
    .domain([d3.min(vals2) - 5, d3.max(vals2) + 5])
    .range([h, 0]);

  // Grid
  g.append("g").attr("class", "grid")
    .call(d3.axisLeft(yL).tickSize(-w).tickFormat("").ticks(4));

  // Axes
  g.append("g").attr("transform", `translate(0,${h})`).call(d3.axisBottom(x).ticks(6).tickFormat(fmt));
  g.append("g").call(d3.axisLeft(yL).ticks(4));
  g.append("g").attr("transform", `translate(${w},0)`)
    .call(d3.axisRight(yR).ticks(4))
    .selectAll("text").attr("fill", line2.color);

  // Draw each line
  for (const [line, yScale] of [[line1, yL], [line2, yR]]) {
    const valid = data.filter(d => d[line.key] != null);
    if (valid.length === 0) continue;

    const lineFn = d3.line()
      .defined(d => d[line.key] != null)
      .x(d => x(parseDate(d.date)))
      .y(d => yScale(d[line.key]))
      .curve(d3.curveMonotoneX);

    g.append("path").datum(data)
      .attr("fill", "none").attr("stroke", line.color)
      .attr("stroke-width", 2).attr("d", lineFn);

    g.selectAll(`.dot-${line.key}`)
      .data(valid.slice(-14)).join("circle")
      .attr("class", `dot-${line.key}`)
      .attr("cx", d => x(parseDate(d.date)))
      .attr("cy", d => yScale(d[line.key]))
      .attr("r", 3).attr("fill", line.color)
      .on("mouseover", (event, d) => showTooltip(event, `<b>${fmt(parseDate(d.date))}</b><br>${line.label}: <b>${d[line.key]}</b>`))
      .on("mouseout", hideTooltip);
  }

}

function showHrvRaw() {
  document.getElementById("chart-hrv-raw").style.display = "";
  document.getElementById("chart-hrv-score").style.display = "none";
  document.getElementById("hrv-legend-raw").style.display = "";
  document.getElementById("hrv-legend-score").style.display = "none";
  document.getElementById("hrv-btn-raw").style.background = "var(--accent2)";
  document.getElementById("hrv-btn-raw").style.color = "#000";
  document.getElementById("hrv-btn-score").style.background = "none";
  document.getElementById("hrv-btn-score").style.color = "var(--text-dim)";
}
function showHrvScore() {
  document.getElementById("chart-hrv-raw").style.display = "none";
  document.getElementById("chart-hrv-score").style.display = "";
  document.getElementById("hrv-legend-raw").style.display = "none";
  document.getElementById("hrv-legend-score").style.display = "";
  document.getElementById("hrv-btn-score").style.background = "var(--accent2)";
  document.getElementById("hrv-btn-score").style.color = "#000";
  document.getElementById("hrv-btn-raw").style.background = "none";
  document.getElementById("hrv-btn-raw").style.color = "var(--text-dim)";
}

// Bar chart factory (for steps, workouts)
function barChart(containerId, data, key, color, opts = {}) {
  const el = document.getElementById(containerId);
  if (!el) return;
  const W = el.clientWidth || 500, H = opts.height || 140;
  const margin = { top: 8, right: 16, bottom: 28, left: 48 };
  const w = W - margin.left - margin.right;
  const h = H - margin.top - margin.bottom;

  const svg = d3.select(el).append("svg")
    .attr("width", W).attr("height", H);
  const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

  const parseDate = d3.timeParse("%Y-%m-%d");
  const fmt = d3.timeFormat("%b %d");

  const x = d3.scaleBand()
    .domain(data.map(d => d.date))
    .range([0, w])
    .padding(0.2);

  const getVal = typeof key === "function" ? key : d => d[key] || 0;
  const yMax = d3.max(data, d => getVal(d)) || 1;
  const y = d3.scaleLinear().domain([0, yMax]).range([h, 0]);

  g.append("g").attr("transform", `translate(0,${h})`)
    .call(d3.axisBottom(x)
      .tickValues(x.domain().filter((_, i, arr) => i % Math.ceil(arr.length / 6) === 0))
      .tickFormat(d => fmt(parseDate(d))));

  g.append("g").call(d3.axisLeft(y).ticks(4)
    .tickFormat(opts.tickFormat || (v => v)));

  g.selectAll("rect")
    .data(data)
    .join("rect")
    .attr("x", d => x(d.date))
    .attr("y", d => y(getVal(d)))
    .attr("width", x.bandwidth())
    .attr("height", d => h - y(getVal(d)))
    .attr("fill", d => typeof color === "function" ? color(d) : color)
    .attr("rx", 2)
    .on("mouseover", (event, d) => {
      const label = opts.label || (typeof key === "string" ? key : "");
      const val = opts.formatVal ? opts.formatVal(getVal(d), d) : getVal(d);
      showTooltip(event, `<b>${fmt(parseDate(d.date))}</b><br>${label}: <b>${val || 0}</b>`);
    })
    .on("mouseout", hideTooltip);
}

// Render charts
window.addEventListener("DOMContentLoaded", () => {
  lineChart("chart-sleep", TREND_DATA, [
    { key: "sleep_score",     label: "Sleep",     color: "#CC2200" },
    { key: "readiness_score", label: "Readiness", color: "#FFCC00" },
  ], { yMin: 0, yMax: 100 });

  dualAxisChart("chart-hrv-raw", TREND_DATA,
    { key: "resting_heart_rate", label: "Resting HR (bpm)", color: "#CC2200" },
    { key: "average_hrv",        label: "HRV (ms)",         color: "#1155BB" }
  );
  lineChart("chart-hrv-score", TREND_DATA, [
    { key: "hrv_balance_score",      label: "HRV Balance",  color: "#1155BB" },
    { key: "resting_heart_rate_score", label: "HR Score",   color: "#CC2200" },
  ], { yMin: 0, yMax: 100 });

  barChart("chart-steps", TREND_DATA, "preferred_steps", d => {
    const v = d.preferred_steps || 0;
    return v >= 8000 ? "#FFCC00" : v >= 5000 ? "#FF6600" : "#CC2200";
  }, { label: "Steps", tickFormat: v => v >= 1000 ? `${(v/1000).toFixed(1)}k` : v,
       formatVal: v => v ? `${v.toLocaleString()}` : "0" });

  lineChart("chart-mood", TREND_DATA, [
    { key: "mood_score", label: "Mood", color: "#FFCC00" },
  ], { yMin: 1, yMax: 5 });

  barChart("chart-workouts", TREND_DATA, d => (d.workout_count || 0) + (d.oura_workout_count || 0),
    d => {
      const strength = (d.workout_count || 0) > 0;
      const oura = (d.oura_workout_count || 0) > 0;
      if (strength && oura) return "#AA44FF";
      if (strength) return "#1155BB";
      if (oura) return "#22AA66";
      return "#CCCCCC";
    },
    { label: "Workouts", height: 90,
      formatVal: (v, d) => {
        if (!v) return "rest day";
        const parts = [];
        if ((d.workout_count || 0) > 0) parts.push(`${d.workout_count} strength`);
        if ((d.oura_workout_count || 0) > 0) parts.push(`${d.oura_workout_count} oura`);
        return parts.join(" + ");
      }
    });
});

// ===== SESSION USAGE TRACKER =====
let sessionIn = 0, sessionOut = 0, sessionCost = 0.0;
function updateSessionUsage(usage) {
  if (!usage) return;
  sessionIn   += usage.input_tokens  || 0;
  sessionOut  += usage.output_tokens || 0;
  sessionCost += usage.cost          || 0;
  const el    = document.getElementById("session-usage");
  const stats = document.getElementById("session-stats");
  if (el && stats) {
    el.style.display = "block";
    stats.textContent =
      `${sessionIn.toLocaleString()} in / ${sessionOut.toLocaleString()} out tokens · $${sessionCost.toFixed(4)}`;
  }
}

// ===== CHAT =====
const chatMessages = document.getElementById("chat-messages");
const chatInput   = document.getElementById("chat-input");
const chatSend    = document.getElementById("chat-send");

const WELCOME_MSG = "Hi Joshua! I have your full health context for today. Ask me anything — workout timing, sleep, recovery, what to eat before a run, or how your trends are looking.";
const HISTORY_KEY  = "health_dashboard_history";

// ── History helpers ──────────────────────────────────────────────────────────
function loadHistory() {
  try { return JSON.parse(localStorage.getItem(HISTORY_KEY) || "[]"); }
  catch { return []; }
}
function saveHistory(h) {
  try { localStorage.setItem(HISTORY_KEY, JSON.stringify(h)); }
  catch {}
}

function renderHistory() {
  const list = document.getElementById("chat-history-list");
  if (!list) return;
  const h = loadHistory();
  if (h.length === 0) {
    list.innerHTML = '<div class="no-data">No chat history yet.</div>';
    return;
  }
  list.innerHTML = "";
  // Show newest first
  [...h].reverse().forEach((entry, i) => {
    const idx = h.length - 1 - i;
    const ts  = new Date(entry.ts).toLocaleString([], { month:"short", day:"numeric", hour:"2-digit", minute:"2-digit" });
    const item = document.createElement("div");
    item.style.cssText = "border:1px solid var(--border);border-radius:8px;margin-bottom:0.5rem;overflow:hidden;";
    item.innerHTML = `
      <button onclick="toggleHistory(this)" data-idx="${idx}"
        style="width:100%;text-align:left;background:var(--card-bg);border:none;padding:0.75rem 1rem;cursor:pointer;display:flex;justify-content:space-between;align-items:center;gap:1rem;">
        <span style="color:var(--text);font-size:0.88rem;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${escHtml(entry.q)}</span>
        <span style="color:var(--text-dim);font-size:0.75rem;white-space:nowrap;">${ts} <span class="hist-arrow">▾</span></span>
      </button>
      <div class="hist-body" style="display:none;padding:1rem;background:rgba(0,0,0,0.15);border-top:1px solid var(--border);">
        <div style="margin-bottom:0.75rem;">
          <div style="font-size:0.72rem;text-transform:uppercase;letter-spacing:0.06em;color:var(--text-dim);margin-bottom:0.3rem;">You</div>
          <div style="color:var(--text);font-size:0.88rem;white-space:pre-wrap;">${escHtml(entry.q)}</div>
        </div>
        <div>
          <div style="font-size:0.72rem;text-transform:uppercase;letter-spacing:0.06em;color:var(--text-dim);margin-bottom:0.3rem;">Claude</div>
          <div style="color:var(--text);font-size:0.88rem;white-space:pre-wrap;">${escHtml(entry.a)}</div>
        </div>
      </div>`;
    list.appendChild(item);
  });
}

function toggleHistory(btn) {
  const body  = btn.nextElementSibling;
  const arrow = btn.querySelector(".hist-arrow");
  const open  = body.style.display !== "none";
  body.style.display  = open ? "none" : "block";
  arrow.textContent   = open ? "▾" : "▴";
}

function clearHistory() {
  localStorage.removeItem(HISTORY_KEY);
  renderHistory();
}

function escHtml(s) {
  return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");
}

// ── Chat ─────────────────────────────────────────────────────────────────────
function appendMsg(role, text) {
  const div = document.createElement("div");
  div.className = `msg ${role}`;
  div.textContent = text;
  chatMessages.appendChild(div);
  chatMessages.scrollTop = chatMessages.scrollHeight;
  return div;
}

// Chat always starts fresh on load
appendMsg("assistant", WELCOME_MSG);
renderHistory();

async function sendMessage(event) {
  event.preventDefault();
  const text = chatInput.value.trim();
  if (!text) return;

  chatInput.value = "";
  chatSend.disabled = true;
  appendMsg("user", text);

  const typing = appendMsg("assistant typing", "…");

  try {
    const resp = await fetch("/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message: text, context: HEALTH_CONTEXT }),
    });

    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();

    typing.className = "msg assistant";
    typing.textContent = data.reply;

    // Save Q&A pair to history
    const h = loadHistory();
    h.push({ q: text, a: data.reply, ts: new Date().toISOString() });
    saveHistory(h);
    renderHistory();

    updateSessionUsage(data.usage);
  } catch (err) {
    typing.className = "msg assistant";
    typing.textContent = "Sorry, couldn't reach the chat server. Make sure chat_server.py is running.";
  }

  chatSend.disabled = false;
  chatInput.focus();
}
</script>

</body>
</html>"""


# ---------------------------------------------------------------------------
# Score helpers (used in Jinja2 template)
# ---------------------------------------------------------------------------

def score_class(v):
    if v is None:
        return "score-na"
    if v >= 75:
        return "score-good"
    if v >= 55:
        return "score-warn"
    return "score-bad"


def mood_class(v):
    if v is None:
        return "score-na"
    if v >= 4:
        return "score-good"
    if v >= 3:
        return "score-warn"
    return "score-bad"


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def build_health_context(today_row: dict, trend_rows: list, personal_context: dict) -> dict:
    """Build the full health context dict sent with every chat message."""
    if not today_row:
        return {"error": "No data for today", "personal": personal_context}

    from datetime import date as _date, timedelta as _td
    yesterday_str = (_date.today() - _td(days=1)).isoformat()
    yesterday_row = next((r for r in trend_rows if str(r.get("date")) == yesterday_str), {})

    recent = [r for r in trend_rows[-7:] if r.get("has_oura_data")]
    avg_sleep = round(sum(r["sleep_score"] for r in recent if r["sleep_score"]) / max(len(recent), 1), 1) if recent else None
    avg_readiness = round(sum(r["readiness_score"] for r in recent if r["readiness_score"]) / max(len(recent), 1), 1) if recent else None
    avg_hrv = round(sum(r["hrv_balance_score"] for r in recent if r["hrv_balance_score"]) / max(len(recent), 1), 1) if recent else None
    avg_rhr = round(sum(r["resting_heart_rate"] for r in recent if r.get("resting_heart_rate")) / max(sum(1 for r in recent if r.get("resting_heart_rate")), 1), 1) if recent else None
    avg_hrv_ms = round(sum(r["average_hrv"] for r in recent if r.get("average_hrv")) / max(sum(1 for r in recent if r.get("average_hrv")), 1), 1) if recent else None

    return {
        "today": {
            "date": str(today_row.get("date", "")),
            "sleep_score": today_row.get("sleep_score"),
            "deep_sleep_score": today_row.get("deep_sleep_score"),
            "rem_sleep_score": today_row.get("rem_sleep_score"),
            "readiness_score": today_row.get("readiness_score"),
            "hrv_balance_score": today_row.get("hrv_balance_score"),
            "recovery_index_score": today_row.get("recovery_index_score"),
            "temperature_deviation": today_row.get("temperature_deviation"),
            "resting_heart_rate": today_row.get("resting_heart_rate"),
            "average_hrv": today_row.get("average_hrv"),
            # Activity and mood lag by one day — use yesterday's row
            "activity_score": yesterday_row.get("activity_score"),
            "preferred_steps": yesterday_row.get("preferred_steps"),
            "active_calories": yesterday_row.get("active_calories"),
            "mood": yesterday_row.get("mood"),
            "mood_score": yesterday_row.get("mood_score"),
            "daylio_activities": yesterday_row.get("daylio_activities"),
            "mood_state": (
                [p.strip() for p in yesterday_row["daylio_activities"].split("|")][0]
                if yesterday_row.get("daylio_activities") else None
            ),
            "mood_tags": (
                [p.strip() for p in yesterday_row["daylio_activities"].split("|")][1:]
                if yesterday_row.get("daylio_activities") and "|" in yesterday_row["daylio_activities"] else []
            ),
            "workout_count": yesterday_row.get("workout_count"),
            "workout_names": yesterday_row.get("workout_names"),
            "total_workout_minutes": yesterday_row.get("total_workout_minutes"),
            "has_oura_data": today_row.get("has_oura_data"),
            "has_workout": yesterday_row.get("has_workout"),
        },
        "weather": {
            "desc": today_row.get("weather_desc"),
            "temp_max_f": today_row.get("temp_max_f"),
            "temp_min_f": today_row.get("temp_min_f"),
            "morning_temp_f": today_row.get("morning_temp_f"),
            "afternoon_temp_f": today_row.get("afternoon_temp_f"),
            "evening_temp_f": today_row.get("evening_temp_f"),
            "morning_precip_prob": today_row.get("morning_precip_prob"),
            "afternoon_precip_prob": today_row.get("afternoon_precip_prob"),
            "evening_precip_prob": today_row.get("evening_precip_prob"),
            "likely_rain": today_row.get("likely_rain"),
            "better_in_morning": today_row.get("better_in_morning"),
            "hot_day": today_row.get("hot_day"),
            "cold_day": today_row.get("cold_day"),
            "precip_prob_max": today_row.get("precip_prob_max"),
        },
        "trends_7day": {
            "avg_sleep_score": avg_sleep,
            "avg_readiness_score": avg_readiness,
            "avg_hrv_balance_score": avg_hrv,
            "avg_resting_heart_rate_bpm": avg_rhr,
            "avg_hrv_ms": avg_hrv_ms,
            "workouts": sum(1 for r in trend_rows[-7:] if r.get("has_workout")),
        },
        "personal": personal_context,
    }


def render_dashboard(today_row: dict, trend_rows: list, insights: dict, personal_context: dict, llm_stats: dict = None) -> str:
    from datetime import datetime

    # Convert rows to plain dicts (RealDictRow → dict, dates/Decimals → JSON-safe)
    def clean_row(r):
        if r is None:
            return {}
        out = {}
        for k, v in dict(r).items():
            if hasattr(v, "isoformat"):
                out[k] = v.isoformat()
            elif isinstance(v, Decimal):
                out[k] = float(v)
            else:
                out[k] = v
        # Parse daylio_activities into mood_state + mood_tags
        acts = out.get("daylio_activities") or ""
        parts = [p.strip() for p in acts.split("|")] if acts else []
        out["mood_state"] = parts[0] if parts else None
        out["mood_tags"]  = parts[1:] if len(parts) > 1 else []
        return out

    today_clean = clean_row(today_row)
    trends_clean = [clean_row(r) for r in trend_rows]

    # Mood and activity are logged with a lag — always show yesterday's entry
    from datetime import date as _date, timedelta as _td
    yesterday_str = (_date.today() - _td(days=1)).isoformat()
    yesterday_row = next((r for r in trends_clean if r.get("date") == yesterday_str), {})

    has_yesterday_mood = bool(yesterday_row.get("has_mood_log"))
    for field in ("mood", "mood_score", "daylio_activities", "mood_state"):
        today_clean[field] = yesterday_row.get(field) if has_yesterday_mood else None
    today_clean["mood_tags"] = yesterday_row.get("mood_tags") or [] if has_yesterday_mood else []

    has_yesterday_activity = bool(yesterday_row.get("has_oura_data") or yesterday_row.get("activity_score"))
    for field in ("activity_score", "preferred_steps", "active_calories", "workout_count",
                  "total_workout_minutes", "workout_names", "has_workout"):
        today_clean[field] = yesterday_row.get(field) if has_yesterday_activity else None

    health_context = build_health_context(today_row, trend_rows, personal_context)

    if llm_stats is None:
        from datetime import date as _date
        llm_stats = {
            "month": _date.today().strftime("%B %Y"),
            "by_model": [],
            "totals": {"total_calls": 0, "total_input_tokens": 0, "total_output_tokens": 0, "total_cost": 0.0},
        }

    template = Template(DASHBOARD_TEMPLATE)
    template.globals["score_class"] = score_class
    template.globals["mood_class"] = mood_class

    trend_dates = [r["date"] for r in trends_clean if r.get("date")]

    return template.render(
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
        today=today_clean,
        insights=insights,
        trend_json=dumps(trends_clean),
        today_json=dumps(today_clean),
        health_context_json=dumps(health_context),
        llm_stats=llm_stats,
        trend_dates=trend_dates,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate health dashboard")
    parser.add_argument("--dry-run", action="store_true", help="Skip Claude API call")
    args = parser.parse_args()

    llm_logging.ensure_table()

    log.info("Connecting to Postgres…")
    conn = psycopg2.connect(DB_DSN)

    log.info("Fetching health data…")
    today_row, trend_rows = fetch_data(conn)
    conn.close()

    if today_row:
        log.info("Got today's row: sleep=%s, readiness=%s, hrv=%s",
                 today_row.get("sleep_score"), today_row.get("readiness_score"),
                 today_row.get("hrv_balance_score"))
    else:
        log.warning("No row for today in daily_summary yet")

    log.info("Got %d trend rows", len(trend_rows))

    personal_context = json.loads(PERSONAL_CONTEXT_PATH.read_text())

    log.info("Getting AI insights%s…", " (dry-run)" if args.dry_run else "")
    insights = get_insights(today_row or {}, list(trend_rows), personal_context, dry_run=args.dry_run)

    log.info("Fetching LLM usage stats…")
    llm_stats = llm_logging.get_monthly_stats()
    log.info("Monthly LLM cost so far: $%.4f (%d calls)", llm_stats["totals"]["total_cost"], llm_stats["totals"]["total_calls"])

    log.info("Rendering dashboard…")
    html = render_dashboard(today_row, list(trend_rows), insights, personal_context, llm_stats)

    OUTPUT_PATH.write_text(html, encoding="utf-8")
    log.info("Dashboard written to %s (%d bytes)", OUTPUT_PATH, len(html))


if __name__ == "__main__":
    main()
