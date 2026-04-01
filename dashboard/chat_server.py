"""
chat_server.py — Flask server for the health dashboard

Serves the static dashboard.html and handles chat API calls.
The /chat endpoint uses Claude tool use + the rag.py retrieval layer so
Claude can query historical health data before answering.

Usage:
    python3 chat_server.py              # runs on port 8080
    python3 chat_server.py --port 8080

Endpoints:
    GET  /            → serves output/dashboard.html
    POST /chat        → RAG chat via Claude Sonnet + DB tools
    GET  /health      → simple health check
"""

import argparse
import json
import logging
import os
from pathlib import Path

import anthropic
from dotenv import load_dotenv
from flask import Flask, jsonify, request, send_file

import context_notes
import llm_logging
import rag

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).parent
load_dotenv(SCRIPT_DIR.parent / "ingestion" / ".env")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
DASHBOARD_PATH    = SCRIPT_DIR / "output" / "dashboard.html"
CHAT_MODEL        = "claude-sonnet-4-6"
MAX_TOOL_ROUNDS   = 5   # safety cap on agentic loop iterations

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------

app = Flask(__name__)

SYSTEM_PROMPT = """You are a personal health advisor for {name}. You have full access to their health database and can query any historical data you need to answer questions accurately.

Personal context:
{personal_context}
{active_notes}
You have four database tools available:
- get_period_stats: aggregate stats for a metric over any date range
- get_daily_records: day-by-day data for trend and correlation questions
- get_top_days: best or worst days for any metric
- get_workout_history: comprehensive workout analysis

Always query the database when the question involves historical data, trends, comparisons, or anything beyond today. Today's metrics are pre-loaded in the context — use tools for anything historical.

Be direct and practical. Acknowledge the reality of new parenthood where relevant. No markdown headers. Keep responses conversational and to the point."""


@app.route("/")
def dashboard():
    if not DASHBOARD_PATH.exists():
        return (
            "<h2>Dashboard not generated yet.</h2>"
            "<p>Run: <code>python3 generate_dashboard.py</code></p>",
            404,
        )
    return send_file(DASHBOARD_PATH)


@app.route("/health")
def health_check():
    return jsonify({"status": "ok", "dashboard_exists": DASHBOARD_PATH.exists()})


DETECTION_MODEL = "claude-haiku-4-5"
DETECTION_PROMPT = """Does this message mention a temporary health condition the person is currently experiencing?
Examples: jetlag, cold, flu, injury, illness, travel fatigue, stress, surgery recovery, medication side effects.
Ignore past conditions that are clearly resolved or hypothetical.

If yes, respond with JSON only:
{"detected": true, "note": "<concise third-person description>", "expires_days": <integer>}

Recovery guidelines for expires_days:
- jetlag: 1 day per timezone hour crossed, typically 5-10 days
- mild cold: 7 days
- flu: 10-14 days
- travel fatigue (no jetlag): 2-3 days
- minor injury/strain: 14 days
- stress/burnout: 7-14 days

If no temporary condition is mentioned, respond with:
{"detected": false}

Respond with JSON only. No other text."""


RUN_DETECTION_PROMPT = """Does this message mention that the person is logging one or more completed runs?
Only detect runs being reported as done — not questions about past runs or future plans.

If yes, extract ALL runs and respond with JSON only:
{{"detected": true, "runs": [{{"distance_miles": <float>, "duration_seconds": <integer>, "date": "<YYYY-MM-DD or null>", "notes": "<string or null>"}}]}}

Conversion rules:
- Distance: convert km to miles (1 km = 0.621371 miles), default to miles if unit unclear
- Duration: convert any format to total seconds (e.g. "28:30" → 1710, "1h 5m" → 3900)
- Date: use null if not specified (caller will default to today); interpret relative terms like "yesterday" using today's date
- notes: capture any qualitative details (route, how it felt, conditions)

Today's date: {today}

If no runs are being reported, respond with:
{{"detected": false}}

Respond with JSON only. No other text."""


def _detect_and_log_run(message: str) -> list[dict] | None:
    """Run a cheap Haiku pre-pass to detect and save reported runs. Returns list of saved runs or None."""
    try:
        today = rag._today_str()
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        resp = client.messages.create(
            model=DETECTION_MODEL,
            max_tokens=300,
            messages=[{"role": "user", "content": message}],
            system=RUN_DETECTION_PROMPT.format(today=today),
        )
        raw = next((b.text for b in resp.content if b.type == "text"), "").strip()
        result = json.loads(raw)
        llm_logging.log_llm_call("run_detection", DETECTION_MODEL, resp.usage.input_tokens, resp.usage.output_tokens)
        if result.get("detected"):
            saved_runs = []
            for run in result.get("runs", []):
                saved = rag.log_run(
                    distance_miles=run["distance_miles"],
                    duration_seconds=run["duration_seconds"],
                    date=run.get("date"),
                    notes=run.get("notes"),
                )
                log.info("Auto-logged run via Haiku: %s", saved)
                saved_runs.append(saved)
            return saved_runs if saved_runs else None
    except Exception as e:
        log.warning("Run detection failed (non-fatal): %s", e)
    return None


def _detect_and_save_condition(message: str) -> None:
    """Run a cheap Haiku pre-pass to detect and persist any temporary health condition."""
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        resp = client.messages.create(
            model=DETECTION_MODEL,
            max_tokens=120,
            messages=[{"role": "user", "content": message}],
            system=DETECTION_PROMPT,
        )
        raw = next((b.text for b in resp.content if b.type == "text"), "").strip()
        result = json.loads(raw)
        if result.get("detected"):
            saved = context_notes.save_note(result["note"], result["expires_days"])
            log.info("Auto-saved context note (expires %s): %s", saved.get("expires"), saved.get("note"))
        llm_logging.log_llm_call("condition_detection", DETECTION_MODEL, resp.usage.input_tokens, resp.usage.output_tokens)
    except Exception as e:
        log.warning("Condition detection failed (non-fatal): %s", e)


@app.route("/chat", methods=["POST"])
def chat():
    if not ANTHROPIC_API_KEY:
        return jsonify({"reply": "ANTHROPIC_API_KEY not configured."}), 500

    data = request.get_json(silent=True) or {}
    user_message = (data.get("message") or "").strip()
    health_context = data.get("context") or {}

    if not user_message:
        return jsonify({"error": "No message provided"}), 400

    personal = health_context.get("personal", {})
    name = personal.get("name", "Joshua")

    # Pre-pass: detect and save any temporary health condition or run before main chat.
    # Done separately so it never gets dropped in favour of answering the question.
    _detect_and_save_condition(user_message)
    saved_run = _detect_and_log_run(user_message)

    active = context_notes.get_active_notes()
    notes_block = ("\n" + context_notes.format_for_prompt(active) + "\n") if active else ""

    system = SYSTEM_PROMPT.format(
        name=name,
        personal_context=json.dumps(personal, indent=2),
        active_notes=notes_block,
    )

    # First user message: today's context + the question
    context_summary = _format_context(health_context)
    if saved_run:
        valid = [r for r in saved_run if not r.get("error")]
        if valid:
            entries = "; ".join(
                f"{r['distance_miles']} mi in {r['duration']} ({r['pace_min_per_mile']} min/mile) on {r['date']}"
                for r in valid
            )
            run_note = f"\n\n[System: {len(valid)} run(s) saved to database — {entries}. Confirm naturally, no need to re-save.]"
        else:
            run_note = ""
    else:
        run_note = ""
    first_message = f"{context_summary}\n\nQuestion: {user_message}{run_note}"

    messages = [{"role": "user", "content": first_message}]

    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        total_in = total_out = 0

        # Agentic tool-use loop
        for round_num in range(MAX_TOOL_ROUNDS):
            response = client.messages.create(
                model=CHAT_MODEL,
                max_tokens=1024,
                system=system,
                tools=rag.TOOL_DEFINITIONS,
                messages=messages,
            )
            total_in  += response.usage.input_tokens
            total_out += response.usage.output_tokens

            if response.stop_reason == "end_turn":
                # Final answer — extract text
                reply = next(
                    (b.text for b in response.content if b.type == "text"), ""
                ).strip()
                log.info(
                    "Chat (%d rounds): %d in / %d out tokens | %s",
                    round_num + 1, total_in, total_out, user_message[:60],
                )
                _, _, total_cost = llm_logging.log_llm_call(
                    "chat", CHAT_MODEL, total_in, total_out
                )
                llm_logging.log_chat_exchange(
                    user_message, reply, total_in, total_out, total_cost
                )
                return jsonify({
                    "reply": reply,
                    "usage": {
                        "input_tokens":  total_in,
                        "output_tokens": total_out,
                        "cost":          round(total_cost, 6),
                    },
                })

            if response.stop_reason != "tool_use":
                break

            # Execute all tool calls and collect results
            messages.append({"role": "assistant", "content": response.content})
            tool_results = []
            for block in response.content:
                if block.type != "tool_use":
                    continue
                log.info("Tool call: %s(%s)", block.name, json.dumps(block.input)[:120])
                result = rag.execute_tool(block.name, block.input)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": json.dumps(result),
                })
                log.info("Tool result: %d chars", len(tool_results[-1]["content"]))

            messages.append({"role": "user", "content": tool_results})

        # Fallback if loop exits without end_turn
        reply = next(
            (b.text for b in response.content if b.type == "text"), "Sorry, I couldn't complete that."
        ).strip()
        _, _, total_cost = llm_logging.log_llm_call(
            "chat", CHAT_MODEL, total_in, total_out
        )
        llm_logging.log_chat_exchange(
            user_message, reply, total_in, total_out, total_cost
        )
        return jsonify({
            "reply": reply,
            "usage": {
                "input_tokens":  total_in,
                "output_tokens": total_out,
                "cost":          round(total_cost, 6),
            },
        })

    except anthropic.APIError as e:
        log.error("Claude API error: %s", e)
        return jsonify({"reply": f"API error: {e}"}), 500


def _format_context(ctx: dict) -> str:
    """Format today's health snapshot into a string for the first message."""
    lines = ["=== TODAY'S HEALTH SNAPSHOT ==="]

    today = ctx.get("today", {})
    if today:
        lines.append(f"Date: {today.get('date', 'today')}")
        if today.get("sleep_score") is not None:
            lines.append(f"Sleep: {today.get('sleep_score')} (deep: {today.get('deep_sleep_score')}, REM: {today.get('rem_sleep_score')})")
        if today.get("readiness_score") is not None:
            lines.append(f"Readiness: {today.get('readiness_score')} | HRV balance: {today.get('hrv_balance_score')}")
        if today.get("preferred_steps") is not None:
            lines.append(f"Steps: {today.get('preferred_steps'):,}")
        if today.get("mood"):
            lines.append(f"Mood: {today.get('mood')} ({today.get('mood_score')}/5)")
        if today.get("has_workout"):
            lines.append(f"Workout: {today.get('workout_names')} ({today.get('total_workout_minutes', 0):.0f} min)")

    trends = ctx.get("trends_7day", {})
    if trends:
        lines.append(f"7-day avgs: sleep={trends.get('avg_sleep_score')}, readiness={trends.get('avg_readiness_score')}, HRV={trends.get('avg_hrv_balance')}")
        lines.append(f"Workouts this week: {trends.get('workouts', 0)}")

    weather = ctx.get("weather", {})
    if weather and weather.get("desc"):
        lines.append(f"Weather: {weather.get('desc')} | {weather.get('temp_min_f')}–{weather.get('temp_max_f')}°F")
        if weather.get("better_in_morning"):
            lines.append("Flag: better to exercise in the morning")
        if weather.get("hot_day"):
            lines.append("Flag: hot day")
        if weather.get("cold_day"):
            lines.append("Flag: cold day")

    lines.append("\nFor historical questions, use the database tools.")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Health dashboard chat server")
    parser.add_argument("--port", type=int, default=8080, help="Port to listen on")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    args = parser.parse_args()

    if not ANTHROPIC_API_KEY:
        log.warning("ANTHROPIC_API_KEY not set — chat endpoint will return errors")

    llm_logging.ensure_table()

    log.info("Starting chat server on %s:%d", args.host, args.port)
    log.info("Dashboard: %s", DASHBOARD_PATH)
    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
