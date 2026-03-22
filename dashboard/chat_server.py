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

    system = SYSTEM_PROMPT.format(
        name=name,
        personal_context=json.dumps(personal, indent=2),
    )

    # First user message: today's context + the question
    context_summary = _format_context(health_context)
    first_message = f"{context_summary}\n\nQuestion: {user_message}"

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
