"""
llm_logging.py — LLM call logging and cost tracking

Writes every Claude API call to the llm_calls Postgres table and
exposes a monthly stats query used by the dashboard.

Pricing (USD per token, 2026):
  claude-haiku-4-5:  $1.00 / 1M input,  $5.00 / 1M output
  claude-sonnet-4-6: $3.00 / 1M input, $15.00 / 1M output
"""

import logging
import os

import psycopg2
import psycopg2.extras

log = logging.getLogger(__name__)

_DSN_DEFAULT = "postgresql://jhumms:health2026@localhost/health_db"

MODEL_PRICING = {
    "claude-haiku-4-5":  {"input": 1.00 / 1_000_000, "output":  5.00 / 1_000_000},
    "claude-sonnet-4-6": {"input": 3.00 / 1_000_000, "output": 15.00 / 1_000_000},
}


def _dsn() -> str:
    return os.getenv("DATABASE_URL", _DSN_DEFAULT)


def ensure_table() -> None:
    """Create llm_calls and chat_history tables if they don't exist. Safe to call repeatedly."""
    sqls = [
        """
        CREATE TABLE IF NOT EXISTS llm_calls (
            id              SERIAL PRIMARY KEY,
            called_at       TIMESTAMP DEFAULT NOW(),
            source          VARCHAR(50),
            model           VARCHAR(100),
            input_tokens    INTEGER,
            output_tokens   INTEGER,
            input_cost_usd  NUMERIC(12, 8),
            output_cost_usd NUMERIC(12, 8),
            total_cost_usd  NUMERIC(12, 8)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS chat_history (
            id            SERIAL PRIMARY KEY,
            asked_at      TIMESTAMP DEFAULT NOW(),
            question      TEXT,
            answer        TEXT,
            input_tokens  INTEGER,
            output_tokens INTEGER,
            cost_usd      NUMERIC(12, 8)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS daily_insights (
            id              SERIAL PRIMARY KEY,
            generated_at    TIMESTAMP DEFAULT NOW(),
            insight_date    DATE UNIQUE,
            status_summary  TEXT,
            recommendations TEXT,
            watchout        TEXT,
            input_tokens    INTEGER,
            output_tokens   INTEGER,
            cost_usd        NUMERIC(12, 8)
        )
        """,
    ]
    try:
        conn = psycopg2.connect(_dsn())
        with conn.cursor() as cur:
            for sql in sqls:
                cur.execute(sql)
        conn.commit()
        conn.close()
    except Exception as e:
        log.error("ensure_table failed: %s", e)


def log_llm_call(
    source: str, model: str, input_tokens: int, output_tokens: int
) -> tuple[float, float, float]:
    """
    Insert one LLM call record and return (input_cost, output_cost, total_cost) in USD.
    Fails silently on DB error so it never breaks the main pipeline.
    """
    pricing = MODEL_PRICING.get(model, {"input": 0.0, "output": 0.0})
    input_cost  = round(input_tokens  * pricing["input"],  8)
    output_cost = round(output_tokens * pricing["output"], 8)
    total_cost  = round(input_cost + output_cost, 8)

    try:
        conn = psycopg2.connect(_dsn())
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO llm_calls
                    (source, model, input_tokens, output_tokens,
                     input_cost_usd, output_cost_usd, total_cost_usd)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (source, model, input_tokens, output_tokens,
                 input_cost, output_cost, total_cost),
            )
        conn.commit()
        conn.close()
    except Exception as e:
        log.error("log_llm_call failed: %s", e)

    log.info(
        "LLM %-10s %-22s in=%6d out=%5d  $%.5f",
        source, model, input_tokens, output_tokens, total_cost,
    )
    return input_cost, output_cost, total_cost


def get_monthly_stats() -> dict:
    """Return current-month LLM usage grouped by model plus overall totals."""
    sql_by_model = """
        SELECT
            model,
            COUNT(*)            AS calls,
            SUM(input_tokens)   AS input_tokens,
            SUM(output_tokens)  AS output_tokens,
            SUM(total_cost_usd) AS total_cost
        FROM llm_calls
        WHERE DATE_TRUNC('month', called_at) = DATE_TRUNC('month', NOW())
        GROUP BY model
        ORDER BY total_cost DESC
    """
    sql_totals = """
        SELECT
            COUNT(*)            AS total_calls,
            SUM(input_tokens)   AS total_input_tokens,
            SUM(output_tokens)  AS total_output_tokens,
            SUM(total_cost_usd) AS total_cost
        FROM llm_calls
        WHERE DATE_TRUNC('month', called_at) = DATE_TRUNC('month', NOW())
    """
    from datetime import date
    empty = {
        "month": date.today().strftime("%B %Y"),
        "by_model": [],
        "totals": {
            "total_calls": 0,
            "total_input_tokens": 0,
            "total_output_tokens": 0,
            "total_cost": 0.0,
        },
    }
    try:
        conn = psycopg2.connect(_dsn())
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql_by_model)
            by_model = [dict(r) for r in cur.fetchall()]
            cur.execute(sql_totals)
            totals = dict(cur.fetchone())
        conn.close()

        for r in by_model:
            r["total_cost"]    = float(r.get("total_cost")    or 0)
            r["input_tokens"]  = int(r.get("input_tokens")    or 0)
            r["output_tokens"] = int(r.get("output_tokens")   or 0)
            r["calls"]         = int(r.get("calls")           or 0)

        totals = {
            "total_calls":         int(totals.get("total_calls")         or 0),
            "total_input_tokens":  int(totals.get("total_input_tokens")  or 0),
            "total_output_tokens": int(totals.get("total_output_tokens") or 0),
            "total_cost":          float(totals.get("total_cost")        or 0),
        }
        return {"month": date.today().strftime("%B %Y"), "by_model": by_model, "totals": totals}
    except Exception as e:
        log.error("get_monthly_stats failed: %s", e)
        return empty


def log_daily_insights(
    insight_date: str, insights: dict, input_tokens: int, output_tokens: int, total_cost: float
) -> None:
    """Upsert today's Haiku summary into daily_insights. Fails silently on DB error."""
    import json as _json
    recs = insights.get("recommendations") or []
    try:
        conn = psycopg2.connect(_dsn())
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO daily_insights
                    (insight_date, status_summary, recommendations, watchout,
                     input_tokens, output_tokens, cost_usd)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (insight_date) DO UPDATE
                    SET status_summary  = EXCLUDED.status_summary,
                        recommendations = EXCLUDED.recommendations,
                        watchout        = EXCLUDED.watchout,
                        input_tokens    = EXCLUDED.input_tokens,
                        output_tokens   = EXCLUDED.output_tokens,
                        cost_usd        = EXCLUDED.cost_usd,
                        generated_at    = NOW()
                """,
                (
                    insight_date,
                    insights.get("status_summary", ""),
                    _json.dumps(recs),
                    insights.get("watchout", ""),
                    input_tokens,
                    output_tokens,
                    round(total_cost, 8),
                ),
            )
        conn.commit()
        conn.close()
    except Exception as e:
        log.error("log_daily_insights failed: %s", e)


def log_chat_exchange(
    question: str, answer: str, input_tokens: int, output_tokens: int, total_cost: float
) -> None:
    """Insert one chat Q&A pair into chat_history. Fails silently on DB error."""
    try:
        conn = psycopg2.connect(_dsn())
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO chat_history
                    (question, answer, input_tokens, output_tokens, cost_usd)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (question, answer, input_tokens, output_tokens, round(total_cost, 8)),
            )
        conn.commit()
        conn.close()
    except Exception as e:
        log.error("log_chat_exchange failed: %s", e)
