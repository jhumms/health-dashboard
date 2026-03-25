# Health Dashboard

A personal health intelligence pipeline running on a Raspberry Pi. Ingests data from Oura Ring, Garmin, Daylio, and manual workout logs, transforms it with dbt, and generates a nightly static dashboard with AI-powered insights and a conversational health assistant.

---

## Architecture

```
Data Sources → Ingestion (Python) → PostgreSQL (raw) → dbt → staging_marts → Dashboard
                                                                                    ↑
                                                                          Claude Haiku (insights)
                                                                          Claude Sonnet (chat)
```

**Orchestration:** Apache Airflow (LocalExecutor on Raspberry Pi)
**Database:** PostgreSQL
**Transformation:** dbt
**AI:** Anthropic Claude (Haiku for nightly insights, Sonnet for chat)
**Frontend:** Static HTML + D3.js charts, served from the Pi

---

## Data Sources

| Source | What it captures |
|--------|-----------------|
| **Oura Ring** | Sleep scores, HRV, readiness, activity |
| **Garmin** | Daily steps |
| **Daylio** | Mood, mood state, activity tags |
| **Daily Strength** | Workout sessions, exercises, duration |
| **Open-Meteo** | Weather (temp, conditions, sunrise/sunset) |

---

## Project Structure

```
health_workflow/
├── ingestion/              # Python scripts to pull data into Postgres raw zone
│   ├── ingest_oura.py
│   ├── ingest_garmin.py
│   ├── ingest_daylio.py
│   ├── ingest_daily_strength.py
│   ├── ingest_weather.py
│   └── .env.example
├── health_dbt/             # dbt project — staging models + daily_summary mart
│   └── models/
│       ├── staging/        # One model per raw source
│       └── marts/          # daily_summary: one row per day, all sources joined
├── dashboard/
│   ├── generate_dashboard.py   # Renders static dashboard.html nightly
│   ├── chat_server.py          # Flask server for Claude Sonnet chat
│   ├── llm_logging.py          # Shared LLM cost tracking utility
│   ├── rag.py                  # RAG tool for chat (queries Postgres)
│   ├── context_notes.py        # Short-term health context with auto-expiry
│   ├── context_notes.json      # Active temporary notes (gitignored)
│   └── personal_context.json.example
└── airflow/
    ├── dags/health_nightly.py  # Nightly pipeline DAG
    └── airflow.cfg.example
```

---

## Setup

### 1. Prerequisites

- Python 3.10+
- PostgreSQL
- dbt (`pip install dbt-postgres`)
- Apache Airflow (`pip install apache-airflow`)
- Anthropic API key

### 2. Environment

Copy and fill in the env file for ingestion:

```bash
cp ingestion/.env.example ingestion/.env
```

Required variables:

```env
DATABASE_URL=postgresql://<user>:<password>@localhost/health_db
ANTHROPIC_API_KEY=sk-...
OURA_ACCESS_TOKEN=...
OPENWEATHERMAP_API_KEY=...   # or uses Open-Meteo (no key needed)
```

### 3. Database

```bash
createdb health_db
psql health_db -c "CREATE SCHEMA raw; CREATE SCHEMA staging_marts;"
```

### 4. dbt

```bash
cd health_dbt
dbt deps
dbt run
```

### 5. Airflow

```bash
cp airflow/airflow.cfg.example airflow/airflow.cfg
# Edit airflow.cfg with your DB credentials and secret key
airflow db init
airflow webserver &
airflow scheduler &
```

Enable the `health_nightly` DAG in the Airflow UI. It runs at 23:00 and orchestrates:
1. rclone sync (Daylio CSV from Google Drive)
2. All ingestion scripts (in parallel where possible)
3. dbt run
4. Dashboard generation

### 6. Dashboard

Generate manually:

```bash
python3 dashboard/generate_dashboard.py
```

Start the chat server:

```bash
python3 dashboard/chat_server.py
```

Open `dashboard/output/dashboard.html` in a browser.

---

## LLM Cost Tracking

All Claude API calls are logged to the `llm_calls` table in PostgreSQL. The dashboard shows monthly cost by model. A separate `chat_history` table stores every chat Q&A exchange, and `daily_insights` stores the nightly Haiku summary.

The nightly Haiku insight is cached per day — re-running `generate_dashboard.py` on the same day will not make a second API call.

---

## Personal Context

`dashboard/personal_context.json` (gitignored) is sent with every AI call to give Claude background about you — goals, health conditions, medications, etc. See `personal_context.json.example` for the expected shape.

---

## Short-Term Context Notes

Temporary health states — jetlag, illness, injury, travel fatigue — are automatically detected and saved when you mention them in chat. They're injected into both the nightly dashboard insights and every chat response until they expire.

**How it works:**

Every chat message runs a lightweight Haiku pre-pass that detects temporary conditions and saves them to `dashboard/context_notes.json` with an estimated expiry date:

```
You: "I came back on a flight from India last Friday, still pretty jetlagged"

→ Auto-saved: "Jetlagged after long-haul flight from India. Circadian rhythm
  significantly disrupted, energy and recovery likely suppressed."
  Expires: 2026-04-01
```

Recovery estimates used:

| Condition | Default expiry |
|-----------|---------------|
| Jetlag | ~1 day per timezone hour crossed (5–10 days) |
| Mild cold | 7 days |
| Flu | 10–14 days |
| Travel fatigue | 2–3 days |
| Minor injury/strain | 14 days |
| Stress/burnout | 7–14 days |

Notes expire silently — no cleanup needed. Active notes are filtered by date on every read and included in the AI context until they pass their expiry.
