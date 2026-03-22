"""
health_nightly.py — Airflow DAG for the nightly health data pipeline

Schedule: 23:00 every night (replaces individual cron jobs)

Pipeline order:
  rclone_daylio ─┐
  ingest_oura    ├─► dbt_run ─► generate_dashboard
  ingest_garmin  │
  ingest_weather │
  ingest_daily_strength
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PYTHON       = "/usr/bin/python3"
INGESTION    = "/home/jhumms/health_workflow/ingestion"
DASHBOARD    = "/home/jhumms/health_workflow/dashboard"
DBT_HOME     = "/home/jhumms/health_workflow/health_dbt"
DBT_BIN      = "/home/jhumms/.local/bin/dbt"
RCLONE       = "/usr/bin/rclone"

default_args = {
    "owner": "jhumms",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="health_nightly",
    description="Nightly health data ingestion → dbt → dashboard",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 3, 21),
    catchup=False,
    default_args=default_args,
    tags=["health"],
) as dag:

    # ── Step 1: sync Daylio CSV from Google Drive ────────────────────────────
    rclone_daylio = BashOperator(
        task_id="rclone_daylio",
        bash_command=(
            f'{RCLONE} move "gdrive:Daylio" '
            f'/home/jhumms/health_workflow/ingestion/drops/daylio '
            f'--include "*.csv"'
        ),
    )

    # ── Step 2: ingest all sources (parallel where possible) ─────────────────
    ingest_oura = BashOperator(
        task_id="ingest_oura",
        bash_command=f"{PYTHON} {INGESTION}/ingest_oura.py",
    )

    ingest_garmin = BashOperator(
        task_id="ingest_garmin",
        bash_command=f"{PYTHON} {INGESTION}/ingest_garmin.py",
    )

    ingest_weather = BashOperator(
        task_id="ingest_weather",
        bash_command=f"{PYTHON} {INGESTION}/ingest_weather.py",
    )

    ingest_daily_strength = BashOperator(
        task_id="ingest_daily_strength",
        bash_command=f"{PYTHON} {INGESTION}/ingest_daily_strength.py",
    )

    # Daylio depends on rclone completing first
    ingest_daylio = BashOperator(
        task_id="ingest_daylio",
        bash_command=f"{PYTHON} {INGESTION}/ingest_daylio.py",
    )

    # ── Step 3: dbt transforms (after all ingestions) ────────────────────────
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_HOME} && {DBT_BIN} run",
    )

    # ── Step 4: generate dashboard (after dbt) ───────────────────────────────
    generate_dashboard = BashOperator(
        task_id="generate_dashboard",
        bash_command=f"{PYTHON} {DASHBOARD}/generate_dashboard.py",
    )

    # ── Dependencies ─────────────────────────────────────────────────────────
    # rclone must finish before Daylio ingest
    rclone_daylio >> ingest_daylio

    # All ingestions must complete before dbt
    [ingest_oura, ingest_garmin, ingest_weather, ingest_daily_strength, ingest_daylio] >> dbt_run

    # dbt must complete before dashboard
    dbt_run >> generate_dashboard
