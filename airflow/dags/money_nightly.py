"""
money_nightly.py — Airflow DAG for the money manager pipeline

Schedule: 8:00 AM daily (after MoneyManager app auto-backup runs overnight)

Pipeline:
  rclone_sync ──► load_to_postgres ──► dbt_run ──► generate_dashboard
                                                         │
                                              (Sundays) weekly_ai_summary
"""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator

PYTHON   = "/usr/bin/python3"
MONEY    = "/home/jhumms/money_manager"
DASH     = f"{MONEY}/dashboard"
DBT_HOME = f"{MONEY}/money_dbt"
DBT_BIN  = "/home/jhumms/.local/bin/dbt"

default_args = {
    "owner": "jhumms",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def _is_sunday(**context):
    """Branch to weekly_ai_summary only on Sundays."""
    execution_date = context["data_interval_start"]
    if execution_date.weekday() == 6:  # Sunday
        return "weekly_ai_summary"
    return "skip_weekly_summary"


with DAG(
    dag_id="money_nightly",
    description="Daily MoneyManager sync → dbt → dashboard",
    schedule_interval="0 8 * * *",
    start_date=pendulum.datetime(2026, 3, 24, tz="America/New_York"),
    catchup=False,
    default_args=default_args,
    tags=["money"],
) as dag:

    # ── Step 1: rclone sync from Google Drive ─────────────────────────────────
    rclone_sync = BashOperator(
        task_id="rclone_sync",
        bash_command=f"{PYTHON} {MONEY}/sync_moneymanager.py",
    )

    # ── Step 2: load existing SQLite → PostgreSQL (catches any missed syncs) ──
    load_to_postgres = BashOperator(
        task_id="load_to_postgres",
        bash_command=f"{PYTHON} {MONEY}/sync_sqlite_to_pg.py",
    )

    # ── Step 3: dbt seed + run ────────────────────────────────────────────────
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_HOME} && {DBT_BIN} seed --select budgets && {DBT_BIN} run",
    )

    # ── Step 4: regenerate dashboard HTML ────────────────────────────────────
    generate_dashboard = BashOperator(
        task_id="generate_dashboard",
        bash_command=f"{PYTHON} {DASH}/generate_dashboard.py",
    )

    # ── Step 5: Sunday-only weekly AI summary ─────────────────────────────────
    branch = BranchPythonOperator(
        task_id="sunday_branch",
        python_callable=_is_sunday,
        provide_context=True,
    )

    weekly_ai_summary = BashOperator(
        task_id="weekly_ai_summary",
        bash_command=f"{PYTHON} {DASH}/weekly_summary.py",
    )

    skip_weekly_summary = EmptyOperator(task_id="skip_weekly_summary")

    # ── Dependencies ──────────────────────────────────────────────────────────
    rclone_sync >> load_to_postgres >> dbt_run >> generate_dashboard >> branch
    branch >> [weekly_ai_summary, skip_weekly_summary]
