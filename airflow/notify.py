"""
notify.py — DAG run email notifications via Gmail SMTP.

Reads credentials from ingestion/.env:
    NOTIFY_EMAIL_FROM     sender address (your Gmail)
    NOTIFY_EMAIL_TO       recipient address
    NOTIFY_SMTP_PASSWORD  Gmail App Password (not your account password)
"""

import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "../ingestion/.env"))

_FROM     = os.getenv("NOTIFY_EMAIL_FROM")
_TO       = os.getenv("NOTIFY_EMAIL_TO")
_PASSWORD = os.getenv("NOTIFY_SMTP_PASSWORD")

_SMTP_HOST = "smtp.gmail.com"
_SMTP_PORT = 587

# ── Colours ──────────────────────────────────────────────────────────────────

_STATE_STYLE = {
    "success":         "background:#d4edda; color:#155724;",   # green
    "failed":          "background:#f8d7da; color:#721c24;",   # red
    "skipped":         "background:#e2e3e5; color:#383d41;",   # grey
    "upstream_failed": "background:#fff3cd; color:#856404;",   # amber
    "running":         "background:#cce5ff; color:#004085;",   # blue
}
_DEFAULT_STYLE = "background:#f5f5f5; color:#333;"

_DAG_HEADER_STYLE = {
    True:  "background:#28a745; color:#fff;",   # success
    False: "background:#dc3545; color:#fff;",   # failure
}


def _build_html(context: dict, dag_succeeded: bool) -> str:
    dag_run   = context["dag_run"]
    dag_id    = dag_run.dag_id
    run_id    = dag_run.run_id
    exec_date = dag_run.execution_date.strftime("%Y-%m-%d %H:%M UTC")
    tis       = dag_run.get_task_instances()

    header_style = _DAG_HEADER_STYLE[dag_succeeded]
    overall      = "SUCCEEDED" if dag_succeeded else "FAILED"

    rows = ""
    for ti in sorted(tis, key=lambda t: t.start_date or ti.execution_date):
        state       = (ti.state or "unknown").lower()
        cell_style  = _STATE_STYLE.get(state, _DEFAULT_STYLE)
        duration    = (
            f"{ti.duration:.1f}s" if ti.duration is not None else "—"
        )
        rows += f"""
        <tr>
          <td style="padding:8px 12px; border-bottom:1px solid #dee2e6;">{ti.task_id}</td>
          <td style="padding:8px 12px; border-bottom:1px solid #dee2e6; text-align:center;">
            <span style="padding:3px 10px; border-radius:4px; font-weight:bold; font-size:13px; {cell_style}">{state.upper()}</span>
          </td>
          <td style="padding:8px 12px; border-bottom:1px solid #dee2e6; text-align:right; color:#666;">{duration}</td>
        </tr>"""

    return f"""
    <html><body style="font-family:Arial,sans-serif; font-size:14px; color:#333; margin:0; padding:0;">
      <table width="600" cellpadding="0" cellspacing="0" style="margin:24px auto; border:1px solid #dee2e6; border-radius:6px; overflow:hidden;">
        <tr>
          <td colspan="3" style="padding:16px 20px; {header_style} font-size:18px; font-weight:bold;">
            {dag_id} — {overall}
          </td>
        </tr>
        <tr>
          <td colspan="3" style="padding:8px 20px; background:#f8f9fa; font-size:12px; color:#666; border-bottom:1px solid #dee2e6;">
            Run: {run_id}<br>Execution: {exec_date}
          </td>
        </tr>
        <tr>
          <th style="padding:8px 12px; background:#f1f3f5; text-align:left; border-bottom:2px solid #dee2e6;">Task</th>
          <th style="padding:8px 12px; background:#f1f3f5; text-align:center; border-bottom:2px solid #dee2e6;">Status</th>
          <th style="padding:8px 12px; background:#f1f3f5; text-align:right; border-bottom:2px solid #dee2e6;">Duration</th>
        </tr>
        {rows}
      </table>
    </body></html>"""


def _send(subject: str, html: str):
    if not all([_FROM, _TO, _PASSWORD]):
        print("[notify] Missing NOTIFY_EMAIL_FROM / NOTIFY_EMAIL_TO / NOTIFY_SMTP_PASSWORD — skipping email")
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = _FROM
    msg["To"]      = _TO
    msg.attach(MIMEText(html, "html"))

    with smtplib.SMTP(_SMTP_HOST, _SMTP_PORT) as server:
        server.ehlo()
        server.starttls()
        server.login(_FROM, _PASSWORD)
        server.sendmail(_FROM, _TO, msg.as_string())

    print(f"[notify] Email sent → {_TO}")


# ── Public callbacks ──────────────────────────────────────────────────────────

def on_success(context: dict):
    dag_id  = context["dag_run"].dag_id
    subject = f"✅ {dag_id} succeeded"
    _send(subject, _build_html(context, dag_succeeded=True))


def on_failure(context: dict):
    dag_id  = context["dag_run"].dag_id
    subject = f"❌ {dag_id} failed"
    _send(subject, _build_html(context, dag_succeeded=False))
