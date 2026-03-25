"""
context_notes.py — Short-term health context with automatic expiry.

Stores temporary notes (e.g. "jetlagged", "have a cold") in context_notes.json.
Each note has a created date and an expiry date; get_active_notes() filters out
anything past its expiry so stale context never reaches the AI.
"""

import json
import logging
from datetime import date, timedelta
from pathlib import Path

log = logging.getLogger(__name__)

NOTES_PATH = Path(__file__).parent / "context_notes.json"


def get_active_notes() -> list[dict]:
    """Return all notes that have not yet expired."""
    if not NOTES_PATH.exists():
        return []
    try:
        notes = json.loads(NOTES_PATH.read_text())
        today = date.today().isoformat()
        return [n for n in notes if n.get("expires", "") >= today]
    except Exception as e:
        log.warning("Could not read context_notes.json: %s", e)
        return []


def save_note(note: str, expires_days: int) -> dict:
    """
    Append a new note expiring in `expires_days` days.
    Returns the saved entry.
    """
    if not note or not note.strip():
        return {"error": "Note text cannot be empty."}
    expires_days = max(1, min(expires_days, 60))  # clamp 1–60 days

    today = date.today()
    entry = {
        "note": note.strip(),
        "created": today.isoformat(),
        "expires": (today + timedelta(days=expires_days)).isoformat(),
    }

    # Load existing, append, write back (preserve unexpired notes only)
    existing = get_active_notes()
    existing.append(entry)
    NOTES_PATH.write_text(json.dumps(existing, indent=2))
    log.info("Saved context note (expires %s): %s", entry["expires"], entry["note"])
    return entry


def format_for_prompt(notes: list[dict]) -> str:
    """Format active notes as a short block for injection into AI prompts."""
    if not notes:
        return ""
    lines = ["Current short-term context (temporary, will auto-expire):"]
    for n in notes:
        lines.append(f"  - {n['note']} (expires {n['expires']})")
    return "\n".join(lines)
