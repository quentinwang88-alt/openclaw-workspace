#!/usr/bin/env python3
"""Append deduplicated workspace memory events.

This helper keeps daily memory human-readable while using memory/index.json as
the lightweight source of truth for repeated task/event fingerprints.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MEMORY_DIR = ROOT / "memory"
INDEX_PATH = MEMORY_DIR / "index.json"


def now_local() -> datetime:
    return datetime.now().astimezone()


def slug(value: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "-" for ch in value.strip())
    return "-".join(part for part in cleaned.split("-") if part) or "general"


def load_index() -> dict:
    if not INDEX_PATH.exists():
        return {
            "version": 1,
            "updatedAt": None,
            "policy": {
                "purpose": "Deduplicate and update recurring memory events before appending to daily memory or promoting to MEMORY.md.",
                "dailyMemory": "Allow progress updates for the same task, but use the same fingerprint when the task identity is the same.",
                "longTermMemory": "Merge by project/fact/rule and keep only the latest stable version.",
                "privacy": "Never store passwords, cookies, tokens, private chat transcripts, or raw sensitive data.",
            },
            "fingerprintFields": ["category", "project", "subject", "date", "status"],
            "events": {},
        }
    return json.loads(INDEX_PATH.read_text(encoding="utf-8"))


def write_index(index: dict) -> None:
    INDEX_PATH.write_text(json.dumps(index, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def append_daily(day: str, time_text: str, summary: str) -> str:
    MEMORY_DIR.mkdir(parents=True, exist_ok=True)
    daily_path = MEMORY_DIR / f"{day}.md"
    if not daily_path.exists():
        daily_path.write_text(f"# {day}\n\n", encoding="utf-8")
    line = f"- {time_text} {summary}\n"
    existing = daily_path.read_text(encoding="utf-8")
    if line not in existing:
        with daily_path.open("a", encoding="utf-8") as handle:
            handle.write(line)
    return str(daily_path.relative_to(ROOT))


def main() -> int:
    parser = argparse.ArgumentParser(description="Append a deduplicated daily memory event.")
    parser.add_argument("--category", required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--subject", required=True)
    parser.add_argument("--status", default="noted")
    parser.add_argument("--summary", required=True)
    parser.add_argument("--date", help="YYYY-MM-DD. Defaults to local today.")
    parser.add_argument("--time", help="HH:MM. Defaults to local current time.")
    parser.add_argument("--no-daily", action="store_true", help="Update index only; do not append daily memory.")
    args = parser.parse_args()

    stamp = now_local()
    day = args.date or stamp.strftime("%Y-%m-%d")
    time_text = args.time or stamp.strftime("%H:%M")
    fingerprint = ":".join(
        [
            slug(args.category),
            slug(args.project),
            slug(args.subject),
            day,
            slug(args.status),
        ]
    )

    index = load_index()
    events = index.setdefault("events", {})
    event = events.get(fingerprint)
    daily_path = None
    if not args.no_daily:
        daily_path = append_daily(day, time_text, args.summary)

    iso = stamp.isoformat(timespec="seconds")
    if event:
        event["lastSeenAt"] = iso
        event["count"] = int(event.get("count", 1)) + 1
        event["status"] = args.status
        event["summary"] = args.summary
        if daily_path:
            event["dailyPath"] = daily_path
        action = "updated"
    else:
        events[fingerprint] = {
            "fingerprint": fingerprint,
            "category": args.category,
            "project": args.project,
            "subject": args.subject,
            "date": day,
            "status": args.status,
            "dailyPath": daily_path,
            "summary": args.summary,
            "firstSeenAt": iso,
            "lastSeenAt": iso,
            "count": 1,
        }
        action = "created"

    index["updatedAt"] = iso
    write_index(index)
    print(json.dumps({"action": action, "fingerprint": fingerprint, "dailyPath": daily_path}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
