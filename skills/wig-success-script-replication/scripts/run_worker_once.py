#!/usr/bin/env python3
"""Poll and execute one bounded batch with a non-overlapping local process lock."""

from __future__ import annotations

import argparse
import fcntl
import json
from pathlib import Path

from _runtime import jsonable
from production_runtime import build_application


SKILL_ROOT = Path(__file__).resolve().parents[1]
VAR_DIR = SKILL_ROOT / "var"
LOCK_PATH = VAR_DIR / "worker.lock"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one wig replication worker poll")
    parser.add_argument("--limit", type=int, default=2)
    args = parser.parse_args()
    if not 1 <= args.limit <= 5:
        raise SystemExit("limit must be between 1 and 5")
    VAR_DIR.mkdir(parents=True, exist_ok=True)
    with LOCK_PATH.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(json.dumps({"status": "skipped", "reason": "worker_already_running"}))
            return 0
        # The daemon is the production composition root; never silently fall back
        # to the domain package's unconfigured compatibility facade.
        app = build_application()
        result = app.run_pending(action="all", limit=args.limit)
        outbox = app.retry_feishu_outbox(limit=100)
        print(json.dumps(jsonable({"tasks": result, "outbox": outbox}), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
