#!/usr/bin/env python3
"""后台轮询 Accio，直到批次进入终态。"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.config import get_settings  # noqa: E402
from app.pipeline import FastMossPipeline  # noqa: E402


TERMINAL_OVERALL_STATUSES = {
    "Hermes完成待人审",
    "已完成",
    "失败",
}

TERMINAL_ACCIO_STATUSES = {
    "已回收",
    "超时",
}


def _utc_now() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _log(event: str, **payload: object) -> None:
    line = {"ts": _utc_now(), "event": event}
    line.update(payload)
    print(json.dumps(line, ensure_ascii=False), flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="轮询 Accio 直到批次完成")
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--interval-seconds", type=int, default=300)
    parser.add_argument("--max-hours", type=float, default=12.0)
    args = parser.parse_args()

    settings = get_settings()
    pipeline = FastMossPipeline.from_settings(settings)

    deadline = datetime.utcnow() + timedelta(hours=max(args.max_hours, 0.1))
    _log(
        "watch_started",
        batch_id=args.batch_id,
        interval_seconds=args.interval_seconds,
        max_hours=args.max_hours,
    )

    while datetime.utcnow() < deadline:
        batch_state = pipeline.db.get_batch(args.batch_id) or {}
        overall_status = str(batch_state.get("overall_status") or "")
        accio_status = str(batch_state.get("accio_status") or "")
        hermes_status = str(batch_state.get("hermes_status") or "")

        if overall_status in TERMINAL_OVERALL_STATUSES or accio_status in TERMINAL_ACCIO_STATUSES:
            _log(
                "already_terminal",
                batch_id=args.batch_id,
                overall_status=overall_status,
                accio_status=accio_status,
                hermes_status=hermes_status,
            )
            return 0

        try:
            result = pipeline.collect_accio_results(batch_id=args.batch_id, limit=1, run_hermes=True)
            batch_state = pipeline.db.get_batch(args.batch_id) or {}
            overall_status = str(batch_state.get("overall_status") or "")
            accio_status = str(batch_state.get("accio_status") or "")
            hermes_status = str(batch_state.get("hermes_status") or "")
            _log(
                "poll_result",
                batch_id=args.batch_id,
                collect_result=result,
                overall_status=overall_status,
                accio_status=accio_status,
                hermes_status=hermes_status,
            )
            if overall_status in TERMINAL_OVERALL_STATUSES or accio_status in TERMINAL_ACCIO_STATUSES:
                _log(
                    "watch_completed",
                    batch_id=args.batch_id,
                    overall_status=overall_status,
                    accio_status=accio_status,
                    hermes_status=hermes_status,
                )
                return 0
        except Exception as exc:  # pragma: no cover - runtime/network path
            _log("poll_error", batch_id=args.batch_id, error=str(exc))

        time.sleep(max(args.interval_seconds, 30))

    final_state = pipeline.db.get_batch(args.batch_id) or {}
    _log(
        "watch_timeout",
        batch_id=args.batch_id,
        overall_status=str(final_state.get("overall_status") or ""),
        accio_status=str(final_state.get("accio_status") or ""),
        hermes_status=str(final_state.get("hermes_status") or ""),
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
