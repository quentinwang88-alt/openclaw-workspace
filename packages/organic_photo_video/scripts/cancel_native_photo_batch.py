#!/usr/bin/env python3
"""Audit or cancel one unpublished Feishu native-photo batch.

Dry-run is the default.  ``--apply`` keeps the batch row for audit while
releasing its content signatures.  Released or queued tasks are rejected.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
for value in (str(WORKSPACE_ROOT), str(PACKAGE_ROOT)):
    if value not in sys.path:
        sys.path.insert(0, value)

from workspace_support import load_repo_env  # noqa: E402
from repositories.rds_repository import RdsRepository  # noqa: E402
from services.main_schedule_bridge import assert_main_queue_rework_allowed  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source_record_id", help="Feishu production row record id")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    load_repo_env()
    repository = RdsRepository.from_env()
    batch = repository.get_production_batch(args.source_record_id)
    if batch is None or batch.manifest_json.get("media_kind") != "native_photo":
        raise ValueError("native-photo batch does not exist")
    tasks = repository.list_tasks_by_source_prefix("feishu_opv", args.source_record_id + ":")
    for task in tasks:
        assert_main_queue_rework_allowed(task.task_id)
    if args.apply:
        batch = repository.cancel_photo_batch(args.source_record_id)
    print(json.dumps({
        "mode": "apply" if args.apply else "dry-run",
        "source_record_id": args.source_record_id,
        "batch_id": batch.batch_id, "batch_status": batch.batch_status,
        "task_ids": [task.task_id for task in tasks],
        "inventory_released": bool(args.apply and batch.batch_status == "cancelled"),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
