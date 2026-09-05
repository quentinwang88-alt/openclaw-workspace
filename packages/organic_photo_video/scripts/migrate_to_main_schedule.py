#!/usr/bin/env python3
"""Idempotently migrate selected OPV renders into the main organic scheduler."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
for value in (str(WORKSPACE_ROOT), str(PACKAGE_ROOT)):
    if value not in sys.path:
        sys.path.insert(0, value)

from workspace_support import load_repo_env  # noqa: E402

load_repo_env()

from repositories.rds_repository import RdsRepository  # noqa: E402
from services.main_schedule_bridge import MainScheduleBridge  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task-id", action="append", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    repository = RdsRepository.from_env()
    bridge = MainScheduleBridge(repository)
    results = []
    for task_id in args.task_id:
        task = repository.get_task(task_id)
        render = (
            repository.get_render(task.selected_render_id or "") if task else None
        )
        preview = {
            "task_id": task_id,
            "task_status": task.task_status if task else None,
            "render_path": render.output_url if render else None,
            "target_country": task.target_country if task else None,
        }
        if args.apply:
            preview.update(bridge.enqueue_task(task_id))
        results.append(preview)
    print(json.dumps({"applied": args.apply, "results": results}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
