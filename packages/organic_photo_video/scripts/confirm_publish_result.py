#!/usr/bin/env python3
"""Confirm the publish result of an OPV task (Stage F-3 requery).

Usage:
    python3 scripts/confirm_publish_result.py --task-id <opv_task_id>

Queries NeoBund for the submitted task, records the ACTUAL attached music,
and moves the OPV task publishing -> published / failed. Safe to run
repeatedly: while NeoBund still reports pending, nothing changes and
nothing is resubmitted.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
for path in (str(WORKSPACE_ROOT), str(PACKAGE_ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)

from workspace_support import load_repo_env  # noqa: E402

load_repo_env()

from repositories.rds_repository import RdsRepository  # noqa: E402
from services.neobund_publisher import OpvPublishFlow, PublishFlowError  # noqa: E402
from services.neobund_wiring import build_opv_neobund  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task-id", required=True)
    args = parser.parse_args()

    repo = RdsRepository.from_env()
    wiring = build_opv_neobund(repository=repo)
    flow = OpvPublishFlow(repo, adapter=wiring.adapter, music_source=wiring.music_source)
    try:
        result = flow.confirm_result(args.task_id)
    except PublishFlowError as exc:
        print(f"confirm skipped: {exc}")
        return 1

    task = repo.get_task(args.task_id)
    record = repo.get_publish_record_by_render(task.selected_render_id or "")
    audio = (record.platform_metadata_json or {}).get("audio") or {}
    print(f"confirm={result}")
    print(f"task={task.task_status} record={record.publish_status}")
    print(f"actual_music={audio.get('actual')}")
    if result == "pending":
        print("still scheduled on NeoBund; run this again after the publish time")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
