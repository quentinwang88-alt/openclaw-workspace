#!/usr/bin/env python3
"""Backfill selected=true for the latest P1-P5 versions of one OPV task."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
for value in (str(WORKSPACE_ROOT), str(PACKAGE_ROOT)):
    if value not in sys.path:
        sys.path.insert(0, value)

from workspace_support import load_repo_env  # noqa: E402
load_repo_env()

from repositories.rds_repository import RdsRepository  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task_id")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    repository = RdsRepository.from_env()
    shots = repository.list_latest_shots(args.task_id)
    if len(shots) != 5 or [shot.slot_index for shot in shots] != [1, 2, 3, 4, 5]:
        print(f"refusing: latest shot set is not P1-P5 ({len(shots)} rows)", file=sys.stderr)
        return 2
    for shot in shots:
        print(
            f"P{shot.slot_index} shot_id={shot.shot_id} v{shot.shot_version} "
            f"status={shot.shot_status} selected={int(shot.is_selected)}"
        )
    if not args.apply:
        print("no_database_writes=1")
        return 0
    for shot in shots:
        repository.select_shot_version(args.task_id, shot.slot_index, shot.shot_id)
    print("selected_slots=5")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
