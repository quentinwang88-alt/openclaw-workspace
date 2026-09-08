#!/usr/bin/env python3
"""Record an explicit operator review, release, and optionally queue a photo pack."""

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
from services.main_schedule_bridge import MainScheduleBridge  # noqa: E402
from services.release_gate import freeze_photo_release  # noqa: E402
from services.workflow_v2 import PhotoPackageReviewService  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--reviewer", required=True, help="real operator name")
    parser.add_argument("--decision", choices=("passed", "failed"), required=True)
    parser.add_argument("--enqueue", action="store_true",
                        help="after a passed review, add the frozen pack to the existing scheduler")
    args = parser.parse_args()
    if args.enqueue and args.decision != "passed":
        parser.error("--enqueue requires --decision passed")
    load_repo_env()
    repository = RdsRepository.from_env()
    review = PhotoPackageReviewService(repository).record(
        args.task_id, decision=args.decision,
        dimensions={"operator_preview": True}, reviewer_type="human",
        reviewer=args.reviewer,
    )
    result = {"task_id": args.task_id, "review_id": review.review_id,
              "decision": review.decision, "queued": False}
    if args.decision == "passed":
        task = repository.get_task(args.task_id)
        result["release"] = freeze_photo_release(repository, task)
        if args.enqueue:
            result["queue"] = MainScheduleBridge(repository).enqueue_task(args.task_id)
            result["queued"] = True
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
