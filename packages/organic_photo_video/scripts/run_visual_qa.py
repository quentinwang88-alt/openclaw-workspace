#!/usr/bin/env python3
"""Run current-policy group QA or an explicit V2 single-shot diagnostic."""

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
load_repo_env()

from repositories.rds_repository import RdsRepository  # noqa: E402
from services.visual_qa import (  # noqa: E402
    CreatorCrmVisualQaAdapter,
    VisualQaService,
    decision_to_dict,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task_id")
    parser.add_argument("--slot-index", type=int, choices=range(1, 6), help="仅复核指定V2图位，不授予整组通过")
    parser.add_argument("--force-recheck", action="store_true", help="忽略单图审核缓存重新判图；会重新调用模型")
    args = parser.parse_args()
    service = VisualQaService(
        RdsRepository.from_env(),
        CreatorCrmVisualQaAdapter(),
        inter_request_delay_seconds=3,
    )
    if args.slot_index is not None:
        decision = service.review_shot(args.task_id, args.slot_index, force_recheck=args.force_recheck)
        print(json.dumps({"task_id": args.task_id, "slot_index": args.slot_index,
                          "scope": "targeted_single", "group_approved": False,
                          "assessment": decision_to_dict(decision)}, ensure_ascii=False, indent=2))
        return 0 if decision.passed else 2
    report = service.review_task(args.task_id, force_recheck=args.force_recheck)
    print(json.dumps({
        "task_id": report.task_id,
        "passed": report.passed,
        "single": {str(k): decision_to_dict(v) for k, v in report.single.items()},
        "group": decision_to_dict(report.group),
    }, ensure_ascii=False, indent=2))
    return 0 if report.passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
