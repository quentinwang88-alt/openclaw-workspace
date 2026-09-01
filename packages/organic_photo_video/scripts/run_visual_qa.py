#!/usr/bin/env python3
"""Run single-shot and five-shot visual QA for one image_review task."""

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
    args = parser.parse_args()
    report = VisualQaService(
        RdsRepository.from_env(), CreatorCrmVisualQaAdapter()
    ).review_task(args.task_id)
    print(json.dumps({
        "task_id": report.task_id,
        "passed": report.passed,
        "single": {str(k): decision_to_dict(v) for k, v in report.single.items()},
        "group": decision_to_dict(report.group),
    }, ensure_ascii=False, indent=2))
    return 0 if report.passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
