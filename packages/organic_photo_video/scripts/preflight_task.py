#!/usr/bin/env python3
"""Run read-only OPV preflight for one RDS task. Never calls Feishu."""

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
from services.asset_resolver import LightTryonAssetReader  # noqa: E402
from services.preflight import PreflightService  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task_id")
    args = parser.parse_args()
    report = PreflightService(
        RdsRepository.from_env(), LightTryonAssetReader()
    ).check_task(args.task_id)
    print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
    return 0 if report.ready_for_generation else 2


if __name__ == "__main__":
    raise SystemExit(main())
