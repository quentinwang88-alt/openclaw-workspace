#!/usr/bin/env python3
"""Repair the Thai publish-only copy for the three approved 2026-09-01 videos."""

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
from services.copy_writer import build_caption  # noqa: E402
from services.locale_quality import copy_locale_issues  # noqa: E402


COPY_FIXES = {
    "opv_task_20260901_137467ccc379": {
        "title": "สาวตัวเล็กต้องรู้: ใส่แจ็กเก็ตครอปยังไงให้ขาดูยาว",
        "one_liner": "จับคู่กับเอวสูง ช่วยให้สัดส่วนดูเพรียวและขายาวขึ้น",
        "hashtags": ["#แฟชั่นผู้หญิง", "#สาวตัวเล็ก", "#ไอเดียแต่งตัว", "#OOTD"],
    },
    "opv_task_20260901_fb88d718fa58": {
        "title": "ลุคนัดคาเฟ่กับแจ็กเก็ตครอปสีฟ้าอ่อน",
        "one_liner": "โทนละมุน แมตช์ง่าย และถ่ายรูปขึ้นกล้อง",
        "hashtags": ["#ลุคคาเฟ่", "#แฟชั่นผู้หญิง", "#ไอเดียแต่งตัว", "#OOTD"],
    },
    "opv_task_20260901_0957b8765974": {
        "title": "แจ็กเก็ตตัวเดียว 3 ลุค: หวาน เท่ และสบาย",
        "one_liner": "เปลี่ยนอารมณ์ลุคได้ง่ายด้วยไอเท็มตัวเดิม",
        "hashtags": ["#แต่งตัว3ลุค", "#แฟชั่นผู้หญิง", "#ไอเดียแต่งตัว", "#OOTD"],
    },
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    repository = RdsRepository.from_env()
    rows = []
    for task_id, patch in COPY_FIXES.items():
        task = repository.get_task(task_id)
        if task is None:
            raise RuntimeError(f"task not found: {task_id}")
        updated = dict(task.copy_json or {})
        updated.update({
            "title": patch["title"],
            "hashtags": patch["hashtags"],
            "caption": build_caption(
                patch["title"], patch["one_liner"], patch["hashtags"]
            ),
        })
        issues = copy_locale_issues(updated, task.target_locale)
        if issues:
            raise RuntimeError(f"invalid repaired copy for {task_id}: {issues}")
        if args.apply:
            repository.update_task_plan(task_id, copy_json=updated)
        rows.append({
            "task_id": task_id,
            "title": updated["title"],
            "caption": updated["caption"],
            "locale_issues": issues,
        })
    print(json.dumps({
        "mode": "apply" if args.apply else "dry_run",
        "tasks": rows,
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
