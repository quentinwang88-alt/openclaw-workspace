#!/usr/bin/env python3
"""Validate and queue an explicit whitelist of OPV review videos."""

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

from domain import statuses  # noqa: E402
from repositories.rds_repository import RdsRepository  # noqa: E402
from services.locale_quality import copy_locale_issues  # noqa: E402
from services.neobund_publisher import OpvPublishFlow  # noqa: E402
from services.neobund_wiring import build_opv_neobund  # noqa: E402
from services.publish_scheduler import AutoPublishScheduler  # noqa: E402


def inspect_task(repository: RdsRepository, task_id: str) -> dict:
    task = repository.get_task(task_id)
    if task is None:
        return {"task_id": task_id, "ready": False, "errors": ["task_not_found"]}
    render = repository.get_render(task.selected_render_id or "")
    records = repository.list_publish_records_by_task(task_id)
    output_path = Path(render.output_url) if render and render.output_url else None
    caption = str((task.copy_json or {}).get("caption") or "").strip()
    errors = []
    if task.task_status not in {
        statuses.TASK_VIDEO_REVIEW,
        statuses.TASK_PUBLISH_PREPARING,
        statuses.TASK_READY_TO_PUBLISH,
        statuses.TASK_PUBLISHING,
        statuses.TASK_PUBLISHED,
    }:
        errors.append(f"invalid_task_status:{task.task_status}")
    if render is None:
        errors.append("selected_render_not_found")
    else:
        if render.qc_status != "passed":
            errors.append(f"qc_not_passed:{render.qc_status}")
        if not render.publish_ready:
            errors.append("render_not_publish_ready")
        if output_path is None or not output_path.is_file():
            errors.append("render_file_missing")
    if not caption:
        errors.append("caption_missing")
    errors.extend(
        f"locale_impurity:{issue}"
        for issue in copy_locale_issues(task.copy_json or {}, task.target_locale)
    )
    return {
        "task_id": task_id,
        "task_status": task.task_status,
        "account_id": task.account_id,
        "country": task.target_country,
        "render_id": render.render_id if render else None,
        "output_path": str(output_path) if output_path else None,
        "qc_status": render.qc_status if render else None,
        "publish_ready": bool(render and render.publish_ready),
        "caption": caption,
        "existing_publish_records": [
            {
                "publish_id": row.publish_id,
                "status": row.publish_status,
                "planned_publish_at_utc": (
                    row.planned_publish_at.isoformat(timespec="seconds")
                    if row.planned_publish_at else None
                ),
                "external_post_id": row.external_post_id,
            }
            for row in records
        ],
        "ready": not errors,
        "errors": errors,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task_ids", nargs="+")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--operator", default="codex:user-approved-20260901")
    args = parser.parse_args()

    repository = RdsRepository.from_env()
    inspections = [inspect_task(repository, task_id) for task_id in args.task_ids]
    report = {"mode": "apply" if args.apply else "dry_run", "tasks": inspections}
    if not all(row["ready"] for row in inspections):
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 2
    if not args.apply:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0

    wiring = build_opv_neobund(repository=repository)
    flow = OpvPublishFlow(
        repository, adapter=wiring.adapter, music_source=wiring.music_source
    )
    scheduler = AutoPublishScheduler(repository, flow)
    schedules = []
    for task_id in args.task_ids:
        task = repository.get_task(task_id)
        if task.task_status == statuses.TASK_VIDEO_REVIEW:
            slot = scheduler.queue_task(task_id, operator=args.operator)
        elif task.task_status == statuses.TASK_PUBLISH_PREPARING:
            slot = scheduler.queue_task(task_id, operator=args.operator)
        else:
            records = repository.list_publish_records_by_task(task_id)
            if not records or not records[0].planned_publish_at:
                raise RuntimeError(f"task {task_id} has no resumable publish schedule")
            slot = scheduler._slot_from_utc(task.account_id, records[0].planned_publish_at)
        schedules.append({
            "task_id": task_id,
            "account_local_time": slot.account_local_time.isoformat(timespec="seconds"),
            "planned_publish_at_utc": slot.planned_publish_at_utc.isoformat(timespec="seconds"),
            "neobund_wall_time": slot.neobund_wall_time.isoformat(timespec="seconds"),
        })
    report["schedules"] = schedules
    report["immediate_events"] = scheduler.tick(task_ids=args.task_ids)
    report["tasks_after"] = [
        inspect_task(repository, task_id) for task_id in args.task_ids
    ]
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
