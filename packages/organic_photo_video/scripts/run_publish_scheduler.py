#!/usr/bin/env python3
"""Run one safe OPV auto-publish tick and sync affected Feishu rows."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
BITABLE_SKILL = WORKSPACE_ROOT / "skills" / "script-run-manager-sync"
for value in (str(WORKSPACE_ROOT), str(BITABLE_SKILL), str(PACKAGE_ROOT)):
    if value not in sys.path:
        sys.path.insert(0, value)

from workspace_support import load_repo_env  # noqa: E402

load_repo_env()

from core.bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # noqa: E402
from repositories.rds_repository import RdsRepository  # noqa: E402
from services.feishu_workflow import FeishuTaskWorkflow  # noqa: E402
from services.neobund_publisher import OpvPublishFlow  # noqa: E402
from services.neobund_wiring import build_opv_neobund  # noqa: E402
from services.publish_scheduler import AutoPublishScheduler  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wiki-token", default="TR10wxEXHiCYIhk8clActVdenpc")
    parser.add_argument("--table-id", default="tblj3x846gU3rshB")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument(
        "--task-id",
        action="append",
        default=[],
        help="Restrict all actions to this task ID; repeat for multiple tasks.",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--allow-legacy-standalone",
        action="store_true",
        help="仅用于人工故障处理；正式图文发布已接入短视频主排班",
    )
    args = parser.parse_args()

    if not args.dry_run and not args.allow_legacy_standalone:
        print(json.dumps({
            "blocked": True,
            "reason": "OPV 独立排班已停用；请通过飞书确认发布进入短视频主排班池",
        }, ensure_ascii=False, indent=2))
        return 2

    repository = RdsRepository.from_env()
    if args.dry_run:
        from datetime import timedelta
        from domain.models import utc_now
        from domain import statuses

        ready = repository.list_publish_records_due(
                statuses.PUBLISH_READY,
                due_before=utc_now() + timedelta(minutes=120),
                limit=args.limit,
            )
        submitted = repository.list_publish_records_due(
                statuses.PUBLISH_SUBMITTED,
                due_before=utc_now() - timedelta(minutes=5),
                limit=args.limit,
            )
        allowed = set(args.task_id) if args.task_id else None
        if allowed is not None:
            ready = [row for row in ready if row.task_id in allowed]
            submitted = [row for row in submitted if row.task_id in allowed]
        report = {
            "task_ids": args.task_id,
            "ready_inside_120m": len(ready),
            "submitted_due_confirm": len(submitted),
            "ready_records": [
                {
                    "publish_id": row.publish_id,
                    "task_id": row.task_id,
                    "planned_publish_at": (
                        row.planned_publish_at.isoformat(timespec="seconds")
                        if row.planned_publish_at else None
                    ),
                    "human_publish_gate": (
                        (row.platform_metadata_json or {}).get("human_publish_gate")
                    ),
                }
                for row in ready
            ],
            "actions": [],
        }
        if args.task_id:
            requested = []
            for task_id in args.task_id:
                task = repository.get_task(task_id)
                render = repository.get_render(task.selected_render_id or "") if task else None
                publish = (
                    repository.get_publish_record_by_render(render.render_id)
                    if render else None
                )
                requested.append({
                    "task_id": task_id,
                    "task_status": task.task_status if task else None,
                    "publish_status": publish.publish_status if publish else None,
                    "planned_publish_at": (
                        publish.planned_publish_at.isoformat(timespec="seconds")
                        if publish and publish.planned_publish_at else None
                    ),
                    "external_post_id": publish.external_post_id if publish else None,
                    "external_post_url": publish.external_post_url if publish else None,
                    "audio_status": (
                        ((publish.platform_metadata_json or {}).get("audio") or {}).get("status")
                        if publish else None
                    ),
                    "selected_bgm": (
                        ((publish.platform_metadata_json or {}).get("audio") or {}).get("selected")
                        if publish else None
                    ),
                })
            report["requested_records"] = requested
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0

    wiring = build_opv_neobund(repository=repository)
    flow = OpvPublishFlow(
        repository, adapter=wiring.adapter, music_source=wiring.music_source
    )
    scheduler = AutoPublishScheduler(repository, flow)
    events = scheduler.tick(limit=args.limit, task_ids=args.task_id or None)
    client = FeishuBitableClient(
        resolve_wiki_bitable_app_token(args.wiki_token), args.table_id
    )
    workflow = FeishuTaskWorkflow(
        repository, client, publish_scheduler=scheduler
    )
    synced = []
    record_ids = sorted({
        str(event.get("feishu_record_id") or "") for event in events
        if event.get("feishu_record_id")
    })
    for record_id in record_ids:
        synced.append(workflow.sync_publication_status(record_id))
    print(json.dumps({
        "events": events,
        "feishu_sync": synced,
        "feishu_request_count": client.request_count,
    }, ensure_ascii=False, indent=2))
    return 0 if not any(event["state"] in {"blocked", "confirm_error"} for event in events) else 2


if __name__ == "__main__":
    raise SystemExit(main())
