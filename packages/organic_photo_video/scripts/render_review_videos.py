#!/usr/bin/env python3
"""Formally approve OPV groups and render a release-path master.

For an inspection-only video use ``render_review_previews.py``.  This command
mutates the workflow and therefore requires an explicit acknowledgement.
"""

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
from services.video_render_flow import VideoRenderFlow  # noqa: E402
from services.video_renderer import FFmpegStillRenderer  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task_id", nargs="+", help="explicit OPV task ids")
    parser.add_argument(
        "--approve-formal",
        action="store_true",
        help="acknowledge that this moves image_review into the formal render path",
    )
    parser.add_argument("--reviewer", default="user_requested_codex_review")
    parser.add_argument(
        "--notes",
        default="用户确认将五图拼接为本地预览成片；不授权发布",
    )
    parser.add_argument(
        "--overlay-profile",
        default="",
        help="optional approved text overlay profile, e.g. OVERLAY_LIGHT_V1",
    )
    args = parser.parse_args()
    if not args.approve_formal:
        parser.error(
            "formal approval is required; use render_review_previews.py for a non-release preview"
        )

    repository = RdsRepository.from_env()
    flow = VideoRenderFlow(repository, FFmpegStillRenderer())
    results = []
    for task_id in args.task_id:
        task = repository.get_task(task_id)
        if task is None:
            raise RuntimeError(f"task not found: {task_id}")
        from services.workflow_v2 import workflow_v2_enabled
        if workflow_v2_enabled(task):
            from services.technical_production import TechnicalCheckService
            checks = TechnicalCheckService(repository)
            if task.task_status == "image_review":
                checks.group(task_id)
                task = flow.approve_group(task_id, "opv_technical_pipeline", args.notes, approval_mode="technical")
            elif task.task_status == "video_review" and args.overlay_profile:
                raise RuntimeError("V2 re-edit requires an explicit child rework revision")
        elif task.task_status == "image_review":
            task = flow.approve_group(task_id, args.reviewer, args.notes)
        elif task.task_status == "video_review" and args.overlay_profile:
            task = repository.transition_task(
                task_id, "video_review", "rendering"
            )
        if task.task_status == "rendering":
            render = flow.render(
                task_id,
                overlay_profile_id=args.overlay_profile or None,
            )
        elif task.task_status == "video_review":
            rows = ([repository.get_render(task.selected_render_id)] if workflow_v2_enabled(task)
                    else repository.list_renders(task_id))
            if not rows:
                raise RuntimeError(f"video_review task has no render: {task_id}")
            render = rows[0]
        else:
            raise RuntimeError(
                f"task {task_id} cannot render from status {task.task_status!r}"
            )
        if workflow_v2_enabled(task) and render.qc_status == "passed":
            checks.render(task_id)
            render = repository.get_render(render.render_id)
        results.append(
            {
                "task_id": task_id,
                "render_id": render.render_id,
                "render_status": render.render_status,
                "qc_status": render.qc_status,
                "duration_ms": render.duration_ms,
                "output_path": render.output_url,
                "publish_ready": bool(render.publish_ready),
                "overlay_profile_id": args.overlay_profile or None,
            }
        )
    print(json.dumps(results, ensure_ascii=False, indent=2))
    return 0 if all(row["qc_status"] == "passed" for row in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
