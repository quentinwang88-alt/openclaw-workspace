#!/usr/bin/env python3
"""Create/continue OPV production through technical QC and an unpublished video."""

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
from services.content_planner import ContentPlannerService  # noqa: E402
from services.hero_first import HeroFirstProducer  # noqa: E402
from services.image_generator import build_default_photo_generator  # noqa: E402
from services.task_intake import TaskIntakeService, TaskRequest  # noqa: E402
from services.workflow_v2 import ScopedReviewService, workflow_v2_enabled  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account-id", default="OPV_TH_TEST_001")
    parser.add_argument("--product-id", required=True)
    parser.add_argument("--product-name", required=True)
    parser.add_argument("--reference-image", action="append", required=True)
    parser.add_argument("--persona-ref", required=True)
    parser.add_argument("--look-ref", required=True)
    parser.add_argument("--scene-ref", required=True)
    parser.add_argument("--theme-id", required=True)
    parser.add_argument("--recipe-id", default="")
    parser.add_argument("--hook-strategy", default="")
    parser.add_argument("--topic", default="")
    parser.add_argument("--idempotency-key", required=True)
    parser.add_argument(
        "--run-visual-qa",
        action="store_true",
        help="deprecated: independent visual review is now a separate diagnostic command",
    )
    parser.add_argument(
        "--skip-visual-qa",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args()
    if args.run_visual_qa and not args.skip_visual_qa:
        parser.error("生产主链不再调用独立视觉审核；如需诊断请单独使用 run_visual_qa.py")

    references = [str(Path(value).expanduser().resolve()) for value in args.reference_image]
    missing = [value for value in references if not Path(value).is_file()]
    if missing:
        raise FileNotFoundError("product reference missing: " + ", ".join(missing))

    repository = RdsRepository.from_env()
    intake = TaskIntakeService(repository).create_task(TaskRequest(
        account_id=args.account_id,
        product_id=args.product_id,
        product_snapshot={
            "product_id": args.product_id,
            "product_name": args.product_name,
            "category": "outerwear",
            "reference_images": references,
            "planned_persona_ref": args.persona_ref,
            "planned_look_ref": args.look_ref,
            "planned_scene_ref": args.scene_ref,
        },
        theme_id=args.theme_id,
        topic_text=args.topic or None,
        source_type="manual_codex_test",
        source_record_id=args.idempotency_key,
        idempotency_key=args.idempotency_key,
        created_by="user_requested_codex_run",
    ))
    task_id = intake.task.task_id
    if intake.task.task_status == "draft":
        ContentPlannerService(
            repository, asset_reader=LightTryonAssetReader()
        ).plan_task(
            task_id,
            theme_id=args.theme_id,
            topic_text=args.topic or None,
            recipe_id=args.recipe_id or None,
            hook_strategy=args.hook_strategy or None,
        )
    task = repository.get_task(task_id)
    report = None
    if task and workflow_v2_enabled(task):
        from services.technical_production import TechnicalProductionFlow
        from services.video_render_flow import VideoRenderFlow
        from services.video_renderer import FFmpegStillRenderer
        TechnicalProductionFlow(repository,
            HeroFirstProducer(repository, build_default_photo_generator(), technical_only=True),
            VideoRenderFlow(repository, FFmpegStillRenderer())).run(task_id)
    elif task and task.task_status in {"planned", "hero_generating", "image_generating", "failed"}:
        report = HeroFirstProducer(
            repository, build_default_photo_generator()
        ).produce(task_id)
    else:
        report = None

    visual = None

    shots = HeroFirstProducer._latest_per_slot(repository.list_shots(task_id))
    current_status = (repository.get_task(task_id) or intake.task).task_status
    output = {
        "task_id": task_id,
        "created": intake.created,
        "task_status": current_status,
        "persona_ref": args.persona_ref,
        "generation": {
            "hero_ok": report.hero_ok if report else None,
            "slots": [
                {
                    "slot_index": shot.slot_index,
                    "status": shot.shot_status,
                    "qa_status": shot.qa_status,
                    "image_path": shot.image_url,
                }
                for shot in shots
            ],
        },
        "visual_qa": {
            "passed": visual.passed,
            "group_score": visual.group.score,
            "group_dimensions": visual.group.dimensions,
            "reason_codes": visual.group.reason_codes,
        } if visual else None,
        "next_gate": (
            "explicit_publish_confirmation" if workflow_v2_enabled(repository.get_task(task_id))
            else "anchor_visual_review" if current_status == "anchor_review"
            else "human_group_approval_before_video_render"
        ),
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
