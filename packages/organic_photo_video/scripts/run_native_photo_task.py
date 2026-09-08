#!/usr/bin/env python3
"""Create or resume one ASSET_REUSE native-photo task through package review.

The command stops at ``photo_packaging``.  It never approves, queues or
publishes content.  Use ``review_native_photo_package.py`` after an operator
has inspected all ordered final slides.
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
from config.loader import load_board_layouts  # noqa: E402
from repositories.rds_repository import RdsRepository  # noqa: E402
from services.hero_first import HeroFirstProducer  # noqa: E402
from services.image_generator import GenerationOutcome  # noqa: E402
from services.photo_package import NativePhotoProductionFlow  # noqa: E402
from services.photo_planner import PhotoReusePlannerService  # noqa: E402
from services.task_intake import TaskIntakeService, TaskRequest  # noqa: E402


class NoAiGenerator:
    def generate_shot(self, _request):
        return GenerationOutcome(ok=False, error="native-photo ASSET_REUSE cannot call an AI image model")


def _layout(layout_id: str) -> dict:
    layouts = {str(item.get("layout_id") or item.get("template_id")): item
               for item in load_board_layouts()}
    if layout_id not in layouts:
        raise ValueError(f"photo layout is missing: {layout_id}")
    return layouts[layout_id]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("request_json", type=Path)
    parser.add_argument("--output-root", type=Path,
                        default=Path.home() / ".openclaw/shared/data/organic_photo_video/photo_packages")
    args = parser.parse_args()
    request = json.loads(args.request_json.read_text(encoding="utf-8"))
    required = {"account_id", "recipe_id", "category_key", "variables", "copy", "asset_set_id"}
    missing = sorted(key for key in required if key not in request)
    if missing:
        raise ValueError("request is missing: " + ", ".join(missing))

    load_repo_env()
    repository = RdsRepository.from_env()
    intake = TaskIntakeService(repository).create_task(TaskRequest(
        account_id=str(request["account_id"]),
        product_id=(str(request.get("product_id") or "") or None),
        product_snapshot=dict(request.get("product_snapshot") or {}),
        media_kind="native_photo", category_key=str(request["category_key"]),
        product_mode=str(request.get("product_mode") or "NO_PRODUCT"),
        topic_text=(str(request.get("topic") or "") or None),
        source_type=str(request.get("source_type") or "native_photo_cli"),
        source_record_id=(str(request.get("source_record_id") or "") or None),
        feishu_record_id=(str(request.get("feishu_record_id") or "") or None),
        requested_shot_count=5, created_by=str(request.get("operator") or "native_photo_cli"),
        idempotency_key=str(request.get("idempotency_key") or args.request_json.resolve()),
    ))
    task = repository.get_task(intake.task.task_id) or intake.task
    if task.task_status == "draft":
        recipe = repository.get_content_recipe(str(request["recipe_id"]))
        if recipe is None:
            raise ValueError("recipe is not seeded in RDS; run seed_reference_data.py --apply")
        spec = dict(recipe.recipe_spec_json or {})
        layout = _layout(str(spec.get("template_id") or ""))
        PhotoReusePlannerService(repository).plan_task(
            task.task_id, recipe_id=recipe.recipe_id,
            variables=dict(request["variables"]), copy_block=dict(request["copy"]),
            layout=layout, asset_set_id=str(request["asset_set_id"]),
            operator=str(request.get("operator") or "native_photo_cli"),
        )
        task = repository.get_task(task.task_id)
    plan = task.plan_json or {}
    layout_id = str((plan.get("template") or {}).get("id") or "")
    flow = NativePhotoProductionFlow(
        repository,
        HeroFirstProducer(repository, NoAiGenerator(), output_root=args.output_root,
                          technical_only=True),
        output_root=args.output_root,
    )
    result = flow.prepare(task.task_id, template=_layout(layout_id))
    result.update(created=intake.created, ai_image_calls=0,
                  next_action="inspect slides, then run review_native_photo_package.py")
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
