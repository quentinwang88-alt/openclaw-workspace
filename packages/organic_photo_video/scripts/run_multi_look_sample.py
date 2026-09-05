#!/usr/bin/env python3
"""Produce one unpublished testing sample for the five-look recipe."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
for value in (ROOT.parents[1], ROOT):
    sys.path.insert(0, str(value))

from workspace_support import load_repo_env
from repositories.rds_repository import RdsRepository
from services.asset_resolver import LightTryonAssetReader
from services.content_story import generate_product_image_story
from services.hero_first import HeroFirstProducer
from services.image_generator import OpenAIImageGenerator
from services.product_reference_resolver import ProductReferenceResolver
from services.technical_production import TechnicalProductionFlow
from services.video_render_flow import VideoRenderFlow
from services.video_renderer import FFmpegStillRenderer


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("product_id")
    parser.add_argument("--run-key", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    load_repo_env()
    repo = RdsRepository.from_env()
    account = repo.get_account_profile("OPV_TH_TEST_001")
    if account is None or account.status != "testing":
        raise ValueError("this runner is restricted to the testing account")
    product = ProductReferenceResolver(repo).resolve_snapshot(args.product_id,
        account_id=account.account_id, selection_key=args.run_key, allow_history_bootstrap=False)
    rows = generate_product_image_story(repo, product_id=args.product_id, market="TH", language="th-TH",
        recipe_id="RECIPE_MULTI_LOOK_V1", theme_id="THEME_TH_OUTFIT_BREAKDOWN_V1",
        hook_strategy="multi_look", account_id=account.account_id, variant_count=1,
        product_snapshot=product, asset_reader=LightTryonAssetReader(),
        source_type="opv_multi_look_acceptance", source_record_id_prefix=args.run_key,
        operator="user_requested_multi_look_sample", persona_ref="TH_APPAREL_SELECTED_01_001")
    task_id = rows[0]["task_id"]
    task = repo.get_task(task_id)
    plan = task.plan_json
    if len(plan.get("shots") or []) != len(plan.get("outfit_sequence") or []) or len(plan.get("shots") or []) < 1:
        raise ValueError("frozen multi-look plan is incomplete")
    states = [shot.get("outfit_state_ref") for shot in plan["shots"]]
    if states != [f"LOOK_{i:02d}" for i in range(1, len(states) + 1)]:
        raise ValueError("page states are not sequentially frozen")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps({"task_id": task_id, "status": "planned", "plan": plan,
        "model_calls_authorized": True, "publish": False}, ensure_ascii=False, indent=2, default=str))
    print(json.dumps({"task_id": task_id, "pages": len(states), "looks": [s["look_ref"] for s in plan["outfit_sequence"]],
                      "phase": "generation_start", "published": False}, ensure_ascii=False), flush=True)
    TechnicalProductionFlow(repo, HeroFirstProducer(repo, OpenAIImageGenerator(), technical_only=True),
        VideoRenderFlow(repo, FFmpegStillRenderer())).run(task_id, overlay_profile_id="OVERLAY_LIGHT_V1")
    task = repo.get_task(task_id)
    render = repo.get_render(task.selected_render_id)
    result = {"task_id": task_id, "status": task.task_status, "active_revision": task.active_revision_id,
        "released_revision": task.released_revision_id, "video": render.output_url, "timeline": render.timeline_json,
        "media_qc": render.qc_json, "publish_records": len(repo.list_publish_records_by_task(task_id)), "published": False}
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    print(json.dumps(result, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
