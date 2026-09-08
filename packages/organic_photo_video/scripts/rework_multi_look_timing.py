#!/usr/bin/env python3
"""Re-render an unpublished multi-look task at its frozen six-second cadence."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
for value in (ROOT.parents[1], ROOT): sys.path.insert(0, str(value))
from workspace_support import load_repo_env
from repositories.rds_repository import RdsRepository
from services.hero_first import HeroFirstProducer
from services.image_generator import build_default_photo_generator
from services.multi_look_planner import multi_look_durations
from services.technical_production import TechnicalProductionFlow
from services.video_render_flow import VideoRenderFlow
from services.video_renderer import FFmpegStillRenderer
from services.workflow_v2 import ReworkService
from services.content_package import advance_package_for_task
from domain.statuses import PACKAGE_QA_REVIEW

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task_id")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    load_repo_env(); repo = RdsRepository.from_env(); task = repo.get_task(args.task_id)
    if not task or repo.get_account_profile(task.account_id).status != "testing" or repo.list_publish_records_by_task(args.task_id):
        raise ValueError("only unpublished testing tasks are allowed")
    active = repo.get_task_revision(task.active_revision_id)
    existing = next((r for r in repo.list_task_revisions(args.task_id)
                     if (r.rework_spec_json or {}).get("idempotency_key") == "multi-look-six-seconds-v1"), None)
    parent = repo.get_task_revision(existing.parent_revision_id) if existing else active
    count = len(parent.plan_snapshot_json["plan"]["shots"])
    timing = {index: value for index, value in enumerate(multi_look_durations(count), 1)}
    if (task.task_status == "rendering"
            and (active.rework_spec_json or {}).get("scope") == "render"):
        # An interrupted command may leave a finished, technically valid render
        # before the final review transition.  Continue that exact revision;
        # never create another render or regenerate its source images.
        pass
    elif task.task_status == "video_review" and (active.rework_spec_json or {}).get("scope") == "timing":
        ReworkService(repo).begin(args.task_id, expected_revision_id=active.revision_id,
            expected_lock_version=active.lock_version, scope="render",
            reason="Retry six-second render after profile-aware media QC update",
            idempotency_key="multi-look-six-seconds-render-v1", operator="user_requested_timing_rework")
    else:
        ReworkService(repo).begin(args.task_id, expected_revision_id=parent.revision_id,
            expected_lock_version=parent.lock_version, scope="timing", timing_ms=timing,
            reason="Shorten five-look reading cadence to six seconds; reuse all selected images",
            idempotency_key="multi-look-six-seconds-v1", operator="user_requested_timing_rework")
        advance_package_for_task(repo, args.task_id, PACKAGE_QA_REVIEW)
    TechnicalProductionFlow(repo, HeroFirstProducer(repo, build_default_photo_generator(), technical_only=True),
        VideoRenderFlow(repo, FFmpegStillRenderer())).run(args.task_id, overlay_profile_id="OVERLAY_LIGHT_V1")
    task = repo.get_task(args.task_id); render = repo.get_render(task.selected_render_id)
    result = {"task_id": task.task_id, "video": render.output_url, "duration_ms": render.qc_json.get("duration_ms"),
              "reused_images": True, "publish_records": len(repo.list_publish_records_by_task(args.task_id))}
    args.output.parent.mkdir(parents=True, exist_ok=True); args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2))
    print(json.dumps(result, ensure_ascii=False))
if __name__ == "__main__": main()
