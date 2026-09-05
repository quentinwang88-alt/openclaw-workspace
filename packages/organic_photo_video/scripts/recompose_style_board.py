#!/usr/bin/env python3
"""Rework a testing board locally, retaining all photos and garment cutouts."""
import argparse
import copy
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
for value in (ROOT, ROOT.parents[1]):
    sys.path.insert(0, str(value))

from workspace_support import load_repo_env
from repositories.rds_repository import RdsRepository
from services.cutout_assets import refresh_manifest_anchor_pixels
from services.hero_first import HeroFirstProducer
from services.technical_production import TechnicalProductionFlow
from services.video_render_flow import VideoRenderFlow
from services.video_renderer import FFmpegStillRenderer
from services.workflow_v2 import ReworkService, frozen_task_view


class NoPhotoGeneration:
    def generate_shot(self, *args, **kwargs):
        raise RuntimeError("Local board rework must never call an image model")


class LocalMatteDecomposer:
    def __init__(self, repo, manifest):
        self.repo, self.manifest = repo, manifest

    def ensure(self, task, *, anchor_path=""):
        manifest = copy.deepcopy(self.manifest)
        person = manifest["assets"]["person_cutout"]
        source = Path(person["source_path"])
        if not source.is_file():
            raise ValueError("original person source is missing")
        for asset in manifest["assets"].values():
            if hashlib.sha256(Path(asset["path"]).read_bytes()).hexdigest() != asset["sha256"]:
                raise ValueError("original cutout bytes changed")
        folder = Path.home() / ".openclaw/shared/data/organic_photo_video/revisions" / task.active_revision_id / task.task_id / "decomposition_assets/normalized"
        bg = task.plan_json["presentation_profile"]["background_color"]
        manifest = refresh_manifest_anchor_pixels(manifest, anchor_path=source, output_dir=folder, background=bg)
        if manifest["assets"]["person_cutout"]["composition_mode"] != "adaptive_background_matte":
            raise ValueError("local matte ambiguous; original revision preserved")
        plan = copy.deepcopy(task.plan_json)
        plan["decomposition_assets"] = manifest
        for shot in plan["shots"]:
            if shot.get("shot_kind") == "composite_board":
                shot["board_spec"]["decomposition_assets"] = manifest
        self.repo.update_task_plan(task.task_id, plan_json=plan)
        return self.repo.get_task(task.task_id)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task_id")
    parser.add_argument("--parent-revision", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    load_repo_env()
    repo = RdsRepository.from_env()
    task = repo.get_task(args.task_id)
    if not task or repo.get_account_profile(task.account_id).status != "testing":
        raise ValueError("testing account required")
    if repo.list_publish_records_by_task(task.task_id):
        raise ValueError("published/scheduled tasks are excluded")
    parent = repo.get_task_revision(args.parent_revision)
    if not parent or parent.task_id != task.task_id:
        raise ValueError("parent does not belong to task")
    manifest = copy.deepcopy(task.plan_json["decomposition_assets"])
    print(json.dumps({"task": task.task_id, "mode": "apply" if args.apply else "read_only",
                      "model_calls": 0, "reused_items": manifest.get("item_roles")}), flush=True)
    if not args.apply:
        return
    active = repo.get_task_revision(task.active_revision_id)
    if (active and active.parent_revision_id == parent.revision_id
            and (active.rework_spec_json or {}).get("idempotency_key") == "pure-style-adaptive-matte-v1"
            and task.released_revision_id == active.revision_id):
        print(json.dumps({"already_complete": True, "video": repo.get_render(task.selected_render_id).output_url}))
        return
    ReworkService(repo).begin(task.task_id, expected_revision_id=parent.revision_id,
        expected_lock_version=parent.lock_version, scope="board",
        reason="Local adaptive person matte; retain garment cutouts and photos; no model calls",
        idempotency_key="pure-style-adaptive-matte-v1", operator="user_requested_style_acceptance")
    local = LocalMatteDecomposer(repo, manifest)
    local.ensure(frozen_task_view(repo, repo.get_task(task.task_id)))
    TechnicalProductionFlow(repo, HeroFirstProducer(repo, NoPhotoGeneration(), technical_only=True,
        decomposer=local), VideoRenderFlow(repo, FFmpegStillRenderer())).run(
        task.task_id, overlay_profile_id="OVERLAY_LIGHT_V1")
    task = repo.get_task(task.task_id)
    print(json.dumps({"task": task.task_id, "released_revision": task.released_revision_id,
                      "video": repo.get_render(task.selected_render_id).output_url}), flush=True)


if __name__ == "__main__":
    main()
