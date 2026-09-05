#!/usr/bin/env python3
"""Normalize four decomposition assets and attach them to an OPV task plan."""

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
from services.cutout_assets import build_asset_manifest  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task_id")
    parser.add_argument("--person-cutout", required=True)
    parser.add_argument("--target-product", required=True)
    parser.add_argument("--top-inner", required=True)
    parser.add_argument("--bottom", required=True)
    parser.add_argument("--layout", default="LEFT_HERO")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    repository = RdsRepository.from_env()
    task = repository.get_task(args.task_id)
    if task is None:
        raise RuntimeError(f"task not found: {args.task_id}")
    root = (
        WORKSPACE_ROOT / "shared" / "data" / "organic_photo_video"
        / args.task_id / "decomposition_assets" / "normalized"
    )
    manifest = build_asset_manifest(
        task_id=args.task_id,
        sources={
            "person_cutout": args.person_cutout,
            "target_product": args.target_product,
            "top_inner": args.top_inner,
            "bottom": args.bottom,
        },
        output_dir=root,
    )
    plan = dict(task.plan_json or {})
    plan["decomposition_assets"] = manifest
    p1 = next(
        (shot for shot in plan.get("shots", []) if int(shot["slot_index"]) == 1),
        None,
    )
    if not p1 or p1.get("shot_kind") != "composite_board":
        raise RuntimeError("task P1 is not a composite board")
    p1["board_spec"] = {
        **dict(p1.get("board_spec") or {}),
        "layout_id": "LAYOUT_OUTFIT_BREAKDOWN_V2",
        "layout_version": 2,
        "layout_variant": args.layout,
        "decomposition_required": True,
        "decomposition_assets": manifest,
    }
    if args.apply:
        repository.update_task_plan(args.task_id, plan_json=plan)
    print(json.dumps({
        "task_id": args.task_id,
        "mode": "apply" if args.apply else "dry-run",
        "layout": args.layout,
        "manifest": manifest,
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
