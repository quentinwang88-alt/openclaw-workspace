#!/usr/bin/env python3
"""Render local P1 layout variants from an existing outfit-breakdown task.

The command is read-only against RDS and never calls the photo model. It uses
the selected anchor image and frozen task snapshots, then writes local PNGs.
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
from services.composite_board_renderer import CompositeBoardRenderer  # noqa: E402
from services.hero_first import HeroFirstProducer  # noqa: E402
from services.image_generator import ShotGenerationRequest  # noqa: E402


# The selected production board defaults to LEFT_HERO; this review command
# renders the other three supported composition directions.
DEFAULT_LAYOUTS = ["RIGHT_HERO", "CENTER_HERO", "BOTTOM_GRID"]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task_id")
    parser.add_argument("--layout", action="append", dest="layouts")
    parser.add_argument(
        "--layout-id",
        default="",
        help="Override the frozen layout id for a local, no-model preview.",
    )
    parser.add_argument("--output-dir", default="")
    args = parser.parse_args()

    repository = RdsRepository.from_env()
    task = repository.get_task(args.task_id)
    if task is None:
        raise RuntimeError(f"task not found: {args.task_id}")
    plan = task.plan_json or {}
    anchor_slot = int(plan.get("anchor_slot") or 2)
    shots = HeroFirstProducer._latest_per_slot(repository.list_shots(args.task_id))
    anchor = next(
        (
            shot for shot in shots
            if shot.slot_index == anchor_slot and shot.is_selected and shot.image_url
        ),
        None,
    ) or next(
        (shot for shot in shots if shot.slot_index == anchor_slot and shot.image_url),
        None,
    )
    if anchor is None:
        raise RuntimeError(f"task has no generated P{anchor_slot} anchor")
    source_shot = next(
        (item for item in plan.get("shots", []) if int(item["slot_index"]) == 1),
        None,
    )
    if not source_shot or source_shot.get("shot_kind") != "composite_board":
        raise RuntimeError("task P1 is not an outfit composite board")

    output_dir = Path(args.output_dir) if args.output_dir else (
        PACKAGE_ROOT.parents[1]
        / "shared" / "data" / "organic_photo_video" / args.task_id
        / "board_variants"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    renderer = CompositeBoardRenderer()
    results = []
    default_layouts = ["REF_LEFT_HERO"] if args.layout_id else DEFAULT_LAYOUTS
    for layout_name in args.layouts or default_layouts:
        plan_shot = dict(source_shot)
        plan_shot["board_spec"] = {
            **dict(source_shot.get("board_spec") or {}),
            "layout_variant": layout_name,
            **({"layout_id": args.layout_id} if args.layout_id else {}),
        }
        outcome = renderer.generate_shot(ShotGenerationRequest(
            task_id=f"{args.task_id}_{layout_name}",
            slot_index=1,
            slot_role="hero",
            shot_version=1,
            plan_shot=plan_shot,
            product=dict((task.product_snapshot_json or {}).get("product") or {}),
            persona_snapshot=dict(((plan.get("persona") or {}).get("snapshot")) or {}),
            look_snapshot=dict(((plan.get("look") or {}).get("snapshot")) or {}),
            scene_snapshot=dict(((plan.get("scene") or {}).get("snapshot")) or {}),
            output_dir=str(output_dir),
            continuity_reference_images=[str(anchor.image_url)],
            recipe_execution=dict(plan.get("recipe_execution") or {}),
        ))
        if not outcome.ok:
            raise RuntimeError(f"{layout_name}: {outcome.error}")
        results.append({
            "layout_variant": layout_name,
            "image_path": outcome.image_path,
            "width": outcome.width,
            "height": outcome.height,
        })
    print(json.dumps(results, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
