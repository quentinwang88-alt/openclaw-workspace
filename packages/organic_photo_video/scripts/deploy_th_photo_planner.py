#!/usr/bin/env python3
"""Deploy only the TH Pick Your Look V3 recipe update; never touch Feishu."""
from __future__ import annotations

import argparse
from dataclasses import asdict
import json
from pathlib import Path
import sys


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
for value in (str(WORKSPACE_ROOT), str(PACKAGE_ROOT)):
    if value not in sys.path:
        sys.path.insert(0, value)

from workspace_support import load_repo_env  # noqa: E402
from config.loader import load_content_recipe_file  # noqa: E402
from repositories.rds_repository import RdsRepository  # noqa: E402


RECIPE_PATH = PACKAGE_ROOT / "config/recipes/PHOTO_TH_PICK_YOUR_LOOK_V3.json"
DEPLOYABLE_RECIPES = {
    "PHOTO_TH_PICK_YOUR_LOOK_V3": RECIPE_PATH,
    "PHOTO_TH_TRAVEL_OUTFIT_V2": (
        PACKAGE_ROOT / "config/recipes/PHOTO_TH_TRAVEL_OUTFIT_V2.json"
    ),
}


def comparable(value):
    if value is None:
        return None
    return {
        key: item for key, item in asdict(value).items()
        if key not in {"created_at", "updated_at"}
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply", action="store_true",
    )
    parser.add_argument(
        "--recipe", action="append", default=None,
        metavar="RECIPE_ID",
        help="deploy this recipe id (repeatable); defaults to PHOTO_TH_PICK_YOUR_LOOK_V3",
    )
    args = parser.parse_args()
    recipe_ids = list(args.recipe) if args.recipe else ["PHOTO_TH_PICK_YOUR_LOOK_V3"]
    unknown = [value for value in recipe_ids if value not in DEPLOYABLE_RECIPES]
    if unknown:
        parser.error(
            "unknown recipe(s): " + ", ".join(unknown)
            + "; available: " + ", ".join(sorted(DEPLOYABLE_RECIPES))
        )
    load_repo_env()
    repository = RdsRepository.from_env()
    exit_code = 0
    for recipe_id in recipe_ids:
        code = _deploy_one(
            repository, DEPLOYABLE_RECIPES[recipe_id],
            apply_changes=args.apply,
        )
        exit_code = exit_code or code
    return exit_code


def _deploy_one(repository, recipe_path: Path, *, apply_changes: bool) -> int:
    expected = load_content_recipe_file(recipe_path)
    current = repository.get_content_recipe(expected.recipe_id)
    report = {
        "recipe_id": expected.recipe_id,
        "mode": "apply" if apply_changes else "dry-run",
        "current_version": getattr(current, "recipe_version", None),
        "target_version": expected.recipe_version,
        "already_matches": comparable(current) == comparable(expected),
        "feishu_writes": 0,
        "generated_tasks": 0,
    }
    if current and current.recipe_version > expected.recipe_version:
        raise ValueError("RDS recipe version is newer than this checkout; refusing downgrade")
    if apply_changes and not report["already_matches"]:
        repository.upsert_content_recipe(expected)
        readback = repository.get_content_recipe(expected.recipe_id)
        if comparable(readback) != comparable(expected):
            raise ValueError("recipe readback mismatch after upsert")
        report["action"] = "updated"
    else:
        report["action"] = "unchanged" if report["already_matches"] else "would_update"
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
