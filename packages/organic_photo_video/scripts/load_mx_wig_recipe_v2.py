"""Directed Phase-2 load: upsert ONLY PHOTO_MX_PICK_YOUR_HAIR_V2 into RDS.

Deliberately narrow (the full seed script would rewrite shared TH config):
one recipe row, idempotent, verified by read-back.  Dry-run by default.

Run: /usr/bin/python3 scripts/load_mx_wig_recipe_v2.py [--apply]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
for path in (str(WORKSPACE_ROOT), str(PACKAGE_ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)

from workspace_support import load_repo_env  # noqa: E402

load_repo_env()

from config.loader import load_content_recipe_file  # noqa: E402
from repositories.rds_repository import RdsRepository  # noqa: E402
from services.photo_wig_flow import MX_WIG_RECIPE_ID  # noqa: E402

RECIPE_PATH = PACKAGE_ROOT / "config" / "recipes" / "PHOTO_MX_PICK_YOUR_HAIR_V2.json"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    recipe = load_content_recipe_file(RECIPE_PATH)
    if recipe.recipe_id != MX_WIG_RECIPE_ID:
        print(f"FAIL: unexpected recipe id {recipe.recipe_id}")
        return 2
    repository = RdsRepository.from_env()
    existing = repository.get_content_recipe(MX_WIG_RECIPE_ID)
    if existing is not None:
        same = (existing.recipe_version == recipe.recipe_version
                and existing.status == recipe.status)
        print(f"already loaded: {MX_WIG_RECIPE_ID} v{existing.recipe_version} "
              f"status={existing.status} match={same}")
        return 0
    if not args.apply:
        print(f"DRY-RUN: would upsert {MX_WIG_RECIPE_ID} "
              f"v{recipe.recipe_version} status={recipe.status} "
              f"flow={recipe.recipe_spec_json.get('execution_flow')} "
              f"(re-run with --apply)")
        return 0
    repository.upsert_content_recipe(recipe)
    readback = repository.get_content_recipe(MX_WIG_RECIPE_ID)
    if readback is None or readback.recipe_spec_json.get("execution_flow") != "mx_wig_choice_v1":
        print("FAIL: read-back verification failed")
        return 2
    print(f"OK: loaded {MX_WIG_RECIPE_ID} v{readback.recipe_version} "
          f"status={readback.status} flow=mx_wig_choice_v1 "
          f"pages={len(readback.recipe_spec_json['content_card']['pages'])}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
