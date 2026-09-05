#!/usr/bin/env python3
"""Install only the new multi-look recipe/profile, never reseed other settings."""
import argparse
from dataclasses import asdict
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
for value in (ROOT.parents[1], ROOT):
    sys.path.insert(0, str(value))
from workspace_support import load_repo_env
from config.loader import load_content_recipe_file, load_render_profile_file
from repositories.rds_repository import RdsRepository


def equivalent(existing, expected):
    return all(getattr(existing, key, None) == value for key, value in asdict(expected).items()
               if key not in {"created_at", "updated_at"})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    load_repo_env()
    repo = RdsRepository.from_env()
    recipe = load_content_recipe_file(ROOT / "config/recipes/RECIPE_MULTI_LOOK_V1.json")
    profile = load_render_profile_file(ROOT / "config/profiles/IMAGE_MULTI_LOOK_V1.json")
    if repo.get_quality_profile(recipe.quality_profile_id) is None or repo.get_theme("THEME_TH_OUTFIT_BREAKDOWN_V1") is None:
        raise ValueError("existing shared quality profile/theme missing; no writes made")
    targets = [("render_profile", profile, repo.get_render_profile, repo.upsert_render_profile, profile.render_profile_id),
               ("recipe", recipe, repo.get_content_recipe, repo.upsert_content_recipe, recipe.recipe_id)]
    for kind, expected, read, write, identifier in targets:
        existing = read(identifier)
        if kind != "render_profile" and existing is not None and not equivalent(existing, expected):
            raise ValueError(f"{identifier} already exists with different configuration; refusing overwrite")
    for kind, expected, read, write, identifier in targets:
        exists = read(identifier) is not None
        if args.apply and (not exists or kind == "render_profile"):
            write(expected)
        if args.apply and not equivalent(read(identifier), expected):
            raise ValueError(f"{identifier} readback mismatch")
        print(json.dumps({"type": kind, "id": identifier,
            "action": "already_matches" if exists else "created" if args.apply else "would_create",
            "readback": "matched" if args.apply else "not_applied"}), flush=True)
    print("account_bgm_publish_unchanged=1; generated_tasks=0; feishu_writes=0")


if __name__ == "__main__":
    main()
