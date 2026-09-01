#!/usr/bin/env python3
"""Seed OPV versioned reference data (market packs, themes, render presets).

Dry-run by default. ``--apply`` performs upserts into the opv_ tables via
RdsRepository. The example account profile is documentation only and is never
seeded by this script; use scripts/import_account_profile.py for accounts.

Requires ORGANIC_PHOTO_VIDEO_DATABASE_URL (falls back to LIKEU_AI_DATABASE_URL).
Never calls Feishu. Never prints connection credentials.
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

from config.loader import load_seed_bundle  # noqa: E402
from repositories.rds_repository import RdsRepository  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config-dir",
        default=str(PACKAGE_ROOT / "config"),
        help="OPV config directory (default: package config/)",
    )
    parser.add_argument("--apply", action="store_true", help="actually upsert into RDS")
    parser.add_argument(
        "--deprecate-preset",
        action="append",
        default=[],
        help="explicit legacy preset id to mark deprecated (repeatable; apply mode only)",
    )
    args = parser.parse_args()

    bundle = load_seed_bundle(Path(args.config_dir))
    print("mode=" + ("apply" if args.apply else "dry-run"))
    print(f"config_dir={args.config_dir}")
    for line in bundle.summary_lines():
        print(line)
    for preset_id in args.deprecate_preset:
        print(f"deprecate_preset={preset_id}")
    shipped_ids = {preset.render_preset_id for preset in bundle.render_presets}
    conflicts = [preset_id for preset_id in args.deprecate_preset if preset_id in shipped_ids]
    if conflicts:
        print(
            "refusing to deprecate shipped preset(s): " + ",".join(conflicts),
            file=sys.stderr,
        )
        return 2

    if not args.apply:
        print("no_database_writes=1")
        return 0

    repository = RdsRepository.from_env()
    for pack in bundle.market_packs:
        repository.upsert_market_pack(pack)
    for theme in bundle.themes:
        repository.upsert_theme(theme)
    for preset in bundle.render_presets:
        repository.upsert_render_preset(preset)
    for recipe in bundle.content_recipes:
        repository.upsert_content_recipe(recipe)
    for profile in bundle.render_profiles:
        repository.upsert_render_profile(profile)
    for profile in bundle.quality_profiles:
        repository.upsert_quality_profile(profile)
    for preset_id in args.deprecate_preset:
        repository.set_render_preset_status(preset_id, "deprecated")
    print(
        "upserted: "
        f"{len(bundle.market_packs)} market_packs, "
        f"{len(bundle.themes)} themes, "
        f"{len(bundle.render_presets)} render_presets, "
        f"{len(bundle.content_recipes)} content_recipes, "
        f"{len(bundle.render_profiles)} render_profiles, "
        f"{len(bundle.quality_profiles)} quality_profiles"
    )
    print("account_example_seeded=0 (examples are never seed data)")
    print(f"deprecated_presets={len(args.deprecate_preset)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
