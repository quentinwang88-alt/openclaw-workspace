#!/usr/bin/env python3
"""Seed OPV versioned reference data (market packs, themes, render presets).

Dry-run by default. ``--apply`` performs upserts into the opv_ tables via
RdsRepository. The example account profile is documentation only and is never
seeded by this script; use scripts/import_account_profile.py for accounts.

``--only``（可重复）把 upsert 收窄到指定实体 id：开一条新市场线时只推该线的
市场包 / 配方 / profile，**不覆盖既有 TH/MX 条目**。未命中的 id 直接报错退出，
避免拼错之后"什么也没写"却被当成部署成功。

Requires ORGANIC_PHOTO_VIDEO_DATABASE_URL (falls back to LIKEU_AI_DATABASE_URL).
Never calls Feishu. Never prints connection credentials.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parents[1]
# PACKAGE_ROOT 必须排在 WORKSPACE_ROOT **之前**：工作区根目录有个同名的 config.py，
# 顺序反了 `import config` 会落到它上面（报 "'config' is not a package"）。用
# `if ... not in sys.path` 判断是不行的 —— 调用方带 PYTHONPATH=.:tests 时包目录已经
# 在 sys.path 里，于是不再前置，反而被后插的 WORKSPACE_ROOT 顶到前面。
for path in (str(WORKSPACE_ROOT), str(PACKAGE_ROOT)):
    if path in sys.path:
        sys.path.remove(path)
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
    parser.add_argument(
        "--only",
        action="append",
        default=[],
        help="upsert only these entity ids (repeatable). Unmatched ids abort; "
             "without --only the whole bundle is written as before.",
    )
    args = parser.parse_args()

    bundle = load_seed_bundle(Path(args.config_dir))
    print("mode=" + ("apply" if args.apply else "dry-run"))
    print(f"config_dir={args.config_dir}")
    for line in bundle.summary_lines():
        print(line)
    for preset_id in args.deprecate_preset:
        print(f"deprecate_preset={preset_id}")

    # 收窄范围：开一条新线只推它自己的条目，既有市场原样不动。
    # key = SeedBundle 上的列表名；value = 该实体的 id 属性名。
    id_attrs = {
        "market_packs": "market_pack_id",
        "themes": "theme_id",
        "render_presets": "render_preset_id",
        "content_recipes": "recipe_id",
        "render_profiles": "render_profile_id",
        "quality_profiles": "quality_profile_id",
    }
    wanted = {str(value) for value in args.only if str(value)}
    scope = None
    if wanted:
        scope = {
            kind: [item for item in getattr(bundle, kind)
                   if str(getattr(item, attr) or "") in wanted]
            for kind, attr in id_attrs.items()
        }
        matched = {str(getattr(item, id_attrs[kind]) or "")
                   for kind in id_attrs for item in scope[kind]}
        unknown = sorted(wanted - matched)
        if unknown:
            print(
                "unknown --only id(s), nothing was written: " + ",".join(unknown),
                file=sys.stderr,
            )
            return 2
        print("scope=" + ",".join(sorted(wanted)))
        for kind, attr in id_attrs.items():
            for item in scope[kind]:
                print(f"  in_scope {kind[:-1]} {getattr(item, attr)}")

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

    chosen = scope or {kind: getattr(bundle, kind) for kind in id_attrs}
    repository = RdsRepository.from_env()
    for pack in chosen["market_packs"]:
        repository.upsert_market_pack(pack)
    for theme in chosen["themes"]:
        repository.upsert_theme(theme)
    for preset in chosen["render_presets"]:
        repository.upsert_render_preset(preset)
    for recipe in chosen["content_recipes"]:
        repository.upsert_content_recipe(recipe)
    for profile in chosen["render_profiles"]:
        repository.upsert_render_profile(profile)
    for profile in chosen["quality_profiles"]:
        repository.upsert_quality_profile(profile)
    for preset_id in args.deprecate_preset:
        repository.set_render_preset_status(preset_id, "deprecated")
    print(
        "upserted: "
        f"{len(chosen['market_packs'])} market_packs, "
        f"{len(chosen['themes'])} themes, "
        f"{len(chosen['render_presets'])} render_presets, "
        f"{len(chosen['content_recipes'])} content_recipes, "
        f"{len(chosen['render_profiles'])} render_profiles, "
        f"{len(chosen['quality_profiles'])} quality_profiles"
    )
    print("account_example_seeded=0 (examples are never seed data)")
    print(f"deprecated_presets={len(args.deprecate_preset)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
