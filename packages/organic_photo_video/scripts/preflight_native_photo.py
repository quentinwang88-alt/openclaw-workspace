#!/usr/bin/env python3
"""Read-only native-photo configuration and asset coverage preflight.

No environment loading, database connection, image generation or remote writes.
Exit 2 means invalid configuration; --require-ready also exits 3 if a Recipe
has no usable execution profile. --verify-files checks local bytes and fonts.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from config.loader import load_board_layouts, load_content_recipes, load_categories
from domain.contracts import PHOTO_RECIPE_V2_SCHEMA_VERSION
from domain.models import AssetSet
from domain.photo_contracts import validate_execution_profiles
from services.asset_set_service import AssetSetService
from services.photo_copy import resolve_photo_copy
from services.photo_content import freeze_content_card


class LocalAssetRepository:
    def __init__(self, assets):
        self.assets = assets

    def list_asset_sets(self, **_kwargs):
        return self.assets


def preflight(config_dir: Path, *, verify_files: bool = False,
              recipe_ids=None) -> dict:
    result = {"mode": "read_only", "external_writes": 0, "errors": [], "recipes": [], "needs_asset": [], "dynamic_input_required": [], "needs_content": [], "canary_market_unbound": []}
    try:
        recipes = [r for r in load_content_recipes(config_dir / "recipes")
                   if r.recipe_spec_json.get("media_kind") == "native_photo"]
        selected = {str(value) for value in (recipe_ids or []) if str(value)}
        if selected:
            recipes = [recipe for recipe in recipes if recipe.recipe_id in selected]
            missing = sorted(selected.difference(recipe.recipe_id for recipe in recipes))
            if missing:
                raise ValueError("unknown native-photo recipe: " + ", ".join(missing))
        layouts = {item.get("layout_id"): item for item in load_board_layouts(config_dir / "layouts")}
        categories = {item["category_key"] for item in load_categories(config_dir / "categories")}
    except Exception as exc:
        result["errors"].append(str(exc))
        return result
    assets = []
    for path in sorted((config_dir / "asset_sets").glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if verify_files:
                from scripts.import_photo_asset_set import normalize
                asset_set = normalize(path)
            else:
                if payload.get("schema_version") != "opv-asset-set-v1":
                    raise ValueError("invalid asset set schema")
                manifest = {"assets": [], "pairs": payload.get("pairs") or [], "content_approval": payload.get("content_approval") or {}}
                for raw in payload.get("assets") or []:
                    source = Path(raw.get("path") or "").expanduser()
                    if not source.is_absolute():
                        source = (path.parent / source).resolve()
                    manifest["assets"].append({**raw, "path": str(source), "sha256": raw.get("sha256") or "0" * 64})
                asset_set = AssetSet(asset_set_id=payload["asset_set_id"], asset_set_key=payload["asset_set_key"],
                    asset_set_version=payload.get("asset_set_version", 1), category_key=payload["category_key"],
                    market=payload.get("market"), status=payload.get("status", "draft"),
                    tags_json=payload.get("tags") or {}, manifest_json=manifest)
            assets.append(asset_set)
        except Exception as exc:
            result["errors"].append(f"{path.name}: {exc}")
    selector = AssetSetService(LocalAssetRepository(assets))
    for recipe in recipes:
        spec = recipe.recipe_spec_json
        errors = validate_execution_profiles(spec)
        # A country-agnostic recipe v2 keeps ``market_policy``,
        # MARKET_PACK_REQUIRED: market and category are bound at request time, so
        # the seed-time preflight verifies the declared capabilities against the
        # shipped Category Adapters instead of a fixed ``category_key``.
        is_v2 = str(spec.get("schema_version") or "") == PHOTO_RECIPE_V2_SCHEMA_VERSION
        if is_v2:
            from services.photo_category_registry import (
                get_photo_category_adapter, registered_category_keys,
            )
            available_capabilities = {
                capability
                for key in registered_category_keys()
                for capability in get_photo_category_adapter(key).capabilities
            }
            missing_capabilities = sorted(
                set(spec.get("required_category_capabilities") or []) - available_capabilities
            )
            if missing_capabilities:
                errors.append(
                    "no shipped category provides " + ", ".join(missing_capabilities)
                )
        elif spec.get("category_key") not in categories:
            errors.append("category is missing")
        layout = layouts.get(spec.get("template_id"))
        if layout is None or layout.get("layout_version") != spec.get("template_version"):
            errors.append("layout version is missing or incompatible")
        report = {"recipe_id": recipe.recipe_id, "recipe_status": recipe.status, "profiles": [], "ready_profile_count": 0}
        markets = list(spec.get("markets") or [])
        for profile in spec.get("execution_profiles") or []:
            if errors:
                break
            if not markets:
                # Canary: there is no market binding to resolve assets against
                # yet, so the profile is reported and left out of needs_asset.
                report["profiles"].append({
                    "profile_id": profile["profile_id"],
                    "copy_variant_count": len(profile.get("copy_variants") or []),
                    "matches": {},
                    "status": "CANARY_MARKET_UNBOUND",
                })
                continue
            matches = {}
            render_candidates = []
            for market in markets:
                try:
                    candidates = selector.candidates(category_key=spec["category_key"], market=market,
                        tags={key: profile["variables"][key] for key in spec["asset_match_keys"] if key in profile["variables"]},
                        asset_set_keys=profile["asset_set_keys"], requirements=spec["asset_requirements"],
                        verify_files=verify_files)
                    qualified = []
                    content_reasons = []
                    for candidate in candidates:
                        try:
                            if recipe.status != "active" or layout.get("schema_version") != "opv-photo-layout-v2":
                                raise ValueError("NEEDS_CONTENT: retired Recipe or non-executable legacy layout")
                            freeze_content_card(profile.get("content_card") or spec.get("content_card"), candidate, spec.get("visual_rules") or {})
                            qualified.append(candidate)
                        except ValueError as exc:
                            content_reasons.append(str(exc))
                    if content_reasons or not (profile.get("content_card") or spec.get("content_card")):
                        if recipe.recipe_id not in result["needs_content"]:
                            result["needs_content"].append(recipe.recipe_id)
                    candidates = qualified
                    matches[market] = [item.asset_set_id for item in candidates]
                    render_candidates.extend((market, item) for item in candidates)
                except Exception as exc:
                    errors.append(str(exc))
            ready = bool(matches) and all(matches.values())
            from services.photo_content_planner import recipe_has_planning_policy
            dynamic = bool(not ready and recipe.status == "active"
                           and recipe_has_planning_policy(recipe.recipe_id))
            report["ready_profile_count"] += int(ready)
            report["profiles"].append({"profile_id": profile["profile_id"], "copy_variant_count": len(profile["copy_variants"]),
                                        "matches": matches, "status": (
                                            "ready" if ready else
                                            "DYNAMIC_INPUT_REQUIRED" if dynamic else "NEEDS_ASSET"
                                        )})
            if verify_files and layout and ready:
                try:
                    from PIL import Image
                    from services.photo_package import normalize_photo_template, _draw_overlay
                    template = normalize_photo_template(layout)
                    # Measure actual localized asset labels, never the placeholder spelling.
                    checks = render_candidates or [(spec["markets"][0], None)]
                    for market, candidate in checks:
                        for variant in profile["copy_variants"]:
                            resolved = resolve_photo_copy(variant["copy"],
                                assets=candidate.manifest_json["assets"] if candidate else [],
                                locale={"TH": "th-TH", "MX": "es-MX"}.get(market, market))
                            for line in resolved["slide_texts"]:
                                _draw_overlay(Image.new("RGB", (template["width"], template["height"])), line, template)
                except Exception as exc:
                    errors.append(f"profile {profile['profile_id']} overlay: {exc}")
        if not report["ready_profile_count"]:
            if is_v2:
                # A canary recipe has no live market binding yet; it must not
                # pollute the actionable needs_asset queue.
                result["canary_market_unbound"].append(recipe.recipe_id)
            else:
                from services.photo_content_planner import recipe_has_planning_policy
                target = (result["dynamic_input_required"]
                          if recipe.status == "active" and recipe_has_planning_policy(recipe.recipe_id)
                          else result["needs_asset"])
                target.append(recipe.recipe_id)
        result["errors"].extend(f"{recipe.recipe_id}: {error}" for error in errors)
        result["recipes"].append(report)
    result["recipe_count"] = len(recipes)
    result["profile_count"] = sum(len(r["profiles"]) for r in result["recipes"])
    result["file_checks"] = verify_files
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config-dir", type=Path, default=PACKAGE_ROOT / "config")
    parser.add_argument("--verify-files", action="store_true")
    parser.add_argument("--require-ready", action="store_true")
    parser.add_argument(
        "--recipe-id", action="append", default=[],
        help="preflight only the selected Recipe; repeat for multiple Recipes",
    )
    args = parser.parse_args()
    result = preflight(
        args.config_dir.resolve(), verify_files=args.verify_files,
        recipe_ids=args.recipe_id,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 2 if result["errors"] else 3 if args.require_ready and result["needs_asset"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
