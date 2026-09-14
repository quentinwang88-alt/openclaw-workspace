#!/usr/bin/env python3
"""Read-only native-photo configuration and asset coverage preflight.

No environment loading, database connection, image generation or remote writes.

两种检查，输出里用 ``check`` 明确区分：

* ``config``（默认）：普通配置检查 —— 配方合同、类目能力、版式版本、素材覆盖。
  只要配置本身没毛病就 ``exit 0``。
* ``production_ready``（``--require-ready``）：生产就绪检查 —— 在此之上要求所选范围内
  **没有静态素材缺口（``needs_asset``）、没有未绑定市场的通用 Recipe
  （``canary_market_unbound``）**，并且（用 ``--preset-name`` 圈定时）所选预设已经
  enabled。未绑定市场的通用 Recipe 不会因为 ``needs_asset`` 为空就被当成就绪。
  动态输入线（素材按任务补的现役产线）只出 ``ready_notes``，不算拦截。

``--preset-name`` 可重复，用来把检查圈到运营真正要开的入口上；它会解析预设的
配方/市场/语言，而不是只比字符串。历史视频入口不在本预检范围内，被点名时报错而
不是静默通过。

Exit 2 means invalid configuration; --require-ready also exits 3 if the selected
scope is not production-ready. --verify-files checks local bytes and fonts.
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


def resolve_preset_scope(config_dir: Path, preset_names, native_photo_ids) -> tuple:
    """把 ``--preset-name`` 解析成「被检查的配方」+ 目录行 + 报错。

    只认原生图文配方：预设指向的其它配方（历史视频线）不算通过，而是明确报出来 ——
    否则 ``--require-ready --preset-name <历史视频预设>`` 会因为"没检查到任何东西"
    而退出 0，那正是本轮要消掉的假就绪。
    """
    from services.feishu_workflow import ProductionPresetCatalog

    catalog = ProductionPresetCatalog(config_dir / "feishu_production_presets.json")
    selected: set = set()
    rows: list = []
    errors: list = []
    for name in preset_names:
        raw = catalog.metadata(name)
        tasks = list(raw.get("tasks") or [])
        # deterministic_one 预设本身没有 tasks，只指向候选项；跟一层即可，
        # 与 ProductionPresetCatalog.resolve 的语义一致。
        for candidate in raw.get("tasks_from") or []:
            tasks.extend(catalog.metadata(str(candidate)).get("tasks") or [])
        recipe_ids = sorted({str(item.get("recipe_id") or "")
                             for item in tasks if item.get("recipe_id")})
        photo_ids = [value for value in recipe_ids if value in native_photo_ids]
        skipped = [value for value in recipe_ids if value not in native_photo_ids]
        row = {
            "name": name,
            "entry_group": catalog.entry_group(name),
            "status": str(raw.get("status", "active")),
            "enabled": str(raw.get("status", "active")) == "active",
            "markets": sorted({str(item.get("market") or "")
                               for item in tasks if item.get("market")}),
            "languages": sorted({str(item.get("language") or "")
                                 for item in tasks if item.get("language")}),
            "native_photo_recipe_ids": photo_ids,
            "outside_native_photo": skipped,
        }
        rows.append(row)
        if not photo_ids:
            errors.append(
                f"预设「{name}」不指向任何原生图文配方（"
                + ("、".join(skipped) or "没有配方") + "），不在本预检范围内"
            )
        selected.update(photo_ids)
        missing = [value for value in recipe_ids if value not in native_photo_ids
                   and value not in skipped]
        if missing:
            errors.append(f"预设「{name}」指向未知配方：" + "、".join(missing))
    return selected, rows, errors


def readiness_blockers(result: dict, presets: list) -> list:
    """硬拦截项（人话版）：这些东西不解决就不算生产就绪。

    只有两类是真拦截：**静态素材缺口**（``needs_asset``）与**未绑定市场的通用
    Recipe**（``canary_market_unbound``，2026-09-14 起不再因为 needs_asset 为空
    就被当成就绪）。动态输入线不在这里 —— 见 ``readiness_notes``。
    """
    blockers: list = []
    for report in result["recipes"]:
        statuses = sorted({str(profile.get("status") or "")
                           for profile in report["profiles"]})
        if "NEEDS_ASSET" in statuses:
            blockers.append(
                f"{report['recipe_id']} 缺静态素材（NEEDS_ASSET），"
                "没有可用的素材集"
            )
        elif "CANARY_MARKET_UNBOUND" in statuses:
            blockers.append(
                f"{report['recipe_id']} 未绑定市场（CANARY_MARKET_UNBOUND），"
                "无法证明这条线能取到素材"
            )
    for row in presets or []:
        if not row["enabled"]:
            blockers.append(
                f"预设「{row['name']}」仍为 {row['status']}，运营还看不到这个入口"
            )
    return blockers


def readiness_notes(result: dict) -> list:
    """说明性提示：不拦生产就绪，但要让人知道运行时会补给什么。"""
    notes: list = []
    for report in result["recipes"]:
        statuses = sorted({str(profile.get("status") or "")
                           for profile in report["profiles"]})
        if "DYNAMIC_INPUT_REQUIRED" in statuses:
            notes.append(
                f"{report['recipe_id']} 的素材按任务动态输入"
                "（参考图 / 商品编码 / 主题），不使用静态素材集"
            )
    for recipe_id in result["needs_content"]:
        notes.append(f"{recipe_id} 的内容卡需要在运行前落定（needs_content）")
    return notes


def settle_readiness(result: dict) -> dict:
    """把就绪判定补齐到结果里；任何返回路径都要经过它，保证 JSON 形状一致。

    生产就绪 = 没有配置错误 + 无硬拦截（静态素材缺口 / 未绑定市场 / 预设未启用）。
    未绑定市场的通用 Recipe 落在 canary_market_unbound，needs_asset 空也照样不就绪；
    动态输入线只出提示，不拦 —— 它本来就是按任务补素材的现役产线。
    """
    result.setdefault("recipe_count", 0)
    result.setdefault("profile_count", 0)
    result.setdefault("file_checks", False)
    result["file_checks"] = result.get("file_checks") or False
    result["not_ready"] = sorted(
        set(result.get("needs_asset") or []) | set(result.get("canary_market_unbound") or [])
    )
    result["ready_blockers"] = readiness_blockers(result, result.get("presets") or [])
    result["ready_notes"] = readiness_notes(result)
    result["ready"] = not result.get("errors") and not result["ready_blockers"]
    return result


def preflight(config_dir: Path, *, verify_files: bool = False,
              recipe_ids=None, preset_names=None, require_ready: bool = False) -> dict:
    result = {"mode": "read_only",
              "check": "production_ready" if require_ready else "config",
              "external_writes": 0, "errors": [], "recipes": [], "presets": [],
              "needs_asset": [], "dynamic_input_required": [], "needs_content": [],
              "canary_market_unbound": [], "not_ready": [], "ready_blockers": [],
              "ready_notes": [], "ready": False,
              "recipe_count": 0, "profile_count": 0, "file_checks": False}
    try:
        recipes = [r for r in load_content_recipes(config_dir / "recipes")
                   if r.recipe_spec_json.get("media_kind") == "native_photo"]
        native_photo_ids = {recipe.recipe_id for recipe in recipes}
        selected = {str(value) for value in (recipe_ids or []) if str(value)}
        # 圈定范围后即使交集为空也必须真的筛成 0 条：把"筛没了"当成"没筛"会
        # 静默放大成整个目录（那正是 preflight 最不该做的事）。
        filtering = bool(selected)
        if preset_names:
            try:
                preset_recipe_ids, result["presets"], preset_errors = resolve_preset_scope(
                    config_dir, preset_names, native_photo_ids)
            except Exception as exc:
                result["errors"].append(str(exc))
                return settle_readiness(result)
            result["errors"].extend(preset_errors)
            # 同时给了 --recipe-id 时取交集：两个筛选器都要满足。
            selected = (selected & preset_recipe_ids) if selected else set(preset_recipe_ids)
            filtering = True
        if filtering:
            recipes = [recipe for recipe in recipes if recipe.recipe_id in selected]
            missing = sorted(selected.difference(recipe.recipe_id for recipe in recipes))
            if missing:
                raise ValueError("unknown native-photo recipe: " + ", ".join(missing))
        layouts = {item.get("layout_id"): item for item in load_board_layouts(config_dir / "layouts")}
        categories = {item["category_key"] for item in load_categories(config_dir / "categories")}
    except Exception as exc:
        result["errors"].append(str(exc))
        return settle_readiness(result)
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
    return settle_readiness(result)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config-dir", type=Path, default=PACKAGE_ROOT / "config")
    parser.add_argument("--verify-files", action="store_true")
    parser.add_argument("--require-ready", action="store_true",
                        help="生产就绪检查：每条被检查的配方必须有已绑定市场的就绪 profile")
    parser.add_argument(
        "--recipe-id", action="append", default=[],
        help="preflight only the selected Recipe; repeat for multiple Recipes",
    )
    parser.add_argument(
        "--preset-name", action="append", default=[],
        help="preflight only the native-photo Recipes reachable from this preset; "
             "repeat for multiple presets",
    )
    args = parser.parse_args()
    result = preflight(
        args.config_dir.resolve(), verify_files=args.verify_files,
        recipe_ids=args.recipe_id, preset_names=args.preset_name,
        require_ready=args.require_ready,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if result["errors"]:
        return 2
    if args.require_ready and not result["ready"]:
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
