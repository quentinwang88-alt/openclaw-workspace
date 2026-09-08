"""Read-only admission of the actual frozen generation inputs, not account defaults."""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Mapping

from services.asset_compatibility import compatibility_errors, uses_pure_color, scene_preference_mismatch
from services.look_selection import supports_dynamic_items, is_dress_recipe
from services.persona_pack import build_persona_pack, evaluate_persona_pack


class AssetReadinessError(ValueError):
    pass


def local_files(values):
    paths = []
    for value in values or []:
        raw = value.get("local_path", "") if isinstance(value, Mapping) else value
        path = Path(str(raw or "")).expanduser()
        if path.is_file() and path.stat().st_size > 0:
            paths.append(str(path))
    return list(dict.fromkeys(paths))


def check_generation_assets(account, product, plan):
    """Missing authoritative inputs block all modes; thin assets never become production-ready."""
    issues = []
    if account is None:
        return {"version": "asset-admission-v1", "ready": False, "mode": "blocked",
                "issues": [{"severity": "error", "code": "account_missing", "message": "账户不存在，禁止生成"}], "capabilities": {}}
    active = account.status == "active"
    rules = account.operating_rules_json or {}
    def issue(code, message, severity="error"):
        issues.append({"code": code, "message": message, "severity": severity})
    if account.status not in {"testing", "active"}:
        issue("account_inactive", "账户不在可生产或试生产状态")
    if plan.get("media_kind") == "native_photo":
        shots = list(plan.get("shots") or [])
        if shots and all(item.get("shot_kind") == "reused_asset" for item in shots):
            for shot in shots:
                path = Path(str(shot.get("asset_path") or "")).expanduser()
                expected = str(shot.get("asset_sha256") or "")
                if (not path.is_file() or not expected
                        or hashlib.sha256(path.read_bytes()).hexdigest() != expected):
                    issue("reused_asset_changed", "冻结复用素材缺失或哈希已变化")
            return {
                "version": "asset-admission-v2", "ready": not any(
                    item["severity"] == "error" for item in issues
                ),
                "mode": "asset_reuse", "issues": issues,
                "selected_persona_ref": None,
                "capabilities": {"ai_image_calls": 0, "product_reference_required": False},
            }
    persona = (plan.get("persona") or {}).get("snapshot") or {}
    look = (plan.get("look") or {}).get("snapshot") or {}
    scene = (plan.get("scene") or {}).get("snapshot") or {}
    refs = local_files(product.get("reference_images"))
    persona_refs = local_files(persona.get("local_reference_images"))
    minimum = max(1, int(rules.get(
        "persona_min_local_refs_active" if active else "persona_min_local_refs_testing",
        3 if active else 1,
    )))
    if not refs:
        issue("product_refs_missing", "商品图包没有可读取的本地图片")
    if len(persona_refs) < minimum:
        issue("persona_refs_insufficient", f"实际选中人物需至少 {minimum} 张本地参考图，当前 {len(persona_refs)} 张")
    if active and persona.get("status") != "enabled":
        issue("persona_not_production_enabled", "正式生产人物必须在原素材库标记 enabled")
    if not active and len(persona_refs) < 3:
        issue("persona_testing_only", "人物参考不足 3 张，仅适合受控试生产", "warning")
    if str(plan.get("presentation_type") or "") == "SCENE_MODEL" and persona:
        # Human-scene production needs typed identity evidence; a single
        # close-up selfie can no longer pass admission.
        for item in evaluate_persona_pack(build_persona_pack(persona))["issues"]:
            issue(item["code"], item["message"])
    for owner, assets in (("persona", persona.get("reference_assets")),
                          ("product", product.get("reference_assets"))):
        for asset in assets or []:
            path = Path(str(asset.get("local_path") or ""))
            expected = asset.get("sha256")
            if expected and (not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected):
                issue("reference_hash_changed", f"{owner} 冻结参考文件缺失或已改变")
    recipe = look.get("recipe") or {}
    if not look:
        issue("look_snapshot_missing", "缺少已选穿搭快照")
    if active and look.get("status") != "enabled":
        issue("look_not_production_enabled", "正式生产穿搭模板必须标记 enabled")
    dynamic_dress = is_dress_recipe(recipe) and supports_dynamic_items(plan)
    required_roles = () if dynamic_dress and (recipe.get("onepiece") or recipe.get("dress")) else (("top_inner",) if dynamic_dress else ("top_inner", "bottom"))
    for key in required_roles:
        if not str(recipe.get(key) or "").strip():
            issue("look_incomplete", f"穿搭模板缺少 {key}")
    if ((plan.get("recipe") or {}).get("content_goal") == "outfit_breakdown"
            or (plan.get("recipe") or {}).get("id") == "RECIPE_OUTFIT_BREAKDOWN_V1"):
        if is_dress_recipe(recipe) and not supports_dynamic_items(plan):
            issue("decomposition_roles_incompatible", "三件式拆解模板不适用于无独立下装的连衣裙组合")
    if not str(recipe.get("footwear") or "").strip():
        new_policy = uses_pure_color(plan) or (plan.get("look_selection") or {}).get("mode") == "auto_library"
        issue("footwear_unspecified", "穿搭缺少鞋履；新模式允许有来源标记的中性鞋补充", "error" if active and not new_policy else "warning")
    if ((look.get("recipe_field_sources") or {}).get("footwear") or {}).get("source") == "policy_neutral_fallback":
        issue("footwear_neutral_fallback", "原模板未注明鞋履，已明确使用无品牌中性平底运动鞋", "warning")
    if not scene or not scene.get("prompt_core"):
        issue("scene_snapshot_missing", "缺少可执行的场景快照")
    if active and scene.get("status") != "enabled":
        issue("scene_not_production_enabled", "正式生产场景必须标记 enabled")
    # Frozen presentation policy owns compatibility, not today's account config.
    pure_color = uses_pure_color(plan)
    scene_ref = (plan.get("scene") or {}).get("ref_id", "")
    for mismatch in compatibility_errors(look, product, scene_ref, scene, pure_color=pure_color):
        issue("incompatible_assets", f"当前商品、穿搭、场景不兼容：{mismatch}")
    if pure_color and scene_preference_mismatch(look, scene_ref, scene):
        issue("scene_preference_only", "纯色背景：场合标签仅作搭配偏好，不限制使用该穿搭", "warning")
    detail = bool(local_files((product.get("reference_roles") or {}).get("detail")))
    for shot in plan.get("shots") or []:
        contract = shot.get("composition_contract") or {}
        framing = str(contract.get("framing") or shot.get("framing") or "").lower()
        if not detail and (shot.get("requires_detail_reference") or "macro" in framing):
            issue("detail_reference_required", "本镜头需要细节参考，当前图包仅支持穿搭及结构中景")
    if not detail:
        issue("no_detail_reference", "无真实细节参考：禁用微距工艺证明", "warning")
    styling_roles = (("onepiece",) if recipe.get("onepiece") or recipe.get("dress") else ("top_inner",)) if dynamic_dress else ("top_inner", "bottom")
    derived = [role for role in styling_roles
               if not local_files([(look.get("item_refs") or {}).get(role)])]
    if derived:
        issue("derived_styling_items", "配套单品使用锚点派生图，不是真实单品实拍凭证", "warning")
    return {
        "version": "asset-admission-v1", "ready": not any(x["severity"] == "error" for x in issues),
        "mode": "production" if active else "testing", "issues": issues,
        "selected_persona_ref": (plan.get("persona") or {}).get("ref_id"),
        "capabilities": {"detail_proof": detail, "derived_styling_roles": derived},
    }


def require_generation_assets(account, product, plan):
    report = check_generation_assets(account, product, plan)
    if not report["ready"]:
        raise AssetReadinessError("; ".join(x["message"] for x in report["issues"] if x["severity"] == "error"))
    return report
