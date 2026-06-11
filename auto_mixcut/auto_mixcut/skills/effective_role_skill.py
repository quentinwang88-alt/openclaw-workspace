from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from auto_mixcut.core.result import Result

from .ai_segment_factory_config import AISegmentFactoryConfig, get_config
from .context import SkillContext
from .hard_subtitle_policy import is_repairable_bottom_caption, is_unusable_hard_subtitle


class EffectiveRoleSkill:
    def __init__(self, ctx: SkillContext, config: Optional[AISegmentFactoryConfig] = None):
        self.ctx = ctx
        self.config = config or get_config()

    def compute_product(self, product_id: str, source_types: list[str] | None = None) -> Result:
        segments = _segments_for_source_types(self.ctx, product_id, source_types)
        latest_tags = _latest_tags_by_segment(self.ctx, [str(s.get("segment_id") or "") for s in segments])
        assets = _assets_by_id(self.ctx, [str(s.get("asset_id") or "") for s in segments])
        results = []
        for segment in segments:
            segment_id = str(segment.get("segment_id") or "")
            asset = assets.get(str(segment.get("asset_id") or "")) or {}
            tag = latest_tags.get(segment_id) or {}
            roles, reason = _compute_roles(segment, asset, tag, self.config)
            write = self.ctx.repo.update(
                "segments", "segment_id", segment_id,
                {"effective_roles_json": roles, "effective_roles_updated_at": datetime.utcnow().isoformat(timespec="seconds"), "effective_roles_reason": reason},
            )
            if not write.success:
                results.append(write.to_dict())
                continue
            results.append(Result.ok({"segment_id": segment_id, "effective_roles": roles, "reason": reason}).to_dict())
        self.ctx.repo.update("content_tasks", "product_id", product_id, {"task_status": "EFFECTIVE_ROLES_COMPUTED"})
        return Result.ok({"count": len(results), "results": results, "source_types": source_types or []})

    def compute_segment(self, segment_id: str) -> Result:
        segment = self.ctx.repo.get("segments", "segment_id", segment_id)
        if not segment:
            return Result.fail("SEGMENT_NOT_FOUND", "segment not found", {"segment_id": segment_id})
        tag = _latest_tag(self.ctx, segment_id)
        asset = self.ctx.repo.get("assets", "asset_id", segment["asset_id"]) or {}
        roles, reason = _compute_roles(segment, asset, tag, self.config)
        self.ctx.repo.update(
            "segments", "segment_id", segment_id,
            {"effective_roles_json": roles, "effective_roles_updated_at": datetime.utcnow().isoformat(timespec="seconds"), "effective_roles_reason": reason},
        )
        return Result.ok({"segment_id": segment_id, "effective_roles": roles, "reason": reason})


def _latest_tag(ctx: SkillContext, segment_id: str) -> Dict[str, Any]:
    rows = ctx.repo.list_where("segment_tags", "segment_id=? ORDER BY id DESC", (segment_id,))
    return rows[0] if rows else {}


def _latest_tags_by_segment(ctx: SkillContext, segment_ids: list[str]) -> Dict[str, Dict[str, Any]]:
    segment_ids = [str(item) for item in segment_ids if str(item or "").strip()]
    if not segment_ids:
        return {}
    latest: Dict[str, Dict[str, Any]] = {}
    for chunk in _chunks(segment_ids, 200):
        placeholders = ",".join("?" for _ in chunk)
        rows = ctx.repo.list_where("segment_tags", f"segment_id IN ({placeholders}) ORDER BY segment_id, id DESC", tuple(chunk))
        for row in rows:
            segment_id = str(row.get("segment_id") or "")
            if segment_id and segment_id not in latest:
                latest[segment_id] = row
    return latest


def _assets_by_id(ctx: SkillContext, asset_ids: list[str]) -> Dict[str, Dict[str, Any]]:
    asset_ids = [str(item) for item in asset_ids if str(item or "").strip()]
    if not asset_ids:
        return {}
    assets: Dict[str, Dict[str, Any]] = {}
    for chunk in _chunks(asset_ids, 200):
        placeholders = ",".join("?" for _ in chunk)
        rows = ctx.repo.list_where("assets", f"asset_id IN ({placeholders})", tuple(chunk))
        for row in rows:
            asset_id = str(row.get("asset_id") or "")
            if asset_id:
                assets[asset_id] = row
    return assets


def _chunks(items: list[str], size: int):
    for idx in range(0, len(items), max(1, size)):
        yield items[idx : idx + size]


def _segments_for_source_types(ctx: SkillContext, product_id: str, source_types: list[str] | None = None) -> list[dict]:
    if not source_types:
        return ctx.repo.list_where("segments", "product_id=?", (product_id,))
    placeholders = ",".join("?" for _ in source_types)
    return ctx.repo.list_where(
        "segments",
        f"product_id=? AND source_type IN ({placeholders})",
        (product_id, *source_types),
    )


def _compute_roles(segment: Dict[str, Any], asset: Dict[str, Any], tag: Dict[str, Any], config: AISegmentFactoryConfig) -> tuple[List[str], str]:

    if asset.get("has_watermark") == "yes":
        return [], "has watermark"
    if is_unusable_hard_subtitle(tag):
        return [], "hard subtitle unusable"

    if segment.get("source_type") == "ai_generated":
        if str(segment.get("anchor_match_level") or "") == "fail":
            return [], "ai anchor fail"
        if tag.get("risk_level") == "high":
            if is_repairable_bottom_caption(tag):
                return _subtitle_repair_roles(tag), "repairable bottom subtitle: crop before render"
            return [], "high risk"
        if tag.get("mixcut_usability") == "no":
            return [], "not usable"
        if tag.get("mixcut_usability") == "needs_processing":
            if is_repairable_bottom_caption(tag):
                return _subtitle_repair_roles(tag), "repairable bottom subtitle: crop before render"
            return [], "needs processing before render"
        return _ai_roles(segment, asset, tag, config)

    trusted_exact = _is_trusted_exact_sku_source(segment, asset)
    clear_product = _tag_has_clear_product(tag)
    low_trust_exact = _is_low_trust_exact_sku_source(segment, asset)
    low_trust_validated = _is_low_trust_validated_source(segment, asset)
    if _has_hard_product_mismatch(tag):
        return [], "medium/high risk"
    if trusted_exact and not tag:
        return ["detail", "scene", "ending"], "trusted exact_sku source without tag: allow detail pending tag"
    if low_trust_validated and not tag:
        return ["detail", "scene", "ending"], "validated low trust source without tag: allow detail pending tag"
    if trusted_exact and clear_product:
        if tag.get("risk_level") == "high":
            if is_repairable_bottom_caption(tag):
                return _subtitle_repair_roles(tag), "repairable bottom subtitle: crop before render"
            return [], "high risk"
        if tag.get("mixcut_usability") == "no":
            return [], "not usable"
        if tag.get("mixcut_usability") == "needs_processing":
            if is_repairable_bottom_caption(tag):
                return _subtitle_repair_roles(tag), "repairable bottom subtitle: crop before render"
            if _is_soft_anchor_uncertainty_issue(tag):
                allow_hero = segment.get("source_trust_level") == "high"
                return _trusted_source_roles(tag, allow_hero=allow_hero), "trusted exact_sku source with soft anchor uncertainty"
            return [], "needs processing before render"
        if tag.get("risk_level") == "medium" and _is_soft_anchor_uncertainty_issue(tag):
            return _trusted_source_roles(tag, allow_hero=True), "trusted exact_sku source with medium soft anchor risk"
        if tag.get("mixcut_usability") == "yes" and tag.get("risk_level") in {"low", "medium"}:
            return _trusted_source_roles(tag, allow_hero=True), "trusted exact_sku source roles inferred from tag"

    if low_trust_exact and clear_product and tag.get("risk_level") in {"low", "medium"}:
        if _is_low_trust_core_review_candidate(tag):
            return _trusted_source_roles(tag, allow_hero=_allow_low_trust_hero(tag)), "low trust exact_sku core role allowed for review"
        if tag.get("mixcut_usability") == "yes" and _is_soft_anchor_uncertainty_issue(tag):
            return _trusted_source_roles(tag, allow_hero=_allow_low_trust_hero(tag)), "low trust exact_sku source with soft anchor uncertainty"
        if tag.get("mixcut_usability") == "needs_processing" and is_repairable_bottom_caption(tag):
            return _subtitle_repair_roles(tag), "low trust exact_sku repairable bottom subtitle: crop before render"

    if low_trust_validated and clear_product and tag.get("risk_level") in {"low", "medium"}:
        if tag.get("mixcut_usability") == "yes":
            return _trusted_source_roles(tag, allow_hero=_allow_low_trust_hero(tag)), "validated low trust source roles inferred from tag"
        if tag.get("mixcut_usability") == "needs_processing" and is_repairable_bottom_caption(tag):
            return _subtitle_repair_roles(tag), "validated low trust repairable bottom subtitle: crop before render"

    if tag.get("risk_level") in {"medium", "high"}:
        if is_repairable_bottom_caption(tag):
            return _subtitle_repair_roles(tag), "repairable bottom subtitle: crop before render"
        return [], "medium/high risk"
    if tag.get("mixcut_usability") == "no":
        return ["scene", "ending"], "declared unusable but retained as scene/ending"
    if tag.get("mixcut_usability") == "needs_processing":
        if is_repairable_bottom_caption(tag):
            return _subtitle_repair_roles(tag), "repairable bottom subtitle: crop before render"
        return [], "needs processing before render"

    if segment.get("source_type") == "ai_generated" and segment.get("frame_consistency_status") not in {None, "pass"}:
        return ["scene", "ending"], "ai generated consistency not pass"
    if segment.get("source_trust_level") == "low":
        return ["scene", "ending"], "low trust source is scene/ending only"
    match = segment.get("product_match_status")
    if segment.get("source_trust_level") == "medium" and match == "uncertain":
        return ["scene", "ending"], "medium trust uncertain anchor"
    if segment.get("source_trust_level") in {"high", "medium"} and match in {"trusted_by_source", "anchor_pass"} and tag.get("mixcut_usability") == "yes":
        base = {"scene", "ending"}
        primary = tag.get("primary_shot_role")
        secondary = tag.get("secondary_roles_json") or []
        for role in [primary, *secondary]:
            if role in {"hero", "detail", "result", "scene", "ending"}:
                base.add(role)
        return sorted(base), "trusted source with product match and low risk"
    return ["scene", "ending"], "default conservative fallback"


def _ai_roles(segment: Dict[str, Any], asset: Dict[str, Any], tag: Dict[str, Any], config: AISegmentFactoryConfig) -> tuple[List[str], str]:
    segment_type = str(segment.get("segment_type") or "")
    anchor_level = str(segment.get("anchor_match_level") or "")

    if anchor_level == "fail":
        return [], "ai anchor fail"

    if anchor_level == "uncertain":
        rule = config.get_segment_type_rule(segment_type)
        return [r for r in rule.possible_roles if r in {"scene", "ending"}] or ["scene", "ending"], "ai anchor uncertain: scene/ending only"

    if anchor_level == "soft_pass":
        rule = config.get_segment_type_rule(segment_type)
        return [r for r in rule.possible_roles if r in {"scene", "ending"}] or ["scene", "ending"], "ai anchor soft_pass: scene/ending only"

    if anchor_level == "strict_pass":
        if not segment_type:
            return _ai_roles_for_missing_segment_type(segment, tag)
        rule = config.get_segment_type_rule(segment_type)
        consistency = str(segment.get("frame_consistency_status") or "")
        risk_level = str(tag.get("risk_level") or "medium")
        visibility = str(tag.get("product_visibility") or "medium")
        usability = str(tag.get("mixcut_usability") or "no")

        if getattr(rule, "require_frame_consistency", False) and consistency not in {None, "pass"}:
            return [r for r in rule.possible_roles if r in {"scene", "ending"}] or ["scene", "ending"], "strict_pass but frame consistency not pass"

        if usability != "yes":
            return [r for r in rule.possible_roles if r in {"scene", "ending"}] or ["scene", "ending"], "strict_pass but not mixcut usable"

        confidence = str(tag.get("confidence") or "low")
        if risk_level not in {"low", "medium"} or visibility not in {"high", "medium"} or confidence not in {"high", "medium"}:
            return [r for r in rule.possible_roles if r in {"scene", "ending"}] or ["scene", "ending"], "strict_pass but core confidence insufficient"

        if rule.core_allowed in {True, "yes"}:
            primary = tag.get("primary_shot_role")
            secondary = tag.get("secondary_roles_json") or []
            roles = set()
            for role in [primary, *secondary]:
                if role in rule.possible_roles:
                    roles.add(role)
            slot_role = str(segment.get("slot_role") or asset.get("slot_role") or "")
            allowed_core_roles = set(segment.get("allowed_core_roles_json") or [])
            if (
                slot_role in {"hero", "detail", "result"}
                and slot_role in allowed_core_roles
                and slot_role in {primary, *secondary}
            ):
                roles.add(slot_role)
            if not roles:
                roles = set(rule.default_roles)
            return sorted(roles), f"ai strict_pass full roles ({rule.risk_level} risk)"

        soft_roles = [r for r in rule.possible_roles if r in {"scene", "ending"}]
        return soft_roles or ["scene", "ending"], f"ai strict_pass but core not allowed for this segment type"

    if segment.get("frame_consistency_status") not in {None, "pass"}:
        return ["scene", "ending"], "ai generated consistency not pass"

    rule = config.get_segment_type_rule(segment_type)
    return [r for r in rule.possible_roles if r in {"scene", "ending"}] or ["scene", "ending"], "ai default fallback"


def _ai_roles_for_missing_segment_type(segment: Dict[str, Any], tag: Dict[str, Any]) -> tuple[List[str], str]:
    consistency = str(segment.get("frame_consistency_status") or "")
    risk_level = str(tag.get("risk_level") or "medium")
    visibility = str(tag.get("product_visibility") or "medium")
    usability = str(tag.get("mixcut_usability") or "no")
    confidence = str(tag.get("confidence") or "low")

    if consistency not in {None, "pass"}:
        return ["scene", "ending"], "strict_pass missing segment_type but frame consistency not pass"
    if usability != "yes":
        return ["scene", "ending"], "strict_pass missing segment_type but not mixcut usable"
    if risk_level not in {"low", "medium"} or visibility not in {"high", "medium"} or confidence not in {"high", "medium"}:
        return ["scene", "ending"], "strict_pass missing segment_type but core confidence insufficient"

    roles = {"scene", "ending"}
    primary = tag.get("primary_shot_role")
    secondary = tag.get("secondary_roles_json") or []
    for role in [primary, *secondary]:
        if role in {"hero", "detail", "result", "scene", "ending"}:
            roles.add(role)
    return sorted(roles), "ai strict_pass missing segment_type; roles inferred from vision tag"



def _subtitle_repair_roles(tag) -> list[str]:
    roles = {"scene", "ending"}
    primary = tag.get("primary_shot_role")
    secondary = tag.get("secondary_roles_json") or []
    for role in [primary, *secondary]:
        if role in {"detail", "result", "scene", "ending"}:
            roles.add(role)
    return sorted(roles)


def _is_trusted_exact_sku_source(segment: Dict[str, Any], asset: Dict[str, Any]) -> bool:
    source_type = str(segment.get("source_type") or asset.get("source_type") or "")
    trust = str(segment.get("source_trust_level") or asset.get("source_trust_level") or "")
    binding = str(segment.get("product_binding_type") or asset.get("product_binding_type") or "")
    match = str(segment.get("product_match_status") or "")
    return (
        source_type in {"authorized_creator", "creator_authorized", "self_shot", "original_script", "creator_original"}
        and trust in {"high", "medium"}
        and binding == "exact_sku"
        and match in {"trusted_by_source", "anchor_pass"}
    )


def _is_low_trust_exact_sku_source(segment: Dict[str, Any], asset: Dict[str, Any]) -> bool:
    source_type = str(segment.get("source_type") or asset.get("source_type") or "")
    trust = str(segment.get("source_trust_level") or asset.get("source_trust_level") or "")
    binding = str(segment.get("product_binding_type") or asset.get("product_binding_type") or "")
    match = str(segment.get("product_match_status") or "")
    return (
        source_type in {"douyin_repost", "competitor"}
        and trust == "low"
        and binding == "exact_sku"
        and match in {"uncertain", "trusted_by_source", "anchor_pass"}
    )


def _is_low_trust_validated_source(segment: Dict[str, Any], asset: Dict[str, Any]) -> bool:
    source_type = str(segment.get("source_type") or asset.get("source_type") or "")
    trust = str(segment.get("source_trust_level") or asset.get("source_trust_level") or "")
    binding = str(segment.get("product_binding_type") or asset.get("product_binding_type") or "")
    match = str(segment.get("product_match_status") or "")
    return (
        source_type in {"douyin_repost", "competitor"}
        and trust == "low"
        and binding in {"exact_sku", "same_style"}
        and match in {"", "uncertain", "trusted_by_source", "anchor_pass"}
    )


def _tag_has_clear_product(tag: Dict[str, Any]) -> bool:
    return (
        str(tag.get("product_visibility") or "") in {"high", "medium"}
        and str(tag.get("confidence") or "") in {"high", "medium"}
    )


def _is_soft_anchor_uncertainty_issue(tag: Dict[str, Any]) -> bool:
    reason = str(tag.get("reason") or "")
    soft_tokens = ["锚点未知", "锚点不确定", "锚点缺失", "商品锚点", "商品信息缺失", "需核对", "需复核", "需确认", "人工确认", "人工核实"]
    if not any(token in reason for token in soft_tokens):
        return False
    hard_tokens = ["水印", "平台", "账号", "logo", "Logo", "错款", "错品类", "竞品", "SKU一致性", "漂移", "无关元素", "品牌包", "遮挡严重"]
    return not any(token in reason for token in hard_tokens)


def _has_hard_product_mismatch(tag: Dict[str, Any]) -> bool:
    reason = str(tag.get("reason") or "")
    hard_tokens = ["错款", "错品类", "SKU一致性", "漂移", "无关元素", "品牌包"]
    return any(token in reason for token in hard_tokens)


def _allow_low_trust_hero(tag: Dict[str, Any]) -> bool:
    return (
        str(tag.get("primary_shot_role") or "") == "hero"
        and str(tag.get("product_visibility") or "") == "high"
        and str(tag.get("confidence") or "") == "high"
        and str(tag.get("mixcut_usability") or "") == "yes"
    )


def _is_low_trust_core_review_candidate(tag: Dict[str, Any]) -> bool:
    if str(tag.get("product_visibility") or "") != "high":
        return False
    if str(tag.get("confidence") or "") != "high":
        return False
    if str(tag.get("risk_level") or "") not in {"low", "medium"}:
        return False
    if str(tag.get("mixcut_usability") or "") not in {"yes", "needs_processing"}:
        return False
    if str(tag.get("text_overlay_risk") or "none") in {"foreign_language_caption", "platform_ui_or_watermark", "large_obstructive_text"}:
        return False
    roles = {tag.get("primary_shot_role"), *(tag.get("secondary_roles_json") or [])}
    return bool(roles.intersection({"hero", "detail", "result"}))


def _trusted_source_roles(tag: Dict[str, Any], allow_hero: bool = True) -> list[str]:
    roles = {"scene", "ending"}
    allowed = {"hero", "detail", "result", "scene", "ending"} if allow_hero else {"detail", "result", "scene", "ending"}
    primary = tag.get("primary_shot_role")
    secondary = tag.get("secondary_roles_json") or []
    for role in [primary, *secondary]:
        if role in allowed:
            roles.add(role)
    return sorted(roles)
