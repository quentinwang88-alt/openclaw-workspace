from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from auto_mixcut.core.result import Result

from .ai_segment_factory_config import AISegmentFactoryConfig, get_config
from .context import SkillContext


class EffectiveRoleSkill:
    def __init__(self, ctx: SkillContext, config: Optional[AISegmentFactoryConfig] = None):
        self.ctx = ctx
        self.config = config or get_config()

    def compute_product(self, product_id: str) -> Result:
        segments = self.ctx.repo.list_where("segments", "product_id=?", (product_id,))
        results = [self.compute_segment(s["segment_id"]).to_dict() for s in segments]
        self.ctx.repo.update("content_tasks", "product_id", product_id, {"task_status": "EFFECTIVE_ROLES_COMPUTED"})
        return Result.ok({"count": len(results), "results": results})

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


def _compute_roles(segment: Dict[str, Any], asset: Dict[str, Any], tag: Dict[str, Any], config: AISegmentFactoryConfig) -> tuple[List[str], str]:

    if asset.get("has_watermark") == "yes":
        return [], "has watermark"

    if segment.get("source_type") == "ai_generated":
        if tag.get("risk_level") == "high":
            if _is_soft_local_subtitle_issue(tag):
                return _soft_local_subtitle_roles(tag), "soft local-language subtitle issue"
            return [], "high risk"
        if tag.get("mixcut_usability") == "no":
            return [], "not usable"
        if tag.get("mixcut_usability") == "needs_processing":
            if _is_soft_local_subtitle_issue(tag):
                return _soft_local_subtitle_roles(tag), "soft local-language subtitle issue"
            return [], "needs processing before render"
        return _ai_roles(segment, tag, config)

    if tag.get("risk_level") in {"medium", "high"}:
        if _is_soft_local_subtitle_issue(tag):
            return _soft_local_subtitle_roles(tag), "soft local-language subtitle issue"
        return [], "medium/high risk"
    if tag.get("mixcut_usability") == "no":
        return [], "not usable"
    if tag.get("mixcut_usability") == "needs_processing":
        if _is_soft_local_subtitle_issue(tag):
            return _soft_local_subtitle_roles(tag), "soft local-language subtitle issue"
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


def _ai_roles(segment: Dict[str, Any], tag: Dict[str, Any], config: AISegmentFactoryConfig) -> tuple[List[str], str]:
    segment_type = str(segment.get("segment_type") or "")
    rule = config.get_segment_type_rule(segment_type)
    anchor_level = str(segment.get("anchor_match_level") or "")

    if anchor_level == "fail":
        return [], "ai anchor fail"

    if anchor_level == "uncertain":
        return [r for r in rule.possible_roles if r in {"scene", "ending"}] or ["scene", "ending"], "ai anchor uncertain: scene/ending only"

    if anchor_level == "soft_pass":
        return [r for r in rule.possible_roles if r in {"scene", "ending"}] or ["scene", "ending"], "ai anchor soft_pass: scene/ending only"

    if anchor_level == "strict_pass":
        consistency = str(segment.get("frame_consistency_status") or "")
        risk_level = str(tag.get("risk_level") or "medium")
        visibility = str(tag.get("product_visibility") or "medium")
        usability = str(tag.get("mixcut_usability") or "no")

        if getattr(rule, "require_frame_consistency", False) and consistency not in {None, "pass"}:
            return [r for r in rule.possible_roles if r in {"scene", "ending"}] or ["scene", "ending"], "strict_pass but frame consistency not pass"

        if risk_level not in {"low"}:
            return [r for r in rule.possible_roles if r in {"scene", "ending"}] or ["scene", "ending"], "strict_pass but risk not low"

        if usability != "yes":
            return [r for r in rule.possible_roles if r in {"scene", "ending"}] or ["scene", "ending"], "strict_pass but not mixcut usable"

        if rule.core_allowed in {True, "yes"} and visibility in {"high", "medium"}:
            primary = tag.get("primary_shot_role")
            secondary = tag.get("secondary_roles_json") or []
            roles = set()
            for role in [primary, *secondary]:
                if role in rule.possible_roles:
                    roles.add(role)
            if not roles:
                roles = set(rule.default_roles)
            return sorted(roles), f"ai strict_pass full roles ({rule.risk_level} risk)"

        soft_roles = [r for r in rule.possible_roles if r in {"scene", "ending"}]
        return soft_roles or ["scene", "ending"], f"ai strict_pass but core not allowed for this segment type"

    if segment.get("frame_consistency_status") not in {None, "pass"}:
        return ["scene", "ending"], "ai generated consistency not pass"

    return [r for r in rule.possible_roles if r in {"scene", "ending"}] or ["scene", "ending"], "ai default fallback"



def _is_soft_local_subtitle_issue(tag) -> bool:
    reason = str(tag.get("reason") or "")
    if not any(token in reason for token in ["字幕", "越南语", "当地语言", "底部文字"]):
        return False
    hard_tokens = ["水印", "平台", "账号", "logo", "Logo", "错款", "SKU一致性", "漂移", "无关元素", "品牌包", "遮挡严重"]
    return not any(token in reason for token in hard_tokens)


def _soft_local_subtitle_roles(tag) -> list[str]:
    roles = {"scene", "ending"}
    primary = tag.get("primary_shot_role")
    secondary = tag.get("secondary_roles_json") or []
    for role in [primary, *secondary]:
        if role in {"detail", "result", "scene", "ending"}:
            roles.add(role)
    return sorted(roles)
