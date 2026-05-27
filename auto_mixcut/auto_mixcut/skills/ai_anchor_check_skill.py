from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from auto_mixcut.core.result import Result

from .context import SkillContext
from .llm_router_skill import LLMRouterSkill

ANCHOR_MATCH_LEVELS = {"strict_pass", "soft_pass", "uncertain", "fail"}
ALLOWED_ROLES = {"hero", "detail", "result", "scene", "ending"}
CORE_ROLES = {"hero", "detail", "result"}
SOFT_ROLES = {"scene", "ending"}


class AIAnchorCheckSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx
        self._router: Optional[LLMRouterSkill] = None

    def _get_router(self) -> LLMRouterSkill:
        if self._router is None:
            self._router = LLMRouterSkill(self.ctx)
        return self._router

    def check_product(self, product_id: str) -> Result:
        segments = self.ctx.repo.list_where(
            "segments",
            "product_id=? AND source_type='ai_generated' AND segment_status='qc_passed'",
            (product_id,),
        )
        results = [self.check_segment(s["segment_id"]).to_dict() for s in segments]
        strict = sum(1 for r in results if r.get("data", {}).get("anchor_match_level") == "strict_pass")
        soft = sum(1 for r in results if r.get("data", {}).get("anchor_match_level") == "soft_pass")
        uncertain = sum(1 for r in results if r.get("data", {}).get("anchor_match_level") == "uncertain")
        failed = sum(1 for r in results if r.get("data", {}).get("anchor_match_level") == "fail")

        self.ctx.repo.update("content_tasks", "product_id", product_id, {"task_status": "AI_ANCHOR_CHECKED"})
        return Result.ok({
            "product_id": product_id,
            "checked": len(results),
            "strict_pass": strict,
            "soft_pass": soft,
            "uncertain": uncertain,
            "fail": failed,
            "results": results,
        })

    def check_segment(self, segment_id: str) -> Result:
        segment = self.ctx.repo.get("segments", "segment_id", segment_id)
        if not segment:
            return Result.fail("SEGMENT_NOT_FOUND", "segment not found", {"segment_id": segment_id})

        product = self.ctx.repo.get("products", "product_id", segment.get("product_id")) or {}

        if self.ctx.settings.mock_llm:
            tag = _latest_tag(self.ctx, segment_id)
            asset = self.ctx.repo.get("assets", "asset_id", segment.get("asset_id")) or {}
            anchor = product.get("product_anchor_json") or {}
            match_level, reason, core_roles, soft_roles = _evaluate_anchor_match(
                anchor=anchor, tag=tag, segment=segment, asset=asset,
            )
        else:
            res = self._get_router().call(
                "ai_anchor_check",
                {
                    "segment_type": str(segment.get("segment_type") or ""),
                    "prompt_version": "v1.0",
                    "image_count": 6,
                },
                product_id=str(product.get("product_id") or ""),
                segment_id=segment_id,
            )
            if not res.success:
                match_level, reason, core_roles, soft_roles = "uncertain", f"LLM anchor check failed: {res.error.message if res.error else 'unknown'}", [], ["scene", "ending"]
            else:
                data = res.data.get("response", {})
                match_level = str(data.get("anchor_match_level") or "uncertain")
                reason = str(data.get("reason") or "")
                core_roles = list(data.get("allowed_core_roles") or [])
                soft_roles = list(data.get("allowed_soft_roles") or [])

        self.ctx.repo.update("segments", "segment_id", segment_id, {
            "anchor_match_level": match_level,
            "anchor_check_reason": reason,
            "allowed_core_roles_json": core_roles,
            "allowed_soft_roles_json": soft_roles,
        })

        return Result.ok({
            "segment_id": segment_id,
            "anchor_match_level": match_level,
            "reason": reason,
            "allowed_core_roles": core_roles,
            "allowed_soft_roles": soft_roles,
        })


def _latest_tag(ctx: SkillContext, segment_id: str) -> Dict[str, Any]:
    rows = ctx.repo.list_where("segment_tags", "segment_id=? ORDER BY id DESC", (segment_id,))
    return rows[0] if rows else {}


def _evaluate_anchor_match(
    anchor: Dict[str, Any],
    tag: Dict[str, Any],
    segment: Dict[str, Any],
    asset: Dict[str, Any],
) -> tuple[str, str, list[str], list[str]]:
    risk_level = str(tag.get("risk_level") or "medium")
    confidence = str(tag.get("confidence") or "low")
    usability = str(tag.get("mixcut_usability") or "no")
    consistency = str(segment.get("frame_consistency_status") or "")

    if usability == "no":
        return "fail", "mixcut_usability判定为不可用", [], []
    if risk_level == "high":
        return "fail", f"风险等级为high，置信度为{confidence}", [], []

    soft_indicators = _count_soft_indicators(anchor, tag, segment, asset)
    strict_indicators = _count_strict_indicators(anchor, tag, segment, asset)

    if consistency == "fail":
        return "fail", "跨帧一致性检查失败", [], ["scene", "ending"]

    if risk_level == "medium" and confidence in {"low", "medium"}:
        return "uncertain", "中等风险且置信度不足", [], ["scene", "ending"]

    if soft_indicators >= 4 and strict_indicators >= 2:
        return "strict_pass", "关键识别点清楚，可承担核心商品镜头", list(CORE_ROLES), list(SOFT_ROLES)
    if soft_indicators >= 3:
        return "soft_pass", "商品方向大体正确，细节不足以承担强商品展示", [], list(SOFT_ROLES)
    if soft_indicators >= 1:
        return "uncertain", "部分通过但不够确定", [], ["scene", "ending"]

    return "fail", "商品关键点不匹配或缺失", [], []


def _count_soft_indicators(anchor: Dict[str, Any], tag: Dict[str, Any], segment: Dict[str, Any], asset: Dict[str, Any]) -> int:
    count = 0

    if tag.get("primary_shot_role") not in {"unusable", ""}:
        count += 1
    if tag.get("product_visibility") in {"high", "medium"}:
        count += 1
    if tag.get("confidence") in {"high", "medium"}:
        count += 1
    if tag.get("risk_level") == "low":
        count += 1
    if tag.get("mixcut_usability") == "yes":
        count += 1

    if anchor:
        count += 1

    return count


def _count_strict_indicators(anchor: Dict[str, Any], tag: Dict[str, Any], segment: Dict[str, Any], asset: Dict[str, Any]) -> int:
    count = 0

    if tag.get("primary_shot_role") in {"hero", "detail", "result", "scene", "ending"}:
        count += 1
    if tag.get("product_visibility") == "high":
        count += 1
    if tag.get("confidence") == "high":
        count += 1
    if tag.get("risk_level") == "low" and tag.get("mixcut_usability") == "yes":
        count += 1
    if segment.get("frame_consistency_status") == "pass":
        count += 1

    return count
