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

    def check_product(self, product_id: str, force: bool = False) -> Result:
        segments = self.ctx.repo.list_where(
            "segments",
            "product_id=? AND source_type='ai_generated' AND segment_status='qc_passed'",
            (product_id,),
        )
        results = [self.check_segment(s["segment_id"], force=force).to_dict() for s in segments]
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

    def check_segment(self, segment_id: str, force: bool = False) -> Result:
        segment = self.ctx.repo.get("segments", "segment_id", segment_id)
        if not segment:
            return Result.fail("SEGMENT_NOT_FOUND", "segment not found", {"segment_id": segment_id})
        if segment.get("anchor_match_level") and not force:
            return Result.ok({
                "segment_id": segment_id,
                "anchor_match_level": segment.get("anchor_match_level"),
                "reason": segment.get("anchor_check_reason") or "anchor check exists",
                "allowed_core_roles": segment.get("allowed_core_roles_json") or [],
                "allowed_soft_roles": segment.get("allowed_soft_roles_json") or [],
                "skipped": True,
            })

        product = self.ctx.repo.get("products", "product_id", segment.get("product_id")) or {}
        tag = _latest_tag(self.ctx, segment_id)
        asset = self.ctx.repo.get("assets", "asset_id", segment.get("asset_id")) or {}
        anchor = product.get("product_anchor_json") or {}

        local_forbidden = _detect_forbidden_mismatch(anchor, tag, segment, asset)
        local_level, local_reason, local_core, local_soft = _evaluate_anchor_match(
            anchor=anchor, tag=tag, segment=segment, asset=asset,
        )
        if local_forbidden:
            match_level, reason, core_roles, soft_roles = "fail", local_forbidden, [], []
        elif _is_prompt_package_exact_sku_core_candidate(tag, segment, asset) and local_level == "strict_pass":
            match_level, reason, core_roles, soft_roles = (
                local_level,
                f"本地锚点/Prompt Package 元数据优先兜底: {local_reason}",
                local_core,
                local_soft,
            )

        elif self.ctx.settings.mock_llm:
            match_level, reason, core_roles, soft_roles = _evaluate_anchor_match(
                anchor=anchor, tag=tag, segment=segment, asset=asset,
            )
        else:
            res = self._get_router().call(
                "ai_anchor_check",
                {
                    "segment_type": str(segment.get("segment_type") or ""),
                    "product_anchor": anchor,
                    "latest_tag": tag,
                    "frame_consistency_status": segment.get("frame_consistency_status"),
                    "frame_consistency_reason": segment.get("frame_consistency_reason"),
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
                forbidden_reason = str(data.get("forbidden_mismatch_reason") or "")
                if data.get("forbidden_mismatch_detected"):
                    match_level, reason, core_roles, soft_roles = (
                        "fail",
                        forbidden_reason or "LLM检测到商品锚点禁用错配项",
                        [],
                        [],
                    )
                if _is_missing_anchor_uncertain(match_level, reason):
                    if local_level in {"strict_pass", "soft_pass"}:
                        match_level, reason, core_roles, soft_roles = (
                            local_level,
                            f"LLM锚点信息缺失，已用本地锚点/标签兜底: {local_reason}",
                            local_core,
                            local_soft,
                        )
                if local_forbidden:
                    match_level, reason, core_roles, soft_roles = (
                        "fail",
                        local_forbidden,
                        [],
                        [],
                    )

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


def _is_missing_anchor_uncertain(match_level: str, reason: str) -> bool:
    if match_level != "uncertain":
        return False
    if any(token in reason for token in ["未提供商品锚点", "未提供目标商品", "商品信息", "匹配锚点", "未提供商品名称", "未提供类目"]):
        return True
    return "未提供" in reason and "锚点" in reason


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

    forbidden_reason = _detect_forbidden_mismatch(anchor, tag, segment, asset)
    if forbidden_reason:
        return "fail", forbidden_reason, [], []

    if usability == "no":
        return "fail", "mixcut_usability判定为不可用", [], []
    if risk_level == "high":
        return "fail", f"风险等级为high，置信度为{confidence}", [], []

    soft_indicators = _count_soft_indicators(anchor, tag, segment, asset)
    strict_indicators = _count_strict_indicators(anchor, tag, segment, asset)

    if consistency == "fail":
        return "fail", "跨帧一致性检查失败", [], ["scene", "ending"]

    if _is_prompt_package_exact_sku_core_candidate(tag, segment, asset):
        return "strict_pass", "Prompt Package exact_sku 生成素材，画面清晰且无硬错配，本地锚点兜底放行核心位", list(CORE_ROLES), list(SOFT_ROLES)

    if risk_level == "medium" and confidence in {"low", "medium"}:
        return "uncertain", "中等风险且置信度不足", [], ["scene", "ending"]

    if soft_indicators >= 4 and strict_indicators >= 2:
        return "strict_pass", "关键识别点清楚，可承担核心商品镜头", list(CORE_ROLES), list(SOFT_ROLES)
    if soft_indicators >= 3:
        return "soft_pass", "商品方向大体正确，细节不足以承担强商品展示", [], list(SOFT_ROLES)
    if soft_indicators >= 1:
        return "uncertain", "部分通过但不够确定", [], ["scene", "ending"]

    return "fail", "商品关键点不匹配或缺失", [], []


def _is_prompt_package_exact_sku_core_candidate(tag: Dict[str, Any], segment: Dict[str, Any], asset: Dict[str, Any]) -> bool:
    source_type = str(segment.get("source_type") or asset.get("source_type") or "")
    binding = str(segment.get("product_binding_type") or asset.get("product_binding_type") or "")
    segment_type = str(segment.get("segment_type") or asset.get("scene_tag") or "")
    slot_role = str(segment.get("slot_role") or asset.get("slot_role") or "")
    has_prompt_package = bool(segment.get("prompt_package_id") or asset.get("prompt_package_id"))
    roles = [tag.get("primary_shot_role"), *(tag.get("secondary_roles_json") or [])]
    return (
        source_type == "ai_generated"
        and binding == "exact_sku"
        and has_prompt_package
        and segment_type in {"product_display", "tryon_result", "product_still", "unboxing", "flatlay"}
        and slot_role in {"hero", "detail", "result"}
        and bool({"hero", "detail", "result"}.intersection(str(role) for role in roles))
        and str(tag.get("product_visibility") or "") == "high"
        and str(tag.get("mixcut_usability") or "") == "yes"
        and str(tag.get("risk_level") or "") in {"low", "medium"}
        and str(tag.get("confidence") or "") in {"high", "medium"}
        and str(segment.get("frame_consistency_status") or "") in {"", "pass"}
    )


COLOR_TOKENS = [
    "黑色", "白色", "米白", "米色", "棕色", "咖色", "褐色", "红色", "粉色",
    "蓝色", "绿色", "黄色", "灰色", "银色", "金色", "紫色", "彩色",
]


def _detect_forbidden_mismatch(anchor: Dict[str, Any], tag: Dict[str, Any], segment: Dict[str, Any], asset: Dict[str, Any]) -> str:
    evidence = _flatten_text({
        "tag": tag,
        "segment_type": segment.get("segment_type"),
        "segment_status": segment.get("segment_status"),
        "asset": {
            "asset_type": asset.get("asset_type"),
            "original_filename": asset.get("original_filename"),
        },
    })
    forbidden = anchor.get("forbidden_mismatch") or []
    if isinstance(forbidden, str):
        forbidden_items = [forbidden]
    elif isinstance(forbidden, list):
        forbidden_items = [str(item) for item in forbidden if item]
    else:
        forbidden_items = []

    for item in forbidden_items:
        hit = _first_forbidden_token(str(item), evidence)
        if hit:
            return f"命中商品锚点禁用错配项：{hit}"

    anchor_text = _flatten_text(anchor)
    anchor_colors = {token for token in COLOR_TOKENS if token in anchor_text and f"非{token}" not in anchor_text}
    evidence_colors = {token for token in COLOR_TOKENS if token in evidence}
    if anchor_colors and evidence_colors:
        conflicts = sorted(evidence_colors - anchor_colors)
        if conflicts:
            return f"颜色锚点不匹配：目标包含{','.join(sorted(anchor_colors))}，画面识别为{','.join(conflicts)}"

    return ""


def _first_forbidden_token(forbidden_text: str, evidence: str) -> str:
    for token in COLOR_TOKENS:
        if token in forbidden_text and token in evidence:
            return token
    for token in ["错品类", "其他类目", "无关配饰", "画面中没有商品", "款式明显不一致"]:
        if token in forbidden_text and token in evidence:
            return token
    return ""


def _flatten_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except TypeError:
        return str(value)


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
