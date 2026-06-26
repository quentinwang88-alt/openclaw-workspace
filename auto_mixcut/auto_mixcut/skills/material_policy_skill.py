from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from auto_mixcut.config.factory_config import factory_config
from auto_mixcut.domain.source_types import LOW_TRUST_REFERENCE_SOURCE_TYPES, TRUSTED_REAL_SOURCE_TYPES
from auto_mixcut.domain.usecases import ADS_USECASES

from .context import SkillContext


SOFT_REVIEW_TOKENS = ["锚点未知", "锚点不确定", "锚点缺失", "商品锚点", "商品信息缺失", "需核对", "需复核", "需确认", "人工确认", "人工核实"]
HARD_REVIEW_TOKENS = ["水印", "平台", "账号", "logo", "Logo", "错款", "错品类", "竞品", "SKU一致性", "漂移", "无关元素", "品牌包", "遮挡严重"]
PUBLISHED_RESULT_VALUES = {"published", "posted", "live", "success", "发布成功", "已发布", "已投放"}


@dataclass(frozen=True)
class MaterialPolicyDecision:
    usecase: str
    ads_usecase: bool
    ads_eligible: bool
    first_slot_allowed: bool
    reuse_allowed: bool
    trusted_real_first: bool
    low_trust_first_slot_candidate: bool
    published_exposure_used: bool
    block_reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "usecase": self.usecase,
            "ads_usecase": self.ads_usecase,
            "ads_eligible": self.ads_eligible,
            "first_slot_allowed": self.first_slot_allowed,
            "reuse_allowed": self.reuse_allowed,
            "trusted_real_first": self.trusted_real_first,
            "low_trust_first_slot_candidate": self.low_trust_first_slot_candidate,
            "published_exposure_used": self.published_exposure_used,
            "block_reasons": list(self.block_reasons),
        }


class MaterialPolicySkill:
    """Central source of truth for material eligibility in mixcut production."""

    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def evaluate_segment(
        self,
        segment: dict[str, Any],
        *,
        asset: dict[str, Any] | None = None,
        tag: dict[str, Any] | None = None,
        usecase: str = "",
        slot_index: int = 0,
        role: str = "",
    ) -> dict[str, Any]:
        asset = asset if asset is not None else self._asset_for_segment(segment)
        tag = tag if tag is not None else self._latest_tag_for_segment(segment)
        return evaluate_material_policy(
            self.ctx,
            segment,
            asset=asset,
            tag=tag,
            usecase=usecase,
            slot_index=slot_index,
            role=role,
        ).to_dict()

    def filter_segments(
        self,
        segments: list[dict[str, Any]],
        *,
        usecase: str = "",
        slot_index: int = 0,
        role: str = "",
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        allowed: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        for segment in segments:
            decision = self.evaluate_segment(segment, usecase=usecase, slot_index=slot_index, role=role)
            if decision.get("ads_eligible") and (not _is_first_slot(slot_index, role) or decision.get("first_slot_allowed")):
                allowed.append(segment)
            else:
                rejected.append(
                    {
                        "segment_id": segment.get("segment_id"),
                        "asset_id": segment.get("asset_id"),
                        "source_type": segment.get("source_type"),
                        "block_reasons": decision.get("block_reasons") or [],
                    }
                )
        return allowed, rejected

    def _asset_for_segment(self, segment: dict[str, Any]) -> dict[str, Any]:
        asset_id = str(segment.get("asset_id") or "")
        if not asset_id:
            return {}
        return self.ctx.repo.get("assets", "asset_id", asset_id) or {}

    def _latest_tag_for_segment(self, segment: dict[str, Any]) -> dict[str, Any]:
        segment_id = str(segment.get("segment_id") or "")
        if not segment_id:
            return {}
        rows = self.ctx.repo.list_where("segment_tags", "segment_id=? ORDER BY id DESC LIMIT 1", (segment_id,))
        return rows[0] if rows else {}


def evaluate_material_policy(
    ctx: SkillContext | None,
    segment: dict[str, Any],
    *,
    asset: dict[str, Any] | None = None,
    tag: dict[str, Any] | None = None,
    usecase: str = "",
    slot_index: int = 0,
    role: str = "",
) -> MaterialPolicyDecision:
    asset = asset or {}
    tag = tag or {}
    usecase_value = str(usecase or "").strip()
    ads_usecase = is_ads_usecase(usecase_value)
    first_slot = _is_first_slot(slot_index, role)
    source_type = _source_type(segment, asset)
    published = _published_exposure_used(ctx, segment) if ads_usecase else False
    trusted_real = _trusted_real_first(segment, asset, tag)
    low_trust_candidate = _low_trust_first_slot_candidate(segment, asset, tag, ads_usecase=ads_usecase)
    block_reasons: list[str] = []

    if ads_usecase and published:
        block_reasons.append("published_exposure_used")
    if ads_usecase and first_slot and source_type in LOW_TRUST_REFERENCE_SOURCE_TYPES and not low_trust_candidate:
        block_reasons.append("ads_low_trust_first_slot_not_allowed")

    ads_eligible = not (ads_usecase and published)
    first_slot_allowed = not any(reason in {"published_exposure_used", "ads_low_trust_first_slot_not_allowed"} for reason in block_reasons)
    reuse_allowed = ads_eligible
    return MaterialPolicyDecision(
        usecase=usecase_value,
        ads_usecase=ads_usecase,
        ads_eligible=ads_eligible,
        first_slot_allowed=first_slot_allowed,
        reuse_allowed=reuse_allowed,
        trusted_real_first=trusted_real,
        low_trust_first_slot_candidate=low_trust_candidate,
        published_exposure_used=published,
        block_reasons=tuple(block_reasons),
    )


def is_ads_usecase(usecase: str = "") -> bool:
    value = str(usecase or "").strip().lower()
    return value in ADS_USECASES or factory_config().ads_fast_mode


def ads_low_trust_first_slot_enabled() -> bool:
    return factory_config().ads_allow_low_trust_first_slot


def _is_first_slot(slot_index: int, role: str) -> bool:
    return int(slot_index or 0) == 1 or str(role or "").strip().lower() == "hero"


def _source_type(segment: dict[str, Any], asset: dict[str, Any]) -> str:
    return str(segment.get("source_type") or asset.get("source_type") or "").strip()


def _trusted_real_first(segment: dict[str, Any], asset: dict[str, Any], tag: dict[str, Any]) -> bool:
    source_type = _source_type(segment, asset)
    trust = str(segment.get("source_trust_level") or asset.get("source_trust_level") or "")
    binding = str(segment.get("product_binding_type") or asset.get("product_binding_type") or "")
    match = str(segment.get("product_match_status") or "")
    if source_type not in TRUSTED_REAL_SOURCE_TYPES:
        return False
    if trust not in {"high", "medium"} or binding != "exact_sku" or match not in {"trusted_by_source", "anchor_pass"}:
        return False
    if str(tag.get("product_visibility") or segment.get("product_visibility") or "") != "high":
        return False
    if str(tag.get("confidence") or segment.get("confidence") or "") not in {"high", "medium"}:
        return False
    if str(tag.get("risk_level") or segment.get("risk_level") or "") != "medium":
        return False
    reason = str(tag.get("reason") or segment.get("effective_roles_reason") or "")
    return any(token in reason for token in SOFT_REVIEW_TOKENS) and not any(token in reason for token in HARD_REVIEW_TOKENS)


def _low_trust_first_slot_candidate(segment: dict[str, Any], asset: dict[str, Any], tag: dict[str, Any], *, ads_usecase: bool) -> bool:
    if ads_usecase and not ads_low_trust_first_slot_enabled():
        return False
    source_type = _source_type(segment, asset)
    trust = str(segment.get("source_trust_level") or asset.get("source_trust_level") or "")
    binding = str(segment.get("product_binding_type") or asset.get("product_binding_type") or "")
    match = str(segment.get("product_match_status") or "")
    if source_type not in LOW_TRUST_REFERENCE_SOURCE_TYPES:
        return False
    if trust != "low" or binding not in {"exact_sku", "same_style"}:
        return False
    if match not in {"", "uncertain", "trusted_by_source", "anchor_pass"}:
        return False
    if "hero" not in (segment.get("effective_roles_json") or []):
        return False
    return (
        str(tag.get("primary_shot_role") or segment.get("primary_shot_role") or "") == "hero"
        and str(tag.get("product_visibility") or segment.get("product_visibility") or "") == "high"
        and str(tag.get("confidence") or segment.get("confidence") or "") == "high"
        and str(tag.get("risk_level") or segment.get("risk_level") or "") == "low"
        and str(tag.get("mixcut_usability") or segment.get("mixcut_usability") or "") == "yes"
        and str(tag.get("text_overlay_risk") or segment.get("text_overlay_risk") or "none") in {"", "none", "low", "minor"}
        and str(asset.get("has_watermark") or segment.get("has_watermark") or "no").strip().lower() not in {"yes", "true", "1"}
    )


def _published_exposure_used(ctx: SkillContext | None, segment: dict[str, Any]) -> bool:
    if _truthy(segment.get("published_at")) or str(segment.get("publish_result") or "").strip().lower() in PUBLISHED_RESULT_VALUES:
        return True
    if ctx is None:
        return False
    segment_id = str(segment.get("segment_id") or "")
    if not segment_id:
        return False
    rows = ctx.repo.list_where("output_segments", "segment_id=?", (segment_id,))
    for row in rows:
        output_id = str(row.get("output_id") or "")
        if not output_id:
            continue
        output = ctx.repo.get("outputs", "output_id", output_id) or {}
        if _truthy(output.get("published_at")):
            return True
    return False


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return bool(str(value or "").strip())
