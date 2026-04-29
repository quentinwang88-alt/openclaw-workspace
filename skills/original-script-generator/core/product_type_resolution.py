#!/usr/bin/env python3
"""产品类型归一与冲突仲裁。"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Sequence


CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "product_type_config.json"
TYPE_GUARD_HIGH_CONFIDENCE_THRESHOLD = 0.85


@dataclass(frozen=True)
class TypeDefinition:
    canonical_type: str
    family: str
    slot: str
    aliases: Sequence[str]
    display_name: str
    description: str
    required_terms: Sequence[str]
    forbidden_terms: Sequence[str]


@dataclass(frozen=True)
class TypeRegistry:
    family_labels: Dict[str, str]
    slot_labels: Dict[str, str]
    business_category_aliases: Dict[str, str]
    type_definitions: Sequence[TypeDefinition]
    type_by_canonical: Dict[str, TypeDefinition]
    alias_to_type: Dict[str, str]


@dataclass(frozen=True)
class ProductTypeContext:
    raw_product_type: str
    business_category: str
    business_family: str
    canonical_family: str
    canonical_slot: str
    canonical_type: str
    display_type: str
    prompt_label: str
    required_terms: List[str]
    forbidden_terms: List[str]
    recognized_by_registry: bool = True
    fallback_used: bool = False
    source: str = "table"

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class ResolvedProductContext(ProductTypeContext):
    vision_family: Optional[str] = None
    vision_slot: Optional[str] = None
    vision_type: Optional[str] = None
    vision_confidence: Optional[float] = None
    conflict_level: str = "none"
    resolution_policy: str = "prefer_table"
    review_required: bool = False
    conflict_reason: Optional[str] = None
    block_required: bool = False
    block_reason: Optional[str] = None


@dataclass(frozen=True)
class VisionContext:
    family: str
    slot: str
    canonical_type: str
    confidence: Optional[float] = None


@lru_cache(maxsize=1)
def load_type_registry() -> TypeRegistry:
    data = json.loads(CONFIG_PATH.read_text())
    definitions = [TypeDefinition(**item) for item in data["types"]]
    type_by_canonical = {item.canonical_type: item for item in definitions}
    alias_to_type = {
        _normalize_text(alias): item.canonical_type
        for item in definitions
        for alias in item.aliases
    }
    return TypeRegistry(
        family_labels=data["family_labels"],
        slot_labels=data["slot_labels"],
        business_category_aliases=data["business_category_aliases"],
        type_definitions=definitions,
        type_by_canonical=type_by_canonical,
        alias_to_type=alias_to_type,
    )


def get_family_labels() -> Dict[str, str]:
    return load_type_registry().family_labels


def get_slot_labels() -> Dict[str, str]:
    return load_type_registry().slot_labels


def _normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""
    return (
        value.strip()
        .replace("（", "")
        .replace("）", "")
        .replace("(", "")
        .replace(")", "")
        .replace("-", "")
        .replace("_", "")
        .replace(" ", "")
        .casefold()
    )


def _resolve_business_family(business_category: Optional[str]) -> str:
    registry = load_type_registry()
    normalized = _normalize_text(business_category)
    if not normalized:
        return "unknown"
    for alias, family in registry.business_category_aliases.items():
        if _normalize_text(alias) == normalized:
            return family
    return "unknown"


def _fallback_canonical_type(business_family: str, raw_product_type: Optional[str]) -> str:
    normalized_raw = _normalize_text(raw_product_type)
    if normalized_raw == _normalize_text("轻上装"):
        return "light_top"
    if normalized_raw == _normalize_text("女装"):
        return "womenwear"
    if business_family == "apparel":
        return "womenwear"
    if business_family in {"jewelry", "accessory"}:
        return "bangle"
    if business_family == "hair_accessory":
        return "hair_clip"
    return "womenwear"


def _match_canonical_type(raw_product_type: Optional[str], business_family: str) -> Optional[str]:
    registry = load_type_registry()
    normalized = _normalize_text(raw_product_type)
    if not normalized:
        return None

    matched = []
    for alias, canonical_type in registry.alias_to_type.items():
        if alias == normalized:
            matched.append(canonical_type)

    if not matched:
        return None

    if business_family in {"unknown", "accessory"}:
        return matched[0]

    for canonical_type in matched:
        if registry.type_by_canonical[canonical_type].family == business_family:
            return canonical_type

    return matched[0]


def normalize_product_type(raw_product_type: Optional[str], business_category: Optional[str] = None) -> ProductTypeContext:
    registry = load_type_registry()
    business_family = _resolve_business_family(business_category)
    matched_canonical_type = _match_canonical_type(raw_product_type, business_family)
    canonical_type = matched_canonical_type
    if canonical_type is None:
        canonical_type = _fallback_canonical_type(business_family, raw_product_type)

    definition = registry.type_by_canonical[canonical_type]
    display_type = raw_product_type.strip() if raw_product_type and raw_product_type.strip() else definition.display_name
    slot_label = registry.slot_labels.get(definition.slot, definition.slot)
    prompt_label = f"{display_type}，属于{slot_label}的{definition.description}"

    return ProductTypeContext(
        raw_product_type=raw_product_type or "",
        business_category=business_category or "",
        business_family=business_family,
        canonical_family=definition.family,
        canonical_slot=definition.slot,
        canonical_type=definition.canonical_type,
        display_type=display_type,
        prompt_label=prompt_label,
        required_terms=list(definition.required_terms),
        forbidden_terms=list(definition.forbidden_terms),
        recognized_by_registry=matched_canonical_type is not None,
        fallback_used=matched_canonical_type is None,
    )


def normalize_vision_type(
    vision_type: Optional[str] = None,
    vision_family: Optional[str] = None,
    vision_slot: Optional[str] = None,
    vision_confidence: Optional[float] = None,
) -> Optional[VisionContext]:
    if not any([vision_type, vision_family, vision_slot]):
        return None

    if vision_type:
        normalized = normalize_product_type(vision_type, vision_family)
        return VisionContext(
            family=normalized.canonical_family,
            slot=vision_slot or normalized.canonical_slot,
            canonical_type=normalized.canonical_type,
            confidence=vision_confidence,
        )

    return VisionContext(
        family=vision_family or "unknown",
        slot=vision_slot or "unknown",
        canonical_type="unknown",
        confidence=vision_confidence,
    )


def resolve_product_context(
    raw_product_type: Optional[str],
    business_category: Optional[str] = None,
    vision_type: Optional[str] = None,
    vision_family: Optional[str] = None,
    vision_slot: Optional[str] = None,
    vision_confidence: Optional[float] = None,
) -> ResolvedProductContext:
    table_context = normalize_product_type(raw_product_type, business_category)
    vision_context = normalize_vision_type(
        vision_type=vision_type,
        vision_family=vision_family,
        vision_slot=vision_slot,
        vision_confidence=vision_confidence,
    )

    conflict_level = "none"
    conflict_reason = None
    review_required = False
    resolution_policy = "prefer_table"
    block_required = False
    block_reason = None

    if vision_context:
        if table_context.canonical_family == vision_context.family and table_context.canonical_slot == vision_context.slot:
            if table_context.canonical_type != vision_context.canonical_type:
                conflict_level = "low"
                conflict_reason = "视觉类型与表格类型细分类不同，但佩戴/使用部位一致"
        else:
            conflict_level = "high"
            conflict_reason = "视觉识别与表格类型的族类或佩戴/使用部位冲突"
            review_required = True
        if (
            not table_context.recognized_by_registry
            and isinstance(vision_context.confidence, (int, float))
            and float(vision_context.confidence) >= TYPE_GUARD_HIGH_CONFIDENCE_THRESHOLD
            and (
                table_context.canonical_family != vision_context.family
                or table_context.canonical_slot != vision_context.slot
            )
        ):
            resolution_policy = "block_unrecognized_table_type"
            block_required = True
            block_reason = (
                f"产品类型“{table_context.raw_product_type or table_context.display_type}”未命中守卫词典，"
                f"当前兜底会落到 {table_context.canonical_family}/{table_context.canonical_slot}，"
                f"但图片高置信识别为 {vision_context.family}/{vision_context.slot}，请先补充产品类型映射后再执行。"
            )
            conflict_reason = block_reason
            review_required = True

    return ResolvedProductContext(
        **table_context.to_dict(),
        vision_family=vision_context.family if vision_context else None,
        vision_slot=vision_context.slot if vision_context else None,
        vision_type=vision_context.canonical_type if vision_context else None,
        vision_confidence=vision_context.confidence if vision_context else None,
        conflict_level=conflict_level,
        resolution_policy=resolution_policy,
        review_required=review_required,
        conflict_reason=conflict_reason,
        block_required=block_required,
        block_reason=block_reason,
    )
