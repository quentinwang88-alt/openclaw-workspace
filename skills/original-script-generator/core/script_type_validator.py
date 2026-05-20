#!/usr/bin/env python3
"""生成结果的类型一致性质检。"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Dict, List

from core.product_type_resolution import ProductTypeContext, ResolvedProductContext


@dataclass(frozen=True)
class ValidationResult:
    is_valid: bool
    matched_forbidden_terms: List[str]
    missing_required_terms: List[str]
    violations: List[str]
    warnings: List[str]

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)


_NEGATION_MARKERS = (
    "不",
    "无",
    "未",
    "勿",
    "非",
    "别",
    "避免",
    "禁止",
    "不得",
    "不做",
    "不写",
    "不要",
    "不能",
    "不应",
    "不许",
    "没有",
    "拒绝",
    "规避",
    "禁止出现",
    "不得出现",
    "不承诺",
    "不暗示",
)


def _is_negated_match(text: str, start: int) -> bool:
    prefix = text[max(0, start - 24):start]
    compact_prefix = "".join(prefix.split())
    return any(marker in compact_prefix for marker in _NEGATION_MARKERS)


def _find_terms(text: str, candidates: List[str]) -> List[str]:
    lowered = text.casefold()
    matches = []
    for candidate in candidates:
        if not candidate:
            continue
        needle = candidate.casefold()
        start = lowered.find(needle)
        found_affirmative = False
        while start >= 0:
            if not _is_negated_match(text, start):
                found_affirmative = True
                break
            start = lowered.find(needle, start + len(needle))
        if found_affirmative:
            matches.append(candidate)
    return matches


def validate_generated_text(text: str, context: ProductTypeContext) -> ValidationResult:
    matched_forbidden_terms = _find_terms(text, list(context.forbidden_terms))
    matched_required_terms = _find_terms(text, list(context.required_terms))
    missing_required_terms = [term for term in context.required_terms if term not in matched_required_terms]

    violations: List[str] = []
    warnings: List[str] = []

    if matched_forbidden_terms:
        violations.append(
            "检测到与最终产品类型冲突的禁词：" + "、".join(matched_forbidden_terms)
        )

    if not matched_required_terms:
        warnings.append(
            "未检测到任何与最终佩戴/使用部位一致的锚点词，建议复核是否写偏。"
        )

    if isinstance(context, ResolvedProductContext) and context.conflict_level == "high" and not matched_required_terms:
        violations.append("高冲突任务未出现任何正确部位锚点词，判定为高风险结果。")

    return ValidationResult(
        is_valid=not violations,
        matched_forbidden_terms=matched_forbidden_terms,
        missing_required_terms=missing_required_terms,
        violations=violations,
        warnings=warnings,
    )
