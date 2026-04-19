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


def _find_terms(text: str, candidates: List[str]) -> List[str]:
    lowered = text.casefold()
    matches = []
    for candidate in candidates:
        if candidate and candidate.casefold() in lowered:
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
