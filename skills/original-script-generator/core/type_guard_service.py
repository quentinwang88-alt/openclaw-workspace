#!/usr/bin/env python3
"""产品类型守卫服务，给不同生成入口统一接入。"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Dict, Optional

from core.product_type_resolution import ResolvedProductContext, resolve_product_context
from core.prompt_contract_builder import build_prompt_contract, build_prompt_contract_payload
from core.script_type_validator import ValidationResult, validate_generated_text


@dataclass(frozen=True)
class GenerationTypeGuard:
    context: ResolvedProductContext
    prompt_contract: str
    prompt_payload: Dict[str, object]

    def to_dict(self) -> Dict[str, object]:
        payload = asdict(self)
        payload["context"] = self.context.to_dict()
        return payload


def prepare_generation_type_guard(
    raw_product_type: Optional[str],
    business_category: Optional[str] = None,
    vision_type: Optional[str] = None,
    vision_family: Optional[str] = None,
    vision_slot: Optional[str] = None,
    vision_confidence: Optional[float] = None,
) -> GenerationTypeGuard:
    context = resolve_product_context(
        raw_product_type=raw_product_type,
        business_category=business_category,
        vision_type=vision_type,
        vision_family=vision_family,
        vision_slot=vision_slot,
        vision_confidence=vision_confidence,
    )
    return GenerationTypeGuard(
        context=context,
        prompt_contract=build_prompt_contract(context),
        prompt_payload=build_prompt_contract_payload(context),
    )


def validate_generation_output(text: str, guard: GenerationTypeGuard) -> ValidationResult:
    return validate_generated_text(text, guard.context)
