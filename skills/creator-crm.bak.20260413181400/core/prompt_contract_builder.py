#!/usr/bin/env python3
"""根据最终类型上下文生成 prompt 契约。"""

from __future__ import annotations

from typing import Dict

from core.product_type_resolution import ProductTypeContext, get_family_labels, get_slot_labels


def build_prompt_contract_payload(context: ProductTypeContext) -> Dict[str, object]:
    family_labels = get_family_labels()
    slot_labels = get_slot_labels()

    return {
        "business_category": context.business_category or family_labels.get(context.canonical_family, context.canonical_family),
        "family_label": family_labels.get(context.canonical_family, context.canonical_family),
        "display_type": context.display_type,
        "slot_label": slot_labels.get(context.canonical_slot, context.canonical_slot),
        "prompt_label": context.prompt_label,
        "required_terms": list(context.required_terms),
        "forbidden_terms": list(context.forbidden_terms),
    }


def build_prompt_contract(context: ProductTypeContext) -> str:
    payload = build_prompt_contract_payload(context)
    required_terms = "、".join(payload["required_terms"]) if payload["required_terms"] else "无"
    forbidden_terms = "、".join(payload["forbidden_terms"]) if payload["forbidden_terms"] else "无"

    return (
        f"- 业务大类：{payload['business_category']}\n"
        f"- 标准族类：{payload['family_label']}\n"
        f"- 最终产品类型：{payload['display_type']}\n"
        f"- 标准佩戴/使用部位：{payload['slot_label']}\n"
        f"- 类型解释：{payload['prompt_label']}\n"
        f"- 必须围绕这些词展开：{required_terms}\n"
        f"- 禁止出现这些错类词：{forbidden_terms}\n"
        "- 如果图片视觉上与最终产品类型冲突，必须以最终产品类型和标准佩戴/使用部位为准，不得擅自改写到其他身体部位。\n"
        "- 如果图片存在白底、无尺度参照、单张易误判等情况，也必须优先遵循最终产品类型。"
    )
