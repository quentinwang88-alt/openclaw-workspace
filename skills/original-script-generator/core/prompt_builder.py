#!/usr/bin/env python3
"""
兼容层：复用现有 prompts.py 中的 prompt builder。
"""

from core.prompts import (
    build_anchor_card_prompt,
    build_expression_plan_prompt,
    build_final_strategy_prompt,
    build_final_video_prompt_prompt,
    build_opening_strategy_prompt,
    build_product_type_guard_prompt,
    build_p2_opening_prompt,
    build_p6_expression_plan_prompt,
    build_p8_variant_prompt,
    build_script_review_prompt,
    build_script_revision_prompt,
    build_script_prompt,
    build_strategy_prompt,
    build_styling_plan_prompt,
    build_variant_prompt,
)

__all__ = [
    "build_anchor_card_prompt",
    "build_product_type_guard_prompt",
    "build_opening_strategy_prompt",
    "build_styling_plan_prompt",
    "build_strategy_prompt",
    "build_final_strategy_prompt",
    "build_expression_plan_prompt",
    "build_script_review_prompt",
    "build_script_revision_prompt",
    "build_final_video_prompt_prompt",
    "build_variant_prompt",
    "build_script_prompt",
    "build_p2_opening_prompt",
    "build_p6_expression_plan_prompt",
    "build_p8_variant_prompt",
]
