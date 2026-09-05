"""Segment-local projection of existing shared visibility guidance."""
from typing import Any, Mapping

from core.visual_execution_contract import build_subject_visibility_guidance


def segment_visibility(master: Mapping[str, Any], scene: Mapping[str, Any]) -> dict:
    world = dict(master.get("production_world") or {})
    result = build_subject_visibility_guidance(
        presentation_mode=str(world.get("presentation_mode") or "PERSON_ON_CAMERA"),
        scene_card=scene,
        product_truth=dict(master.get("product_truth") or {}),
    )
    if not result:
        return {}
    # Styling was selected upstream. Only lighting/background may improve
    # separation; never resend the shared helper's alternative outfit advice.
    result["separation"]["outfit_guidance"] = (
        "冻结穿搭保持不变；仅通过背景、站位与光线调整商品边界。"
    )
    return result
