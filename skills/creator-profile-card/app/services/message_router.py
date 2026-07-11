"""Message routing for creator outreach."""

from __future__ import annotations

from typing import Any, Dict


BLOCKED_STAGES = {"冷却", "放弃"}


def route_message(
    *,
    outreach_scene: str,
    relationship_stage: str,
    current_action: str = "",
    manually_prioritized: bool = False,
) -> Dict[str, Any]:
    """Route by explicit scene + relationship stage.

    `outreach_scene` decides which message family to use.
    `relationship_stage` decides CTA strength and whether sending is allowed.
    """
    scene = (outreach_scene or "").strip().lower()
    stage = (relationship_stage or "冷").strip()

    if stage in BLOCKED_STAGES:
        return {
            "should_send": False,
            "message_purpose": "no_message",
            "reason": f"关系阶段={stage}",
        }

    if scene == "batch":
        if stage in {"热", "合作中"} and not manually_prioritized:
            return {
                "should_send": False,
                "message_purpose": "no_message",
                "reason": "热/合作中达人不走批量话术，应转单点/维护",
            }
        return {"should_send": True, "message_purpose": "batch_content_opportunity", "reason": ""}

    if scene == "single":
        if current_action in {"关系维护", "relationship_maintenance"}:
            return {"should_send": True, "message_purpose": "relationship_maintenance", "reason": ""}
        if current_action in {"轻跟进", "follow_up"}:
            return {"should_send": True, "message_purpose": "follow_up", "reason": ""}
        return {"should_send": True, "message_purpose": "single_product_invitation", "reason": ""}

    if scene == "maintenance":
        if current_action in {"轻跟进", "follow_up"}:
            return {"should_send": True, "message_purpose": "follow_up", "reason": ""}
        if current_action in {"主动新品邀约", "商品邀约", "single_product_invitation"}:
            return {"should_send": True, "message_purpose": "single_product_invitation", "reason": ""}
        return {"should_send": True, "message_purpose": "relationship_maintenance", "reason": ""}

    return {
        "should_send": False,
        "message_purpose": "no_message",
        "reason": "缺少明确 outreach_scene，禁止隐式猜测话术场景",
    }
