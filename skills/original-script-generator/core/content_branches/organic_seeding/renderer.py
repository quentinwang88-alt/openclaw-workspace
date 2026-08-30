"""Deterministic human-review and video-prompt projections."""
from __future__ import annotations

from typing import Any, Dict

from ..video_execution import render_video_execution_prompt


def render_complete_script(result: Dict[str, Any]) -> str:
    theme = result.get("seed_theme") or {}
    topic = result.get("topic_contract") or {}
    story = result.get("story_spine") or {}
    retention = result.get("retention_contract") or {}
    visual = result.get("visual_blueprint") or {}
    voice = result.get("voiceover") or {}
    creative_review = (result.get("quality") or {}).get("creative_review") or {}
    lines = [
        f"【种草目标】{theme.get('objective', '')}",
        f"【观看价值】{theme.get('viewer_payoff', '')}",
        f"【真实触发】{story.get('human_trigger', '')}",
        f"【对应观众】{story.get('viewer_relevance', '')}",
        f"【说话动机】{story.get('creator_motive', '')}",
        f"【核心价值】{story.get('core_value', '')}｜事实ID：{story.get('core_claim_ref', '')}",
        f"【种草余味】{story.get('affinity_residue', '')}",
        f"【话题命题】{topic.get('topic_thesis', '')}",
        f"【观众张力】{topic.get('audience_tension', '')}",
        f"【前3秒】首帧：{retention.get('first_frame_job', '')}｜悬念：{retention.get('first_3s_open_loop', '')}",
        f"【中段回答】{retention.get('payoff', '')}",
        f"【商品角色】{theme.get('product_role', '')}｜露出：{theme.get('product_prominence', '')}",
        f"【生活时刻】{theme.get('lived_context', '')}",
        f"【内容子角度】{theme.get('angle_family', '')}｜证明重点：{theme.get('proof_focus', '')}",
        f"【目标语言口播】{voice.get('target_text', '')}",
        f"【中文对照】{voice.get('chinese_translation', '')}",
        f"【创意评级】{creative_review.get('overall_grade', '待审')}",
        "【画面】",
    ]
    for unit in visual.get("capture_units") or []:
        if not isinstance(unit, dict):
            continue
        subject_action = unit.get("subject_action") or unit.get("action") or ""
        camera_action = unit.get("camera_action") or "固定机位"
        lines.append(
            f"- {unit.get('unit_id', '')}｜{unit.get('duration_seconds', '')}s｜"
            f"{unit.get('shot', '')}｜镜头：{camera_action}｜动作：{subject_action}｜"
            f"证据：{unit.get('product_evidence', '')}"
        )
    return "\n".join(lines).strip()


def render_video_prompt(result: Dict[str, Any]) -> str:
    brief = result.get("video_execution_brief")
    if isinstance(brief, dict) and brief.get("capture_units"):
        return render_video_execution_prompt(brief)
    visual = result.get("visual_blueprint") or {}
    explicit = str(visual.get("video_generation_prompt") or "").strip()
    return explicit
