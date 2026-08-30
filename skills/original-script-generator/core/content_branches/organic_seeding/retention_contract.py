"""First-three-second and audio-momentum contracts for organic seeding."""
from __future__ import annotations

from typing import Any, Dict

from .contracts import OrganicSeedThemeContract
from .topic_engine import OrganicTopicContract


def compile_retention_contract(
    *,
    theme: OrganicSeedThemeContract,
    topic: OrganicTopicContract,
    duration_seconds: float,
    story_spine: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    duration = max(6.0, float(duration_seconds or 15.0))
    payoff_start = min(6.0, round(duration * 0.4, 1))
    payoff_end = min(duration - 2.0, max(payoff_start + 2.0, round(duration * 0.67, 1)))
    story = dict(story_spine or {})
    has_core_value = bool(str(story.get("core_value") or "").strip())
    voiceover_min = round(duration * (0.70 if has_core_value else 0.47), 1)
    voiceover_max = round(duration * 0.96, 1)
    return {
        "schema_version": "organic-retention-contract-v3-adaptive-spoken-space",
        "topic_contract_id": topic.contract_id,
        "topic_family": topic.topic_family,
        "first_frame_job": topic.first_frame_strategy,
        "first_800ms_visual_move": (
            "从已经发生一半的动作、构图反差或结果状态开始；第一帧之后立即产生可见变化"
        ),
        "first_1500ms_spoken_move": (
            f"直接表达{topic.attention_mechanism}对应的判断、矛盾或结果，不用时间地点铺垫"
        ),
        "first_3s_open_loop": topic.open_loop,
        "payoff_window_seconds": [payoff_start, payoff_end],
        "payoff": topic.payoff,
        "comment_trigger": topic.comment_trigger,
        "voiceover_density": {
            "schema_version": "organic-spoken-space-v2-soft-target",
            "target_measured_seconds": [voiceover_min, voiceover_max],
            "content_mode": "CORE_VALUE_STORY" if has_core_value else "FACTUAL_OBSERVATION",
            "semantic_intent": "ONE_CREDIBLE_CAUSAL_THOUGHT",
            "single_topic_only": True,
            "second_selling_point_allowed": False,
            "minimum_is_hard": False,
            "padding_policy": "DO_NOT_PAD;LEAVE_ROOM_FOR_BGM_AND_NATURAL_SOUND",
        },
        "clip_rhythm": {
            "preferred_visible_clips": 4 if duration >= 12 else 3,
            "opening_clip_seconds": [1.2, 2.2],
            "later_clip_seconds": [2.5, 5.5],
            "max_consecutive_same_viewing_relation": 1,
            "information_gain_required_after_opening": True,
        },
        "static_opening_forbidden": [
            "只站着看镜头",
            "只看镜子",
            "匀速慢走且没有构图变化",
            "先用完整远景建立环境",
        ],
        "audio_arc": {
            "profile": topic.audio_profile,
            "opening_energy": "medium",
            "voiceover_bed_energy": "low",
            "payoff_energy": "medium",
            "opening_accent": "轻量节拍或环境音色变化，不使用商业转场音",
            "duck_under_voiceover": True,
        },
        "product_role": theme.product_role,
    }
