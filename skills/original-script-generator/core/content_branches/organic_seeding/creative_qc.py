"""Non-blocking creative and retention review for organic-seeding scripts."""
from __future__ import annotations

import json
from typing import Any, Dict, List


STATIC_MARKERS = (
    "站着", "静止", "安静观察", "只看", "匀速", "保持坐姿", "自然站定",
    "stand still", "static", "look at the mirror",
)


def _text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _grade(*, strong: bool, available: bool) -> str:
    if strong:
        return "A"
    return "B" if available else "C"


def evaluate_organic_creative(
    *,
    topic_contract: Dict[str, Any],
    retention_contract: Dict[str, Any],
    visual_blueprint: Dict[str, Any],
    voiceover: Dict[str, Any],
    story_spine: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Return review grades only; safety QC remains the sole hard blocker."""

    units = [item for item in visual_blueprint.get("capture_units") or [] if isinstance(item, dict)]
    first = units[0] if units else {}
    opening = visual_blueprint.get("opening_design") or {}
    first_blob = " ".join(
        _text(first.get(key)) for key in ("shot", "camera_action", "subject_action", "information_gain")
    ).lower()
    first_duration = first.get("duration_seconds")
    first_duration_ok = isinstance(first_duration, (int, float)) and float(first_duration) <= 2.5
    first_has_action = bool(_text(first.get("subject_action")))
    first_is_static = any(marker.lower() in first_blob for marker in STATIC_MARKERS)
    opening_available = bool(
        _text(opening.get("visible_action"))
        and _text(retention_contract.get("first_3s_open_loop"))
        and first_has_action
    )
    opening_strong = bool(opening_available and first_duration_ok and not first_is_static)

    story = dict(story_spine or {})
    translation = _text(voiceover.get("chinese_translation"))
    hook_surface = _text(voiceover.get("hook_surface"))
    topic_thesis = _text(topic_contract.get("topic_thesis"))
    topic_available = bool(topic_thesis and hook_surface and len(translation) >= 8)
    topic_strong = bool(topic_available and len(hook_surface) >= 6 and len(translation) >= 18)

    gains: List[str] = []
    relations: List[str] = []
    for unit in units:
        gain = _text(unit.get("information_gain"))
        if gain and gain not in gains:
            gains.append(gain)
        relation = "|".join(
            _text(unit.get(key)) for key in ("shot_size", "camera_relation", "subject_action")
        )
        if relation and relation not in relations:
            relations.append(relation)
    information_available = len(units) >= 3 and len(relations) >= 2
    information_strong = len(gains) >= max(2, len(units) - 1) and len(relations) >= 3

    closing = _text(voiceover.get("closing_mode")).upper()
    has_question_surface = any(mark in translation for mark in ("?", "？", "还是", "会选", "你会"))
    comment_available = bool(_text(topic_contract.get("comment_trigger")))
    comment_strong = closing == "OPEN_DISCUSSION" and has_question_surface

    commerce = voiceover.get("commerce_elements") or {}
    organic_available = not any(value is True for value in commerce.values())
    audio = retention_contract.get("audio_arc") or {}
    audio_available = all(
        _text(audio.get(key)) for key in ("opening_energy", "voiceover_bed_energy", "payoff_energy")
    )
    motive = _text(voiceover.get("speaker_motive_realization")) or _text(story.get("creator_motive"))
    audience = _text(voiceover.get("viewer_relevance_realization")) or _text(story.get("viewer_relevance"))
    core_value = _text(voiceover.get("core_value_realization")) or _text(story.get("core_value"))
    affinity = _text(voiceover.get("affinity_residue")) or _text(story.get("affinity_residue"))
    analytic_markers = ("首先", "其次", "最后总结", "综上", "这说明", "因此可以看出")
    analytic_surface = any(marker in translation for marker in analytic_markers)
    spoken_available = bool(translation and _text(voiceover.get("target_text")))
    spoken_strong = bool(spoken_available and len(translation) >= 12 and not analytic_surface)
    measured_duration = voiceover.get("estimated_duration_seconds")
    duration_budget = voiceover.get("duration_budget_seconds")
    duration_ratio = (
        float(measured_duration) / float(duration_budget)
        if isinstance(measured_duration, (int, float))
        and isinstance(duration_budget, (int, float))
        and float(duration_budget) > 0
        else 0.0
    )
    density_available = duration_ratio >= 0.55
    density_strong = bool(core_value and 0.70 <= duration_ratio <= 0.98)

    opening_grade = "A" if opening_strong else ("C" if first_is_static or not opening_available else "B")
    dimensions = {
        "opening_strength": opening_grade,
        "topic_clarity": _grade(strong=topic_strong, available=topic_available),
        "information_gain": _grade(strong=information_strong, available=information_available),
        "commentability": _grade(strong=comment_strong, available=comment_available),
        "organicness": _grade(strong=organic_available, available=organic_available),
        "audio_arc": _grade(strong=audio_available, available=audio_available),
        "voiceover_density": _grade(strong=density_strong, available=density_available),
        "credible_motive": _grade(strong=bool(motive and core_value), available=bool(motive)),
        "audience_relevance": _grade(strong=bool(audience and core_value), available=bool(audience)),
        "affinity_residue": _grade(strong=bool(affinity and core_value), available=bool(affinity)),
        "spoken_naturalness": _grade(strong=spoken_strong, available=spoken_available),
        "market_fit": "B" if spoken_available else "C",
    }
    grades = list(dimensions.values())
    if dimensions["opening_strength"] == "C" or dimensions["topic_clarity"] == "C":
        overall = "C"
    elif "C" not in grades and grades.count("A") >= 6:
        overall = "A"
    elif grades.count("C") <= 1 and grades.count("A") >= 2:
        overall = "B"
    else:
        overall = "C"
    reasons: List[str] = []
    if dimensions["opening_strength"] == "C":
        reasons.append("FIRST_3S_ATTENTION_MOVE_UNAVAILABLE")
    elif dimensions["opening_strength"] == "B":
        reasons.append("FIRST_3S_ATTENTION_MOVE_WEAK")
    if dimensions["information_gain"] == "C":
        reasons.append("VISIBLE_INFORMATION_GAIN_LOW")
    if dimensions["topic_clarity"] == "C":
        reasons.append("TOPIC_THESIS_NOT_REALIZED")
    if dimensions["commentability"] == "C":
        reasons.append("COMMENT_TRIGGER_NOT_REALIZED")
    if dimensions["voiceover_density"] == "C":
        reasons.append("VOICEOVER_SPOKEN_SPACE_LIGHT")
    if dimensions["credible_motive"] == "C":
        reasons.append("CREDIBLE_SPEAKING_MOTIVE_UNAVAILABLE")
    if dimensions["audience_relevance"] == "C":
        reasons.append("AUDIENCE_RELEVANCE_UNAVAILABLE")
    if dimensions["affinity_residue"] == "C":
        reasons.append("AFFINITY_RESIDUE_UNAVAILABLE")
    if dimensions["spoken_naturalness"] == "C":
        reasons.append("SPOKEN_NATURALNESS_NEEDS_HUMAN_REVIEW")
    return {
        "schema_version": "organic-creative-qc-v2-story-soft-ranking",
        "overall_grade": overall,
        "dimensions": dimensions,
        "review_reasons": reasons,
        "production_recommendation": (
            "READY_FOR_HUMAN_REVIEW" if overall in {"A", "B"}
            else "CREATIVE_REVISION_RECOMMENDED"
        ),
        "hard_block": False,
        "evidence": {
            "first_clip_seconds": first_duration,
            "first_clip_static_marker": first_is_static,
            "unique_information_gains": len(gains),
            "unique_viewing_relations": len(relations),
            "topic_family": _text(topic_contract.get("topic_family")),
            "voiceover_hook_surface": hook_surface,
            "voiceover_duration_ratio": round(duration_ratio, 3),
            "story_id": _text(story.get("story_id")),
            "core_claim_ref": _text(story.get("core_claim_ref")),
            "machine_structural_pass_only": True,
            "native_language_review": "PENDING",
        },
    }
