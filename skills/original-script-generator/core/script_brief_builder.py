#!/usr/bin/env python3
"""
代码侧 script_brief 构建器。
"""

from typing import Any, Dict, List, Optional


def _non_empty_text(value: Any) -> str:
    return str(value or "").strip()


def _first_non_empty_text(*values: Any) -> str:
    for value in values:
        text = _non_empty_text(value)
        if text:
            return text
    return ""


def _take_string_items(values: Any, limit: int) -> List[str]:
    if not isinstance(values, list):
        return []
    items: List[str] = []
    for value in values:
        text = _non_empty_text(value)
        if text:
            items.append(text)
        if len(items) >= limit:
            break
    return items


def _take_dict_items(values: Any, limit: int, keys: List[str]) -> List[Dict[str, str]]:
    if not isinstance(values, list):
        return []
    items: List[Dict[str, str]] = []
    for value in values:
        if not isinstance(value, dict):
            continue
        item = {key: _non_empty_text(value.get(key)) for key in keys}
        if any(item.values()):
            items.append(item)
        if len(items) >= limit:
            break
    return items


def _pick_candidate_selling_point(anchor_card: Dict[str, Any], primary_selling_point: str) -> Dict[str, str]:
    candidates = anchor_card.get("candidate_primary_selling_points") if isinstance(anchor_card, dict) else []
    if not isinstance(candidates, list):
        return {}
    normalized_primary = _non_empty_text(primary_selling_point).lower()
    best_match: Optional[Dict[str, Any]] = None
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        selling_point = _non_empty_text(candidate.get("selling_point"))
        if not selling_point:
            continue
        lowered = selling_point.lower()
        if normalized_primary and (normalized_primary in lowered or lowered in normalized_primary):
            best_match = candidate
            break
        if best_match is None:
            best_match = candidate
    if not isinstance(best_match, dict):
        return {}
    return {
        "selling_point": _non_empty_text(best_match.get("selling_point")),
        "how_to_tell": _non_empty_text(best_match.get("how_to_tell")),
        "how_to_show": _non_empty_text(best_match.get("how_to_show")),
        "risk_if_missed": _non_empty_text(best_match.get("risk_if_missed")),
    }


def _pick_opening_strategy(opening_strategies: Dict[str, Any], final_strategy: Dict[str, Any]) -> Dict[str, str]:
    items = opening_strategies.get("opening_strategies") if isinstance(opening_strategies, dict) else []
    if not isinstance(items, list):
        return {}
    selected_name = _non_empty_text(final_strategy.get("selected_opening_strategy_name"))
    opening_mode = _non_empty_text(final_strategy.get("opening_mode"))
    visual_entry_mode = _non_empty_text(final_strategy.get("visual_entry_mode"))
    fallback: Optional[Dict[str, Any]] = None
    for item in items:
        if not isinstance(item, dict):
            continue
        if fallback is None:
            fallback = item
        if selected_name and _non_empty_text(item.get("strategy_name")) == selected_name:
            fallback = item
            break
        if opening_mode and opening_mode == _non_empty_text(item.get("opening_mode_candidate")):
            fallback = item
        if visual_entry_mode and visual_entry_mode == _non_empty_text(item.get("visual_entry_mode_candidate")):
            fallback = item
    if not isinstance(fallback, dict):
        return {}
    return {
        "strategy_name": _non_empty_text(fallback.get("strategy_name")),
        "angle_bucket": _non_empty_text(fallback.get("angle_bucket")),
        "opening_mode_candidate": _non_empty_text(fallback.get("opening_mode_candidate")),
        "visual_entry_mode_candidate": _non_empty_text(fallback.get("visual_entry_mode_candidate")),
        "first_frame_visual": _non_empty_text(fallback.get("first_frame_visual")),
        "action_design": _non_empty_text(fallback.get("action_design")),
        "first_product_focus": _non_empty_text(fallback.get("first_product_focus")),
        "native_expression_entry": _non_empty_text(fallback.get("native_expression_entry")),
        "opening_first_line_type": _non_empty_text(fallback.get("opening_first_line_type")),
        "suggested_short_line": _non_empty_text(fallback.get("suggested_short_line")),
        "style_note": _non_empty_text(fallback.get("style_note")),
        "risk_note": _non_empty_text(fallback.get("risk_note")),
    }


def _summarize_existing_scripts(existing_scripts: Optional[Dict[str, Dict[str, Any]]]) -> List[Dict[str, str]]:
    if not isinstance(existing_scripts, dict):
        return []
    results: List[Dict[str, str]] = []
    for strategy_id, script_json in existing_scripts.items():
        if not isinstance(script_json, dict):
            continue
        storyboard = script_json.get("storyboard") if isinstance(script_json.get("storyboard"), list) else []
        first_shot = storyboard[0] if storyboard and isinstance(storyboard[0], dict) else {}
        results.append(
            {
                "strategy_id": _non_empty_text(strategy_id),
                "opening": _non_empty_text(first_shot.get("shot_content")),
                "action": _non_empty_text(first_shot.get("person_action")),
                "ending": (
                    _non_empty_text((storyboard[-1] or {}).get("voiceover_text_target_language"))
                    or _non_empty_text((storyboard[-1] or {}).get("voiceover_text_zh"))
                )
                if storyboard
                else "",
            }
        )
    return results[:3]


def build_script_brief(
    product_type: str,
    anchor_card: Dict[str, Any],
    opening_strategies: Dict[str, Any],
    persona_style_emotion_pack: Dict[str, Any],
    final_strategy: Dict[str, Any],
    expression_plan: Dict[str, Any],
    existing_scripts: Optional[Dict[str, Dict[str, Any]]] = None,
    type_guard_json: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    primary_selling_point = _non_empty_text(final_strategy.get("primary_selling_point"))
    styling_completion_tag = _first_non_empty_text(
        final_strategy.get("styling_completion_tag"),
        persona_style_emotion_pack.get("styling_completion_tag"),
    )
    persona_visual_tone = _first_non_empty_text(
        final_strategy.get("persona_visual_tone"),
        persona_style_emotion_pack.get("persona_visual_tone"),
    )
    styling_key_anchor = _first_non_empty_text(
        final_strategy.get("styling_key_anchor"),
        persona_style_emotion_pack.get("styling_key_anchor"),
    )
    emotion_arc_tag = _first_non_empty_text(
        final_strategy.get("emotion_arc_tag"),
        persona_style_emotion_pack.get("emotion_arc_tag"),
    )
    return {
        "product_type": _non_empty_text(product_type),
        "product_positioning_one_liner": _non_empty_text(anchor_card.get("product_positioning_one_liner")),
        "hard_anchors": _take_dict_items(anchor_card.get("hard_anchors"), 5, ["anchor", "reason_not_changeable", "confidence"]),
        "display_anchors": _take_dict_items(anchor_card.get("display_anchors"), 5, ["anchor", "why_must_show", "recommended_shot_type"]),
        "parameter_anchors": _take_dict_items(
            anchor_card.get("parameter_anchors"),
            5,
            ["parameter_name", "parameter_value", "why_must_preserve", "execution_note", "confidence"],
        ),
        "selected_primary_selling_point": _pick_candidate_selling_point(anchor_card, primary_selling_point),
        "operation_anchors": _take_string_items(anchor_card.get("operation_anchors"), 4),
        "fixation_result_anchors": _take_string_items(anchor_card.get("fixation_result_anchors"), 4),
        "before_after_result_anchors": _take_string_items(anchor_card.get("before_after_result_anchors"), 4),
        "selected_opening_strategy": _pick_opening_strategy(opening_strategies, final_strategy),
        "persona_pack": {
            "persona_state": _non_empty_text(persona_style_emotion_pack.get("persona_state") or final_strategy.get("persona_state")),
            "hairstyle_rule": _non_empty_text(persona_style_emotion_pack.get("hairstyle_rule")),
            "clothing_rule": _non_empty_text(persona_style_emotion_pack.get("clothing_rule")),
            "emotion_progression": _non_empty_text(persona_style_emotion_pack.get("emotion_progression")),
            "anti_template_warnings": _take_string_items(persona_style_emotion_pack.get("anti_template_warnings"), 4),
        },
        "light_control_fields": {
            "styling_completion_tag": styling_completion_tag,
            "persona_visual_tone": persona_visual_tone,
            "styling_key_anchor": styling_key_anchor,
            "emotion_arc_tag": emotion_arc_tag,
        },
        "type_guard": {
            "display_type": _non_empty_text((type_guard_json or {}).get("display_type")),
            "canonical_family": _non_empty_text((type_guard_json or {}).get("canonical_family")),
            "canonical_slot": _non_empty_text((type_guard_json or {}).get("canonical_slot")),
            "prompt_label": _non_empty_text((type_guard_json or {}).get("prompt_label")),
            "resolution_policy": _non_empty_text((type_guard_json or {}).get("resolution_policy")),
            "conflict_level": _non_empty_text((type_guard_json or {}).get("conflict_level")),
            "conflict_reason": _non_empty_text((type_guard_json or {}).get("conflict_reason")),
            "required_terms": _take_string_items((type_guard_json or {}).get("required_terms"), 6),
            "forbidden_terms": _take_string_items((type_guard_json or {}).get("forbidden_terms"), 6),
            "prompt_contract": _non_empty_text((type_guard_json or {}).get("prompt_contract")),
        },
        "final_strategy": {
            "strategy_id": _non_empty_text(final_strategy.get("strategy_id")),
            "strategy_name": _non_empty_text(final_strategy.get("strategy_name")),
            "primary_selling_point": primary_selling_point,
            "dominant_user_question": _non_empty_text(final_strategy.get("dominant_user_question")),
            "proof_thesis": _non_empty_text(final_strategy.get("proof_thesis")),
            "decision_thesis": _non_empty_text(final_strategy.get("decision_thesis")),
            "opening_mode": _non_empty_text(final_strategy.get("opening_mode")),
            "visual_entry_mode": _non_empty_text(final_strategy.get("visual_entry_mode")),
            "proof_mode": _non_empty_text(final_strategy.get("proof_mode")),
            "ending_mode": _non_empty_text(final_strategy.get("ending_mode")),
            "scene_subspace": _non_empty_text(final_strategy.get("scene_subspace")),
            "rhythm_signature": _non_empty_text(final_strategy.get("rhythm_signature")),
            "action_entry_mode": _non_empty_text(final_strategy.get("action_entry_mode")),
            "styling_completion_tag": styling_completion_tag,
            "persona_visual_tone": persona_visual_tone,
            "styling_key_anchor": styling_key_anchor,
            "emotion_arc_tag": emotion_arc_tag,
        },
        "expression_plan": {
            "native_expression_entry": _non_empty_text(expression_plan.get("native_expression_entry")),
            "opening_expression_task": _non_empty_text(expression_plan.get("opening_expression_task")),
            "middle_expression_task": _non_empty_text(expression_plan.get("middle_expression_task")),
            "ending_expression_task": _non_empty_text(expression_plan.get("ending_expression_task")),
            "most_likely_empty_point": _non_empty_text(expression_plan.get("most_likely_empty_point")),
        },
        "avoid_same_as_existing_scripts": _summarize_existing_scripts(existing_scripts),
    }
