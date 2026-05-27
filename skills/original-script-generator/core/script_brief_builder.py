#!/usr/bin/env python3
"""
代码侧 script_brief 构建器。
"""

import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional


_AI_SHOT_RISK_REGISTRY_CACHE: Optional[Dict[str, Any]] = None
_AI_SHOT_RISK_REGISTRY_PATH = Path(__file__).resolve().parent.parent / "config" / "ai_shot_risk_registry.json"
_CONTROL_LAYER_NON_CHINESE_RUN_RE = re.compile(r"[^\u4e00-\u9fff]{11,}")
_ENGINEERING_TOKEN_RE = re.compile(r"^[A-Za-z0-9_./|:-]{1,64}$")
_ENGINEERING_TOKEN_CHARS_RE = re.compile(r"[^A-Za-z0-9_./|:-]+")
_CONTROL_LAYER_CONTENT_KEYS = {"native_expression_entry", "suggested_short_line", "ending"}

logger = logging.getLogger(__name__)


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


def _take_template_items(values: Any, limit: int) -> List[Any]:
    if not isinstance(values, list):
        return []
    items: List[Any] = []
    for value in values:
        text = _non_empty_text(value)
        if isinstance(value, dict):
            item = {
                "id": _non_empty_text(value.get("id")),
                "desc": _non_empty_text(value.get("desc") or value.get("description")),
            }
            if item["id"] or item["desc"]:
                items.append(item)
        elif text:
            items.append(text)
        if len(items) >= limit:
            break
    return items


def _confidence_text(value: Any) -> str:
    text = _non_empty_text(value)
    return text if text in {"high", "medium", "low"} else "low"


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


def _take_replacement_templates(values: Any, limit: int = 6) -> List[Dict[str, str]]:
    if isinstance(values, dict):
        normalized_values = [
            {
                "template_id": _non_empty_text(template_id),
                "when_to_use": _non_empty_text(template_id),
                "replacement_shot": _non_empty_text(replacement_shot),
            }
            for template_id, replacement_shot in values.items()
        ]
        return _take_dict_items(normalized_values, limit, ["template_id", "when_to_use", "replacement_shot"])
    return _take_dict_items(values, limit, ["template_id", "when_to_use", "replacement_shot"])


def _take_enforced_key_visual_constraints(values: Any, limit: int = 3) -> List[Dict[str, str]]:
    if not isinstance(values, list):
        return []
    items: List[Dict[str, str]] = []
    for value in values:
        if not isinstance(value, dict):
            continue
        confidence = _non_empty_text(value.get("confidence"))
        if confidence not in {"high", "medium"}:
            continue
        item = {
            "constraint": _non_empty_text(value.get("constraint")),
            "confidence": confidence,
            "basis": _non_empty_text(value.get("basis")),
        }
        if item["constraint"]:
            items.append(item)
        if len(items) >= limit:
            break
    return items


def _build_hair_accessory_profile(anchor_card: Dict[str, Any]) -> Dict[str, str]:
    fields = [
        "hair_accessory_subtype",
        "placement_zone",
        "hold_scope",
        "orientation",
        "primary_result",
    ]
    return {field: _non_empty_text(anchor_card.get(field)) or "unknown" for field in fields}


def _build_category_execution_contract(anchor_card: Dict[str, Any]) -> Dict[str, Any]:
    raw_contract = anchor_card.get("category_execution_contract") if isinstance(anchor_card, dict) else {}
    if not isinstance(raw_contract, dict):
        raw_contract = {}

    audio_policy = raw_contract.get("audio_policy")
    if not isinstance(audio_policy, dict):
        audio_policy = {}
    field_confidence = raw_contract.get("field_confidence")
    if not isinstance(field_confidence, dict):
        field_confidence = {}

    product_subtype = _first_non_empty_text(
        raw_contract.get("product_subtype"),
        anchor_card.get("hair_accessory_subtype"),
        "unknown",
    )
    placement_zone = _first_non_empty_text(
        raw_contract.get("placement_zone"),
        anchor_card.get("placement_zone"),
        "unknown",
    )
    hold_scope = _first_non_empty_text(
        raw_contract.get("hold_scope"),
        anchor_card.get("hold_scope"),
        "unknown",
    )
    orientation = _first_non_empty_text(
        raw_contract.get("orientation"),
        anchor_card.get("orientation"),
        "unknown",
    )
    primary_visual_result = _first_non_empty_text(
        raw_contract.get("primary_visual_result"),
        anchor_card.get("primary_result"),
        "unknown",
    )

    return {
        "display_family": _non_empty_text(raw_contract.get("display_family")) or "unknown",
        "product_subtype": product_subtype,
        "use_case": _non_empty_text(raw_contract.get("use_case")) or "unknown",
        "placement_zone": placement_zone,
        "hold_scope": hold_scope,
        "orientation": orientation,
        "primary_visual_result": primary_visual_result,
        "operation_policy": _non_empty_text(raw_contract.get("operation_policy")) or "unknown",
        "field_confidence": {
            "product_subtype": _confidence_text(field_confidence.get("product_subtype")),
            "use_case": _confidence_text(field_confidence.get("use_case")),
            "placement_zone": _confidence_text(field_confidence.get("placement_zone")),
            "hold_scope": _confidence_text(field_confidence.get("hold_scope")),
            "orientation": _confidence_text(field_confidence.get("orientation")),
            "primary_visual_result": _confidence_text(field_confidence.get("primary_visual_result")),
            "operation_policy": _confidence_text(field_confidence.get("operation_policy")),
        },
        "safe_shot_templates": _take_template_items(raw_contract.get("safe_shot_templates"), 4),
        "forbidden_actions": _take_template_items(raw_contract.get("forbidden_actions"), 4),
        "result_priority": _non_empty_text(raw_contract.get("result_priority")),
        "season_context": {
            "primary_season": _non_empty_text(
                (raw_contract.get("season_context") or {}).get("primary_season")
                if isinstance(raw_contract.get("season_context"), dict)
                else ""
            )
            or "unknown",
            "weather_signal": _non_empty_text(
                (raw_contract.get("season_context") or {}).get("weather_signal")
                if isinstance(raw_contract.get("season_context"), dict)
                else ""
            )
            or "unknown",
        },
        "hat_risk_tier": _non_empty_text(raw_contract.get("hat_risk_tier")) or "unknown",
        "set_relationship": _non_empty_text(raw_contract.get("set_relationship")) or "unknown",
        "co_styling_hint": {
            "pair_with": _take_string_items(
                (raw_contract.get("co_styling_hint") or {}).get("pair_with")
                if isinstance(raw_contract.get("co_styling_hint"), dict)
                else [],
                4,
            ),
        },
        "audio_policy": {
            "bgm_style": _non_empty_text(audio_policy.get("bgm_style")),
            "bgm_energy": _non_empty_text(audio_policy.get("bgm_energy")) or "low",
            "voiceover_priority": _non_empty_text(audio_policy.get("voiceover_priority")) or "high",
            "sfx_policy": _non_empty_text(audio_policy.get("sfx_policy")),
            "allowed_sfx": _take_string_items(audio_policy.get("allowed_sfx"), 4),
            "forbidden_sfx": _take_string_items(audio_policy.get("forbidden_sfx"), 4),
            "sfx_timing_rules": _take_string_items(audio_policy.get("sfx_timing_rules"), 4),
            "audio_negative_constraints": _take_string_items(audio_policy.get("audio_negative_constraints"), 4),
        },
    }


def _build_human_performance_contract(persona_style_emotion_pack: Dict[str, Any]) -> Dict[str, Any]:
    raw_contract = (
        persona_style_emotion_pack.get("human_performance_contract")
        if isinstance(persona_style_emotion_pack, dict)
        else {}
    )
    if not isinstance(raw_contract, dict) or not raw_contract:
        return {}
    gaze_rule = raw_contract.get("gaze_rule") if isinstance(raw_contract.get("gaze_rule"), dict) else {}
    active_limit = raw_contract.get("active_micro_reaction_limit")
    if not isinstance(active_limit, int):
        active_limit = 0
    min_points = gaze_rule.get("min_points_required")
    if not isinstance(min_points, int):
        min_points = 3
    return {
        "performance_family": _non_empty_text(raw_contract.get("performance_family")),
        "persona_mode": _non_empty_text(raw_contract.get("persona_mode")),
        "expression_arc": _take_string_items(raw_contract.get("expression_arc"), 5),
        "gaze_plan": _take_string_items(raw_contract.get("gaze_plan"), 6),
        "gaze_rule": {
            "min_points_required": min_points,
            "final_point_options": _take_string_items(gaze_rule.get("final_point_options"), 4),
        },
        "micro_reaction_beats": _take_string_items(raw_contract.get("micro_reaction_beats"), 6),
        "body_language_beats": _take_string_items(raw_contract.get("body_language_beats"), 6),
        "product_interaction_beats": _take_string_items(raw_contract.get("product_interaction_beats"), 6),
        "relatable_moment": _non_empty_text(raw_contract.get("relatable_moment")),
        "performance_intensity": _non_empty_text(raw_contract.get("performance_intensity")),
        "active_micro_reaction_limit": active_limit,
        "forbidden_performance": _take_string_items(raw_contract.get("forbidden_performance"), 8),
        "scene_seed_brief": _build_scene_seed_brief(raw_contract.get("scene_seed_brief")),
    }


def _build_scene_seed_brief(raw_value: Any) -> Dict[str, Any]:
    if not isinstance(raw_value, dict) or not raw_value:
        return {}
    boundary = raw_value.get("micro_behavior_boundary") if isinstance(raw_value.get("micro_behavior_boundary"), dict) else {}
    strategy_by_role = _build_scene_seed_strategy_by_role(
        raw_value.get("strategy_by_script_role")
        or raw_value.get("scene_seed_strategy_by_script_role")
        or raw_value.get("scene_seed_strategy")
    )
    enabled = bool(raw_value.get("enabled"))
    brief = {
        "enabled": enabled,
        "display_family": _non_empty_text(raw_value.get("display_family")),
        "seed_goal": _non_empty_text(raw_value.get("seed_goal")),
        "strategy_by_script_role": strategy_by_role,
        "moment_hints": _take_string_items(raw_value.get("moment_hints"), 5),
        "small_tension_hints": _take_string_items(raw_value.get("small_tension_hints"), 5),
        "micro_behavior_boundary": {
            "safe_behavior_hints": _take_string_items(boundary.get("safe_behavior_hints"), 5),
            "risk_boundary": _take_string_items(boundary.get("risk_boundary"), 6),
        },
        "payoff_direction": _non_empty_text(raw_value.get("payoff_direction")),
        "anti_template_guidance": _take_string_items(raw_value.get("anti_template_guidance"), 4),
    }
    if not any(
        [
            brief["seed_goal"],
            brief["strategy_by_script_role"],
            brief["moment_hints"],
            brief["small_tension_hints"],
            brief["micro_behavior_boundary"]["safe_behavior_hints"],
            brief["payoff_direction"],
        ]
    ):
        return {}
    return brief


def _build_scene_seed_strategy_by_role(raw_value: Any) -> Dict[str, Dict[str, str]]:
    if not isinstance(raw_value, dict):
        return {}
    allowed_keys = [
        "seed_mode",
        "moment_bias",
        "tension_bias",
        "camera_gaze_bias",
        "payoff_bias",
    ]
    strategies: Dict[str, Dict[str, str]] = {}
    for role in ("S1", "S2", "S3", "S4"):
        value = raw_value.get(role) or raw_value.get(role.lower())
        if not isinstance(value, dict):
            continue
        item = {key: _non_empty_text(value.get(key)) for key in allowed_keys}
        if any(item.values()):
            strategies[role] = item
    return strategies


def _format_path(path_segments: List[str]) -> str:
    return ".".join(segment for segment in path_segments if segment)


def _should_skip_control_layer_warning(path_segments: List[str], value: str) -> bool:
    if not value:
        return True
    last_key = path_segments[-1] if path_segments else ""
    if "audio_policy" in path_segments:
        return True
    if last_key in _CONTROL_LAYER_CONTENT_KEYS:
        return True
    if _ENGINEERING_TOKEN_RE.fullmatch(value):
        return True
    long_runs = _CONTROL_LAYER_NON_CHINESE_RUN_RE.findall(value)
    if long_runs:
        normalized_runs = [
            _ENGINEERING_TOKEN_CHARS_RE.sub("", run)
            for run in long_runs
        ]
        if normalized_runs and all(
            (not run) or bool(_ENGINEERING_TOKEN_RE.fullmatch(run))
            for run in normalized_runs
        ):
            return True
    return False


def _clean_expression_control_text(value: Any) -> str:
    text = _non_empty_text(value)
    if not text:
        return ""
    text = re.sub(
        r"[“\"']([^”\"']*[A-Za-zÀ-ỹĐđ][^”\"']*)[”\"']",
        "“目标语言口播由 P7 按意图生成”",
        text,
    )
    text = re.sub(
        r"((?:目标语言|越南语|泰语|英语|印尼语|马来语)[^。；;\n]{0,16}(?:口播|字幕)[^。；;\n]{0,16})[：:][^。；;\n]+",
        r"\1：按口播意图生成，不在控制字段预写完整句子",
        text,
    )
    text = re.sub(
        r"((?:口播|字幕)[^。；;\n]{0,16}(?:使用|用)(?:目标语言|越南语|泰语|英语|印尼语|马来语)[^。；;\n]{0,12})[：:][^。；;\n]+",
        r"\1：按口播意图生成，不在控制字段预写完整句子",
        text,
    )
    return text.strip()


def _log_control_layer_language_warnings(payload: Any, path_segments: Optional[List[str]] = None) -> None:
    path_segments = path_segments or []
    if isinstance(payload, dict):
        for key, value in payload.items():
            _log_control_layer_language_warnings(value, [*path_segments, str(key)])
        return
    if isinstance(payload, list):
        for index, value in enumerate(payload):
            _log_control_layer_language_warnings(value, [*path_segments, f"[{index}]"])
        return
    if not isinstance(payload, str):
        return
    text = payload.strip()
    if _should_skip_control_layer_warning(path_segments, text):
        return
    match = _CONTROL_LAYER_NON_CHINESE_RUN_RE.search(text)
    if not match:
        return
    logger.warning(
        "script_brief_builder 控制层字段疑似语言混用: path=%s run=%s value=%s",
        _format_path(path_segments),
        match.group(0)[:40],
        text[:120],
    )


def _load_ai_shot_risk_registry() -> Dict[str, Any]:
    global _AI_SHOT_RISK_REGISTRY_CACHE
    if _AI_SHOT_RISK_REGISTRY_CACHE is not None:
        return _AI_SHOT_RISK_REGISTRY_CACHE
    try:
        _AI_SHOT_RISK_REGISTRY_CACHE = json.loads(_AI_SHOT_RISK_REGISTRY_PATH.read_text(encoding="utf-8"))
    except Exception:
        _AI_SHOT_RISK_REGISTRY_CACHE = {}
    return _AI_SHOT_RISK_REGISTRY_CACHE


def _ai_shot_risk_profile_key(product_type: str) -> str:
    text = _non_empty_text(product_type)
    if any(token in text for token in ("耳线", "耳环", "耳饰", "耳钉", "耳夹", "耳坠")):
        return "ear_accessory"
    if any(token in text for token in ("发饰", "发夹", "抓夹", "边夹", "发箍", "发圈", "头绳", "头箍")):
        return "hair_accessory"
    if any(token in text for token in ("项链", "戒指", "手链", "手镯", "手环", "饰品", "首饰", "包", "帽", "围巾", "墨镜")):
        return "general_accessory"
    return "clothing"


def _build_ai_shot_risk_profile(product_type: str) -> Dict[str, Any]:
    registry = _load_ai_shot_risk_registry()
    profile_key = _ai_shot_risk_profile_key(product_type)
    profile = registry.get(profile_key)
    if not isinstance(profile, dict):
        profile = registry.get("default") if isinstance(registry.get("default"), dict) else {}
    if not isinstance(profile, dict):
        profile = {}
    return {
        "profile_key": profile_key,
        "forbidden": _take_string_items(profile.get("forbidden"), 6),
        "high_risk": _take_string_items(profile.get("high_risk"), 6),
        "medium_risk": _take_string_items(profile.get("medium_risk"), 6),
        "low_risk": _take_string_items(profile.get("low_risk"), 6),
        "replacement_templates": _take_replacement_templates(profile.get("replacement_templates"), 6),
        "candidate_failures": _take_string_items(profile.get("candidate_failures"), 6),
    }


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
                "scene_seed": " / ".join(
                    part
                    for part in [
                        _non_empty_text(
                            (script_json.get("scene_seed") or {}).get("moment")
                            if isinstance(script_json.get("scene_seed"), dict)
                            else ""
                        ),
                        _non_empty_text(
                            (script_json.get("scene_seed") or {}).get("small_tension")
                            if isinstance(script_json.get("scene_seed"), dict)
                            else ""
                        ),
                    ]
                    if part
                ),
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
    ai_shot_risk_profile = _build_ai_shot_risk_profile(product_type)
    script_brief = {
        "product_type": _non_empty_text(product_type),
        "product_positioning_one_liner": _non_empty_text(anchor_card.get("product_positioning_one_liner")),
        "hard_anchors": _take_dict_items(anchor_card.get("hard_anchors"), 3, ["anchor", "reason_not_changeable", "confidence"]),
        "display_anchors": _take_dict_items(anchor_card.get("display_anchors"), 3, ["anchor", "why_must_show", "recommended_shot_type"]),
        "key_visual_constraints": _take_enforced_key_visual_constraints(anchor_card.get("key_visual_constraints")),
        "hair_accessory_profile": _build_hair_accessory_profile(anchor_card),
        "category_execution_contract": _build_category_execution_contract(anchor_card),
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
        "focus_control": {
            "script_role": _non_empty_text(final_strategy.get("script_role")),
            "primary_focus": _non_empty_text(final_strategy.get("primary_focus")),
            "secondary_focus": _non_empty_text(final_strategy.get("secondary_focus")),
        },
        "ai_shot_risk_profile": ai_shot_risk_profile,
        "persona_pack": {
            "persona_state": _non_empty_text(persona_style_emotion_pack.get("persona_state") or final_strategy.get("persona_state")),
            "appearance_anchor": _non_empty_text(persona_style_emotion_pack.get("appearance_anchor")),
            "attractiveness_boundary": _non_empty_text(persona_style_emotion_pack.get("attractiveness_boundary")),
            "hairstyle_rule": _non_empty_text(persona_style_emotion_pack.get("hairstyle_rule")),
            "makeup_rule": _non_empty_text(persona_style_emotion_pack.get("makeup_rule")),
            "clothing_rule": _non_empty_text(persona_style_emotion_pack.get("clothing_rule")),
            "accessory_rule": _non_empty_text(persona_style_emotion_pack.get("accessory_rule")),
            "emotion_progression": _non_empty_text(persona_style_emotion_pack.get("emotion_progression")),
            "movement_style": _non_empty_text(persona_style_emotion_pack.get("movement_style")),
            "anti_template_warnings": _take_string_items(persona_style_emotion_pack.get("anti_template_warnings"), 4),
        },
        "human_performance_contract": _build_human_performance_contract(persona_style_emotion_pack),
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
            "final_strategy_id": _non_empty_text(final_strategy.get("final_strategy_id")),
            "strategy_name": _non_empty_text(final_strategy.get("strategy_name")),
            "script_role": _non_empty_text(final_strategy.get("script_role")),
            "primary_focus": _non_empty_text(final_strategy.get("primary_focus")),
            "secondary_focus": _non_empty_text(final_strategy.get("secondary_focus")),
            "primary_selling_point": primary_selling_point,
            "dominant_user_question": _non_empty_text(final_strategy.get("dominant_user_question")),
            "proof_thesis": _non_empty_text(final_strategy.get("proof_thesis")),
            "decision_thesis": _non_empty_text(final_strategy.get("decision_thesis")),
            "main_attention_mechanism": _non_empty_text(final_strategy.get("main_attention_mechanism")),
            "main_shooting_method": _non_empty_text(final_strategy.get("main_shooting_method")),
            "aux_shooting_method": _non_empty_text(final_strategy.get("aux_shooting_method")),
            "selected_opening_strategy_name": _non_empty_text(final_strategy.get("selected_opening_strategy_name")),
            "opening_angle": _non_empty_text(final_strategy.get("opening_angle")),
            "proof_path": _non_empty_text(final_strategy.get("proof_path")),
            "performance_strategy_hint": _non_empty_text(
                final_strategy.get("performance_strategy_hint") or final_strategy.get("performance_bias")
            ),
            "contract_alignment_note": _non_empty_text(final_strategy.get("contract_alignment_note")),
            "risk_controls": _take_string_items(final_strategy.get("risk_controls"), 4),
            "opening_mode": _non_empty_text(final_strategy.get("opening_mode")),
            "opening_strategy": _non_empty_text(final_strategy.get("opening_strategy")),
            "opening_first_line_type": _non_empty_text(final_strategy.get("opening_first_line_type")),
            "opening_first_shot": _non_empty_text(final_strategy.get("opening_first_shot")),
            "visual_entry_mode": _non_empty_text(final_strategy.get("visual_entry_mode")),
            "proof_mode": _non_empty_text(final_strategy.get("proof_mode")),
            "selling_point_proof_method": _non_empty_text(final_strategy.get("selling_point_proof_method")),
            "core_proof_method": _non_empty_text(final_strategy.get("core_proof_method")),
            "ending_mode": _non_empty_text(final_strategy.get("ending_mode")),
            "purchase_bridge_method": _non_empty_text(final_strategy.get("purchase_bridge_method")),
            "decision_style": _non_empty_text(final_strategy.get("decision_style")),
            "scene_suggestion": _non_empty_text(final_strategy.get("scene_suggestion")),
            "scene_subspace": _non_empty_text(final_strategy.get("scene_subspace")),
            "scene_function": _non_empty_text(final_strategy.get("scene_function")),
            "persona_state_suggestion": _non_empty_text(final_strategy.get("persona_state_suggestion")),
            "persona_state": _non_empty_text(final_strategy.get("persona_state")),
            "persona_presence_role": _non_empty_text(final_strategy.get("persona_presence_role")),
            "persona_polish_level": _non_empty_text(final_strategy.get("persona_polish_level")),
            "rhythm_signature": _non_empty_text(final_strategy.get("rhythm_signature")),
            "action_entry_mode": _non_empty_text(final_strategy.get("action_entry_mode")),
            "styling_base_logic": _non_empty_text(final_strategy.get("styling_base_logic")),
            "styling_base_constraints": _take_string_items(final_strategy.get("styling_base_constraints"), 6),
            "styling_completion_tag": styling_completion_tag,
            "persona_visual_tone": persona_visual_tone,
            "styling_key_anchor": styling_key_anchor,
            "emotion_arc_tag": emotion_arc_tag,
            "opening_emotion": _non_empty_text(final_strategy.get("opening_emotion")),
            "middle_emotion": _non_empty_text(final_strategy.get("middle_emotion")),
            "ending_emotion": _non_empty_text(final_strategy.get("ending_emotion")),
            "voiceover_style": _non_empty_text(final_strategy.get("voiceover_style")),
            "product_dominance_rule": _non_empty_text(final_strategy.get("product_dominance_rule")),
            "realism_principles": _take_string_items(final_strategy.get("realism_principles"), 6),
            "forbidden_patterns": _take_string_items(final_strategy.get("forbidden_patterns"), 6),
            "risk_note": _non_empty_text(final_strategy.get("risk_note")),
        },
        "expression_plan": {
            "native_expression_entry": _non_empty_text(expression_plan.get("native_expression_entry")),
            "opening_expression_task": _clean_expression_control_text(expression_plan.get("opening_expression_task")),
            "middle_expression_task": _clean_expression_control_text(expression_plan.get("middle_expression_task")),
            "ending_expression_task": _clean_expression_control_text(expression_plan.get("ending_expression_task")),
            "voiceover_intent": _non_empty_text(expression_plan.get("voiceover_intent")),
            "voiceover_language_requirement": _non_empty_text(expression_plan.get("voiceover_language_requirement")),
            "most_likely_empty_point": _non_empty_text(expression_plan.get("most_likely_empty_point")),
        },
        "avoid_same_as_existing_scripts": _summarize_existing_scripts(existing_scripts),
    }
    _log_control_layer_language_warnings(script_brief)
    return script_brief
