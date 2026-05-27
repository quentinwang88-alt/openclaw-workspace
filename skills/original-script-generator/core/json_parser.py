#!/usr/bin/env python3
"""
LLM JSON 解析与 schema 校验。
"""

import json
import re
from typing import Any, Dict, List, Optional

from core.constants import SCRIPT_ROLES


class JSONParseError(Exception):
    """LLM JSON 解析失败。"""


_CJK_RE = re.compile(r"[\u4e00-\u9fff]")
_LATINISH_RE = re.compile(r"[A-Za-zÀ-ỹĐđ]")
HAIR_ACCESSORY_FIELD_OPTIONS = {
    "hair_accessory_subtype": {
        "",
        "scrunchie",
        "small_side_clip",
        "claw_clip",
        "headband",
        "hair_tie",
        "hair_band",
        "styling_tool",
        "other_hair_accessory",
        "unknown",
    },
    "placement_zone": {
        "",
        "face_side",
        "back_head",
        "top_head",
        "low_ponytail",
        "half_up",
        "bun_area",
        "full_head",
        "unknown",
    },
    "hold_scope": {
        "",
        "flyaway_hair",
        "small_hair_section",
        "half_hair",
        "low_ponytail",
        "bun",
        "decorative_only",
        "unknown",
    },
    "orientation": {
        "",
        "horizontal_clip",
        "vertical_clip",
        "wrap_around",
        "tie_up",
        "insert_fix",
        "wear_on_head",
        "unknown",
    },
    "primary_result": {
        "",
        "cleaner_hairline",
        "stronger_hold",
        "more_complete_hairstyle",
        "faster_hair_fix",
        "decorative_focus",
        "softer_face_shape",
        "more_volume",
        "unknown",
    },
}
CATEGORY_CONTRACT_FIELD_OPTIONS = {
    "display_family": {
        "",
        "ear_accessory",
        "hair_accessory",
        "apparel",
        "apparel_accessory",
        "general_accessory",
        "unknown",
    },
    "product_subtype": {
        "",
        "scrunchie",
        "small_side_clip",
        "claw_clip",
        "headband",
        "hair_tie",
        "hair_band",
        "styling_tool",
        "other_hair_accessory",
        "scarf",
        "hat",
        "scarf_hat_set",
        "unknown",
    },
    "use_case": {
        "",
        "low_ponytail",
        "low_bun",
        "loose_bun",
        "low_bun_or_loose_bun",
        "half_up",
        "bun_area",
        "face_side_fix",
        "back_head_fix",
        "top_head_wear",
        "ponytail_or_bun_uncertain",
        "winter_outing",
        "winter_commute",
        "winter_travel",
        "cold_weather_outfit",
        "photo_outfit",
        "before_going_out",
        "daily_commute",
        "unknown",
    },
    "placement_zone": {
        "",
        "face_side",
        "back_head",
        "top_head",
        "low_ponytail",
        "half_up",
        "bun_area",
        "full_head",
        "neck_shoulder",
        "head",
        "head_face",
        "upper_body",
        "scarf_hat_combo",
        "unknown",
    },
    "hold_scope": {
        "",
        "flyaway_hair",
        "small_hair_section",
        "half_hair",
        "low_ponytail",
        "bun",
        "decorative_only",
        "upper_body_styling",
        "face_frame",
        "warmth_visual_coverage",
        "winter_outfit_completion",
        "decorative_outfit",
        "unknown",
    },
    "orientation": {
        "",
        "horizontal_clip",
        "vertical_clip",
        "wrap_around",
        "tie_up",
        "insert_fix",
        "wear_on_head",
        "wrapped_neck",
        "draped_shoulder",
        "worn_on_head",
        "front_brim",
        "full_set_wear",
        "unknown",
    },
    "operation_policy": {
        "",
        "result_first_process_avoid",
        "process_allowed_once",
        "process_forbidden",
        "static_result_only",
        "unknown",
    },
}
SEASON_CONTEXT_PRIMARY_OPTIONS = {"winter", "summer", "shoulder_season", "year_round", "unknown"}
SEASON_CONTEXT_WEATHER_OPTIONS = {"cold", "hot", "mild", "rainy", "sunny", "unknown"}
HAT_RISK_TIER_OPTIONS = {"low_risk", "medium_risk", "high_risk", "unknown"}
SET_RELATIONSHIP_OPTIONS = {"same_color", "matching_color", "mix_match", "unknown"}
CONTRACT_CONFIDENCE_FIELDS = (
    "product_subtype",
    "use_case",
    "placement_zone",
    "hold_scope",
    "orientation",
    "primary_visual_result",
    "operation_policy",
)
CONFIDENCE_OPTIONS = {"high", "medium", "low"}


def strip_markdown_fence(text: str) -> str:
    cleaned = text.strip()
    cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    return cleaned.strip()


def extract_json_candidate(text: str) -> str:
    cleaned = strip_markdown_fence(text)
    if not cleaned:
        raise JSONParseError("模型返回为空")

    if cleaned[0] in "{[":
        return cleaned

    object_match = re.search(r"(\{.*\})", cleaned, flags=re.DOTALL)
    if object_match:
        return object_match.group(1).strip()

    array_match = re.search(r"(\[.*\])", cleaned, flags=re.DOTALL)
    if array_match:
        return array_match.group(1).strip()

    first_object = cleaned.find("{")
    first_array = cleaned.find("[")
    candidates = [index for index in (first_object, first_array) if index >= 0]
    if candidates:
        return cleaned[min(candidates):].strip()

    raise JSONParseError(f"未找到 JSON 结构: {cleaned[:300]}")


def parse_json_text(text: str) -> Any:
    candidate = extract_json_candidate(text)
    try:
        return json.loads(candidate)
    except json.JSONDecodeError as exc:
        raise JSONParseError(f"JSON 解析失败: {exc}; 内容片段: {candidate[:500]}")


def _non_empty_string(value: Any) -> str:
    return value.strip() if isinstance(value, str) and value.strip() else ""


def _require_dict(value: Any, label: str) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise JSONParseError(f"{label} 不是对象")
    return value


def _require_list(value: Any, label: str) -> List[Any]:
    if not isinstance(value, list) or not value:
        raise JSONParseError(f"{label} 缺失或为空")
    return value


def _require_fields(obj: Dict[str, Any], fields: List[str], label: str) -> None:
    missing_fields = [field for field in fields if field not in obj]
    if missing_fields:
        raise JSONParseError(f"{label} 缺少字段: {', '.join(missing_fields)}")


def _ensure_string_field(obj: Dict[str, Any], field: str, label: str, allow_empty: bool = False) -> None:
    value = obj.get(field)
    if not isinstance(value, str):
        raise JSONParseError(f"{label}.{field} 必须存在且为字符串")
    if not allow_empty and not value.strip():
        raise JSONParseError(f"{label}.{field} 不能为空")


def _ensure_optional_string_field(obj: Dict[str, Any], field: str, label: str) -> None:
    if field not in obj:
        return
    value = obj.get(field)
    if not isinstance(value, str):
        raise JSONParseError(f"{label}.{field} 必须为字符串")


def _looks_like_non_chinese_descriptive_text(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    cjk_count = len(_CJK_RE.findall(text))
    latin_count = len(_LATINISH_RE.findall(text))
    if cjk_count > 0:
        return False
    return latin_count >= 6


def _target_language_allows_chinese(target_language: Optional[str]) -> bool:
    text = str(target_language or "").strip().lower()
    return any(token in text for token in ("中文", "汉语", "普通话", "粤语", "chinese", "mandarin", "cantonese", "zh"))


def _ensure_target_language_field(
    obj: Dict[str, Any],
    field: str,
    label: str,
    *,
    target_language: Optional[str] = None,
    allow_empty: bool = True,
) -> None:
    _ensure_string_field(obj, field, label, allow_empty=allow_empty)
    text = str(obj.get(field) or "").strip()
    if not text and allow_empty:
        return
    if not _target_language_allows_chinese(target_language) and _CJK_RE.search(text):
        raise JSONParseError(f"{label}.{field} 必须使用目标语言，不得包含中文；中文对照请放入 *_zh 字段或留空")


def _ensure_chinese_descriptive_field(obj: Dict[str, Any], field: str, label: str, allow_empty: bool = False) -> None:
    _ensure_string_field(obj, field, label, allow_empty=allow_empty)
    text = str(obj.get(field) or "").strip()
    if not text and allow_empty:
        return
    if _looks_like_non_chinese_descriptive_text(text):
        raise JSONParseError(f"{label}.{field} 除口播/字幕外必须使用中文描述")


def _prefer_localized_descriptive_text(
    *values: Any,
    default: str = "",
    allow_nonlocalized_fallback: bool = False,
) -> str:
    first_any = ""
    for value in values:
        text = _coerce_scalar_text(value)
        if text and not first_any:
            first_any = text
        if text and not _looks_like_non_chinese_descriptive_text(text):
            return text
    if allow_nonlocalized_fallback and first_any:
        return first_any
    if default.strip():
        return default.strip()
    return first_any


def _coerce_scalar_text(value: Any, preferred_keys: Optional[List[str]] = None) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        for item in value:
            text = _coerce_scalar_text(item, preferred_keys=preferred_keys)
            if text:
                return text
        return ""
    if isinstance(value, dict):
        keys = preferred_keys or []
        for key in keys:
            text = _coerce_scalar_text(value.get(key), preferred_keys=preferred_keys)
            if text:
                return text
        for item in value.values():
            text = _coerce_scalar_text(item, preferred_keys=preferred_keys)
            if text:
                return text
    return ""


def _coerce_string_list(value: Any) -> List[str]:
    if isinstance(value, list):
        items: List[str] = []
        for item in value:
            text = _coerce_scalar_text(item, preferred_keys=["text", "value", "label", "content"])
            if text:
                items.append(text)
        return items
    if isinstance(value, str):
        chunks = re.split(r"[\n；;。]+", value)
        return [item.strip(" -•\t\r") for item in chunks if item.strip(" -•\t\r")]
    if isinstance(value, dict):
        for key in ("items", "warnings", "anti_template_warnings", "list"):
            if key in value:
                items = _coerce_string_list(value.get(key))
                if items:
                    return items
    return []


def _normalize_persona_style_emotion_pack_payload(payload: Any) -> Dict[str, Any]:
    pack = _require_dict(payload, "人物穿搭情绪强化包")
    for wrapper_key in ("persona_style_emotion_pack", "persona_pack", "人物穿搭情绪强化包", "data", "result"):
        wrapped = pack.get(wrapper_key)
        if isinstance(wrapped, dict):
            pack = dict(wrapped)
            break

    normalized = dict(pack)
    aliases = {
        "persona_state": ["persona", "persona_type", "persona_style", "persona_state_suggestion", "state"],
        "appearance_anchor": ["appearance", "appearance_focus", "appearance_rule", "look_anchor"],
        "attractiveness_boundary": ["attractiveness_rule", "beauty_boundary", "beauty_rule"],
        "hairstyle_rule": ["hair_rule", "hairstyle", "hair_style_rule"],
        "makeup_rule": ["make_up_rule", "makeup", "makeup_style"],
        "clothing_rule": ["outfit_rule", "styling_rule", "clothing", "dress_rule"],
        "accessory_rule": ["accessories_rule", "accessory", "accessory_boundary"],
        "emotion_progression": ["emotion_curve", "emotion_flow", "emotion_rule", "emotion"],
        "movement_style": ["movement", "movement_rule", "action_style", "movement_direction"],
        "styling_completion_tag": ["styling_completion", "styling_completion_direction", "styling_tag", "outfit_completion_tag"],
        "persona_visual_tone": ["visual_tone", "persona_tone", "persona_visual_style", "visual_persona_tone"],
        "styling_key_anchor": ["key_anchor", "styling_anchor", "visual_anchor", "styling_focus_anchor"],
        "emotion_arc_tag": ["emotion_arc", "emotion_path", "emotion_arc_style", "emotion_track"],
    }
    defaults = {
        # P3 是辅助控制层，优先做轻归一化，避免单字段漂移直接阻断整条任务。
        "persona_state": "R1 轻分享型",
        "appearance_anchor": "真实顺眼、本地日常分享感，不抢商品主角",
        "attractiveness_boundary": "不网红化、不过度精修、不过度漂亮到喧宾夺主",
        "hairstyle_rule": "发型必须服务商品展示，保持头发与商品结构清楚可见",
        "makeup_rule": "淡妆或伪素颜，只提气色，不做强精致妆感",
        "clothing_rule": "低饱和、干净、完整的基础穿搭，服务商品，不抢镜",
        "accessory_rule": "不叠加抢眼配饰，避免头部与上半身区域竞争注意力",
        "emotion_progression": "开头轻疑问或轻困扰，中段轻确认或轻惊喜，结尾轻满意或轻推荐",
        "movement_style": "对镜整理、顺手展示、自然确认结果",
        "styling_completion_tag": "干净日常感",
        "persona_visual_tone": "克制顺眼型",
        "styling_key_anchor": "头部区域清爽",
        "emotion_arc_tag": "轻疑问 → 轻确认 → 轻安心",
    }

    for field, alias_list in aliases.items():
        text = _coerce_scalar_text(
            normalized.get(field),
            preferred_keys=[field, "text", "value", "label", "name", "content", "state", "persona", "type"],
        )
        if not text:
            for alias in alias_list:
                text = _coerce_scalar_text(
                    normalized.get(alias),
                    preferred_keys=[alias, "text", "value", "label", "name", "content", "state", "persona", "type"],
                )
                if text:
                    break
        normalized[field] = text or defaults[field]

    warnings = _coerce_string_list(normalized.get("anti_template_warnings"))
    if not warnings:
        for alias in ("warnings", "anti_template_warning", "template_warnings", "anti_template_notes"):
            warnings = _coerce_string_list(normalized.get(alias))
            if warnings:
                break
    if not warnings:
        warnings = [
            "不要网红脸模板",
            "不要全程同一种轻笑",
            "不要主播感或测评腔",
        ]
    normalized["anti_template_warnings"] = warnings[:6]
    normalized["human_performance_contract"] = _normalize_human_performance_contract(
        normalized.get("human_performance_contract")
    )
    return normalized


def _micro_reaction_limit_for_intensity(performance_intensity: str) -> int:
    return {
        "low": 2,
        "low_to_medium": 3,
        "medium": 4,
    }.get(str(performance_intensity or "").strip(), 0)


def _normalize_human_performance_contract(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict) or not value:
        return {}
    gaze_rule = value.get("gaze_rule") if isinstance(value.get("gaze_rule"), dict) else {}
    performance_intensity = _coerce_scalar_text(value.get("performance_intensity"))
    active_limit = value.get("active_micro_reaction_limit")
    if not isinstance(active_limit, int):
        active_limit = _micro_reaction_limit_for_intensity(performance_intensity)
    min_points = gaze_rule.get("min_points_required") if isinstance(gaze_rule, dict) else 3
    if not isinstance(min_points, int):
        try:
            min_points = int(str(min_points).strip())
        except Exception:
            min_points = 3
    return {
        "performance_family": _coerce_scalar_text(value.get("performance_family")),
        "persona_mode": _coerce_scalar_text(value.get("persona_mode")),
        "expression_arc": _coerce_string_list(value.get("expression_arc"))[:5],
        "gaze_plan": _coerce_string_list(value.get("gaze_plan"))[:6],
        "gaze_rule": {
            "min_points_required": min_points,
            "final_point_options": _coerce_string_list(gaze_rule.get("final_point_options"))[:4],
        },
        "micro_reaction_beats": _coerce_string_list(value.get("micro_reaction_beats"))[:6],
        "body_language_beats": _coerce_string_list(value.get("body_language_beats"))[:6],
        "product_interaction_beats": _coerce_string_list(value.get("product_interaction_beats"))[:6],
        "relatable_moment": _coerce_scalar_text(value.get("relatable_moment")),
        "performance_intensity": performance_intensity,
        "forbidden_performance": _coerce_string_list(value.get("forbidden_performance"))[:8],
        "active_micro_reaction_limit": active_limit,
        "scene_seed_brief": _normalize_scene_seed_brief(value.get("scene_seed_brief")),
    }


def _normalize_scene_seed_brief(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict) or not value:
        return {}
    boundary = value.get("micro_behavior_boundary") if isinstance(value.get("micro_behavior_boundary"), dict) else {}
    return {
        "enabled": bool(value.get("enabled")),
        "display_family": _coerce_scalar_text(value.get("display_family")),
        "seed_goal": _coerce_scalar_text(value.get("seed_goal")),
        "strategy_by_script_role": _normalize_scene_seed_strategy_by_role(
            value.get("strategy_by_script_role")
            or value.get("scene_seed_strategy_by_script_role")
            or value.get("scene_seed_strategy")
        ),
        "moment_hints": _coerce_string_list(value.get("moment_hints"))[:5],
        "small_tension_hints": _coerce_string_list(value.get("small_tension_hints"))[:5],
        "micro_behavior_boundary": {
            "safe_behavior_hints": _coerce_string_list(boundary.get("safe_behavior_hints"))[:5],
            "risk_boundary": _coerce_string_list(boundary.get("risk_boundary"))[:6],
        },
        "payoff_direction": _coerce_scalar_text(value.get("payoff_direction")),
        "anti_template_guidance": _coerce_string_list(value.get("anti_template_guidance"))[:4],
    }


def _normalize_scene_seed_strategy_by_role(value: Any) -> Dict[str, Dict[str, str]]:
    if not isinstance(value, dict):
        return {}
    allowed_keys = (
        "seed_mode",
        "moment_bias",
        "tension_bias",
        "camera_gaze_bias",
        "payoff_bias",
    )
    result: Dict[str, Dict[str, str]] = {}
    for role in ("S1", "S2", "S3", "S4"):
        raw_item = value.get(role) or value.get(role.lower())
        if not isinstance(raw_item, dict):
            continue
        item = {key: _coerce_scalar_text(raw_item.get(key)) for key in allowed_keys}
        if any(item.values()):
            result[role] = item
    return result


def _normalize_expression_plan_payload(payload: Any) -> Dict[str, Any]:
    plan = _require_dict(payload, "表达扩充计划")
    for wrapper_key in ("expression_plan", "表达扩充计划", "data", "result"):
        wrapped = plan.get(wrapper_key)
        if isinstance(wrapped, dict):
            plan = dict(wrapped)
            break

    normalized = dict(plan)
    aliases = {
        "exp_id": ["expression_id", "plan_id", "id"],
        "main_expression_pattern": ["main_pattern", "primary_expression_pattern", "main_expression"],
        "aux_expression_pattern": ["aux_pattern", "secondary_expression_pattern", "aux_expression"],
        "native_expression_entry": ["native_entry", "expression_entry"],
        "opening_expression_task": ["opening_task", "opening_expression"],
        "middle_expression_task": ["middle_task", "middle_expression"],
        "ending_expression_task": ["ending_task", "ending_expression"],
        "human_touch_focus_point": ["human_touch", "human_focus", "touch_focus_point"],
        "most_likely_empty_point": ["empty_point", "likely_empty_point"],
        "expression_weight_control": ["weight_control", "expression_balance", "expression_control"],
        "voiceover_intent": ["voiceover_task", "spoken_intent", "spoken_line_intent"],
        "voiceover_language_requirement": ["language_requirement", "voiceover_language_rule"],
    }
    defaults = {
        "exp_id": "EXP_AUTO",
        "main_expression_pattern": "轻判断 + 结果确认",
        "aux_expression_pattern": "轻补充说明 + 轻顾虑解除",
        "native_expression_entry": "像真实分享者顺手说出的第一反应",
        "opening_expression_task": "先把继续看下去的理由说清楚，不做平铺介绍",
        "middle_expression_task": "优先承担 proof，把结果或顾虑解除说实",
        "ending_expression_task": "用轻决策方式收住，不写硬催单",
        "human_touch_focus_point": "保留真实分享语气和顺手确认感",
        "most_likely_empty_point": "中段容易只剩氛围描述、缺少有效 proof",
        "expression_weight_control": "表达服务商品，不喧宾夺主，前中后轻推进",
        "voiceover_intent": "按开头、中段、结尾的表达任务生成目标语言口播，不预写完整台词",
        "voiceover_language_requirement": "P7 生成口播时必须使用目标语言，不得使用中文",
    }
    for field, alias_list in aliases.items():
        text = _coerce_scalar_text(
            normalized.get(field),
            preferred_keys=[field, "text", "value", "label", "name", "content", "summary"],
        )
        if not text:
            for alias in alias_list:
                text = _coerce_scalar_text(
                    normalized.get(alias),
                    preferred_keys=[alias, "text", "value", "label", "name", "content", "summary"],
                )
                if text:
                    break
        normalized[field] = text or defaults[field]
    return normalized


def _normalize_changed_feeling_layers(values: Any) -> List[str]:
    if isinstance(values, str):
        values = re.split(r"[，,、/|；;\s]+", values)
    if not isinstance(values, list):
        return []
    alias_map = {
        "person": "person",
        "persona": "person",
        "人物": "person",
        "人设": "person",
        "outfit": "outfit",
        "styling": "outfit",
        "style": "outfit",
        "穿搭": "outfit",
        "搭配": "outfit",
        "scene": "scene",
        "场景": "scene",
        "空间": "scene",
        "emotion": "emotion",
        "mood": "emotion",
        "feeling": "emotion",
        "情绪": "emotion",
    }
    normalized: List[str] = []
    for item in values:
        raw = str(item or "").strip()
        text = raw.lower()
        mapped = alias_map.get(text, alias_map.get(raw, raw))
        if mapped == raw:
            matched_any = False
            if any(token in raw for token in ("person", "persona", "人物", "人设")):
                normalized.append("person")
                matched_any = True
            if any(token in raw for token in ("outfit", "styling", "style", "穿搭", "搭配")):
                normalized.append("outfit")
                matched_any = True
            if any(token in raw for token in ("scene", "场景", "空间")):
                normalized.append("scene")
                matched_any = True
            if any(token in raw for token in ("emotion", "mood", "feeling", "情绪")):
                normalized.append("emotion")
                matched_any = True
            if matched_any:
                continue
            mapped = ""
        if mapped in {"person", "outfit", "scene", "emotion"}:
            normalized.append(mapped)
    deduped: List[str] = []
    for item in normalized:
        if item not in deduped:
            deduped.append(item)
    return deduped


def _normalize_consistency_checks(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    alias_map = {
        "persona_manifestation": "person_manifestation",
        "person_manifestation": "person_manifestation",
        "outfit_manifestation": "outfit_manifestation",
        "styling_manifestation": "outfit_manifestation",
        "scene_manifestation": "scene_manifestation",
        "emotion_manifestation": "emotion_manifestation",
        "mood_manifestation": "emotion_manifestation",
    }
    normalized: Dict[str, Any] = {}
    for key, item in value.items():
        mapped_key = alias_map.get(str(key or "").strip(), str(key or "").strip())
        normalized[mapped_key] = item
    return normalized


def _normalize_variant_internal_state(internal: Dict[str, Any], variant: Dict[str, Any]) -> Dict[str, Any]:
    focus = str(variant.get("variant_focus", "") or "").strip().lower()
    focus_to_layer = {
        "persona": "person",
        "outfit": "outfit",
        "scene": "scene",
        "emotion": "emotion",
    }
    normalized = dict(internal or {})
    normalized["changed_feeling_layers"] = _normalize_changed_feeling_layers(
        normalized.get("changed_feeling_layers") or ([focus_to_layer[focus]] if focus in focus_to_layer else [])
    )
    normalized["consistency_checks"] = _normalize_consistency_checks(normalized.get("consistency_checks"))
    normalized.setdefault("variant_name", str(variant.get("variant_id", "") or "").strip() or "variant")
    normalized.setdefault("main_adjustment", str(variant.get("variant_focus", "") or "").strip())
    normalized.setdefault("test_goal", "同方向变体测试")
    normalized.setdefault("variant_change_summary", "")
    normalized.setdefault("inherited_core_items", [])
    normalized.setdefault("changed_structure_fields", [])
    normalized.setdefault("main_change", "")
    normalized.setdefault("secondary_change", "")
    normalized.setdefault("difference_summary", "")
    normalized.setdefault("coverage", ["hook", "proof", "decision"])
    normalized.setdefault("proof_blueprint", [])
    normalized.setdefault("person_variant_layer", {})
    normalized.setdefault("outfit_variant_layer", {})
    normalized.setdefault("scene_variant_layer", {})
    normalized.setdefault("emotion_variant_layer", {})
    normalized["consistency_checks"].setdefault("person_manifestation", "")
    normalized["consistency_checks"].setdefault("outfit_manifestation", "")
    normalized["consistency_checks"].setdefault("scene_manifestation", "")
    normalized["consistency_checks"].setdefault("emotion_manifestation", "")
    return normalized


def _split_boundary_text(value: Any) -> List[str]:
    if isinstance(value, list):
        items = value
    elif isinstance(value, str):
        items = re.split(r"[\n\r]+|[；;]+", value)
    else:
        items = []
    normalized: List[str] = []
    for item in items:
        text = str(item or "").strip()
        text = re.sub(r"^[-*•]\s*", "", text)
        if text:
            normalized.append(text)
    return normalized


def _normalize_variant_final_prompt(final_prompt: Any) -> Dict[str, Any]:
    if isinstance(final_prompt, str):
        final_prompt = {
            "video_setup": {
                "video_theme": "",
                "product_focus": "",
                "person_final": "",
                "outfit_final": "",
                "scene_final": "",
                "emotion_final": "",
                "overall_style": final_prompt.strip(),
            },
            "shot_execution": [],
            "style_boundaries": [],
        }
    if not isinstance(final_prompt, dict):
        return {}

    normalized = dict(final_prompt)
    video_setup = normalized.get("video_setup")
    if isinstance(video_setup, str):
        video_setup = {
            "video_theme": "",
            "product_focus": "",
            "person_final": "",
            "outfit_final": "",
            "scene_final": "",
            "emotion_final": "",
            "overall_style": video_setup.strip(),
        }
    elif not isinstance(video_setup, dict):
        video_setup = {}

    alias_keys = {
        "video_theme": ["theme", "title"],
        "product_focus": ["product_present_focus", "product_anchor", "product_anchor_focus"],
        "person_final": ["persona_final", "person", "person_state"],
        "outfit_final": ["styling_final", "outfit", "styling"],
        "scene_final": ["scene", "scene_setup"],
        "emotion_final": ["emotion", "mood_final"],
        "overall_style": ["style", "style_boundary", "overall_tone"],
    }
    for key, aliases in alias_keys.items():
        if not isinstance(video_setup.get(key), str):
            video_setup[key] = ""
        if not str(video_setup.get(key) or "").strip():
            for alias in aliases:
                candidate = video_setup.get(alias)
                if isinstance(candidate, str) and candidate.strip():
                    video_setup[key] = candidate.strip()
                    break

    shot_execution_raw = normalized.get("shot_execution")
    if not isinstance(shot_execution_raw, list):
        shot_execution_raw = []

    first_visual = ""
    first_action = ""
    first_focus = ""
    first_voiceover = ""
    for shot in shot_execution_raw:
        if not isinstance(shot, dict):
            continue
        if not first_visual:
            first_visual = str(
                shot.get("visual")
                or shot.get("shot_content")
                or shot.get("scene")
                or shot.get("frame")
                or ""
            ).strip()
        if not first_action:
            first_action = str(shot.get("person_action") or shot.get("action") or "").strip()
        if not first_focus:
            first_focus = str(
                shot.get("product_focus")
                or shot.get("anchor_reference")
                or shot.get("product_anchor")
                or ""
            ).strip()
        if not first_voiceover:
            first_voiceover = str(
                shot.get("voiceover")
                or shot.get("voiceover_text_target_language")
                or shot.get("voiceover_text")
                or ""
            ).strip()
        if first_visual and first_action and first_focus and first_voiceover:
            break

    if not str(video_setup.get("video_theme") or "").strip():
        video_setup["video_theme"] = "短视频变体"
    if not str(video_setup.get("product_focus") or "").strip():
        video_setup["product_focus"] = first_focus or "商品保持画面主角"
    if not str(video_setup.get("person_final") or "").strip():
        video_setup["person_final"] = first_action or "人物自然出镜"
    if not str(video_setup.get("outfit_final") or "").strip():
        video_setup["outfit_final"] = "日常穿搭底盘，不抢商品主角"
    if not str(video_setup.get("scene_final") or "").strip():
        video_setup["scene_final"] = first_visual or "生活化真实场景"
    if not str(video_setup.get("emotion_final") or "").strip():
        video_setup["emotion_final"] = first_voiceover or "轻分享"
    if not str(video_setup.get("overall_style") or "").strip():
        video_setup["overall_style"] = "原生自然，商品必须是主角"
    video_setup["video_theme"] = _prefer_localized_descriptive_text(
        video_setup.get("video_theme"),
        default="短视频变体",
    )
    video_setup["product_focus"] = _prefer_localized_descriptive_text(
        video_setup.get("product_focus"),
        first_focus,
        default="商品保持画面主角",
    )
    video_setup["person_final"] = _prefer_localized_descriptive_text(
        video_setup.get("person_final"),
        first_action,
        default="人物自然出镜",
    )
    video_setup["outfit_final"] = _prefer_localized_descriptive_text(
        video_setup.get("outfit_final"),
        default="日常穿搭底盘，不抢商品主角",
    )
    video_setup["scene_final"] = _prefer_localized_descriptive_text(
        video_setup.get("scene_final"),
        first_visual,
        default="生活化真实场景",
    )
    video_setup["emotion_final"] = _prefer_localized_descriptive_text(
        video_setup.get("emotion_final"),
        default="轻分享",
    )
    video_setup["overall_style"] = _prefer_localized_descriptive_text(
        video_setup.get("overall_style"),
        default="原生自然，商品必须是主角",
    )
    normalized["video_setup"] = video_setup

    shots = normalized.get("shot_execution")
    if not isinstance(shots, list):
        shots = []
    normalized_shots: List[Dict[str, Any]] = []
    for index, shot in enumerate(shots, 1):
        if not isinstance(shot, dict):
            continue
        item = dict(shot)
        if not isinstance(item.get("shot_no"), int):
            item["shot_no"] = index
        alias_map = {
            "visual": ["shot_content", "scene", "frame"],
            "person_action": ["action", "person_move"],
            "product_focus": ["anchor_reference", "product_anchor", "anchor_focus"],
            "voiceover": ["voiceover_text_target_language", "voiceover_text", "voiceover_text_zh"],
        }
        for key, aliases in alias_map.items():
            value = item.get(key)
            if not isinstance(value, str) or not value.strip():
                item[key] = ""
                for alias in aliases:
                    candidate = item.get(alias)
                    if isinstance(candidate, str) and candidate.strip():
                        item[key] = candidate.strip()
                        break
        if not isinstance(item.get("duration"), str):
            item["duration"] = str(item.get("duration", "") or "")
        item["visual"] = _prefer_localized_descriptive_text(
            item.get("visual"),
            default="生活化画面推进",
        )
        item["person_action"] = _prefer_localized_descriptive_text(
            item.get("person_action"),
            default="人物自然完成动作",
        )
        item["product_focus"] = _prefer_localized_descriptive_text(
            item.get("product_focus"),
            default="商品关键锚点与变化结果",
        )
        normalized_shots.append(item)
    normalized["shot_execution"] = normalized_shots

    if "style_boundaries" not in normalized or not isinstance(normalized.get("style_boundaries"), list):
        normalized["style_boundaries"] = _split_boundary_text(normalized.get("style_boundaries"))
    if not normalized["style_boundaries"]:
        normalized["style_boundaries"] = _split_boundary_text(normalized.get("execution_boundary"))
    if not normalized["style_boundaries"]:
        normalized["style_boundaries"] = ["原生自然", "商品必须是主角"]
    normalized_boundaries: List[str] = []
    for item in normalized["style_boundaries"]:
        text = _coerce_scalar_text(item)
        if not text or _looks_like_non_chinese_descriptive_text(text):
            continue
        normalized_boundaries.append(text)
    normalized["style_boundaries"] = normalized_boundaries
    if not normalized["style_boundaries"]:
        normalized["style_boundaries"] = ["原生自然", "商品必须是主角"]
    return normalized


def _ensure_string_list_field(obj: Dict[str, Any], field: str, label: str, allow_empty_items: bool = False) -> None:
    value = obj.get(field)
    if not isinstance(value, list):
        raise JSONParseError(f"{label}.{field} 必须为数组")
    if not value:
        raise JSONParseError(f"{label}.{field} 不能为空")
    for index, item in enumerate(value, 1):
        if not isinstance(item, str):
            raise JSONParseError(f"{label}.{field}[{index}] 必须为字符串")
        if not allow_empty_items and not item.strip():
            raise JSONParseError(f"{label}.{field}[{index}] 不能为空")


def _ensure_list_field(obj: Dict[str, Any], field: str, label: str, allow_empty: bool = False) -> List[Any]:
    value = obj.get(field)
    if not isinstance(value, list):
        raise JSONParseError(f"{label}.{field} 必须为数组")
    if not allow_empty and not value:
        raise JSONParseError(f"{label}.{field} 不能为空")
    return value


def _validate_dict_list_items(
    values: List[Any],
    label: str,
    required_fields: List[str],
    optional_string_fields: Optional[List[str]] = None,
) -> None:
    for index, item_value in enumerate(values, 1):
        item = _require_dict(item_value, f"{label} 第 {index} 项")
        _require_fields(item, required_fields, f"{label} 第 {index} 项")
        for key in required_fields:
            _ensure_string_field(item, key, f"{label} 第 {index} 项", allow_empty=False)
        for key in optional_string_fields or []:
            _ensure_optional_string_field(item, key, f"{label} 第 {index} 项")


def _normalize_probability_value(value: Any, label: str) -> float:
    if isinstance(value, (int, float)):
        number = float(value)
    else:
        text = str(value or "").strip()
        if not text:
            raise JSONParseError(f"{label} 不能为空")
        if text.endswith("%"):
            text = text[:-1].strip()
            try:
                number = float(text) / 100.0
            except ValueError as exc:
                raise JSONParseError(f"{label} 必须为 0-1 之间的小数或百分比") from exc
        else:
            try:
                number = float(text)
            except ValueError as exc:
                raise JSONParseError(f"{label} 必须为数字") from exc
            if number > 1 and number <= 100:
                number = number / 100.0
    if number < 0 or number > 1:
        raise JSONParseError(f"{label} 必须在 0 到 1 之间")
    return round(number, 4)


def validate_product_type_guard_payload(payload: Any) -> None:
    guard = _require_dict(payload, "产品类型视觉守卫")
    _require_fields(
        guard,
        [
            "vision_family",
            "vision_slot",
            "vision_type",
            "vision_confidence",
            "visible_evidence",
            "risk_note",
        ],
        "产品类型视觉守卫",
    )
    allowed_families = {"apparel", "jewelry", "hair_accessory", "apparel_accessory", "accessory", "unknown"}
    allowed_slots = {
        "body",
        "upper_body",
        "lower_body",
        "full_body",
        "wrist",
        "neck",
        "ear",
        "finger",
        "hair",
        "neck_shoulder",
        "head_face",
        "upper_body_accessory",
        "unknown",
    }

    _ensure_string_field(guard, "vision_family", "产品类型视觉守卫")
    _ensure_string_field(guard, "vision_slot", "产品类型视觉守卫")
    _ensure_string_field(guard, "vision_type", "产品类型视觉守卫", allow_empty=True)
    _ensure_string_field(guard, "risk_note", "产品类型视觉守卫", allow_empty=True)

    if guard["vision_family"] not in allowed_families:
        raise JSONParseError(f"产品类型视觉守卫.vision_family 必须为 {', '.join(sorted(allowed_families))}")
    if guard["vision_slot"] not in allowed_slots:
        raise JSONParseError(f"产品类型视觉守卫.vision_slot 必须为 {', '.join(sorted(allowed_slots))}")

    guard["vision_confidence"] = _normalize_probability_value(
        guard.get("vision_confidence"),
        "产品类型视觉守卫.vision_confidence",
    )
    values = _ensure_list_field(guard, "visible_evidence", "产品类型视觉守卫", allow_empty=True)
    for index, item in enumerate(values, 1):
        if not isinstance(item, str):
            raise JSONParseError(f"产品类型视觉守卫.visible_evidence[{index}] 必须为字符串")


def _coerce_contract_template_list(value: Any) -> List[Any]:
    if not isinstance(value, list):
        return []
    normalized: List[Any] = []
    for item in value:
        if isinstance(item, dict):
            template = {
                "id": _coerce_scalar_text(item.get("id")),
                "desc": _coerce_scalar_text(
                    item.get("desc"),
                    preferred_keys=["desc", "description", "text", "value", "content"],
                )
                or _coerce_scalar_text(
                    item.get("description"),
                    preferred_keys=["desc", "description", "text", "value", "content"],
                ),
            }
            if template["id"] or template["desc"]:
                normalized.append(template)
            continue
        text = _coerce_scalar_text(item)
        if text:
            normalized.append(text)
    return normalized


def _normalize_category_execution_contract(card: Dict[str, Any]) -> Dict[str, Any]:
    raw_contract = card.get("category_execution_contract")
    if not isinstance(raw_contract, dict):
        raw_contract = {}

    normalized: Dict[str, Any] = {}
    for field_name, allowed_values in CATEGORY_CONTRACT_FIELD_OPTIONS.items():
        value = str(raw_contract.get(field_name) or "").strip()
        if not value:
            fallback_field = "hair_accessory_subtype" if field_name == "product_subtype" else field_name
            value = str(card.get(fallback_field) or "").strip()
        if value not in allowed_values:
            value = "unknown" if "unknown" in allowed_values else ""
        normalized[field_name] = value

    normalized["primary_visual_result"] = _coerce_scalar_text(
        raw_contract.get("primary_visual_result"),
        preferred_keys=["primary_visual_result", "text", "value", "content"],
    ) or _coerce_scalar_text(card.get("primary_result")) or "unknown"
    raw_confidence = raw_contract.get("field_confidence")
    if not isinstance(raw_confidence, dict):
        raw_confidence = {}
    normalized["field_confidence"] = {
        field: str(raw_confidence.get(field) or "").strip()
        if str(raw_confidence.get(field) or "").strip() in CONFIDENCE_OPTIONS
        else "low"
        for field in CONTRACT_CONFIDENCE_FIELDS
    }
    normalized["safe_shot_templates"] = _coerce_contract_template_list(raw_contract.get("safe_shot_templates"))[:4]
    normalized["forbidden_actions"] = _coerce_contract_template_list(raw_contract.get("forbidden_actions"))[:4]
    normalized["result_priority"] = _coerce_scalar_text(
        raw_contract.get("result_priority"),
        preferred_keys=["result_priority", "text", "value", "content"],
    )

    raw_audio = raw_contract.get("audio_policy")
    if not isinstance(raw_audio, dict):
        raw_audio = {}
    audio_policy = {
        "bgm_style": _coerce_scalar_text(raw_audio.get("bgm_style")),
        "bgm_energy": str(raw_audio.get("bgm_energy") or "").strip(),
        "voiceover_priority": str(raw_audio.get("voiceover_priority") or "").strip() or "high",
        "sfx_policy": _coerce_scalar_text(raw_audio.get("sfx_policy")),
        "allowed_sfx": _coerce_string_list(raw_audio.get("allowed_sfx"))[:4],
        "forbidden_sfx": _coerce_string_list(raw_audio.get("forbidden_sfx"))[:4],
        "sfx_timing_rules": _coerce_string_list(raw_audio.get("sfx_timing_rules"))[:4],
        "audio_negative_constraints": _coerce_string_list(raw_audio.get("audio_negative_constraints"))[:4],
    }
    if audio_policy["bgm_energy"] not in {"", "low", "medium"}:
        audio_policy["bgm_energy"] = "low"
    if audio_policy["voiceover_priority"] != "high":
        audio_policy["voiceover_priority"] = "high"
    normalized["audio_policy"] = audio_policy
    raw_season = raw_contract.get("season_context")
    if not isinstance(raw_season, dict):
        raw_season = {}
    primary_season = str(raw_season.get("primary_season") or "").strip()
    weather_signal = str(raw_season.get("weather_signal") or "").strip()
    normalized["season_context"] = {
        "primary_season": primary_season if primary_season in SEASON_CONTEXT_PRIMARY_OPTIONS else "unknown",
        "weather_signal": weather_signal if weather_signal in SEASON_CONTEXT_WEATHER_OPTIONS else "unknown",
    }
    hat_risk_tier = str(raw_contract.get("hat_risk_tier") or "").strip()
    set_relationship = str(raw_contract.get("set_relationship") or "").strip()
    normalized["hat_risk_tier"] = hat_risk_tier if hat_risk_tier in HAT_RISK_TIER_OPTIONS else "unknown"
    normalized["set_relationship"] = (
        set_relationship if set_relationship in SET_RELATIONSHIP_OPTIONS else "unknown"
    )
    raw_co_styling = raw_contract.get("co_styling_hint")
    if not isinstance(raw_co_styling, dict):
        raw_co_styling = {}
    normalized["co_styling_hint"] = {
        "pair_with": _coerce_string_list(raw_co_styling.get("pair_with"))[:4],
    }
    return normalized


def validate_anchor_card_payload(payload: Any) -> None:
    card = _require_dict(payload, "锚点卡")
    if "parameter_anchors" not in card or not isinstance(card.get("parameter_anchors"), list):
        card["parameter_anchors"] = []
    if "key_visual_constraints" not in card or not isinstance(card.get("key_visual_constraints"), list):
        card["key_visual_constraints"] = []
    card["category_execution_contract"] = _normalize_category_execution_contract(card)
    for field_name in HAIR_ACCESSORY_FIELD_OPTIONS:
        if field_name not in card:
            card[field_name] = ""
        card[field_name] = str(card.get(field_name) or "").strip()
        if card[field_name] not in HAIR_ACCESSORY_FIELD_OPTIONS[field_name]:
            card[field_name] = "unknown"
    card["hard_anchors"] = (card.get("hard_anchors") or [])[:3] if isinstance(card.get("hard_anchors"), list) else []
    card["display_anchors"] = (card.get("display_anchors") or [])[:3] if isinstance(card.get("display_anchors"), list) else []
    card["key_visual_constraints"] = card["key_visual_constraints"][:3]
    card["candidate_primary_selling_points"] = (
        (card.get("candidate_primary_selling_points") or [])[:3]
        if isinstance(card.get("candidate_primary_selling_points"), list)
        else []
    )
    card["persona_suggestions"] = (
        (card.get("persona_suggestions") or [])[:2]
        if isinstance(card.get("persona_suggestions"), list)
        else []
    )
    card["scene_suggestions"] = (
        (card.get("scene_suggestions") or [])[:2]
        if isinstance(card.get("scene_suggestions"), list)
        else []
    )
    card["camera_mandates"] = (
        (card.get("camera_mandates") or [])[:3]
        if isinstance(card.get("camera_mandates"), list)
        else []
    )
    card["parameter_anchors"] = (
        (card.get("parameter_anchors") or [])[:5]
        if isinstance(card.get("parameter_anchors"), list)
        else []
    )
    for anchor_field in (
        "structure_anchors",
        "operation_anchors",
        "fixation_result_anchors",
        "before_after_result_anchors",
        "scene_usage_anchors",
    ):
        card[anchor_field] = (
            (card.get(anchor_field) or [])[:3]
            if isinstance(card.get(anchor_field), list)
            else []
        )
    _require_fields(
        card,
        [
            "product_positioning_one_liner",
            "hard_anchors",
            "display_anchors",
            "key_visual_constraints",
            "hair_accessory_subtype",
            "placement_zone",
            "hold_scope",
            "orientation",
            "primary_result",
            "category_execution_contract",
            "distortion_alerts",
            "candidate_primary_selling_points",
            "persona_suggestions",
            "scene_suggestions",
            "camera_mandates",
            "parameter_anchors",
            "structure_anchors",
            "operation_anchors",
            "fixation_result_anchors",
            "before_after_result_anchors",
            "scene_usage_anchors",
        ],
        "锚点卡",
    )
    _ensure_string_field(card, "product_positioning_one_liner", "锚点卡")
    for field_name, allowed_values in HAIR_ACCESSORY_FIELD_OPTIONS.items():
        _ensure_string_field(card, field_name, "锚点卡", allow_empty=True)
        if card.get(field_name) not in allowed_values:
            raise JSONParseError(f"锚点卡.{field_name} 值不在允许范围内")
    contract = _require_dict(card.get("category_execution_contract"), "锚点卡.category_execution_contract")
    for field_name, allowed_values in CATEGORY_CONTRACT_FIELD_OPTIONS.items():
        _ensure_string_field(contract, field_name, "锚点卡.category_execution_contract", allow_empty=True)
        if contract.get(field_name) not in allowed_values:
            raise JSONParseError(f"锚点卡.category_execution_contract.{field_name} 值不在允许范围内")
    _ensure_string_field(contract, "primary_visual_result", "锚点卡.category_execution_contract", allow_empty=True)
    _ensure_string_field(contract, "result_priority", "锚点卡.category_execution_contract", allow_empty=True)
    field_confidence = _require_dict(
        contract.get("field_confidence"),
        "锚点卡.category_execution_contract.field_confidence",
    )
    for field in CONTRACT_CONFIDENCE_FIELDS:
        _ensure_string_field(field_confidence, field, "锚点卡.category_execution_contract.field_confidence")
        if field_confidence.get(field) not in CONFIDENCE_OPTIONS:
            raise JSONParseError(f"锚点卡.category_execution_contract.field_confidence.{field} 必须为 high|medium|low")
    for field in ("safe_shot_templates", "forbidden_actions"):
        values = _ensure_list_field(contract, field, "锚点卡.category_execution_contract", allow_empty=True)
        for index, item in enumerate(values, 1):
            if isinstance(item, str):
                continue
            if isinstance(item, dict):
                _ensure_string_field(item, "id", f"锚点卡.category_execution_contract.{field}[{index}]", allow_empty=True)
                _ensure_string_field(item, "desc", f"锚点卡.category_execution_contract.{field}[{index}]", allow_empty=True)
                if not str(item.get("id") or item.get("desc") or "").strip():
                    raise JSONParseError(f"锚点卡.category_execution_contract.{field}[{index}] 的 id/desc 不能同时为空")
                continue
            raise JSONParseError(f"锚点卡.category_execution_contract.{field}[{index}] 必须为字符串或包含 id/desc 的对象")
    audio_policy = _require_dict(contract.get("audio_policy"), "锚点卡.category_execution_contract.audio_policy")
    for field in ("bgm_style", "bgm_energy", "voiceover_priority", "sfx_policy"):
        _ensure_string_field(audio_policy, field, "锚点卡.category_execution_contract.audio_policy", allow_empty=True)
    if audio_policy.get("bgm_energy") not in {"", "low", "medium"}:
        raise JSONParseError("锚点卡.category_execution_contract.audio_policy.bgm_energy 必须为 low|medium")
    if audio_policy.get("voiceover_priority") not in {"", "high"}:
        raise JSONParseError("锚点卡.category_execution_contract.audio_policy.voiceover_priority 必须为 high")
    for field in ("allowed_sfx", "forbidden_sfx", "sfx_timing_rules", "audio_negative_constraints"):
        values = _ensure_list_field(audio_policy, field, "锚点卡.category_execution_contract.audio_policy", allow_empty=True)
        for index, item in enumerate(values, 1):
            if not isinstance(item, str):
                raise JSONParseError(f"锚点卡.category_execution_contract.audio_policy.{field}[{index}] 必须为字符串")
    season_context = _require_dict(contract.get("season_context"), "锚点卡.category_execution_contract.season_context")
    for field in ("primary_season", "weather_signal"):
        _ensure_string_field(season_context, field, "锚点卡.category_execution_contract.season_context", allow_empty=True)
    if season_context.get("primary_season") not in SEASON_CONTEXT_PRIMARY_OPTIONS:
        raise JSONParseError("锚点卡.category_execution_contract.season_context.primary_season 值不在允许范围内")
    if season_context.get("weather_signal") not in SEASON_CONTEXT_WEATHER_OPTIONS:
        raise JSONParseError("锚点卡.category_execution_contract.season_context.weather_signal 值不在允许范围内")
    _ensure_string_field(contract, "hat_risk_tier", "锚点卡.category_execution_contract", allow_empty=True)
    if contract.get("hat_risk_tier") not in HAT_RISK_TIER_OPTIONS:
        raise JSONParseError("锚点卡.category_execution_contract.hat_risk_tier 值不在允许范围内")
    _ensure_string_field(contract, "set_relationship", "锚点卡.category_execution_contract", allow_empty=True)
    if contract.get("set_relationship") not in SET_RELATIONSHIP_OPTIONS:
        raise JSONParseError("锚点卡.category_execution_contract.set_relationship 值不在允许范围内")
    co_styling_hint = _require_dict(
        contract.get("co_styling_hint"),
        "锚点卡.category_execution_contract.co_styling_hint",
    )
    pair_with_values = _ensure_list_field(
        co_styling_hint,
        "pair_with",
        "锚点卡.category_execution_contract.co_styling_hint",
        allow_empty=True,
    )
    for index, item in enumerate(pair_with_values, 1):
        if not isinstance(item, str):
            raise JSONParseError(
                f"锚点卡.category_execution_contract.co_styling_hint.pair_with[{index}] 必须为字符串"
            )
    _validate_dict_list_items(
        _ensure_list_field(card, "hard_anchors", "锚点卡"),
        "hard_anchors",
        ["anchor", "reason_not_changeable", "confidence"],
    )
    _validate_dict_list_items(
        _ensure_list_field(card, "display_anchors", "锚点卡"),
        "display_anchors",
        ["anchor", "why_must_show", "recommended_shot_type"],
    )
    _validate_dict_list_items(
        _ensure_list_field(card, "key_visual_constraints", "锚点卡", allow_empty=True),
        "key_visual_constraints",
        ["constraint", "confidence", "basis"],
    )
    for index, item in enumerate(card.get("key_visual_constraints") or [], 1):
        confidence = str((item or {}).get("confidence", "") or "").strip()
        if confidence not in {"high", "medium", "low"}:
            raise JSONParseError(f"key_visual_constraints 第 {index} 项.confidence 必须为 high|medium|low")
    _ensure_string_list_field(card, "distortion_alerts", "锚点卡", allow_empty_items=False)
    _validate_dict_list_items(
        _ensure_list_field(card, "candidate_primary_selling_points", "锚点卡"),
        "candidate_primary_selling_points",
        ["selling_point", "how_to_tell", "how_to_show", "risk_if_missed"],
    )
    _validate_dict_list_items(
        _ensure_list_field(card, "persona_suggestions", "锚点卡"),
        "persona_suggestions",
        ["persona", "why_fit"],
    )
    _validate_dict_list_items(
        _ensure_list_field(card, "scene_suggestions", "锚点卡"),
        "scene_suggestions",
        ["scene", "why_fit", "not_recommended_scene"],
    )
    _validate_dict_list_items(
        _ensure_list_field(card, "camera_mandates", "锚点卡"),
        "camera_mandates",
        ["stage", "must_do"],
    )
    _validate_dict_list_items(
        _ensure_list_field(card, "parameter_anchors", "锚点卡", allow_empty=True),
        "parameter_anchors",
        ["parameter_name", "parameter_value", "why_must_preserve", "execution_note", "confidence"],
    )
    for field in (
        "structure_anchors",
        "operation_anchors",
        "fixation_result_anchors",
        "before_after_result_anchors",
        "scene_usage_anchors",
    ):
        values = _ensure_list_field(card, field, "锚点卡", allow_empty=True)
        for index, item in enumerate(values, 1):
            if not isinstance(item, str):
                raise JSONParseError(f"锚点卡.{field}[{index}] 必须为字符串")


def validate_opening_strategy_payload(payload: Any) -> None:
    container = _require_dict(payload, "首镜策略")
    strategies = _ensure_list_field(container, "opening_strategies", "首镜策略")
    if len(strategies) != 5:
        raise JSONParseError(f"opening_strategies 数量必须为 5，当前为 {len(strategies)}")
    _validate_dict_list_items(
        strategies,
        "opening_strategies",
        [
            "strategy_name",
            "angle_bucket",
            "opening_mode_candidate",
            "visual_entry_mode_candidate",
            "first_frame_visual",
            "shot_size",
            "action_design",
            "first_product_focus",
            "native_expression_entry",
            "opening_first_line_type",
            "suggested_short_line",
            "style_note",
            "risk_note",
        ],
    )


def validate_persona_style_emotion_pack_payload(payload: Any) -> None:
    pack = _normalize_persona_style_emotion_pack_payload(payload)
    if isinstance(payload, dict):
        payload.clear()
        payload.update(pack)
    required_fields = [
        "persona_state",
        "appearance_anchor",
        "attractiveness_boundary",
        "hairstyle_rule",
        "makeup_rule",
        "clothing_rule",
        "accessory_rule",
        "emotion_progression",
        "movement_style",
        "styling_completion_tag",
        "persona_visual_tone",
        "styling_key_anchor",
        "emotion_arc_tag",
        "anti_template_warnings",
    ]
    _require_fields(pack, required_fields, "人物穿搭情绪强化包")
    for field in required_fields[:-1]:
        _ensure_string_field(pack, field, "人物穿搭情绪强化包")
    warnings = _ensure_list_field(pack, "anti_template_warnings", "人物穿搭情绪强化包")
    for index, item in enumerate(warnings, 1):
        if not isinstance(item, str) or not item.strip():
            raise JSONParseError(f"人物穿搭情绪强化包.anti_template_warnings[{index}] 不能为空")
    contract = pack.get("human_performance_contract")
    if contract in (None, ""):
        pack["human_performance_contract"] = {}
        return
    contract = _require_dict(contract, "人物穿搭情绪强化包.human_performance_contract")
    if not contract:
        return
    _require_fields(
        contract,
        [
            "performance_family",
            "persona_mode",
            "expression_arc",
            "gaze_plan",
            "gaze_rule",
            "micro_reaction_beats",
            "body_language_beats",
            "product_interaction_beats",
            "relatable_moment",
            "performance_intensity",
            "forbidden_performance",
            "active_micro_reaction_limit",
        ],
        "人物穿搭情绪强化包.human_performance_contract",
    )
    for field in ("performance_family", "persona_mode", "relatable_moment", "performance_intensity"):
        _ensure_string_field(contract, field, "人物穿搭情绪强化包.human_performance_contract", allow_empty=True)
    for field in (
        "expression_arc",
        "gaze_plan",
        "micro_reaction_beats",
        "body_language_beats",
        "product_interaction_beats",
        "forbidden_performance",
    ):
        values = _ensure_list_field(contract, field, "人物穿搭情绪强化包.human_performance_contract", allow_empty=True)
        for index, item in enumerate(values, 1):
            if not isinstance(item, str):
                raise JSONParseError(f"人物穿搭情绪强化包.human_performance_contract.{field}[{index}] 必须为字符串")
    gaze_rule = _require_dict(contract.get("gaze_rule"), "人物穿搭情绪强化包.human_performance_contract.gaze_rule")
    if not isinstance(gaze_rule.get("min_points_required"), int):
        raise JSONParseError("人物穿搭情绪强化包.human_performance_contract.gaze_rule.min_points_required 必须为整数")
    final_points = _ensure_list_field(
        gaze_rule,
        "final_point_options",
        "人物穿搭情绪强化包.human_performance_contract.gaze_rule",
        allow_empty=True,
    )
    for index, item in enumerate(final_points, 1):
        if not isinstance(item, str):
            raise JSONParseError(
                f"人物穿搭情绪强化包.human_performance_contract.gaze_rule.final_point_options[{index}] 必须为字符串"
            )
    if not isinstance(contract.get("active_micro_reaction_limit"), int):
        raise JSONParseError("人物穿搭情绪强化包.human_performance_contract.active_micro_reaction_limit 必须为整数")
    scene_seed_brief = contract.get("scene_seed_brief")
    if scene_seed_brief not in (None, ""):
        scene_seed_brief = _require_dict(
            scene_seed_brief,
            "人物穿搭情绪强化包.human_performance_contract.scene_seed_brief",
        )
        boundary = scene_seed_brief.get("micro_behavior_boundary")
        if boundary not in (None, ""):
            boundary = _require_dict(
                boundary,
                "人物穿搭情绪强化包.human_performance_contract.scene_seed_brief.micro_behavior_boundary",
            )
            for field in ("safe_behavior_hints", "risk_boundary"):
                values = _ensure_list_field(
                    boundary,
                    field,
                    "人物穿搭情绪强化包.human_performance_contract.scene_seed_brief.micro_behavior_boundary",
                    allow_empty=True,
                )
                for index, item in enumerate(values, 1):
                    if not isinstance(item, str):
                        raise JSONParseError(
                            "人物穿搭情绪强化包.human_performance_contract."
                            f"scene_seed_brief.micro_behavior_boundary.{field}[{index}] 必须为字符串"
                        )
        strategy_by_role = scene_seed_brief.get("strategy_by_script_role")
        if strategy_by_role not in (None, ""):
            strategy_by_role = _require_dict(
                strategy_by_role,
                "人物穿搭情绪强化包.human_performance_contract.scene_seed_brief.strategy_by_script_role",
            )
            for role, item in strategy_by_role.items():
                if str(role) not in {"S1", "S2", "S3", "S4"}:
                    raise JSONParseError(
                        "人物穿搭情绪强化包.human_performance_contract."
                        "scene_seed_brief.strategy_by_script_role 只允许 S1/S2/S3/S4"
                    )
                item = _require_dict(
                    item,
                    "人物穿搭情绪强化包.human_performance_contract."
                    f"scene_seed_brief.strategy_by_script_role.{role}",
                )
                for field in ("seed_mode", "moment_bias", "tension_bias", "camera_gaze_bias", "payoff_bias"):
                    if field in item and not isinstance(item.get(field), str):
                        raise JSONParseError(
                            "人物穿搭情绪强化包.human_performance_contract."
                            f"scene_seed_brief.strategy_by_script_role.{role}.{field} 必须为字符串"
                        )


def _validate_language_fields(
    obj: Dict[str, Any],
    label: str,
    subtitle_required: bool = False,
    target_language: Optional[str] = None,
) -> None:
    _require_fields(
        obj,
        [
            "subtitle_text_target_language",
            "subtitle_text_zh",
            "voiceover_text_target_language",
            "voiceover_text_zh",
        ],
        label,
    )

    subtitle_target = _non_empty_string(obj.get("subtitle_text_target_language"))
    subtitle_zh = _non_empty_string(obj.get("subtitle_text_zh"))
    voiceover_target = _non_empty_string(obj.get("voiceover_text_target_language"))

    _ensure_target_language_field(
        obj,
        "subtitle_text_target_language",
        label,
        target_language=target_language,
        allow_empty=not subtitle_required,
    )
    _ensure_string_field(obj, "subtitle_text_zh", label, allow_empty=not subtitle_required)
    _ensure_target_language_field(
        obj,
        "voiceover_text_target_language",
        label,
        target_language=target_language,
        allow_empty=True,
    )
    _ensure_string_field(obj, "voiceover_text_zh", label, allow_empty=True)

    if subtitle_required and not subtitle_target:
        raise JSONParseError(f"{label}.subtitle_text_target_language 不能为空")
    if subtitle_required and not subtitle_zh:
        raise JSONParseError(f"{label}.subtitle_text_zh 不能为空")


def _validate_storyboard_tasks(
    storyboard: List[Any],
    label: str,
    *,
    allow_first_visual_hook_without_voiceover: bool = False,
) -> None:
    total = len(storyboard)

    def infer_task(index: int) -> str:
        if total <= 1:
            return "proof+decision"
        if index == 1:
            return "hook"
        if allow_first_visual_hook_without_voiceover and index == 2:
            first_voiceover = _non_empty_string(_require_dict(storyboard[0], f"{label} 第 1 个镜头").get("voiceover_text_target_language"))
            if not first_voiceover:
                return "hook"
        if index == total:
            return "decision"
        return "proof"

    early_hook_with_voiceover = False
    for index, shot_value in enumerate(storyboard, 1):
        shot = _require_dict(shot_value, f"{label} 第 {index} 个镜头")
        explicit_task = "spoken_line_task" in shot and str(shot.get("spoken_line_task", "") or "").strip()
        if explicit_task:
            _ensure_string_field(shot, "spoken_line_task", f"{label} 第 {index} 个镜头")
            spoken_line_task = str(shot.get("spoken_line_task", "")).strip()
        else:
            spoken_line_task = infer_task(index)
            shot["spoken_line_task"] = spoken_line_task
        if spoken_line_task not in {"hook", "proof", "decision", "proof+decision", "none"}:
            raise JSONParseError(
                f"{label} 第 {index} 个镜头 spoken_line_task 必须为 hook|proof|decision|proof+decision|none"
            )

        voiceover = _non_empty_string(shot.get("voiceover_text_target_language"))
        if index <= 2 and spoken_line_task == "hook" and voiceover:
            early_hook_with_voiceover = True
        if (
            allow_first_visual_hook_without_voiceover
            and index == 1
            and spoken_line_task == "hook"
            and not voiceover
        ):
            continue
        if explicit_task and spoken_line_task != "none" and not voiceover:
            raise JSONParseError(
                f"{label} 第 {index} 个镜头 spoken_line_task={spoken_line_task} 时必须有口播内容"
            )
    if allow_first_visual_hook_without_voiceover and not early_hook_with_voiceover:
        raise JSONParseError(f"{label} S4 首镜可无口播，但前两镜内必须至少有一个带口播的 hook 镜头")


def _validate_spoken_structure_summary(summary: Dict[str, Any], label: str) -> None:
    coverage = summary.get("coverage")
    spoken_line_count = summary.get("spoken_line_count")

    if not isinstance(coverage, list) or not coverage:
        raise JSONParseError(f"{label}.coverage 必须为非空数组")
    if any(not isinstance(item, str) for item in coverage):
        raise JSONParseError(f"{label}.coverage 必须为字符串数组")

    normalized_coverage = {str(item).strip() for item in coverage if str(item).strip()}
    allowed_coverage = {"hook", "proof", "decision"}
    if not normalized_coverage.issubset(allowed_coverage):
        raise JSONParseError(f"{label}.coverage 只能包含 hook|proof|decision")
    if normalized_coverage != allowed_coverage:
        raise JSONParseError(f"{label}.coverage 必须完整覆盖 hook|proof|decision")

    if not isinstance(spoken_line_count, int):
        raise JSONParseError(f"{label}.spoken_line_count 必须为整数")
    if spoken_line_count < 2 or spoken_line_count > 4:
        raise JSONParseError(f"{label}.spoken_line_count 必须在 2 到 4 之间")


def validate_strategy_payload(payload: Any) -> None:
    container = _require_dict(payload, "策略输出")
    if "difference_check" in container:
        _ensure_string_field(container, "difference_check", "策略输出", allow_empty=True)
    strategies = _require_list(container.get("strategies"), "strategies")
    if len(strategies) != 4:
        raise JSONParseError(f"strategies 数量必须为 4，当前为 {len(strategies)}")

    light_control_defaults_by_strategy_id = {
        "S1": {
            "styling_completion_tag": "干净日常感",
            "persona_visual_tone": "轻分享型",
            "styling_key_anchor": "头部区域清爽",
            "emotion_arc_tag": "轻疑问 → 轻确认 → 轻安心",
        },
        "S2": {
            "styling_completion_tag": "柔和精致感",
            "persona_visual_tone": "克制顺眼型",
            "styling_key_anchor": "领口干净",
            "emotion_arc_tag": "平静观察 → 小惊喜 → 满意确认",
        },
        "S3": {
            "styling_completion_tag": "安静通勤感",
            "persona_visual_tone": "轻判断型",
            "styling_key_anchor": "肩线利落",
            "emotion_arc_tag": "轻顾虑 → 轻被说服 → 轻满意",
        },
        "S4": {
            "styling_completion_tag": "轻约会感",
            "persona_visual_tone": "小惊喜型",
            "styling_key_anchor": "配色低对比但不发灰",
            "emotion_arc_tag": "平静进入 → 轻结果成立 → 轻推荐",
        },
    }

    required_fields = [
        "strategy_id",
        "final_strategy_id",
        "strategy_name",
        "script_role",
        "primary_focus",
        "secondary_focus",
        "primary_selling_point",
        "dominant_user_question",
        "proof_thesis",
        "decision_thesis",
        "main_attention_mechanism",
        "opening_mode",
        "selected_opening_strategy_name",
        "opening_strategy",
        "opening_first_line_type",
        "opening_first_shot",
        "proof_mode",
        "selling_point_proof_method",
        "core_proof_method",
        "ending_mode",
        "purchase_bridge_method",
        "decision_style",
        "scene_suggestion",
        "scene_subspace",
        "scene_function",
        "visual_entry_mode",
        "rhythm_signature",
        "persona_state_suggestion",
        "persona_state",
        "persona_presence_role",
        "persona_polish_level",
        "action_entry_mode",
        "styling_completion_tag",
        "persona_visual_tone",
        "styling_key_anchor",
        "emotion_arc_tag",
        "styling_base_logic",
        "styling_base_constraints",
        "opening_emotion",
        "middle_emotion",
        "ending_emotion",
        "main_shooting_method",
        "aux_shooting_method",
        "voiceover_style",
        "product_dominance_rule",
        "realism_principles",
        "forbidden_patterns",
        "risk_note",
    ]

    for index, strategy_value in enumerate(strategies, 1):
        strategy = _require_dict(strategy_value, f"strategies 第 {index} 项")
        strategy_id_hint = str(strategy.get("strategy_id", "") or "").strip().upper()
        default_light_control = light_control_defaults_by_strategy_id.get(strategy_id_hint, {})
        for key, value in default_light_control.items():
            if not str(strategy.get(key, "") or "").strip():
                strategy[key] = value
        if not str(strategy.get("final_strategy_id", "") or "").strip() and strategy_id_hint in {"S1", "S2", "S3", "S4"}:
            strategy["final_strategy_id"] = f"Final_{strategy_id_hint}"
        if not str(strategy.get("strategy_name", "") or "").strip() and strategy_id_hint:
            strategy["strategy_name"] = f"{strategy_id_hint}策略"
        opening_angle = str(strategy.get("opening_angle", "") or "").strip()
        proof_path = str(strategy.get("proof_path", "") or "").strip()
        performance_hint = str(
            strategy.get("performance_strategy_hint")
            or strategy.get("performance_bias")
            or ""
        ).strip()
        risk_controls = strategy.get("risk_controls")
        if isinstance(risk_controls, str):
            risk_controls = [risk_controls]
        elif not isinstance(risk_controls, list):
            risk_controls = []
        risk_note = str(strategy.get("risk_note", "") or "").strip()
        primary_focus = str(strategy.get("primary_focus", "") or strategy.get("primary_selling_point", "") or "").strip()
        secondary_focus = str(strategy.get("secondary_focus", "") or "").strip()
        role_defaults = {
            "cognitive_reframing": {
                "ending_mode": "误判纠偏收尾",
                "decision_style": "不是小点缀而是主视觉的轻判断",
                "rhythm_signature": "先纠偏再确认",
            },
            "result_delivery": {
                "ending_mode": "结果感收尾",
                "decision_style": "结果成立后的轻确认",
                "rhythm_signature": "结果先给再复核",
            },
            "risk_resolution": {
                "ending_mode": "顾虑化解收尾",
                "decision_style": "风险解除后的安心判断",
                "rhythm_signature": "先提出顾虑再化解",
            },
            "aura_enhancement": {
                "ending_mode": "氛围完成度收尾",
                "decision_style": "整体感成立后的轻分享",
                "rhythm_signature": "惊艳结果到整体确认",
            },
        }.get(str(strategy.get("script_role", "") or "").strip(), {})
        id_defaults = {
            "S1": {
                "visual_entry_mode": "误判纠偏结果进入",
                "action_entry_mode": "镜前轻观察",
                "persona_state": "轻观察型真实用户",
            },
            "S2": {
                "visual_entry_mode": "结果变化进入",
                "action_entry_mode": "结果复核轻分享",
                "persona_state": "克制顺眼型真实用户",
            },
            "S3": {
                "visual_entry_mode": "顾虑场景进入",
                "action_entry_mode": "问题位置轻确认",
                "persona_state": "轻顾虑到安心型真实用户",
            },
            "S4": {
                "visual_entry_mode": "惊艳首镜进入",
                "action_entry_mode": "整体氛围轻确认",
                "persona_state": "轻惊喜型真实用户",
            },
        }.get(strategy_id_hint, {})
        if not str(strategy.get("primary_focus", "") or "").strip():
            strategy["primary_focus"] = primary_focus or "围绕主视觉结果做单点证明"
        if not str(strategy.get("primary_selling_point", "") or "").strip():
            strategy["primary_selling_point"] = primary_focus or strategy["primary_focus"]
        if not str(strategy.get("dominant_user_question", "") or "").strip():
            strategy["dominant_user_question"] = f"这个商品能不能让{strategy['primary_focus']}成立"
        if not str(strategy.get("proof_thesis", "") or "").strip():
            strategy["proof_thesis"] = f"用{proof_path or '结果镜头'}证明{strategy['primary_focus']}"
        if not str(strategy.get("decision_thesis", "") or "").strip():
            strategy["decision_thesis"] = f"确认{strategy['primary_focus']}后轻收尾"
        compact_defaults = {
            "main_attention_mechanism": opening_angle or strategy.get("proof_thesis", ""),
            "main_shooting_method": proof_path or "结果与细节组合",
            "aux_shooting_method": secondary_focus or "轻微动态复核",
            "selected_opening_strategy_name": opening_angle or "结果先给",
            "opening_mode": opening_angle or "结果先给",
            "opening_strategy": opening_angle or "先给结果再进入证明",
            "opening_first_line_type": "目标语言轻判断",
            "opening_first_shot": opening_angle or "已使用结果首镜",
            "proof_mode": proof_path or "A_result_detail_only",
            "selling_point_proof_method": strategy.get("proof_thesis", ""),
            "core_proof_method": proof_path or strategy.get("proof_thesis", ""),
            "ending_mode": "轻确认收尾",
            "purchase_bridge_method": strategy.get("decision_thesis", ""),
            "decision_style": "朋友式轻判断",
            "scene_suggestion": "家中镜前自然分享",
            "scene_subspace": "镜前",
            "scene_function": "出门前确认",
            "visual_entry_mode": opening_angle or "结果画面进入",
            "rhythm_signature": f"{strategy_id_hint or index}轻节奏",
            "persona_state_suggestion": performance_hint or "镜前自然确认",
            "persona_state": performance_hint or "真实用户轻分享",
            "persona_presence_role": "真实使用者",
            "persona_polish_level": "自然但干净",
            "action_entry_mode": performance_hint or "低风险轻互动",
            "styling_base_logic": strategy.get("contract_alignment_note") or "服从商品契约做结果证明",
            "opening_emotion": "轻观察",
            "middle_emotion": "轻确认",
            "ending_emotion": "轻满意",
            "voiceover_style": "目标语言朋友式轻分享",
            "product_dominance_rule": "商品 proof 优先，人物表演辅助",
            "risk_note": risk_note or "避免违反 category_execution_contract",
        }
        compact_defaults.update(id_defaults)
        compact_defaults.update(role_defaults)
        for key, value in compact_defaults.items():
            if not str(strategy.get(key, "") or "").strip():
                strategy[key] = value
        if not isinstance(strategy.get("styling_base_constraints"), list):
            strategy["styling_base_constraints"] = [
                str(strategy.get("contract_alignment_note") or "不改变商品使用契约").strip()
            ]
        if not isinstance(strategy.get("realism_principles"), list):
            strategy["realism_principles"] = [performance_hint or "人物反应真实克制"]
        if not isinstance(strategy.get("forbidden_patterns"), list):
            strategy["forbidden_patterns"] = [item for item in risk_controls if str(item or "").strip()] or [risk_note or "不生成高风险动作"]
        if strategy.get("secondary_focus") is None:
            strategy["secondary_focus"] = ""
        _require_fields(strategy, required_fields, f"strategies 第 {index} 项")
        for key in required_fields:
            if key in {"styling_base_constraints", "realism_principles", "forbidden_patterns"}:
                _ensure_string_list_field(strategy, key, f"strategies 第 {index} 项")
            else:
                _ensure_string_field(strategy, key, f"strategies 第 {index} 项", allow_empty=(key == "secondary_focus"))
        if str(strategy.get("strategy_id", "") or "").strip() not in {"S1", "S2", "S3", "S4"}:
            raise JSONParseError(f"strategies 第 {index} 项 strategy_id 必须为 S1/S2/S3/S4")
        if str(strategy.get("final_strategy_id", "") or "").strip() != f"Final_{strategy.get('strategy_id')}":
            raise JSONParseError(f"strategies 第 {index} 项 final_strategy_id 与 strategy_id 不一致")
        if str(strategy.get("script_role", "") or "").strip() not in set(SCRIPT_ROLES):
            raise JSONParseError(
                f"strategies 第 {index} 项 script_role 必须为 {'/'.join(SCRIPT_ROLES)}"
            )
        primary_focus = str(strategy.get("primary_focus", "") or "").strip()
        secondary_focus = str(strategy.get("secondary_focus", "") or "").strip()
        if secondary_focus and primary_focus == secondary_focus:
            raise JSONParseError(f"strategies 第 {index} 项 secondary_focus 不能与 primary_focus 相同")


def validate_expression_plan_payload(payload: Any) -> None:
    plan = _normalize_expression_plan_payload(payload)
    if isinstance(payload, dict):
        payload.clear()
        payload.update(plan)
    required_fields = [
        "exp_id",
        "main_expression_pattern",
        "aux_expression_pattern",
        "native_expression_entry",
        "opening_expression_task",
        "middle_expression_task",
        "ending_expression_task",
        "human_touch_focus_point",
        "most_likely_empty_point",
        "expression_weight_control",
        "voiceover_intent",
        "voiceover_language_requirement",
    ]
    _require_fields(plan, required_fields, "表达扩充计划")
    for key in required_fields:
        _ensure_string_field(plan, key, "表达扩充计划")


PROOF_PATH_OPTIONS = {
    "A_result_detail_only",
    "B_result_with_light_compare",
    "C_result_with_short_process",
    "D_result_with_light_compare_and_short_process",
}


def _normalize_performance_payload(value: Any, fallback: Any = "") -> Dict[str, str]:
    if isinstance(value, dict):
        gaze = _coerce_scalar_text(value.get("gaze"), preferred_keys=["gaze", "text", "value", "content"])
        expression = _coerce_scalar_text(
            value.get("expression_or_micro_reaction"),
            preferred_keys=["expression_or_micro_reaction", "micro_reaction", "expression", "text", "value", "content"],
        )
        body_language = _coerce_scalar_text(
            value.get("body_language"),
            preferred_keys=["body_language", "movement", "action", "text", "value", "content"],
        )
        product_interaction = _coerce_scalar_text(
            value.get("product_interaction"),
            preferred_keys=["product_interaction", "interaction", "text", "value", "content"],
        )
    else:
        text = _coerce_scalar_text(value)
        if not text:
            text = _prefer_localized_descriptive_text(
                fallback,
                default="人物通过眼神和轻微动作自然确认商品效果",
            )
        gaze = text if any(token in text for token in ("看", "视线", "眼神", "镜", "camera", "gaze")) else "人物看向商品和镜中结果"
        expression = text if any(token in text for token in ("笑", "抿嘴", "点头", "满意", "观察", "眼神")) else ""
        body_language = text if any(token in text for token in ("侧头", "靠近", "后退", "肩", "身体", "站姿")) else ""
        product_interaction = text if any(token in text for token in ("轻触", "整理", "拨", "夹", "发饰", "商品")) else ""

    if not gaze:
        gaze = "人物看向商品和镜中结果"
    return {
        "gaze": gaze,
        "expression_or_micro_reaction": expression,
        "body_language": body_language,
        "product_interaction": product_interaction,
    }


def _performance_to_text(value: Any) -> str:
    if isinstance(value, dict):
        return "；".join(
            str(value.get(key) or "").strip()
            for key in ("gaze", "expression_or_micro_reaction", "body_language", "product_interaction")
            if str(value.get(key) or "").strip()
        )
    return str(value or "").strip()


def _normalize_shot_skeleton_item(value: Any, index: int, default_proof_path: str) -> Dict[str, Any]:
    item = value if isinstance(value, dict) else {}
    shot_index = item.get("shot_index") or item.get("shot") or item.get("shot_no") or index
    if not isinstance(shot_index, int):
        shot_index_text = str(shot_index or "").strip()
        shot_index = int(shot_index_text) if shot_index_text.isdigit() else index
    proof_path = str(item.get("proof_path") or default_proof_path or "A_result_detail_only").strip()
    if proof_path not in PROOF_PATH_OPTIONS:
        proof_path = "A_result_detail_only"
    return {
        "shot_index": shot_index,
        "time_range": str(item.get("time_range") or item.get("duration") or "").strip(),
        "role": str(item.get("role") or item.get("task") or "").strip(),
        "shot_purpose": _prefer_localized_descriptive_text(
            item.get("shot_purpose"),
            item.get("purpose"),
            default="服务 hook / proof / decision 的分镜骨架",
        ),
        "proof_path": proof_path,
    }


def _normalize_scene_seed(value: Any) -> Dict[str, str]:
    if not isinstance(value, dict):
        value = {}
    return {
        "moment": _coerce_scalar_text(value.get("moment")),
        "small_tension": _coerce_scalar_text(value.get("small_tension")),
        "micro_behavior": _coerce_scalar_text(value.get("micro_behavior")),
        "payoff_feeling": _coerce_scalar_text(value.get("payoff_feeling")),
    }


def _normalize_script_payload(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}

    script = dict(payload)

    positioning = script.get("script_positioning")
    if not isinstance(positioning, dict):
        positioning = {}
    else:
        positioning = dict(positioning)

    opening_design = script.get("opening_design")
    if not isinstance(opening_design, dict):
        opening_design = {}
    else:
        opening_design = dict(opening_design)

    direction_hint = _prefer_localized_descriptive_text(
        positioning.get("direction_type"),
        opening_design.get("opening_mode"),
        default="内容方向",
    )
    positioning["direction_type"] = direction_hint
    positioning["script_title"] = _prefer_localized_descriptive_text(
        positioning.get("script_title"),
        direction_hint,
        default="15秒原创脚本",
    )
    positioning["core_primary_selling_point"] = _prefer_localized_descriptive_text(
        positioning.get("core_primary_selling_point"),
        opening_design.get("expression_entry"),
        default="商品关键卖点",
    )
    script["script_positioning"] = positioning
    proof_path = str(script.get("proof_path") or "").strip()
    if proof_path not in PROOF_PATH_OPTIONS:
        proof_path = "A_result_detail_only"
    script["proof_path"] = proof_path
    script["scene_seed"] = _normalize_scene_seed(script.get("scene_seed"))
    script["performance_strategy"] = _prefer_localized_descriptive_text(
        script.get("performance_strategy"),
        default="根据脚本角色分配眼神路径、微反应强度和轻分享位置",
    )

    storyboard = script.get("storyboard")
    if isinstance(storyboard, list):
        anchor_fallback = _prefer_localized_descriptive_text(
            positioning.get("core_primary_selling_point"),
            opening_design.get("first_frame"),
            default="商品关键锚点与变化结果",
        )
        normalized_storyboard: List[Any] = []
        for shot in storyboard:
            if not isinstance(shot, dict):
                normalized_storyboard.append(shot)
                continue
            item = dict(shot)
            anchor_reference = _coerce_scalar_text(item.get("anchor_reference"))
            if not anchor_reference or _looks_like_non_chinese_descriptive_text(anchor_reference):
                item["anchor_reference"] = anchor_fallback
            item["ai_shot_risk"] = str(item.get("ai_shot_risk", "") or "low").strip() or "low"
            item["replacement_template_id"] = str(item.get("replacement_template_id", "") or "").strip()
            raw_task_type = str(item.get("task_type", "") or "").strip().lower()
            compact_task_type = re.sub(r"\s+", "", raw_task_type)
            task_type_aliases = {
                "hook": "attention",
                "opening": "attention",
                "attention": "attention",
                "开头": "attention",
                "吸引": "attention",
                "proof": "proof",
                "middle": "proof",
                "demonstration": "proof",
                "证明": "proof",
                "中段": "proof",
                "decision": "bridge",
                "ending": "bridge",
                "bridge": "bridge",
                "收束": "bridge",
                "结尾": "bridge",
            }
            if compact_task_type in task_type_aliases:
                item["task_type"] = task_type_aliases[compact_task_type]
            spoken_line_task = str(item.get("spoken_line_task", "") or "").strip()
            voiceover_text = _coerce_scalar_text(
                item.get("voiceover_text_target_language")
            ) or _coerce_scalar_text(item.get("voiceover"))
            if spoken_line_task in {"hook", "proof", "decision", "proof+decision"} and not voiceover_text:
                item["spoken_line_task"] = "none"
            item["performance"] = _normalize_performance_payload(
                item.get("performance"),
                fallback=item.get("person_action") or item.get("shot_content"),
            )
            normalized_storyboard.append(item)
        script["storyboard"] = normalized_storyboard
        raw_skeleton = script.get("shot_skeleton")
        if not isinstance(raw_skeleton, list) or not raw_skeleton:
            raw_skeleton = [
                {
                    "shot_index": item.get("shot_no"),
                    "time_range": item.get("duration"),
                    "role": item.get("spoken_line_task"),
                    "shot_purpose": item.get("shot_purpose"),
                    "proof_path": proof_path,
                }
                for item in normalized_storyboard
                if isinstance(item, dict)
            ]
        script["shot_skeleton"] = [
            _normalize_shot_skeleton_item(item, index, proof_path)
            for index, item in enumerate(raw_skeleton, 1)
        ][:6]

    full_15s_flow = script.get("full_15s_flow")
    if not isinstance(full_15s_flow, list) or len(full_15s_flow) < 3:
        storyboard_for_flow = script.get("storyboard") if isinstance(script.get("storyboard"), list) else []
        first_range = str((storyboard_for_flow[0] or {}).get("duration", "0-3s") if storyboard_for_flow else "0-3s")
        last_range = str((storyboard_for_flow[-1] or {}).get("duration", "12-15s") if storyboard_for_flow else "12-15s")
        full_15s_flow = [
            {"stage": "opening", "time_range": first_range, "task": "hook", "summary": "首镜给出商品和停留理由"},
            {"stage": "middle", "time_range": "3-12s", "task": "proof", "summary": "围绕主证明点完成细节和结果复核"},
            {"stage": "ending", "time_range": last_range, "task": "decision", "summary": "轻决策收束并保留自然分享感"},
        ]
    script["full_15s_flow"] = full_15s_flow

    execution_constraints = script.get("execution_constraints")
    if not isinstance(execution_constraints, dict):
        execution_constraints = {}
    else:
        execution_constraints = dict(execution_constraints)
    default_constraint = _prefer_localized_descriptive_text(
        positioning.get("core_primary_selling_point"),
        opening_design.get("first_frame"),
        default="商品和使用结果必须清楚",
    )
    execution_constraints.setdefault("visual_style", "真实自然光短视频，画面干净，商品关系清楚")
    execution_constraints.setdefault("person_constraints", "人物状态自然克制，表演服务商品结果")
    execution_constraints.setdefault("styling_constraints", "穿搭简洁，不抢商品，辅助商品视觉完成度")
    execution_constraints.setdefault("tone_completion_constraints", "从观察到确认的轻分享语气，不硬广")
    execution_constraints.setdefault("scene_constraints", "家中或镜前真实使用场景，避免棚拍广告感")
    execution_constraints.setdefault("emotion_progression_constraints", "观察、确认、轻满意的自然推进")
    execution_constraints.setdefault("camera_focus", default_constraint)
    execution_constraints.setdefault("product_priority_principle", "商品 proof 主线优先于人物表演和氛围")
    execution_constraints.setdefault("realism_principle", "不展示复杂高风险过程，优先结果和细节复核")
    script["execution_constraints"] = execution_constraints

    audio_layer = script.get("audio_layer")
    if not isinstance(audio_layer, dict):
        audio_layer = {}
    else:
        audio_layer = dict(audio_layer)
    audio_layer.setdefault("bgm_style", "低存在感日常背景音乐")
    audio_layer.setdefault("bgm_energy", "low")
    audio_layer.setdefault("sfx_cues", [])
    audio_layer.setdefault("voiceover_priority", "high")
    audio_layer.setdefault("mix_note", "SFX 不盖过口播，画面动作弱时音效克制")
    audio_layer.setdefault(
        "audio_negative_constraints",
        ["不要盖住口播", "不要夸张闪光音", "不要廉价 bling 效果"],
    )
    script["audio_layer"] = audio_layer

    rhythm_checkpoints = script.get("rhythm_checkpoints")
    if not isinstance(rhythm_checkpoints, dict):
        rhythm_checkpoints = {}
    else:
        rhythm_checkpoints = dict(rhythm_checkpoints)
    rhythm_checkpoints.setdefault("hook_complete_by", "3s")
    rhythm_checkpoints.setdefault("core_proof_start_between", "4-8s")
    rhythm_checkpoints.setdefault("decision_signal_by", "12s")
    rhythm_checkpoints.setdefault("risk_resolution_decision_by", "9s_or_not_applicable")
    risk_decision_value = str(rhythm_checkpoints.get("risk_resolution_decision_by", "") or "").strip().lower()
    if risk_decision_value in {"", "not_applicable", "not applicable", "n/a", "na", "none", "不适用"}:
        rhythm_checkpoints["risk_resolution_decision_by"] = "9s_or_not_applicable"
    elif risk_decision_value in {"9", "9s", "9秒", "before_9s", "by_9s"}:
        rhythm_checkpoints["risk_resolution_decision_by"] = "9s"
    script["rhythm_checkpoints"] = rhythm_checkpoints

    negative_constraints = script.get("negative_constraints")
    if not isinstance(negative_constraints, list):
        negative_constraints = []
    if not negative_constraints:
        negative_constraints = [
            "不要让人物表演压过商品",
            "不要生成复杂佩戴或高风险操作过程",
            "不要使用与画面动作不匹配的音效",
        ]
    script["negative_constraints"] = negative_constraints

    return script


def _validate_script_positioning(positioning: Any, label: str) -> None:
    obj = _require_dict(positioning, label)
    _require_fields(obj, ["script_title", "direction_type", "core_primary_selling_point"], label)
    for key in ("script_title", "direction_type", "core_primary_selling_point"):
        _ensure_chinese_descriptive_field(obj, key, label)


def _validate_audio_layer(audio_layer: Any) -> None:
    layer = _require_dict(audio_layer, "audio_layer")
    _require_fields(
        layer,
        ["bgm_style", "bgm_energy", "sfx_cues", "voiceover_priority", "mix_note", "audio_negative_constraints"],
        "audio_layer",
    )
    _ensure_chinese_descriptive_field(layer, "bgm_style", "audio_layer", allow_empty=True)
    _ensure_string_field(layer, "bgm_energy", "audio_layer")
    if layer.get("bgm_energy") not in {"low", "medium"}:
        raise JSONParseError("audio_layer.bgm_energy 必须为 low|medium")
    _ensure_string_field(layer, "voiceover_priority", "audio_layer")
    if layer.get("voiceover_priority") != "high":
        raise JSONParseError("audio_layer.voiceover_priority 必须为 high")
    _ensure_string_field(layer, "mix_note", "audio_layer", allow_empty=True)

    sfx_cues = _ensure_list_field(layer, "sfx_cues", "audio_layer", allow_empty=True)
    if len(sfx_cues) > 3:
        raise JSONParseError("audio_layer.sfx_cues 最多 3 个")
    for index, cue_value in enumerate(sfx_cues, 1):
        cue = _require_dict(cue_value, f"audio_layer.sfx_cues 第 {index} 项")
        _require_fields(cue, ["time_range", "sfx_type", "purpose", "volume_note"], f"audio_layer.sfx_cues 第 {index} 项")
        for key in ("time_range", "sfx_type", "purpose", "volume_note"):
            _ensure_string_field(cue, key, f"audio_layer.sfx_cues 第 {index} 项", allow_empty=True)

    negative_constraints = _ensure_list_field(layer, "audio_negative_constraints", "audio_layer", allow_empty=True)
    for index, item in enumerate(negative_constraints, 1):
        if not isinstance(item, str):
            raise JSONParseError(f"audio_layer.audio_negative_constraints[{index}] 必须为字符串")


def _normalize_rhythm_checkpoint_value(key: str, value: Any) -> Any:
    """Normalize common LLM wording variants before strict schema validation."""
    text = str(value or "").strip().lower()
    compact = re.sub(r"\s+", "", text)
    compact = compact.replace("秒", "s")
    compact = compact.replace("之前", "前").replace("以内", "内")

    if key == "hook_complete_by" and (
        compact in {"3", "3s", "3s前", "3s内", "by3s", "before3s"}
        or compact in {"0-3s", "0~3s", "0到3s", "0至3s", "0–3s", "前0-3s"}
        or re.search(r"0(?:-|~|到|至|–)3s", compact)
        or re.search(r"(?:前|by|before)?3s(?:前|内|以内|完成)?", compact)
    ):
        return "3s"
    if key == "core_proof_start_between" and (
        compact in {
            "4-8s",
            "4~8s",
            "4–8s",
            "4到8s",
            "4至8s",
            "between4-8s",
        }
        or ("4" in compact and "8" in compact)
    ):
        return "4-8s"
    if key == "decision_signal_by" and (
        compact in {"12", "12s", "12s前", "12s内", "by12s", "before12s"}
        or re.search(r"(?:前|by|before)?12s(?:前|内|以内|完成)?", compact)
    ):
        return "12s"
    if key == "risk_resolution_decision_by":
        if compact in {"", "none", "na", "n/a", "not_applicable", "notapplicable", "不适用"}:
            return "9s_or_not_applicable"
        if compact in {"9", "9s", "9s前", "9s内", "by9s", "before9s"} or re.search(
            r"(?:前|by|before)?9s(?:前|内|以内|完成)?",
            compact,
        ):
            return "9s"
        if compact in {"9s_or_not_applicable", "9sornotapplicable", "9s或不适用"}:
            return "9s_or_not_applicable"
    return value


def _validate_rhythm_checkpoints(rhythm_checkpoints: Any) -> None:
    checkpoints = _require_dict(rhythm_checkpoints, "rhythm_checkpoints")
    _require_fields(
        checkpoints,
        ["hook_complete_by", "core_proof_start_between", "decision_signal_by", "risk_resolution_decision_by"],
        "rhythm_checkpoints",
    )
    for key in ("hook_complete_by", "core_proof_start_between", "decision_signal_by", "risk_resolution_decision_by"):
        checkpoints[key] = _normalize_rhythm_checkpoint_value(key, checkpoints.get(key))
        _ensure_string_field(checkpoints, key, "rhythm_checkpoints")
    if checkpoints.get("hook_complete_by") != "3s":
        raise JSONParseError("rhythm_checkpoints.hook_complete_by 必须为 3s")
    if checkpoints.get("core_proof_start_between") != "4-8s":
        raise JSONParseError("rhythm_checkpoints.core_proof_start_between 必须为 4-8s")
    if checkpoints.get("decision_signal_by") != "12s":
        raise JSONParseError("rhythm_checkpoints.decision_signal_by 必须为 12s")
    if checkpoints.get("risk_resolution_decision_by") not in {"9s_or_not_applicable", "9s"}:
        raise JSONParseError("rhythm_checkpoints.risk_resolution_decision_by 必须为 9s_or_not_applicable 或 9s")


def validate_script_schema_v2(script_json: Any, target_language: Optional[str] = None) -> None:
    script = _normalize_script_payload(script_json)
    if isinstance(script_json, dict):
        script_json.clear()
        script_json.update(script)
    script = _require_dict(script, "脚本输出")
    _require_fields(
        script,
        [
            "proof_path",
            "performance_strategy",
            "shot_skeleton",
            "script_positioning",
            "opening_design",
            "full_15s_flow",
            "storyboard",
            "execution_constraints",
            "rhythm_checkpoints",
            "audio_layer",
            "negative_constraints",
        ],
        "脚本输出",
    )
    if script.get("proof_path") not in PROOF_PATH_OPTIONS:
        raise JSONParseError("proof_path 必须为 A_result_detail_only|B_result_with_light_compare|C_result_with_short_process|D_result_with_light_compare_and_short_process")
    _ensure_chinese_descriptive_field(script, "performance_strategy", "脚本输出", allow_empty=True)
    shot_skeleton = _ensure_list_field(script, "shot_skeleton", "脚本输出", allow_empty=True)
    if shot_skeleton and len(shot_skeleton) != len(script.get("storyboard") or []):
        raise JSONParseError("shot_skeleton 数量必须与 storyboard 一致")
    for index, skeleton_value in enumerate(shot_skeleton, 1):
        skeleton = _require_dict(skeleton_value, f"shot_skeleton 第 {index} 项")
        _require_fields(skeleton, ["shot_index", "time_range", "role", "shot_purpose", "proof_path"], f"shot_skeleton 第 {index} 项")
        if not isinstance(skeleton.get("shot_index"), int):
            raise JSONParseError(f"shot_skeleton 第 {index} 项 shot_index 必须为整数")
        for key in ("time_range", "role", "shot_purpose", "proof_path"):
            if key in {"time_range", "role", "proof_path"}:
                _ensure_string_field(skeleton, key, f"shot_skeleton 第 {index} 项")
                if key == "proof_path" and skeleton.get(key) not in PROOF_PATH_OPTIONS:
                    raise JSONParseError(f"shot_skeleton 第 {index} 项 proof_path 不在允许范围内")
            else:
                _ensure_chinese_descriptive_field(skeleton, key, f"shot_skeleton 第 {index} 项")

    _validate_script_positioning(script.get("script_positioning"), "script_positioning")
    _validate_rhythm_checkpoints(script.get("rhythm_checkpoints"))

    opening_design = _require_dict(script.get("opening_design"), "opening_design")
    _require_fields(
        opening_design,
        ["opening_mode", "first_frame", "expression_entry", "first_line_type"],
        "opening_design",
    )
    for key in ("opening_mode", "first_frame", "expression_entry", "first_line_type"):
        _ensure_string_field(opening_design, key, "opening_design", allow_empty=True)

    full_15s_flow = _ensure_list_field(script, "full_15s_flow", "脚本输出")
    if len(full_15s_flow) < 3:
        raise JSONParseError("full_15s_flow 至少需要 3 段")
    for index, item_value in enumerate(full_15s_flow, 1):
        item = _require_dict(item_value, f"full_15s_flow 第 {index} 项")
        _require_fields(item, ["stage", "time_range", "task", "summary"], f"full_15s_flow 第 {index} 项")
        for key in ("stage", "time_range", "task", "summary"):
            _ensure_string_field(item, key, f"full_15s_flow 第 {index} 项")

    storyboard = _require_list(script.get("storyboard"), "storyboard")
    if len(storyboard) < 4 or len(storyboard) > 6:
        raise JSONParseError("storyboard 数量必须在 4 到 6 之间")
    execution_constraints = script.get("execution_constraints")
    if not isinstance(execution_constraints, dict):
        execution_constraints = {}
        script["execution_constraints"] = execution_constraints
    for key in (
        "visual_style",
        "person_constraints",
        "styling_constraints",
        "tone_completion_constraints",
        "scene_constraints",
        "emotion_progression_constraints",
        "camera_focus",
        "product_priority_principle",
        "realism_principle",
    ):
        if key not in execution_constraints or not isinstance(execution_constraints.get(key), str):
            execution_constraints[key] = str(execution_constraints.get(key, "") or "")
        _ensure_chinese_descriptive_field(execution_constraints, key, "execution_constraints", allow_empty=True)

    required_shot_fields = [
        "shot_no",
        "duration",
        "shot_content",
        "shot_purpose",
        "subtitle_text_target_language",
        "subtitle_text_zh",
        "voiceover_text_target_language",
        "voiceover_text_zh",
        "spoken_line_task",
        "person_action",
        "performance",
        "style_note",
        "anchor_reference",
        "task_type",
        "ai_shot_risk",
        "replacement_template_id",
    ]

    for index, shot_value in enumerate(storyboard, 1):
        shot = _require_dict(shot_value, f"storyboard 第 {index} 个镜头")
        _require_fields(shot, required_shot_fields, f"storyboard 第 {index} 个镜头")
        for key in (
            "duration",
            "shot_content",
            "shot_purpose",
            "person_action",
            "style_note",
            "anchor_reference",
            "task_type",
            "ai_shot_risk",
            "replacement_template_id",
        ):
            if key == "replacement_template_id":
                _ensure_string_field(shot, key, f"storyboard 第 {index} 个镜头", allow_empty=True)
            elif key in {"duration", "task_type", "ai_shot_risk"}:
                _ensure_string_field(shot, key, f"storyboard 第 {index} 个镜头")
            else:
                _ensure_chinese_descriptive_field(shot, key, f"storyboard 第 {index} 个镜头")
        performance = _require_dict(shot.get("performance"), f"storyboard 第 {index} 个镜头.performance")
        _require_fields(
            performance,
            ["gaze", "expression_or_micro_reaction", "body_language", "product_interaction"],
            f"storyboard 第 {index} 个镜头.performance",
        )
        _ensure_string_field(performance, "gaze", f"storyboard 第 {index} 个镜头.performance")
        if not str(performance.get("gaze") or "").strip():
            raise JSONParseError(f"storyboard 第 {index} 个镜头.performance.gaze 不能为空")
        for perf_key in ("expression_or_micro_reaction", "body_language", "product_interaction"):
            _ensure_chinese_descriptive_field(
                performance,
                perf_key,
                f"storyboard 第 {index} 个镜头.performance",
                allow_empty=True,
            )
        _validate_language_fields(
            shot,
            f"storyboard 第 {index} 个镜头",
            subtitle_required=False,
            target_language=target_language,
        )
        _ensure_string_field(shot, "spoken_line_task", f"storyboard 第 {index} 个镜头")
        for optional_key in ("person_state", "styling_base_role", "scene_function", "current_emotion"):
            _ensure_optional_string_field(shot, optional_key, f"storyboard 第 {index} 个镜头")

        if shot.get("task_type") not in {"attention", "proof", "bridge"}:
            raise JSONParseError(
                f"storyboard 第 {index} 个镜头 task_type 必须为 attention|proof|bridge"
            )
        if shot.get("ai_shot_risk") not in {"low", "medium", "high", "forbidden"}:
            raise JSONParseError(
                f"storyboard 第 {index} 个镜头 ai_shot_risk 必须为 low|medium|high|forbidden"
            )

    positioning = _require_dict(script.get("script_positioning"), "script_positioning")
    direction_type = str(positioning.get("direction_type", "") or "").strip()
    _validate_storyboard_tasks(
        storyboard,
        "storyboard",
        allow_first_visual_hook_without_voiceover=("高惊艳首镜" in direction_type),
    )
    negative_constraints = _ensure_list_field(script, "negative_constraints", "脚本输出", allow_empty=True)
    for index, item in enumerate(negative_constraints, 1):
        if not isinstance(item, str):
            raise JSONParseError(f"negative_constraints[{index}] 必须为字符串")
    _validate_audio_layer(script.get("audio_layer"))


def validate_review_payload(payload: Any, target_language: Optional[str] = None) -> None:
    review = _require_dict(payload, "质检输出")
    _require_fields(review, ["pass", "major_issues", "minor_issues", "repair_actions", "repaired_script"], "质检输出")
    if not isinstance(review.get("pass"), bool):
        raise JSONParseError("质检输出.pass 必须为布尔值")
    for field in ("major_issues", "minor_issues", "repair_actions"):
        values = _ensure_list_field(review, field, "质检输出", allow_empty=True)
        for index, item in enumerate(values, 1):
            if not isinstance(item, str):
                raise JSONParseError(f"质检输出.{field}[{index}] 必须为字符串")
    if "human_stiffness_check" in review:
        check = _require_dict(review.get("human_stiffness_check"), "质检输出.human_stiffness_check")
        for field in (
            "timing_consistency_check",
            "timeline_consistency_check",
            "ai_shot_risk_check",
            "emotion_flatness_check",
            "gaze_monotony_check",
            "category_interaction_missing_check",
        ):
            if not isinstance(check.get(field), bool):
                raise JSONParseError(f"质检输出.human_stiffness_check.{field} 必须为布尔值")
        if not isinstance(check.get("hit_count"), int):
            raise JSONParseError("质检输出.human_stiffness_check.hit_count 必须为整数")
        _ensure_string_field(check, "summary", "质检输出.human_stiffness_check", allow_empty=True)
    validate_script_payload(review.get("repaired_script"), target_language=target_language)


def _normalize_video_prompt_payload(payload: Any) -> Dict[str, Any]:
    prompt = _require_dict(payload, "最终视频提示词")
    for wrapper_key in ("final_video_prompt", "video_prompt", "最终视频提示词", "data", "result"):
        wrapped = prompt.get(wrapper_key)
        if isinstance(wrapped, dict):
            prompt = dict(wrapped)
            break

    normalized = dict(prompt)

    raw_video_setup = normalized.get("video_setup")
    if isinstance(raw_video_setup, dict):
        video_setup = _prefer_localized_descriptive_text(
            "；".join(str(value or "").strip() for value in raw_video_setup.values() if str(value or "").strip()),
            default="原生自然短视频脚本",
        )
    else:
        video_setup = _prefer_localized_descriptive_text(
            raw_video_setup,
            default="原生自然短视频脚本",
        )
    normalized["video_setup"] = video_setup

    raw_execution_boundary = normalized.get("execution_boundary")
    if isinstance(raw_execution_boundary, list):
        execution_boundary = "；".join(str(item or "").strip() for item in raw_execution_boundary if str(item or "").strip())
    elif isinstance(raw_execution_boundary, dict):
        execution_boundary = "；".join(str(value or "").strip() for value in raw_execution_boundary.values() if str(value or "").strip())
    else:
        execution_boundary = str(raw_execution_boundary or "").strip()
    execution_boundary = _prefer_localized_descriptive_text(
        execution_boundary,
        default="原生自然，商品保持画面主角",
    )
    normalized["execution_boundary"] = execution_boundary

    raw_sound_design = normalized.get("sound_design")
    if isinstance(raw_sound_design, dict):
        sound_design = dict(raw_sound_design)
    elif isinstance(raw_sound_design, str) and raw_sound_design.strip():
        sound_design = {"bgm": raw_sound_design.strip()}
    else:
        sound_design = {}
    sound_defaults = {
        "bgm": "清晰可感知的无歌词背景音乐，中低音量，持续作为情绪底色",
        "voiceover_mix": "口播是信息主线，BGM 在口播出现时自动压低，不盖过口播",
        "rhythm_relation": "音乐可轻度贴合镜头切换、手部动作和商品近景停顿，但不改变既定动作链",
        "sfx": "少量且只服务明确画面动作",
    }
    for key, default in sound_defaults.items():
        sound_design[key] = _prefer_localized_descriptive_text(sound_design.get(key), default=default)
    normalized["sound_design"] = sound_design

    shot_execution_raw = normalized.get("shot_execution")
    if not isinstance(shot_execution_raw, list):
        shot_execution_raw = []

    total_shots = len([shot for shot in shot_execution_raw if isinstance(shot, dict)])

    def infer_task(index: int) -> str:
        if total_shots <= 1:
            return "proof+decision"
        if index == 1:
            return "hook"
        if index == total_shots:
            return "decision"
        return "proof"

    compact_shots: List[Dict[str, Any]] = []
    normalized_boundary = re.sub(r"\s+", "", execution_boundary)
    for index, shot_value in enumerate(shot_execution_raw, 1):
        if not isinstance(shot_value, dict):
            continue
        shot_no_raw = shot_value.get("shot_no")
        if isinstance(shot_no_raw, int):
            shot_no = shot_no_raw
        else:
            shot_no_text = str(shot_no_raw or "").strip()
            shot_no = int(shot_no_text) if shot_no_text.isdigit() else index

        spoken_line_task = str(shot_value.get("spoken_line_task", "") or "").strip()
        if spoken_line_task not in {"hook", "proof", "decision", "proof+decision", "none"}:
            spoken_line_task = infer_task(index)

        style_note = _prefer_localized_descriptive_text(
            shot_value.get("style_note"),
            shot_value.get("style"),
            shot_value.get("note"),
            default="",
        )
        normalized_style_note = re.sub(r"\s+", "", style_note)
        if normalized_style_note and normalized_style_note in normalized_boundary:
            style_note = ""

        compact_shots.append(
            {
                "shot_no": shot_no,
                "duration": str(shot_value.get("duration", "") or "").strip(),
                "shot_content": _prefer_localized_descriptive_text(
                    shot_value.get("shot_content"),
                    shot_value.get("visual"),
                    shot_value.get("scene"),
                    shot_value.get("frame"),
                    default="镜头画面推进",
                ),
                "voiceover_text_target_language": _coerce_scalar_text(
                    shot_value.get("voiceover_text_target_language")
                )
                or _coerce_scalar_text(shot_value.get("voiceover"))
                or _coerce_scalar_text(shot_value.get("voiceover_text")),
                "voiceover_text_zh": _coerce_scalar_text(shot_value.get("voiceover_text_zh")),
                "spoken_line_task": spoken_line_task,
                "person_action": _prefer_localized_descriptive_text(
                    shot_value.get("person_action"),
                    shot_value.get("action"),
                    default="人物自然完成动作",
                ),
                "performance": _normalize_performance_payload(
                    shot_value.get("performance"),
                    fallback=shot_value.get("person_action") or shot_value.get("action"),
                ),
                "style_note": style_note,
            }
        )

    normalized["shot_execution"] = compact_shots
    return normalized


def validate_video_prompt_payload(payload: Any) -> None:
    prompt = _normalize_video_prompt_payload(payload)
    if isinstance(payload, dict):
        payload.clear()
        payload.update(prompt)
    _require_fields(
        prompt,
        [
            "video_setup",
            "shot_execution",
            "sound_design",
            "execution_boundary",
        ],
        "最终视频提示词",
    )
    for key in ("video_setup", "execution_boundary"):
        _ensure_chinese_descriptive_field(prompt, key, "最终视频提示词")
    sound_design = _require_dict(prompt.get("sound_design"), "sound_design")
    _require_fields(sound_design, ["bgm", "voiceover_mix", "rhythm_relation", "sfx"], "sound_design")
    for key in ("bgm", "voiceover_mix", "rhythm_relation", "sfx"):
        _ensure_chinese_descriptive_field(sound_design, key, "sound_design")

    shot_execution = _require_list(prompt.get("shot_execution"), "shot_execution")
    if len(shot_execution) < 4 or len(shot_execution) > 6:
        raise JSONParseError("shot_execution 数量必须在 4 到 6 之间")
    for index, shot_value in enumerate(shot_execution, 1):
        shot = _require_dict(shot_value, f"shot_execution 第 {index} 项")
        _require_fields(
            shot,
            [
                "shot_no",
                "duration",
                "shot_content",
                "voiceover_text_target_language",
                "voiceover_text_zh",
                "spoken_line_task",
                "person_action",
                "performance",
            ],
            f"shot_execution 第 {index} 项",
        )
        if not isinstance(shot.get("shot_no"), int):
            raise JSONParseError(f"shot_execution 第 {index} 项 shot_no 必须为整数")
        _ensure_string_field(shot, "duration", f"shot_execution 第 {index} 项")
        for key in ("shot_content", "person_action"):
            _ensure_chinese_descriptive_field(shot, key, f"shot_execution 第 {index} 项")
        performance = _require_dict(shot.get("performance"), f"shot_execution 第 {index} 项.performance")
        _require_fields(
            performance,
            ["gaze", "expression_or_micro_reaction", "body_language", "product_interaction"],
            f"shot_execution 第 {index} 项.performance",
        )
        _ensure_string_field(performance, "gaze", f"shot_execution 第 {index} 项.performance")
        if not str(performance.get("gaze") or "").strip():
            raise JSONParseError(f"shot_execution 第 {index} 项.performance.gaze 不能为空")
        for perf_key in ("expression_or_micro_reaction", "body_language", "product_interaction"):
            _ensure_chinese_descriptive_field(
                performance,
                perf_key,
                f"shot_execution 第 {index} 项.performance",
                allow_empty=True,
            )
        _ensure_string_field(shot, "voiceover_text_target_language", f"shot_execution 第 {index} 项", allow_empty=True)
        _ensure_string_field(shot, "voiceover_text_zh", f"shot_execution 第 {index} 项", allow_empty=True)
        _ensure_string_field(shot, "spoken_line_task", f"shot_execution 第 {index} 项")
        if shot.get("spoken_line_task") not in {"hook", "proof", "decision", "proof+decision", "none"}:
            raise JSONParseError(
                f"shot_execution 第 {index} 项 spoken_line_task 必须为 hook|proof|decision|proof+decision|none"
            )
        if "style_note" not in shot or not isinstance(shot.get("style_note"), str):
            shot["style_note"] = str(shot.get("style_note", "") or "")
        _ensure_chinese_descriptive_field(shot, "style_note", f"shot_execution 第 {index} 项", allow_empty=True)
        for forbidden_key in ("shot_purpose", "anchor_reference", "task_type", "subtitle", "subtitle_text_target_language", "subtitle_text_zh", "voiceover"):
            if forbidden_key in shot:
                raise JSONParseError(f"shot_execution 第 {index} 项 不应包含 {forbidden_key}")


def validate_variant_schema_v2(
    payload: Any,
    expected_count: int = 5,
    expected_variant_ids: Optional[List[str]] = None,
    target_language: Optional[str] = None,
) -> None:
    container = _require_dict(payload, "变体输出")
    if "variant_count" in container:
        if container.get("variant_count") != expected_count:
            raise JSONParseError(
                f"variant_count 必须为 {expected_count}，当前为 {container.get('variant_count')}"
            )
    variants = container.get("variants")
    if not isinstance(variants, list):
        raise JSONParseError("变体输出缺少 variants 数组")
    if len(variants) != expected_count:
        raise JSONParseError(f"变体数量必须为 {expected_count}，当前为 {len(variants)}")

    required_variant_fields = [
        "variant_id",
        "variant_no",
        "variant_strength",
        "variant_focus",
        "source_script_id",
        "source_strategy_id",
        "strategy_id",
        "strategy_name",
        "primary_selling_point",
        "final_video_script_prompt",
    ]

    actual_variant_ids: List[str] = []
    for index, variant_value in enumerate(variants, 1):
        variant = _require_dict(variant_value, f"variants 第 {index} 项")
        _require_fields(variant, required_variant_fields, f"variants 第 {index} 项")

        for key in (
            "variant_id",
            "variant_strength",
            "variant_focus",
            "source_script_id",
            "source_strategy_id",
            "strategy_id",
            "strategy_name",
            "primary_selling_point",
        ):
            _ensure_string_field(variant, key, f"variants 第 {index} 项")
        if not isinstance(variant.get("variant_no"), int):
            raise JSONParseError(f"variants 第 {index} 项 variant_no 必须为整数")
        if variant.get("variant_strength") not in {"light", "medium", "heavy"}:
            raise JSONParseError(f"variants 第 {index} 项 variant_strength 必须为 light|medium|heavy")
        if variant.get("variant_focus") not in {
            "opening",
            "proof",
            "ending",
            "scene",
            "rhythm",
            "persona",
            "action",
            "outfit",
            "emotion",
        }:
            raise JSONParseError(
                f"variants 第 {index} 项 variant_focus 必须为 opening|proof|ending|scene|rhythm|persona|action|outfit|emotion"
            )

        final_prompt = _normalize_variant_final_prompt(variant.get("final_video_script_prompt"))
        if not final_prompt:
            raise JSONParseError(f"variants 第 {index} 项 final_video_script_prompt 缺失或格式错误")
        variant["final_video_script_prompt"] = final_prompt
        video_setup = _require_dict(final_prompt.get("video_setup"), f"variants 第 {index} 项 final_video_script_prompt.video_setup")
        _require_fields(
            video_setup,
            [
                "video_theme",
                "product_focus",
                "person_final",
                "outfit_final",
                "scene_final",
                "emotion_final",
                "overall_style",
            ],
            f"variants 第 {index} 项 final_video_script_prompt.video_setup",
        )
        for key in (
            "video_theme",
            "product_focus",
            "person_final",
            "outfit_final",
            "scene_final",
            "emotion_final",
            "overall_style",
        ):
            _ensure_chinese_descriptive_field(
                video_setup,
                key,
                f"variants 第 {index} 项 final_video_script_prompt.video_setup",
            )

        shot_execution = _require_list(final_prompt.get("shot_execution"), f"variants 第 {index} 项 final_video_script_prompt.shot_execution")
        if len(shot_execution) < 4 or len(shot_execution) > 6:
            raise JSONParseError(f"variants 第 {index} 项 final_video_script_prompt.shot_execution 数量必须在 4 到 6 之间")
        for shot_index, shot_value in enumerate(shot_execution, 1):
            shot = _require_dict(shot_value, f"variants 第 {index} 项 final_video_script_prompt.shot_execution 第 {shot_index} 项")
            _require_fields(
                shot,
                ["shot_no", "duration", "visual", "person_action", "product_focus"],
                f"variants 第 {index} 项 final_video_script_prompt.shot_execution 第 {shot_index} 项",
            )
            if not isinstance(shot.get("shot_no"), int):
                raise JSONParseError(
                    f"variants 第 {index} 项 final_video_script_prompt.shot_execution 第 {shot_index} 项 shot_no 必须为整数"
                )
            if "voiceover" not in shot or not isinstance(shot.get("voiceover"), str):
                shot["voiceover"] = str(shot.get("voiceover", "") or "")
            for key in ("duration", "visual", "person_action", "product_focus"):
                _ensure_chinese_descriptive_field(
                    shot,
                    key,
                    f"variants 第 {index} 项 final_video_script_prompt.shot_execution 第 {shot_index} 项",
                )
            _ensure_target_language_field(
                shot,
                "voiceover",
                f"variants 第 {index} 项 final_video_script_prompt.shot_execution 第 {shot_index} 项",
                target_language=target_language,
                allow_empty=True,
            )

        style_boundaries = final_prompt.get("style_boundaries")
        if not isinstance(style_boundaries, list):
            style_boundaries = []
            final_prompt["style_boundaries"] = style_boundaries
        for item_index, item in enumerate(style_boundaries, 1):
            if not isinstance(item, str):
                raise JSONParseError(
                    f"variants 第 {index} 项 final_video_script_prompt.style_boundaries 第 {item_index} 个元素必须为字符串"
                )
            if _looks_like_non_chinese_descriptive_text(item):
                raise JSONParseError(
                    f"variants 第 {index} 项 final_video_script_prompt.style_boundaries 第 {item_index} 个元素除口播/字幕外必须使用中文描述"
                )

        internal_state = variant.get("internal_variant_state")
        if internal_state is not None:
            if not isinstance(internal_state, dict):
                internal_state = {}
            internal = _require_dict(internal_state, f"variants 第 {index} 项 internal_variant_state")
            internal = _normalize_variant_internal_state(internal, variant)
            variant["internal_variant_state"] = internal
            for key in (
                "variant_name",
                "main_adjustment",
                "test_goal",
                "variant_change_summary",
                "main_change",
                "secondary_change",
                "difference_summary",
            ):
                _ensure_string_field(internal, key, f"variants 第 {index} 项 internal_variant_state", allow_empty=True)

            for list_key in ("inherited_core_items", "changed_structure_fields", "changed_feeling_layers", "coverage"):
                value = internal.get(list_key)
                if not isinstance(value, list):
                    value = []
                    internal[list_key] = value
                for item_index, item in enumerate(value, 1):
                    if not isinstance(item, str):
                        raise JSONParseError(
                            f"variants 第 {index} 项 internal_variant_state.{list_key} 第 {item_index} 个元素必须为字符串"
                        )

            internal["changed_feeling_layers"] = _normalize_changed_feeling_layers(internal.get("changed_feeling_layers"))
            changed_layers = {str(item).strip() for item in internal.get("changed_feeling_layers", []) if str(item).strip()}
            if not changed_layers.issubset({"person", "outfit", "scene", "emotion"}):
                raise JSONParseError(f"variants 第 {index} 项 internal_variant_state.changed_feeling_layers 只能包含 person|outfit|scene|emotion")

            coverage = {str(item).strip() for item in internal.get("coverage", []) if str(item).strip()}
            if coverage and not coverage.issubset({"hook", "proof", "decision"}):
                raise JSONParseError(f"variants 第 {index} 项 internal_variant_state.coverage 只能包含 hook|proof|decision")

            proof_blueprint = internal.get("proof_blueprint")
            if not isinstance(proof_blueprint, list):
                proof_blueprint = []
                internal["proof_blueprint"] = proof_blueprint
            for proof_index, proof_value in enumerate(proof_blueprint, 1):
                proof_item = _require_dict(
                    proof_value,
                    f"variants 第 {index} 项 internal_variant_state.proof_blueprint 第 {proof_index} 项",
                )
                _require_fields(
                    proof_item,
                    ["anchor", "action", "visible_result", "concern_relieved"],
                    f"variants 第 {index} 项 internal_variant_state.proof_blueprint 第 {proof_index} 项",
                )
                for key in ("anchor", "action", "visible_result", "concern_relieved"):
                    _ensure_string_field(
                        proof_item,
                        key,
                        f"variants 第 {index} 项 internal_variant_state.proof_blueprint 第 {proof_index} 项",
                    )

            for layer_key, required_fields in (
                (
                    "person_variant_layer",
                    [
                        "person_identity_base",
                        "person_style_base",
                        "appearance_boundary",
                        "body_presentation_boundary",
                        "camera_relationship",
                    ],
                ),
                (
                    "outfit_variant_layer",
                    [
                        "outfit_core_formula",
                        "product_role_in_outfit",
                        "silhouette_boundary",
                        "pairing_boundary",
                        "color_mood_boundary",
                    ],
                ),
                (
                    "scene_variant_layer",
                    [
                        "scene_domain_base",
                        "scene_subspace",
                        "scene_function_moment",
                        "light_boundary",
                        "prop_boundary",
                    ],
                ),
                (
                    "emotion_variant_layer",
                    [
                        "emotion_base",
                        "emotion_curve",
                        "emotion_intensity_boundary",
                        "delivery_boundary",
                    ],
                ),
            ):
                layer = internal.get(layer_key)
                if not isinstance(layer, dict):
                    layer = {}
                    internal[layer_key] = layer
                for key in required_fields:
                    layer.setdefault(key, "")
                for key in required_fields:
                    _ensure_string_field(layer, key, f"variants 第 {index} 项 internal_variant_state.{layer_key}", allow_empty=True)

            internal["consistency_checks"] = _normalize_consistency_checks(internal.get("consistency_checks"))
            consistency_checks = _require_dict(
                internal.get("consistency_checks"),
                f"variants 第 {index} 项 internal_variant_state.consistency_checks",
            )
            for key in (
                "person_manifestation",
                "outfit_manifestation",
                "scene_manifestation",
                "emotion_manifestation",
            ):
                consistency_checks.setdefault(key, "")
                _ensure_string_field(
                    consistency_checks,
                    key,
                    f"variants 第 {index} 项 internal_variant_state.consistency_checks",
                    allow_empty=True,
                )

        actual_variant_ids.append(str(variant.get("variant_id", "")).strip())

    if expected_variant_ids and actual_variant_ids != expected_variant_ids:
        raise JSONParseError(
            f"variant_id 顺序或内容不符合预期，期望 {expected_variant_ids}，实际 {actual_variant_ids}"
        )


def validate_script_payload(script_json: Any, target_language: Optional[str] = None) -> None:
    validate_script_schema_v2(script_json, target_language=target_language)


def validate_variant_payload(
    payload: Any,
    expected_count: int = 5,
    expected_variant_ids: Optional[List[str]] = None,
    target_language: Optional[str] = None,
) -> None:
    validate_variant_schema_v2(
        payload,
        expected_count=expected_count,
        expected_variant_ids=expected_variant_ids,
        target_language=target_language,
    )
