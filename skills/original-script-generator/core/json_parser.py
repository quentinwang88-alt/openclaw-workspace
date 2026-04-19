#!/usr/bin/env python3
"""
LLM JSON 解析与 schema 校验。
"""

import json
import re
from typing import Any, Dict, List, Optional


class JSONParseError(Exception):
    """LLM JSON 解析失败。"""


_CJK_RE = re.compile(r"[\u4e00-\u9fff]")
_LATINISH_RE = re.compile(r"[A-Za-zÀ-ỹĐđ]")


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
    return normalized


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
    allowed_families = {"apparel", "jewelry", "hair_accessory", "accessory", "unknown"}
    allowed_slots = {"body", "upper_body", "lower_body", "full_body", "wrist", "neck", "ear", "finger", "hair", "unknown"}

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


def validate_anchor_card_payload(payload: Any) -> None:
    card = _require_dict(payload, "锚点卡")
    if "parameter_anchors" not in card or not isinstance(card.get("parameter_anchors"), list):
        card["parameter_anchors"] = []
    _require_fields(
        card,
        [
            "product_positioning_one_liner",
            "hard_anchors",
            "display_anchors",
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


def _validate_language_fields(obj: Dict[str, Any], label: str, subtitle_required: bool = False) -> None:
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

    _ensure_string_field(obj, "subtitle_text_target_language", label, allow_empty=not subtitle_required)
    _ensure_string_field(obj, "subtitle_text_zh", label, allow_empty=not subtitle_required)
    _ensure_string_field(obj, "voiceover_text_target_language", label, allow_empty=True)
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
        _require_fields(strategy, required_fields, f"strategies 第 {index} 项")
        for key in required_fields:
            if key in {"styling_base_constraints", "realism_principles", "forbidden_patterns"}:
                _ensure_string_list_field(strategy, key, f"strategies 第 {index} 项")
            else:
                _ensure_string_field(strategy, key, f"strategies 第 {index} 项")
        if str(strategy.get("strategy_id", "") or "").strip() not in {"S1", "S2", "S3", "S4"}:
            raise JSONParseError(f"strategies 第 {index} 项 strategy_id 必须为 S1/S2/S3/S4")
        if str(strategy.get("final_strategy_id", "") or "").strip() != f"Final_{strategy.get('strategy_id')}":
            raise JSONParseError(f"strategies 第 {index} 项 final_strategy_id 与 strategy_id 不一致")


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
    ]
    _require_fields(plan, required_fields, "表达扩充计划")
    for key in required_fields:
        _ensure_string_field(plan, key, "表达扩充计划")


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
            normalized_storyboard.append(item)
        script["storyboard"] = normalized_storyboard

    return script


def _validate_script_positioning(positioning: Any, label: str) -> None:
    obj = _require_dict(positioning, label)
    _require_fields(obj, ["script_title", "direction_type", "core_primary_selling_point"], label)
    for key in ("script_title", "direction_type", "core_primary_selling_point"):
        _ensure_chinese_descriptive_field(obj, key, label)


def validate_script_schema_v2(script_json: Any) -> None:
    script = _normalize_script_payload(script_json)
    if isinstance(script_json, dict):
        script_json.clear()
        script_json.update(script)
    script = _require_dict(script, "脚本输出")
    _require_fields(
        script,
        [
            "script_positioning",
            "opening_design",
            "full_15s_flow",
            "storyboard",
            "execution_constraints",
            "negative_constraints",
        ],
        "脚本输出",
    )

    _validate_script_positioning(script.get("script_positioning"), "script_positioning")

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
        "style_note",
        "anchor_reference",
        "task_type",
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
        ):
            if key in {"duration", "task_type"}:
                _ensure_string_field(shot, key, f"storyboard 第 {index} 个镜头")
            else:
                _ensure_chinese_descriptive_field(shot, key, f"storyboard 第 {index} 个镜头")
        _validate_language_fields(shot, f"storyboard 第 {index} 个镜头", subtitle_required=False)
        _ensure_string_field(shot, "spoken_line_task", f"storyboard 第 {index} 个镜头")
        for optional_key in ("person_state", "styling_base_role", "scene_function", "current_emotion"):
            _ensure_optional_string_field(shot, optional_key, f"storyboard 第 {index} 个镜头")

        if shot.get("task_type") not in {"attention", "proof", "bridge"}:
            raise JSONParseError(
                f"storyboard 第 {index} 个镜头 task_type 必须为 attention|proof|bridge"
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


def validate_review_payload(payload: Any) -> None:
    review = _require_dict(payload, "质检输出")
    _require_fields(review, ["pass", "major_issues", "minor_issues", "repair_actions", "repaired_script"], "质检输出")
    if not isinstance(review.get("pass"), bool):
        raise JSONParseError("质检输出.pass 必须为布尔值")
    for field in ("major_issues", "minor_issues", "repair_actions"):
        values = _ensure_list_field(review, field, "质检输出", allow_empty=True)
        for index, item in enumerate(values, 1):
            if not isinstance(item, str):
                raise JSONParseError(f"质检输出.{field}[{index}] 必须为字符串")
    validate_script_payload(review.get("repaired_script"))


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
            "execution_boundary",
        ],
        "最终视频提示词",
    )
    for key in ("video_setup", "execution_boundary"):
        _ensure_chinese_descriptive_field(prompt, key, "最终视频提示词")

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
            ],
            f"shot_execution 第 {index} 项",
        )
        if not isinstance(shot.get("shot_no"), int):
            raise JSONParseError(f"shot_execution 第 {index} 项 shot_no 必须为整数")
        _ensure_string_field(shot, "duration", f"shot_execution 第 {index} 项")
        for key in ("shot_content", "person_action"):
            _ensure_chinese_descriptive_field(shot, key, f"shot_execution 第 {index} 项")
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
        _ensure_string_field(
            shot,
            "voiceover",
            f"variants 第 {index} 项 final_video_script_prompt.shot_execution 第 {shot_index} 项",
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


def validate_script_payload(script_json: Any) -> None:
    validate_script_schema_v2(script_json)


def validate_variant_payload(
    payload: Any,
    expected_count: int = 5,
    expected_variant_ids: Optional[List[str]] = None,
) -> None:
    validate_variant_schema_v2(
        payload,
        expected_count=expected_count,
        expected_variant_ids=expected_variant_ids,
    )
