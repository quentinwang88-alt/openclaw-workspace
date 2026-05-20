#!/usr/bin/env python3
"""
原创脚本生成提示词构建。
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


def _compact_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))


def _list_text_items(values: Any, limit: int = 4) -> List[str]:
    if not isinstance(values, list):
        return []
    items: List[str] = []
    for value in values:
        if isinstance(value, dict):
            text = str(value.get("desc") or value.get("description") or value.get("text") or value.get("value") or "").strip()
        else:
            text = str(value or "").strip()
        if text:
            items.append(text)
        if len(items) >= limit:
            break
    return items


def _compact_q1_context(
    anchor_card_json: Dict[str, Any],
    final_strategy_json: Dict[str, Any],
    expression_plan_json: Dict[str, Any],
    persona_style_emotion_pack_json: Dict[str, Any],
    pre_qc_result: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    contract = anchor_card_json.get("category_execution_contract") if isinstance(anchor_card_json, dict) else {}
    if not isinstance(contract, dict):
        contract = {}
    human_contract = (
        persona_style_emotion_pack_json.get("human_performance_contract")
        if isinstance(persona_style_emotion_pack_json, dict)
        else {}
    )
    if not isinstance(human_contract, dict):
        human_contract = {}
    audio_policy = contract.get("audio_policy") if isinstance(contract.get("audio_policy"), dict) else {}
    return {
        "category_execution_contract": {
            "display_family": contract.get("display_family", ""),
            "product_subtype": contract.get("product_subtype", ""),
            "use_case": contract.get("use_case", ""),
            "placement_zone": contract.get("placement_zone", ""),
            "hold_scope": contract.get("hold_scope", ""),
            "orientation": contract.get("orientation", ""),
            "primary_visual_result": contract.get("primary_visual_result", ""),
            "operation_policy": contract.get("operation_policy", ""),
            "field_confidence": contract.get("field_confidence", {}),
            "safe_shot_templates": _list_text_items(contract.get("safe_shot_templates"), 4),
            "forbidden_actions": _list_text_items(contract.get("forbidden_actions"), 4),
            "result_priority": contract.get("result_priority", ""),
            "audio_policy": {
                "bgm_style": audio_policy.get("bgm_style", ""),
                "bgm_energy": audio_policy.get("bgm_energy", ""),
                "voiceover_priority": audio_policy.get("voiceover_priority", ""),
                "forbidden_sfx": _list_text_items(audio_policy.get("forbidden_sfx"), 4),
                "audio_negative_constraints": _list_text_items(audio_policy.get("audio_negative_constraints"), 4),
            },
        },
        "key_visual_constraints": [
            {
                "constraint": item.get("constraint", ""),
                "confidence": item.get("confidence", ""),
            }
            for item in (anchor_card_json.get("key_visual_constraints") or [])[:3]
            if isinstance(item, dict)
        ],
        "parameter_anchors": [
            {
                "parameter_name": item.get("parameter_name", ""),
                "parameter_value": item.get("parameter_value", ""),
                "confidence": item.get("confidence", ""),
            }
            for item in (anchor_card_json.get("parameter_anchors") or [])[:5]
            if isinstance(item, dict)
        ],
        "strategy_core": {
            "strategy_id": final_strategy_json.get("strategy_id", ""),
            "script_role": final_strategy_json.get("script_role", ""),
            "primary_focus": final_strategy_json.get("primary_focus", ""),
            "secondary_focus": final_strategy_json.get("secondary_focus", ""),
            "primary_selling_point": final_strategy_json.get("primary_selling_point", ""),
            "proof_path": final_strategy_json.get("proof_path", final_strategy_json.get("proof_mode", "")),
            "opening_angle": final_strategy_json.get("opening_angle", final_strategy_json.get("opening_mode", "")),
            "performance_strategy_hint": final_strategy_json.get(
                "performance_strategy_hint",
                final_strategy_json.get("performance_bias", ""),
            ),
            "risk_note": final_strategy_json.get("risk_note", ""),
        },
        "expression_core": {
            "main_expression_pattern": expression_plan_json.get("main_expression_pattern", ""),
            "native_expression_entry": expression_plan_json.get("native_expression_entry", ""),
            "opening_expression_task": expression_plan_json.get("opening_expression_task", ""),
            "middle_expression_task": expression_plan_json.get("middle_expression_task", ""),
            "ending_expression_task": expression_plan_json.get("ending_expression_task", ""),
            "voiceover_intent": expression_plan_json.get("voiceover_intent", ""),
            "voiceover_language_requirement": expression_plan_json.get("voiceover_language_requirement", ""),
        },
        "human_performance_contract": {
            "performance_family": human_contract.get("performance_family", ""),
            "persona_mode": human_contract.get("persona_mode", ""),
            "gaze_plan": human_contract.get("gaze_plan", [])[:6] if isinstance(human_contract.get("gaze_plan"), list) else [],
            "gaze_rule": human_contract.get("gaze_rule", {}),
            "performance_intensity": human_contract.get("performance_intensity", ""),
            "active_micro_reaction_limit": human_contract.get("active_micro_reaction_limit", 0),
            "forbidden_performance": (
                human_contract.get("forbidden_performance", [])[:6]
                if isinstance(human_contract.get("forbidden_performance"), list)
                else []
            ),
        },
        "pre_qc_result": pre_qc_result or {},
    }


def _short_text(value: Any, limit: int = 160) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def _short_list(values: Any, limit: int = 3, item_limit: int = 120) -> List[Any]:
    if not isinstance(values, list):
        return []
    items: List[Any] = []
    for value in values:
        if isinstance(value, dict):
            compact = {}
            for key in ("id", "desc", "constraint", "confidence", "parameter_name", "parameter_value", "template_id", "replacement_shot"):
                if key in value and str(value.get(key) or "").strip():
                    compact[key] = _short_text(value.get(key), item_limit)
            if compact:
                items.append(compact)
        else:
            text = _short_text(value, item_limit)
            if text:
                items.append(text)
        if len(items) >= limit:
            break
    return items


def _compact_contract_for_p7(contract: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(contract, dict):
        contract = {}
    audio_policy = contract.get("audio_policy") if isinstance(contract.get("audio_policy"), dict) else {}
    return {
        "display_family": contract.get("display_family", ""),
        "product_subtype": contract.get("product_subtype", ""),
        "use_case": contract.get("use_case", ""),
        "placement_zone": contract.get("placement_zone", ""),
        "hold_scope": contract.get("hold_scope", ""),
        "orientation": contract.get("orientation", ""),
        "primary_visual_result": _short_text(contract.get("primary_visual_result"), 220),
        "operation_policy": contract.get("operation_policy", ""),
        "field_confidence": contract.get("field_confidence", {}),
        "safe_shot_templates": _short_list(contract.get("safe_shot_templates"), 3, 120),
        "forbidden_actions": _short_list(contract.get("forbidden_actions"), 4, 120),
        "result_priority": _short_text(contract.get("result_priority"), 180),
        "season_context": contract.get("season_context", {}),
        "hat_risk_tier": contract.get("hat_risk_tier", ""),
        "set_relationship": contract.get("set_relationship", ""),
        "co_styling_hint": contract.get("co_styling_hint", {}),
        "audio_policy": {
            "bgm_style": _short_text(audio_policy.get("bgm_style"), 90),
            "bgm_energy": audio_policy.get("bgm_energy", ""),
            "voiceover_priority": audio_policy.get("voiceover_priority", "high"),
            "sfx_policy": _short_text(audio_policy.get("sfx_policy"), 120),
            "allowed_sfx": _short_list(audio_policy.get("allowed_sfx"), 3, 60),
            "forbidden_sfx": _short_list(audio_policy.get("forbidden_sfx"), 4, 60),
            "audio_negative_constraints": _short_list(audio_policy.get("audio_negative_constraints"), 4, 80),
        },
    }


def _operation_policy_template(operation_policy: str, proof_path: str) -> Dict[str, Any]:
    op = str(operation_policy or "").strip()
    proof = str(proof_path or "").strip() or "A_result_detail_only"
    ranges = ["0-2.5s", "2.5-4.5s", "4.5-6.5s", "6.5-9.5s", "9.5-12s", "12-15s"]
    if op == "process_allowed_once" and proof in {"C_result_with_short_process", "D_result_with_light_compare_and_short_process"}:
        purposes = [
            ("hook", "结果首镜，先展示已完成佩戴/使用效果"),
            ("proof", "产品结构或关键细节近景"),
            ("transition", "一次短促低风险过程：靠近/一瞬闭合/立即切结果"),
            ("proof", "已完成结果复核，证明关系成立"),
            ("proof", "整体完成度或使用场景"),
            ("decision", "轻决策收尾"),
        ]
    elif op in {"process_forbidden", "static_result_only"}:
        purposes = [
            ("hook", "静态结果首镜，商品关系清楚"),
            ("proof", "产品局部细节"),
            ("proof", "角度变化或静物细节，不拍过程"),
            ("proof", "轻微动态复核，人物/商品不纠缠"),
            ("proof", "整体完成度"),
            ("decision", "轻决策收尾"),
        ]
    elif proof == "B_result_with_light_compare":
        purposes = [
            ("hook", "结果首镜，先展示已完成效果"),
            ("proof", "一次轻对比，只证明变化，不展开过程"),
            ("proof", "回到已完成结果复核"),
            ("proof", "产品细节或局部结果"),
            ("proof", "整体完成度"),
            ("decision", "轻决策收尾"),
        ]
    else:
        purposes = [
            ("hook", "结果首镜，先展示已完成效果"),
            ("proof", "产品细节近景"),
            ("proof", "已完成结果复核"),
            ("proof", "整体完成度或使用场景"),
            ("decision", "轻决策信号"),
            ("decision", "轻分享收尾"),
        ]
    return {
        "source": "code_operation_policy_template",
        "operation_policy": op,
        "proof_path": proof,
        "time_rule": "总时长 15s，严格使用下列连续 time_range；不得重叠、缺秒或改成完整教程。",
        "shot_skeleton_template": [
            {
                "shot_index": index,
                "time_range": time_range,
                "role": role,
                "shot_purpose": purpose,
                "proof_path": proof,
            }
            for index, (time_range, (role, purpose)) in enumerate(zip(ranges, purposes), 1)
        ],
    }


def _compact_script_brief_for_p7(script_brief_json: Dict[str, Any]) -> Dict[str, Any]:
    brief = script_brief_json if isinstance(script_brief_json, dict) else {}
    final_strategy = brief.get("final_strategy") if isinstance(brief.get("final_strategy"), dict) else {}
    contract = _compact_contract_for_p7(
        brief.get("category_execution_contract") if isinstance(brief.get("category_execution_contract"), dict) else {}
    )
    human_contract = brief.get("human_performance_contract") if isinstance(brief.get("human_performance_contract"), dict) else {}
    ai_profile = brief.get("ai_shot_risk_profile") if isinstance(brief.get("ai_shot_risk_profile"), dict) else {}
    proof_path = final_strategy.get("proof_path") or brief.get("proof_path") or "A_result_detail_only"
    compact = {
        "product_type": brief.get("product_type", ""),
        "product_positioning_one_liner": _short_text(brief.get("product_positioning_one_liner"), 120),
        "category_execution_contract": contract,
        "p7_execution_template": _operation_policy_template(contract.get("operation_policy", ""), str(proof_path)),
        "key_visual_constraints": _short_list(brief.get("key_visual_constraints"), 3, 130),
        "parameter_anchors": _short_list(brief.get("parameter_anchors"), 4, 100),
        "selected_primary_selling_point": brief.get("selected_primary_selling_point", {}),
        "selected_opening_strategy": {
            "opening_mode_candidate": (brief.get("selected_opening_strategy") or {}).get("opening_mode_candidate", "")
            if isinstance(brief.get("selected_opening_strategy"), dict)
            else "",
            "first_frame_visual": _short_text(
                (brief.get("selected_opening_strategy") or {}).get("first_frame_visual", "")
                if isinstance(brief.get("selected_opening_strategy"), dict)
                else "",
                140,
            ),
            "action_design": _short_text(
                (brief.get("selected_opening_strategy") or {}).get("action_design", "")
                if isinstance(brief.get("selected_opening_strategy"), dict)
                else "",
                120,
            ),
        },
        "focus_control": brief.get("focus_control", {}),
        "final_strategy": {
            "strategy_id": final_strategy.get("strategy_id", ""),
            "script_role": final_strategy.get("script_role", ""),
            "primary_focus": _short_text(final_strategy.get("primary_focus"), 140),
            "secondary_focus": _short_text(final_strategy.get("secondary_focus"), 120),
            "primary_selling_point": _short_text(final_strategy.get("primary_selling_point"), 140),
            "proof_thesis": _short_text(final_strategy.get("proof_thesis"), 160),
            "decision_thesis": _short_text(final_strategy.get("decision_thesis"), 120),
            "opening_angle": final_strategy.get("opening_angle", ""),
            "proof_path": proof_path,
            "performance_strategy_hint": _short_text(final_strategy.get("performance_strategy_hint"), 160),
            "scene_suggestion": _short_text(final_strategy.get("scene_suggestion"), 100),
            "risk_note": _short_text(final_strategy.get("risk_note"), 140),
        },
        "light_control_fields": brief.get("light_control_fields", {}),
        "persona_pack": {
            "persona_state": (brief.get("persona_pack") or {}).get("persona_state", "")
            if isinstance(brief.get("persona_pack"), dict)
            else "",
            "clothing_rule": _short_text(
                (brief.get("persona_pack") or {}).get("clothing_rule", "")
                if isinstance(brief.get("persona_pack"), dict)
                else "",
                160,
            ),
            "movement_style": _short_text(
                (brief.get("persona_pack") or {}).get("movement_style", "")
                if isinstance(brief.get("persona_pack"), dict)
                else "",
                120,
            ),
            "emotion_progression": _short_text(
                (brief.get("persona_pack") or {}).get("emotion_progression", "")
                if isinstance(brief.get("persona_pack"), dict)
                else "",
                140,
            ),
        },
        "human_performance_contract": {
            "performance_family": human_contract.get("performance_family", ""),
            "persona_mode": human_contract.get("persona_mode", ""),
            "gaze_plan": _short_list(human_contract.get("gaze_plan"), 5, 60),
            "gaze_rule": human_contract.get("gaze_rule", {}),
            "micro_reaction_beats": _short_list(human_contract.get("micro_reaction_beats"), 4, 90),
            "body_language_beats": _short_list(human_contract.get("body_language_beats"), 4, 90),
            "product_interaction_beats": _short_list(human_contract.get("product_interaction_beats"), 4, 90),
            "performance_intensity": human_contract.get("performance_intensity", ""),
            "active_micro_reaction_limit": human_contract.get("active_micro_reaction_limit", 0),
            "forbidden_performance": _short_list(human_contract.get("forbidden_performance"), 4, 90),
        },
        "ai_shot_risk_profile": {
            "profile_key": ai_profile.get("profile_key", ""),
            "forbidden": _short_list(ai_profile.get("forbidden"), 4, 90),
            "high_risk": _short_list(ai_profile.get("high_risk"), 4, 90),
            "replacement_templates": _short_list(ai_profile.get("replacement_templates"), 4, 120),
        },
        "expression_plan": {
            "native_expression_entry": _short_text((brief.get("expression_plan") or {}).get("native_expression_entry", ""), 100)
            if isinstance(brief.get("expression_plan"), dict)
            else "",
            "voiceover_intent": _short_text((brief.get("expression_plan") or {}).get("voiceover_intent", ""), 180)
            if isinstance(brief.get("expression_plan"), dict)
            else "",
            "voiceover_language_requirement": (brief.get("expression_plan") or {}).get("voiceover_language_requirement", "")
            if isinstance(brief.get("expression_plan"), dict)
            else "",
        },
        "avoid_same_as_existing_scripts": _short_list(brief.get("avoid_same_as_existing_scripts"), 2, 120),
    }
    return compact


def _fill_template(template: str, values: Dict[str, Any]) -> str:
    filled = template
    for key, value in values.items():
        filled = filled.replace(f"{{{{{key}}}}}", str(value))
    return filled


PROMPT_CACHE_STABLE_PREFIX = """【稳定提示词前缀 / prompt-cache-friendly】
以下原则对原创短视频脚本生成全链路长期稳定生效：
- category_execution_contract 是商品执行硬约束；
- human_performance_contract 是人物表演软增强；
- 商品 proof 主线优先于人物表演和风格修辞；
- 目标语言字段不得混入中文；
- 过程动作必须服从 operation_policy，禁止用高风险 AI 镜头硬证明；
- audio_layer 必须服从 audio_policy，SFX 不得暗示不存在的动作；
- 输出必须是合法 JSON，不要输出 markdown 或解释文字。
动态输入统一放在后续“动态输入区”，不得改写上述稳定规则。"""


def _with_prompt_cache_prefix(stage_id: str, prompt: str) -> str:
    return f"{PROMPT_CACHE_STABLE_PREFIX}\n\n【阶段ID】{stage_id}\n\n【动态输入区与阶段输出要求】\n{prompt}"


def _optional_note_text(value: str) -> str:
    return value.strip() if isinstance(value, str) and value.strip() else "(空)"


PERFORMANCE_PROFILES_PATH = (
    Path(__file__).resolve().parent.parent
    / "assets"
    / "performance_profiles"
    / "performance_profiles.yaml"
)
_PERFORMANCE_PROFILES_CACHE: Optional[Dict[str, Any]] = None


def _load_performance_profiles() -> Dict[str, Any]:
    global _PERFORMANCE_PROFILES_CACHE
    if _PERFORMANCE_PROFILES_CACHE is not None:
        return _PERFORMANCE_PROFILES_CACHE
    try:
        data = yaml.safe_load(PERFORMANCE_PROFILES_PATH.read_text(encoding="utf-8")) or {}
    except Exception:
        data = {}
    _PERFORMANCE_PROFILES_CACHE = data if isinstance(data, dict) else {}
    return _PERFORMANCE_PROFILES_CACHE


def _display_family_from_anchor(anchor_card_json: Dict[str, Any], product_type: str, type_guard_json: Optional[Dict[str, Any]]) -> str:
    contract = anchor_card_json.get("category_execution_contract") if isinstance(anchor_card_json, dict) else {}
    if isinstance(contract, dict):
        display_family = str(contract.get("display_family") or "").strip()
        if display_family:
            return display_family
    canonical_family = str((type_guard_json or {}).get("canonical_family") or "").strip()
    if canonical_family in {"hair_accessory", "apparel_accessory"}:
        return canonical_family
    text = str(product_type or "")
    if any(token in text for token in ("发饰", "发夹", "抓夹", "边夹", "发箍", "发圈", "头绳", "头箍")):
        return "hair_accessory"
    if any(token in text for token in ("围巾", "帽子", "针织帽", "毛线帽", "渔夫帽", "贝雷帽", "护耳帽")):
        return "apparel_accessory"
    if any(token in text for token in ("耳线", "耳环", "耳饰", "耳钉", "耳夹", "耳坠")):
        return "ear_accessory"
    if any(token in text for token in ("上装", "女装", "连衣裙", "裤", "裙", "衬衫", "外套")):
        return "apparel"
    return ""


def _active_micro_reaction_limit(performance_intensity: str) -> int:
    return {
        "low": 2,
        "low_to_medium": 3,
        "medium": 4,
    }.get(str(performance_intensity or "").strip(), 0)


def _profile_variant_key(anchor_card_json: Dict[str, Any]) -> str:
    contract = anchor_card_json.get("category_execution_contract") if isinstance(anchor_card_json, dict) else {}
    if not isinstance(contract, dict):
        return ""
    product_subtype = str(contract.get("product_subtype") or "").strip()
    if product_subtype in {"scarf", "hat", "scarf_hat_set"}:
        return f"{product_subtype}_variant"
    return ""


def _performance_profile_for_prompt(
    anchor_card_json: Dict[str, Any],
    product_type: str,
    type_guard_json: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    config = _load_performance_profiles()
    display_family = _display_family_from_anchor(anchor_card_json, product_type, type_guard_json)
    enabled = set(config.get("enabled_display_families") or [])
    profiles = config.get("profiles") if isinstance(config.get("profiles"), dict) else {}
    if display_family not in enabled:
        return {
            "enable_human_performance_contract": False,
            "display_family": display_family,
            "reason": "当前 display_family 未在 performance_profiles.yaml 启用",
        }
    family_profile = profiles.get(display_family) if isinstance(profiles, dict) else {}
    default_profile = family_profile.get("default") if isinstance(family_profile, dict) else {}
    if not isinstance(default_profile, dict):
        default_profile = {}
    profile = dict(default_profile)
    variant_key = _profile_variant_key(anchor_card_json)
    variant_profile = family_profile.get(variant_key) if isinstance(family_profile, dict) and variant_key else {}
    if isinstance(variant_profile, dict):
        profile.update(variant_profile)
        profile["selected_variant"] = variant_key
    profile["enable_human_performance_contract"] = True
    profile["display_family"] = display_family
    profile["active_micro_reaction_limit"] = _active_micro_reaction_limit(profile.get("performance_intensity"))
    return profile


ACCOUNT_STYLE_BOUNDARY = "真实、轻精致、自然、可挂主页、不硬广、有审美完成度、商品必须是主角"
AI_VIDEO_RHYTHM_RULE = """【AI 视频节奏执行规则】
- 禁止使用“停住 / 停半拍 / 定格 / 静止 / 停留1秒 / 最后1秒轻停 / 站定不动 / 保持不动 / 完全静止”等停顿、冻结、卡帧类表达；
- 不要要求“最后停 0.5 秒 / 1 秒”，收尾必须保持自然微动态；
- 如果需要让商品被看清，改用“镜头保持稳定、构图保持清楚、人物有极轻微目光移动、头部仅做 1–3 度微动、耳饰保持清晰可见、商品在连续轻微动态中被看清”等表达；
- 所有分镜动作都应是稳定构图下的轻微连续动态，避免让 AI 视频模型理解成画面冻结。"""
AUDIO_LAYER_RULE = """【音频层设计】
- 请在正式脚本输出中新增 audio_layer 字段，用于后期剪辑或自动化音频处理；
- audio_layer 只作为后期增强层，不改变画面脚本主结构；
- 如果 script_brief.category_execution_contract.audio_policy 存在，audio_layer 必须优先服从该 audio_policy；
- BGM 必须低存在感，不盖过口播；bgm_energy 只允许 low / medium，默认 low；
- 每条视频最多设计 1–3 个关键 SFX，不要每个镜头都加；
- 发饰类音频只做轻辅助：开头可按首镜类型选择 subtle_pop / very_light_room_tone_lift / clean_tick，但不要强行每条都加；不使用明显“柔和闪光提示音”；
- 发饰类只有画面明确出现夹口闭合 / 夹住头发 / 开合动作时，才可使用 soft_click / clean_clip_click / light_snap；没有明确夹合动作时，不要强行使用 soft_click；
- 发饰类头发轻动、轻侧头、结果镜头可使用 very_light_hair_rustle / soft_brush / subtle_room_tone，服务固定感和操作真实感；
- 发饰类 BGM 默认轻快、清爽、生活化，不压口播、不过强鼓点、不做强情绪 EDM；mix_note 必须提醒 SFX 不盖过口播，ASMR 类音效只做点缀，画面动作弱时 SFX 也要克制；
- 耳饰类只允许非常轻的 soft_chime / subtle_room_tone 点缀，避免明显闪光提示音和廉价闪光感；
- 女装类可使用 fabric_swipe / light_transition / subtle_room_tone，服务换镜和穿搭结果；
- 冬季围巾 / 帽子类 BGM 默认 low 或 medium-low，口播优先；围巾只可少量使用 very_light_fabric_rustle，帽子通常只需 very_light_room_tone_lift 或不用 SFX；围巾帽子套装以低存在感生活化 BGM 为主；
- 冬季围巾 / 帽子类禁止夸张风声、大片 whoosh、闪光音、强冷风音效、帽子飞走感，也不要用音效暗示强防风或强保暖效果；
- general_accessory 默认少用音效，只允许 light_tap / subtle_chime / subtle_room_tone；
- 所有 SFX 必须与画面动作匹配，不能为了热闹乱加；
- voiceover_priority 固定为 high，BGM 与 SFX 不得盖过口播；
- audio_negative_constraints 默认包含：不要夸张闪光音、不要游戏音效、不要过强鼓点、不要盖住口播、不要廉价 bling 效果、不要恐怖 / 悬疑 / 过度戏剧感。"""
P7_AUDIO_LAYER_RULE = """【P7 音频执行边界】
P7 不需要输出 audio_layer；代码侧会本地补齐低存在感 BGM、voiceover_priority=high 和空/少量安全 SFX。
画面和口播不得暗示 category_execution_contract.audio_policy 禁止的动作或音效；没有明确夹合动作时，不写 click 类表达。"""
CATEGORY_EXECUTION_CONTRACT_ANCHOR_RULE = """【category_execution_contract 生成要求】
请基于产品图片和 product_type，在产品锚点卡中新增 category_execution_contract。
P1 是唯一的类目使用契约推断层，负责根据产品图和 product_type 生成 category_execution_contract；后续阶段只继承，不二次推断类目逻辑。

该 contract 用于统一约束后续 P4/P5/P7/Q1：
1. 商品应该怎么用；
2. 用在哪里；
3. 最重要的视觉结果是什么；
4. 哪些动作适合 AI 视频生成；
5. 哪些动作或表现应该禁止；
6. BGM / SFX 应该如何配合。

字段要求：
- display_family：ear_accessory / hair_accessory / apparel / apparel_accessory / general_accessory；
- product_subtype：发饰类优先使用 scrunchie / small_side_clip / claw_clip / headband / hair_tie / hair_band / styling_tool / other_hair_accessory / unknown；冬季服饰配件首期只使用 scarf / hat / scarf_hat_set / unknown；
- use_case：发饰可用 low_ponytail / low_bun / loose_bun / half_up / bun_area / face_side_fix / back_head_fix / top_head_wear / ponytail_or_bun_uncertain / unknown；冬季围巾帽子可用 winter_outing / winter_commute / winter_travel / cold_weather_outfit / photo_outfit / before_going_out / daily_commute / unknown；
- placement_zone：发饰可用 face_side / back_head / top_head / low_ponytail / half_up / bun_area / full_head / unknown；冬季围巾帽子可用 neck_shoulder / head / head_face / upper_body / scarf_hat_combo / unknown；
- hold_scope：发饰可用 flyaway_hair / small_hair_section / half_hair / low_ponytail / bun / decorative_only / unknown；冬季围巾帽子可用 upper_body_styling / face_frame / warmth_visual_coverage / winter_outfit_completion / decorative_outfit / unknown，其中 warmth_visual_coverage 只表示视觉覆盖感，不是功效承诺；
- orientation：发饰可用 horizontal_clip / vertical_clip / wrap_around / tie_up / insert_fix / wear_on_head / unknown；冬季围巾帽子可用 wrapped_neck / draped_shoulder / worn_on_head / front_brim / full_set_wear / unknown；
- operation_policy：result_first_process_avoid / process_allowed_once / process_forbidden / static_result_only；
- primary_visual_result 必须用中文写清“这款商品最应该被视频证明的视觉结果”，不要只写更好看/更精致/更有气质；
- safe_shot_templates 输出 3-4 个低风险镜头模板；
- forbidden_actions 输出 3-4 个该商品不适合 AI 视频生成的动作或错误表现；
- result_priority 说明脚本优先证明什么结果；
- audio_policy 必须包含 bgm_style / bgm_energy / voiceover_priority / sfx_policy / allowed_sfx / forbidden_sfx / sfx_timing_rules / audio_negative_constraints。
- field_confidence 必须为 product_subtype / use_case / placement_zone / hold_scope / orientation / primary_visual_result / operation_policy 输出 high / medium / low；不确定时允许 low，不得为了完整性硬猜。
- 如果 display_family = apparel_accessory，必须额外输出 season_context / hat_risk_tier / set_relationship / co_styling_hint。

【apparel_accessory 冬季围巾 / 帽子 contract 规则】
- 首期只在 product_type 明确为围巾 / 帽子 / 围巾帽子套装时启用 apparel_accessory，不影响发饰、耳环、女装；
- season_context.primary_season 可为 winter / summer / shoulder_season / year_round / unknown，weather_signal 可为 cold / hot / mild / rainy / sunny / unknown；
- 冬季围巾、冬季帽子、围巾帽子套装默认优先 season_context = winter + cold；如果产品图明显不是冬季款，允许 unknown / summer / shoulder_season，不得硬猜；
- hat_risk_tier：low_risk 适合棒球帽、针织帽等结构清楚基础帽；medium_risk 适合渔夫帽、软檐帽；high_risk 适合宽檐帽、贝雷帽、护耳帽、复杂绒边或耳罩帽型；非帽子输出 unknown；
- set_relationship：same_color / matching_color / mix_match / unknown，仅 scarf_hat_set 重点填写；
- co_styling_hint.pair_with 首期优先 winter_coat / knit_sweater，必要时可补 wool_coat / basic_turtleneck，但不要把围巾帽子孤立展示；
- primary_visual_result 必须围绕冬季出门 / 冬季旅行 / 冬季穿搭完整度 / 围巾帽子与外套协同；
- 禁止编造材质、保暖等级、防晒等级、防风、防水、极寒适用等功效承诺；
- 围巾 proof 重点是脖颈、肩部、上半身和外套/针织衫关系更完整，不是复杂围法教程；
- 帽子 proof 重点是帽型、脸部轮廓、头肩比例和冬季穿搭关系，不是强功能测试；
- 围巾帽子套装 proof 重点是套装让冬季上半身更完整，不分别做两个单品功能测试。

【contract 字段优先级】
下游执行时按以下优先级理解 contract：
1. primary_visual_result
2. result_priority
3. operation_policy
4. use_case
5. placement_zone
6. hold_scope
7. orientation
8. safe_shot_templates
9. forbidden_actions
10. audio_policy

说明：
- 先看最终要证明什么视觉结果；
- 再看过程能不能拍；
- 再看使用场景和位置；
- 最后才看方向和动作细节；
- 不允许因为 orientation = wrap_around，就违反 operation_policy 去生成完整环绕过程。

注意：
- 这些字段只服务脚本生成和视频还原；
- 不是商品真实参数；
- 不要编造材质、重量、精确尺寸、品牌、功效；
- 不确定时允许输出 unknown；
- 但不要把 hair_accessory 全部默认写成小发夹 / 小抓夹 / 脸侧碎发 / 低马尾；
- 如果产品图明显是发髻 / 盘发 / 低髻外侧使用，不能默认写成 low_ponytail。"""
CATEGORY_EXECUTION_CONTRACT_STRATEGY_RULE = """【category_execution_contract 使用规则】
P4 / P5 必须继承 anchor_card_json.category_execution_contract。
四条脚本的 script_role 和 primary_focus 必须基于 category_execution_contract 动态生成。
P4 / P5 是策略分配层，只能继承 contract 来生成 script_role / primary_focus / secondary_focus，不得根据 product_type 二次推断使用场景。

禁止无视 contract，把所有 hair_accessory 都写成：
- 小发夹
- 小抓夹
- 脸侧碎发整理
- 一小束头发
- 低马尾根部

如果 category_execution_contract 与 P4/P5 生成内容冲突，以 category_execution_contract 为准。
如果 contract 指向 low_bun / loose_bun / bun_area，则脚本主线不得写成“低马尾根部更集中”，应围绕“低髻 / 松散盘发更柔和、更完整”展开。
如果 contract 指向 display_family = apparel_accessory，则策略主线必须围绕冬季出门 / 冬季旅行 / 冬季穿搭完整度 / 围巾帽子与外套或针织衫协同；不得退化成普通美女戴帽或围巾微笑展示。
如果 product_subtype = scarf / hat / scarf_hat_set，则 primary_focus 必须动态体现围巾、帽型或套装搭配关系，不得把围巾帽子当成普通首饰或发饰。
不得无视 contract 把 scrunchie 写成低马尾，把发箍写成发夹，把大抓夹写成小边夹。
如果发现 contract 可能不合理，只能输出 contract_conflict_warning，不得擅自改写 contract。"""
CATEGORY_EXECUTION_CONTRACT_SCRIPT_RULE = """【category_execution_contract 使用规则】
你必须先读取 script_brief.category_execution_contract，再生成脚本。
P7 是脚本执行层，必须执行 contract；不得重新发明使用场景、佩戴区域、动作策略。

必须遵守：
1. primary_visual_result；
2. result_priority；
3. operation_policy；
4. use_case；
5. placement_zone；
6. hold_scope；
7. orientation；
8. safe_shot_templates；
9. forbidden_actions；
10. audio_policy。

字段优先级按上述顺序执行；不允许因为 orientation = wrap_around，就违反 operation_policy 去生成完整环绕过程。
如果脚本创意与 category_execution_contract 冲突，以 category_execution_contract 为准。
如果发现 contract 可能不合理，只能输出 contract_conflict_warning，不得擅自改写 contract。

【operation_policy 执行规则】
如果 operation_policy = result_first_process_avoid：
- 首镜优先结果先给；
- 可有 0-1 个轻量准备或结构展示动作；
- 不允许完整佩戴 / 套入 / 环绕 / 夹入过程成为核心 proof；
- proof 优先使用结果镜头、静物细节、轻微动态复核。

如果 operation_policy = process_forbidden：
- 不出现任何完整佩戴 / 套入 / 夹入 / 扎发过程；
- 只能拍已完成结果、产品细节、轻微动态；
- 如果需要解释操作，用口播或静物镜头代替。

如果 operation_policy = process_allowed_once：
- 每组 4 条脚本中最多 1 条可出现一个低风险过程镜头；
- 过程镜头必须短促、清楚；
- 过程只服务结果证明，不得拖成教程；
- 过程后必须快速回到已完成结果。

如果 operation_policy = static_result_only：
- 不依赖动作 proof；
- 只通过静态结果、角度变化、局部细节、微动态证明；
- 禁止过程镜头。

【audio_policy 使用规则】
你必须继承 category_execution_contract.audio_policy 生成 audio_layer。
要求：
1. audio_layer 必须服从 audio_policy；
2. 不得生成 forbidden_sfx；
3. 只有满足 sfx_timing_rules 的画面，才能使用对应 SFX；
4. 如果画面没有明确夹合动作，不得使用 soft_click / clean_clip_click；
5. 如果 operation_policy = result_first_process_avoid，audio_layer 不得强化完整操作过程；
6. BGM 必须符合 bgm_style / bgm_energy；
7. 口播优先级最高，BGM 和 SFX 不得盖过口播。

对于 result_first_process_avoid 的 scrunchie / 发圈类，P7 应优先使用：
结果先给 → 产品细节 → 已佩戴结果复核 → 整体完成度

不要使用：
结果先给 → 回到未戴 → 完整套入 → 戴好结果

【apparel_accessory_winter_outfit_check_mode】
启用条件：script_brief.category_execution_contract.display_family = apparel_accessory 且 season_context.primary_season = winter。
核心规则：
1. 前 3 秒优先展示已佩戴结果；
2. 镜头必须看到配饰和上半身 / 头肩 / 脸部比例关系；
3. 优先使用 co_styling_hint.pair_with 中的 winter_coat / knit_sweater；
4. 不展示复杂围法、复杂戴帽教程；
5. 人物状态是镜前冬季出门自检，不是主播讲解；
6. 场景优先：玄关镜前、卧室镜前、衣柜旁、出门前门口；
7. 不默认生成夸张雪景大片、强风户外、棚拍广告感；
8. 不得承诺强保暖 / 防寒 / 防风 / 防晒 / 防水 / 极寒适用。

默认结构：
- scarf：0-3s 已围好结果，胸口以上或上半身中近景，围巾和肩颈 / 外套关系清楚；3-6s 手轻整理围巾边缘；6-10s 半步后退看镜中上半身整体；10-12s 局部细节 proof；12-15s 出门前确认，轻分享收尾。
- hat：0-3s 已戴好结果，帽型、脸部比例和头肩关系清楚；3-6s 手轻压帽檐或整理帽边；6-10s 侧脸或轻转头展示帽型；10-12s 半步后退看帽子和冬季外套 / 针织衫整体关系；12-15s 冬季出门 / 旅行 / 通勤场景轻收尾。
- scarf_hat_set：0-3s 围巾帽子已一起佩戴，上半身冬季穿搭完整；3-6s 轻整理围巾边缘或帽檐，避免复杂过程；6-10s 半步后退看镜中整体；10-12s 局部细节 proof，带到围巾质感和帽型；12-15s 冬季旅行 / 出门 / 拍照场景轻收尾。

proof_path 推荐：
- scarf：优先 A_result_detail_only / B_result_with_light_compare；避免 C / D，除非只是非常轻的整理围巾边缘。
- hat：hat_risk_tier = low_risk 可用 C_result_with_short_process；medium_risk 优先 A / B；high_risk 只用 A 或 static_result_only。
- scarf_hat_set：首期优先 A_result_detail_only；same_color / matching_color 可增加轻微搭配细节 proof；mix_match 只展示当前这一套组合成立，不做多色托盘镜。

set_relationship 处理：
- same_color：重点拍同色一体感，围巾和帽子的颜色、材质、冬季氛围一致；
- matching_color：重点拍配色搭配，围巾、帽子、外套或针织衫之间颜色协调；
- mix_match：只展示当前组合成立，不做多个颜色快速切换，不让脚本变成组合教程。"""
CATEGORY_EXECUTION_CONTRACT_QC_RULE = """【category_execution_contract 检查】
Q1 不新增复杂检查项，只检查脚本是否违反 category_execution_contract。
Q1 是契约一致性检查层，只检查脚本是否违反 contract，不重新推断类目逻辑。

检查：
1. use_case 是否被改错；
2. placement_zone / hold_scope / orientation 是否被错误使用；
3. primary_visual_result 是否被偏离；
4. operation_policy 是否被违反；
5. safe_shot_templates 是否基本被继承；
6. forbidden_actions 是否出现；
7. result_priority 是否被偏离；
8. audio_layer 是否违反 audio_policy；
9. 脚本主线是否与 contract 冲突。

field_confidence 处理规则：
- high confidence 被违反：进入 major_issues，并做最小修正；
- medium confidence 被违反：进入 minor_issues / suggest_fix，并给出最小修正建议；
- low confidence 被违反：不强拦，只记录参考。

字段优先级：
1. primary_visual_result
2. result_priority
3. operation_policy
4. use_case
5. placement_zone
6. hold_scope
7. orientation
8. safe_shot_templates
9. forbidden_actions
10. audio_policy

如果发现 contract 可能不合理，只能输出 contract_conflict_warning，不得擅自改写 contract。

【audio_policy 检查】
检查 audio_layer 是否违反 category_execution_contract.audio_policy：
1. 是否使用 forbidden_sfx；
2. SFX 是否和画面动作匹配；
3. 是否在没有夹合动作时使用 soft_click / clean_clip_click；
4. BGM 是否可能盖过口播；
5. SFX 是否过多；
6. 是否用音效暗示不存在的动作；
7. 是否违背 operation_policy。

处理：
- 使用 forbidden_sfx 或动作不匹配，记为 major_issues，并最小修正；
- SFX 过多、BGM 略强，记为 minor_issues。

【apparel_accessory contract 检查】
如果 display_family = apparel_accessory：
- 检查 season_context 是否被执行；冬季款不得写成夏季或泛泛无季节日常；
- 检查 product_subtype = scarf / hat / scarf_hat_set 是否被误写成发饰、首饰或普通服装；
- 检查 hat_risk_tier 是否约束了帽子过程；medium/high risk 不得展示复杂戴帽、强风或甩头测试；
- 检查 set_relationship 是否与画面一致；same_color 不得生成不相关颜色，mix_match 不得做复杂多色组合教程；
- 检查 co_styling_hint 是否基本使用；冬季围巾帽子应带到 winter_coat / knit_sweater 或同等冬季上半身搭配；
- 检查是否出现强保暖 / 防寒 / 防风 / 防晒 / 防水等功效承诺，出现则 major_issues。"""
HUMAN_PERFORMANCE_CONTRACT_RULE = """【human_performance_contract 生成要求】
P3 需要根据 performance_profile_json 生成轻量 human_performance_contract。
如果 performance_profile_json.enable_human_performance_contract = true，输出必须包含 human_performance_contract。
如果未启用，human_performance_contract 可输出为空对象，并回退到原有人物 / 穿搭 / 情绪逻辑。

P3 必须遵守：
- P3 只负责人物如何观察、反应、移动、分享；
- P3 不得重新判断商品怎么用；
- P3 不得覆盖 P1 category_execution_contract；
- P3 不得改变 product_subtype / use_case / placement_zone / hold_scope / orientation / operation_policy / primary_visual_result；
- P3 不得引入 P1 forbidden_actions；
- 人物表演不能覆盖商品使用契约，不能把商品 proof 主线改成人物表演主线。

human_performance_contract 字段：
- performance_family：人物表演大类，例如 mirror_hair_check / face_side_detail_check / outfit_fit_check；
- persona_mode：人物状态模式，例如镜前自检、朋友式分享、出门前整理；
- expression_arc：15 秒内表情变化，从观察到满意，再到分享或确认；
- gaze_plan：弹性眼神路径，不是必须逐字逐项按顺序执行；
- gaze_rule：至少包含 min_points_required 和 final_point_options；
- micro_reaction_beats：可用微反应池；
- body_language_beats：可用身体语言池；
- product_interaction_beats：人物与商品的低风险互动方式；
- relatable_moment：人物动机，只指导 voiceover / opening context / performance motivation，不得扩写成复杂剧情；
- performance_intensity：人物表演强度；
- forbidden_performance：禁止的人物表现；
- active_micro_reaction_limit：由 performance_intensity 转换出的全片主动微反应数量上限。

performance_intensity 执行规则：
- low：active_micro_reaction_limit = 2；全片最多 2 个主动微反应；不允许夸张惊喜、明显挑眉、明显大笑；眼神变化和身体微动为主；
- low_to_medium：active_micro_reaction_limit = 3；全片最多 3 个主动微反应；允许一次轻微满意反应，例如嘴角上扬、轻点头；允许一次短暂看镜头分享；
- medium：active_micro_reaction_limit = 4；全片最多 4 个主动微反应；可加入更明确的确认动作，例如半步后退、轻转身、整理衣摆后点头；不允许夸张表演或硬广。

gaze_plan 执行规则：
- gaze_plan 是弹性眼神路径，不是必须逐字逐项按顺序执行；
- P7 必须在全片中至少使用 gaze_rule.min_points_required 个 gaze point；
- 默认 min_points_required = 3；
- 最后一个 gaze point 必须落在 gaze_rule.final_point_options 之一；
- 不得全片只看同一个方向。

apparel_accessory 规则：
- 如果 performance_profile_json.display_family = apparel_accessory，人物动机必须是冬季出门前镜前自检；
- scarf_variant 重点使用轻整理围巾边缘、看围巾和外套/针织衫关系、半步后退看上半身整体；
- hat_variant 重点使用轻压帽檐/整理帽边、轻转头展示帽型、看帽子和冬季上衣/外套关系；
- scarf_hat_set_variant 重点使用轻整理其中一处、帽子和围巾同时清楚可见、看整套上半身关系；
- 人物表演不能抢商品，不能变成只拍脸或美女微笑展示。"""
HUMAN_PERFORMANCE_SCRIPT_RULE = """【P7 内部两步生成规则】
P7 必须先内部完成 Step 1，再完成 Step 2：

Step 1：生成 shot_skeleton，只决定 shot_index / time_range / role / shot_purpose / proof_path。
- time_range 必须连续、不得重叠、不得缺秒，总时长必须等于 15 秒；
- 默认可使用 6 镜头结构：0-2.5s / 2.5-4.5s / 4.5-6.5s / 6.5-9.5s / 9.5-12s / 12-15s；
- 或使用 0-3s / 3-5s / 5-7s / 7-10s / 10-12.5s / 12.5-15s；
- 每个 shot 必须有 hook / proof / decision / transition 等角色；
- 不得先写完整 storyboard 再随手补时间。

Step 2：基于 shot_skeleton 填充 visual / action / performance / voiceover / subtitle / audio / constraints。

【proof_path 选择规则】
P7 在生成 shot_skeleton 时，必须先确定 proof_path：
- A_result_detail_only：已戴/已夹结果 → 商品细节 → 结果复核 → 整体完成度 → 轻决策；不回到未戴/未夹/未穿状态，不展示佩戴过程；
- B_result_with_light_compare：已戴/已夹结果 → 一次同角度轻对比 → 已戴/已夹结果复核 → 整体完成度 → 轻决策；未戴状态只出现 1 次，总时长 ≤ 25%，不再出现过程镜头；
- C_result_with_short_process：已戴/已夹结果 → 商品细节 → 一次短促靠近/闭合暗示 → 已戴/已夹结果复核 → 整体完成度 → 轻决策；过程短促，不展开完整佩戴/夹合/盘发过程，不再做前后对比；
- D_result_with_light_compare_and_short_process：谨慎使用；已戴/已夹结果 → 一次轻对比 → 一次短促过程 transition → 已戴/已夹结果复核 → 整体完成度 → 轻决策；对比证明变化，过程只作为 transition，不重复证明同一件事。

【时间线一致性规则】
- 在 result_first 类脚本中，已展示结果后，未戴 / 未夹 / 未穿状态最多出现 1 次；
- 未戴 / 未夹 / 未穿状态总时长不得超过全片 25%；
- 同一证明逻辑不得在不同镜头重复表达；
- 不得在结果、未戴、结果、未戴之间反复跳转；
- 可以出现“轻对比 + 一次短促过程”，但必须分工明确、时长受控、动作低风险。

【抓夹/发夹短促过程降级规则】
如果使用短促过程，必须写成低风险动作：
- 手持抓夹从发髻外侧靠近，夹齿只短暂打开；
- 闭合动作只出现一瞬；
- 下一帧立即切到已夹好结果；
- 手快速离开画面；
- 不展示夹齿如何深入头发，不展示完整夹发过程。

禁止写法：
- 扣住头发与发髻边缘；
- 夹住大量头发；
- 完整夹入；
- 反复调整；
- 手、头发、抓夹长时间纠缠；
- 展示完整盘发或固定教程。

【human_performance_contract 使用规则】
P7 必须读取 script_brief.human_performance_contract。
human_performance_contract 是人物表演软增强，category_execution_contract 是商品执行硬约束；两者冲突时永远以 category_execution_contract 为准。

每个 storyboard 镜头必须新增 performance 字段。performance 必须是对象，包含：
- gaze：每个镜头必须有，说明人物看哪里；
- expression_or_micro_reaction：至少覆盖 50% 镜头；
- body_language：每个镜头建议保留，允许简短；
- product_interaction：如有商品互动，必须低风险且不得违反 category_execution_contract。

P7 内部必须执行：
- 每个镜头至少分配一个 gaze；
- 至少 50% 镜头分配 expression_or_micro_reaction；
- 全片至少使用 gaze_plan 中 3 个 gaze point；
- 最后一个 gaze point 必须落在 camera 或整体确认点；
- active_micro_reaction 数量不得超过 active_micro_reaction_limit；
- product_interaction 不得违反 category_execution_contract；
- performance 不得只写身体动作或位置。

禁止把 performance 写成泛化模板：
- 自然微笑
- 亲和展示
- 开心看镜头
- 女生对镜微笑
- 人物自然展示商品
- 人物轻微侧头
- 人物站在镜前

必须写成类似：
- 人物先看镜子里的发髻，轻抿嘴观察珠花是否夸张；
- 视线从镜中整体移到发饰位置，眼神轻微变亮；
- 短暂看向镜头，嘴角自然上扬，像朋友轻分享；
- 回到镜中整体，轻点头确认可以出门。

P7 必须遵守：
- active_micro_reaction_limit；
- gaze_rule.min_points_required；
- 最后一个 gaze point 落在 gaze_rule.final_point_options 之一；
- 不得出现 forbidden_performance；
- 不得为了人物更自然而改变 use_case / placement_zone / operation_policy / primary_visual_result；
- 不得引入 category_execution_contract.forbidden_actions。

【performance_strategy 规则】
P7 在生成 storyboard 前，必须根据 script_role / S1-S4 生成 performance_strategy，并让它影响 gaze 分布、micro reaction 强度、camera gaze 频率、decision 表情位置。
- S1 强停留原生型：更像随手记录；gaze 更偏 mirror / product_position；camera gaze 少，只在中后段短暂出现；micro reaction 更轻，偏观察、抿嘴、轻微满意；不强调强决策表情；
- S2 平衡型：观察与分享平衡；前半段 mirror / product_position；中后段至少一次 camera gaze；至少一次嘴角自然上扬或轻点头；结尾有轻分享感；
- S3 强购买承接型：更强调确认问题被解决；decision 反应提前；micro reaction 可以更明确，例如轻点头、眼神放松；gaze 从问题位置转向整体确认；但不得硬推销；
- S4 高惊艳首镜型：0-3s 首镜表情反应更明显；允许一次眼神轻微变亮；但不能夸张惊喜或主播式表演；后续快速回到自然观察和轻分享。

验收要求：S1-S4 不得只在卖点上不同；人物 gaze 分布、micro reaction 强度、camera gaze 频率、decision 表情位置也要有轻微差异。"""
P7_CATEGORY_EXECUTION_CONTRACT_RULE = """【category_execution_contract P7 精简执行规则】
- 先读 script_brief.category_execution_contract；它是商品硬约束，冲突时高于人物表演和创意。
- 字段优先级：primary_visual_result > result_priority > operation_policy > use_case > placement_zone > hold_scope > orientation > safe_shot_templates > forbidden_actions > audio_policy。
- 必须执行 primary_visual_result / operation_policy / placement_zone / forbidden_actions；不得重新推断商品怎么用。
- 不允许因为 orientation = wrap_around 就生成完整环绕过程。
- operation_policy=result_first_process_avoid：结果先给；0-1 个轻量结构展示；不拍完整佩戴/套入/环绕/夹入过程作为 proof。
- operation_policy=process_allowed_once：最多一个短促低风险过程镜头；过程只服务结果，之后快速回到已完成结果。
- operation_policy=process_forbidden/static_result_only：不拍完整过程，只用结果、细节、角度变化、轻微动态。
- audio_layer 必须服从 audio_policy；不得使用 forbidden_sfx；没有明确夹合动作时，不得使用 soft_click / clean_clip_click；BGM/SFX 不盖过口播。
- apparel_accessory 冬季模式：保留冬季出门/旅行/上半身搭配；用 winter_coat/knit_sweater；不做复杂围法/戴帽教程；不承诺强保暖/防风/防寒/防晒。
- 如发现 contract 可能不合理，只输出 contract_conflict_warning，不得擅自改写 contract。"""
P7_HUMAN_PERFORMANCE_SCRIPT_RULE = """【human_performance_contract P7 精简执行规则】
- human_performance_contract 是人物表演软增强，不得覆盖 category_execution_contract。
- Step 1：生成 shot_skeleton 时优先继承 p7_execution_template；Step 2 再填充 visual / action / performance / voiceover / audio。
- 每个 storyboard 镜头都必须有 performance；performance 必须是对象，包含 gaze / expression_or_micro_reaction / body_language / product_interaction。
- gaze 每镜头必须有；全片至少使用 gaze_plan 中 3 个 gaze point；最后一个 gaze point 落在 gaze_rule.final_point_options 之一。
- expression_or_micro_reaction 至少覆盖 50% 镜头；active micro reaction 不得超过 active_micro_reaction_limit。
- product_interaction 必须低风险，不得引入 forbidden_actions 或复杂佩戴过程。
- 禁止 performance 写成“自然微笑 / 亲和展示 / 开心看镜头 / 人物自然展示商品 / 女生对镜微笑”。
- S1：更生活化，camera gaze 少，微反应轻；S2：观察和分享平衡；S3：更强调确认问题解决但不硬推销；S4：首镜反应更明显但不夸张。
- S1-S4 不得只在卖点上不同，gaze 分布、微反应强度、camera gaze 频率、decision 表情位置也要有差异。"""
HUMAN_STIFFNESS_QC_RULE = """【human_stiffness_check】
Q1 必须逐镜头检查最终 storyboard.performance，不要只看全局【情绪】字段。
如果镜头没有 performance 字段，视为 human_performance_contract 未落地。

【timing_consistency_check】
- 若镜头时间总和不等于 15 秒，进入 major_issues；
- 若时间段缺失、重叠、不连续，进入 major_issues。

【timeline_consistency_check】
- 若 result_first 类脚本中，已展示结果后未戴 / 未夹 / 未穿状态出现超过 1 次，进入 major_issues；
- 若未戴 / 未夹 / 未穿状态总时长超过 25%，进入 major_issues；
- 若同一证明逻辑在多个镜头重复表达，进入 minor_issues；严重重复则 major_issues；
- 若脚本在结果、未戴、结果、未戴之间反复跳转，进入 major_issues；
- 若对比 + 过程共存，但分别承担不同证明任务且时长受控，可以 PASS。

【ai_shot_risk_check 强化】
对于过程镜头：
- 如果描述夹齿深入头发、扣住发髻边缘、反复夹发，判定 high_risk；
- 若 operation_policy 不允许复杂过程，则进入 major_issues；
- 修正时替换为“靠近 + 一瞬闭合 + 切已夹好结果”。

【human_stiffness_check】
Q1 人物僵硬检查收敛成 3 项，不做复杂审美判断：

1. emotion_flatness_check：
统计包含 expression_or_micro_reaction 的镜头数量。
- 若少于总镜头数 50%，fail；
- 若连续 3 个镜头没有表情 / 微反应变化，fail；
- 失败例：每个镜头都是“女生自然微笑展示商品”。

2. gaze_monotony_check：
统计 gaze point。
- 若全片 gaze point 少于 3 个，fail；
- 若全片基本只看同一个方向，fail；
- 若最后一个 gaze point 不在 camera 或整体确认点，fail；
- 失败例：四个镜头全部是人物看镜头微笑。

3. category_interaction_missing_check：
检查是否缺少该 display_family 对应的真实互动动作。第一阶段重点检查 hair_accessory。
- hair_accessory：至少出现镜前确认 / 看发饰位置 / 轻整理发型 / 看整体效果中的 1-2 个；
- ear_accessory：至少出现半侧脸 / 拨头发 / 轻转头 / 耳侧细节露出中的 1-2 个；
- apparel：至少出现全身镜 / 整理衣摆或肩线 / 半步后退 / 轻转身中的 1-2 个。
- apparel_accessory：至少出现镜前上半身确认 / 看围巾或帽子位置 / 轻整理围巾边缘或帽檐 / 半步后退看外套或针织衫搭配关系中的 1-2 个。

【apparel_accessory_winter_check】
如果 display_family = apparel_accessory，采用 P0 / P1 / P2 分级验收。

P0 合规线，必须 100% 通过；违反进入 major_issues：
1. 不得出现强保暖 / 防寒 / 防风 / 防晒 / 防水功效承诺；
2. 不得出现复杂围法、复杂打结、复杂戴帽教程；
3. 不得退化成“美女戴帽展示”或“美女围围巾微笑展示”；
4. 不得商品关系不清，看不出围巾 / 帽子 / 套装怎么佩戴；
5. 不得出现明显帽型变形、围巾穿模、遮住大半张脸；
6. 不得违反 set_relationship，例如同色套装被生成成完全不相关颜色。

P1 可投放线，明显缺失时按严重程度进入 major_issues 或 minor_issues：
1. 前 3 秒能看到已佩戴效果；
2. 围巾 / 帽子和上半身、头肩或脸部比例关系清楚；
3. 有镜前出门自检感；
4. 季节场景与 season_context 一致；
5. co_styling_hint 被基本使用，例如冬季外套 / 针织衫出现。

P2 优化线，不一定单条强拦，但用于 suggest_fix：
1. 局部细节 proof 完整；
2. 冬季旅行 / 通勤 / 出门 / 拍照场景明确；
3. 人物不抢商品；
4. 搭配关系成立；
5. 口播不像硬广，像朋友式轻分享；
6. 音效不抢口播，不制造廉价广告感。

类目专项：
- scarf：看清围巾和脖颈 / 肩部 / 上衣 / 外套关系；避免复杂打结和多圈绕脖教程；有轻整理边缘、垂感或上半身整体 proof；避免遮住大半张脸；没有强保暖 / 防寒功效承诺。
- hat：看清帽型、帽檐和脸部 / 头肩比例；根据 hat_risk_tier 控制过程；避免帽檐变形、遮住整张脸；避免大幅甩头和强风测试；没有强防风 / 保暖 / 防晒功效承诺。
- scarf_hat_set：帽子和围巾同时清楚可见；set_relationship 与画面一致；带到冬季外套 / 针织衫；避免同时复杂操作两件商品；证明上半身冬季穿搭完整度，而不是分散成两个独立单品测试。

判定标准：
- 命中 2 项及以上：major_issues；
- 命中 1 项：minor_issues / suggest_fix；
- 命中 0 项：PASS。

修正规则：
- 只补 performance beat / gaze / micro_reaction；
- 不得重写商品策略；
- 不得改 P1 category_execution_contract；
- 不得改变商品 proof 主线；
- 不得引入 forbidden_actions。"""
CONTROL_LAYER_LANGUAGE_RULE = (
    "统一规则：本流程中，所有策略说明、角色分类、质检说明、执行约束、字段描述等控制性文本必须使用中文；"
    "字幕、口播、本地化表达等面向最终用户的内容层文本使用 target_language；"
    "工程枚举值可保留英文 key，但面向模型的自然语言解释必须中文。"
)
TYPE_GUARD_FAMILY_LABELS = {
    "apparel": "服装",
    "jewelry": "首饰",
    "hair_accessory": "发饰",
    "apparel_accessory": "服饰配件",
    "accessory": "配饰",
    "unknown": "未知",
}

PRODUCT_SELLING_NOTE_RULES = """“产品卖点说明”使用规则：
- 若 product_selling_note 为空，则禁止仅凭图片主动推断设计来源、寓意、宗教、民俗或功效含义
- 若 product_selling_note 为空，只允许围绕外观结构、佩戴/上身结果、风格气质、用户可感知价值来写
- 若 product_selling_note 不为空，可把其中内容当作卖点背景、设计灵感、轻寓意、送礼表达或表达限制的优先参考
- 涉及寓意时，只能写为设计灵感 / 好意头 / 轻寓意 / 祝福感，禁止升级为招财、转运、保平安、开运、灵验、带来结果等强承诺
- 若 product_selling_note 与商品实物明显冲突，以商品实物外观为准，不得硬写不成立的信息"""

PRODUCT_PARAMETER_INFO_RULES = """“产品参数信息”使用规则：
- 若 product_parameter_info 为空，则只能保留图片里直接可见、可读、可确认的参数事实
- 若 product_parameter_info 不为空，可把它视为人工确认的参数事实来源，优先写入 parameter_anchors
- 不得把 product_parameter_info 改写成更强结论，不得脑补图片里看不见的功效、材质背书或品牌承诺
- 若 product_parameter_info 与图片明显冲突，优先保留更稳妥、可确认的表达，不得硬写冲突事实"""

OPENING_FIXED_POOL = """opening_mode：
- 轻顾虑冲突型
- 轻判断型
- 结果先给型
- 高惊艳首镜型

proof_mode：
- 细节证明型
- 结果证明型
- 顾虑化解型
- 搭配成立型

ending_mode：
- 适合谁收尾
- 结果感收尾
- 场景代入收尾
- 顾虑化解收尾
- 轻安利收尾

scene_subspace：
- H1 窗边自然光
- H2 镜前 / 玄关镜前
- H3 梳妆台 / 桌边
- H4 床边 / 坐姿分享
- H5 衣柜 / 穿衣区

visual_entry_mode：
- V1 局部质感压镜型
- V2 上脸 / 上身结果先给型
- V3 动作进入型
- V4 高惊艳首镜型

persona_state：
- R1 轻分享型
- R2 小惊喜型
- R3 轻判断型
- R4 轻冷静型

action_entry_mode：
- A1 手部进入
- A2 转头 / 侧脸进入
- A3 佩戴动作进入
- A4 镜前整理进入
- A5 半步后退 / 整体结果进入

styling_completion_tag：
- 安静通勤感
- 干净日常感
- 柔和精致感
- 轻约会感
- 轻冷淡感

persona_visual_tone：
- 克制顺眼型
- 轻判断型
- 轻分享型
- 小惊喜型

styling_key_anchor（只允许输出 1 个）：
- 领口干净
- 肩线利落
- 面料柔和但不过软塌
- 头部区域清爽
- 耳侧区域无遮挡
- 发型边界清楚
- 上身轮廓干净
- 配色低对比但不发灰

emotion_arc_tag：
- 轻疑问 → 轻确认 → 轻安心
- 轻顾虑 → 轻被说服 → 轻满意
- 平静观察 → 小惊喜 → 满意确认
- 轻判断 → 轻发现 → 轻认同
- 平静进入 → 轻结果成立 → 轻推荐"""

PERSONA_LIGHT_CONTROL_RULES = """【新增规则：人物穿搭与情绪轻优化】
1. 人物穿搭不能只停留在“不要出错”，还要给出一个明确的完成度方向；
2. 必须为当前视频选择一个 styling_completion_tag；
3. 必须为当前人物选择一个 persona_visual_tone；
4. 必须选择一个 styling_key_anchor，作为这条视频里最关键的穿搭视觉锚点；
5. 必须选择一个 emotion_arc_tag，作为整条视频的人物轻情绪推进轨迹；
6. styling_completion_tag 决定穿搭整体感觉；
7. persona_visual_tone 决定人物视觉气质；
8. styling_key_anchor 决定这条视频里最关键的穿搭视觉点；
9. emotion_arc_tag 决定人物情绪如何从开头自然走到结尾；
10. 所有这些设定都必须服从商品优先原则，不得抢商品。"""

PERSONA_LIGHT_CONTROL_POOL = """styling_completion_tag：
- 安静通勤感
- 干净日常感
- 柔和精致感
- 轻约会感
- 轻冷淡感

persona_visual_tone：
- 克制顺眼型
- 轻判断型
- 轻分享型
- 小惊喜型

styling_key_anchor（只允许输出 1 个）：
- 领口干净
- 肩线利落
- 面料柔和但不过软塌
- 头部区域清爽
- 耳侧区域无遮挡
- 发型边界清楚
- 上身轮廓干净
- 配色低对比但不发灰

emotion_arc_tag：
- 轻疑问 → 轻确认 → 轻安心
- 轻顾虑 → 轻被说服 → 轻满意
- 平静观察 → 小惊喜 → 满意确认
- 轻判断 → 轻发现 → 轻认同
- 平静进入 → 轻结果成立 → 轻推荐"""

PERSONA_LIGHT_CONTROL_DEFAULTS = """默认分配建议：

S1 强停留原生型：
- styling_completion_tag：干净日常感 / 安静通勤感
- persona_visual_tone：轻分享型 / 克制顺眼型
- styling_key_anchor：头部区域清爽 / 领口干净
- emotion_arc_tag：轻疑问 → 轻确认 → 轻安心

S2 平衡型：
- styling_completion_tag：柔和精致感 / 安静通勤感
- persona_visual_tone：克制顺眼型 / 轻判断型
- styling_key_anchor：领口干净 / 上身轮廓干净
- emotion_arc_tag：平静观察 → 小惊喜 → 满意确认

S3 强购买承接型：
- styling_completion_tag：安静通勤感 / 轻冷淡感
- persona_visual_tone：轻判断型 / 克制顺眼型
- styling_key_anchor：耳侧区域无遮挡 / 肩线利落 / 发型边界清楚
- emotion_arc_tag：轻顾虑 → 轻被说服 → 轻满意

S4 高惊艳首镜型：
- styling_completion_tag：柔和精致感 / 轻约会感
- persona_visual_tone：小惊喜型 / 轻判断型
- styling_key_anchor：头部区域清爽 / 配色低对比但不发灰
- emotion_arc_tag：平静进入 → 轻结果成立 → 轻推荐"""

HAIR_ACCESSORY_KEYWORDS = {
    "发饰",
    "发夹",
    "抓夹",
    "边夹",
    "刘海夹",
    "香蕉夹",
    "竖夹",
    "鲨鱼夹",
    "发箍",
    "发圈",
    "发带",
    "发绳",
    "头绳",
    "头箍",
    "盘发",
}

EAR_ACCESSORY_KEYWORDS = {"耳线", "耳环", "耳饰", "耳钉", "耳夹", "耳坠"}
HAIR_CLIP_KEYWORDS = {"发夹", "抓夹", "边夹", "刘海夹", "香蕉夹", "竖夹", "鲨鱼夹"}
JEWELRY_ACCESSORY_KEYWORDS = {
    "配饰",
    "饰品",
    "首饰",
    "手圈",
    "手环",
    "手链",
    "手镯",
    "手串",
    "戒指",
    "项链",
    "吊坠",
    "脚链",
    "胸针",
    "bracelet",
    "bangle",
    "cuff",
    "ring",
    "necklace",
    "pendant",
    "anklet",
    "brooch",
}

PROMPT_PRODUCT_TYPE_GUARD = """你是产品类型视觉识别与冲突分析助手。

你的任务是：先只根据图片判断这个商品在视觉上最像什么类型，再输出标准化的图片侧类型判断结果。

注意：
1. 这一步先做图片视觉判断，不要被表格产品类型带偏；
2. 最终仲裁由代码侧完成；
3. 只能基于图片可见信息判断；
4. 如果图片信息不足，可以输出 unknown 并降低 confidence。

输入信息：
- table_product_type: {{table_product_type}}
- business_category: {{business_category}}

请输出合法 JSON：
{
  "vision_family": "apparel|jewelry|hair_accessory|apparel_accessory|accessory|unknown",
  "vision_slot": "body|upper_body|lower_body|full_body|wrist|neck|ear|finger|hair|neck_shoulder|head_face|upper_body_accessory|unknown",
  "vision_type": "",
  "vision_confidence": 0.0,
  "visible_evidence": [""],
  "risk_note": ""
}

规则：
- vision_type 只写图片最像的类型短词，例如：女装、轻上装、上衣、外套、连衣裙、下装、项链、项圈、手链、手镯、细手圈、戒指、耳饰、抓夹、发夹、发箍、发圈、扎发绳、发带、发簪、围巾、帽子、围巾帽子套装
- 如果无法判断到具体类型，可保留空字符串，但 family/slot 应尽量判断
- vision_confidence 必须输出 0 到 1 之间的小数
- visible_evidence 写 2-4 条图片可见依据
- 不要输出 markdown，不要输出解释文字。"""


def _normalized_product_family(product_type: str) -> str:
    text = str(product_type or "").strip()
    lowered = text.lower()
    if any(keyword in text for keyword in HAIR_ACCESSORY_KEYWORDS):
        return "发饰"
    if any(keyword in text for keyword in ("围巾", "帽子", "针织帽", "毛线帽", "渔夫帽", "贝雷帽", "护耳帽")):
        return "服饰配件"
    if any(keyword in text for keyword in EAR_ACCESSORY_KEYWORDS):
        return "耳饰"
    if any(keyword in text for keyword in JEWELRY_ACCESSORY_KEYWORDS if keyword.isascii() is False):
        return "首饰"
    if any(keyword in lowered for keyword in JEWELRY_ACCESSORY_KEYWORDS if keyword.isascii()):
        return "首饰"
    return "服装"


def _product_family_from_type_guard(product_type: str, type_guard_json: Optional[Dict[str, Any]] = None) -> str:
    if isinstance(type_guard_json, dict):
        family = str(type_guard_json.get("canonical_family", "") or "").strip()
        mapped = TYPE_GUARD_FAMILY_LABELS.get(family)
        if mapped:
            return mapped
    return _normalized_product_family(product_type)


def _build_type_guard_block(type_guard_json: Optional[Dict[str, Any]] = None) -> str:
    if not isinstance(type_guard_json, dict) or not type_guard_json:
        return ""

    prompt_contract = str(type_guard_json.get("prompt_contract", "") or "").strip()
    raw_product_type = str(type_guard_json.get("raw_product_type", "") or "").strip()
    display_type = str(type_guard_json.get("display_type", "") or "").strip()
    canonical_family = TYPE_GUARD_FAMILY_LABELS.get(
        str(type_guard_json.get("canonical_family", "") or "").strip(),
        str(type_guard_json.get("canonical_family", "") or "").strip(),
    )
    canonical_slot = str(type_guard_json.get("canonical_slot", "") or "").strip()
    conflict_level = str(type_guard_json.get("conflict_level", "") or "").strip()
    conflict_reason = str(type_guard_json.get("conflict_reason", "") or "").strip()
    vision_family = str(type_guard_json.get("vision_family", "") or "").strip()
    vision_slot = str(type_guard_json.get("vision_slot", "") or "").strip()
    vision_type = str(type_guard_json.get("vision_type", "") or "").strip()
    review_required = bool(type_guard_json.get("review_required"))
    visible_evidence = type_guard_json.get("visible_evidence") if isinstance(type_guard_json.get("visible_evidence"), list) else []
    evidence_text = "；".join(str(item or "").strip() for item in visible_evidence if str(item or "").strip())
    confidence_value = type_guard_json.get("vision_confidence")
    confidence_text = ""
    if isinstance(confidence_value, (int, float)):
        confidence_text = f"{float(confidence_value):.2f}"
    elif str(confidence_value or "").strip():
        confidence_text = str(confidence_value).strip()

    header_lines = [
        "【产品类型总控约束】",
        f"- 表格原始产品类型：{raw_product_type or '未填写'}",
        f"- 最终采用类型：{display_type or raw_product_type or '未填写'}",
        f"- 标准族类：{canonical_family or '未知'}",
        f"- 标准佩戴/使用部位：{canonical_slot or 'unknown'}",
        f"- 冲突等级：{conflict_level or 'none'}",
        f"- 视觉识别：{vision_type or 'unknown'} / {vision_family or 'unknown'} / {vision_slot or 'unknown'}",
        f"- 视觉置信度：{confidence_text or 'unknown'}",
        f"- 是否需要人工复核：{'是' if review_required else '否'}",
        f"- 冲突原因：{conflict_reason or '无'}",
    ]
    if evidence_text:
        header_lines.append(f"- 图片侧依据：{evidence_text}")

    footer_lines = [
        "执行要求：",
        "1. 后续所有阶段都必须优先遵循表格产品类型和最终标准佩戴/使用部位，不得被图片角度带偏。",
        "2. 如果图片视觉与表格类型冲突，允许记录冲突，但不允许擅自把商品改写成其他部位使用的品类。",
        "3. 如果图片存在白底、无尺度参照、单张易误判等情况，必须继续优先遵循最终产品类型。",
        "4. 输出文本如果出现与最终类型冲突的禁词或错误部位，视为类型跑偏。",
    ]
    parts = ["\n".join(header_lines)]
    if prompt_contract:
        parts.append("类型契约：\n" + prompt_contract)
    parts.append("\n".join(footer_lines))
    return "\n\n".join(part for part in parts if part).strip()


def _append_type_guard_block(prompt: str, type_guard_json: Optional[Dict[str, Any]] = None) -> str:
    prompt = f"{prompt}\n\n{CONTROL_LAYER_LANGUAGE_RULE}"
    block = _build_type_guard_block(type_guard_json)
    if not block:
        return prompt
    return f"{prompt}\n\n{block}"


def _is_hair_accessory(product_type: str) -> bool:
    return _normalized_product_family(product_type) == "发饰"


def _is_hair_clip(product_type: str) -> bool:
    text = str(product_type or "").strip()
    return any(keyword in text for keyword in HAIR_CLIP_KEYWORDS)


def _hair_accessory_anchor_rules(product_type: str) -> str:
    if not _is_hair_accessory(product_type):
        return ""
    return """【发饰类特别要求】
如果 product_type 属于发饰类，请额外输出：
1. hair_accessory_subtype：scrunchie / small_side_clip / claw_clip / headband / hair_tie / hair_band / styling_tool / other_hair_accessory
2. placement_zone：face_side / back_head / top_head / low_ponytail / half_up / bun_area / full_head / unknown
3. hold_scope：flyaway_hair / small_hair_section / half_hair / low_ponytail / bun / decorative_only / unknown
4. orientation：horizontal_clip / vertical_clip / wrap_around / tie_up / insert_fix / wear_on_head / unknown
5. primary_result：cleaner_hairline / stronger_hold / more_complete_hairstyle / faster_hair_fix / decorative_focus / softer_face_shape / more_volume / unknown

判断原则：
- 只根据图片和商品类型做轻量推断，不要求精确；
- 不确定可输出 unknown，不要为了完整性硬猜；
- 这些字段只用于后续脚本方向选择，不是商品真实参数；
- 不要把所有发饰都默认判成 small_side_clip 或 claw_clip。
- scrunchie / 发圈 / 大肠发圈 / 布面褶皱发圈不要默认写成低马尾根部；如果图片明显用于发髻 / 盘发 / 低髻外侧，应优先识别为 bun_area / bun / wrap_around。

同时继续输出：
- structure_anchors：例如抓夹/边夹/发箍/发圈/发带/盘发工具结构
- operation_anchors：例如从哪里夹、是否单手可操作、是否适合半扎/盘发/整理碎发
- fixation_result_anchors：例如夹上后稳不稳、能否收住头发、是否容易松散
- before_after_result_anchors：例如脸边更干净、后脑更利落、整体更完整
- scene_usage_anchors：例如通勤、上学、居家、洗脸护肤、快速出门等

发饰脚本优先证明：
- 上头前后变化
- 操作门槛
- 固定结果
- 发型完成度
- 使用场景"""


def _hair_accessory_opening_rules(product_type: str) -> str:
    if not _is_hair_accessory(product_type):
        return ""
    return """【发饰类特别要求】
发饰类 Hook 优先来自：
- 发型问题切入
- 快速变化切入
- 操作门槛切入
- 结果先给切入

不要只拍发饰单体特写，必须让用户尽快看到“夹上 / 戴上 / 用上之后的变化”。"""


def _hair_accessory_persona_rules(product_type: str) -> str:
    if not _is_hair_accessory(product_type):
        return ""
    return """【发饰类特别要求】
1. hairstyle_rule 必须服务于发饰展示，不允许头发本身复杂到掩盖发饰作用；
2. clothing_rule 更克制，避免头部区域被耳饰、帽子、大围巾等抢注意力；
3. emotion_progression 更适合：
   - 开头：轻困扰 / 轻嫌麻烦 / 轻疑问
   - 中段：轻确认 / 轻惊喜 / 轻觉得顺手
   - 结尾：轻满意 / 轻安心 / 轻推荐
4. movement_style 优先是：
   - 对镜整理头发
   - 出门前快速处理
   - 顺手夹一下确认效果"""


def _hair_accessory_strategy_rules(product_type: str) -> str:
    if not _is_hair_accessory(product_type):
        return ""
    return """【发饰类特别规则】
如果 display_family = hair_accessory，则四条脚本的 primary_focus 不得写死为“小发夹 / 小抓夹 / 脸侧碎发 / 一小束头发 / 半扎发型”。
必须先读取 anchor_card_json 中的：
- hair_accessory_subtype
- placement_zone
- hold_scope
- orientation
- primary_result

再根据这些字段动态生成四条脚本的主表达重点；不确定字段为 unknown 时，也要选择更稳妥的“发饰子类型待确认”表达，不要硬套小发夹逻辑。

四个 script_role 的发饰类通用定义：
- cognitive_reframing：纠正对该发饰的常见误判，必须跟具体子类型有关，不要泛泛写“不是普通发饰”。
- result_delivery：直接把戴上 / 夹上 / 扎上之后的发型结果给出来，不要把结构拆解当主线。
- risk_resolution：解决该子类型最常见的使用顾虑，例如夹不住、扎不稳、勒头、显幼、不好上手等，decision 信号要前移。
- aura_enhancement：强调整体发型更完整、更有完成度，适合出门前 / 镜前确认 / 日常整理好后的情境。

发饰类四条策略必须拉开 primary_focus，不能四条都同时讲“夹得住 + 夹好后好看 + 结构清楚 + 结果完整”这一整套组合。"""


def _hair_accessory_expression_rules(product_type: str) -> str:
    if not _is_hair_accessory(product_type):
        return ""
    return """【发饰类特别规则】
发饰类表达必须围绕：
- 变化
- 操作
- 固定结果
- 场景可用性

不要把发饰写成单纯审美展示物。"""


def _hair_accessory_script_rules(product_type: str) -> str:
    if not _is_hair_accessory(product_type):
        return ""
    lines = [
        "生成发饰类脚本前必须先读取 script_brief.hair_accessory_profile 中的 hair_accessory_subtype / placement_zone / hold_scope / orientation / primary_result。",
        "禁止忽略这些字段，直接套用“小发夹 / 小抓夹 / 脸侧碎发整理 / 一小束头发 / 半扎发型”模板。",
        "每条脚本只能有 1 个 primary_focus；proof 段镜头停留中心和口播中心必须围绕 primary_focus，secondary_focus 最多 1 个，可为空。",
        "除 primary_focus / secondary_focus 外，其他卖点只能背景露出，不得成为独立 proof 镜头、镜头目的中心或独立口播中心。",
        "发饰类四条脚本不得都讲同一件事，必须按照 script_role 拉开主线。",
        "优先使用低风险镜头：已佩戴 / 已夹好 / 已扎好状态、轻微侧头 / 回头、结果镜头、固定关系镜头、发饰静物特写、开合 / 结构短特写。",
        "避免长时间夹发过程、手/头发/发饰长时间纠缠同框、反复调整发饰位置、大幅甩头测试稳固、复杂盘发过程、持续用手整理头发再露出发饰。",
        "每组 4 条脚本中，最多 1 条可使用“夹 / 扎 / 戴的过程”作为主要 proof；其余脚本优先用“结果成立 + 固定关系 + 轻微动态”完成 proof。",
        "只要能用状态镜头表达，就不要用过程镜头。",
        "固定关系不能一律理解成“夹住一小束头发”：small_side_clip 是夹住脸侧碎发 / 小束头发；claw_clip 是夹住半头 / 后脑发束 / 低盘区域；headband 是戴在头顶 / 发际线附近的压发与装饰关系；hair_tie 是扎住马尾 / 低扎 / 发束；hair_band 是环绕头部或局部发束；styling_tool 是帮助形成盘发 / 固定结构。",
        "首镜优先展示使用后的发型结果，前 3 秒内看到发饰已经在头发上、发型状态更完整 / 更整齐 / 更有装饰感；若子类型不是夹类，应改成戴好 / 扎好 / 使用后结果。",
        "必须明确佩戴方向、佩戴区域和作用范围，且要与 hair_accessory_profile 匹配。",
        "不允许只拍发饰单体特写。",
        "不允许没有操作、没有变化、没有使用结果的空展示。",
        "proof 至少证明以下两项中的两项：使用后的发型变化、发饰与头发的固定/收束/佩戴关系、日常场景下的完成度。",
        "固定证明不要靠大动作或甩头，优先使用轻微转头、轻微低头、镜前确认、侧后方已夹好结果。",
        "字幕一镜一句，短句优先，不要解释太长。",
        "decision 优先收住：适合谁、日常会不会真用到、是不是买来不会闲置。",
        "必须输出 rhythm_checkpoints，且使用机器可检查格式：hook_complete_by=3s，core_proof_start_between=4-8s，decision_signal_by=12s，risk_resolution_decision_by=9s_or_not_applicable。",
        "不要输出“proof 在第1-15秒展开”“decision 在第14秒前出现”这类宽泛描述。",
    ]
    if _is_hair_clip(product_type):
        lines.extend(
            [
                "抓夹/边夹类允许出现快速整理、出门前、到达后镜前补夹、摘盔后恢复等场景，但头盔元素不要喧宾夺主。",
                "发夹类首镜不要只拍材质或装饰，要尽快把头发变化和固定效果接实。",
            ]
        )
    return "\n".join(f"- {line}" for line in lines)


def _hair_accessory_qc_rules(product_type: str) -> str:
    if not _is_hair_accessory(product_type):
        return ""
    return """发饰类轻质检：
- 是否错误地把当前发饰写成“小发夹 / 小抓夹 / 一小束头发 / 脸侧碎发整理”模板
- 是否已正确读取并使用 hair_accessory_subtype / placement_zone / hold_scope / orientation / primary_result
- 四条脚本的 primary_focus 是否明显不同
- 当前脚本的 proof 是否围绕 primary_focus
- 是否过度依赖夹 / 扎 / 戴的过程镜头
- 是否出现长时间夹发 / 扎发、手/头发/发饰纠缠、复杂整理动作、大幅甩头等高风险动作
- audio_layer 是否在没有明确夹合动作时误用 soft_click
- rhythm_checkpoints 是否为机器可检查格式
- 是否只展示发饰好看，而没有展示发型变化或固定 / 收束 / 佩戴关系

处理原则：能最小修正就最小修正；轻微问题不阻断；只有明显跑偏时进入 major issue。"""


def _hair_accessory_variant_rules(product_type: str) -> str:
    if not _is_hair_accessory(product_type):
        return ""
    return """【发饰类特别规则】
如果 product_type 属于发饰类：
1. 变体不允许都只在开头变化；
2. 必须适度测试：
   - 操作呈现方式
   - 固定效果的 proof 方式
   - 发型变化的呈现顺序
3. 不允许 5 条都还是“单体展示 + 一句泛安利”。"""


PROMPT_P1 = """你是一个资深的跨境电商短视频内容分析专家、商品视觉锚点分析专家。

你的任务不是写脚本，而是基于输入的商品图片与最少背景信息，生成一份严格结构化的产品锚点卡，供后续短视频策略与脚本生成使用。

你必须遵守以下规则：
1. 只能基于图片中可见信息和输入字段进行判断；
2. 不得编造价格、材质成分、功能参数、品牌背书、尺寸信息；
3. 对无法从图片确认的内容，直接省略，不要猜测成事实；
4. 输出必须是合法 JSON，不要输出 markdown，不要输出解释文字。

输入信息：
- target_country: {{target_country}}
- target_language: {{target_language}}
- product_type: {{product_type}}
- product_selling_note: {{product_selling_note}}
- product_parameter_info: {{product_parameter_info}}

请结合图片输出 JSON，结构如下：
{
  "product_positioning_one_liner": "",
  "hard_anchors": [
    {
      "anchor": "",
      "reason_not_changeable": "",
      "confidence": "high|medium|low"
    }
  ],
  "display_anchors": [
    {
      "anchor": "",
      "why_must_show": "",
      "recommended_shot_type": ""
    }
  ],
  "key_visual_constraints": [
    {
      "constraint": "",
      "confidence": "high|medium|low",
      "basis": ""
    }
  ],
  "hair_accessory_subtype": "",
  "placement_zone": "",
  "hold_scope": "",
  "orientation": "",
  "primary_result": "",
  "category_execution_contract": {
    "display_family": "",
    "product_subtype": "",
    "use_case": "",
    "placement_zone": "",
    "hold_scope": "",
    "orientation": "",
    "primary_visual_result": "",
    "operation_policy": "",
    "field_confidence": {
      "product_subtype": "high|medium|low",
      "use_case": "high|medium|low",
      "placement_zone": "high|medium|low",
      "hold_scope": "high|medium|low",
      "orientation": "high|medium|low",
      "primary_visual_result": "high|medium|low",
      "operation_policy": "high|medium|low"
    },
    "safe_shot_templates": [],
    "forbidden_actions": [],
    "result_priority": "",
    "season_context": {
      "primary_season": "winter|summer|shoulder_season|year_round|unknown",
      "weather_signal": "cold|hot|mild|rainy|sunny|unknown"
    },
    "hat_risk_tier": "low_risk|medium_risk|high_risk|unknown",
    "set_relationship": "same_color|matching_color|mix_match|unknown",
    "co_styling_hint": {
      "pair_with": []
    },
    "audio_policy": {
      "bgm_style": "",
      "bgm_energy": "low|medium",
      "voiceover_priority": "high",
      "sfx_policy": "",
      "allowed_sfx": [],
      "forbidden_sfx": [],
      "sfx_timing_rules": [],
      "audio_negative_constraints": []
    }
  },
  "distortion_alerts": [""],
  "candidate_primary_selling_points": [
    {
      "selling_point": "",
      "how_to_tell": "",
      "how_to_show": "",
      "risk_if_missed": ""
    }
  ],
  "persona_suggestions": [
    {
      "persona": "",
      "why_fit": ""
    }
  ],
  "scene_suggestions": [
    {
      "scene": "",
      "why_fit": "",
      "not_recommended_scene": ""
    }
  ],
  "camera_mandates": [
    {
      "stage": "opening|middle|ending",
      "must_do": ""
    }
  ],
  "parameter_anchors": [
    {
      "parameter_name": "",
      "parameter_value": "",
      "why_must_preserve": "",
      "execution_note": "",
      "confidence": "high|medium|low"
    }
  ],
  "structure_anchors": [""],
  "operation_anchors": [""],
  "fixation_result_anchors": [""],
  "before_after_result_anchors": [""],
  "scene_usage_anchors": [""]
}

【P1 输出上限】
- hard_anchors 最多 3 条；
- display_anchors 最多 3 条；
- key_visual_constraints 最多 3 条；
- candidate_primary_selling_points 最多 3 条；
- persona_suggestions 最多 2 条；
- scene_suggestions 最多 2 条；
- camera_mandates 最多 3 条；
- parameter_anchors 最多 5 条；
- structure_anchors / operation_anchors / fixation_result_anchors / before_after_result_anchors / scene_usage_anchors 每项最多 3 条；
- category_execution_contract.safe_shot_templates 最多 4 条；
- category_execution_contract.forbidden_actions 最多 4 条；
- audio_policy 内 allowed_sfx / forbidden_sfx / sfx_timing_rules / audio_negative_constraints 每项最多 4 条。
请优先输出高置信、对视频还原最有用的信息，不要为了填满数量写泛化内容。

【类目规则】
- 服装类：重点锁定版型、领口、肩线、长度、面料视觉感、上身结果
- 耳饰类：重点锁定耳钩/耳针结构、上耳垂坠比例、局部质感、脸侧结果
- 发饰类：重点锁定结构、操作方式、固定方式、上头前后变化、使用场景
- 首饰类：重点锁定佩戴位置、圈口/直径/宽度/厚薄/克重等可见参数、局部质感、上手结果

{{hair_accessory_rules}}

{{category_execution_contract_rules}}

【发饰轻量字段规则】
- 只有 product_type 属于发饰类时，才需要认真填写 hair_accessory_subtype / placement_zone / hold_scope / orientation / primary_result；
- 非发饰类可以统一输出空字符串；
- 发饰类字段只服务于脚本方向，不是商品真实参数；能判断就判断，不确定允许输出 unknown；
- 不要为了完整性硬猜，不要把所有发饰默认判成 small_side_clip 或 claw_clip。

【关键视觉防错锚点规则】
- key_visual_constraints 不是抽取真实商品参数，而是少量生成“防止 AI 视频还原错误”的关键视觉约束
- 每个商品最多输出 0–5 条；如果图片信息不足，可输出空数组 []
- 只写图片中可观察或可合理推测的信息，且必须与视频还原错误强相关
- 优先写相对长度、落点、方向、佩戴方式、体量级别、版型比例
- 不写材质、重量、品牌、功效、精确规格
- confidence 只能是 high / medium / low；high 与 medium 会进入后续脚本强执行，low 只作为参考
- 轻量模板示例：
  - 耳饰：短垂比例、末端落点、贴耳主体、避免长流苏
  - 发饰：横夹/竖夹、佩戴方向、侧边固定/后脑盘发
  - 项链：锁骨链/中长链、吊坠落点、领口关系
  - 手链：贴腕/轻松动、手腕局部结果
  - 戒指：单指主戴、上手比例、美甲不抢镜
  - 女装上装：衣长不要被拉长、版型不要变厚重、肩线/领口不要生成错、轻上装不要生成成厚外套或长外套

【参数锚点规则】
- 如果图片或图中文字里明确出现尺寸、直径、厚度、宽度、克重、数量、材质名、颜色名等可确认参数，请写入 parameter_anchors
- parameter_anchors 只允许收录图片中直接可见、可读、可确认的参数，不得猜测
- 如果 product_parameter_info 不为空，可将其中人工确认的参数事实补入 parameter_anchors，并优先保持这些事实不被后续阶段改写
- 如果图片里没有明确参数，请输出空数组 []
- parameter_anchors 用于后续脚本和质检保持参数事实，不代表每个参数都必须口播

{{product_selling_note_rules}}"""


PROMPT_P2 = """你是一个资深的短视频首镜设计专家、服装/耳饰/发饰/首饰视觉抓停留专家，尤其擅长为 TK 冷启动内容设计“前3秒更有原生抓力”的首镜方案。

你的任务是：基于商品锚点卡，为这个商品设计一组真正有“首屏吸引力”的首镜方案。

【输入信息】
- 商品类目：{{product_family}}
- 商品锚点卡：{{anchor_card_json}}
- 目标国家：{{target_country}}
- 账号调性：真实、轻精致、可挂主页、不硬广、有审美完成度
- 当前目标：更适合 TK 冷启动，强化前3秒抓力

【任务要求】
请输出 5 个首镜吸引策略，且尽量拉开差异。

至少覆盖：
1. 局部质感先行
2. 结果先给
3. 动作进入
4. 问题切入
5. 高惊艳首镜

首镜表达切入口优先从以下类型中选择：
1. 轻吐槽式
2. 判断反差式
3. 顾虑冲突式
4. 结果先行式
5. 误区纠正式

如果 target_country 属于东南亚市场，则场景建议优先推荐达人家中自然分享场景。

【类目提醒】
- 服装类：优先上身效果、搭配完成度、比例结果
- 耳饰类：优先局部质感、上耳结果、脸侧提升
- 发饰类：优先上头前后变化、操作过程、快速整理结果、固定效果
- 首饰类：优先上手结果、佩戴位置关系、圈口/粗细/体量感、局部质感和参数可信度

【每个策略输出】
1. strategy_name
2. angle_bucket
3. opening_mode_candidate
4. visual_entry_mode_candidate
5. first_frame_visual
6. shot_size
7. action_design
8. first_product_focus
9. native_expression_entry
10. opening_first_line_type
11. suggested_short_line
12. style_note
13. risk_note

【特别约束】
1. 首镜必须优先解决“画面吸引力 + 表达抓力”，而不是先解释商品；
2. 开头第一句不要只是平铺描述商品或场景；
3. 优先让第一句自带一个真实顾虑、轻矛盾、判断、反差，或让人想听后半句的口子；
4. 不要让人物、背景、动作喧宾夺主，必须让商品成为视觉主角；
5. 避免以下弱开头作为优先方案：
   - 我最近会穿这种……
   - 这件很适合……
   - 今天想分享……
   - 这种看起来……
   - 同样是……这种会……

{{hair_accessory_rules}}

输出必须是合法 JSON，不要输出 markdown，不要输出解释文字。
输出结构：
{
  "opening_strategies": [
    {
      "strategy_name": "",
      "angle_bucket": "",
      "opening_mode_candidate": "",
      "visual_entry_mode_candidate": "",
      "first_frame_visual": "",
      "shot_size": "",
      "action_design": "",
      "first_product_focus": "",
      "native_expression_entry": "",
      "opening_first_line_type": "",
      "suggested_short_line": "",
      "style_note": "",
      "risk_note": ""
    }
  ]
}"""


PROMPT_P3 = """你是一个资深的短视频人物设定顾问、造型顾问、情绪推进设计顾问。

你的任务不是写脚本，而是为当前商品生成一份《人物 / 穿搭 / 情绪强化包》。

【输入信息】
- 商品类目：{{product_family}}
- 商品锚点卡：{{anchor_card_json}}
- performance_profile_json：{{performance_profile_json}}
- 目标国家：{{target_country}}
- 账号调性：真实、轻精致、自然、不硬广

【输出要求】
请输出结构化结果，包含：
1. persona_state
2. appearance_anchor
3. attractiveness_boundary
4. hairstyle_rule
5. makeup_rule
6. clothing_rule
7. accessory_rule
8. emotion_progression
9. movement_style
10. styling_completion_tag
11. persona_visual_tone
12. styling_key_anchor
13. emotion_arc_tag
14. anti_template_warnings
15. human_performance_contract

【硬约束】
1. 人物必须真实、顺眼，但不能过度网红脸、过度精修感、过度漂亮到喧宾夺主；
2. 耳饰类必须至少一侧耳朵完整露出；
3. 发饰类必须优先保证头发可看清、发型变化可判断；
4. 配饰类默认穿低饱和纯色上衣，不叠加抢眼配饰；
5. 女装类必须保证搭配完整、比例成立；
6. 情绪必须有推进；
7. 不要全程同一种轻笑模板；
8. 不要主播感，不要测评腔，不要过度表演。
9. 不要让人物最终都收敛成“温柔、轻笑、顺手分享”的单一模板；
10. 穿搭表述不能只写负向约束，还要体现一个正向完成度方向；
11. 情绪推进不能只停留在“自然一点”，而要让模型知道开头、中段、结尾分别处于什么轻状态。

{{light_control_rules}}

【人物穿搭与情绪轻控制字段池】
{{light_control_pool}}

{{hair_accessory_rules}}

{{human_performance_contract_rules}}

【persona_state 建议池】
- R1 轻分享型
- R2 小惊喜型
- R3 轻判断型
- R4 轻冷静型

【输出格式要求】
1. 必须输出单个合法 JSON 对象；
2. persona_state 必须是单个字符串，不要输出数组或对象；
3. styling_completion_tag / persona_visual_tone / styling_key_anchor / emotion_arc_tag 都必须是单个字符串；
4. anti_template_warnings 必须是字符串数组，建议 3-5 条；
5. 不要额外包一层 result / data / persona_pack。

输出结构：
{
  "persona_state": "",
  "appearance_anchor": "",
  "attractiveness_boundary": "",
  "hairstyle_rule": "",
  "makeup_rule": "",
  "clothing_rule": "",
  "accessory_rule": "",
  "emotion_progression": "",
  "movement_style": "",
  "styling_completion_tag": "",
  "persona_visual_tone": "",
  "styling_key_anchor": "",
  "emotion_arc_tag": "",
  "anti_template_warnings": [""],
  "human_performance_contract": {
    "performance_family": "",
    "persona_mode": "",
    "expression_arc": [],
    "gaze_plan": [],
    "gaze_rule": {
      "min_points_required": 3,
      "final_point_options": []
    },
    "micro_reaction_beats": [],
    "body_language_beats": [],
    "product_interaction_beats": [],
    "relatable_moment": "",
    "performance_intensity": "",
    "forbidden_performance": [],
    "active_micro_reaction_limit": 0
  }
}

输出必须是合法 JSON，不要输出 markdown，不要输出解释文字。"""


PROMPT_P4 = """你是一个资深的短视频内容策略专家，擅长为跨境电商服装/耳饰/发饰/首饰商品设计原创短视频打法。

你的任务不是直接写脚本，而是先完成《内容策略匹配卡》的极短结构卡。
P4 是策略分配层：只决定 S1-S4 各自讲什么、怎么证明、风险在哪里，不写完整镜头、不写完整口播、不复述完整 contract。

【输入信息】
- 商品类目：{{product_family}}
- 商品锚点卡：{{anchor_card_json}}
- 首镜吸引策略：{{opening_strategies_json}}
- 人物 / 穿搭 / 情绪强化包：{{persona_style_emotion_pack_json}}
- 目标国家：{{target_country}}
- 视频时长：15秒
- 账号调性：{{account_style_boundary}}

【任务要求】
请输出 4 套差异明确的《内容策略匹配卡》：
1. S1 强停留原生型
2. S2 平衡型
3. S3 强购买承接型
4. S4 高惊艳首镜型

每套只输出极短结构字段：
- strategy_id / final_strategy_id / strategy_name
- script_role
- primary_focus / secondary_focus
- primary_selling_point
- dominant_user_question
- proof_thesis / decision_thesis
- opening_angle
- proof_path
- performance_bias
- scene_suggestion / scene_subspace
- risk_note

允许少量补充字段，但不要输出完整镜头、完整口播、长段解释或重复规则。

【特别约束】
1. 4 套策略必须真正拉开差异；
2. dominant_user_question 不允许只是同一句换说法；
3. proof_thesis 不允许只是同一购买理由的轻改写；
4. decision_thesis 不允许只是同一收尾判断的轻改写；
5. 4 条 script_role 必须完整覆盖以下 4 种固定角色，各用一次：cognitive_reframing / result_delivery / risk_resolution / aura_enhancement；
6. 每条都必须明确 primary_focus；secondary_focus 可以为空字符串，但不能与 primary_focus 相同；
7. 若 secondary_focus 为空，该策略的 proof 设计必须更集中服务 primary_focus，不要横向扩卖点；
8. opening_angle / proof_path 都必须服从 script_role；
9. 不允许 4 套都使用同一种 opening_angle；
10. 不允许 4 套都使用同一种收尾判断方式；
11. 至少出现 3 种不同的 proof_path / proof 思路；
12. 至少出现 3 种不同的 opening_angle / 视觉进入思路；
13. 至少 2 种不同的 performance_bias；
14. 至少 3 种不同的人物互动或画面进入思路；
15. 4 条脚本的节奏感不允许完全相同；
16. S4 与 S1 的差异不能只是“更好看一点”；
17. 如果 target_country 属于东南亚市场：
    - 至少 2 套以家中自然分享场景为主
    - 家中场景至少覆盖 2 种 scene_subspace
    - 不允许 4 条都落在同一个窗边 / 同一面镜子 / 同一机位逻辑里
    - S4 仍应优先保留家中自然分享语境
18. 这些差异必须是轻差异，不得把人物写成明显不同的人设；
19. 差异服务于内容感知分化，不服务于抢商品注意力。
20. proof_path 必须服从 category_execution_contract.operation_policy：
    - result_first_process_avoid 只允许 A_result_detail_only / B_result_with_light_compare；
    - process_forbidden / static_result_only 只允许 A_result_detail_only；
    - process_allowed_once 可使用 C_result_with_short_process，每组最多 1 条；D 只有在 contract 明确允许轻对比和短过程时才可用；
    - 不允许为了 S1-S4 差异而违反 operation_policy。

【script_role 顶层分类规则】
- script_role 是整条 15 秒视频的顶层职责，opening_angle 只是前 3 秒执行方式；
- cognitive_reframing：必须明确指出一个常见误判，并通过画面或口播完成“不是 X，而是 Y”的纠偏；
- result_delivery：必须先让戴上 / 穿上 / 夹上后的结果成立，不能只停留在结构说明；
- risk_resolution：必须明确提出一个风险，并在中段完成化解；
- aura_enhancement：必须包含整体感 / 氛围感 / 造型完成度镜头，不能只讲局部功能。

【primary_focus / secondary_focus 规则】
- primary_focus 是这条 15 秒视频唯一必须讲清的一件事，同时承担主购买理由与主证明逻辑；
- secondary_focus 是可选辅助证明点，最多 1 个；
- 复杂结构商品默认只允许 1 个主 proof 主题 + 1 个辅助主题；
- 其他结构细节只能背景性露出，不得成为独立 proof 镜头主题。

{{light_control_defaults}}

{{category_execution_contract_rules}}

{{hair_accessory_rules}}

{{product_selling_note_rules}}

输出必须是合法 JSON，不要输出 markdown，不要输出解释文字。
字段要短，单个自然语言字段建议 12-28 个中文字符；risk_note 最多一句。
输出结构：
{
  "contract_conflict_warning": "",
  "strategies": [
    {
      "strategy_id": "S1|S2|S3|S4",
      "final_strategy_id": "Final_S1|Final_S2|Final_S3|Final_S4",
      "strategy_name": "",
      "script_role": "cognitive_reframing|result_delivery|risk_resolution|aura_enhancement",
      "primary_focus": "",
      "secondary_focus": "",
      "primary_selling_point": "",
      "dominant_user_question": "",
      "proof_thesis": "",
      "decision_thesis": "",
      "opening_angle": "",
      "proof_path": "A_result_detail_only|B_result_with_light_compare|C_result_with_short_process|D_result_with_light_compare_and_short_process",
      "performance_bias": "",
      "scene_suggestion": "",
      "scene_subspace": "",
      "risk_note": ""
    }
  ]
}

{{repair_block}}"""


PROMPT_P5 = """你是一个资深的短视频内容策略总控专家。

你的任务不是重新生成策略，而是检查并定稿已有的 4 套极短策略卡。
P5 只做选择、收敛、补齐和冲突提醒，不复述完整 contract，不写完整脚本，不写完整口播。

【输入信息】
- product_type: {{product_type}}
- target_country: {{target_country}}
- target_language: {{target_language}}
- anchor_card_json: {{anchor_card_json}}
- opening_strategies_json: {{opening_strategies_json}}
- persona_style_emotion_pack_json: {{persona_style_emotion_pack_json}}
- strategies_json: {{strategies_json}}

【核心任务】
1. 检查 S1 / S2 / S3 / S4 是否真正有差异；
2. 对过于相似的地方做轻度修正；
3. 输出 4 个标准化的已定稿策略包，字段保持短而可执行。

【语义级检查重点】
1. dominant_user_question 不得两两只是换说法
2. proof_thesis 不得两两只是同一购买理由的改写
3. decision_thesis 不得两两只是同一收尾判断的改写
4. 用户看完后最可能记住的购买理由，4 条之间必须尽量拉开
5. 4 条 script_role 必须完整覆盖 cognitive_reframing / result_delivery / risk_resolution / aura_enhancement
6. 每条都必须有 primary_focus；secondary_focus 可以为空，但不能与 primary_focus 相同
7. script_role 必须先成立，再选择 opening_angle / proof_path；opening_angle 不能替代 script_role

【差异检查规则】
1. 不允许 4 套都使用同一种 opening_angle；
2. 不允许 4 套都使用同一种收尾判断方式；
3. 至少出现 3 种不同的 proof_path / proof 思路；
4. 至少出现 3 种不同的 opening_angle / 视觉进入思路；
5. 至少 2 种不同的 performance_strategy_hint；
6. 至少 3 种不同的人物互动或画面进入思路；
7. 4 条脚本的节奏感不允许完全相同；
8. S4 与 S1 的差异不能只是“更好看一点”；
9. S4 必须在首镜目标、首镜画面组织、开头进入方式、后续承接方式上与 S1/S2/S3 明显不同。
10. 若 secondary_focus 为空，该策略的 proof 设计必须更集中服务 primary_focus。
11. 若 opening/proof/ending 已有差异，但人物视觉感、穿搭完成感、情绪轨迹仍然像同一条模板，应视为感知近似度仍偏高，需要轻修正。
12. P5 必须检查每条策略的 script_role 是否成立：cognitive_reframing 要有误判纠偏，result_delivery 要有结果先给，risk_resolution 要有风险与化解，aura_enhancement 要有整体气质提升。
13. P5 必须检查 primary_focus 是否足够聚焦；secondary_focus 只能作为 1 个辅助证明点，不得横向扩成第二条主线。
14. P5 必须检查 proof_path 是否服从 category_execution_contract.operation_policy；若冲突，以 operation_policy 为准，不得为了差异化保留过程镜头。

【东南亚规则】
如果 target_country 属于东南亚市场，请额外检查：
1. 至少 2 套以家中自然分享场景为主；
2. 家中场景至少覆盖 2 种 scene_subspace；
3. 不允许 4 条都落在同一个窗边 / 同一面镜子 / 同一机位逻辑里；
4. 若近似度过高，优先修正顺序：
   - proof_path
   - opening_angle
   - scene_subspace
   - performance_strategy_hint
5. 若前述修正后仍然近似，可轻修正：
   - performance_strategy_hint
   - risk_controls
   - scene_suggestion

{{hair_accessory_rules}}

{{category_execution_contract_rules}}

输出结构：
{
  "contract_conflict_warning": "",
  "difference_check": "",
  "strategies": [
    {
      "strategy_id": "S1|S2|S3|S4",
      "final_strategy_id": "Final_S1|Final_S2|Final_S3|Final_S4",
      "strategy_name": "",
      "script_role": "cognitive_reframing|result_delivery|risk_resolution|aura_enhancement",
      "primary_focus": "",
      "secondary_focus": "",
      "primary_selling_point": "",
      "dominant_user_question": "",
      "proof_thesis": "",
      "decision_thesis": "",
      "opening_angle": "",
      "proof_path": "A_result_detail_only|B_result_with_light_compare|C_result_with_short_process|D_result_with_light_compare_and_short_process",
      "performance_strategy_hint": "",
      "contract_alignment_note": "",
      "risk_controls": [""],
      "scene_suggestion": "",
      "scene_subspace": "",
      "risk_note": ""
    }
  ]
}

输出必须是合法 JSON，不要输出 markdown，不要输出长篇解释。"""


PROMPT_P6 = """你是一个资深的短视频表达策略专家、内容血肉扩充专家。

你的任务是：基于已经定稿的策略包，输出表达扩充计划。

【输入信息】
- 商品类目：{{product_family}}
- 商品锚点卡：{{anchor_card_json}}
- 已定稿策略包：{{final_strategy_json}}
- 人物 / 穿搭 / 情绪强化包：{{persona_style_emotion_pack_json}}
- 目标国家：{{target_country}}
- 目标语言：{{target_language}}
- 视频时长：15秒
- 账号调性：{{account_style_boundary}}

【输出字段】
1. exp_id
2. main_expression_pattern
3. aux_expression_pattern
4. native_expression_entry
5. opening_expression_task
6. middle_expression_task
7. ending_expression_task
8. human_touch_focus_point
9. most_likely_empty_point
10. expression_weight_control
11. voiceover_intent
12. voiceover_language_requirement

【特别规则】
1. 表达必须更有原生感，不要像“正式介绍商品”；
2. 不能改变主卖点；
3. 不能让表达层喧宾夺主；
4. 对于 S4：
   - 表达层不要抢首镜画面；
   - 开头允许先画面打人，再让语言快速进入；
   - 中段必须更快把首镜接实，避免空钩子；
5. 如果 target_country 属于东南亚市场，请优先保留家中自然分享语境；
6. opening_expression_task / middle_expression_task / ending_expression_task 只能写中文执行任务，不得嵌入完整目标语言口播句子；
7. 不要在中文控制字段里写越南语/泰语/英语完整台词；目标语言口播只写成 voiceover_intent 的意图；
8. voiceover_language_requirement 固定说明：P7 生成口播时必须使用目标语言 {{target_language}}，不得使用中文。

{{hair_accessory_rules}}

【输出格式要求】
1. 必须输出单个合法 JSON 对象；
2. 每个字段都必须是字符串，不要输出数组或对象；
3. exp_id 建议写成 `EXP_S1 / EXP_S2 / EXP_S3 / EXP_S4` 之一；
4. 不要额外包一层 result / data / expression_plan。

输出结构：
{
  "exp_id": "",
  "main_expression_pattern": "",
  "aux_expression_pattern": "",
  "native_expression_entry": "",
  "opening_expression_task": "",
  "middle_expression_task": "",
  "ending_expression_task": "",
  "human_touch_focus_point": "",
  "most_likely_empty_point": "",
  "expression_weight_control": "",
  "voiceover_intent": "",
  "voiceover_language_requirement": "P7 生成口播时必须使用目标语言 {{target_language}}，不得使用中文"
}

输出必须是合法 JSON，不要输出 markdown，不要输出解释文字。"""


PROMPT_P7 = """你是一个资深的跨境电商短视频脚本策划专家、AI视频生成提示词专家。

你的任务是：基于 script_brief 输出一条可直接用于短视频生成的 15 秒原创脚本。

【输入信息】
- script_brief: {{script_brief_json}}
- target_country: {{target_country}}
- target_language: {{target_language}}
- product_type: {{product_type}}
- 视频时长：15秒

【核心要求】
1. 前3秒必须先解决“为什么值得继续看”；
2. 第一眼必须让用户明确看到商品；
3. 全片必须覆盖 hook / proof / decision；
4. 不强制使用“严格三句三任务”；
5. 口播允许 2–4 句短句灵活完成任务覆盖；
6. 默认 4–6 个镜头；
7. 单镜头默认 1–3 秒，尽量不超过 4 秒；
8. proof 可以由 2 个连续短镜头共同完成；
9. decision 必须是轻决策收束，不是默认催单；
10. 内部说明中文；字幕默认不输出，`subtitle_text_target_language` 和 `subtitle_text_zh` 默认留空字符串；口播只输出目标语言 {{target_language}}，`voiceover_text_target_language` 不得包含中文，`voiceover_text_zh` 默认留空字符串；
11. 你必须执行 script_brief，不要重新发明主卖点或方向；
12. script_brief.focus_control 中的 script_role 必须在镜头推进里成立，opening / proof / ending 都要服从它；
13. proof 镜头的视觉焦点和口播中心只能服务于 primary_focus 与 secondary_focus；
14. 如果 secondary_focus 为空，至少 2 个 proof 镜头要持续服务 primary_focus，不要横向扩卖点；
15. hook 必须在前 3 秒内完成，至少一个核心 proof 起始点要落在 4–8 秒区间，decision 信号必须在 12 秒前出现；
16. 若 script_role = risk_resolution，decision 信号必须在 9 秒前出现；
17. 脚本必须严格继承 styling_completion_tag / persona_visual_tone / styling_key_anchor / emotion_arc_tag；
18. 穿搭表述不能只写“低饱和纯色、不抢商品”，还要体现当前的 styling_completion_tag；
19. 人物状态不能只写“自然、轻笑、顺手分享”，还要体现当前的 persona_visual_tone；
20. styling_key_anchor 必须落实到执行约束里，说明哪一个穿搭视觉点最关键；
21. emotion_arc_tag 必须落实到人物动作与表情推进中；
22. script_brief.ai_shot_risk_profile 中的 forbidden / high_risk 要主动规避；若当前镜头容易踩雷，优先改用 replacement_templates；
23. 上述字段都只负责轻度拉开气质，不允许压过商品展示。
24. 如果 script_brief.parameter_anchors 非空，脚本必须保持这些参数事实，不得改写、偷换或与镜头设计冲突；
25. parameter_anchors 不要求逐条口播，但关键参数至少要做到“画面可见、表达不冲突、结果不被写歪”。
26. 如果 script_brief.key_visual_constraints 非空，P7 必须严格遵守这些关键视觉防错锚点，不得把相对长度、落点、方向、佩戴方式、体量级别、版型比例写反或写变形。
27. key_visual_constraints 是视频还原防错约束，不是新增卖点，不要求逐条口播，但必须在镜头内容、人物动作、执行约束中不冲突。
28. 不要输出大段“为什么这样设计”的解释。
29. script_brief 已经是 P7 精简执行版；不得抱怨字段不足，不得要求上游完整 JSON。
30. 必须优先使用 script_brief.p7_execution_template.shot_skeleton_template 作为 shot_skeleton 基线，只允许在不违反 category_execution_contract 时微调 shot_purpose，不要重新设计时间线。
31. 为了降低超时风险，不要输出 full_15s_flow / execution_constraints / rhythm_checkpoints / audio_layer / negative_constraints；这些字段由代码侧根据 storyboard 和 contract 本地补齐。
32. 如果某个镜头没有 voiceover_text_target_language，spoken_line_task 必须写 "none"，不要写 hook/proof/decision；视觉 proof 可以无口播，但有任务标签的镜头必须有目标语言口播。
33. storyboard 与 shot_skeleton 数量必须一致；默认直接输出 6 个镜头；不要输出 schema 以外字段。

【script_role 执行规则】
- 你必须先根据 script_role 判断整条脚本职责，而不是只看 opening_mode；
- cognitive_reframing：不是 X，而是 Y，必须明确指出一个常见误判并完成纠偏；
- result_delivery：戴上 / 穿上 / 夹上之后，结果已经成立，必须包含成品直出或结果先给镜头；
- risk_resolution：即便在 X 情况下也 OK，必须明确提出一个风险并在中段完成化解；
- aura_enhancement：用了之后整体更有感觉，必须包含整体感 / 氛围感 / 造型完成度镜头；
- opening / proof / decision 都必须服务于 script_role。

【primary_focus 执行规则】
- proof 段的镜头停留中心和口播中心必须围绕 primary_focus 展开；
- secondary_focus 最多只能作为一个辅助主题；
- 如果 secondary_focus 为空，primary_focus 对应的 proof 镜头不少于 2 个；
- 其他结构细节只能作为背景性露出，不得成为独立 proof 镜头主题；
- 对复杂结构商品，默认只允许 1 个主 proof 主题 + 1 个辅助主题。

【AI 可拍性规则】
- 每个 storyboard 镜头必须输出 ai_shot_risk 与 replacement_template_id；
- 不得生成 ai_shot_risk = forbidden 的镜头；
- 命中 high_risk 时，优先使用 script_brief.ai_shot_risk_profile.replacement_templates 替代，并填写 replacement_template_id；
- 耳饰类特别禁止：戴耳环动作、手触耳饰、撩 / 顺 / 整理耳侧发丝的过程帧、大幅转头、剧烈流苏摆动；
- 发饰类特别禁止：把完整夹发过程作为核心 proof、手指/头发/发夹三者长时间纠缠同框、大幅甩头测试固定效果、复杂盘发过程。

【15 秒硬节点规则】
- hook 必须在前 3 秒内完成；
- 至少一个核心 proof 的起始点必须在 4–8 秒区间；
- decision 信号必须在 12 秒前出现；
- 如果 script_role = risk_resolution，decision 信号必须在总时长 60% 节点前出现，即 15 秒视频要在 9 秒前出现；
- 时间类规则冲突时，取更早者；该规则不适用于结构类、镜头类、字段类规则。

【p7_execution_template 使用规则】
- Step 1：生成 shot_skeleton 时，直接继承 script_brief.p7_execution_template.shot_skeleton_template；
- 保持 6 镜头 time_range 连续，总时长 15 秒；
- shot_purpose 可以结合 primary_focus 微调，但不得改变 role / time_range / proof_path 的大结构；
- operation_policy 已经在模板中体现，不要因为 orientation 或创意差异加入完整过程；
- Step 2：再基于 shot_skeleton 填充 storyboard / performance / voiceover / audio。

{{ai_video_rhythm_rule}}

{{hair_accessory_rules}}

{{category_execution_contract_rules}}

{{human_performance_script_rules}}

{{audio_layer_rule}}

【输出结构】
{
  "contract_conflict_warning": "",
  "proof_path": "A_result_detail_only|B_result_with_light_compare|C_result_with_short_process|D_result_with_light_compare_and_short_process",
  "performance_strategy": "",
  "shot_skeleton": [
    {
      "shot_index": 1,
      "time_range": "",
      "role": "hook|proof|decision|transition",
      "shot_purpose": "",
      "proof_path": ""
    }
  ],
  "script_positioning": {
    "script_title": "",
    "direction_type": "",
    "core_primary_selling_point": ""
  },
  "opening_design": {
    "opening_mode": "",
    "first_frame": "",
    "expression_entry": "",
    "first_line_type": ""
  },
  "storyboard": [
    {
      "shot_no": 1,
      "duration": "",
      "shot_content": "",
      "shot_purpose": "",
      "subtitle_text_target_language": "",
      "subtitle_text_zh": "",
      "voiceover_text_target_language": "",
      "voiceover_text_zh": "",
      "spoken_line_task": "hook|proof|decision|proof+decision|none",
      "person_action": "",
      "performance": {
        "gaze": "",
        "expression_or_micro_reaction": "",
        "body_language": "",
        "product_interaction": ""
      },
      "style_note": "",
      "anchor_reference": "",
      "task_type": "attention|proof|bridge",
      "ai_shot_risk": "low|medium|high|forbidden",
      "replacement_template_id": ""
    }
  ],
  "local_completion_note": "full_15s_flow / execution_constraints / rhythm_checkpoints / audio_layer / negative_constraints 由代码侧补齐，本字段可省略"
}

输出必须是合法 JSON，不要输出 markdown，不要输出解释文字。"""


PROMPT_Q1 = """你是一个资深的短视频脚本质检与修正专家。

你的任务是对已经生成的正式脚本做轻质检，并在必要时做最小修正。

【输入信息】
- 商品类目：{{product_family}}
- Q1 精简上下文：{{q1_context_json}}
- 当前正式脚本：{{script_json}}
- 目标国家：{{target_country}}
- 目标语言：{{target_language}}
- 视频时长：15秒

【Q1 输入边界】
- 你只审当前 storyboard / audio_layer / performance 是否与 Q1 精简上下文一致；
- 不重新推断商品类目，不重评 P4/P5 创意优劣，不扩写新策略；
- category_execution_contract 是商品执行硬约束，human_performance_contract 只做人表演软约束；
- 代码侧 precheck 已覆盖 timing、目标语言中文、forbidden_sfx、明显 forbidden_actions 等硬规则；你需要确认修复后的语义是否自然，并处理 precheck 未覆盖的同义违规。

【质检目标】
检查以下问题：
1. 是否满足 4–6 镜头
2. 单镜头是否大多在 1–3 秒，是否有明显过长镜头
3. 是否覆盖 hook / proof / decision
4. 中段是否真的承担 proof，而不是只在描述氛围
5. 结尾是否是轻决策收束，而不是默认下单引导
6. 首镜是否足够清楚地让人知道卖什么
7. S4 是否有“首镜强但中段空”的问题
8. script_role 是否先成立，再延续到 proof / ending
9. primary_focus / secondary_focus 是否收得住，proof 是否没有横向扩卖点
10. hook 是否在前 3 秒内完成
11. 是否至少有一个核心 proof 起始点落在 4–8 秒区间
12. decision 信号是否在 12 秒前出现；若 script_role = risk_resolution，是否在 9 秒前出现
13. 人物状态是否符合 persona_state
14. 情绪推进是否成立
15. 穿着是否符合要求，是否抢商品
16. 配饰类是否完整露耳、无遮挡关键结构
17. 是否存在明显模板化表达
18. 当前脚本的人物视觉感是否符合 persona_visual_tone
19. 当前脚本的穿搭描述是否只停留在“避免错误”，还是已经体现 styling_completion_tag
20. 当前脚本是否明确落实了 styling_key_anchor
21. 当前脚本的情绪推进是否符合 emotion_arc_tag
22. 是否存在“人物看起来仍像同一批模板人、同一类安全穿搭、同一种平情绪轨迹”的问题
23. 如果商品锚点卡里存在 parameter_anchors，当前脚本是否错误改写、忽略关键参数，或让参数事实与画面/口播冲突
24. 是否踩中了常见 AI 高风险拍法，如空镜炫技、人物压过商品、首镜强但中段空、只拍氛围不进入 proof
25. 最终脚本和后续视频提示词是否存在“停住 / 停半拍 / 定格 / 静止 / 停留1秒 / 最后1秒轻停 / 站定不动 / 保持不动 / 完全静止 / 最后停0.5秒或1秒”等停顿、冻结、卡帧类表达；如有，必须改成“稳定构图 + 极轻微连续动态”表达
26. 如果商品锚点卡里存在 key_visual_constraints，当前脚本是否违反了其中 high / medium 约束，例如把短垂比例写成长流苏、把末端落点写错、把佩戴方向/位置写反、把轻上装写成厚外套或长外套
27. 是否出现 ai_shot_risk = forbidden 的镜头；如出现，进入 major_issues，并优先用 replacement_templates 做最小修正
28. 是否出现 high_risk 镜头；如出现，默认进入 minor_issues，并建议替换
29. 如果商品类目为发饰，是否满足：前 3 秒看到夹好结果、明确横夹/竖夹/侧边/后脑/半扎等佩戴关系、不过度依赖夹发过程、证明固定效果、字幕不过长、不是只展示发饰好看
30. audio_layer 是否存在明显问题：BGM 可能盖过口播、SFX 过多、SFX 与画面动作不匹配、发饰类遗漏关键固定音效机会、耳饰类使用过度闪光音效、女装类转场音效过重、general_accessory 音效过密导致廉价模板感
31. voiceover_text_target_language / subtitle_text_target_language 是否真正使用目标语言 {{target_language}}；如果含中文，进入 major_issues，并只把口播/字幕最小修正为目标语言；voiceover_text_zh 默认留空，不要把中文口播塞进目标语言字段

{{hair_accessory_rules}}

{{category_execution_contract_rules}}

{{human_stiffness_qc_rule}}

{{ai_video_rhythm_rule}}

{{audio_layer_rule}}

【修正规则】
1. 这是轻质检，不是强阻断精品审稿；
2. 只有重大问题才明显修；
3. 轻微问题记 minor_issues，不阻断；
4. 修正必须遵守“最小改动原则”；
5. repaired_script 必须返回修正后的完整脚本 JSON；若无需修正，则返回原脚本。
6. 人物视觉感 / 穿搭完成度 / 情绪推进这些项当前阶段默认作为 warning，不阻断流程；
7. 只有在人物明显抢商品、穿搭明显跑偏、完全无气质差异可感知、styling_key_anchor 完全没有执行、emotion_arc_tag 完全没有执行导致整条视频情绪像死水时，才提升为 major issue。
8. parameter_anchors 当前默认也是轻质检项；只有出现明显参数改写、关键参数被反向表达、或脚本内容与可见参数事实直接冲突时，才提升为 major issue。
9. audio_layer 问题默认记为 minor_issues，不阻断流程；只做最小修正，例如减少 SFX、降低 BGM 存在感、替换廉价音效。

输出结构：
{
  "pass": true,
  "contract_conflict_warning": "",
  "major_issues": [""],
  "minor_issues": [""],
  "suggest_fix": [""],
  "human_stiffness_check": {
    "timing_consistency_check": false,
    "timeline_consistency_check": false,
    "ai_shot_risk_check": false,
    "emotion_flatness_check": false,
    "gaze_monotony_check": false,
    "category_interaction_missing_check": false,
    "hit_count": 0,
    "summary": ""
  },
  "repair_actions": [""],
  "repaired_script": {}
}

输出必须是合法 JSON，不要输出 markdown，不要输出解释文字。"""


PROMPT_P7_VIDEO = """你是最终视频提示词生成器。

你的任务是基于“已经通过质检的脚本”，生成一版干净、紧凑、适合视频生成模型使用的最终视频提示词。

你的职责只有一个：干净转写。
你必须忠实执行脚本，不得重新分析脚本，不得补充策略解释，不得做自检、自证、质检说明。

输入信息：
- target_country: {{target_country}}
- target_language: {{target_language}}
- product_type: {{product_type}}
- account_style_boundary: {{account_style_boundary}}
- anchor_card_json: {{anchor_card_json}}
- final_strategy_json: {{final_strategy_json}}
- script_json: {{script_json}}

要求：
- 删除所有自检、自证、质检、解释性内容
- 不要输出“为什么这样设计”
- 除口播字段外，其它所有字段一律使用中文描述
- 最终喂视频模型的分镜层不要输出 `shot_purpose`、`anchor_reference`、`task_type`、字幕字段
- 分镜层只保留：镜头编号、时长、镜头内容、人物动作、口播、口播任务、必要风格提醒
- 必须保留 P7 中的核心人物表现细节：眼神路径、微反应、身体姿态、低风险商品互动、表情变化
- 必须保留 P7 performance 中的核心信息：gaze / expression_or_micro_reaction / body_language / product_interaction
- 不得把 performance beat 降级成“女生戴着商品对镜微笑 / 人物自然展示商品 / 女生自然微笑展示耳环”
- 应保留为“人物先看镜子里的整体效果，轻微观察后自然笑一下，随后短暂看向镜头像朋友分享”这类具体执行
- 如果 display_family = apparel_accessory，必须保留 season_context（冬季 / 冷天气 / 出门 / 旅行氛围）、co_styling_hint（外套 / 针织衫 / 上半身搭配关系）、配饰和上半身 / 头肩 / 脸部比例关系、镜前出门自检状态、轻整理动作、不做强功效承诺
- apparel_accessory 不得降级成“美女戴帽子微笑展示 / 女生围着围巾开心看镜头 / 模特展示冬季围巾帽子”；应保留为“人物在镜前轻整理围巾边缘，半步后退看上半身整体，围巾和冬季外套让出门穿搭更完整”这类具体执行
- 最终渲染给视频模型的文本必须尽量控制在 1800 字符以内，硬上限 2000 字符；因此字段内容必须短、准、可执行
- `voiceover_text_zh` 默认留空字符串，除非调用方后续明确要求保留中文对照
- `style_note` 只保留该镜头独有的提醒；如果某条提醒已经在全局执行边界里出现，不要在每个镜头重复写
- 把重复的风格提醒尽量上提到 `execution_boundary`
- `video_setup` 和 `execution_boundary` 都要用短句，不要写成长段方法论或重复限制
- `video_setup` 里必须显式保留 1-3 个最关键的商品锚点，优先写成短句，例如“商品锚点：xxx / xxx”，不要只保留空泛风格词
- `execution_boundary` 里必须显式写出锚点执行要求，例如“至少 1 镜清楚交代 xxx”，不要只写泛化拍摄原则
- `shot_execution` 里至少有 1 个镜头要直接服务于最关键商品锚点，不能所有镜头都只剩泛化氛围描述
- 如果 `anchor_card_json.parameter_anchors` 非空，不得在最终视频提示词里改写这些参数事实；关键参数可在 `video_setup` 或相关镜头里轻量保留
- 如果 `anchor_card_json.key_visual_constraints` 存在 high / medium 约束，最终视频提示词必须遵守并轻量保留，不得把关键视觉比例、落点、方向、佩戴方式、体量级别、版型比例写错
- 输出结构收敛为：视频整体设定 / 分镜执行 / 统一执行边界
- 必须输出 `sound_design`，它只描述听感结构，不改变镜头和口播内容；口播是信息主线，BGM 只做持续情绪底色

{{ai_video_rhythm_rule}}

请输出 JSON，包含：
1. video_setup
2. shot_execution
3. sound_design
4. execution_boundary

JSON 结构如下：
{
  "video_setup": "时长 / 场景 / 人物状态 / 穿搭底盘边界 / 商品呈现重点 / 整体风格",
  "shot_execution": [
    {
      "shot_no": 1,
      "duration": "",
      "shot_content": "",
      "voiceover_text_target_language": "",
      "voiceover_text_zh": "",
      "spoken_line_task": "hook|proof|decision|proof+decision|none",
      "person_action": "",
      "performance": {
        "gaze": "",
        "expression_or_micro_reaction": "",
        "body_language": "",
        "product_interaction": ""
      },
      "style_note": ""
    }
  ],
  "sound_design": {
    "bgm": "清晰可感知的无歌词背景音乐，中低音量，持续作为情绪底色",
    "voiceover_mix": "口播是信息主线，BGM 在口播出现时自动压低，不盖过口播",
    "rhythm_relation": "音乐可轻度贴合镜头切换、手部动作和商品近景停顿，但不改变既定动作链",
    "sfx": "少量且只服务明确画面动作"
  },
  "execution_boundary": ""
}"""


PROMPT_P8 = """你是一个资深的短视频脚本变体设计专家。

你的任务是：基于一条已经成立的正式脚本，生成 {{variant_count}} 个轻变体版本。

【目标】
测试：
1. 哪种前3秒更容易让用户继续看
2. 哪种中段更能把开头接实
3. 哪种结尾更自然地收住购买意向

【输入信息】
- target_country: {{target_country}}
- target_language: {{target_language}}
- product_type: {{product_type}}
- anchor_card_json: {{anchor_card_json}}
- final_strategy_json: {{final_strategy_json}}
- expression_plan_json: {{expression_plan_json}}
- persona_style_emotion_pack_json: {{persona_style_emotion_pack_json}}
- original_script_json: {{original_script_json}}
- source_script_id: {{source_script_id}}
- source_strategy_id: {{source_strategy_id}}
- canonical_strategy_id: {{canonical_strategy_id}}
- direction_allowed_pool_json: {{direction_allowed_pool_json}}
- person_variant_layer_json: {{person_variant_layer_json}}
- outfit_variant_layer_json: {{outfit_variant_layer_json}}
- scene_variant_layer_json: {{scene_variant_layer_json}}
- emotion_variant_layer_json: {{emotion_variant_layer_json}}
- variant_plan_json: {{variant_plan_json}}
- debug_mode: {{debug_mode}}

本次只输出以下 variant_id：
- {{variant_ids}}

【必须保留不变的部分】
1. 商品本体及关键锚点；
2. 原脚本唯一主卖点；
3. 原策略包核心任务；
4. 原表达扩充计划主线方向；
5. 商品优先原则；
6. 账号调性：真实、轻精致、自然、不硬广、可挂主页；
7. 如果 target_country 属于东南亚市场，家中自然分享场景高权重原则不能被破坏。

【必须满足】
1. 每条变体都必须覆盖 hook / proof / decision；
2. 每条变体默认使用 4–6 个镜头推进；
3. 单镜头默认 1–3 秒，尽量不超过 4 秒；
4. 中段不能只写情绪陪衬；
5. 结尾不能只写泛安利；
6. 不允许所有变体只是开头不同，中后段逻辑完全一样；
7. S4 即使做变体，也不得滑向广告片；首镜后必须尽快进入 proof，不得空钩子。
8. `source_strategy_id` 必须严格等于输入里的 `source_strategy_id`；`strategy_id` 必须严格等于输入里的 `canonical_strategy_id`。
9. 不要把 `strategy_id` 写成 `source_strategy_id`，不要写成 `Final_S1/Final_S2/Final_S3/Final_S4`，也不要写成 `S1_V1/S4_V2` 这类变体化命名。
10. 如果 target_country 属于东南亚市场，`final_video_script_prompt.video_setup.scene_final` 必须明确写出家中自然分享子场景，例如“家中穿衣区/镜前”“家中衣柜/穿衣区”“家中梳妆台/桌边”“家中窗边自然光”“家中床边/坐姿分享”“家中玄关镜前”，不要只写“生活化真实场景”“真实场景”“日常场景”这类泛标签。
11. 如果 target_country 属于东南亚市场，优先让 V1/V2/V3 落在明确的家中自然分享子场景，并通过不同子场景拉开差异。
12. 每个 `shot_execution.voiceover` 只允许使用目标语言 {{target_language}}，不得输出中文；如果无口播则留空字符串，不要写“无”；所有中文说明只能放在 visual / person_action / product_focus / style_boundaries 等控制字段。

{{ai_video_rhythm_rule}}

{{hair_accessory_rules}}

{{product_selling_note_rules}}

输出必须是合法 JSON，不要输出 markdown，不要输出解释文字。
输出 JSON 结构如下：
{
  "variant_count": {{variant_count}},
  "variants": [
    {
      "variant_id": "V1|V2|V3|V4|V5",
      "variant_no": 1,
      "variant_strength": "light|medium|heavy",
      "variant_focus": "opening|proof|ending|scene|rhythm|persona|action|outfit|emotion",
      "source_script_id": "",
      "source_strategy_id": "",
      "strategy_id": "",
      "strategy_name": "",
      "primary_selling_point": "",
      "final_video_script_prompt": {
        "video_setup": {
          "video_theme": "",
          "product_focus": "",
          "person_final": "",
          "outfit_final": "",
          "scene_final": "",
          "emotion_final": "",
          "overall_style": ""
        },
        "shot_execution": [
          {
            "shot_no": 1,
            "duration": "",
            "visual": "",
            "person_action": "",
            "product_focus": "",
            "voiceover": ""
          }
        ],
        "style_boundaries": [""]
      },
      "internal_variant_state": {
        "variant_name": "",
        "main_adjustment": "",
        "test_goal": "",
        "variant_change_summary": "",
        "inherited_core_items": [""],
        "changed_structure_fields": [""],
        "changed_feeling_layers": ["person|outfit|scene|emotion"],
        "main_change": "",
        "secondary_change": "",
        "difference_summary": "",
        "coverage": ["hook", "proof", "decision"],
        "proof_blueprint": [
          {
            "anchor": "",
            "action": "",
            "visible_result": "",
            "concern_relieved": ""
          }
        ],
        "person_variant_layer": {
          "person_identity_base": "",
          "person_style_base": "",
          "appearance_boundary": "",
          "body_presentation_boundary": "",
          "camera_relationship": ""
        },
        "outfit_variant_layer": {
          "outfit_core_formula": "",
          "product_role_in_outfit": "",
          "silhouette_boundary": "",
          "pairing_boundary": "",
          "color_mood_boundary": ""
        },
        "scene_variant_layer": {
          "scene_domain_base": "",
          "scene_subspace": "",
          "scene_function_moment": "",
          "light_boundary": "",
          "prop_boundary": ""
        },
        "emotion_variant_layer": {
          "emotion_base": "",
          "emotion_curve": "",
          "emotion_intensity_boundary": "",
          "delivery_boundary": ""
        },
        "consistency_checks": {
          "person_manifestation": "",
          "outfit_manifestation": "",
          "scene_manifestation": "",
          "emotion_manifestation": ""
        }
      }
    }
  ]
}

{{repair_block}}"""


def build_anchor_card_prompt(
    target_country: str,
    target_language: str,
    product_type: str,
    product_selling_note: str = "",
    product_parameter_info: str = "",
    hair_clip_mode: bool = False,
    type_guard_json: Optional[Dict[str, Any]] = None,
) -> str:
    del hair_clip_mode
    prompt = _fill_template(
        PROMPT_P1,
        {
            "target_country": target_country,
            "target_language": target_language,
            "product_type": product_type,
            "product_selling_note": _optional_note_text(product_selling_note),
            "product_parameter_info": _optional_note_text(product_parameter_info),
            "hair_accessory_rules": _hair_accessory_anchor_rules(product_type),
            "category_execution_contract_rules": CATEGORY_EXECUTION_CONTRACT_ANCHOR_RULE,
            "product_selling_note_rules": PRODUCT_SELLING_NOTE_RULES,
        },
    )
    prompt = _with_prompt_cache_prefix("P1_anchor_card", prompt)
    return _append_type_guard_block(prompt, type_guard_json)


def build_product_type_guard_prompt(
    table_product_type: str,
    business_category: str = "",
) -> str:
    return _fill_template(
        PROMPT_PRODUCT_TYPE_GUARD,
        {
            "table_product_type": table_product_type or "(空)",
            "business_category": business_category or "(空)",
        },
    )


def build_opening_strategy_prompt(
    target_country: str,
    target_language: str,
    product_type: str,
    anchor_card_json: Dict[str, Any],
    product_selling_note: str = "",
    type_guard_json: Optional[Dict[str, Any]] = None,
) -> str:
    del product_selling_note
    prompt = _fill_template(
        PROMPT_P2,
        {
            "target_country": target_country,
            "target_language": target_language,
            "product_family": _product_family_from_type_guard(product_type, type_guard_json),
            "anchor_card_json": _compact_json(anchor_card_json),
            "hair_accessory_rules": _hair_accessory_opening_rules(product_type),
        },
    )
    return _append_type_guard_block(prompt, type_guard_json)


def build_styling_plan_prompt(
    target_country: str,
    target_language: str,
    product_type: str,
    anchor_card_json: Dict[str, Any],
    product_selling_note: str = "",
    type_guard_json: Optional[Dict[str, Any]] = None,
) -> str:
    del target_language, product_selling_note
    prompt = _fill_template(
        PROMPT_P3,
        {
            "target_country": target_country,
            "product_family": _product_family_from_type_guard(product_type, type_guard_json),
            "anchor_card_json": _compact_json(anchor_card_json),
            "performance_profile_json": _compact_json(
                _performance_profile_for_prompt(anchor_card_json, product_type, type_guard_json)
            ),
            "light_control_rules": PERSONA_LIGHT_CONTROL_RULES,
            "light_control_pool": PERSONA_LIGHT_CONTROL_POOL,
            "hair_accessory_rules": _hair_accessory_persona_rules(product_type),
            "human_performance_contract_rules": HUMAN_PERFORMANCE_CONTRACT_RULE,
        },
    )
    return _append_type_guard_block(prompt, type_guard_json)


def build_strategy_prompt(
    target_country: str,
    target_language: str,
    product_type: str,
    anchor_card_json: Dict[str, Any],
    opening_strategies_json: Dict[str, Any],
    persona_style_emotion_pack_json: Dict[str, Any],
    product_selling_note: str = "",
    repair_instruction: str = "",
    hair_accessory_mode: bool = False,
    hair_clip_mode: bool = False,
    clip_expression_mode: str = "",
    type_guard_json: Optional[Dict[str, Any]] = None,
) -> str:
    del hair_accessory_mode, hair_clip_mode, clip_expression_mode
    repair_block = f"\n附加修正要求：\n{repair_instruction.strip()}\n" if repair_instruction.strip() else ""
    prompt = _fill_template(
        PROMPT_P4,
        {
            "target_country": target_country,
            "target_language": target_language,
            "product_family": _product_family_from_type_guard(product_type, type_guard_json),
            "anchor_card_json": _compact_json(anchor_card_json),
            "opening_strategies_json": _compact_json(opening_strategies_json),
            "persona_style_emotion_pack_json": _compact_json(persona_style_emotion_pack_json),
            "account_style_boundary": ACCOUNT_STYLE_BOUNDARY,
            "fixed_pool": OPENING_FIXED_POOL,
            "light_control_defaults": PERSONA_LIGHT_CONTROL_DEFAULTS,
            "category_execution_contract_rules": CATEGORY_EXECUTION_CONTRACT_STRATEGY_RULE,
            "hair_accessory_rules": _hair_accessory_strategy_rules(product_type),
            "product_selling_note_rules": PRODUCT_SELLING_NOTE_RULES,
            "repair_block": repair_block,
        },
    )
    prompt = _with_prompt_cache_prefix("P4_strategy_candidates", prompt)
    return _append_type_guard_block(prompt, type_guard_json)


def build_final_strategy_prompt(
    target_country: str,
    target_language: str,
    product_type: str,
    anchor_card_json: Dict[str, Any],
    product_selling_note: str = "",
    opening_strategies_json: Optional[Dict[str, Any]] = None,
    styling_plans_json: Optional[Dict[str, Any]] = None,
    strategies_json: Optional[Dict[str, Any]] = None,
    repair_instruction: str = "",
    hair_accessory_mode: bool = False,
    hair_clip_mode: bool = False,
    clip_expression_mode: str = "",
    type_guard_json: Optional[Dict[str, Any]] = None,
) -> str:
    del product_selling_note, hair_accessory_mode, hair_clip_mode, clip_expression_mode
    prompt = _fill_template(
        PROMPT_P5,
        {
            "product_type": product_type,
            "target_country": target_country,
            "target_language": target_language,
            "anchor_card_json": _compact_json(anchor_card_json),
            "opening_strategies_json": _compact_json(opening_strategies_json or {}),
            "persona_style_emotion_pack_json": _compact_json(styling_plans_json or {}),
            "strategies_json": _compact_json(strategies_json or {}),
            "category_execution_contract_rules": CATEGORY_EXECUTION_CONTRACT_STRATEGY_RULE,
            "hair_accessory_rules": _hair_accessory_strategy_rules(product_type),
        }
    )
    prompt = _with_prompt_cache_prefix("P5_strategy_cards", prompt)
    prompt = _append_type_guard_block(prompt, type_guard_json)
    return prompt + (f"\n\n附加修正要求：\n{repair_instruction.strip()}" if repair_instruction.strip() else "")


def build_expression_plan_prompt(
    target_country: str,
    target_language: str,
    product_type: str,
    anchor_card_json: Dict[str, Any],
    final_strategy_json: Dict[str, Any],
    product_selling_note: str = "",
    persona_style_emotion_pack_json: Optional[Dict[str, Any]] = None,
    type_guard_json: Optional[Dict[str, Any]] = None,
) -> str:
    del product_selling_note
    prompt = _fill_template(
        PROMPT_P6,
        {
            "target_country": target_country,
            "target_language": target_language,
            "product_family": _product_family_from_type_guard(product_type, type_guard_json),
            "anchor_card_json": _compact_json(anchor_card_json),
            "final_strategy_json": _compact_json(final_strategy_json),
            "persona_style_emotion_pack_json": _compact_json(persona_style_emotion_pack_json or {}),
            "account_style_boundary": ACCOUNT_STYLE_BOUNDARY,
            "hair_accessory_rules": _hair_accessory_expression_rules(product_type),
        },
    )
    return _append_type_guard_block(prompt, type_guard_json)


def build_script_prompt(
    target_country: str,
    target_language: str,
    product_type: str,
    script_brief_json: Dict[str, Any],
    product_selling_note: str = "",
    existing_script_jsons: Optional[Dict[str, Dict[str, Any]]] = None,
    current_script_json: Optional[Dict[str, Any]] = None,
    repair_instruction: str = "",
    hair_accessory_mode: bool = False,
    hair_clip_mode: bool = False,
    clip_expression_mode: str = "",
    type_guard_json: Optional[Dict[str, Any]] = None,
) -> str:
    del product_selling_note, existing_script_jsons, hair_accessory_mode, hair_clip_mode, clip_expression_mode
    prompt = _fill_template(
        PROMPT_P7,
        {
            "target_country": target_country,
            "target_language": target_language,
            "product_type": product_type,
            "script_brief_json": _compact_json(_compact_script_brief_for_p7(script_brief_json)),
            "category_execution_contract_rules": P7_CATEGORY_EXECUTION_CONTRACT_RULE,
            "human_performance_script_rules": P7_HUMAN_PERFORMANCE_SCRIPT_RULE,
            "hair_accessory_rules": _hair_accessory_script_rules(product_type),
            "ai_video_rhythm_rule": AI_VIDEO_RHYTHM_RULE,
            "audio_layer_rule": P7_AUDIO_LAYER_RULE,
        },
    )
    prompt = _with_prompt_cache_prefix("P7_script", prompt)
    prompt = _append_type_guard_block(prompt, type_guard_json)
    extras: List[str] = []
    if isinstance(current_script_json, dict) and current_script_json:
        extras.append(
            "当前待修脚本 JSON：\n"
            f"{_compact_json(current_script_json)}\n"
            "这是一轮基于现有脚本的定向修订，请优先在当前脚本上做必要重排和修正，不要重新发明主卖点。"
        )
    if repair_instruction.strip():
        extras.append(f"附加修正要求：\n{repair_instruction.strip()}")
    if extras:
        prompt = f"{prompt}\n\n" + "\n\n".join(extras)
    return prompt


def build_script_review_prompt(
    target_country: str,
    product_type: str,
    anchor_card_json: Dict[str, Any],
    final_strategy_json: Dict[str, Any],
    expression_plan_json: Dict[str, Any],
    persona_style_emotion_pack_json: Dict[str, Any],
    script_json: Dict[str, Any],
    target_language: str = "",
    type_guard_json: Optional[Dict[str, Any]] = None,
    pre_qc_result: Optional[Dict[str, Any]] = None,
) -> str:
    q1_context = _compact_q1_context(
        anchor_card_json=anchor_card_json,
        final_strategy_json=final_strategy_json,
        expression_plan_json=expression_plan_json,
        persona_style_emotion_pack_json=persona_style_emotion_pack_json,
        pre_qc_result=pre_qc_result,
    )
    prompt = _fill_template(
        PROMPT_Q1,
        {
            "target_country": target_country,
            "target_language": target_language,
            "product_family": _product_family_from_type_guard(product_type, type_guard_json),
            "q1_context_json": _compact_json(q1_context),
            "script_json": _compact_json(script_json),
            "category_execution_contract_rules": CATEGORY_EXECUTION_CONTRACT_QC_RULE,
            "human_stiffness_qc_rule": HUMAN_STIFFNESS_QC_RULE,
            "hair_accessory_rules": _hair_accessory_qc_rules(product_type),
            "ai_video_rhythm_rule": AI_VIDEO_RHYTHM_RULE,
            "audio_layer_rule": AUDIO_LAYER_RULE,
        },
    )
    prompt = _with_prompt_cache_prefix("Q1_review", prompt)
    if pre_qc_result:
        prompt = (
            f"{prompt}\n\n"
            "【代码侧 Q1 precheck 结果补充说明】\n"
            "执行要求：\n"
            "- 如果 precheck 已标记 high-confidence major，不要重新辩论是否违规，只做最小修正或保留为 major_issues；\n"
            "- 如果 precheck 只是 warnings，不要强拦；\n"
            "- repaired_script 必须同时满足 precheck 和语义质检。"
        )
    return _append_type_guard_block(prompt, type_guard_json)


def build_script_revision_prompt(
    anchor_card_json: Dict[str, Any],
    final_strategy_json: Dict[str, Any],
    expression_plan_json: Dict[str, Any],
    script_json: Dict[str, Any],
    review_json: Dict[str, Any],
) -> str:
    return _compact_json(
        {
            "anchor_card_json": anchor_card_json,
            "final_strategy_json": final_strategy_json,
            "expression_plan_json": expression_plan_json,
            "script_json": script_json,
            "review_json": review_json,
        }
    )


def build_final_video_prompt_prompt(
    target_country: str,
    target_language: str,
    product_type: str,
    anchor_card_json: Dict[str, Any],
    final_strategy_json: Dict[str, Any],
    script_json: Dict[str, Any],
    type_guard_json: Optional[Dict[str, Any]] = None,
) -> str:
    prompt = _fill_template(
        PROMPT_P7_VIDEO,
        {
            "target_country": target_country,
            "target_language": target_language,
            "product_type": product_type,
            "account_style_boundary": ACCOUNT_STYLE_BOUNDARY,
            "anchor_card_json": _compact_json(anchor_card_json),
            "final_strategy_json": _compact_json(final_strategy_json),
            "script_json": _compact_json(script_json),
            "ai_video_rhythm_rule": AI_VIDEO_RHYTHM_RULE,
        },
    )
    return _append_type_guard_block(prompt, type_guard_json)


def build_variant_prompt(
    target_country: str,
    target_language: str,
    product_type: str,
    anchor_card_json: Dict[str, Any],
    final_strategy_json: Dict[str, Any],
    expression_plan_json: Dict[str, Any],
    original_script_json: Dict[str, Any],
    source_script_id: str,
    source_strategy_id: str,
    direction_allowed_pool_json: Dict[str, Any],
    person_variant_layer_json: Dict[str, Any],
    outfit_variant_layer_json: Dict[str, Any],
    scene_variant_layer_json: Dict[str, Any],
    emotion_variant_layer_json: Dict[str, Any],
    variant_plan_json: List[Dict[str, Any]],
    debug_mode: bool = True,
    product_selling_note: str = "",
    repair_instruction: str = "",
    variant_ids: Optional[List[str]] = None,
    persona_style_emotion_pack_json: Optional[Dict[str, Any]] = None,
    type_guard_json: Optional[Dict[str, Any]] = None,
) -> str:
    repair_block = f"\n附加修正要求：\n{repair_instruction.strip()}\n" if repair_instruction.strip() else ""
    resolved_variant_ids = variant_ids or ["V1", "V2", "V3", "V4", "V5"]
    prompt = _fill_template(
        PROMPT_P8,
        {
            "variant_count": len(resolved_variant_ids),
            "target_country": target_country,
            "target_language": target_language,
            "product_type": product_type,
            "anchor_card_json": _compact_json(anchor_card_json),
            "final_strategy_json": _compact_json(final_strategy_json),
            "expression_plan_json": _compact_json(expression_plan_json),
            "persona_style_emotion_pack_json": _compact_json(persona_style_emotion_pack_json or {}),
            "original_script_json": _compact_json(original_script_json),
            "source_script_id": source_script_id,
            "source_strategy_id": source_strategy_id,
            "canonical_strategy_id": str(final_strategy_json.get("strategy_id", "") or "").strip(),
            "direction_allowed_pool_json": _compact_json(direction_allowed_pool_json),
            "person_variant_layer_json": _compact_json(person_variant_layer_json),
            "outfit_variant_layer_json": _compact_json(outfit_variant_layer_json),
            "scene_variant_layer_json": _compact_json(scene_variant_layer_json),
            "emotion_variant_layer_json": _compact_json(emotion_variant_layer_json),
            "variant_plan_json": _compact_json(variant_plan_json),
            "debug_mode": "true" if debug_mode else "false",
            "variant_ids": ", ".join(resolved_variant_ids),
            "hair_accessory_rules": _hair_accessory_variant_rules(product_type),
            "product_selling_note_rules": PRODUCT_SELLING_NOTE_RULES,
            "repair_block": repair_block,
            "ai_video_rhythm_rule": AI_VIDEO_RHYTHM_RULE,
        },
    )
    return _append_type_guard_block(prompt, type_guard_json)


def build_p2_opening_prompt(
    target_country: str,
    target_language: str,
    product_type: str,
    anchor_card_json: Dict[str, Any],
    product_selling_note: str = "",
    type_guard_json: Optional[Dict[str, Any]] = None,
) -> str:
    return build_opening_strategy_prompt(
        target_country=target_country,
        target_language=target_language,
        product_type=product_type,
        anchor_card_json=anchor_card_json,
        product_selling_note=product_selling_note,
        type_guard_json=type_guard_json,
    )


def build_p6_expression_plan_prompt(
    target_country: str,
    target_language: str,
    product_type: str,
    anchor_card_json: Dict[str, Any],
    final_s1_json: Dict[str, Any],
    product_selling_note: str = "",
    final_s2_json: Optional[Dict[str, Any]] = None,
    final_s3_json: Optional[Dict[str, Any]] = None,
    final_s4_json: Optional[Dict[str, Any]] = None,
    type_guard_json: Optional[Dict[str, Any]] = None,
) -> str:
    del final_s2_json, final_s3_json, final_s4_json
    return build_expression_plan_prompt(
        target_country=target_country,
        target_language=target_language,
        product_type=product_type,
        anchor_card_json=anchor_card_json,
        final_strategy_json=final_s1_json,
        product_selling_note=product_selling_note,
        type_guard_json=type_guard_json,
    )


def build_p8_variant_prompt(
    target_country: str,
    target_language: str,
    product_type: str,
    anchor_card_json: Dict[str, Any],
    final_strategy_json: Dict[str, Any],
    expression_plan_json: Dict[str, Any],
    original_script_json: Dict[str, Any],
    source_script_id: str,
    source_strategy_id: str,
    direction_allowed_pool_json: Dict[str, Any],
    person_variant_layer_json: Dict[str, Any],
    outfit_variant_layer_json: Dict[str, Any],
    scene_variant_layer_json: Dict[str, Any],
    emotion_variant_layer_json: Dict[str, Any],
    variant_plan_json: List[Dict[str, Any]],
    debug_mode: bool = True,
    product_selling_note: str = "",
    repair_instruction: str = "",
    variant_ids: Optional[List[str]] = None,
    persona_style_emotion_pack_json: Optional[Dict[str, Any]] = None,
    type_guard_json: Optional[Dict[str, Any]] = None,
) -> str:
    return build_variant_prompt(
        target_country=target_country,
        target_language=target_language,
        product_type=product_type,
        anchor_card_json=anchor_card_json,
        final_strategy_json=final_strategy_json,
        expression_plan_json=expression_plan_json,
        original_script_json=original_script_json,
        source_script_id=source_script_id,
        source_strategy_id=source_strategy_id,
        direction_allowed_pool_json=direction_allowed_pool_json,
        person_variant_layer_json=person_variant_layer_json,
        outfit_variant_layer_json=outfit_variant_layer_json,
        scene_variant_layer_json=scene_variant_layer_json,
        emotion_variant_layer_json=emotion_variant_layer_json,
        variant_plan_json=variant_plan_json,
        debug_mode=debug_mode,
        product_selling_note=product_selling_note,
        repair_instruction=repair_instruction,
        variant_ids=variant_ids,
        persona_style_emotion_pack_json=persona_style_emotion_pack_json,
        type_guard_json=type_guard_json,
    )
