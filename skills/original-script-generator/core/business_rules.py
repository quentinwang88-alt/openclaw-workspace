#!/usr/bin/env python3
"""
原创脚本生成业务规则。
"""

import re
from typing import Any, Dict, List, Optional, Tuple

from core.constants import (
    HOME_SHARE_SCENE_KEYWORDS,
    SEA_COUNTRIES,
    SEA_COUNTRY_ALIASES,
    SEA_HOME_PRIORITY_SCENES,
    SEA_HOME_SCENE_MIN_COUNT,
    SCRIPT_ROLES,
)


def normalize_country(target_country: str) -> str:
    if not target_country:
        return ""
    return re.sub(r"\s+", " ", target_country.strip().lower())


def is_sea_market(target_country: str) -> bool:
    return normalize_country(target_country) in SEA_COUNTRY_ALIASES


def canonical_sea_country(target_country: str) -> str:
    return SEA_COUNTRY_ALIASES.get(normalize_country(target_country), "")


def normalize_text(value: str) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", "", value.strip().lower())


def is_home_share_scene(scene_text: str) -> bool:
    normalized = normalize_text(scene_text)
    if normalized in {"h1窗边自然光", "h2镜前/玄关镜前", "h3梳妆台/桌边", "h4床边/坐姿分享", "h5衣柜/穿衣区"}:
        return True
    return any(normalize_text(keyword) in normalized for keyword in HOME_SHARE_SCENE_KEYWORDS)


def preferred_sea_scene_order() -> List[str]:
    return [
        "H1 窗边自然光",
        "H2 镜前/玄关镜前",
        "H3 梳妆台/桌边",
        "H4 床边/坐姿分享",
        "H5 衣柜/穿衣区",
    ]


def extract_scene_signature(scene_text: str) -> str:
    normalized = normalize_text(scene_text)
    for keyword in SEA_HOME_PRIORITY_SCENES:
        if normalize_text(keyword) in normalized:
            return normalize_text(keyword)
    return normalized


def _field_value(strategy: Dict[str, object], key: str) -> str:
    value = strategy.get(key)
    if isinstance(value, list):
        return normalize_text(" ".join(str(item) for item in value if item))
    return normalize_text(str(value or ""))


def _contains_unwanted_ad_expression(text: str) -> bool:
    normalized = normalize_text(text)
    if not normalized:
        return False

    blocked_tokens = ("棚拍", "商拍", "广告片", "studio", "campaign", "heroshot", "大片感")
    negative_guards = ("避免", "不要", "不得", "别", "非", "不是", "avoid", "no", "not", "without")

    for token in blocked_tokens:
        for match in re.finditer(re.escape(token), normalized):
            prefix = normalized[max(0, match.start() - 8):match.start()]
            if any(guard in prefix for guard in negative_guards):
                continue
            return True
    return False


def _parse_duration_seconds(value: Any) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    matches = re.findall(r"(\d+(?:\.\d+)?)", text)
    if not matches:
        return 0.0
    try:
        numbers = [float(item) for item in matches]
    except ValueError:
        return 0.0
    if not numbers:
        return 0.0
    if len(numbers) >= 2 and any(token in text for token in ("-", "~", "至", "到")):
        return max(numbers[:2])
    return numbers[0]


def _expand_spoken_task(task: str) -> List[str]:
    normalized = str(task or "").strip()
    if normalized == "proof+decision":
        return ["proof", "decision"]
    if normalized:
        return [normalized]
    return []


def _extract_timed_storyboard_nodes(script_json: Dict[str, object]) -> List[Dict[str, object]]:
    storyboard = _collect_storyboard_shots(script_json)
    nodes: List[Dict[str, object]] = []
    current_start = 0.0
    for index, shot in enumerate(storyboard, 1):
        duration = _parse_duration_seconds(shot.get("duration"))
        end_time = current_start + duration if duration > 0 else current_start
        nodes.append(
            {
                "shot_no": index,
                "start": current_start,
                "end": end_time,
                "tasks": _expand_spoken_task(str(shot.get("spoken_line_task", "") or "")),
            }
        )
        current_start = end_time
    return nodes


def validate_script_time_nodes(
    final_strategy: Dict[str, object],
    script_json: Dict[str, object],
) -> Tuple[List[str], List[str]]:
    warnings: List[str] = []
    violations: List[str] = []

    nodes = _extract_timed_storyboard_nodes(script_json)
    if not nodes:
        return warnings, ["脚本缺少可用于 15 秒硬节点校验的 storyboard 时序"]

    hook_completion: Optional[float] = None
    proof_starts: List[float] = []
    decision_starts: List[float] = []

    contiguous_hook = True
    for node in nodes:
        tasks = node.get("tasks") if isinstance(node.get("tasks"), list) else []
        start = float(node.get("start") or 0.0)
        end = float(node.get("end") or start)
        task_set = {str(item or "").strip() for item in tasks if str(item or "").strip()}

        if contiguous_hook and "hook" in task_set:
            hook_completion = end
        elif task_set - {"none"}:
            contiguous_hook = False

        if "proof" in task_set:
            proof_starts.append(start)
        if "decision" in task_set:
            decision_starts.append(start)

    if hook_completion is None:
        violations.append("未检测到 hook 时序，无法完成前 3 秒停留校验")
    elif hook_completion > 3.0:
        warnings.append(f"hook 完成时间约 {hook_completion:.1f}s，超过前 3 秒节点")

    if not proof_starts:
        violations.append("未检测到 proof 起始节点，无法完成 4–8 秒 proof 校验")
    elif not any(4.0 <= item <= 8.0 for item in proof_starts):
        earliest_proof = min(proof_starts)
        warnings.append(f"核心 proof 起始时间约 {earliest_proof:.1f}s，未落在 4–8 秒区间")

    decision_deadline = 9.0 if str(final_strategy.get("script_role", "") or "").strip() == "risk_resolution" else 12.0
    if not decision_starts:
        violations.append("未检测到 decision 信号，无法完成决策收束节点校验")
    else:
        earliest_decision = min(decision_starts)
        if earliest_decision > decision_deadline:
            message = f"decision 信号约在 {earliest_decision:.1f}s 出现，晚于 {decision_deadline:.0f}s 节点"
            if decision_deadline <= 9.0 or earliest_decision - decision_deadline >= 1.0:
                violations.append(message)
            else:
                warnings.append(message)

    return warnings, violations


def count_home_share_strategies(strategies: List[Dict[str, object]]) -> int:
    count = 0
    for strategy in strategies:
        candidates = [
            str(strategy.get("scene_subspace", "") or ""),
            str(strategy.get("opening_first_shot", "") or ""),
            str(strategy.get("opening_strategy", "") or ""),
        ]
        if any(is_home_share_scene(item) for item in candidates if item):
            count += 1
    return count


def strategies_too_similar(strategies: List[Dict[str, object]]) -> bool:
    if len(strategies) < 4:
        return True

    signatures = {
        (
            _field_value(strategy, "script_role"),
            _field_value(strategy, "primary_focus"),
            _field_value(strategy, "secondary_focus"),
            _field_value(strategy, "opening_mode"),
            _field_value(strategy, "proof_mode"),
            _field_value(strategy, "ending_mode"),
            _field_value(strategy, "scene_subspace"),
            _field_value(strategy, "visual_entry_mode"),
            _field_value(strategy, "persona_state"),
            _field_value(strategy, "action_entry_mode"),
            _field_value(strategy, "dominant_user_question"),
            _field_value(strategy, "proof_thesis"),
            _field_value(strategy, "decision_thesis"),
            _field_value(strategy, "styling_completion_tag"),
            _field_value(strategy, "persona_visual_tone"),
            _field_value(strategy, "styling_key_anchor"),
            _field_value(strategy, "emotion_arc_tag"),
        )
        for strategy in strategies
    }
    return len(signatures) < len(strategies)


def validate_strategy_distribution(
    target_country: str,
    strategies: List[Dict[str, object]],
) -> Optional[str]:
    if len(strategies) != 4:
        return f"策略数量必须为 4，当前为 {len(strategies)}"

    strategy_ids = [str(item.get("strategy_id", "") or "").strip() for item in strategies]
    if strategy_ids != ["S1", "S2", "S3", "S4"]:
        return f"strategy_id 顺序必须为 S1/S2/S3/S4，当前为 {strategy_ids}"

    if len({_field_value(item, "opening_mode") for item in strategies}) < 2:
        return "4 套策略的 opening_mode 区分度不足"
    script_roles = {_field_value(item, "script_role") for item in strategies if _field_value(item, "script_role")}
    if script_roles and script_roles != {normalize_text(item) for item in SCRIPT_ROLES}:
        return "script_role 需要完整覆盖 4 种固定角色"
    if len({_field_value(item, "proof_mode") for item in strategies}) < 3:
        return "proof_mode 少于 3 种"
    if len({_field_value(item, "ending_mode") for item in strategies}) < 3:
        return "ending_mode 少于 3 种"
    if len({_field_value(item, "visual_entry_mode") for item in strategies}) < 3:
        return "visual_entry_mode 少于 3 种"
    if len({_field_value(item, "persona_state") for item in strategies}) < 2:
        return "persona_state 少于 2 种"
    if len({_field_value(item, "action_entry_mode") for item in strategies}) < 3:
        return "action_entry_mode 少于 3 种"
    if len({_field_value(item, "dominant_user_question") for item in strategies}) < 3:
        return "dominant_user_question 区分度不足"
    if len({_field_value(item, "proof_thesis") for item in strategies}) < 3:
        return "proof_thesis 区分度不足"
    if len({_field_value(item, "decision_thesis") for item in strategies}) < 3:
        return "decision_thesis 区分度不足"
    if len({_field_value(item, "styling_completion_tag") for item in strategies}) < 2:
        return "styling_completion_tag 少于 2 种"
    if len({_field_value(item, "persona_visual_tone") for item in strategies}) < 2:
        return "persona_visual_tone 少于 2 种"
    if len({_field_value(item, "emotion_arc_tag") for item in strategies}) < 2:
        return "emotion_arc_tag 少于 2 种"
    if len({_field_value(item, "styling_key_anchor") for item in strategies if _field_value(item, "styling_key_anchor")}) < 2:
        return "styling_key_anchor 不能 4 条都完全相同"

    s1 = strategies[0]
    s4 = strategies[3]
    if (
        _field_value(s1, "opening_mode") == _field_value(s4, "opening_mode")
        and _field_value(s1, "visual_entry_mode") == _field_value(s4, "visual_entry_mode")
        and _field_value(s1, "opening_first_shot") == _field_value(s4, "opening_first_shot")
    ):
        return "S4 与 S1 的首镜逻辑拉开不足"

    if strategies_too_similar(strategies):
        return "多套策略在开场、proof 或差异字段上相似度过高"

    if not is_sea_market(target_country):
        return None

    home_count = count_home_share_strategies(strategies)
    if home_count < SEA_HOME_SCENE_MIN_COUNT:
        return f"家中自然分享场景数量不足，当前 {home_count} 套，至少需要 {SEA_HOME_SCENE_MIN_COUNT} 套"

    scene_subspaces = {_field_value(item, "scene_subspace") for item in strategies if _field_value(item, "scene_subspace")}
    if len(scene_subspaces) < 2:
        return "东南亚市场下家中 scene_subspace 至少需要 2 种"

    s4_scene_candidates = [
        str(s4.get("scene_subspace", "") or ""),
        str(s4.get("opening_first_shot", "") or ""),
        str(s4.get("opening_strategy", "") or ""),
    ]
    if not any(is_home_share_scene(item) for item in s4_scene_candidates if item):
        return "S4 在东南亚市场下必须明确落在家中自然分享语境"

    s4_text = " ".join(
        [
            str(s4.get("opening_strategy", "") or ""),
            str(s4.get("opening_first_shot", "") or ""),
            str(s4.get("risk_note", "") or ""),
        ]
    )
    if _contains_unwanted_ad_expression(s4_text):
        return "S4 在东南亚市场下滑向广告片/棚拍表达"

    return None


def _collect_storyboard_shots(script_json: Dict[str, object]) -> List[Dict[str, object]]:
    storyboard = script_json.get("storyboard")
    if not isinstance(storyboard, list):
        return []
    return [shot for shot in storyboard if isinstance(shot, dict)]


def _merge_text_parts(*values: object) -> str:
    parts: List[str] = []
    for value in values:
        if isinstance(value, list):
            parts.extend(str(item or "") for item in value)
        else:
            parts.append(str(value or ""))
    return normalize_text(" ".join(parts))


def _classify_entry_family(script_json: Dict[str, object]) -> str:
    opening_design = script_json.get("opening_design") if isinstance(script_json.get("opening_design"), dict) else {}
    storyboard = _collect_storyboard_shots(script_json)
    first_shot = storyboard[0] if storyboard else {}
    text = _merge_text_parts(
        opening_design.get("visual", ""),
        first_shot.get("shot_content", ""),
        first_shot.get("person_action", ""),
    )

    if any(token in text for token in ("拨发", "撩发", "整理头发", "手从下方进入", "手部进入", "手轻托", "抬手")):
        return "hand_reveal"
    if any(token in text for token in ("转头", "侧脸", "偏头", "轻转脸", "回头")):
        return "face_turn"
    if any(token in text for token in ("佩戴", "戴上", "上耳", "上身", "穿上", "背上")):
        return "wear_result"
    if any(token in text for token in ("镜前", "照镜", "镜子")):
        return "mirror_entry"
    if any(token in text for token in ("后退", "半步", "整体结果", "全身", "上半身", "肩颈范围")):
        return "overall_result"
    return "static_closeup"


def _classify_proof_family(shot: Dict[str, object]) -> str:
    text = _merge_text_parts(
        shot.get("shot_content", ""),
        shot.get("person_action", ""),
        shot.get("anchor_reference", ""),
    )
    if any(token in text for token in ("对比", "前后", "换上", "换成")):
        return "comparison_proof"
    if any(token in text for token in ("搭配", "造型", "衣服", "穿搭", "整体")):
        return "styling_proof"
    if any(token in text for token in ("上脸", "上身", "结果", "脸侧", "比例", "整体结果", "定格", "佩戴后", "上耳后", "上头后")):
        return "result_proof"
    if any(token in text for token in ("晃", "摆动", "走动", "转身", "动态", "回摆", "甩", "轻动")):
        return "motion_proof"
    if any(token in text for token in ("细节", "结构", "连接", "纹理", "特写", "近景", "材质", "花纹")):
        return "detail_proof"
    if any(token in text for token in ("不压", "不挑", "不显", "不会", "好驾驭", "不夸张", "适合")):
        return "concern_relief_proof"
    return "generic_proof"


def _classify_shot_focus_family(shot: Dict[str, object]) -> str:
    text = _merge_text_parts(
        shot.get("shot_content", ""),
        shot.get("person_action", ""),
        shot.get("anchor_reference", ""),
    )
    result_tokens = (
        "佩戴后",
        "上耳后",
        "上头后",
        "上身后",
        "结果镜",
        "结果感",
        "结果承接",
        "整体变化",
        "脸侧",
        "侧脸",
        "上脸",
        "完整垂落",
        "线条落在脸侧",
    )
    if any(token in text for token in ("全身", "上半身", "肩颈范围", "整体", "半步后退", "后退", "全景")):
        return "result_frame"
    if any(token in text for token in result_tokens) and any(
        token in text for token in ("半身", "上半身", "镜前", "侧脸", "脸侧", "结果", "整体", "垂落", "露脸")
    ):
        return "result_frame"
    if any(token in text for token in ("近景", "特写", "耳部", "局部", "更近", "压近", "镜头从脸侧超近切入")):
        return "localized_close"
    if any(token in text for token in ("镜前", "桌边", "床边", "衣柜前", "窗边", "站立")):
        return "scene_result"
    return "neutral_frame"


def _extract_proof_sequence(script_json: Dict[str, object]) -> List[str]:
    sequence: List[str] = []
    for shot in _collect_storyboard_shots(script_json):
        spoken_line_task = normalize_text(str(shot.get("spoken_line_task", "") or ""))
        task_type = normalize_text(str(shot.get("task_type", "") or ""))
        if spoken_line_task in {"proof", "proof+decision"} or task_type == "proof":
            sequence.append(_classify_proof_family(shot))
        if len(sequence) >= 2:
            break
    return sequence


def _classify_ending_family(script_json: Dict[str, object]) -> str:
    storyboard = _collect_storyboard_shots(script_json)
    if not storyboard:
        return "unknown"
    last_shot = storyboard[-1]
    text = _merge_text_parts(
        last_shot.get("shot_content", ""),
        last_shot.get("person_action", ""),
        last_shot.get("voiceover_text_target_language", ""),
        last_shot.get("voiceover_text_zh", ""),
    )
    if any(token in text for token in ("适合谁", "谁戴", "更适合", "适合")):
        return "who_fit"
    if any(token in text for token in ("场景", "日常", "通勤", "出门", "今天")):
        return "scene_fit"
    if any(token in text for token in ("下单", "点下面", "带走", "入手", "买")):
        return "purchase_push"
    if any(token in text for token in ("定格", "停住", "结果", "上脸", "上身", "整体")):
        return "result_hold"
    if any(token in text for token in ("不夸张", "不挑", "不会", "好驾驭")):
        return "concern_relief"
    return "light_share"


def validate_script_direction_separation(
    final_strategy: Dict[str, object],
    script_json: Dict[str, object],
    existing_scripts: Optional[Dict[str, Dict[str, object]]] = None,
) -> Optional[str]:
    if not existing_scripts:
        return None

    strategy_id = str(final_strategy.get("strategy_id", "") or "").strip().upper()
    if not strategy_id:
        return None

    entry_family = _classify_entry_family(script_json)
    proof_sequence = _extract_proof_sequence(script_json)
    ending_family = _classify_ending_family(script_json)
    storyboard = _collect_storyboard_shots(script_json)

    if strategy_id == "S4":
        early_focus = [_classify_shot_focus_family(shot) for shot in storyboard[:3]]
        early_focus = [item for item in early_focus if item]
        if len(early_focus) >= 3 and all(item == "localized_close" for item in early_focus[:3]):
            return "S4 前3镜仍然是连续局部拆解，缺少高惊艳首镜方向应有的结果压制后快速回落"
        if proof_sequence[:2] and all(item in {"detail_proof", "motion_proof", "generic_proof"} for item in proof_sequence[:2]):
            return "S4 前段 proof 过早滑向细节拆解，没有尽快把首镜的结果感接实"

    target_siblings = existing_scripts.items()
    if strategy_id == "S4" and "S1" in existing_scripts:
        target_siblings = [("S1", existing_scripts["S1"])]

    for sibling_id, sibling_script in target_siblings:
        if not isinstance(sibling_script, dict) or sibling_id == strategy_id:
            continue

        sibling_entry = _classify_entry_family(sibling_script)
        sibling_proof = _extract_proof_sequence(sibling_script)
        sibling_ending = _classify_ending_family(sibling_script)

        same_entry = entry_family == sibling_entry
        same_proof = bool(proof_sequence) and proof_sequence == sibling_proof
        same_ending = ending_family == sibling_ending

        if strategy_id == "S4" and sibling_id == "S1":
            if same_entry and same_proof:
                return "S4 与 S1 的开场触发和前段 proof 展开同构，惊艳型没有真正拉开"
            if same_proof and same_ending:
                return "S4 与 S1 的中后段推进过近，仍然像同一条脚本只换了更强开头"

        if same_entry and same_proof and same_ending:
            return f"{strategy_id} 与 {sibling_id} 在开场、proof 和结尾结构上过于同构"

    return None


def validate_sea_scene_distribution(
    target_country: str,
    final_strategies: List[Dict[str, object]],
) -> Optional[str]:
    return validate_strategy_distribution(target_country, final_strategies)


def describe_sea_market_scope() -> str:
    return ", ".join(SEA_COUNTRIES)
