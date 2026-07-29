"""Compile a routed structure contract into an executable original-video shot plan."""

from __future__ import annotations

import copy
from typing import Any, Dict, List, Sequence


PLAN_SCHEMA_VERSION = "original-structure-execution-plan-v1"
SUPPORTED_BEATS = {"HOOK", "PROOF", "USE_PROCESS", "ENDING"}
SUPPORTED_CARRIERS = {"HAND_ONLY", "STATIC_PRODUCT", "MIXED", "WEARER_ACTIVE"}
SUPPORTED_CONTINUITY = {"SINGLE_SHOT", "CONTINUOUS_LOW_CUT", "MULTI_CUT"}
SUPPORTED_OPENINGS = {
    "PRODUCT_REVEAL",
    "PERSON_REVEAL",
    "RESULT_REVEAL",
    "PROCESS_REVEAL",
    "PROBLEM_REVEAL",
    "TEXT_REVEAL",
}


def _dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _enum(value: Any, allowed: Sequence[str], default: str = "UNAVAILABLE") -> str:
    normalized = _text(value).upper()
    return normalized if normalized in allowed else default


def _beats(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []
    result: List[str] = []
    for item in value:
        beat = _text(item).upper()
        if beat in SUPPORTED_BEATS:
            result.append(beat)
    return result


def _shot_count(hard: Dict[str, Any], beat_count: int, continuity: str) -> int:
    measured = hard.get("shot_count")
    if isinstance(measured, dict) and measured.get("authority") == "VIDEO_MEASURED":
        for key in ("median", "min", "max"):
            try:
                value = int(round(float(measured.get(key))))
            except (TypeError, ValueError):
                continue
            return max(4, min(6, max(value, beat_count)))
    if continuity == "CONTINUOUS_LOW_CUT":
        return max(4, min(6, beat_count))
    if continuity == "MULTI_CUT":
        return max(5, min(6, max(beat_count, 6)))
    return max(4, min(6, max(beat_count, 5)))


def _expand_beats(beats: List[str], shot_count: int) -> List[str]:
    if not beats:
        return []
    expanded = list(beats[:shot_count])
    while len(expanded) < shot_count:
        proof_indexes = [index for index, beat in enumerate(expanded) if beat == "PROOF"]
        if proof_indexes:
            insert_at = proof_indexes[0] + 1
            while insert_at < len(expanded) and expanded[insert_at] == "PROOF":
                insert_at += 1
            expanded.insert(insert_at, "PROOF")
            continue
        insert_at = max(1, len(expanded) - (1 if expanded[-1] == "ENDING" else 0))
        expanded.insert(insert_at, expanded[insert_at - 1])
    return expanded


def _time_ranges(shot_count: int) -> List[str]:
    boundaries = {
        4: (0.0, 3.0, 6.5, 10.5, 15.0),
        5: (0.0, 2.5, 5.0, 8.0, 11.5, 15.0),
        6: (0.0, 2.5, 4.5, 6.5, 9.5, 12.0, 15.0),
    }[shot_count]

    def fmt(value: float) -> str:
        return str(int(value)) if value.is_integer() else str(value)

    return [f"{fmt(boundaries[index])}-{fmt(boundaries[index + 1])}s" for index in range(shot_count)]


def _carrier_schedule(carrier: str, opening: str, shot_count: int) -> List[str]:
    if carrier in {"HAND_ONLY", "STATIC_PRODUCT", "WEARER_ACTIVE"}:
        return [carrier] * shot_count
    if carrier != "MIXED":
        return ["UNAVAILABLE"] * shot_count

    first = "WEARER_ACTIVE" if opening == "PERSON_REVEAL" else "STATIC_PRODUCT"
    second = "STATIC_PRODUCT" if first == "WEARER_ACTIVE" else "WEARER_ACTIVE"
    schedule = [first, second]
    while len(schedule) < shot_count:
        schedule.append("HAND_ONLY" if len(schedule) == 2 else "WEARER_ACTIVE")
    return schedule[:shot_count]


def _continuity_groups(continuity: str, shot_count: int) -> List[str]:
    if continuity == "CONTINUOUS_LOW_CUT":
        return ["A"] * shot_count
    if continuity == "MULTI_CUT":
        return [f"C{index}" for index in range(1, shot_count + 1)]
    return ["UNAVAILABLE"] * shot_count


def _visual_task(beat: str, opening: str, carrier: str, operation_policy: str) -> str:
    if beat == "HOOK":
        opening_tasks = {
            "PRODUCT_REVEAL": "首帧以商品主体或关键结构为视觉中心；人物可以存在，但不能抢走第一视觉焦点",
            "PERSON_REVEAL": "首帧先给人物使用或穿戴后的整体关系，商品必须同时清楚可见",
            "RESULT_REVEAL": "首帧直接给使用后的结果，不先讲过程",
            "PROCESS_REVEAL": "首帧从一个清楚、低风险的使用动作切入，商品主体保持可辨认",
            "PROBLEM_REVEAL": "首帧先呈现具体问题状态，随后立即让商品进入解决关系",
            "TEXT_REVEAL": "首帧文字钩子只能辅助，商品画面仍是主体",
        }
        task = opening_tasks.get(opening, "前3秒建立清楚的商品视觉钩子，不补造未知开场机制")
        if carrier == "STATIC_PRODUCT":
            return f"{task}；只使用商品、静物支架或人台承载，不加入真人"
        if carrier == "HAND_ONLY":
            return f"{task}；只允许手部进入画面，不出现完整人物"
        return task
    if beat == "USE_PROCESS":
        if operation_policy == "result_first_process_avoid":
            return "展示一次短促、低风险、与商品真实使用直接相关的动作；不拍完整穿戴教程，动作后立即回到结果"
        return "展示一个可确认的真实使用步骤；过程必须服务证明，不得变成长教程或复杂交互"
    if beat == "ENDING":
        return "用已成立的结果或使用场景完成收束；允许轻决策口播，不引入新的视觉卖点"
    if beat == "PROOF":
        if carrier == "STATIC_PRODUCT":
            return "用商品独立画面、结构细节或角度变化提供可见证明，不加入人物承载"
        if carrier == "HAND_ONLY":
            return "以手部承载商品完成细节或功能证明，避免人物整体出镜"
        if carrier == "WEARER_ACTIVE":
            return "通过穿戴者或使用者的结果、动作和场景关系证明主卖点"
        return "用可见画面证明当前主卖点，不扩写新的证明主题"
    return "按结构合同推进当前镜头"


def _shot_beat(value: Any) -> str:
    return _text(_dict(value).get("structure_beat")).upper()


def _partition_storyboard_for_plan(
    storyboard: List[Dict[str, Any]],
    shot_plan: List[Dict[str, Any]],
) -> List[List[int]]:
    """Partition surplus model shots into authoritative plan slots.

    The review model occasionally expands a four-shot plan into five shots by
    splitting one PROOF into two.  We preserve all prose by grouping adjacent
    shots, while a beat-aware dynamic programme keeps HOOK/ENDING aligned.
    """
    source_count = len(storyboard)
    target_count = len(shot_plan)
    if source_count < target_count or target_count <= 0:
        return []
    if source_count == target_count:
        return [[index] for index in range(source_count)]

    infinity = float("inf")
    costs = [[infinity] * (source_count + 1) for _ in range(target_count + 1)]
    previous = [[-1] * (source_count + 1) for _ in range(target_count + 1)]
    costs[0][0] = 0.0
    for target_index in range(1, target_count + 1):
        expected = _text(shot_plan[target_index - 1].get("structure_beat")).upper()
        min_consumed = target_index
        max_consumed = source_count - (target_count - target_index)
        for consumed in range(min_consumed, max_consumed + 1):
            for split in range(target_index - 1, consumed):
                if costs[target_index - 1][split] == infinity:
                    continue
                group = storyboard[split:consumed]
                mismatch = 0.0
                for item in group:
                    observed = _shot_beat(item)
                    mismatch += 0.5 if not observed else (0.0 if observed == expected else 6.0)
                candidate = costs[target_index - 1][split] + mismatch + 0.1 * (len(group) - 1)
                if candidate < costs[target_index][consumed]:
                    costs[target_index][consumed] = candidate
                    previous[target_index][consumed] = split

    if previous[target_count][source_count] < 0:
        return []
    groups: List[List[int]] = []
    consumed = source_count
    for target_index in range(target_count, 0, -1):
        split = previous[target_index][consumed]
        groups.append(list(range(split, consumed)))
        consumed = split
    groups.reverse()
    return groups


def _merge_text_values(values: List[Any]) -> str:
    merged: List[str] = []
    for value in values:
        text = _text(value)
        if text and text not in merged:
            merged.append(text)
    return "；".join(merged)


def _merge_shot_group(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    merged = copy.deepcopy(items[0])
    text_fields = (
        "shot_content",
        "scene_description",
        "visual_description",
        "shot_purpose",
        "subtitle_text_target_language",
        "subtitle_text_zh",
        "voiceover_text_target_language",
        "voiceover_text_zh",
        "person_action",
        "style_note",
        "anchor_reference",
    )
    for field in text_fields:
        values = [item.get(field) for item in items if item.get(field) is not None]
        if values:
            merged[field] = _merge_text_values(values)
    performances = [item.get("performance") for item in items if isinstance(item.get("performance"), dict)]
    if performances:
        performance: Dict[str, Any] = {}
        for key in {key for item in performances for key in item}:
            performance[key] = _merge_text_values([item.get(key) for item in performances])
        merged["performance"] = performance
    return merged


def _reconcile_surplus_shots(
    storyboard: List[Dict[str, Any]],
    raw_skeleton: List[Any],
    shot_plan: List[Dict[str, Any]],
) -> tuple[List[Dict[str, Any]], List[Any]]:
    if len(storyboard) <= len(shot_plan) or not all(isinstance(item, dict) for item in storyboard):
        return storyboard, raw_skeleton
    groups = _partition_storyboard_for_plan(storyboard, shot_plan)
    if not groups:
        return storyboard, raw_skeleton
    reconciled_storyboard = [
        _merge_shot_group([storyboard[index] for index in group])
        for group in groups
    ]
    reconciled_skeleton: List[Any] = []
    for group in groups:
        skeleton_items = [raw_skeleton[index] for index in group if index < len(raw_skeleton)]
        dict_items = [item for item in skeleton_items if isinstance(item, dict)]
        if dict_items:
            reconciled_skeleton.append(_merge_shot_group(dict_items))
        else:
            reconciled_skeleton.append(_merge_text_values(skeleton_items))
    return reconciled_storyboard, reconciled_skeleton


def _spoken_task(beat: str, index: int, shot_count: int, has_ending: bool) -> str:
    if beat == "HOOK":
        return "hook"
    if beat == "ENDING":
        return "decision"
    if index == shot_count and not has_ending:
        return "proof+decision"
    return "proof"


def compile_structure_execution_plan(
    structure_contract: Dict[str, Any],
    category_execution_contract: Dict[str, Any],
) -> Dict[str, Any]:
    """Return an explicit 4-6 shot plan when a usable structure contract exists."""
    contract = _dict(structure_contract)
    hard = _dict(contract.get("hard_constraints"))
    beats = _beats(hard.get("beat_sequence"))
    if not beats:
        return {}

    carrier = _enum(hard.get("content_carrier"), SUPPORTED_CARRIERS)
    continuity = _enum(hard.get("continuity_mode"), SUPPORTED_CONTINUITY)
    opening = _text(hard.get("visual_hook_type")).upper() or "UNAVAILABLE"
    category_contract = _dict(category_execution_contract)
    operation_policy = _text(category_contract.get("operation_policy"))
    blocking_conflicts: List[str] = []
    if "USE_PROCESS" in beats and operation_policy in {"process_forbidden", "static_result_only"}:
        blocking_conflicts.append(
            f"结构要求 USE_PROCESS，但商品执行合同 operation_policy={operation_policy} 禁止过程镜头"
        )

    count = _shot_count(hard, len(beats), continuity)
    expanded_beats = _expand_beats(beats, count)
    carriers = _carrier_schedule(carrier, opening, count)
    groups = _continuity_groups(continuity, count)
    ranges = _time_ranges(count)
    has_ending = "ENDING" in beats
    shot_plan: List[Dict[str, Any]] = []
    for index, beat in enumerate(expanded_beats, 1):
        shot_carrier = carriers[index - 1]
        shot_plan.append(
            {
                "shot_index": index,
                "time_range": ranges[index - 1],
                "structure_beat": beat,
                "carrier_mode": shot_carrier,
                "continuity_group": groups[index - 1],
                "opening_mechanism": opening if index == 1 else "",
                "visual_task": _visual_task(beat, opening, shot_carrier, operation_policy),
                "spoken_task_hint": _spoken_task(beat, index, count, has_ending),
            }
        )

    provenance = _dict(contract.get("provenance"))
    return {
        "plan_schema_version": PLAN_SCHEMA_VERSION,
        "source": "structure_contract_compiler",
        "contract_applied": True,
        "direction_assignment_id": _text(provenance.get("direction_assignment_id")),
        "macro_family_key": _text(_dict(contract.get("direction_identity")).get("macro_family_key")),
        "beat_sequence": beats,
        "content_carrier": carrier,
        "continuity_mode": continuity,
        "opening_mechanism": opening,
        "shot_count": count,
        "operation_policy": operation_policy,
        "blocking_conflicts": blocking_conflicts,
        "shot_plan": shot_plan,
    }


def apply_structure_execution_plan(script_json: Dict[str, Any], plan: Dict[str, Any]) -> Dict[str, Any]:
    """Materialize authoritative structure metadata without inventing storyboard prose."""
    if not isinstance(script_json, dict) or not isinstance(plan, dict) or not plan.get("contract_applied"):
        return script_json
    shot_plan = [item for item in (plan.get("shot_plan") or []) if isinstance(item, dict)]
    if not shot_plan:
        return script_json

    script_json["structure_execution_plan"] = copy.deepcopy(plan)
    raw_skeleton = script_json.get("shot_skeleton") if isinstance(script_json.get("shot_skeleton"), list) else []
    storyboard = script_json.get("storyboard") if isinstance(script_json.get("storyboard"), list) else []
    storyboard, raw_skeleton = _reconcile_surplus_shots(storyboard, raw_skeleton, shot_plan)
    script_json["storyboard"] = storyboard
    default_proof_path = _text(script_json.get("proof_path")) or "A_result_detail_only"
    skeleton: List[Dict[str, Any]] = []
    for index, plan_shot in enumerate(shot_plan):
        source_item = raw_skeleton[index] if index < len(raw_skeleton) else {}
        if isinstance(source_item, dict):
            skeleton_item = dict(source_item)
        else:
            legacy_text = _text(source_item)
            skeleton_item = {"shot_purpose": legacy_text} if legacy_text else {}
        structural = {
            "structure_beat": plan_shot.get("structure_beat", ""),
            "carrier_mode": plan_shot.get("carrier_mode", ""),
            "continuity_group": plan_shot.get("continuity_group", ""),
            "opening_mechanism": plan_shot.get("opening_mechanism", ""),
        }
        skeleton_item.update(structural)
        skeleton_item["shot_index"] = index + 1
        skeleton_item["time_range"] = str(
            plan_shot.get("time_range") or skeleton_item.get("time_range") or ""
        )
        skeleton_item["role"] = str(
            skeleton_item.get("role")
            or skeleton_item.get("task")
            or plan_shot.get("spoken_task_hint")
            or "proof"
        )
        skeleton_item["shot_purpose"] = str(
            plan_shot.get("visual_task") or skeleton_item.get("shot_purpose") or "按结构合同推进当前镜头"
        )
        skeleton_item["proof_path"] = str(
            skeleton_item.get("proof_path") or default_proof_path
        )
        skeleton.append(skeleton_item)
        if index < len(storyboard) and isinstance(storyboard[index], dict):
            storyboard[index].update(structural)
            storyboard[index]["shot_no"] = index + 1
            storyboard[index]["duration"] = str(plan_shot.get("time_range") or storyboard[index].get("duration") or "")
    script_json["shot_skeleton"] = skeleton
    return script_json


def apply_structure_plan_to_video_prompt(prompt_json: Dict[str, Any], plan: Dict[str, Any]) -> Dict[str, Any]:
    """Carry the authoritative per-shot structural metadata into the final prompt."""
    if not isinstance(prompt_json, dict) or not isinstance(plan, dict) or not plan.get("contract_applied"):
        return prompt_json
    shot_plan = [item for item in (plan.get("shot_plan") or []) if isinstance(item, dict)]
    shots = prompt_json.get("shot_execution") if isinstance(prompt_json.get("shot_execution"), list) else []
    for index, plan_shot in enumerate(shot_plan):
        if index >= len(shots) or not isinstance(shots[index], dict):
            continue
        shots[index].update(
            {
                "shot_no": index + 1,
                "duration": str(plan_shot.get("time_range") or shots[index].get("duration") or ""),
                "structure_beat": plan_shot.get("structure_beat", ""),
                "carrier_mode": plan_shot.get("carrier_mode", ""),
                "continuity_group": plan_shot.get("continuity_group", ""),
                "opening_mechanism": plan_shot.get("opening_mechanism", ""),
            }
        )
    prompt_json["structure_execution_plan"] = copy.deepcopy(plan)
    return prompt_json
