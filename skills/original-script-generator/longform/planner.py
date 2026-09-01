from __future__ import annotations

import copy
import json
import math
import re
from typing import Any, Dict, List, Mapping, Tuple

from .contracts import PLAN_SCHEMA_VERSION, stable_id, text, validate_master_contract


def plan_segment_durations(total_seconds: int) -> List[int]:
    """Balance 25-45 seconds over the fewest possible <=15s H3 segments."""

    total = int(total_seconds)
    if not 25 <= total <= 45:
        raise ValueError("长视频总时长必须在25-45秒")
    segment_count = int(math.ceil(total / 15))
    base, remainder = divmod(total, segment_count)
    durations = [base + 1] * remainder + [base] * (segment_count - remainder)
    if any(not 4 <= value <= 15 for value in durations):
        raise ValueError(f"无法把{total}秒切成H3支持的4-15秒整数片段")
    return durations


def split_duration(total_seconds: int) -> Tuple[int, int]:
    """Backward-compatible two-segment projection for existing callers."""

    durations = plan_segment_durations(total_seconds)
    if len(durations) != 2:
        raise ValueError(f"{total_seconds}秒需要{len(durations)}个片段，不能投影为双段")
    return durations[0], durations[1]


def _partition_units(
    units: List[Dict[str, Any]], durations: List[int]
) -> List[List[Dict[str, Any]]]:
    """Partition only at unit boundaries, keeping at least three per segment."""

    if len(units) < len(durations) * 3:
        raise ValueError("拍摄单元不足，无法保证每个视频片段至少三个信息单元")
    total_seconds = sum(durations)
    result: List[List[Dict[str, Any]]] = []
    start = 0
    elapsed = 0
    for index, duration in enumerate(durations[:-1]):
        elapsed += duration
        ideal = round(len(units) * elapsed / total_seconds)
        remaining_segments = len(durations) - index - 1
        lower = start + 3
        upper = len(units) - remaining_segments * 3
        end = min(max(lower, ideal), upper)
        result.append(units[start:end])
        start = end
    result.append(units[start:])
    return result


def _bridge_state(master: Mapping[str, Any], last_a: Mapping[str, Any],
                  scene_block: Mapping[str, Any] | None = None) -> Dict[str, Any]:
    world = dict(master.get("production_world") or {})
    explicit = last_a.get("end_state") if isinstance(last_a.get("end_state"), Mapping) else {}
    state = {
        "person_state": text(explicit.get("person_state") or world.get("person_state")),
        "product_wear_state": text(explicit.get("product_wear_state") or world.get("product_wear_state")),
        "pose_or_action_state": text(
            explicit.get("pose_or_action_state")
            or last_a.get("observable_end_state")
            or last_a.get("character_action")
            or last_a.get("observable_action")
        ),
        "camera_state": text(
            explicit.get("camera_state") or last_a.get("camera") or world.get("camera")
        ),
        "scene_state": text(
            explicit.get("scene_state")
            or (scene_block or {}).get("location")
            or world.get("scene")
        ),
        "outfit_state": text(explicit.get("outfit_state") or world.get("outfit")),
        "lighting_state": text(explicit.get("lighting_state") or world.get("lighting")),
    }
    missing = [key for key, value in state.items() if not value]
    if missing:
        raise ValueError("桥接状态缺失: " + ", ".join(missing))
    return state


def _limited(values: Any, limit: int = 3) -> List[str]:
    result: List[str] = []
    for value in values if isinstance(values, (list, tuple)) else []:
        item = text(value)
        if item and item not in result:
            result.append(item)
        if len(result) >= limit:
            break
    return result


def _compact_unit(unit: Mapping[str, Any]) -> Dict[str, Any]:
    """Project one audited capture unit into H3's small execution vocabulary."""

    return {
        "unit_id": text(unit.get("unit_id")),
        "beat": text(unit.get("beat")),
        "duration_seconds": unit.get("estimated_duration_seconds") or "按本段自然分配",
        "shot_and_camera": text(unit.get("camera")),
        "visible_action": text(
            unit.get("character_action") or unit.get("observable_action")
        ),
        "visual_result": text(unit.get("visual_content")),
        "product_anchors": _limited(unit.get("product_anchors_visible"), 3),
        "edit_before": "DIRECT_CUT",
    }


def _clean_execution_text(value: Any) -> str:
    result = text(value)
    for source, target in (
        ("把模特妆容换成淡妆", "自然淡妆"),
        ("其余发型身材不变", ""),
        ("注意还原参考图的肤色以及皮肤自然纹理", "自然肤色与真实皮肤纹理"),
        ("还原参考图的肤色", "自然肤色"),
    ):
        result = result.replace(source, target)
    result = re.sub(r"[，,]{2,}", "，", result)
    result = re.sub(r"\s+", " ", result)
    return result.strip(" ，。；,;.")


def _compact_world(master: Mapping[str, Any], scene_block: Mapping[str, Any] | None = None) -> Dict[str, Any]:
    world = dict(master.get("production_world") or {})
    character = dict(world.get("character") or {})
    persona = dict(world.get("persona_contract") or {})
    persona_projection = dict(persona.get("script_projection") or {})
    outfit_projection = dict(world.get("outfit_prompt_projection") or {})
    scene = dict(world.get("scene_contract") or {})
    if scene_block:
        scene.update({key: value for key, value in dict(scene_block).items() if value})
    saliency = dict(world.get("visual_saliency") or {})
    exposure = dict(saliency.get("exposure") or {})
    separation = dict(saliency.get("separation") or {})
    return {
        "same_person": _clean_execution_text(character.get("identity") or world.get("person_state")),
        "appearance": _clean_execution_text(
            persona_projection.get("appearance") or character.get("appearance")
        ),
        "hair_makeup": _clean_execution_text(
            persona_projection.get("hair_makeup") or character.get("hair_makeup")
        ),
        "frozen_outfit": text(outfit_projection.get("frozen_outfit") or world.get("outfit")),
        "same_scene": text(scene.get("location") or scene.get("description") or world.get("scene")),
        "scene_moment": text(scene.get("moment")),
        "lighting": text(scene.get("lighting") or world.get("lighting")),
        "background": text(scene.get("background")),
        "camera_relationship": text(world.get("camera")),
        "native_exposure": text(exposure.get("guidance")),
        # Background separation may improve saliency; outfit colour is frozen
        # and therefore deliberately absent from this execution projection.
        "background_separation": text(separation.get("background_guidance")),
    }


def _compact_identity(master: Mapping[str, Any]) -> Dict[str, Any]:
    identity = dict(master.get("product_identity_lock") or {})
    truth = dict(master.get("product_truth") or {})
    quantity = truth.get("display_quantity_contract")
    return {
        "must_preserve": _limited(identity.get("must_preserve"), 3),
        "must_not_change": _limited(identity.get("must_not_change"), 3),
        "display_quantity": quantity if isinstance(quantity, Mapping) else "按参考图实际数量",
    }


def _compact_bridge(bridge: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        key: text(bridge.get(key))
        for key in (
            "person_state", "product_wear_state", "pose_or_action_state",
            "camera_state", "scene_state", "outfit_state", "lighting_state",
        )
        if text(bridge.get(key))
    }


def _segment_prompt(
    master: Mapping[str, Any], segment: str, duration: int,
    units: List[Dict[str, Any]], *, is_first: bool, is_final: bool,
    start_bridge: Mapping[str, Any], end_bridge: Mapping[str, Any],
    scene_block: Mapping[str, Any], incoming_boundary_mode: str,
) -> str:
    if is_first:
        continuity = "片段A从统一首帧开始；这是全片唯一开场。"
    elif incoming_boundary_mode == "DISCONTINUOUS_CUT":
        continuity = (
            f"片段{segment}在普通硬切后进入新的生活场景；人物、商品、穿搭和已完成穿戴状态不变，"
            "但不模仿上一片段姿势或机位。不得重新开场、重新穿戴、重新系结或重复介绍商品。"
        )
    else:
        continuity = (
            f"片段{segment}从上一片段实际尾帧的同一人物、商品佩戴状态、姿势、机位、场景和光线继续；"
            "不得重新开场、重新穿戴、重新系结或重复介绍商品。"
        )
    bridge_finish_guidance = (
        f"片段{segment}最后约0.5秒尽量让商品主体自然清楚、不过度遮挡，方便下一段承接；"
        "做不到时仍按原动作自然结束，不为此增加摆拍。"
        if not is_final else ""
    )
    compact_units = [_compact_unit(unit) for unit in units]
    world = _compact_world(master, scene_block)
    identity = _compact_identity(master)
    bridge_projection = {
        "start_state": _compact_bridge(start_bridge),
        "planned_end_state": _compact_bridge(end_bridge),
    }
    return f"""生成一段竖屏 {duration} 秒的原生达人手机分享视频，作为同一条完整长视频的片段{segment}。
延续参考画面中的同一件商品，不重新设计、增加或删除商品可见结构。
{continuity}
按顺序执行以下 {len(compact_units)} 个可见拍摄单元。单元之间使用普通直接切镜；不得压成一镜到底，不得增加广告大片式运镜：
{json.dumps(compact_units, ensure_ascii=False, separators=(',', ':'))}

本片段生产世界，只执行这些冻结结果，不重新设计人物、商品或穿搭：
{json.dumps(world, ensure_ascii=False, separators=(',', ':'))}

商品身份锁：
{json.dumps(identity, ensure_ascii=False, separators=(',', ':'))}

片段衔接状态：
{json.dumps(bridge_projection, ensure_ascii=False, separators=(',', ':'))}

拍摄要求：本片段内人物、商品、穿搭、地点、时刻和手机关系保持连续；每个拍摄单元必须实际出现，商品结构和数量服从参考图及身份锁。{bridge_finish_guidance}普通手机质感，自然直切。不要字幕、对口型、影视广告布光、慢动作或无意义空镜。"""


def _group_units_for_segments(
    units: List[Dict[str, Any]], durations: List[int], segment_ids: List[str]
) -> List[List[Dict[str, Any]]]:
    hinted = {segment_id: [] for segment_id in segment_ids}
    valid = True
    seen_order: List[str] = []
    for unit in units:
        segment_id = text(unit.get("segment_id") or unit.get("segment")).upper()
        if segment_id not in hinted:
            valid = False
            break
        hinted[segment_id].append(unit)
        if not seen_order or seen_order[-1] != segment_id:
            seen_order.append(segment_id)
    if valid and seen_order == segment_ids and all(len(hinted[key]) >= 3 for key in segment_ids):
        return [hinted[key] for key in segment_ids]
    return _partition_units(units, durations)


def _resolve_scene_blocks(
    master: Mapping[str, Any], segment_ids: List[str],
) -> tuple[List[Dict[str, Any]], Dict[str, str]]:
    mode = text(master.get("scene_mode") or "single").lower()
    blocks = [dict(item) for item in master.get("scene_blocks") or [] if isinstance(item, Mapping)][:2]
    if not blocks:
        raise ValueError("scene_blocks 不能为空")
    if mode == "single" or len(blocks) == 1:
        block = blocks[0]
        block["segment_ids"] = list(segment_ids)
        return [block], {segment_id: text(block.get("scene_id")) for segment_id in segment_ids}

    mapping: Dict[str, str] = {}
    for block in blocks:
        for segment_id in block.get("segment_ids") or []:
            normalized = text(segment_id).upper()
            if normalized in segment_ids and normalized not in mapping:
                mapping[normalized] = text(block.get("scene_id"))
    if set(mapping) != set(segment_ids) or len(set(mapping.values())) < 2:
        split_at = 1 if len(segment_ids) == 2 else 2
        mapping = {
            segment_id: text(blocks[0 if index < split_at else 1].get("scene_id"))
            for index, segment_id in enumerate(segment_ids)
        }
    sequence = [mapping[segment_id] for segment_id in segment_ids]
    switch_count = sum(
        sequence[index] != sequence[index - 1] for index in range(1, len(sequence))
    )
    if switch_count > 1:
        # Returning to a previous scene makes frame lineage ambiguous and is
        # usually only a camera-position change mislabeled as a new scene.
        # Collapse safely instead of inventing a second switch or blocking a
        # batch; the model prompt prevents this shape for new contracts.
        primary_id = sequence[0]
        mapping = {segment_id: primary_id for segment_id in segment_ids}
        blocks = [next(item for item in blocks if text(item.get("scene_id")) == primary_id)]
        blocks[0]["resolution_status"] = "NONCONTIGUOUS_SCENE_SEQUENCE_COLLAPSED_TO_SINGLE"
    for block in blocks:
        block["segment_ids"] = [
            segment_id for segment_id in segment_ids
            if mapping[segment_id] == text(block.get("scene_id"))
        ]
    return blocks, mapping


def compile_longform_plan(master_contract: Mapping[str, Any]) -> Dict[str, Any]:
    master = validate_master_contract(master_contract)
    total = int(master["target_duration_seconds"])
    durations = plan_segment_durations(total)
    units = copy.deepcopy(master["capture_units"])
    segment_ids = [chr(ord("A") + index) for index in range(len(durations))]
    unit_groups = _group_units_for_segments(units, durations, segment_ids)
    scene_blocks, segment_scene_map = _resolve_scene_blocks(master, segment_ids)
    scene_by_id = {text(item.get("scene_id")): item for item in scene_blocks}
    boundaries: List[Dict[str, Any]] = []
    for index, group in enumerate(unit_groups[:-1], 1):
        from_segment = segment_ids[index - 1]
        to_segment = segment_ids[index]
        from_scene = segment_scene_map[from_segment]
        to_scene = segment_scene_map[to_segment]
        boundary_mode = "CONTINUOUS" if from_scene == to_scene else "DISCONTINUOUS_CUT"
        state = (
            _bridge_state(master, group[-1], scene_by_id.get(from_scene))
            if boundary_mode == "CONTINUOUS" else {}
        )
        boundaries.append({
            "boundary_id": f"K{index}",
            "from_segment": from_segment,
            "to_segment": to_segment,
            "boundary_mode": boundary_mode,
            "from_scene_id": from_scene,
            "to_scene_id": to_scene,
            "planned_state": state,
            "tail_extraction_required": boundary_mode == "CONTINUOUS",
        })

    segments: List[Dict[str, Any]] = []
    for index, (segment_id, duration, group) in enumerate(
        zip(segment_ids, durations, unit_groups)
    ):
        for unit in group:
            unit["segment"] = segment_id
            unit["segment_id"] = segment_id
            unit["scene_id"] = segment_scene_map[segment_id]
        is_final = index == len(durations) - 1
        outgoing_mode = boundaries[index]["boundary_mode"] if not is_final else "NONE"
        incoming_mode = boundaries[index - 1]["boundary_mode"] if index > 0 else "MASTER_OPENING"
        segment = {
            "segment_id": segment_id,
            "scene_id": segment_scene_map[segment_id],
            "scene_block": scene_by_id[segment_scene_map[segment_id]],
            "duration_seconds": duration,
            "generation_mode": "first_last" if outgoing_mode == "CONTINUOUS" else "first_frame",
            "start_keyframe": (
                "K0" if index == 0 else
                (f"K{index}_ACTUAL" if incoming_mode == "CONTINUOUS" else f"S{segment_id}_ENTRY")
            ),
            "frame_contract": {
                "start_source": (
                    "MASTER_OPENING" if index == 0 else
                    ("PREVIOUS_ACTUAL_TAIL" if incoming_mode == "CONTINUOUS" else "SCENE_ENTRY_GENERATED")
                ),
                "end_source": "PLANNED_SAME_SCENE" if outgoing_mode == "CONTINUOUS" else "NONE",
                "incoming_boundary_mode": incoming_mode,
                "outgoing_boundary_mode": outgoing_mode,
            },
            "capture_units": group,
        }
        if outgoing_mode == "CONTINUOUS":
            segment["end_keyframe"] = f"K{index + 1}_PLANNED"
        start_bridge = boundaries[index - 1]["planned_state"] if index > 0 and incoming_mode == "CONTINUOUS" else {}
        end_bridge = boundaries[index]["planned_state"] if not is_final and outgoing_mode == "CONTINUOUS" else {}
        segment["video_prompt"] = _segment_prompt(
            master, segment_id, duration, group,
            is_first=index == 0, is_final=is_final,
            start_bridge=start_bridge, end_bridge=end_bridge,
            scene_block=scene_by_id[segment_scene_map[segment_id]],
            incoming_boundary_mode=incoming_mode,
        )
        segments.append(segment)

    plan = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "master_contract_id": master["contract_id"],
        "product_code": master["product_code"],
        "target_duration_seconds": total,
        "production_world": master["production_world"],
        "product_identity_lock": master["product_identity_lock"],
        "semantic_spine": master["semantic_spine"],
        "longform_argument_bundle": master["longform_argument_bundle"],
        "scene_mode": master["scene_mode"],
        "scene_blocks": scene_blocks,
        "bridge_contract": {
            "schema_version": "longform-bridge-v4-boundary-aware",
            "authority": "BOUNDARY_MODE_OWNS_FRAME_LINEAGE",
            "boundaries": boundaries,
            "actual_bridge_frame_required": any(
                item["boundary_mode"] == "CONTINUOUS" for item in boundaries
            ),
            "selection_window_seconds": 0.8,
            "soft_end_state_preference": {
                "goal": "PRODUCT_NATURALLY_READABLE",
                "guidance": "片段A最后约0.5秒尽量让商品主体自然清楚、不过度遮挡",
                "failure_policy": "CONTINUE_WITH_BEST_AVAILABLE_FRAME",
                "hard_validation": False,
            },
        },
        "segments": segments,
        "voiceover_contract": {
            "contract_name": "creative_longform_single_v1",
            "target_duration_seconds": total,
            "one_continuous_voiceover": True,
            "tts_after_merge": True,
            "segment_unit_ids": {
                segment["segment_id"]: [item["unit_id"] for item in segment["capture_units"]]
                for segment in segments
            },
            "argument_bundle": master["longform_argument_bundle"],
            "segment_scene_roles": {
                segment["segment_id"]: {
                    "scene_id": segment["scene_id"],
                    "narrative_role": text(segment["scene_block"].get("narrative_role")),
                }
                for segment in segments
            },
        },
        "merge_contract": {
            "transition": "HARD_CUT",
            "strip_source_audio": True,
            "normalize_before_concat": True,
            "single_tts_after_concat": False,
            "semantic_section_tts_after_concat": True,
            "subtitles": False,
        },
    }
    plan["plan_id"] = stable_id("LFP_", plan)
    return plan
