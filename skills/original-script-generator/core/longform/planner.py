from __future__ import annotations

import copy
import json
import math
import re
from typing import Any, Dict, List, Mapping, Tuple

from .contracts import PLAN_SCHEMA_VERSION, stable_id, text, validate_master_contract
from .visual_guidance import segment_visibility


def plan_segment_durations(total_seconds: int) -> List[int]:
    """Balance 20-45 seconds over the fewest possible <=15s H3 segments."""

    total = int(total_seconds)
    if not 20 <= total <= 45:
        raise ValueError("长视频总时长必须在20-45秒")
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


def _compact_unit(unit: Mapping[str, Any], index: int) -> Dict[str, Any]:
    """Project one audited capture unit into H3's small execution vocabulary."""

    return {
        "unit_id": text(unit.get("unit_id")),
        "beat": text(unit.get("beat")),
        "priority_role": text(unit.get("execution_priority")) or (
            "OPENING" if index == 0 else "CORE" if index == 1 else "PAYOFF"
        ),
        "visual_role": text(unit.get("visual_role")),
        "duration_seconds": unit.get("estimated_duration_seconds") or "按本段自然分配",
        "shot_and_camera": text(unit.get("camera")),
        "visible_action": _postdub_visual_text(
            unit.get("character_action") or unit.get("observable_action")
        ),
        "visual_result": _postdub_visual_text(unit.get("visual_content")),
        "product_anchors": _limited(unit.get("product_anchors_visible"), 3),
        "edit_before": "START_FRAME" if index == 0 else "DIRECT_CUT",
    }


def _postdub_visual_text(value: Any) -> str:
    """Remove speaking cues when H3 receives no audio and TTS is added later."""

    result = text(value)
    replacements = (
        ("正对自己的手机镜头自然交流", "自然看向自己的手机镜头"),
        ("面对自己的手机自然分享", "自然看向自己的手机展示"),
        ("面对手机自然说话", "自然看向手机"),
        ("面对手机说话", "自然看向手机"),
        ("面对手机分享", "自然看向手机展示"),
        ("自然继续当前分享", "自然继续当前展示"),
        ("保持自然说话状态", "保持自然注视手机的状态"),
        ("自然说话状态", "自然注视手机的状态"),
        ("正在开口分享", "正准备展示"),
        ("开口分享", "准备展示"),
        ("对镜介绍", "对镜展示"),
    )
    for source, target in replacements:
        result = result.replace(source, target)
    return result


def _detail_focus(master: Mapping[str, Any], limit: int = 2) -> List[str]:
    """Reuse existing product authority; never create a second detail library."""

    identity = dict(master.get("product_identity_lock") or {})
    candidates: List[Any] = list(identity.get("must_preserve") or [])
    bundle = dict(master.get("longform_argument_bundle") or {})
    candidates.extend(bundle.get("visual_facts") or [])
    candidates.extend(master.get("verified_facts") or [])
    results: List[str] = []
    for raw in candidates:
        if isinstance(raw, Mapping):
            item = text(
                raw.get("fact_text") or raw.get("text") or raw.get("value")
                or raw.get("description")
            )
        else:
            item = text(raw)
        if item and item not in results:
            results.append(item)
        if len(results) >= limit:
            break
    return results


def _segment_visual_role(master: Mapping[str, Any], segment_id: str) -> str:
    progression = dict(master.get("scene_progression_contract") or {})
    roles = dict(progression.get("segment_visual_roles") or {})
    return text(roles.get(segment_id)) or "CONTINUED_PRODUCT_STORY"


def _looks_like_detail(unit: Mapping[str, Any]) -> bool:
    material = " ".join(text(unit.get(key)) for key in (
        "visual_role", "camera", "visual_content", "product_surface",
    )).upper()
    return any(marker in material for marker in (
        "PRODUCT_DOMINANT", "DETAIL", "CLOSE_UP", "特写", "近距离", "细节", "局部",
    ))


def _execution_units(
    units: List[Dict[str, Any]], *, segment_role: str,
    detail_focus: List[str], incoming_boundary_mode: str,
    used_signatures: set[str] | None = None,
) -> List[Dict[str, Any]]:
    """Keep three executable viewing jobs; retain the full authored list for audit."""

    used_signatures = used_signatures if used_signatures is not None else set()
    projected = [copy.deepcopy(item) for item in units]
    detail_led = "DETAIL" in segment_role
    if detail_led and projected:
        detail_candidates = [
            index for index, item in enumerate(projected) if _looks_like_detail(item)
        ]
        detail_candidates.sort(
            key=lambda index: (
                _execution_unit_signature(projected[index]) in used_signatures,
                index,
            )
        )
        detail_index = detail_candidates[0] if detail_candidates else None
        if detail_index not in {None, 0} and incoming_boundary_mode == "DISCONTINUOUS_CUT":
            projected.insert(0, projected.pop(detail_index))
        first = projected[0]
        if detail_focus and not _looks_like_detail(first):
            focus_text = "、".join(detail_focus)
            first["visual_role"] = "PRODUCT_DOMINANT_DETAIL"
            first["camera"] = "商品主导的自然中近景，人物和少量场景仍可识别"
            first["product_anchors_visible"] = _limited(
                [*(first.get("product_anchors_visible") or []), *detail_focus], 3
            )
            if incoming_boundary_mode == "DISCONTINUOUS_CUT":
                first["visual_content"] = (
                    f"商品主导的自然中近景，清楚呈现{focus_text}；"
                    "人物保持已经完成的穿戴状态，背景只保留少量当前场景识别信息。"
                )
                first["character_action"] = (
                    "人物不重新穿戴，只做一次自然的小幅姿态调整，让商品细节持续可见"
                )
            else:
                first["visual_content"] = (
                    f"先让商品成为画面主体，清楚呈现{focus_text}；"
                    f"随后自然进入原画面任务：{text(first.get('visual_content'))}"
                )
    if len(projected) <= 3:
        selected = projected
    else:
        chosen = {0, len(projected) - 1}
        if detail_led:
            detail_candidates = [
                index for index, item in enumerate(projected) if _looks_like_detail(item)
            ]
            detail_candidates.sort(
                key=lambda index: (
                    _execution_unit_signature(projected[index]) in used_signatures,
                    index,
                )
            )
            detail_index = detail_candidates[0] if detail_candidates else 1
            chosen.add(detail_index)
        else:
            middle_candidates = list(range(1, len(projected) - 1))
            middle_candidates.sort(
                key=lambda index: (
                    _execution_unit_signature(projected[index]) in used_signatures,
                    abs(index - len(projected) / 2),
                )
            )
            chosen.add(middle_candidates[0] if middle_candidates else 1)
        for index in range(len(projected)):
            if len(chosen) >= 3:
                break
            chosen.add(index)
        selected = [item for index, item in enumerate(projected) if index in chosen][:3]
    for index, item in enumerate(selected):
        item["execution_priority"] = ("OPENING", "CORE", "PAYOFF")[index]
    return selected


def _execution_unit_signature(unit: Mapping[str, Any]) -> str:
    """Small semantic signature used only as a soft cross-segment tie-breaker."""

    material = " ".join(text(unit.get(key)) for key in (
        "visual_content", "character_action", "observable_action",
        "camera", "product_surface", "product_anchors_visible",
    )).lower()

    def category(groups: list[tuple[str, tuple[str, ...]]], fallback: str) -> str:
        for label, markers in groups:
            if any(marker in material for marker in markers):
                return label
        return fallback

    part = category([
        ("COLLAR", ("领口", "立领", "衣领", "collar")),
        ("CLOSURE", ("前襟", "门襟", "扣", "拉链", "closure", "zip")),
        ("SLEEVE", ("袖", "cuff", "sleeve")),
        ("HEM", ("下摆", "衣长", "hem")),
        ("BACK", ("背面", "后背", "back")),
        ("WHOLE", ("全身", "整体", "完整穿搭", "full body")),
    ], "GENERAL")
    action = category([
        ("TOUCH", ("轻扶", "触碰", "摸", "指尖", "touch")),
        ("TURN", ("转身", "侧转", "回身", "turn")),
        ("WALK", ("走", "迈步", "离开", "walk")),
        ("SIT_STAND", ("坐下", "起身", "站起", "sit")),
        ("HOLD", ("拿", "拎", "手持", "hold")),
        ("STATIC", ("停住", "站稳", "静置", "保持", "static")),
    ], "NATURAL")
    framing = category([
        ("CLOSE", ("特写", "近景", "近距离", "close-up", "close up")),
        ("FULL", ("全身", "中远景", "较宽", "full body", "wide")),
        ("MEDIUM", ("中景", "半身", "mid shot", "medium")),
    ], "UNSPECIFIED")
    return f"{part}::{action}::{framing}"


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
    scene = dict(scene_block) if scene_block else dict(world.get("scene_contract") or {})
    saliency = segment_visibility(master, scene)
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
        "outfit_detail": text(outfit_projection.get("hair_neckline_outer")),
        "outfit_finish": text(outfit_projection.get("palette_visibility_finish")),
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
        "visible_closure_contract": dict(identity.get("visible_closure_contract") or {}),
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
    outgoing_boundary_mode: str, entry_frame_role: str, segment_visual_role: str,
) -> str:
    if is_first:
        continuity = "片段A从统一首帧开始；这是全片唯一开场。"
    elif incoming_boundary_mode == "DISCONTINUOUS_CUT":
        if entry_frame_role == "SETUP_ENTRY":
            continuity = (
                f"片段{segment}在普通硬切后留在同一生活地点，但切到新的手机机位和商品观察关系；"
                "人物、商品、穿搭和已完成穿戴状态不变，不模仿上一片段姿势。"
                "不得重新开场、重新穿戴、重新系结或重复介绍商品。"
            )
        else:
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
        if not is_final and outgoing_boundary_mode == "CONTINUOUS" else ""
    )
    compact_units = [_compact_unit(unit, index) for index, unit in enumerate(units)]
    world = _compact_world(master, scene_block)
    identity = _compact_identity(master)
    bridge_projection = {
        "start_state": _compact_bridge(start_bridge),
        "planned_end_state": _compact_bridge(end_bridge),
    }
    return f"""生成一段竖屏 {duration} 秒的原生达人手机分享视频，作为同一条完整长视频的片段{segment}。
延续参考画面中的同一件商品，不重新设计、增加或删除商品可见结构。
{continuity}
本段唯一视觉职责：{segment_visual_role}。
声音路由为后期画外旁白：H3不接收口播音频。画面中的人物始终不说话，嘴唇自然闭合或放松，不做连续开合、发音或对口型动作；只用眼神、轻微点头、手势和商品动作表达。
优先按顺序形成以下 {len(compact_units)} 个观看任务。OPENING必须从输入首帧自然开始，CORE与PAYOFF按成片自然节奏直接切换；不要求机械逐项表演，但不能把三种观看关系压成同机位的小动作：
{json.dumps(compact_units, ensure_ascii=False, separators=(',', ':'))}

本片段生产世界，只执行这些冻结结果，不重新设计人物、商品或穿搭：
{json.dumps(world, ensure_ascii=False, separators=(',', ':'))}

商品身份锁：
{json.dumps(identity, ensure_ascii=False, separators=(',', ':'))}

片段衔接状态：
{json.dumps(bridge_projection, ensure_ascii=False, separators=(',', ':'))}

拍摄要求：本片段内人物、商品和穿搭保持连续；同一拍摄单元内地点、时刻和手机关系自然稳定，单元之间允许普通直接切镜。商品结构和数量服从参考图及身份锁。{bridge_finish_guidance}普通手机质感，自然直切。不要字幕；不要让人物在画面中说话或做口型；不要影视广告布光、慢动作或无意义空镜。"""


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
    segment_roles = {
        segment_id: _segment_visual_role(master, segment_id)
        for segment_id in segment_ids
    }
    detail_focus = _detail_focus(master)
    boundaries: List[Dict[str, Any]] = []
    for index, group in enumerate(unit_groups[:-1], 1):
        from_segment = segment_ids[index - 1]
        to_segment = segment_ids[index]
        from_scene = segment_scene_map[from_segment]
        to_scene = segment_scene_map[to_segment]
        scene_changed = from_scene != to_scene
        next_role = segment_roles[to_segment]
        # A new detail/proof chapter needs a new visual setup even when the
        # location stays unchanged. Tail continuity is reserved for a real
        # continuation of the same physical action.
        setup_change = not scene_changed and "DETAIL" in next_role
        boundary_mode = (
            "DISCONTINUOUS_CUT" if scene_changed or setup_change else "CONTINUOUS"
        )
        entry_frame_role = (
            "SCENE_ENTRY" if scene_changed else
            "SETUP_ENTRY" if setup_change else "ACTUAL_TAIL"
        )
        state = (
            _bridge_state(master, group[-1], scene_by_id.get(from_scene))
            if boundary_mode == "CONTINUOUS" else {}
        )
        boundaries.append({
            "boundary_id": f"K{index}",
            "from_segment": from_segment,
            "to_segment": to_segment,
            "boundary_mode": boundary_mode,
            "entry_frame_role": entry_frame_role,
            "from_scene_id": from_scene,
            "to_scene_id": to_scene,
            "planned_state": state,
            "tail_extraction_required": boundary_mode == "CONTINUOUS",
        })

    segments: List[Dict[str, Any]] = []
    used_execution_signatures: set[str] = set()
    repeated_execution_signatures: List[Dict[str, str]] = []
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
        entry_frame_role = (
            boundaries[index - 1]["entry_frame_role"] if index > 0 else "MASTER_OPENING"
        )
        execution_units = _execution_units(
            group,
            segment_role=segment_roles[segment_id],
            detail_focus=detail_focus,
            incoming_boundary_mode=incoming_mode,
            used_signatures=used_execution_signatures,
        )
        prior_segment_signatures = set(used_execution_signatures)
        for unit in execution_units:
            signature = _execution_unit_signature(unit)
            if signature in prior_segment_signatures:
                repeated_execution_signatures.append({
                    "segment_id": segment_id,
                    "unit_id": text(unit.get("unit_id")),
                    "signature": signature,
                })
            used_execution_signatures.add(signature)
        segment = {
            "segment_id": segment_id,
            "scene_id": segment_scene_map[segment_id],
            "scene_block": scene_by_id[segment_scene_map[segment_id]],
            "duration_seconds": duration,
            "segment_visual_role": segment_roles[segment_id],
            "detail_focus": detail_focus if "DETAIL" in segment_roles[segment_id] else [],
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
                "entry_frame_role": entry_frame_role,
            },
            "capture_units": group,
            "execution_units": execution_units,
        }
        if outgoing_mode == "CONTINUOUS":
            segment["end_keyframe"] = f"K{index + 1}_PLANNED"
        start_bridge = boundaries[index - 1]["planned_state"] if index > 0 and incoming_mode == "CONTINUOUS" else {}
        end_bridge = boundaries[index]["planned_state"] if not is_final and outgoing_mode == "CONTINUOUS" else {}
        segment["video_prompt"] = _segment_prompt(
            master, segment_id, duration, execution_units,
            is_first=index == 0, is_final=is_final,
            start_bridge=start_bridge, end_bridge=end_bridge,
            scene_block=scene_by_id[segment_scene_map[segment_id]],
            incoming_boundary_mode=incoming_mode,
            outgoing_boundary_mode=outgoing_mode,
            entry_frame_role=entry_frame_role,
            segment_visual_role=segment_roles[segment_id],
        )
        segments.append(segment)

    progression = dict(master.get("scene_progression_contract") or {})
    progression["resolved_scene_count"] = len(scene_blocks)
    progression["resolution_status"] = (
        "PREFERENCE_REALIZED"
        if int(progression.get("preferred_scene_count") or 1) == len(scene_blocks)
        else "SINGLE_SCENE_FALLBACK"
    )
    product_detail_segments = [
        item["segment_id"] for item in segments
        if any(_looks_like_detail(unit) for unit in item.get("execution_units") or [])
    ]
    warnings: List[str] = []
    if int(progression.get("preferred_scene_count") or 1) == 2 and len(scene_blocks) < 2:
        warnings.append("PREFERRED_SECOND_SCENE_NOT_REALIZED")
    if not product_detail_segments:
        warnings.append("PRODUCT_DOMINANT_DETAIL_NOT_REALIZED")
    if len(set(segment_roles.values())) < len(segment_roles):
        warnings.append("SEGMENT_VISUAL_ROLES_NOT_DISTINCT")
    if repeated_execution_signatures:
        warnings.append("CROSS_SEGMENT_ACTION_SIGNATURE_REUSED")
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
        "scene_progression_contract": progression,
        "scene_blocks": scene_blocks,
        "visual_progression_report": {
            "schema_version": "longform-visual-progression-report-v1",
            "scene_count": len(scene_blocks),
            "segment_visual_roles": segment_roles,
            "product_detail_segment_ids": product_detail_segments,
            "repeated_execution_signatures": repeated_execution_signatures,
            "warnings": warnings,
            "status": "LOW_VISUAL_PROGRESSION" if warnings else "READY",
            "hard_blocking": False,
            "automatic_regeneration": False,
        },
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
                    "segment_visual_role": segment["segment_visual_role"],
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
