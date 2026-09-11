from __future__ import annotations

import json
import re
from typing import Any, Dict, Mapping

from core.first_frame_contract import render_first_frame_prompt
from core.visual_execution_contract import build_opening_scene_projection

from .contracts import stable_id, text
from .planner import _postdub_visual_text
from .visual_guidance import segment_visibility


_UNBOUND_PERSONA_REPLACEMENTS = (
    ("把模特妆容换成淡妆", "自然淡妆"),
    ("其余发型身材不变", ""),
    ("注意还原参考图的肤色以及皮肤自然纹理", "自然肤色与真实皮肤纹理"),
    ("还原参考图的肤色", "自然肤色"),
    ("保持参考图", "保持自然写实"),
)


def _clean_unbound_persona_text(value: Any) -> str:
    result = text(value)
    for source, target in _UNBOUND_PERSONA_REPLACEMENTS:
        result = result.replace(source, target)
    result = re.sub(r"[，,]{2,}", "，", result)
    result = re.sub(r"\s+", " ", result)
    return result.strip(" ，。；,;.")


def _project_persona_for_unbound_frame(persona: Mapping[str, Any]) -> Dict[str, Any]:
    result = dict(persona or {})
    projection = dict(result.get("script_projection") or {})
    for key in ("identity", "appearance", "hair_makeup"):
        projection[key] = _clean_unbound_persona_text(projection.get(key))
    result["script_projection"] = projection
    return result


def _frame_safe_saliency(master: Mapping[str, Any], *,
                         scene: Mapping[str, Any], camera: str) -> Dict[str, Any]:
    saliency = segment_visibility(master, scene)
    # A frame belongs to its segment, not to the source short video's opening.
    # Keep the shared saliency intent without inheriting an earlier location or
    # full-body opening composition over this segment's product-detail shot.
    saliency["opening_focus"] = {
        "guidance": f"按本段冻结景别（{camera}）呈现商品；商品细节取景不扩展为全身。",
    }
    return saliency


def _postdub_frame_text(value: Any) -> str:
    result = _postdub_visual_text(value)
    for source, target in (
        ("刚准备开口", "自然看向手机"),
        ("准备开口", "自然看向手机"),
        ("自然开口", "自然展示"),
    ):
        result = result.replace(source, target)
    return result


def _base_frame_contract(master: Mapping[str, Any], visual: str, action: str,
                         anchors: list[str],
                         scene_block: Mapping[str, Any] | None = None,
                         camera: str = "") -> Dict[str, Any]:
    world = dict(master.get("production_world") or {})
    scene_contract = dict(scene_block) if scene_block else dict(
        world.get("scene_contract") or {"location": world.get("scene")}
    )
    presentation = text(world.get("presentation_mode")) or "PERSON_ON_CAMERA"
    frame_camera = text(camera or world.get("camera")) or "普通手机竖屏平视构图"
    return {
        "product_identity_lock": dict(master.get("product_identity_lock") or {}),
        "product_truth": dict(master.get("product_truth") or {}),
        "persona_contract": _project_persona_for_unbound_frame(
            dict(world.get("persona_contract") or {})
        ),
        "persona_reference_assets": [
            dict(asset)
            for asset in dict(master.get("frozen_reference_assets") or {}).get("assets") or []
            if isinstance(asset, Mapping) and asset.get("role") == "PERSONA_REFERENCE"
        ],
        "outfit_contract": dict(world.get("outfit_contract") or {}),
        "outfit_prompt_projection": dict(world.get("outfit_prompt_projection") or {}),
        "scene_contract": scene_contract,
        "visual_saliency": _frame_safe_saliency(
            master, scene=scene_contract, camera=frame_camera,
        ),
        "opening_scene_projection": build_opening_scene_projection(
            scene_contract, presentation_mode=presentation,
        ),
        "opening_contract": {
            "visual_content": _postdub_frame_text(visual),
            "character_action": _postdub_frame_text(action),
            "natural_emotion": _postdub_frame_text(world.get("natural_emotion")) or "自然专注，不做广告式表演",
            "camera": frame_camera,
            "carrier_mode": text(world.get("carrier_mode")) or "PERSON_ON_CAMERA",
            "product_anchors_visible": anchors,
        },
        "presentation_mode": presentation,
        "capture_mode": text(world.get("capture_mode")) or "CREATOR_SELF_SHOT",
        "body_proportion_authority": dict(world.get("body_proportion_authority") or {}),
    }


def build_keyframe_contracts(master: Mapping[str, Any], plan: Mapping[str, Any]) -> Dict[str, Any]:
    segments = list(plan.get("segments") or [])
    boundaries = list(dict(plan.get("bridge_contract") or {}).get("boundaries") or [])
    first_unit = dict((segments[0].get("execution_units") or segments[0].get("capture_units") or [])[0])
    k0_contract = _base_frame_contract(
        master,
        text(first_unit.get("visual_content")),
        text(first_unit.get("character_action") or first_unit.get("observable_action")),
        list(first_unit.get("product_anchors_visible") or []),
        dict(segments[0].get("scene_block") or {}),
        camera=text(first_unit.get("camera")),
    )
    k0_prompt = render_first_frame_prompt(k0_contract, frame_role="LONGFORM_OPENING")
    result = {
        "schema_version": "longform-keyframe-package-v8-shared-visibility",
        "frozen_reference_assets": dict(
            master.get("frozen_reference_assets") or {}
        ),
        "segment_frames": {},
        "K0": {
            "role": "MASTER_FIRST_FRAME",
            "prompt": k0_prompt,
            "source": "SYSTEM_GENERATED",
            "reference_policy": {
                "primary": "PRODUCT_AND_APPROVED_PERSONA_REFERENCES",
            },
        },
    }
    result["segment_frames"][str(segments[0]["segment_id"])] = {
        "start": {"key": "K0", "source": "MASTER_OPENING"},
        "end": {"key": "", "source": "NONE"},
    }
    for index, boundary in enumerate(boundaries, 1):
        segment = segments[index - 1]
        next_segment = segments[index]
        from_id = str(segment["segment_id"])
        to_id = str(next_segment["segment_id"])
        boundary_mode = text(boundary.get("boundary_mode") or "CONTINUOUS")
        if boundary_mode == "DISCONTINUOUS_CUT":
            entry_role = text(boundary.get("entry_frame_role") or "SCENE_ENTRY")
            first_unit = dict(
                (next_segment.get("execution_units") or next_segment.get("capture_units") or [])[0]
            )
            entry_contract = _base_frame_contract(
                master,
                text(first_unit.get("visual_content")),
                text(first_unit.get("character_action") or first_unit.get("observable_action")),
                list(first_unit.get("product_anchors_visible") or []),
                dict(next_segment.get("scene_block") or {}),
                camera=text(first_unit.get("camera")),
            )
            entry_key = f"S{to_id}_ENTRY"
            entry_guidance = (
                "这是同一生活地点内的新手机机位和商品观察关系，不延续上一段姿势。\n"
                if entry_role == "SETUP_ENTRY" else
                "这是新的真实生活场景进入画面，不延续上一段姿势或机位。\n"
            )
            entry_guidance += (
                "保持同一人物、商品和穿搭单品；这是明确切镜，"
                "穿着状态以本段首个单元的画面为准，不照搬K0的敞合状态或姿势。\n"
            )
            persona = dict(entry_contract.get("persona_contract") or {})
            hair = text(dict(persona.get("script_projection") or {}).get("hair_makeup"))
            if hair:
                entry_guidance += f"人物保持K0中的同一发长、分缝和卷直状态：{hair}。\n"
            result[entry_key] = {
                "role": entry_role,
                "prompt": entry_guidance + render_first_frame_prompt(
                    entry_contract, frame_role=entry_role
                ),
                "source": "SYSTEM_GENERATED",
                "generation_strategy": (
                    "NEW_SETUP_WITH_FROZEN_PERSON_PRODUCT_OUTFIT"
                    if entry_role == "SETUP_ENTRY" else
                    "NEW_SCENE_WITH_FROZEN_PERSON_PRODUCT_OUTFIT"
                ),
                "reference_policy": {
                    "primary": "PRODUCT_AND_APPROVED_PERSONA_REFERENCES",
                    "continuity_locks": ["SAME_PERSONA", "SAME_PRODUCT", "SAME_OUTFIT"],
                    "scene_authority": (
                        "SAME_SCENE_NEW_CAMERA_RELATION"
                        if entry_role == "SETUP_ENTRY" else "TARGET_SCENE_BLOCK"
                    ),
                },
                "failure_policy": "CONTINUE_WITH_GENERATED_RESULT",
                "human_confirmation_required": False,
            }
            result["segment_frames"][from_id]["end"] = {"key": "", "source": "NONE"}
            result["segment_frames"][to_id] = {
                "start": {"key": entry_key, "source": "SCENE_ENTRY_GENERATED"},
                "end": {"key": "", "source": "NONE"},
            }
            continue

        bridge_unit = dict(
            (segment.get("execution_units") or segment.get("capture_units") or [])[-1]
        )
        bridge_contract = _base_frame_contract(
            master,
            text(bridge_unit.get("visual_content")),
            text(
                bridge_unit.get("observable_end_state")
                or bridge_unit.get("character_action")
            ),
            list(bridge_unit.get("product_anchors_visible") or []),
            dict(segment.get("scene_block") or {}),
            camera=text(bridge_unit.get("camera")),
        )
        base_prompt = render_first_frame_prompt(bridge_contract, frame_role="BRIDGE")
        start_frame_info = (result["segment_frames"].get(from_id) or {}).get("start", {})
        previous_keyframe = (
            f"K{index - 1}_PLANNED"
            if start_frame_info.get("source") == "PREVIOUS_ACTUAL_TAIL"
            else str(start_frame_info.get("key") or "K0")
        )
        planned_key = f"K{index}_PLANNED"
        actual_key = f"K{index}_ACTUAL"
        result[planned_key] = {
            "role": "SEGMENT_BRIDGE_REFERENCE",
            "prompt": (
                "这不是独立重做的新画面。"
                f"以上一张已生成的{previous_keyframe}为人物、商品、穿搭和场景的主要视觉血缘，"
                "在同一生活时刻中延续到下面的桥接状态；"
                "商品参考图只用于校正同一商品，不重新设计商品结构。\n"
                + base_prompt
            ),
            "source": "SYSTEM_GENERATED",
            "generation_strategy": f"CONTINUATION_EDIT_FROM_{previous_keyframe}",
            "reference_policy": {
                "primary": f"GENERATED_{previous_keyframe}",
                "secondary": "ORIGINAL_PRODUCT_REFERENCES",
                "secondary_purpose": "CORRECT_SAME_PRODUCT_ONLY",
            },
            "failure_policy": "CONTINUE_WITH_GENERATED_RESULT",
            "human_confirmation_required": False,
        }
        result[actual_key] = {
            "role": f"SEGMENT_{to_id}_FIRST_FRAME",
            "source": f"EXTRACT_FROM_SEGMENT_{from_id}_LAST_0_8S",
            "required_before_next_submit": True,
            "selection_policy": "CLARITY_EXPOSURE_LOW_MOTION_WITH_SOFT_PRODUCT_READABILITY",
            "failure_policy": "CONTINUE_WITH_BEST_AVAILABLE_FRAME",
            "human_confirmation_required": False,
        }
        result["segment_frames"][from_id]["end"] = {
            "key": planned_key, "source": "PLANNED_SAME_SCENE",
        }
        result["segment_frames"].setdefault(to_id, {
            "start": {"key": actual_key, "source": "PREVIOUS_ACTUAL_TAIL"},
            "end": {"key": "", "source": "NONE"},
        })
    result["keyframe_package_id"] = stable_id("LFK_", result)
    return result
