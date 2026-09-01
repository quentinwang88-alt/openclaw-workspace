from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Iterable, List, Mapping


LONGFORM_SCHEMA_VERSION = "longform-original-master-v3-content-scene"
PLAN_SCHEMA_VERSION = "longform-original-plan-v5-boundary-contracts"
VOICEOVER_SCHEMA_VERSION = "creative-longform-segmented-v2"
MIN_DURATION_SECONDS = 25
MAX_DURATION_SECONDS = 45
MIN_CAPTURE_UNITS = 7
MAX_CAPTURE_UNITS = 13


class LongformContractError(ValueError):
    pass


def text(value: Any) -> str:
    return str(value or "").strip()


def stable_id(prefix: str, material: Any, length: int = 20) -> str:
    raw = json.dumps(material, ensure_ascii=False, sort_keys=True, default=str)
    return prefix + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:length].upper()


def _require_mapping(value: Any, field: str) -> Dict[str, Any]:
    if not isinstance(value, Mapping) or not value:
        raise LongformContractError(f"{field} 必须是非空对象")
    return dict(value)


def _require_text(value: Any, field: str) -> str:
    result = text(value)
    if not result:
        raise LongformContractError(f"{field} 不能为空")
    return result


def _dedupe(values: Iterable[Any]) -> List[str]:
    result: List[str] = []
    for value in values:
        item = text(value)
        if item and item not in result:
            result.append(item)
    return result


def segment_count_for_duration(duration_seconds: int) -> int:
    """Return the minimum number of H3 segments required by the 15s cap."""

    duration = int(duration_seconds)
    return max(2, (duration + 14) // 15)


def recommended_capture_unit_range(duration_seconds: int) -> tuple[int, int]:
    """Soft authoring targets; validation only protects three units per segment."""

    duration = int(duration_seconds)
    if duration <= 25:
        return 7, 8
    if duration <= 30:
        return 8, 9
    if duration <= 40:
        return 10, 12
    return 11, 13


def recommended_argument_range(duration_seconds: int) -> tuple[int, int]:
    """Soft content-capacity target, not a quota that must be padded."""

    return (2, 3) if int(duration_seconds) <= 30 else (3, 4)


def _normalize_argument_bundle(value: Mapping[str, Any], duration: int) -> Dict[str, Any]:
    semantic = dict(value.get("semantic_spine") or {})
    primary = dict((value.get("longform_argument_bundle") or {}).get("primary_argument") or {})
    if not primary:
        primary = dict(semantic.get("selling_argument") or {})
    if not text(primary.get("argument_id")):
        primary["argument_id"] = text(primary.get("source_argument_id")) or "PRIMARY"
    if not text(primary.get("text")):
        primary["text"] = text(
            primary.get("primary_selling_point")
            or semantic.get("core_buying_reason")
        )
    primary["argument_role"] = "PRIMARY_OUTCOME"

    raw_bundle = dict(value.get("longform_argument_bundle") or {})
    supporting: List[Dict[str, Any]] = []
    for raw in raw_bundle.get("supporting_arguments") or value.get("approved_supporting_arguments") or []:
        if not isinstance(raw, Mapping):
            continue
        item = dict(raw)
        item["argument_id"] = text(item.get("argument_id") or item.get("source_argument_id"))
        item["text"] = text(
            item.get("text") or item.get("primary_selling_point")
            or item.get("supporting_reason")
        )
        item["argument_role"] = text(item.get("argument_role")) or "PRODUCT_REASON"
        if item["argument_id"] and item["text"] and item["argument_id"] != primary["argument_id"]:
            supporting.append(item)
    minimum, maximum = recommended_argument_range(duration)
    effective_count = min(maximum, (1 if primary.get("text") else 0) + len(supporting))
    return {
        "schema_version": "longform-argument-bundle-v1",
        "primary_argument": primary,
        "supporting_arguments": supporting[: max(0, maximum - 1)],
        "visual_facts": list(raw_bundle.get("visual_facts") or value.get("verified_facts") or []),
        "content_capacity": {
            "recommended_effective_argument_range": [minimum, maximum],
            "available_effective_argument_count": effective_count,
            "shortfall_is_blocking": False,
            "policy": "USE_DISTINCT_VALUE_ONLY;DO_NOT_PAD_OR_LIST_ALL",
        },
    }


def _normalize_scene_blocks(value: Mapping[str, Any]) -> List[Dict[str, Any]]:
    world = dict(value.get("production_world") or {})
    raw_blocks = value.get("scene_blocks") or []
    blocks: List[Dict[str, Any]] = []
    for index, raw in enumerate(raw_blocks[:2], 1):
        if not isinstance(raw, Mapping):
            continue
        item = dict(raw)
        item["scene_id"] = text(item.get("scene_id")) or f"SCENE_{index}"
        item["location"] = text(item.get("location") or item.get("description"))
        item["narrative_role"] = text(item.get("narrative_role")) or "PRIMARY_USAGE_WORLD"
        item["segment_ids"] = _dedupe(item.get("segment_ids") or [])
        if item["location"]:
            blocks.append(item)
    if not blocks:
        scene = dict(world.get("scene_contract") or {})
        blocks = [{
            "scene_id": "SCENE_1",
            "location": text(scene.get("location") or world.get("scene")),
            "moment": text(scene.get("moment")),
            "lighting": text(scene.get("lighting") or world.get("lighting")),
            "background": text(scene.get("background")),
            "narrative_role": "PRIMARY_USAGE_WORLD",
            "segment_ids": [],
            "source": "LEGACY_PRODUCTION_WORLD",
        }]
    return blocks


def validate_master_contract(contract: Mapping[str, Any]) -> Dict[str, Any]:
    """Validate only the few boundaries required for a coherent two-part video.

    Richness is deliberately a soft design target. We block missing authority,
    broken continuity and invalid duration, not subjective creativity.
    """

    value = dict(contract or {})
    duration = int(value.get("target_duration_seconds") or 0)
    if not MIN_DURATION_SECONDS <= duration <= MAX_DURATION_SECONDS:
        raise LongformContractError("target_duration_seconds 必须在 25-45 秒")

    value["schema_version"] = LONGFORM_SCHEMA_VERSION
    value["product_code"] = _require_text(value.get("product_code"), "product_code")
    value["target_country"] = _require_text(value.get("target_country"), "target_country")
    value["target_language"] = _require_text(value.get("target_language"), "target_language")
    value["production_world"] = _require_mapping(value.get("production_world"), "production_world")
    value["product_identity_lock"] = _require_mapping(
        value.get("product_identity_lock"), "product_identity_lock"
    )
    value["semantic_spine"] = _require_mapping(value.get("semantic_spine"), "semantic_spine")
    mode = text(value.get("scene_mode") or "single").lower()
    value["scene_mode"] = mode if mode in {"single", "auto", "multi"} else "single"
    value["longform_argument_bundle"] = _normalize_argument_bundle(value, duration)
    value["scene_blocks"] = _normalize_scene_blocks(value)

    units = value.get("capture_units") or []
    segment_count = segment_count_for_duration(duration)
    minimum_for_segments = max(MIN_CAPTURE_UNITS, segment_count * 3)
    if (
        not isinstance(units, list)
        or not minimum_for_segments <= len(units) <= MAX_CAPTURE_UNITS
    ):
        suggested_min, suggested_max = recommended_capture_unit_range(duration)
        raise LongformContractError(
            "capture_units 必须足够让每段至少获得3个有信息增量的拍摄单元；"
            f"当前{duration}秒至少需要{minimum_for_segments}个，"
            f"建议{suggested_min}-{suggested_max}个，最多{MAX_CAPTURE_UNITS}个"
        )
    normalized_units: List[Dict[str, Any]] = []
    seen_ids = set()
    for index, raw in enumerate(units, 1):
        unit = _require_mapping(raw, f"capture_units[{index - 1}]")
        unit_id = text(unit.get("unit_id")) or f"CU{index:02d}"
        if unit_id in seen_ids:
            raise LongformContractError(f"capture_unit_id 重复: {unit_id}")
        seen_ids.add(unit_id)
        unit["unit_id"] = unit_id
        unit["beat"] = _require_text(unit.get("beat"), f"{unit_id}.beat").upper()
        unit["visual_content"] = _require_text(
            unit.get("visual_content"), f"{unit_id}.visual_content"
        )
        unit["information_gain"] = _require_text(
            unit.get("information_gain"), f"{unit_id}.information_gain"
        )
        unit["product_anchors_visible"] = _dedupe(unit.get("product_anchors_visible") or [])
        normalized_units.append(unit)
    value["capture_units"] = normalized_units

    # One hook is enough for the whole work. Part B is continuation, never a
    # second standalone short video with another introduction.
    if sum(1 for item in normalized_units if item["beat"] in {"HOOK", "ATTENTION"}) != 1:
        raise LongformContractError("完整长视频必须且只能有一个 HOOK/ATTENTION 单元")
    if normalized_units[0]["beat"] not in {"HOOK", "ATTENTION"}:
        raise LongformContractError("唯一 HOOK 必须位于第一个拍摄单元")

    value["contract_id"] = text(value.get("contract_id")) or stable_id(
        "LFC_", {key: val for key, val in value.items() if key != "contract_id"}
    )
    return value
