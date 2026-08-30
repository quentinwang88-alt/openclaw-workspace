"""Lossless semantic hand-off for original-script planning.

The original pipeline has several useful compact summaries, but a compact
summary must not become a new source of truth.  This module keeps the reviewed
operator wording, assigns explicit semantic roles, and gives visual planning
and central voiceover the same per-item thesis.

The compiler is deliberately deterministic.  It consumes the already-reviewed
central selling-point catalogue and therefore adds no per-script model call,
retry, or new production dependency.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from typing import Any, Dict, Iterable, List, Mapping, Sequence


SEMANTIC_SPINE_SCHEMA_VERSION = "script-semantic-spine-v1"
MARKET_CONTEXT_SCHEMA_VERSION = "product-market-context-v1"
CONTEXT_BRIDGE_SCHEMA_VERSION = "whole-video-context-bridge-v1"


def _text(value: Any) -> str:
    return str(value or "").strip()


def _stable_id(prefix: str, value: Any, length: int = 20) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    return prefix + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:length].upper()


def semantic_spine_enabled() -> bool:
    return os.getenv("ORIGINAL_SCRIPT_SEMANTIC_SPINE_V1_ENABLED", "1").strip().lower() not in {
        "0", "false", "off", "no",
    }


_TAG_KEYWORDS = {
    "TRAVEL": (
        "旅行", "旅游", "旅遊", "行李", "出游", "度假", "出國", "出国",
        "travel", "trip", "vacation", "holiday",
    ),
    "COLD_CLIMATE": (
        "寒冷", "低温", "低溫", "降温", "降溫", "冷天", "冬季", "冬天",
        "保暖", "抗冷", "冷气", "冷氣", "空调", "空調", "cold", "winter",
        "air-con", "aircon", "air conditioner",
    ),
    "COMMUTE": (
        "办公室", "辦公室", "通勤", "上班", "工作", "会议", "會議",
        "office", "commute", "work", "meeting",
    ),
    "CASUAL_OUTING": (
        "休闲", "休閒", "日常", "逛街", "见朋友", "見朋友", "出门", "出門",
        "casual", "daily", "outing",
    ),
    "PHOTO_FRIENDLY": (
        "拍照", "出片", "上镜", "上鏡", "打卡", "镜头", "鏡頭", "咖啡",
        "photo", "camera", "photogenic", "cafe",
    ),
    "BODY_RESULT": (
        "显瘦", "顯瘦", "遮肉", "身形", "身材", "比例", "腰线", "腰線",
        "腿长", "腿長", "slim", "body", "waist", "proportion",
    ),
    "HOME": (
        "家里", "家中", "居家", "客厅", "臥室", "卧室", "玄关", "公寓",
        "home", "apartment", "bedroom", "living room",
    ),
    "TRANSIT": (
        "机场", "機場", "候机", "候機", "车站", "車站", "等车", "等車",
        "搭车", "搭車", "airport", "station", "transit",
    ),
    "HOTEL_STAY": (
        "酒店", "旅馆", "旅館", "民宿", "hotel", "resort",
    ),
}


def semantic_tags(value: Any) -> List[str]:
    if isinstance(value, Mapping):
        material = " ".join(_text(item) for item in value.values())
    elif isinstance(value, (list, tuple, set)):
        material = " ".join(_text(item) for item in value)
    else:
        material = _text(value)
    lowered = material.lower()
    return [
        tag for tag, keywords in _TAG_KEYWORDS.items()
        if any(keyword.lower() in lowered for keyword in keywords)
    ]


def _split_clauses(value: Any) -> List[str]:
    text = _text(value)
    if not text:
        return []
    clauses = [
        item.strip(" \t\r\n，,；;。.!！？?")
        for item in re.split(r"[，,；;。.!！？?\n]+", text)
    ]
    return [item for item in clauses if item]


def _argument_source_text(argument: Mapping[str, Any]) -> str:
    return _text(
        argument.get("source_operator_expression")
        or argument.get("audience_situation")
        or argument.get("target_need")
        or argument.get("operator_expression")
        or argument.get("primary_selling_point")
    )


def _argument_id(argument: Mapping[str, Any]) -> str:
    return _text(
        argument.get("source_argument_id")
        or argument.get("argument_id")
        or argument.get("value_id")
    )


def _is_scenario_argument(argument: Mapping[str, Any]) -> bool:
    return bool(
        _text(argument.get("claim_type")).lower() == "scenario"
        or _text(argument.get("claim_theme")).lower() == "scenario"
        or _text(argument.get("audience_need_authority")).upper()
        == "APPROVED_SELLING_SCENARIO"
        or semantic_tags(_argument_source_text(argument))
        and bool({"TRAVEL", "COMMUTE", "TRANSIT", "HOTEL_STAY"} & set(
            semantic_tags(_argument_source_text(argument))
        ))
    )


def _primary_context_clause(argument: Mapping[str, Any]) -> str:
    source = _argument_source_text(argument)
    clauses = _split_clauses(source)
    if not clauses:
        return ""
    context_tags = {"TRAVEL", "COMMUTE", "TRANSIT", "HOTEL_STAY", "HOME", "CASUAL_OUTING"}
    for clause in clauses:
        if context_tags & set(semantic_tags(clause)):
            return clause
    return clauses[0] if _is_scenario_argument(argument) else ""


def _supporting_context_clauses(argument: Mapping[str, Any], primary: str) -> List[str]:
    result: List[str] = []
    for clause in _split_clauses(_argument_source_text(argument)):
        if clause == primary:
            continue
        tags = set(semantic_tags(clause))
        if tags & {"TRAVEL", "COMMUTE", "CASUAL_OUTING", "TRANSIT", "HOTEL_STAY", "HOME"}:
            result.append(clause)
    return list(dict.fromkeys(result))


def _source_claim_ids(argument: Mapping[str, Any]) -> List[str]:
    values: List[str] = []
    for key in ("source_claim_ids", "normalization_claim_ids"):
        raw = argument.get(key)
        if isinstance(raw, (list, tuple)):
            values.extend(_text(item) for item in raw if _text(item))
    return list(dict.fromkeys(values))


def build_product_market_context(
    *,
    product_code: str,
    target_country: str,
    target_language: str,
    product_type: str,
    selling_point_catalog: Iterable[Mapping[str, Any]],
) -> Dict[str, Any]:
    """Compile a cached-ready market lens without inventing product effects.

    This is positioning context, not a product claim.  It may combine an
    explicit travel occasion with a separately reviewed cold-environment
    occasion, but it never upgrades either source into a temperature or
    performance promise.
    """

    catalog = [dict(item) for item in selling_point_catalog if isinstance(item, Mapping)]
    scenario_rows = [item for item in catalog if _is_scenario_argument(item)]
    scenario_rows.sort(key=lambda item: (
        0 if _text(item.get("operator_priority")).lower() == "core" else 1,
        catalog.index(item),
    ))
    primary_row = scenario_rows[0] if scenario_rows else {}
    primary_clause = _primary_context_clause(primary_row)
    all_source_text = " ".join(_argument_source_text(item) for item in catalog)
    all_tags = set(semantic_tags(all_source_text))
    primary_tags = set(semantic_tags(primary_clause))

    if "TRAVEL" in primary_tags and "COLD_CLIMATE" in all_tags:
        primary_text = "前往气温较低地区旅行"
        primary_kind = "TRAVEL_TO_COOLER_DESTINATION"
    else:
        primary_text = primary_clause
        primary_kind = next(iter(primary_tags), "UNAVAILABLE") if primary_clause else "UNAVAILABLE"

    supporting: List[Dict[str, Any]] = []
    seen_support: set[str] = set()
    for row in scenario_rows:
        row_primary = _primary_context_clause(row)
        for clause in [row_primary, *_supporting_context_clauses(row, row_primary)]:
            if not clause or clause == primary_clause or clause in seen_support:
                continue
            seen_support.add(clause)
            supporting.append({
                "context_id": _stable_id("MCTX_", {"text": clause}),
                "text": clause,
                "semantic_tags": semantic_tags(clause),
                "source_argument_id": _argument_id(row),
            })

    source_ids = list(dict.fromkeys(
        _argument_id(item) for item in catalog if _argument_id(item)
    ))
    claim_ids = list(dict.fromkeys(
        claim_id for item in catalog for claim_id in _source_claim_ids(item)
    ))
    material = {
        "product_code": product_code,
        "target_country": target_country,
        "target_language": target_language,
        "product_type": product_type,
        "primary_text": primary_text,
        "source_argument_ids": source_ids,
        "source_claim_ids": claim_ids,
    }
    return {
        "schema_version": MARKET_CONTEXT_SCHEMA_VERSION,
        "contract_id": _stable_id("PMC_", material),
        "status": "AVAILABLE" if primary_text else "UNAVAILABLE",
        "product_code": _text(product_code),
        "target_country": _text(target_country),
        "target_language": _text(target_language),
        "product_type": _text(product_type),
        "primary_usage_world": {
            "context_id": _stable_id("MCTX_", {"kind": primary_kind, "text": primary_text}),
            "kind": primary_kind,
            "text": primary_text,
            "semantic_tags": sorted(set([*primary_tags, *({"COLD_CLIMATE"} if primary_kind == "TRAVEL_TO_COOLER_DESTINATION" else set())])),
            "authority": "OPERATOR_INPUT_COMPILED",
            "source_argument_id": _argument_id(primary_row),
            "source_claim_ids": _source_claim_ids(primary_row),
        },
        "secondary_usage_worlds": supporting,
        "source_argument_ids": source_ids,
        "source_claim_ids": claim_ids,
        "source_snapshot_hash": _stable_id("PMS_", material),
        "generation_provenance": {
            "mode": "DETERMINISTIC_FROM_REVIEWED_CATALOG",
            "model_call_added": False,
        },
        "hard_required": False,
    }


def build_script_semantic_spine(
    *,
    product_code: str,
    content_bundle: Mapping[str, Any],
    market_context: Mapping[str, Any],
) -> Dict[str, Any]:
    argument = (
        dict(content_bundle.get("selling_argument") or {})
        if isinstance(content_bundle.get("selling_argument"), Mapping)
        else {}
    )
    raw_text = _argument_source_text(argument)
    primary_clause = _primary_context_clause(argument)
    explicit_context = bool(primary_clause and _is_scenario_argument(argument))
    market_primary = (
        dict(market_context.get("primary_usage_world") or {})
        if isinstance(market_context.get("primary_usage_world"), Mapping)
        else {}
    )
    primary_context = primary_clause if explicit_context else _text(market_primary.get("text"))
    primary_context_tags = semantic_tags(primary_context)
    # Preserve the market-level climate/occasion refinement when the selected
    # argument points at the same usage world (for example travel + reviewed
    # cold-environment usage elsewhere in the catalogue).
    if (
        explicit_context
        and "TRAVEL" in set(primary_context_tags)
        and _text(market_primary.get("kind")) == "TRAVEL_TO_COOLER_DESTINATION"
    ):
        primary_context = _text(market_primary.get("text")) or primary_context
        primary_context_tags = list(market_primary.get("semantic_tags") or semantic_tags(primary_context))

    canonical = _text(
        argument.get("operator_expression")
        or argument.get("core_value")
        or (content_bundle.get("value_proposition") or {}).get("text")
        or content_bundle.get("content_mainline")
    )
    if "百搭" in raw_text or "versatile" in raw_text.lower():
        core_reason = "一件商品适配多种穿搭或使用场景"
    else:
        core_reason = canonical

    segments: List[Dict[str, Any]] = []
    if primary_clause:
        segments.append({
            "segment_id": _stable_id("SEG_", {"role": "PRIMARY_USAGE_SCENARIO", "text": primary_clause}),
            "role": "PRIMARY_USAGE_SCENARIO",
            "text": primary_clause,
            "semantic_tags": semantic_tags(primary_clause),
        })
    if core_reason:
        segments.append({
            "segment_id": _stable_id("SEG_", {"role": "CORE_BUYING_REASON", "text": core_reason}),
            "role": "CORE_BUYING_REASON",
            "text": core_reason,
            "semantic_tags": semantic_tags(core_reason),
        })
    supporting_clauses = _supporting_context_clauses(argument, primary_clause)
    for clause in supporting_clauses:
        segments.append({
            "segment_id": _stable_id("SEG_", {"role": "SUPPORTING_USAGE_EXAMPLE", "text": clause}),
            "role": "SUPPORTING_USAGE_EXAMPLE",
            "text": clause,
            "semantic_tags": semantic_tags(clause),
        })

    primary_segment_ids = [
        item["segment_id"] for item in segments
        if item["role"] in {"PRIMARY_USAGE_SCENARIO", "CORE_BUYING_REASON"}
    ]
    optional_segment_ids = [
        item["segment_id"] for item in segments
        if item["role"] == "SUPPORTING_USAGE_EXAMPLE"
    ]
    source_argument_id = _argument_id(argument)
    material = {
        "product_code": product_code,
        "source_argument_id": source_argument_id,
        "raw_text": raw_text,
        "primary_context": primary_context,
        "core_reason": core_reason,
        "market_contract_id": _text(market_context.get("contract_id")),
    }
    return {
        "schema_version": SEMANTIC_SPINE_SCHEMA_VERSION,
        "spine_id": _stable_id("SSP_", material),
        "status": "AVAILABLE" if (raw_text or core_reason) else "UNAVAILABLE",
        "source_argument": {
            "source_argument_id": source_argument_id,
            "source_claim_ids": _source_claim_ids(argument),
            "raw_text": raw_text,
            "source": _text(argument.get("source") or "FEISHU_OPERATOR_CONFIRMED_ARGUMENT"),
        },
        "source_semantic_segments": segments,
        "product_market_context": dict(market_context),
        "script_thesis": {
            "primary_narrative_context": primary_context,
            "primary_narrative_context_tags": list(dict.fromkeys(primary_context_tags)),
            "selected_source_span": primary_clause or raw_text,
            "core_buying_reason": core_reason,
            "target_audience": _text(content_bundle.get("audience_tension_text")),
            "supporting_situations": supporting_clauses,
            "must_preserve_semantic_ids": primary_segment_ids,
            "optional_semantic_ids": optional_segment_ids,
        },
        "authority": {
            "product_facts": "HARD_SOURCE",
            "operator_argument": "HARD_SEMANTIC",
            "market_context": "STRATEGIC_DEFAULT",
            "creative_scene": "SOFT_EXECUTION",
            "rhetorical_surface": "CREATIVE",
        },
        "policy": {
            "lower_layers_may_omit_optional_semantics": True,
            "lower_layers_may_replace_primary_semantics": False,
            "sentence_to_shot_alignment_required": False,
            "whole_video_context_consistency_required": True,
        },
    }


def scene_context_tags(value: Mapping[str, Any]) -> List[str]:
    material = " ".join(_text(value.get(key)) for key in (
        "scene_family_key", "scene_motif", "persona_role", "opening_action", "action_grammar",
    ))
    tags = set(semantic_tags(material))
    family = _text(value.get("scene_family_key")).upper()
    if family == "OFFICE_WORKBREAK":
        tags.add("COMMUTE")
    if family in {"TRAVEL_PREP", "TRAVEL_TRANSIT", "TRAVEL_STAY"}:
        tags.add("TRAVEL")
    if family == "TRAVEL_TRANSIT":
        tags.add("TRANSIT")
    if family == "TRAVEL_STAY":
        tags.add("HOTEL_STAY")
    if family == "HOME_ROUTINE":
        tags.add("HOME")
    return sorted(tags)


def classify_scene_relation(
    semantic_spine: Mapping[str, Any], scene: Mapping[str, Any]
) -> Dict[str, Any]:
    thesis = (
        semantic_spine.get("script_thesis")
        if isinstance(semantic_spine.get("script_thesis"), Mapping)
        else {}
    )
    primary_tags = set(thesis.get("primary_narrative_context_tags") or [])
    selected_tags = set(scene_context_tags(scene))
    if not primary_tags:
        relation = "NEUTRAL"
        reason = "PRIMARY_CONTEXT_UNAVAILABLE"
    elif primary_tags & selected_tags:
        relation = "SUPPORTS"
        reason = "SHARED_CONTEXT_TAG"
    elif "TRAVEL" in primary_tags and "COMMUTE" in selected_tags:
        relation = "CONFLICTS"
        reason = "TRAVEL_VS_EXPLICIT_COMMUTE"
    elif "COMMUTE" in primary_tags and (
        {"TRAVEL", "TRANSIT", "HOTEL_STAY"} & selected_tags
    ):
        relation = "CONFLICTS"
        reason = "COMMUTE_VS_EXPLICIT_TRAVEL"
    else:
        relation = "NEUTRAL"
        reason = "NO_EXPLICIT_CONTRADICTION"
    return {
        "relation": relation,
        "reason": reason,
        "primary_context_tags": sorted(primary_tags),
        "scene_context_tags": sorted(selected_tags),
        "hard_required": False,
    }


def scene_relation_score(relation_contract: Mapping[str, Any]) -> int:
    relation = _text(relation_contract.get("relation")).upper()
    if relation == "SUPPORTS":
        return -60
    if relation == "CONFLICTS":
        return 1000
    return 0


def build_context_bridge(
    semantic_spine: Mapping[str, Any], creative_contract: Mapping[str, Any]
) -> Dict[str, Any]:
    thesis = (
        semantic_spine.get("script_thesis")
        if isinstance(semantic_spine.get("script_thesis"), Mapping)
        else {}
    )
    relation = classify_scene_relation(semantic_spine, creative_contract)
    scene_family = _text(creative_contract.get("scene_family_key"))
    if relation["relation"] == "SUPPORTS" and scene_family in {
        "TRAVEL_PREP", "TRAVEL_TRANSIT", "TRAVEL_STAY",
    }:
        mode = "SCENE_ANCHORED"
        speaker_context = _text(
            creative_contract.get("persona_role")
            or creative_contract.get("scene_motif")
        )
    elif _text(thesis.get("primary_narrative_context")):
        mode = "SITUATION_ANCHORED"
        speaker_context = ""
    else:
        mode = "PRODUCT_ANCHORED"
        speaker_context = ""
    return {
        "schema_version": CONTEXT_BRIDGE_SCHEMA_VERSION,
        "bridge_id": _stable_id("CTXB_", {
            "spine_id": semantic_spine.get("spine_id"),
            "scene_family": scene_family,
            "scene_motif": creative_contract.get("scene_motif"),
            "mode": mode,
        }),
        "primary_narrative_context": _text(
            thesis.get("primary_narrative_context")
        ),
        "selected_visual_context": {
            "scene_family_key": scene_family,
            "scene_motif": _text(creative_contract.get("scene_motif")),
        },
        "scene_relation": relation,
        "voiceover_context_mode": mode,
        "allowed_spoken_context": list(dict.fromkeys([
            _text(thesis.get("primary_narrative_context")),
            _text(thesis.get("selected_source_span")),
        ])),
        "speaker_context": speaker_context,
        "sentence_to_shot_alignment_required": False,
        "whole_video_context_consistency_required": True,
    }


def semantic_trace(
    semantic_spine: Mapping[str, Any],
    context_bridge: Mapping[str, Any],
    *,
    voiceover_context_mode: str = "",
    realized_context_id: str = "",
    realized_context_text: str = "",
) -> Dict[str, Any]:
    thesis = dict(semantic_spine.get("script_thesis") or {})
    relation = dict(context_bridge.get("scene_relation") or {})
    return {
        "schema_version": "semantic-trace-v1",
        "spine_id": _text(semantic_spine.get("spine_id")),
        "source_text": _text((semantic_spine.get("source_argument") or {}).get("raw_text")),
        "primary_narrative_context": _text(thesis.get("primary_narrative_context")),
        "core_buying_reason": _text(thesis.get("core_buying_reason")),
        "scene_relation": _text(relation.get("relation")) or "UNAVAILABLE",
        "scene_relation_reason": _text(relation.get("reason")),
        "voiceover_context_mode": (
            _text(voiceover_context_mode)
            or _text(context_bridge.get("voiceover_context_mode"))
        ),
        "realized_context_id": _text(realized_context_id),
        "realized_context_text": _text(realized_context_text),
        "drift_status": (
            "PRIMARY_SCENARIO_DRIFT"
            if _text(relation.get("relation")) == "CONFLICTS"
            else "NO_EXPLICIT_DRIFT"
        ),
        "is_blocking": _text(relation.get("relation")) == "CONFLICTS",
    }
