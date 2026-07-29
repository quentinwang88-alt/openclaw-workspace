"""Thin adapter from visual-first original scripts to the central voiceover engine."""

from __future__ import annotations

import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List


WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
LIGHT_TRYON_SCRIPTS = WORKSPACE_ROOT / "skills" / "lightweight-tryon-video" / "scripts"
if str(LIGHT_TRYON_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(LIGHT_TRYON_SCRIPTS))

from light_tryon.voiceover_engine_bridge import (  # noqa: E402
    load_active_voiceover_hooks,
    run_voiceover_engine_variant,
)


VOICEOVER_BRIDGE_VERSION = (
    "original-central-voiceover-bridge-v33-core-proof-first"
)
VOICEOVER_ARGUMENT_CONTRACT_VERSION = "voiceover-argument-contract-v5-selling-point-authority"
VOICEOVER_KNOWLEDGE_SNAPSHOT_PATH = (
    Path.home()
    / ".openclaw"
    / "shared"
    / "data"
    / "voiceover_hook_knowledge_snapshot.json"
)


HOOK_SURFACE_CONTRACTS = {
    "DETAIL_SURPRISE": {
        "attention_move": "提示观众看一个容易忽略但当前画面能立刻证明的细节",
        "speaker_stance": "像朋友刚发现一个细节后顺手提醒，不像商品解说员",
        "surface_options": [
            "短称呼或自然语气＋具体需求/细节发现",
            "轻问题＋随后用画面回答",
            "动作发生后的一句自然反应＋连续观察",
        ],
        "avoid": ["泛泛说先看这个产品", "直接念卖点名称", "逐镜头说明词"],
    },
    "AUDIENCE_NEED_CALLOUT": {
        "attention_move": "先点名一种具体选择需求，再让画面立刻回答",
        "speaker_stance": "朋友式提醒，不制造焦虑",
        "surface_options": [
            "自然称呼＋具体需求问题＋随后给答案",
            "正在找某种明确特征吗＋先看这个",
            "像朋友确认需求一样开口＋自然过渡到两个相关细节",
        ],
        # Central voiceover may choose a brief, locale-native address for this
        # hook.  Keep the ban on empty universal promises, not on the address.
        "avoid": ["所有人都适合", "制造身材焦虑", "没有具体需求的泛人群承诺"],
    },
    "DISCOVERY_RESULT_PROMISE": {
        "attention_move": "先表达刚发现的结果，再用后续画面证明",
        "speaker_stance": "真实试穿后的自然发现",
        "surface_options": ["自然反应＋结果预告", "先说发现＋邀请继续看"],
        "avoid": ["夸张承诺", "没有画面证据的结果"],
    },
    "GENERAL_PRODUCT_SHARE": {
        "attention_move": "从当前动作或可见结果自然进入分享",
        "speaker_stance": "低销售压力的朋友分享",
        "surface_options": ["自然称呼＋具体观察", "动作指向＋个人判断"],
        "avoid": ["泛泛商品介绍", "主播式催单"],
    },
}


def _preferred_hook_id(direction: Dict[str, Any], allowed_hook_ids: List[str]) -> str:
    """Choose a speech hook from authorised content, never shot grammar."""

    allowed = set(allowed_hook_ids)
    bundle = (
        direction.get("content_bundle_brief")
        if isinstance(direction.get("content_bundle_brief"), dict)
        else {}
    )
    tension = _text(bundle.get("audience_tension", {}).get("text")).lower()
    fact_text = " ".join(
        _text(item.get("fact_text")).lower()
        for item in bundle.get("claim_atoms", [])
        if isinstance(item, dict)
    )
    pain_terms = ("身材", "版型", "贴身", "比例", "腰线", "显瘦", "显高", "อ้วน", "หุ่น")
    detail_terms = ("细节", "扣", "袖", "口袋", "领", "拉链", "抽褶", "五金", "detail")
    if any(token in tension for token in pain_terms) and "PAIN_REFRAME" in allowed:
        return "PAIN_REFRAME"
    if tension and "AUDIENCE_NEED_CALLOUT" in allowed:
        return "AUDIENCE_NEED_CALLOUT"
    if any(token in fact_text for token in detail_terms) and "DETAIL_SURPRISE" in allowed:
        return "DETAIL_SURPRISE"
    for candidate in (
        "GENERAL_PRODUCT_SHARE",
        "USER_ADVOCACY_STANCE",
        "VISUAL_RESULT_DIRECT",
        "DISCOVERY_RESULT_PROMISE",
        "DETAIL_SURPRISE",
    ):
        if candidate in allowed:
            return candidate
    return allowed_hook_ids[0] if allowed_hook_ids else ""


def resolve_voiceover_hook_policy(
    direction: Dict[str, Any],
    active_hook_ids: List[str] | set[str],
    *,
    requested_hook_id: str = "",
) -> Dict[str, Any]:
    """Resolve one truthful hook policy for single and candidate generation.

    Candidate generation used to name a raw preferred hook before the single
    generation path filtered it.  That allowed a ``VOC_*_PAIN_REFRAME`` label
    to silently execute ``DETAIL_SURPRISE``.  Keep eligibility, selection and
    candidate naming on one authority instead.
    """

    active = {_text(item) for item in active_hook_ids if _text(item)}
    bundle = (
        direction.get("content_bundle_brief")
        if isinstance(direction.get("content_bundle_brief"), dict)
        else {}
    )
    eligible: List[str] = []
    for item in bundle.get("preferred_hook_angles") or bundle.get("eligible_hook_ids") or []:
        hook_id = _text(item)
        if hook_id in active and hook_id not in eligible:
            eligible.append(hook_id)

    tension_text = _text(bundle.get("audience_tension", {}).get("text")).lower()
    pain_terms = (
        "身材", "版型", "贴身", "比例", "腰线", "显瘦", "显高", "อ้วน", "หุ่น"
    )
    if "PAIN_REFRAME" in eligible and not any(
        token in tension_text for token in pain_terms
    ):
        eligible = [item for item in eligible if item != "PAIN_REFRAME"]
    if not tension_text:
        eligible = [
            item for item in eligible
            if item not in {"PAIN_REFRAME", "AUDIENCE_NEED_CALLOUT"}
        ]

    if not eligible:
        fallback = _hook_id_for(direction, active)
        if fallback:
            eligible = [fallback]
    if not eligible:
        raise RuntimeError("中央口播引擎没有可用 ACTIVE 钩子")

    requested = _text(requested_hook_id)
    if requested and requested not in eligible:
        raise ValueError(
            f"请求的中央口播钩子不可用：{requested}；当前可用={','.join(eligible)}"
        )
    bundled_primary = _text(bundle.get("primary_hook_id"))
    selected = (
        requested
        or (bundled_primary if bundled_primary in eligible else "")
        or _preferred_hook_id(direction, eligible)
        or eligible[0]
    )
    return {
        "eligible_hook_ids": eligible,
        "selected_hook_id": selected,
        "requested_hook_id": requested,
        "tension_available": bool(tension_text),
    }


def _text(value: Any) -> str:
    return str(value or "").strip()


def _hook_qc_status(
    expected_hook_id: str,
    actual_hook_id: str,
    qc: Dict[str, Any],
) -> str:
    """Expose whether the chosen hook survived generation without new QC calls."""

    if _text(expected_hook_id) != _text(actual_hook_id):
        return "NOT_REALIZED"
    warning_codes = {
        _text(item.get("code")).upper()
        for item in (qc.get("warnings") or [])
        if isinstance(item, dict)
    }
    if any(
        code in {"HOOK_INTENT_NOT_REALIZED", "COPY_HOOK_INTENT_LOST"}
        or code == "HOOK_DELIVERY_TOO_FLAT"
        or ("GENERIC" in code and "OPENING" in code)
        for code in warning_codes
    ):
        return "WEAK"
    return "REALIZED"


def _hook_delivery_status(qc: Dict[str, Any]) -> str:
    warning_codes = {
        _text(item.get("code")).upper()
        for item in (qc.get("warnings") or [])
        if isinstance(item, dict)
    }
    if "HOOK_DELIVERY_TOO_FLAT" in warning_codes:
        return "FLAT_WARNING"
    return "ENERGETIC"


def _selection_readiness(
    qc: Dict[str, Any],
    *,
    target_duration_sec: float = 15.0,
) -> Dict[str, Any]:
    """Expose whether a generated candidate can be selected without repair.

    The central engine deliberately keeps modest duration overages as warnings
    so one long candidate cannot fail a batch.  Selection is a different
    decision: a 15-second production run should not automatically take a
    candidate whose central estimate already exceeds 15 seconds.
    """

    estimate = (
        qc.get("duration_estimate")
        if isinstance(qc.get("duration_estimate"), dict)
        else {}
    )
    try:
        estimated_sec = float(estimate.get("estimated_sec") or 0)
    except (TypeError, ValueError):
        estimated_sec = 0.0
    try:
        upper_sec = float(estimate.get("upper_sec") or 0)
    except (TypeError, ValueError):
        upper_sec = 0.0

    if estimated_sec > target_duration_sec:
        return {
            "status": "LONG_WARNING",
            "auto_selectable": False,
            "estimated_sec": estimated_sec,
            "upper_sec": upper_sec,
            "warning_code": "COPY_DURATION_ESTIMATE_WARNING",
            "message": "中心估时超过目标时长；保留候选供人工查看，自动选择时跳过。",
        }
    return {
        "status": "READY_FOR_SELECTION",
        "auto_selectable": True,
        "estimated_sec": estimated_sec,
        "upper_sec": upper_sec,
        "warning_code": "",
        "message": "",
    }


def _normalized_fact_text(value: Any) -> str:
    """Keep fact de-duplication narrow and deterministic for stage-0 copy."""

    return re.sub(r"[\s，,。；;：:、()（）【】\[\]‘’“”'\"-]", "", _text(value))


def _claim_role_priority(value: Any) -> int:
    role = _text(value).lower()
    if role in {"core", "core_result", "core_fact", "core_proof"}:
        return 2
    if role in {"proof", "visual_proof", "supporting_value", "supporting_fact"}:
        return 1
    return 0


def _dedupe_voiceover_claim_atoms(
    atoms: List[Dict[str, Any]],
    supported_shots: Dict[str, List[int]],
) -> List[Dict[str, Any]]:
    """Remove only overlapping, semantically contained facts before copy generation.

    This intentionally does not use embeddings or a broad synonym list.  It
    handles concrete duplicate pairs such as “短款衣长” and “短款衣长收在腰线附近”
    when the same visual evidence supports both.  The original content bundle
    and visual plan remain untouched.
    """

    unique: List[Dict[str, Any]] = []
    for raw_atom in atoms:
        if not isinstance(raw_atom, dict):
            continue
        claim_key = _text(raw_atom.get("claim_key"))
        fact_text = _text(raw_atom.get("fact_text"))
        if not claim_key or not fact_text or fact_text == "UNAVAILABLE":
            continue
        normalized = _normalized_fact_text(fact_text)
        if not normalized:
            continue
        candidate = dict(raw_atom)
        candidate_shots = set(supported_shots.get(claim_key) or [])
        duplicate_index = None
        for index, existing in enumerate(unique):
            existing_key = _text(existing.get("claim_key"))
            existing_normalized = _normalized_fact_text(existing.get("fact_text"))
            existing_shots = set(supported_shots.get(existing_key) or [])
            same_visual_evidence = bool(candidate_shots & existing_shots)
            same_or_contained_fact = (
                normalized == existing_normalized
                or normalized in existing_normalized
                or existing_normalized in normalized
            )
            if same_visual_evidence and same_or_contained_fact:
                duplicate_index = index
                break
        if duplicate_index is None:
            unique.append(candidate)
            continue

        existing = unique[duplicate_index]
        existing_normalized = _normalized_fact_text(existing.get("fact_text"))
        # Retain the richer wording, while retaining core status when the
        # shorter fact was the original narrative anchor.
        if len(normalized) > len(existing_normalized):
            if _claim_role_priority(existing.get("role")) > _claim_role_priority(candidate.get("role")):
                candidate["role"] = existing.get("role")
            unique[duplicate_index] = candidate
    return unique


def select_voiceover_claim_atoms(
    atoms: List[Dict[str, Any]],
    *,
    preferred_count: int = 2,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Choose a compact 15s spoken fact set without changing source facts."""

    valid = [dict(item) for item in atoms if isinstance(item, dict)]
    if len(valid) <= preferred_count:
        return valid, []
    ranked = sorted(
        enumerate(valid),
        key=lambda pair: (-_claim_role_priority(pair[1].get("role")), pair[0]),
    )
    chosen_indexes = {index for index, _ in ranked[:preferred_count]}
    selected = [item for index, item in enumerate(valid) if index in chosen_indexes]
    suppressed = [item for index, item in enumerate(valid) if index not in chosen_indexes]
    return selected, suppressed


def _country_code(value: str) -> str:
    normalized = _text(value).lower()
    mapping = {"泰国": "TH", "thailand": "TH", "越南": "VN", "vietnam": "VN", "马来西亚": "MY", "malaysia": "MY"}
    return mapping.get(normalized, normalized.upper()[:2] or "TH")


def _language_code(value: str) -> str:
    normalized = _text(value).lower()
    mapping = {"泰语": "th", "thai": "th", "越南语": "vi", "vietnamese": "vi", "马来语": "ms", "malay": "ms"}
    return mapping.get(normalized, normalized or "th")


def _parse_range_ms(value: str, fallback_start: int, fallback_end: int) -> tuple[int, int]:
    numbers = re.findall(r"\d+(?:\.\d+)?", _text(value))
    if len(numbers) >= 2:
        return int(float(numbers[0]) * 1000), int(float(numbers[1]) * 1000)
    return fallback_start, fallback_end


def _hook_id_for(direction: Dict[str, Any], active_ids: set[str]) -> str:
    reference = direction.get("execution_reference") if isinstance(direction.get("execution_reference"), dict) else {}
    mechanisms = {str(value or "").upper() for value in reference.get("proof_mechanisms", [])}
    plan = direction.get("structure_execution_plan") if isinstance(direction.get("structure_execution_plan"), dict) else {}
    hook_type = _text(plan.get("opening_mechanism") or reference.get("visual_hook_type")).upper()
    primary = _text(direction.get("p2_lite", {}).get("primary_observation")).lower()
    candidates: List[str] = []
    if "COMPARE" in mechanisms or "对比" in primary:
        candidates.append("BINARY_COMPARISON")
    if "DETAIL_MACRO" in mechanisms and any(token in primary for token in ("细节", "扣", "袖", "口袋", "领", "纹")):
        candidates.append("DETAIL_SURPRISE")
    if hook_type in {"RESULT_REVEAL", "PERSON_REVEAL", "PRODUCT_REVEAL"}:
        candidates.append("VISUAL_RESULT_DIRECT")
    candidates.extend(["GENERAL_PRODUCT_SHARE", "DISCOVERY_RESULT_PROMISE"])
    return next((item for item in candidates if item in active_ids), sorted(active_ids)[0] if active_ids else "")


def _visual_timeline(direction: Dict[str, Any], visual_plan: Dict[str, Any]) -> Dict[str, Any]:
    shots = [item for item in visual_plan.get("shots", []) if isinstance(item, dict)]
    bundle = direction.get("content_bundle_brief") if isinstance(direction.get("content_bundle_brief"), dict) else {}
    claim_text_by_key = {
        _text(item.get("claim_key")): _text(item.get("fact_text"))
        for item in bundle.get("claim_atoms", [])
        if isinstance(item, dict) and _text(item.get("claim_key")) and _text(item.get("fact_text"))
    }
    total_ms = 15000
    slots: List[Dict[str, Any]] = []
    for index, shot in enumerate(shots, 1):
        default_start = int((index - 1) * total_ms / max(1, len(shots)))
        default_end = int(index * total_ms / max(1, len(shots)))
        start_ms, end_ms = _parse_range_ms(
            _text(shot.get("duration") or shot.get("time_range")),
            default_start,
            default_end,
        )
        raw_visibility = _text(shot.get("product_visibility")).upper()
        engine_visibility = {
            "FULL": "high",
            "PARTIAL": "medium",
            "OCCLUDED": "low",
            "NONE": "low",
            "HIGH": "high",
            "MEDIUM": "medium",
            "LOW": "low",
        }.get(raw_visibility, "medium")
        slots.append(
            {
                "start_ms": start_ms,
                "end_ms": end_ms,
                "visual_event": _text(shot.get("shot_content")),
                "observations": [
                    value
                    for value in (
                        _text(shot.get("observable_action")),
                        _text(shot.get("product_visibility")),
                    )
                    if value
                ],
                "event_tags": [
                    _text(shot.get("structure_beat")),
                    _text(shot.get("carrier_mode")),
                ],
                "speakable_facts": [
                    claim_text_by_key[key]
                    for key in shot.get("supported_claim_keys", [])
                    if _text(key) in claim_text_by_key
                ],
                "recommended_line_function": "hook" if index == 1 else "proof",
                "product_visibility": engine_visibility,
                "confidence": 0.9,
            }
        )
    return {
        "duration_ms": total_ms,
        "mainline_summary": _text(bundle.get("content_mainline") or direction.get("p2_lite", {}).get("primary_observation")),
        "visual_slots": slots,
        "overall_confidence": 0.9,
        "uncertainties": direction.get("execution_reference", {}).get("unknown_fields", []),
    }


def _shot_for_time(start_ms: int, visual_plan: Dict[str, Any]) -> int:
    shots = [item for item in visual_plan.get("shots", []) if isinstance(item, dict)]
    for index, shot in enumerate(shots, 1):
        default_start = int((index - 1) * 15000 / max(1, len(shots)))
        default_end = int(index * 15000 / max(1, len(shots)))
        shot_start, shot_end = _parse_range_ms(
            _text(shot.get("duration") or shot.get("time_range")), default_start, default_end
        )
        if shot_start <= start_ms < shot_end:
            return index
    return min(len(shots), max(1, round(start_ms / 15000 * max(1, len(shots))) + 1))


def _silent_windows(
    lines: List[Dict[str, Any]],
    *,
    total_duration_ms: int,
) -> List[Dict[str, int]]:
    """Return real unspoken time windows, including a tail inside a shot."""

    intervals: List[tuple[int, int]] = []
    for line in lines:
        try:
            start_ms = max(0, int(line.get("start_ms") or 0))
            end_ms = min(total_duration_ms, int(line.get("end_ms") or 0))
        except (TypeError, ValueError):
            continue
        if end_ms > start_ms:
            intervals.append((start_ms, end_ms))
    merged: List[List[int]] = []
    for start_ms, end_ms in sorted(intervals):
        if not merged or start_ms > merged[-1][1]:
            merged.append([start_ms, end_ms])
        else:
            merged[-1][1] = max(merged[-1][1], end_ms)
    windows: List[Dict[str, int]] = []
    cursor = 0
    for start_ms, end_ms in merged:
        if start_ms > cursor:
            windows.append(
                {
                    "start_ms": cursor,
                    "end_ms": start_ms,
                    "duration_ms": start_ms - cursor,
                }
            )
        cursor = max(cursor, end_ms)
    if cursor < total_duration_ms:
        windows.append(
            {
                "start_ms": cursor,
                "end_ms": total_duration_ms,
                "duration_ms": total_duration_ms - cursor,
            }
        )
    return windows


def build_voiceover_variant_id(
    product_code: str,
    direction: Dict[str, Any],
    visual_plan: Dict[str, Any],
) -> str:
    """Pin voiceover cache identity to the creative and visual execution."""

    diversity = direction.get("creative_diversity_contract") if isinstance(direction.get("creative_diversity_contract"), dict) else {}
    blueprint = direction.get("creative_blueprint") if isinstance(direction.get("creative_blueprint"), dict) else {}
    visual_snapshot = {
        "creative_blueprint_id": blueprint.get("creative_blueprint_id", ""),
        "creative_diversity_contract_id": diversity.get("contract_id", ""),
        "shots": [
            {
                key: shot.get(key)
                for key in (
                    "duration",
                    "shot_content",
                    "observable_action",
                    "supported_claim_keys",
                    "structure_beat",
                    "carrier_mode",
                    "continuity_group",
                    "opening_mechanism",
                )
            }
            for shot in visual_plan.get("shots", [])
            if isinstance(shot, dict)
        ],
    }
    source_key = "|".join(
        [
            VOICEOVER_BRIDGE_VERSION,
            product_code,
            _text(direction.get("direction_assignment_id")),
            _text(direction.get("execution_reference", {}).get("execution_card_id")),
            _text(diversity.get("contract_id")),
            _text(blueprint.get("creative_blueprint_id")),
            hashlib.sha256(
                json.dumps(visual_snapshot, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
            ).hexdigest(),
        ]
    )
    return "OSG_REAL_" + hashlib.sha256(source_key.encode("utf-8")).hexdigest()[:16].upper()


def build_voiceover_expression_contract(
    direction: Dict[str, Any],
    visual_plan: Dict[str, Any],
) -> Dict[str, Any]:
    """Translate structure, creative voice and verified content into speech rules."""

    bundle = (
        direction.get("content_bundle_brief")
        if isinstance(direction.get("content_bundle_brief"), dict)
        else {}
    )
    execution_plan = (
        direction.get("structure_execution_plan")
        if isinstance(direction.get("structure_execution_plan"), dict)
        else {}
    )
    reference = (
        direction.get("execution_reference")
        if isinstance(direction.get("execution_reference"), dict)
        else {}
    )
    p2_lite = direction.get("p2_lite") if isinstance(direction.get("p2_lite"), dict) else {}
    shots = [item for item in visual_plan.get("shots", []) if isinstance(item, dict)]
    supported_shots: Dict[str, List[int]] = {}
    for shot_no, shot in enumerate(shots, 1):
        for claim_key in shot.get("supported_claim_keys") or []:
            key = _text(claim_key)
            if key:
                supported_shots.setdefault(key, []).append(shot_no)
    claim_atoms = []
    bundle_argument = (
        bundle.get("selling_argument")
        if isinstance(bundle.get("selling_argument"), dict)
        else {}
    )
    bundle_core_keys = {
        _text(key)
        for key in bundle_argument.get("core_proof_claim_keys") or []
        if _text(key)
    }
    for atom in _dedupe_voiceover_claim_atoms(
        [item for item in bundle.get("claim_atoms") or [] if isinstance(item, dict)],
        supported_shots,
    ):
        claim_key = _text(atom.get("claim_key"))
        fact_text = _text(atom.get("fact_text"))
        claim_atoms.append(
            {
                "claim_key": claim_key,
                "fact_text": fact_text,
                "role": _text(atom.get("role")) or "visual_proof",
                "argument_relation": (
                    "DIRECT_SUPPORT"
                    if claim_key in bundle_core_keys
                    else "OPTIONAL_PRODUCT_DETAIL"
                ),
                "supported_shot_nos": supported_shots.get(claim_key, []),
            }
        )
    shot_plan = [
        item for item in execution_plan.get("shot_plan", []) if isinstance(item, dict)
    ]
    opening = next(
        (
            _text(item.get("opening_mechanism"))
            for item in shot_plan
            if _text(item.get("opening_mechanism"))
        ),
        _text(execution_plan.get("opening_mechanism") or reference.get("visual_hook_type")),
    )
    blueprint = direction.get("creative_blueprint") if isinstance(direction.get("creative_blueprint"), dict) else {}
    persona = blueprint.get("persona") if isinstance(blueprint.get("persona"), dict) else {}
    scene = blueprint.get("scene") if isinstance(blueprint.get("scene"), dict) else {}
    event = blueprint.get("event_design") if isinstance(blueprint.get("event_design"), dict) else {}
    voice_identity = blueprint.get("voice_identity") if isinstance(blueprint.get("voice_identity"), dict) else {}
    retention_hook = blueprint.get("retention_hook") if isinstance(blueprint.get("retention_hook"), dict) else {}
    audio_constraints = [
        {
            "shot_no": index,
            "hard_constraint": _text(shot.get("audio_hard_constraint")) or "NONE",
            "preference": _text(shot.get("audio_preference")) or "SILENCE_PREFERRED",
        }
        for index, shot in enumerate(shots, 1)
    ]
    argument_contract = build_voiceover_argument_contract(direction, visual_plan)
    return {
        "schema_version": "voiceover-expression-contract-v2",
        "content_mainline": _text(
            argument_contract.get("content", {}).get("value_proposition", {}).get("text")
            or bundle.get("content_mainline")
            or p2_lite.get("primary_observation")
        ),
        "argument_contract": argument_contract,
        "claim_atoms": claim_atoms,
        "structure_context": {
            "macro_family_key": _text(execution_plan.get("macro_family_key")),
            "beat_sequence": [
                _text(item.get("structure_beat")) for item in shot_plan if _text(item.get("structure_beat"))
            ],
            "carrier_sequence": [
                _text(item.get("carrier_mode")) for item in shot_plan if _text(item.get("carrier_mode"))
            ],
            "continuity_sequence": [
                _text(item.get("continuity_group")) for item in shot_plan if _text(item.get("continuity_group"))
            ],
            "opening_mechanism": opening,
            "proof_mechanisms": [
                _text(item) for item in reference.get("proof_mechanisms", []) if _text(item)
            ],
            "visual_hook_type": _text(reference.get("visual_hook_type")),
        },
        "speech_policy": {
            "min_semantic_beats": 2,
            "max_semantic_beats": 3,
            "plan_mode": "deterministic_semantic_segments",
            "alignment_granularity": "whole_video_semantic_segment",
            "claim_coverage_required": False,
            "claim_exactly_once": False,
            "preferred_spoken_claim_count": 2,
            "maximum_spoken_claim_count": 3,
            "full_claim_coverage_required": False,
            "composite_claim_may_select_representative_detail": True,
            "allow_non_claim_rhetoric": True,
            "opening_must_name_visible_fact": False,
            "opening_attention_surface_required": True,
            "audience_or_need_callout_preferred": True,
            "natural_particles_preferred": True,
            "target_natural_particle_count": [1, 3],
            "relationship_language_preferred": True,
            "closing_mode": "natural_reaction_or_light_decision",
            "generic_cta_required": False,
            "fixed_choice_question_required": False,
            "preserve_silent_tail": False,
            "semantic_segments_may_span_shots": True,
            "target_nonspace_char_range": [120, 165],
            "char_range_is_hard_limit": False,
            "soft_warning_polish_enabled": False,
            "targeted_warning_polish_codes": [],
            "native_fluency_in_semantic_qc": True,
            "voiceover_policy_version": (
                "central-voiceover-v32-relational-language"
            ),
            "approved_sample_guidance_enabled": True,
            "narrative_anchor_preferred": True,
        },
        "conflict_policy": {
            "policy_version": "voiceover-priority-v19-argument-contract",
            "voiceover_priority_on_soft_alignment_conflict": True,
            "whole_video_evidence_is_sufficient": True,
            "local_timing_and_order_are_warnings": True,
            "natural_sound_may_duck_under_voiceover": True,
            "only_must_silent_blocks_voiceover": True,
            "hard_blocks": [
                "invented_or_forbidden_claim",
                "wrong_product",
                "broken_lineage_or_missing_evidence_anywhere",
                "unintelligible_target_language",
                "impossible_total_duration",
                "must_silent_violation",
            ],
        },
        "creative_voice_context": {
            "grounding_mode": _text(blueprint.get("voiceover_grounding_mode")),
            "creative_thesis": _text(blueprint.get("creative_thesis")),
            "creator_motivation": _text(blueprint.get("creator_motivation")),
            "viewer_relationship": _text(blueprint.get("viewer_relationship")),
            "speaker_identity": _text(persona.get("identity")),
            "speaking_personality": _text(persona.get("speaking_personality")),
            "scene_moment": _text(scene.get("moment")),
            "tone": _text(voice_identity.get("tone")),
            "relationship_mode": _text(voice_identity.get("relationship_mode")),
            "particle_density": _text(voice_identity.get("particle_density")),
            "sales_pressure": _text(voice_identity.get("sales_pressure")),
            "forbidden_tone": list(voice_identity.get("forbidden_tone") or []),
            "event_context": _text(event.get("natural_event")),
            "core_result_moment": _text(event.get("core_result_moment")),
            "opening_event": _text(retention_hook.get("opening_event")),
            "delayed_answer": _text(retention_hook.get("delayed_answer")),
        },
        # The original flow owns the argument, not the Thai hook wording.
        # An empty map deliberately delegates the hook surface to the central
        # voiceover engine so every downstream flow shares one expression layer.
        "hook_surface_contracts": {},
        "audio_policy": {
            "authority_order": [
                "audio_hard_constraint",
                "central_voiceover_semantic_segments",
                "audio_preference",
            ],
            "minimum_natural_sound_window_ms": 0,
            "silence_window_required": False,
            "shots": audio_constraints,
        },
        "forbidden_leaps": [
            "只看到口袋结构时不得声称口袋实用或可装物",
            "只看到衣长或腰线时不得声称显瘦、塑形或优化身材",
            "不得从单次画面推断舒适、百搭、多场景或材质性能",
        ],
    }


def build_voiceover_argument_contract(
    direction: Dict[str, Any],
    visual_plan: Dict[str, Any],
) -> Dict[str, Any]:
    """Compile original-script knowledge into the central speech contract."""

    bundle = direction.get("content_bundle_brief") if isinstance(direction.get("content_bundle_brief"), dict) else {}
    blueprint = direction.get("creative_blueprint") if isinstance(direction.get("creative_blueprint"), dict) else {}
    persona = blueprint.get("persona") if isinstance(blueprint.get("persona"), dict) else {}
    scene = blueprint.get("scene") if isinstance(blueprint.get("scene"), dict) else {}
    event = blueprint.get("event_design") if isinstance(blueprint.get("event_design"), dict) else {}
    value = bundle.get("value_proposition") if isinstance(bundle.get("value_proposition"), dict) else {}
    tension = bundle.get("audience_tension") if isinstance(bundle.get("audience_tension"), dict) else {}
    selling_argument = bundle.get("selling_argument") if isinstance(bundle.get("selling_argument"), dict) else {}
    tension_available = (
        _text(bundle.get("content_mode")) == "SELLING_ARGUMENT"
        and _text(tension.get("status")) == "AVAILABLE"
        and bool(_text(tension.get("text")))
    )
    shots = [item for item in visual_plan.get("shots", []) if isinstance(item, dict)]
    supported: Dict[str, List[int]] = {}
    for index, shot in enumerate(shots, 1):
        shot_no = int(shot.get("shot_no") or index)
        for claim_key in shot.get("supported_claim_keys") or []:
            key = _text(claim_key)
            if key:
                supported.setdefault(key, []).append(shot_no)
    core_proof_keys = {
        _text(key) for key in selling_argument.get("core_proof_claim_keys") or [] if _text(key)
    }
    optional_visual_keys = {
        _text(key) for key in selling_argument.get("optional_visual_claim_keys") or [] if _text(key)
    }
    proof_atoms = []
    raw_atoms = [
        item
        for item in bundle.get("proof_atoms") or bundle.get("claim_atoms") or []
        if isinstance(item, dict)
    ]
    for atom in _dedupe_voiceover_claim_atoms(raw_atoms, supported):
        claim_key = _text(atom.get("claim_key"))
        fact_text = _text(atom.get("fact_text"))
        proof_atoms.append(
            {
                "claim_key": claim_key,
                "fact_text": fact_text,
                "evidence_mode": "VISUAL_REQUIRED",
                "supported_shot_nos": supported.get(claim_key, []),
                "proof_scope": (
                    "SPOKEN_CORE" if claim_key in core_proof_keys
                    else "VISUAL_ONLY" if claim_key in optional_visual_keys
                    else "SPOKEN_CORE"
                ),
            }
        )
    material = {
        "version": VOICEOVER_ARGUMENT_CONTRACT_VERSION,
        "bundle_id": bundle.get("content_bundle_id"),
        "value": value,
        "tension": tension,
        "selling_argument": selling_argument,
        "proof_atoms": proof_atoms,
        "voice": {
            "identity": persona.get("identity"),
            "relationship": blueprint.get("viewer_relationship"),
            "personality": persona.get("speaking_personality"),
            "scene": scene.get("moment"),
            "event_context": event.get("natural_event"),
            "core_result_moment": event.get("core_result_moment"),
        },
    }
    return {
        "contract_schema_version": VOICEOVER_ARGUMENT_CONTRACT_VERSION,
        "contract_id": "VAC_" + hashlib.sha256(
            json.dumps(material, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()[:24],
        "content": {
            "value_proposition": {
                "text": _text(value.get("text")),
                "authority": _text(value.get("authority")) or "UNAVAILABLE",
                "allowed_strength": _text(value.get("allowed_strength")) or "NONE",
                "status": _text(value.get("status")) or "SELLING_POINT_UNAVAILABLE",
                "proof_thesis": _text(event.get("core_result_moment"))
                or _text(value.get("proof_thesis")),
                "decision_thesis": _text(value.get("decision_thesis")),
                "source_claim_ids": list(value.get("source_claim_ids") or []),
            },
            "audience_tension": {
                "text": _text(tension.get("text")),
                "usage_scope": _text(tension.get("usage_scope")) or "HOOK_ONLY",
                "status": _text(tension.get("status")) or "UNAVAILABLE",
            },
            "proof_atoms": proof_atoms,
            "selling_argument": {
                "argument_id": _text(selling_argument.get("argument_id")),
                "status": _text(selling_argument.get("status")) or "UNAVAILABLE",
                "source": _text(selling_argument.get("source")) or "UNAVAILABLE",
                "source_claim_ids": list(selling_argument.get("source_claim_ids") or []),
                "core_value": _text(selling_argument.get("core_value")),
                "target_need": _text(selling_argument.get("target_need")),
                "proof_thesis": _text(selling_argument.get("proof_thesis")),
                "decision_thesis": _text(selling_argument.get("decision_thesis")),
                "allowed_strength": _text(selling_argument.get("allowed_strength")),
                "verification_status": _text(selling_argument.get("verification_status")),
                "evidence_requirement": _text(selling_argument.get("evidence_requirement")),
                "operator_priority": _text(selling_argument.get("operator_priority")),
                "proof_match_status": _text(selling_argument.get("proof_match_status")) or "NOT_APPLICABLE",
                "core_proof_claim_keys": sorted(core_proof_keys),
                "optional_visual_claim_keys": sorted(optional_visual_keys),
            },
        },
        "creative_voice_context": {
            "grounding_mode": _text(blueprint.get("voiceover_grounding_mode")),
            "speaker_identity": _text(persona.get("identity")),
            "viewer_relationship": _text(blueprint.get("viewer_relationship")),
            "speaking_personality": _text(persona.get("speaking_personality")),
            "scene_moment": _text(scene.get("moment")),
            "event_context": _text(event.get("natural_event")),
            "core_result_moment": _text(event.get("core_result_moment")),
        },
        "expression_policy": {
            "preferred_hook_angles": [
                _text(item)
                for item in bundle.get("preferred_hook_angles") or bundle.get("eligible_hook_ids") or []
                if _text(item)
            ],
            "target_semantic_segments": 3,
            "candidate_count": 3,
            "generic_cta_required": False,
            "fixed_choice_question_required": False,
            "selling_point_authority": "GOVERNED_LIBRARY",
            "visual_proof_match_is_blocking": False,
            # This is soft expression guidance, not a QC gate.  Without an
            # authorised audience tension, the model should open from a
            # personal selection criterion or direct observation instead of
            # inventing an opposing opinion (for example, "who says...").
            "rhetorical_conflict_allowed": tension_available,
            "hook_stance": (
                "AUTHORISED_AUDIENCE_TENSION"
                if tension_available
                else "PERSONAL_SELECTION_CRITERION"
            ),
        },
        "timing_policy": {
            "target_duration_seconds": 15,
            "target_nonspace_char_range": [120, 165],
            "char_range_is_hard_limit": False,
        },
        "forbidden_claims": [
            "未经授权的显瘦或显高效果",
            "未经授权的舒适、保暖或材质性能",
            "价格、销量、库存和社会证明",
        ],
    }


def run_central_voiceover(
    *,
    product_code: str,
    target_country: str,
    target_language: str,
    direction: Dict[str, Any],
    visual_plan: Dict[str, Any],
    voiceover_root: str = "",
    voiceover_db_path: str = "",
    model_command: str = "",
    qc_model_command: str = "",
    candidate_hook_id: str = "",
    candidate_id: str = "",
    force_duration_compression: bool = False,
) -> Dict[str, Any]:
    hooks = load_active_voiceover_hooks(
        voiceover_root or None,
        db_path=voiceover_db_path or None,
    )
    active_ids = {str(item.get("hook_id") or "") for item in hooks if item.get("hook_id")}
    bundle = (
        direction.get("content_bundle_brief")
        if isinstance(direction.get("content_bundle_brief"), dict)
        else {}
    )
    hook_policy = resolve_voiceover_hook_policy(
        direction,
        active_ids,
        requested_hook_id=candidate_hook_id,
    )
    allowed_hook_ids = list(hook_policy["eligible_hook_ids"])
    preferred_hook_id = _text(hook_policy["selected_hook_id"])
    p2_lite = direction.get("p2_lite") if isinstance(direction.get("p2_lite"), dict) else {}
    supported_shots: Dict[str, List[int]] = {}
    for shot_no, shot in enumerate(
        [item for item in visual_plan.get("shots", []) if isinstance(item, dict)], 1
    ):
        for claim_key in shot.get("supported_claim_keys") or []:
            key = _text(claim_key)
            if key:
                supported_shots.setdefault(key, []).append(shot_no)
    raw_claim_atoms = [
        item
        for item in bundle.get("proof_atoms") or bundle.get("claim_atoms", [])
        if isinstance(item, dict)
    ]
    deduped_claim_atoms = _dedupe_voiceover_claim_atoms(
        raw_claim_atoms, supported_shots
    )
    selling_argument = (
        bundle.get("selling_argument")
        if isinstance(bundle.get("selling_argument"), dict)
        else {}
    )
    core_proof_keys = {
        _text(key)
        for key in selling_argument.get("core_proof_claim_keys") or []
        if _text(key)
    }
    core_atoms = [
        atom for atom in deduped_claim_atoms
        if _text(atom.get("claim_key")) in core_proof_keys
    ]
    # Factual-only / historical bundles may predate the split.  Preserve a
    # deterministic one-fact fallback, but never restore the old default of
    # narrating two unrelated details.
    claim_atoms, suppressed_claim_atoms = select_voiceover_claim_atoms(
        core_atoms or deduped_claim_atoms,
        preferred_count=1,
    )
    selling_points = [
        _text(item.get("fact_text"))
        for item in claim_atoms
        if _text(item.get("fact_text")) and _text(item.get("fact_text")) != "UNAVAILABLE"
    ]
    primary = selling_points[0] if selling_points else _text(p2_lite.get("primary_observation"))
    secondary_points = selling_points[1:]
    if primary and primary not in selling_points:
        selling_points.insert(0, primary)
    if not selling_points:
        raise ValueError("内容论证包没有可交给中央口播引擎的已确认卖点")
    # These are not free-form selling points: each entry already exists in the
    # content bundle and has at least one concrete visual slot. The central
    # engine may speak the fact, but cannot upgrade it into a benefit.
    visual_fact_inputs = [
        {
            "claim_key": _text(atom.get("claim_key")),
            "fact_text": _text(atom.get("fact_text")),
            "supported_shot_nos": supported_shots.get(_text(atom.get("claim_key")), []),
        }
        for atom in claim_atoms
        if _text(atom.get("fact_text"))
        and _text(atom.get("fact_text")) != "UNAVAILABLE"
        and supported_shots.get(_text(atom.get("claim_key")))
    ]
    variant_id = build_voiceover_variant_id(product_code, direction, visual_plan)
    if candidate_id or preferred_hook_id:
        suffix = _text(candidate_id) or preferred_hook_id
        variant_id += "_" + hashlib.sha256(suffix.encode("utf-8")).hexdigest()[:8].upper()
    if force_duration_compression:
        # A selected long candidate must get a fresh engine job rather than
        # silently returning the cached long draft under the same variant id.
        variant_id += "_DURATION_FIX"
    expression_contract = build_voiceover_expression_contract(direction, visual_plan)
    effective_claim_keys = {_text(item.get("claim_key")) for item in claim_atoms}
    expression_contract["claim_atoms"] = [
        item
        for item in expression_contract.get("claim_atoms") or []
        if _text(item.get("claim_key")) in effective_claim_keys
    ]
    argument_contract = expression_contract.get("argument_contract")
    if isinstance(argument_contract, dict):
        content = argument_contract.get("content")
        if isinstance(content, dict):
            content["proof_atoms"] = [
                item
                for item in content.get("proof_atoms") or []
                if _text(item.get("claim_key")) in effective_claim_keys
            ]
        expression_policy = argument_contract.get("expression_policy")
        if isinstance(expression_policy, dict):
            expression_policy.update(
                {
                    "preferred_spoken_claim_count": min(1, len(claim_atoms)),
                    "maximum_spoken_claim_count": 2,
                    "full_claim_coverage_required": False,
                    "composite_claim_may_select_representative_detail": True,
                }
            )
    expression_contract.setdefault("speech_policy", {}).update(
        {
            "claim_coverage_required": False,
            "preferred_spoken_claim_count": min(1, len(claim_atoms)),
            "maximum_spoken_claim_count": 2,
            "full_claim_coverage_required": False,
            "composite_claim_may_select_representative_detail": True,
            # V31 quality findings rank candidates instead of entering another
            # automatic rewrite loop.  Only a human-requested duration fix
            # reuses the existing single compression pass.
            "soft_warning_polish_enabled": bool(force_duration_compression),
            "targeted_warning_polish_codes": (
                ["COPY_DURATION_ESTIMATE_WARNING"]
                if force_duration_compression
                else []
            ),
            "manual_duration_compression_requested": bool(force_duration_compression),
        }
    )
    ready = run_voiceover_engine_variant(
        product={
            "product_id": product_code,
            "source_product_code": product_code,
            "core_selling_points": selling_points,
            "visual_fact_inputs": visual_fact_inputs,
            "market": _country_code(target_country),
            "language": _language_code(target_language),
        },
        strategy={
            "hook_id": preferred_hook_id,
            "allowed_hook_ids": allowed_hook_ids,
            "primary_selling_point": primary,
            "secondary_selling_points": secondary_points,
            "expression_contract": expression_contract,
        },
        variant={
            "variant_id": variant_id,
            "target_duration_seconds": 15,
            "format_type": "original_15s",
            "plan_version": VOICEOVER_BRIDGE_VERSION,
            "production_batch_id": f"stage0:{product_code}:{VOICEOVER_BRIDGE_VERSION}",
            "strategy_group_id": _text(direction.get("direction_assignment_id")),
        },
        timeline=_visual_timeline(direction, visual_plan),
        root=voiceover_root or None,
        db_path=voiceover_db_path or None,
        knowledge_snapshot_path=VOICEOVER_KNOWLEDGE_SNAPSHOT_PATH,
        model_command=model_command,
        qc_model_command=qc_model_command,
    )
    ready_hook_id = _text(ready.get("hook_id")) or preferred_hook_id
    engine_qc = (
        ready.get("voiceover_engine_qc")
        if isinstance(ready.get("voiceover_engine_qc"), dict)
        else {}
    )
    if candidate_hook_id and ready_hook_id != preferred_hook_id:
        raise ValueError(
            "中央口播候选钩子执行错位："
            f"请求={preferred_hook_id}，实际={ready_hook_id}"
        )
    minimum_claims = min(1, len(claim_atoms))
    selected_claim_count = int(ready.get("selected_claim_count") or 0)
    if selected_claim_count < minimum_claims:
        raise ValueError(
            "中央口播引擎未能把内容论证包映射成至少一个核心可验证卖点"
            f"（期望>={minimum_claims}，实际={selected_claim_count}）"
        )
    by_shot: Dict[int, List[Dict[str, Any]]] = {}
    for beat in ready.get("beats", []) or []:
        if not isinstance(beat, dict):
            continue
        shot_no = _shot_for_time(int(beat.get("suggested_start_ms") or 0), visual_plan)
        by_shot.setdefault(shot_no, []).append(beat)
    lines: List[Dict[str, Any]] = []
    for shot_no, beats in sorted(by_shot.items()):
        segment_start_ms = min(int(item.get("suggested_start_ms") or 0) for item in beats)
        segment_end_ms = max(
            int(item.get("suggested_end_ms") or segment_start_ms + 1) for item in beats
        )
        lines.append(
            {
                "shot_no": shot_no,
                "end_shot_no": _shot_for_time(
                    max(segment_start_ms, segment_end_ms - 1), visual_plan
                ),
                "start_ms": segment_start_ms,
                "end_ms": segment_end_ms,
                "voiceover_text_target_language": " ".join(_text(item.get("speech_text")) for item in beats if _text(item.get("speech_text"))),
                "voiceover_text_zh": " ".join(_text(item.get("chinese_translation")) for item in beats if _text(item.get("chinese_translation"))),
                "spoken_line_task": "+".join(dict.fromkeys(_text(item.get("role")) for item in beats if _text(item.get("role")))),
            }
        )
    shot_count = len([item for item in visual_plan.get("shots", []) if isinstance(item, dict)])
    spoken = {
        covered_shot
        for item in lines
        for covered_shot in range(
            int(item["shot_no"]),
            int(item.get("end_shot_no") or item["shot_no"]) + 1,
        )
    }
    total_duration_ms = 15000
    minimum_silence_window_ms = int(
        expression_contract.get("audio_policy", {}).get(
            "minimum_natural_sound_window_ms", 0
        )
        or 0
    )
    silent_windows = _silent_windows(
        lines,
        total_duration_ms=total_duration_ms,
    )
    selection_readiness = _selection_readiness(
        engine_qc,
        target_duration_sec=15.0,
    )
    return {
        "voiceover_plan_schema_version": "visual-first-voiceover-v5",
        "bridge_version": VOICEOVER_BRIDGE_VERSION,
        "copy_generation_mode": "MODEL" if _text(model_command) else "LOCAL_DETERMINISTIC",
        "candidate_id": _text(candidate_id) or preferred_hook_id,
        "source": "voiceover_copy_engine",
        "hook_id": ready_hook_id or allowed_hook_ids[0],
        "selected_hook_id": preferred_hook_id,
        "hook_structure_status": (
            "MATCHED"
            if preferred_hook_id == ready_hook_id
            else "MISMATCHED"
        ),
        "hook_delivery_status": _hook_delivery_status(engine_qc),
        "hook_qc_status": _hook_qc_status(
            preferred_hook_id,
            ready_hook_id,
            engine_qc,
        ),
        "selection_readiness": selection_readiness,
        "eligible_hook_ids": allowed_hook_ids,
        "effective_claim_keys": [
            _text(item.get("claim_key")) for item in claim_atoms
        ],
        "suppressed_claim_keys": [
            _text(item.get("claim_key")) for item in suppressed_claim_atoms
        ],
        "selected_claim_ids": ready.get("selected_claim_ids", []),
        "selected_claim_count": selected_claim_count,
        "hook_source": "voiceover_copy_engine.hook_archetypes",
        "expression_contract": ready.get("voiceover_expression_contract") or expression_contract,
        "copy_plan": ready.get("voiceover_copy_plan") or {},
        "lines": lines,
        "silent_shots": [index for index in range(1, shot_count + 1) if index not in spoken],
        "silent_windows": silent_windows,
        "total_duration_ms": total_duration_ms,
        "minimum_silence_window_ms": minimum_silence_window_ms,
        "engine_provenance": {
            "job_id": ready.get("voiceover_engine_job_id"),
            "video_id": ready.get("voiceover_engine_video_id"),
            "analysis_id": ready.get("voiceover_engine_analysis_id"),
            "qc": engine_qc,
            "manual_duration_compression_requested": bool(force_duration_compression),
            "downstream_rewritten": False,
        },
    }


def run_central_voiceover_candidates(
    *,
    candidate_count: int = 3,
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    """Generate governed alternatives through the same central engine."""

    direction = kwargs.get("direction") if isinstance(kwargs.get("direction"), dict) else {}
    hooks = load_active_voiceover_hooks(
        kwargs.get("voiceover_root") or None,
        db_path=kwargs.get("voiceover_db_path") or None,
    )
    active_ids = {
        _text(item.get("hook_id")) for item in hooks if _text(item.get("hook_id"))
    }
    policy = resolve_voiceover_hook_policy(direction, active_ids)
    hook_ids = list(policy["eligible_hook_ids"])
    preferred_order = [
        "PAIN_REFRAME",
        "AUDIENCE_NEED_CALLOUT",
        "DISCOVERY_RESULT_PROMISE",
        "DETAIL_SURPRISE",
        "GENERAL_PRODUCT_SHARE",
    ]
    ordered = [item for item in preferred_order if item in hook_ids]
    ordered.extend(item for item in hook_ids if item not in ordered)
    outputs: List[Dict[str, Any]] = []
    for index, hook_id in enumerate(ordered[: max(1, min(3, int(candidate_count)))], 1):
        outputs.append(
            run_central_voiceover(
                **kwargs,
                candidate_hook_id=hook_id,
                candidate_id=f"VOC_{index}_{hook_id}",
            )
        )
    if not outputs:
        outputs.append(run_central_voiceover(**kwargs, candidate_id="VOC_1_DEFAULT"))
    return outputs
