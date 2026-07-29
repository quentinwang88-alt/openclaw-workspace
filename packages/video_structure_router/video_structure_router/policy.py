"""Versioned routing policy and contract construction."""

from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Dict, Iterable, List, Sequence, Tuple

from .models import RouteRequest, StructureCandidate


POLICY_VERSION = "structure-router-v1.1-exploration-rotation"
FAMILY_POLICY_VERSION = "exact-coarse-beat-v1"
COMPATIBILITY_POLICY_VERSION = "original-flow-capability-v1"
FEEDBACK_POLICY = {
    "display_only_below_videos": 8,
    "directional_min_videos": 8,
    "directional_min_products": 2,
    "routing_min_videos": 20,
    "routing_min_products": 3,
    "routing_min_execution_variants": 2,
}

COUNTRY_ALIASES = {
    "TH": {"TH", "泰国", "THAILAND"},
    "VN": {"VN", "越南", "VIETNAM"},
    "MY": {"MY", "马来西亚", "MALAYSIA"},
    "MX": {"MX", "墨西哥", "MEXICO"},
    "PH": {"PH", "菲律宾", "PHILIPPINES"},
    "ID": {"ID", "印度尼西亚", "印尼", "INDONESIA"},
}


def _stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def stable_hash(value: Any) -> str:
    return hashlib.sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _normalized_country_values(value: str) -> set:
    text = str(value or "").strip().upper()
    if not text:
        return set()
    for canonical, aliases in COUNTRY_ALIASES.items():
        normalized_aliases = {item.upper() for item in aliases}
        if text in normalized_aliases:
            return normalized_aliases | {canonical}
    return {text}


def _text_match(target: str, candidates: Iterable[str]) -> bool:
    target_text = str(target or "").strip().lower()
    if not target_text:
        return False
    for value in candidates:
        candidate = str(value or "").strip().lower()
        if candidate and (target_text in candidate or candidate in target_text):
            return True
    return False


def country_match(target: str, values: Iterable[str]) -> bool:
    target_values = _normalized_country_values(target)
    if not target_values:
        return False
    candidate_values = set()
    for value in values:
        candidate_values.update(_normalized_country_values(value))
    return bool(target_values & candidate_values)


def visual_axis_difference(left: StructureCandidate, right: StructureCandidate) -> Tuple[int, int]:
    different = 0
    comparable = 0
    for axis in (
        "content_carrier",
        "continuity_mode",
        "shot_count_band",
        "cut_density",
        "visual_hook_type",
    ):
        left_value = left.visual_archetype.get(axis, "UNAVAILABLE")
        right_value = right.visual_archetype.get(axis, "UNAVAILABLE")
        if left_value == "UNAVAILABLE" or right_value == "UNAVAILABLE":
            continue
        comparable += 1
        if left_value != right_value:
            different += 1
    return different, comparable


def visual_distance(left: StructureCandidate, right: StructureCandidate) -> float:
    different, comparable = visual_axis_difference(left, right)
    if comparable == 0:
        return 0.0
    return different / comparable


def narrative_distance(left: StructureCandidate, right: StructureCandidate) -> float:
    return 0.0 if left.macro_family_key == right.macro_family_key else 1.0


def candidate_is_compatible(candidate: StructureCandidate, request: RouteRequest) -> Tuple[bool, List[str]]:
    capabilities = request.capabilities or {}
    reasons: List[str] = []
    allowed_carriers = {str(item) for item in capabilities.get("allowed_carriers", []) if str(item)}
    if allowed_carriers and candidate.content_carrier and candidate.content_carrier not in allowed_carriers:
        reasons.append(f"carrier={candidate.content_carrier} 不受当前流程支持")
    allowed_continuity = {str(item) for item in capabilities.get("allowed_continuity_modes", []) if str(item)}
    if allowed_continuity and candidate.continuity_mode and candidate.continuity_mode not in allowed_continuity:
        reasons.append(f"continuity={candidate.continuity_mode} 不受当前流程支持")
    forbidden_beats = {str(item) for item in capabilities.get("forbidden_beats", []) if str(item)}
    conflicting_beats = [beat for beat in candidate.beat_sequence if beat in forbidden_beats]
    if conflicting_beats:
        reasons.append(f"结构包含流程禁止的 Beat: {','.join(conflicting_beats)}")

    min_shots = capabilities.get("min_shots")
    max_shots = capabilities.get("max_shots")
    if candidate.shot_count_min is not None and candidate.shot_count_max is not None:
        if min_shots is not None and candidate.shot_count_max < int(min_shots):
            reasons.append(f"实测镜头上限 {candidate.shot_count_max} 低于流程下限 {min_shots}")
        if max_shots is not None and candidate.shot_count_min > int(max_shots):
            reasons.append(f"实测镜头下限 {candidate.shot_count_min} 高于流程上限 {max_shots}")
        if candidate.shot_count_median is not None and min_shots is not None and candidate.shot_count_median < int(min_shots):
            reasons.append(f"实测镜头中位数 {candidate.shot_count_median:g} 低于流程下限 {min_shots}")
        if candidate.shot_count_median is not None and max_shots is not None and candidate.shot_count_median > int(max_shots):
            reasons.append(f"实测镜头中位数 {candidate.shot_count_median:g} 高于流程上限 {max_shots}")
    return not reasons, reasons


def base_score(candidate: StructureCandidate, request: RouteRequest) -> float:
    evidence = {
        "HUMAN_VALIDATED": 1.0,
        "STABLE": 0.95,
        "VIDEO_SUPPORTED": 0.88,
        "VIDEO_SUPPORTED_PARTIAL": 0.78,
        "BOOTSTRAP": 0.46,
    }.get(candidate.evidence_tier, 0.35)
    score = evidence
    if _text_match(request.category, candidate.categories) or _text_match(request.product_type, candidate.categories):
        score += 0.18
    if country_match(request.target_country, candidate.countries):
        score += 0.08
    score += min(0.12, math.log1p(max(0, candidate.distinct_videos)) / 60.0)
    score += min(0.06, max(0.0, candidate.cohesion) * 0.06)
    score += min(0.06, max(0.0, candidate.extraction_confidence) * 0.06)
    if candidate.duration_median is not None and request.duration_seconds > 0:
        relative_gap = abs(candidate.duration_median - request.duration_seconds) / request.duration_seconds
        score += max(-0.08, 0.05 - relative_gap * 0.08)
    return score


def _tie_break(candidate: StructureCandidate, seed: int) -> float:
    digest = hashlib.sha256(f"{seed}:{candidate.candidate_key}".encode("utf-8")).hexdigest()
    return int(digest[:8], 16) / 0xFFFFFFFF / 10000.0


def _recent_usage(candidate: StructureCandidate, request: RouteRequest) -> int:
    usage = (request.capabilities or {}).get("recent_cluster_usage") or {}
    if not isinstance(usage, dict):
        return 0
    versioned_key = f"{candidate.cluster_id}:{candidate.cluster_version}"
    if versioned_key in usage:
        return max(0, int(usage.get(versioned_key) or 0))
    return max(0, int(usage.get(str(candidate.cluster_id)) or 0))


def _exploration_rotation_penalty(
    candidate: StructureCandidate,
    request: RouteRequest,
) -> float:
    count = _recent_usage(candidate, request)
    if count <= 0:
        return 0.0
    # A recently used exploratory cluster should yield to a comparable unseen
    # one.  The cap prevents history from overriding real compatibility and
    # evidence differences forever.
    return min(0.26, 0.16 + 0.025 * (count - 1))


def select_candidates(
    candidates: Sequence[StructureCandidate],
    request: RouteRequest,
) -> Tuple[List[Tuple[StructureCandidate, float, str]], List[str], Dict[str, Any]]:
    compatible: List[StructureCandidate] = []
    rejected: Dict[str, List[str]] = {}
    for candidate in candidates:
        ok, reasons = candidate_is_compatible(candidate, request)
        if ok:
            compatible.append(candidate)
        else:
            rejected[candidate.candidate_key] = reasons

    ranked = sorted(
        compatible,
        key=lambda item: (-(base_score(item, request) + _tie_break(item, request.random_seed)), item.candidate_key),
    )
    if not ranked:
        raise RuntimeError("没有与当前生产流程能力兼容的视频结构候选")

    count = max(1, min(int(request.direction_count or 1), len(ranked)))
    selected: List[Tuple[StructureCandidate, float, str]] = []
    remaining = list(ranked)

    role_names = ["BASELINE", "VISUAL_CONTRAST", "PROCESS_OR_PERSON", "EXPERIMENTAL_DIVERSITY"]
    while remaining and len(selected) < count:
        index = len(selected)
        role = role_names[index] if index < len(role_names) else f"EXPLORE_{index + 1}"
        if index == 0:
            chosen = remaining[0]
            score = base_score(chosen, request)
        else:
            def objective(item: StructureCandidate) -> float:
                diversity = sum(
                    0.58 * visual_distance(item, existing[0]) + 0.42 * narrative_distance(item, existing[0])
                    for existing in selected
                ) / len(selected)
                process_bonus = 0.0
                if index == 2 and (
                    "USE_PROCESS" in item.beat_sequence
                    or item.content_carrier in {"WEARER_ACTIVE", "HAND_ONLY"}
                ):
                    process_bonus = 0.18
                video_bonus = 0.06 if item.evidence_tier.startswith("VIDEO_") else 0.0
                return (
                    base_score(item, request) * 0.55
                    + diversity * 0.45
                    + process_bonus
                    + video_bonus
                    - _exploration_rotation_penalty(item, request)
                )

            chosen = max(remaining, key=lambda item: (objective(item), item.candidate_key))
            score = objective(chosen)
        selected.append((chosen, round(score, 6), role))
        remaining.remove(chosen)

    degraded: List[str] = []
    if len(selected) < request.direction_count:
        degraded.append(f"仅找到 {len(selected)} 个兼容候选，少于请求的 {request.direction_count} 个方向")
    family_count = len({item[0].macro_family_key for item in selected})
    if len(selected) >= 2 and family_count < 2:
        degraded.append("方向只覆盖 1 个叙事家族")
    for left_index in range(len(selected)):
        for right_index in range(left_index + 1, len(selected)):
            different, comparable = visual_axis_difference(selected[left_index][0], selected[right_index][0])
            if comparable >= 2 and different < 2:
                degraded.append(
                    f"{left_index + 1}/{right_index + 1} 方向仅有 {different} 个可确认视觉轴不同"
                )
            elif comparable < 2:
                degraded.append(
                    f"{left_index + 1}/{right_index + 1} 方向可比较视觉轴不足，无法证明至少两轴不同"
                )

    diagnostics = {
        "candidate_count": len(candidates),
        "compatible_count": len(compatible),
        "rejected_count": len(rejected),
        "rejected": rejected,
        "selected_family_count": family_count,
        "recent_cluster_usage": dict(
            (request.capabilities or {}).get("recent_cluster_usage") or {}
        ),
        "selected_recent_usage": {
            item[0].candidate_key: _recent_usage(item[0], request)
            for item in selected
        },
    }
    return selected, list(dict.fromkeys(degraded)), diagnostics


def build_structure_contract(
    candidate: StructureCandidate,
    *,
    selection_run_id: str,
    direction_assignment_id: str,
    direction_index: int,
    output_slot: str,
    direction_role: str,
    policy_version: str,
    data_snapshot_hash: str,
) -> Dict[str, Any]:
    hard: Dict[str, Any] = {
        "beat_sequence": candidate.beat_sequence or "UNAVAILABLE",
        "required_beats": candidate.required_beats or "UNAVAILABLE",
        "content_carrier": candidate.content_carrier or "UNAVAILABLE",
        "continuity_mode": candidate.continuity_mode or "UNAVAILABLE",
        "cut_density": candidate.cut_density or "UNAVAILABLE",
        "visual_hook_type": candidate.visual_hook_type or "UNAVAILABLE",
        "proof_mechanisms": candidate.proof_mechanisms or "UNAVAILABLE",
        "ending_pattern": candidate.ending_pattern or "UNAVAILABLE",
        "shot_count": (
            {
                "min": candidate.shot_count_min,
                "max": candidate.shot_count_max,
                "median": candidate.shot_count_median,
                "authority": "VIDEO_MEASURED",
            }
            if candidate.shot_count_min is not None
            else "UNAVAILABLE"
        ),
    }
    unknown = [key for key, value in hard.items() if value == "UNAVAILABLE"]
    translation = {
        "narrative_instruction": (
            f"按 {' → '.join(candidate.beat_sequence)} 推进，不把它改回统一六镜头叙事。"
            if candidate.beat_sequence
            else "叙事 Beat 暂无可靠证据，不得编造。"
        ),
        "visual_instruction": "；".join(
            value
            for value in (
                f"承载方式固定为 {candidate.content_carrier}" if candidate.content_carrier else "",
                f"连续性采用 {candidate.continuity_mode}" if candidate.continuity_mode else "",
                f"切镜密度采用 {candidate.cut_density}" if candidate.cut_density else "",
                f"首个视觉钩子采用 {candidate.visual_hook_type}" if candidate.visual_hook_type else "",
            )
            if value
        ),
        "do_not_infer": [f"不得自行补写 {key}" for key in unknown],
    }
    return {
        "contract_schema_version": "structure-direction-contract-v1",
        "direction_identity": {
            "macro_family_key": candidate.macro_family_key,
            "visual_archetype_key": candidate.visual_archetype_key,
            "proof_expression_key": "+".join(candidate.proof_mechanisms) or "UNAVAILABLE",
        },
        "hard_constraints": hard,
        "soft_preferences": {
            "macro_structure_name": candidate.macro_structure_name,
            "structure_description": candidate.structure_description,
            "optional_beats": candidate.optional_beats,
            "variation_axes": candidate.variation_axes,
            "representative_cases": candidate.representative_cases,
        },
        "unknown_constraints": unknown,
        "execution_translation": translation,
        "evidence": {
            "evidence_tier": candidate.evidence_tier,
            "cluster_status": candidate.cluster_status,
            "member_count": candidate.member_count,
            "distinct_videos": candidate.distinct_videos,
            "cohesion": candidate.cohesion,
            "extraction_confidence": candidate.extraction_confidence,
            "profile_types": candidate.profile_types,
            "independence_levels": candidate.independence_levels,
        },
        "provenance": {
            "selection_run_id": selection_run_id,
            "direction_assignment_id": direction_assignment_id,
            "direction_index": direction_index,
            "output_slot": output_slot,
            "direction_role": direction_role,
            "candidate_key": candidate.candidate_key,
            "source_kind": candidate.source_kind,
            "source_run_id": candidate.source_run_id,
            "cluster_id": candidate.cluster_id,
            "cluster_version": candidate.cluster_version,
            "prototype_id": candidate.prototype_id,
            "policy_version": policy_version,
            "data_snapshot_hash": data_snapshot_hash,
        },
    }
