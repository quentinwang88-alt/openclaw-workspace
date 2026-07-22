from __future__ import annotations

import itertools
import math
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from .database import LightTryonDB
from .supplement_shots import asset_shot_roles, segment_shot_roles
from .utils import normalized_list


DIVERSITY_POLICY_VERSION = "narrative-visual-diversity-v1"
CAPACITY_POLICY_VERSION = "narrative-asset-capacity-v1"
DELIVERY_GATE_POLICY_VERSION = "narrative-delivery-gate-v1"
MAX_SHARED_DURATION_RATIO = 0.60
MAX_FIRST_ASSET_REUSE = 2
MAX_DUPLICATE_SEQUENCE_COUNT = 0
MIN_DIVERSITY_SCORE = 70.0

CORE_ROLE_ORDER = (
    "main_wear_upper",
    "fit_turn",
    "scenario_pose",
    "detail_closure",
    "detail_fabric",
    "detail_neckline",
    "detail_waistline",
    "detail_sleeve",
)


def recommended_role_quotas(target_count: int) -> dict[str, int]:
    """Return an automatic material target; no operator fields are required."""

    target = max(1, int(target_count))
    return {
        "main_wear_upper": min(4, max(2, math.ceil(target / 3))),
        "fit_turn": min(3, max(1, math.ceil(target / 4))),
        "scenario_pose": min(3, max(1, math.ceil(target / 4))),
        "detail_closure": min(3, max(1, math.ceil(target / 5))),
        "detail_fabric": min(3, max(1, math.ceil(target / 5))),
        "detail_neckline": min(3, max(1, math.ceil(target / 5))),
        "detail_waistline": min(3, max(1, math.ceil(target / 5))),
        "detail_sleeve": min(3, max(1, math.ceil(target / 5))),
    }


def assess_product_asset_capacity(
    db: LightTryonDB,
    product_id: str,
    *,
    target_count: int | None = None,
    plan_version: str | None = None,
) -> dict[str, Any]:
    variants = db.list_narrative_variants(product_id, plan_version=plan_version)
    target = max(1, int(target_count or len(variants) or 1))
    ready_assets = [
        row
        for row in db.list_media_assets(product_id)
        if row.get("tag_status") == "completed"
        and row.get("asset_status") == "ready"
        and Path(str(row.get("file_path") or "")).is_file()
    ]
    required_role_duration_ms = _required_role_durations(variants)
    role_assets: dict[str, set[str]] = defaultdict(set)
    visual_groups: dict[str, list[str]] = defaultdict(list)
    for asset in ready_assets:
        qc = asset.get("qc_result") if isinstance(asset.get("qc_result"), dict) else {}
        group_id = str(qc.get("duplicate_group_id") or asset.get("asset_id") or "")
        visual_groups[group_id].append(str(asset.get("asset_id") or ""))
        observed = asset.get("observed_tags") if isinstance(asset.get("observed_tags"), dict) else {}
        role_max_duration: dict[str, int] = defaultdict(int)
        for segment in observed.get("segments") or []:
            if not isinstance(segment, dict):
                continue
            duration_ms = max(0, int(segment.get("end_ms") or 0) - int(segment.get("start_ms") or 0))
            for role in segment_shot_roles(segment):
                role_max_duration[role] = max(role_max_duration[role], duration_ms)
        for role in asset_shot_roles(asset):
            minimum = int(required_role_duration_ms.get(role) or 0)
            if int(role_max_duration.get(role) or 0) >= minimum:
                role_assets[role].add(group_id)
    quotas = recommended_role_quotas(target)
    role_counts = {role: len(role_assets.get(role, set())) for role in CORE_ROLE_ORDER}
    deficits = {
        role: max(0, int(quotas[role]) - int(role_counts.get(role, 0)))
        for role in CORE_ROLE_ORDER
    }
    shots = db.list_product_supplement_shots(product_id)
    output_groups: dict[str, list[str]] = defaultdict(list)
    for shot in shots:
        asset_id = str(shot.get("output_asset_id") or "")
        if asset_id:
            output_groups[asset_id].append(str(shot.get("shot_id") or ""))
    duplicate_groups = [
        {"asset_id": asset_id, "shot_ids": shot_ids, "count": len(shot_ids)}
        for asset_id, shot_ids in output_groups.items()
        if len(shot_ids) > 1
    ]
    unique_assets = len(visual_groups)
    near_duplicate_groups = [
        {"duplicate_group_id": group_id, "asset_ids": asset_ids, "count": len(asset_ids)}
        for group_id, asset_ids in visual_groups.items()
        if len(asset_ids) > 1
    ]
    # Five clips form a typical 22-second cut.  Every additional two unique
    # clips usually support one more visibly different cut without forcing a
    # new hook/selling-point restriction.
    stable_capacity = min(target, max(1, math.floor(unique_assets * 2 / 3)))
    coverage = sum(
        min(1.0, role_counts.get(role, 0) / max(1, quota))
        for role, quota in quotas.items()
    ) / max(1, len(quotas))
    stable_capacity = min(stable_capacity, max(1, math.floor(target * max(0.35, coverage))))
    expandable_capacity = min(target, max(stable_capacity, math.floor(unique_assets * 0.9)))
    if stable_capacity >= target and not duplicate_groups and not near_duplicate_groups:
        risk = "low"
    elif stable_capacity >= math.ceil(target * 0.75):
        risk = "medium"
    else:
        risk = "high"
    return {
        "policy_version": CAPACITY_POLICY_VERSION,
        "product_id": product_id,
        "plan_version": plan_version or "",
        "target_count": target,
        "unique_ready_assets": unique_assets,
        "role_counts": role_counts,
        "recommended_role_quotas": quotas,
        "required_role_duration_ms": required_role_duration_ms,
        "role_deficits": deficits,
        "missing_shot_count": sum(deficits.values()),
        "stable_capacity": stable_capacity,
        "expandable_capacity": expandable_capacity,
        "capacity_warning": stable_capacity < target,
        "duplicate_return_groups": duplicate_groups,
        "duplicate_return_count": sum(max(0, row["count"] - 1) for row in duplicate_groups),
        "near_duplicate_groups": near_duplicate_groups,
        "near_duplicate_count": sum(max(0, row["count"] - 1) for row in near_duplicate_groups),
        "coverage_ratio": round(coverage, 4),
        "risk_level": risk,
        "recommendation": (
            "先补充缺口镜头再批量混剪"
            if stable_capacity < target
            else "当前素材可进入多样性混剪"
        ),
    }


def _required_role_durations(variants: list[dict[str, Any]]) -> dict[str, int]:
    """Return the longest real narration interval each evidence role must cover.

    Counting a three-second segment as capacity for a five-second proof beat is
    a false positive: the planner cannot use it without looping or padding the
    same visual.  Use real TTS/beat timings when available.
    """

    required: dict[str, int] = defaultdict(int)
    for variant in variants:
        assembly = variant.get("assembly_plan") if isinstance(variant.get("assembly_plan"), dict) else {}
        intervals = assembly.get("beat_alignment") or []
        if not intervals:
            timeline = variant.get("tts_timeline")
            if isinstance(timeline, dict):
                intervals = timeline.get("placements") or []
            elif isinstance(timeline, list):
                intervals = timeline
        for interval in intervals:
            if not isinstance(interval, dict):
                continue
            duration_ms = max(0, int(interval.get("end_ms") or 0) - int(interval.get("start_ms") or 0))
            for role in normalized_list(interval.get("required_shot_roles")):
                if role in CORE_ROLE_ORDER:
                    required[role] = max(required[role], duration_ms)
    return {role: int(required.get(role) or 0) for role in CORE_ROLE_ORDER}


def product_plan_usage(
    db: LightTryonDB,
    product_id: str,
    *,
    exclude_variant_id: str = "",
) -> dict[str, Any]:
    segment_use: Counter[str] = Counter()
    asset_use: Counter[str] = Counter()
    first_segment_use: Counter[str] = Counter()
    first_asset_use: Counter[str] = Counter()
    sequences: list[tuple[str, ...]] = []
    for variant in db.list_narrative_variants(product_id):
        if str(variant.get("variant_id") or "") == exclude_variant_id:
            continue
        clips = (variant.get("assembly_plan") or {}).get("clips") or []
        if not clips:
            continue
        sequence = tuple(str(row.get("duplicate_group_id") or row.get("asset_id") or "") for row in clips)
        sequences.append(sequence)
        for row in clips:
            segment_use[str(row.get("segment_id") or row.get("asset_id") or "")] += 1
            asset_use[str(row.get("duplicate_group_id") or row.get("asset_id") or "")] += 1
        first = clips[0]
        first_segment_use[str(first.get("segment_id") or first.get("asset_id") or "")] += 1
        first_asset_use[str(first.get("duplicate_group_id") or first.get("asset_id") or "")] += 1
    return {
        "segment_use": segment_use,
        "asset_use": asset_use,
        "first_segment_use": first_segment_use,
        "first_asset_use": first_asset_use,
        "sequences": sequences,
    }


def evaluate_product_diversity(
    db: LightTryonDB,
    product_id: str,
    *,
    plan_version: str | None = None,
    persist: bool = False,
) -> dict[str, Any]:
    variants = db.list_narrative_variants(product_id, plan_version=plan_version)
    planned = [row for row in variants if (row.get("assembly_plan") or {}).get("clips")]
    pairwise: list[dict[str, Any]] = []
    for left, right in itertools.combinations(planned, 2):
        left_clips = (left.get("assembly_plan") or {}).get("clips") or []
        right_clips = (right.get("assembly_plan") or {}).get("clips") or []
        left_map = _asset_duration_map(left_clips)
        right_map = _asset_duration_map(right_clips)
        shared_ms = sum(min(left_map[key], right_map[key]) for key in left_map.keys() & right_map.keys())
        denominator = max(1, min(sum(left_map.values()), sum(right_map.values())))
        left_sequence = tuple(str(row.get("duplicate_group_id") or row.get("asset_id") or "") for row in left_clips)
        right_sequence = tuple(str(row.get("duplicate_group_id") or row.get("asset_id") or "") for row in right_clips)
        pairwise.append({
            "left_variant_id": left["variant_id"],
            "right_variant_id": right["variant_id"],
            "shared_duration_ratio": round(shared_ms / denominator, 4),
            "same_first_asset": bool(left_sequence and right_sequence and left_sequence[0] == right_sequence[0]),
            "same_asset_sequence": left_sequence == right_sequence,
        })
    ratios = [row["shared_duration_ratio"] for row in pairwise]
    first_counts = Counter(
        str(
            ((row.get("assembly_plan") or {}).get("clips") or [{}])[0].get("duplicate_group_id")
            or ((row.get("assembly_plan") or {}).get("clips") or [{}])[0].get("asset_id")
            or ""
        )
        for row in planned
    )
    sequence_counts = Counter(
        tuple(
            str(clip.get("duplicate_group_id") or clip.get("asset_id") or "")
            for clip in (row.get("assembly_plan") or {}).get("clips") or []
        )
        for row in planned
    )
    max_first_reuse = max(first_counts.values(), default=0)
    duplicate_sequences = sum(max(0, count - 1) for count in sequence_counts.values())
    average_ratio = sum(ratios) / len(ratios) if ratios else 0.0
    maximum_ratio = max(ratios, default=0.0)
    first_duplicate_rate = (
        sum(max(0, count - 2) for count in first_counts.values()) / max(1, len(planned))
    )
    sequence_duplicate_rate = duplicate_sequences / max(1, len(planned))
    score = max(0.0, 100.0 - average_ratio * 70 - first_duplicate_rate * 20 - sequence_duplicate_rate * 10)
    warnings = []
    if max_first_reuse > MAX_FIRST_ASSET_REUSE:
        warnings.append("同一首镜在批次中出现超过2次")
    if maximum_ratio > MAX_SHARED_DURATION_RATIO:
        warnings.append("存在两条视频共享画面时长超过60%")
    if duplicate_sequences:
        warnings.append("存在完全相同的素材排列顺序")
    enough_variants = len(planned) >= 2
    asset_rows = {str(row.get("asset_id") or ""): row for row in db.list_media_assets(product_id)}
    contract_assets: set[str] = set()
    usage: dict[str, dict[str, Any]] = defaultdict(lambda: {"videos": set(), "total_ms": 0, "first_uses": 0})
    for variant in planned:
        for index, clip in enumerate((variant.get("assembly_plan") or {}).get("clips") or []):
            asset_id = str(clip.get("asset_id") or "")
            group_id = str(clip.get("duplicate_group_id") or asset_id)
            entry = usage[group_id]
            entry["videos"].add(str(variant.get("variant_id") or ""))
            entry["total_ms"] += int(clip.get("duration_ms") or 0)
            if index == 0:
                entry["first_uses"] += 1
            asset = asset_rows.get(asset_id) or {}
            qc = asset.get("qc_result") if isinstance(asset.get("qc_result"), dict) else {}
            if (
                qc.get("tagging_method") == "supplement_source_contract_fallback"
                and not qc.get("final_visual_qc_passed")
            ):
                contract_assets.add(asset_id)
    if contract_assets:
        warnings.append("存在仅通过生成契约入库、尚未完成最终视觉复核的素材")
    bottlenecks = []
    recommended_roles: list[str] = []
    high_reuse_threshold = max(2, math.ceil(len(planned) * 0.75))
    for group_id, entry in usage.items():
        videos_used = len(entry["videos"])
        first_uses = int(entry["first_uses"])
        if videos_used < high_reuse_threshold and first_uses <= MAX_FIRST_ASSET_REUSE:
            continue
        asset = asset_rows.get(group_id) or {}
        roles = sorted(asset_shot_roles(asset)) if asset else []
        bottlenecks.append({
            "asset_id": group_id,
            "videos_used": videos_used,
            "total_duration_ms": int(entry["total_ms"]),
            "first_shot_uses": first_uses,
            "supported_roles": roles,
        })
        if first_uses > MAX_FIRST_ASSET_REUSE and "main_wear_upper" not in recommended_roles:
            recommended_roles.append("main_wear_upper")
        for role in roles:
            if role in CORE_ROLE_ORDER and role.startswith("detail_") and role not in recommended_roles:
                recommended_roles.append(role)
    gate_reasons = []
    if not enough_variants:
        gate_reasons.append("至少需要2条已规划视频")
    if max_first_reuse > MAX_FIRST_ASSET_REUSE:
        gate_reasons.append("首镜复用超限")
    if maximum_ratio > MAX_SHARED_DURATION_RATIO:
        gate_reasons.append("共享画面比例超限")
    if duplicate_sequences > MAX_DUPLICATE_SEQUENCE_COUNT:
        gate_reasons.append("存在重复镜头序列")
    if enough_variants and score < MIN_DIVERSITY_SCORE:
        gate_reasons.append("多样性得分不足")
    if contract_assets:
        gate_reasons.append("契约回退素材尚未完成最终视觉复核")
    delivery_gate = {
        "policy_version": DELIVERY_GATE_POLICY_VERSION,
        "passed": enough_variants and not gate_reasons,
        "reasons": gate_reasons,
        "thresholds": {
            "maximum_shared_duration_ratio": MAX_SHARED_DURATION_RATIO,
            "max_first_asset_reuse": MAX_FIRST_ASSET_REUSE,
            "max_duplicate_sequence_count": MAX_DUPLICATE_SEQUENCE_COUNT,
            "minimum_diversity_score": MIN_DIVERSITY_SCORE,
        },
    }
    report = {
        "policy_version": DIVERSITY_POLICY_VERSION,
        "product_id": product_id,
        "plan_version": plan_version or "",
        "planned_variant_count": len(planned),
        "average_shared_duration_ratio": round(average_ratio, 4),
        "maximum_shared_duration_ratio": round(maximum_ratio, 4),
        "max_first_asset_reuse": max_first_reuse,
        "duplicate_sequence_count": duplicate_sequences,
        "diversity_score": round(score, 2) if enough_variants else None,
        "status": ("pass" if not warnings else "warning") if enough_variants else "not_evaluated",
        "warnings": warnings if enough_variants else ["至少需要2条已规划视频才能评估批次画面多样性"],
        "contract_fallback_asset_count": len(contract_assets),
        "contract_fallback_asset_ids": sorted(contract_assets),
        "asset_usage_bottlenecks": sorted(
            bottlenecks, key=lambda row: (row["videos_used"], row["first_shot_uses"], row["total_duration_ms"]), reverse=True,
        ),
        "recommended_supplement_roles": recommended_roles,
        "delivery_gate": delivery_gate,
        "pairwise": pairwise,
    }
    if persist:
        for variant in planned:
            assembly = variant.get("assembly_plan") or {}
            own_pairs = [
                row for row in pairwise
                if variant["variant_id"] in {row["left_variant_id"], row["right_variant_id"]}
            ]
            db.update_narrative_variant(
                variant["variant_id"],
                assembly_plan={
                    **assembly,
                    "final_qc": {
                        **(assembly.get("final_qc") if isinstance(assembly.get("final_qc"), dict) else {}),
                        "status": (
                            "ready_for_review" if delivery_gate["passed"]
                            else ("blocked" if enough_variants else "pending")
                        ),
                        "reason": (
                            "batch_delivery_gate_passed" if delivery_gate["passed"]
                            else (";".join(gate_reasons) if enough_variants else "diversity_not_evaluated")
                        ),
                        "delivery_gate": delivery_gate,
                    },
                    "diversity_qc": {
                        **{key: value for key, value in report.items() if key != "pairwise"},
                        "pairwise": own_pairs,
                    },
                },
            )
    return report


def _asset_duration_map(clips: list[dict[str, Any]]) -> dict[str, int]:
    totals: dict[str, int] = defaultdict(int)
    for clip in clips:
        asset_id = str(clip.get("duplicate_group_id") or clip.get("asset_id") or "")
        if asset_id:
            totals[asset_id] += int(clip.get("duration_ms") or 0)
    return dict(totals)
