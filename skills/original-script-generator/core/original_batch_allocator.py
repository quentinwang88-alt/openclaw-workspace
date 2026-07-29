"""V1 原创批次分配器 — 内容候选 + 三轮分配算法"""
from __future__ import annotations

import copy
import hashlib
import json
import random
from collections import Counter
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from core.original_batch_models import (
    PlanItem,
    build_allocation_signature,
    generate_batch_item_id,
    ITEM_ROLES,
    HOOK_ID_BLACKLIST_FOR_NO_TENSION,
    CONTENT_ANGLE_KEYS,
)
from core.original_batch_storage import POLICY_VERSION as MODEL_POLICY_VERSION
from core.reality_reference import build_content_bundle_brief


# This is deliberately a small surface rotation, not another creative layer.
# The hook still determines whether a relational opening is appropriate; the
# batch only prevents every item from collapsing to the same first-person
# entry point.
_RELATIONSHIP_COMPATIBILITY = {
    "AUDIENCE_NEED_CALLOUT": {"AUDIENCE_ADDRESS", "VIEWER_REFERENCE", "VIEWER_INVITATION"},
    "PAIN_REFRAME": {"AUDIENCE_ADDRESS", "VIEWER_REFERENCE", "VIEWER_INVITATION"},
    "USER_ADVOCACY_STANCE": {"AUDIENCE_ADDRESS", "PERSONAL_STANCE"},
    "VISUAL_RESULT_DIRECT": {"AUDIENCE_ADDRESS", "VIEWER_INVITATION", "PERSONAL_STANCE"},
    "DISCOVERY_RESULT_PROMISE": {"AUDIENCE_ADDRESS", "VIEWER_INVITATION", "PERSONAL_STANCE"},
    "GENERAL_PRODUCT_SHARE": {"AUDIENCE_ADDRESS", "PERSONAL_STANCE"},
    "DETAIL_SURPRISE": {"VIEWER_INVITATION", "NO_ADDRESS"},
}

_RELATIONSHIP_DEFAULTS = {
    "AUDIENCE_NEED_CALLOUT": "VIEWER_REFERENCE",
    "PAIN_REFRAME": "VIEWER_REFERENCE",
    "USER_ADVOCACY_STANCE": "PERSONAL_STANCE",
    "VISUAL_RESULT_DIRECT": "VIEWER_INVITATION",
    "DISCOVERY_RESULT_PROMISE": "PERSONAL_STANCE",
    "GENERAL_PRODUCT_SHARE": "PERSONAL_STANCE",
    "DETAIL_SURPRISE": "VIEWER_INVITATION",
}


def _relationship_schedule(requested_count: int, rng: random.Random) -> List[str]:
    """Return a deterministic, shuffled soft surface mix for a batch.

    Count 1 delegates to the hook.  Small batches carry one direct audience
    address; a 10-item batch targets three addresses and two viewer references.
    The schedule is advisory: an incompatible hook falls back naturally.
    """
    count = max(0, int(requested_count))
    if count <= 1:
        return ["HOOK_DECIDES"] * count
    address_count = 1 if count <= 4 else min(3, max(1, round(count * 0.3)))
    reference_count = 0 if count == 2 else (1 if count <= 7 else 2)
    reference_count = min(reference_count, max(0, count - address_count))
    schedule = (
        ["AUDIENCE_ADDRESS"] * address_count
        + ["VIEWER_REFERENCE"] * reference_count
        + ["HOOK_DECIDES"] * (count - address_count - reference_count)
    )
    rng.shuffle(schedule)
    return schedule


def _relationship_device_for_item(hook_id: str, scheduled_device: str) -> str:
    allowed = _RELATIONSHIP_COMPATIBILITY.get(_text(hook_id), set())
    if scheduled_device in allowed:
        return scheduled_device
    default = _RELATIONSHIP_DEFAULTS.get(_text(hook_id), "HOOK_DECIDES")
    return default if default in allowed else "HOOK_DECIDES"


def _text(value: Any) -> str:
    return str(value or "").strip()


def _stable_id(prefix: str, material: Any) -> str:
    raw = json.dumps(material, ensure_ascii=False, sort_keys=True, default=str)
    return prefix + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20].upper()


def _direction_carrier(direction: Dict[str, Any]) -> str:
    contract = (
        direction.get("structure_contract")
        if isinstance(direction.get("structure_contract"), dict)
        else {}
    )
    hard = contract.get("hard_constraints") if isinstance(contract.get("hard_constraints"), dict) else {}
    return _text(hard.get("content_carrier")).upper()


def _annotate_carrier_fit(
    bundle: Dict[str, Any],
    *,
    direction: Dict[str, Any],
) -> Dict[str, Any]:
    """Attach a planning-only carrier fit without re-evaluating the value.

    `WEARER_REQUIRED` is only emitted by the central claim adapter for
    visual-result claims.  A mismatch defers the pairing before model calls;
    it does not change claim authorisation, proof readiness or voiceover copy.
    Legacy and benefit bundles remain flexible by default.
    """

    result = copy.deepcopy(bundle)
    argument = result.get("selling_argument") if isinstance(result.get("selling_argument"), dict) else {}
    dependency = _text(argument.get("visual_dependency")).upper() or "FLEXIBLE"
    carrier = _direction_carrier(direction)
    is_mismatch = (
        dependency == "WEARER_REQUIRED"
        and carrier not in {"WEARER_ACTIVE", "MIXED"}
    )
    result["carrier_fit_status"] = "DEFERRED" if is_mismatch else "MATCHED"
    result["carrier_fit_reason"] = (
        "WEARER_VISUAL_REQUIRED" if is_mismatch else "NOT_APPLICABLE"
    )
    result["recommended_carriers"] = (
        ["WEARER_ACTIVE", "MIXED"] if dependency == "WEARER_REQUIRED" else []
    )
    return result


def _bundle_argument_key(bundle: Dict[str, Any]) -> str:
    argument = (
        bundle.get("selling_argument")
        if isinstance(bundle.get("selling_argument"), dict)
        else {}
    )
    argument_id = _text(argument.get("argument_id"))
    if argument_id:
        return argument_id
    return _text(bundle.get("content_angle_key"))


# ── Content bundle candidates ──────────────────────────────────────────


def build_content_bundle_candidates(
    anchor_card: Dict[str, Any],
    execution_reference: Dict[str, Any],
    *,
    product_type: str = "",
    selling_point_catalog: Optional[Iterable[Dict[str, Any]]] = None,
    product_selling_note: str = "",
    max_candidates: int = 3,
) -> List[Dict[str, Any]]:
    """Generate up to max_candidates distinct content directions.

    Value arguments are diversified before hooks, structures or visual facts.
    Facts can repeat as proof across different arguments; rotating "five
    buttons" into the thesis is deliberately not a diversity mechanism.
    """
    # First bundle: the existing logic
    primary = build_content_bundle_brief(
        anchor_card,
        execution_reference,
        product_type=product_type,
        selling_point_catalog=selling_point_catalog,
        product_selling_note=product_selling_note,
    )
    primary_argument = primary.get("selling_argument") if isinstance(primary.get("selling_argument"), dict) else {}
    primary_argument_id = _text(primary_argument.get("argument_id"))
    primary["content_angle_key"] = (
        f"ARGUMENT_{primary_argument_id}"
        if _text(primary.get("content_mode")) == "SELLING_ARGUMENT" and primary_argument_id
        else "FACTUAL_OBSERVATION"
    )
    candidates: List[Dict[str, Any]] = [primary]
    catalog = list(selling_point_catalog or [])

    # Existing final strategies are valid legacy selling arguments.  Central
    # catalog rows must explicitly say SELLING_ARGUMENT; its feature rows are
    # evidence only and never become an angle on their own.
    value_angles: List[Dict[str, Any]] = []
    for sp in catalog:
        if isinstance(sp, dict):
            kind = _text(sp.get("argument_kind")).upper()
            is_legacy_value = not kind and bool(_text(sp.get("primary_selling_point") or sp.get("selling_point")))
            if kind == "SELLING_ARGUMENT" or is_legacy_value:
                value_angles.append(sp)

    seen_argument_ids = {primary_argument_id} if primary_argument_id else set()
    for value_angle in value_angles:
        if len(candidates) >= max_candidates:
            break
        variant = build_content_bundle_brief(
            anchor_card,
            execution_reference,
            product_type=product_type,
            selling_point_catalog=[value_angle],
            product_selling_note=product_selling_note,
        )
        argument = variant.get("selling_argument") if isinstance(variant.get("selling_argument"), dict) else {}
        argument_id = _text(argument.get("argument_id"))
        if _text(variant.get("content_mode")) != "SELLING_ARGUMENT" or not argument_id:
            continue
        if argument_id in seen_argument_ids:
            continue
        seen_argument_ids.add(argument_id)
        variant["content_angle_key"] = f"ARGUMENT_{argument_id}"
        candidates.append(variant)

    # Keep at most one factual observation for traceability and later routing.
    # It must not be cloned under new allocation identities merely to fill an
    # original-video quantity request.

    return candidates


def _build_variant_bundle(
    primary: Dict[str, Any],
    anchor_card: Dict[str, Any],
    reference: Dict[str, Any],
    *,
    angle_key: str,
    tension_text: str = "",
    tension_status: str = "UNAVAILABLE",
    rotate_facts: bool = False,
    used_facts: set = None,
    used_groups: set = None,
) -> Optional[Dict[str, Any]]:
    """Clone the primary bundle with a different angle key."""
    variant = copy.deepcopy(primary)
    variant["content_angle_key"] = angle_key
    variant["audience_tension_text"] = tension_text or primary.get("audience_tension_text", "")
    variant["audience_tension_status"] = tension_status

    if rotate_facts and (used_facts or used_groups):
        atoms = variant.get("claim_atoms", [])
        primary_first = next((a for a in atoms if a.get("role") == "core_result"), None)
        others = [a for a in atoms if a.get("role") != "core_result"]
        if primary_first and others:
            # Rotate: second atom becomes core, first goes to back
            new_core = others[0]
            new_core["role"] = "core_result"
            primary_first["role"] = "supporting_value"
            reordered = [new_core] + [a for a in others if a != new_core] + [primary_first]
            variant["claim_atoms"] = reordered
            variant["content_mainline"] = _text(new_core.get("fact_text"))
            variant["content_bundle_id"] = _stable_id("CBR_VAR_", {
                "angle": angle_key,
                "mainline": variant["content_mainline"],
                "atoms": [a.get("claim_key") for a in reordered],
            })
    return variant


# ── Hook eligibility ───────────────────────────────────────────────────


def _eligible_hooks_for_bundle(
    bundle: Dict[str, Any],
    active_hook_ids: List[str],
) -> Tuple[List[str], List[str]]:
    """Return (eligible_hook_ids, suppressed_for_later_tracing)."""
    eligible = list(
        bundle.get("preferred_hook_angles")
        or bundle.get("eligible_hook_ids")
        or []
    )
    if not eligible:
        eligible = list(active_hook_ids[:3])  # fallback
    tension = (
        bundle.get("audience_tension")
        if isinstance(bundle.get("audience_tension"), dict)
        else {}
    )
    tension_status = _text(
        tension.get("status") or bundle.get("audience_tension_status")
    ).upper()
    tension_available = tension_status == "AVAILABLE" and bool(
        _text(tension.get("text") or bundle.get("audience_tension_text"))
    )
    # Legacy tests and frozen packages may only carry the top-level status.
    if not tension and tension_status == "AVAILABLE":
        tension_available = True
    suppressed = []
    result = []
    for hid in eligible:
        hid = _text(hid)
        if not hid:
            continue
        if not tension_available and hid in HOOK_ID_BLACKLIST_FOR_NO_TENSION:
            suppressed.append(hid)
            continue
        if hid in active_hook_ids:
            result.append(hid)
    if not result and active_hook_ids:
        result = [h for h in active_hook_ids if h not in HOOK_ID_BLACKLIST_FOR_NO_TENSION][:2]
    return result, suppressed


# ── Three-round allocation ─────────────────────────────────────────────


def allocate_batch_items(
    *,
    product_code: str,
    requested_count: int,
    directions: List[Dict[str, Any]],
    anchor_card: Dict[str, Any],
    active_hook_ids: List[str],
    creative_policy_version: str,
    random_seed: int,
    recent_creative_usage: Optional[List[Dict[str, Any]]] = None,
    selling_point_catalog: Optional[Iterable[Dict[str, Any]]] = None,
    product_selling_note: str = "",
    product_type: str = "",
    top_category: str = "",
) -> Tuple[List[PlanItem], Dict[str, Any]]:
    """Three-round deterministic allocation returning items and allocation summary."""
    recent = list(recent_creative_usage or [])
    rng = random.Random(random_seed)
    items: List[PlanItem] = []
    reserved_visual_signatures: List[str] = []
    deferred_content: List[Dict[str, Any]] = []

    # ── Prepare candidate pools ────────────────────────────────────────
    structure_pool: List[Dict[str, Any]] = []
    content_pools: Dict[int, List[Dict[str, Any]]] = {}
    for idx, d in enumerate(directions):
        ref = d.get("execution_reference", {}) or {}
        # The source reference can have a different carrier from the routed
        # production direction.  Candidate selling arguments must respect the
        # route contract (especially S4 static) rather than inherit the
        # reference video's person-led carrier.
        bundle_reference = dict(ref)
        hard = (
            d.get("structure_contract", {}).get("hard_constraints", {})
            if isinstance(d.get("structure_contract"), dict)
            else {}
        )
        bundle_reference["content_carrier"] = _text(
            hard.get("content_carrier") or ref.get("content_carrier")
        )
        raw_candidates = build_content_bundle_candidates(
            anchor_card, bundle_reference,
            product_type=product_type,
            selling_point_catalog=selling_point_catalog,
            product_selling_note=product_selling_note,
            # A test batch should see the available selling-point breadth.
            # The previous fixed limit of three candidates per carrier made a
            # six-point product repeat two arguments before reaching the rest.
            max_candidates=max(3, requested_count),
        )
        all_candidates = [
            _annotate_carrier_fit(bundle, direction=d)
            for bundle in raw_candidates
        ]
        candidates: List[Dict[str, Any]] = []
        for bundle in all_candidates:
            if bundle.get("carrier_fit_status") == "DEFERRED":
                deferred_content.append(
                    {
                        "direction_assignment_id": d.get("direction_assignment_id", ""),
                        "output_slot": d.get("output_slot", ""),
                        "content_bundle_id": bundle.get("content_bundle_id", ""),
                        "content_mode": bundle.get("content_mode", "FACTUAL_OBSERVATION"),
                        "argument_readiness": bundle.get("argument_readiness", "NOT_APPLICABLE"),
                        "downgrade_reason": bundle.get("carrier_fit_reason", "WEARER_VISUAL_REQUIRED"),
                        "recommended_carriers": bundle.get("recommended_carriers", []),
                        "recommended_flow": "WEARER_OR_MIXED_STRUCTURE",
                    }
                )
                continue
            if bundle.get("original_15s_eligible") is True:
                candidates.append(bundle)
                continue
            deferred_content.append(
                {
                    "direction_assignment_id": d.get("direction_assignment_id", ""),
                    "output_slot": d.get("output_slot", ""),
                    "content_bundle_id": bundle.get("content_bundle_id", ""),
                    "content_mode": bundle.get("content_mode", "FACTUAL_OBSERVATION"),
                    "argument_readiness": bundle.get("argument_readiness", "NOT_APPLICABLE"),
                    "downgrade_reason": bundle.get("downgrade_reason", ""),
                    "recommended_flow": bundle.get("recommended_flow", "LIGHT_VIDEO_OR_MIXCUT"),
                }
            )
        content_pools[idx] = candidates
        structure_pool.append(d)

    # Usage counters
    struct_usage: Counter = Counter()
    angle_usage: Counter = Counter()
    argument_usage: Counter = Counter()
    hook_usage: Counter = Counter()
    visual_usage: Counter = Counter()
    used_signatures: set = set()
    relationship_schedule = _relationship_schedule(requested_count, rng)
    mother_bundle_indices: Dict[int, int] = {}

    # ── Round 1: STRUCTURE_MOTHER ──────────────────────────────────────
    for struct_idx, direction in enumerate(structure_pool):
        eligible_pairs = [
            (bundle_index, bundle)
            for bundle_index, bundle in enumerate(content_pools[struct_idx])
            if argument_usage[_bundle_argument_key(bundle)] < 2
        ]
        if not eligible_pairs:
            continue
        bundle_index, bundle = min(
            eligible_pairs,
            key=lambda pair: (
                angle_usage[_text(pair[1].get("content_angle_key", "FACT_DISCOVERY"))],
                pair[0],
            ),
        )
        angle_key = _text(bundle.get("content_angle_key", "FACT_DISCOVERY"))
        eligible_hooks, _ = _eligible_hooks_for_bundle(bundle, active_hook_ids)
        if not eligible_hooks:
            continue
        hook_id = _pick_least_used(eligible_hooks, hook_usage, rng)
        relationship_device = _relationship_device_for_item(
            hook_id,
            relationship_schedule[len(items)] if len(items) < len(relationship_schedule) else "HOOK_DECIDES",
        )

        creative = _allocate_creative(
            product_code=product_code, direction=direction,
            bundle=bundle, recent_usage=recent,
            reserved_signatures=reserved_visual_signatures,
            creative_policy_version=creative_policy_version,
            rng=rng,
        )
        if not creative:
            continue
        visual_sig = creative.get("visual_signature", "")

        item = _make_item(
            product_code=product_code, batch_id="", item_index=len(items) + 1,
            item_role="STRUCTURE_MOTHER", direction=direction,
            bundle=bundle, angle_key=angle_key, hook_id=hook_id,
            eligible_hooks=eligible_hooks, creative=creative,
            visual_signature=visual_sig, policy_version=MODEL_POLICY_VERSION,
            used_signatures=used_signatures,
            anchor_card=anchor_card, product_type=product_type,
            top_category=top_category,
            relationship_device=relationship_device,
        )
        if item:
            items.append(item)
            reserved_visual_signatures.append(visual_sig)
            used_signatures.add(item.allocation_signature)
            struct_usage[struct_idx] += 1
            angle_usage[angle_key] += 1
            argument_usage[_bundle_argument_key(bundle)] += 1
            hook_usage[hook_id] += 1
            visual_usage[visual_sig] += 1
            mother_bundle_indices[struct_idx] = bundle_index

    if len(items) >= requested_count:
        return items, _build_summary(
            items, structure_pool, "ROUND1_COMPLETE",
            requested_count=requested_count,
            deferred_content=deferred_content,
        )

    # ── Round 2: CONTENT_VARIANT ───────────────────────────────────────
    pending = [
        (struct_idx, bundle_index, direction, bundle)
        for struct_idx, direction in enumerate(structure_pool)
        for bundle_index, bundle in enumerate(content_pools[struct_idx])
        if bundle_index != mother_bundle_indices.get(struct_idx)
        and argument_usage[_bundle_argument_key(bundle)] < 2
    ]
    while pending and len(items) < requested_count:
        pending = [
            entry
            for entry in pending
            if argument_usage[_bundle_argument_key(entry[3])] < 2
        ]
        if not pending:
            break
        # Prefer a selling argument not yet represented in the batch, then the
        # less-used structure. This is breadth-first allocation, not a new
        # creative rule: all candidates were already authorised and frozen.
        pending.sort(
            key=lambda entry: (
                angle_usage[_text(entry[3].get("content_angle_key", "FACT_DISCOVERY"))],
                struct_usage[entry[0]],
                entry[1],
                entry[0],
            )
        )
        struct_idx, _, direction, bundle = pending.pop(0)
        angle_key = _text(bundle.get("content_angle_key", "FACT_DISCOVERY"))
        eligible_hooks, _ = _eligible_hooks_for_bundle(bundle, active_hook_ids)
        if not eligible_hooks:
            continue
        hook_id = _pick_least_used(eligible_hooks, hook_usage, rng)
        relationship_device = _relationship_device_for_item(
            hook_id,
            relationship_schedule[len(items)] if len(items) < len(relationship_schedule) else "HOOK_DECIDES",
        )

        creative = _allocate_creative(
            product_code=product_code, direction=direction,
            bundle=bundle, recent_usage=recent,
            reserved_signatures=reserved_visual_signatures,
            creative_policy_version=creative_policy_version,
            rng=rng,
        )
        if not creative:
            continue
        visual_sig = creative.get("visual_signature", "")

        item = _make_item(
            product_code=product_code, batch_id="", item_index=len(items) + 1,
            item_role="CONTENT_VARIANT", direction=direction,
            bundle=bundle, angle_key=angle_key, hook_id=hook_id,
            eligible_hooks=eligible_hooks, creative=creative,
            visual_signature=visual_sig, policy_version=MODEL_POLICY_VERSION,
            used_signatures=used_signatures,
            anchor_card=anchor_card, product_type=product_type,
            top_category=top_category,
            relationship_device=relationship_device,
        )
        if item:
            items.append(item)
            reserved_visual_signatures.append(visual_sig)
            used_signatures.add(item.allocation_signature)
            struct_usage[struct_idx] += 1
            angle_usage[angle_key] += 1
            argument_usage[_bundle_argument_key(bundle)] += 1
            hook_usage[hook_id] += 1
            visual_usage[visual_sig] += 1

    if len(items) >= requested_count:
        return items, _build_summary(
            items, structure_pool, "ROUND2_COMPLETE",
            requested_count=requested_count,
            deferred_content=deferred_content,
        )

    return items[:requested_count], _build_summary(
        items, structure_pool, "PARTIAL_CONTENT_CAPACITY",
        requested_count=requested_count,
        deferred_content=deferred_content,
    )


# ── Helpers ────────────────────────────────────────────────────────────


def _pick_least_used(candidates: List[str], usage: Counter, rng: random.Random) -> str:
    min_use = min(usage.get(c, 0) for c in candidates)
    least = [c for c in candidates if usage.get(c, 0) == min_use]
    rng.shuffle(least)
    return least[0]


def _allocate_creative(
    *,
    product_code: str,
    direction: Dict[str, Any],
    bundle: Dict[str, Any],
    recent_usage: List[Dict[str, Any]],
    reserved_signatures: List[str],
    creative_policy_version: str,
    rng: random.Random,
) -> Optional[Dict[str, Any]]:
    """Call build_creative_diversity_contract, incorporating batch-reserved patterns."""
    from core.complete_script_v3 import build_creative_diversity_contract

    contract = direction.get("structure_contract", {}) or {}
    hard = contract.get("hard_constraints", {}) or {}
    carrier = hard.get("content_carrier", "WEARER_ACTIVE")

    contract_country = _text(direction.get("country") or "泰国")
    contract_category = _text(direction.get("category") or "女装")

    augmented = list(recent_usage)
    for sig in reserved_signatures:
        if sig:
            augmented.append({
                "product_code": product_code,
                "status": "RESERVED",
                "visual_signature": sig,
                "scene_motif": sig.split("|")[1] if "|" in sig else "",
                "opening_action": sig.split("|")[2] if "|" in sig else "",
                "persona_role": sig.split("|")[0] if "|" in sig else "",
                "direction_id": direction.get("direction_assignment_id", ""),
            })

    # Add carrier-aware creative direction
    enriched = dict(direction)
    enriched["_carrier_hint"] = carrier

    try:
        creative = build_creative_diversity_contract(
            product_code=product_code,
            country=contract_country,
            category=contract_category,
            product_type=_text(bundle.get("product_type") or "外套"),
            direction=enriched,
            recent_usage=augmented,
        )
        return creative
    except Exception:
        return None


def _make_item(
    *,
    product_code: str,
    batch_id: str,
    item_index: int,
    item_role: str,
    direction: Dict[str, Any],
    bundle: Dict[str, Any],
    angle_key: str,
    hook_id: str,
    eligible_hooks: List[str],
    creative: Dict[str, Any],
    visual_signature: str,
    policy_version: str,
    used_signatures: set,
    anchor_card: Optional[Dict[str, Any]] = None,
    product_type: str = "",
    top_category: str = "",
    relationship_device: str = "HOOK_DECIDES",
) -> Optional[PlanItem]:
    da_id = direction.get("direction_assignment_id", "")
    atoms = bundle.get("claim_atoms", [])
    claim_keys = [_text(a.get("claim_key")) for a in atoms if _text(a.get("claim_key"))]

    selling_argument = bundle.get("selling_argument") if isinstance(bundle.get("selling_argument"), dict) else {}
    selling_argument_id = _text(selling_argument.get("argument_id"))
    sig = build_allocation_signature(
        da_id, angle_key, claim_keys, hook_id, visual_signature, selling_argument_id,
    )
    if sig in used_signatures:
        return None

    contract = direction.get("structure_contract", {}) or {}
    hard = contract.get("hard_constraints", {}) or {}
    evidence = contract.get("evidence", {}) or {}
    frozen_package = {
        "schema_version": "original-frozen-direction-package-v1",
        "output_slot": direction.get("output_slot", f"S{int(item_index)}"),
        "direction_assignment_id": da_id,
        "selection_run_id": direction.get("selection_run_id", ""),
        "cluster_id": direction.get("cluster_id"),
        "cluster_version": direction.get("cluster_version", ""),
        "evidence_tier": evidence.get(
            "evidence_tier", direction.get("evidence_tier", "")
        ),
        "structure_contract": contract,
        "structure_execution_plan": direction.get("structure_execution_plan", {}),
        "execution_reference": direction.get("execution_reference", {}),
        "structure_source_mode": direction.get(
            "structure_source_mode", "VIDEO_REFERENCED"
        ),
        "content_bundle_brief": bundle,
        "p2_lite": direction.get("p2_lite", {}),
        "creative_diversity_contract": creative,
        "requested_hook_id": hook_id,
        "content_angle_key": angle_key,
        "selling_argument_id": selling_argument_id,
    }
    # The simplified path consumes the same frozen plan without changing the
    # allocator or adding another database.  It is a compact input contract,
    # not a second creative decision layer.
    from core.simplified_complete_script import build_simplified_creative_seed
    frozen_package["simplified_creative_seed"] = build_simplified_creative_seed(
        anchor_card=dict(anchor_card or {}),
        structure_contract=contract,
        content_bundle=bundle,
        creative_contract=creative,
        execution_reference=direction.get("execution_reference", {}) or {},
        requested_hook_id=hook_id,
        content_angle_key=angle_key,
        relationship_device=relationship_device,
        product_type=product_type,
        top_category=top_category,
    )

    item_snapshot = {
        "item_index": item_index,
        "item_role": item_role,
        "allocation_signature": sig,
        "direction_assignment_id": da_id,
        "content_angle_key": angle_key,
        "claim_keys": claim_keys,
        "requested_hook_id": hook_id,
        "visual_signature": visual_signature,
        "frozen_direction_package": frozen_package,
    }
    snapshot_hash = _stable_id("SN_", item_snapshot)

    return PlanItem(
        batch_item_id=generate_batch_item_id(batch_id or "PLANNING", item_index, sig),
        batch_id=batch_id or "PLANNING",
        item_index=item_index,
        item_role=item_role,
        product_code=product_code,
        selection_run_id=direction.get("selection_run_id", ""),
        direction_assignment_id=da_id,
        compatibility_slot=direction.get("output_slot", f"S{int(item_index)}"),
        structure_contract_json=json.dumps(contract, ensure_ascii=False, sort_keys=True, default=str),
        allocation_signature=sig,
        policy_version=policy_version,
        item_snapshot_hash=snapshot_hash,
        content_bundle_id=bundle.get("content_bundle_id", _stable_id("CBR_", bundle)),
        content_bundle_json=json.dumps(bundle, ensure_ascii=False, sort_keys=True, default=str),
        content_angle_key=angle_key,
        audience_tension_status=bundle.get("audience_tension_status", "UNAVAILABLE"),
        claim_keys_json=json.dumps(claim_keys, ensure_ascii=False),
        requested_hook_id=hook_id,
        eligible_hook_ids_json=json.dumps(eligible_hooks, ensure_ascii=False),
        cluster_id=direction.get("cluster_id"),
        cluster_version=direction.get("cluster_version", ""),
        evidence_tier=evidence.get("evidence_tier", direction.get("evidence_tier", "")),
        macro_family_key=contract.get("direction_identity", {}).get("macro_family_key", hard.get("macro_family_key", "")),
        carrier_mode=hard.get("content_carrier", ""),
        execution_reference_json=json.dumps(direction.get("execution_reference", {}), ensure_ascii=False, default=str),
        audience_tension_text=bundle.get("audience_tension_text", ""),
        creative_contract_id=creative.get("contract_id", ""),
        visual_signature=visual_signature,
        frozen_direction_package_json=json.dumps(
            frozen_package, ensure_ascii=False, sort_keys=True, default=str
        ),
        status="PLANNED",
    )


def _build_summary(
    items: List[PlanItem],
    structures: List[Dict[str, Any]],
    round_label: str,
    *,
    requested_count: Optional[int] = None,
    deferred_content: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    families = list(set(it.macro_family_key for it in items if it.macro_family_key))
    carriers = list(set(it.carrier_mode for it in items if it.carrier_mode))
    hooks = list(set(it.requested_hook_id for it in items if it.requested_hook_id))
    angles = list(set(it.content_angle_key for it in items if it.content_angle_key))

    struct_counts: Counter = Counter()
    for it in items:
        struct_counts[it.direction_assignment_id] += 1

    requested = int(requested_count) if requested_count is not None else len(items)
    planned = len(items)
    allocation_status = (
        "COMPLETE" if planned >= requested else "PARTIAL_CONTENT_CAPACITY"
    )
    return {
        "policy_version": MODEL_POLICY_VERSION,
        "allocation_round": round_label,
        "allocation_status": allocation_status,
        "requested_count": requested,
        "planned_count": planned,
        "shortage_count": max(0, requested - planned),
        "deferred_content": list(deferred_content or []),
        "structure_count": len(structures),
        "unique_families": len(families),
        "unique_carriers": len(carriers),
        "unique_hooks": len(hooks),
        "unique_angles": len(angles),
        "structure_distribution": dict(struct_counts),
        "families": families,
        "carriers": carriers,
        "hooks": hooks,
        "angles": angles,
    }
