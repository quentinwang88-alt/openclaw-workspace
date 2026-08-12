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
from core.product_selling_argument_adapter import (
    compatible_structure_carriers,
    normalized_carrier_requirement,
    normalized_proof_subject,
)
from core.outfit_template_provider import (
    get_outfit_template_provider_snapshot,
    without_outfit_display_metadata,
)
from core.persona_template_provider import load_persona_templates
from core.product_type_resolution import normalize_product_type


def _relationship_schedule(requested_count: int, rng: random.Random) -> List[str]:
    """Delegate relationship language to the allocated central hook.

    Kept as a compatibility helper for frozen reports/tests.  The allocator no
    longer injects a batch quota for ``姐妹们``/viewer references; the hook
    archetype and its governed samples decide whether an address is natural.
    """
    del rng
    count = max(0, int(requested_count))
    return ["HOOK_DECIDES"] * count


_HOOK_RELATIONSHIP_PREFERENCES = {
    "AUDIENCE_NEED_CALLOUT": ("VIEWER_REFERENCE", "AUDIENCE_ADDRESS"),
    "PAIN_REFRAME": ("VIEWER_REFERENCE", "PERSONAL_STANCE"),
    "USER_ADVOCACY_STANCE": ("AUDIENCE_ADDRESS", "VIEWER_REFERENCE"),
    "DETAIL_SURPRISE": ("VIEWER_INVITATION", "PERSONAL_STANCE"),
    "DISCOVERY_RESULT_PROMISE": ("PERSONAL_STANCE", "VIEWER_INVITATION"),
    "VISUAL_RESULT_DIRECT": ("NO_ADDRESS", "PERSONAL_STANCE"),
    "GENERAL_PRODUCT_SHARE": ("PERSONAL_STANCE", "VIEWER_REFERENCE"),
}


def _relationship_device_for_hook(
    hook_id: str,
    already_assigned: Sequence[str],
) -> str:
    """Choose one hook-compatible soft relationship surface.

    This is not a copy template or pass/fail quota.  It only prevents a whole
    batch from collapsing into ``HOOK_DECIDES`` or the same personal opener.
    At most one explicit audience address is preferred in each block of five.
    """

    preferences = _HOOK_RELATIONSHIP_PREFERENCES.get(
        _text(hook_id).upper(), ("HOOK_DECIDES",)
    )
    block_size = len(already_assigned) % 5
    current_block = (
        list(already_assigned)[-block_size:] if block_size else []
    )
    last = already_assigned[-1] if already_assigned else ""
    for device in preferences:
        if device == "AUDIENCE_ADDRESS" and "AUDIENCE_ADDRESS" in current_block:
            continue
        if device == last and len(preferences) > 1:
            continue
        return device
    return preferences[0]


def _text(value: Any) -> str:
    return str(value or "").strip()


def _stable_id(prefix: str, material: Any) -> str:
    material = without_outfit_display_metadata(material)
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


_COLOR_TOKEN_GROUPS = {
    "BLACK": ("黑色", "近黑", "纯黑", "black", "สีดำ"),
    "WHITE": ("纯白", "正白", "white", "สีขาว"),
    "LIGHT_NEUTRAL": ("白色", "米白", "奶白", "象牙白", "米色", "杏色", "卡其", "ivory", "cream", "beige", "khaki", "สีครีม", "สีเบจ", "สีกากี"),
    "PINK": ("粉色", "粉红", "pink", "สีชมพู"),
    "GREEN": ("绿色", "抹茶", "green", "matcha", "สีเขียว"),
    "BROWN": ("棕色", "咖色", "褐色", "brown", "สีน้ำตาล"),
    "BLUE": ("蓝色", "牛仔蓝", "blue", "สีน้ำเงิน"),
    "RED": ("红色", "酒红", "red", "สีแดง"),
    "GREY": ("灰色", "灰白", "grey", "gray", "สีเทา"),
}


def _semantic_text(value: Any) -> str:
    if isinstance(value, dict):
        return " ".join(_semantic_text(item) for item in value.values())
    if isinstance(value, (list, tuple, set)):
        return " ".join(_semantic_text(item) for item in value)
    return _text(value)


def _explicit_color_tokens(value: Any) -> set[str]:
    material = _semantic_text(value).lower()
    return {
        color for color, tokens in _COLOR_TOKEN_GROUPS.items()
        if any(token.lower() in material for token in tokens)
    }


def _annotate_variant_fit(bundle: Dict[str, Any], *, anchor_card: Dict[str, Any]) -> Dict[str, Any]:
    """Defer only an explicit, provable variant conflict; unknown stays usable."""

    result = copy.deepcopy(bundle)
    argument = result.get("selling_argument") if isinstance(result.get("selling_argument"), dict) else {}
    argument_colors = _explicit_color_tokens({
        "operator_expression": argument.get("operator_expression"),
        "core_value": argument.get("core_value"),
        "creative_core_value": argument.get("creative_core_value"),
    })
    product_colors = _explicit_color_tokens({
        "product_name": anchor_card.get("product_name"),
        "product_positioning": anchor_card.get("product_positioning_one_liner"),
        "identity_anchors": anchor_card.get("identity_anchors"),
        "hard_anchors": anchor_card.get("hard_anchors"),
        "display_anchors": anchor_card.get("display_anchors"),
    })
    mismatch = bool(argument_colors and product_colors and argument_colors.isdisjoint(product_colors))
    result["variant_fit_status"] = "DEFERRED" if mismatch else "MATCHED"
    result["variant_fit_reason"] = "VARIANT_MISMATCH" if mismatch else "NOT_APPLICABLE"
    result["argument_variant_tokens"] = sorted(argument_colors)
    result["product_variant_tokens"] = sorted(product_colors)
    return result


def _is_creator_wearable_batch(top_category: str, product_type: str) -> bool:
    """Whether person-led creator sharing should be the default production mix."""

    category = _text(top_category).lower()
    product = _text(product_type).lower()
    if "女装" in category or "apparel" in category:
        return True
    hand_first = ("戒指", "手链", "手镯", "发饰", "ring", "bracelet", "hair")
    wearable = (
        "围巾", "帽", "耳", "项链", "包", "鞋", "配饰",
        "scarf", "hat", "ear", "necklace", "bag", "shoe", "accessory",
    )
    return not any(term in product for term in hand_first) and (
        "配饰" in category or any(term in product for term in wearable)
    )


def _is_creator_direction(direction: Dict[str, Any]) -> bool:
    return _direction_carrier(direction) in {
        "WEARER_ACTIVE", "MIXED", "PERSON_ON_CAMERA"
    }


def _creator_weighted_directions(
    directions: List[Dict[str, Any]],
    *,
    requested_count: int,
    top_category: str,
    product_type: str,
) -> List[Dict[str, Any]]:
    """Keep structure diversity while limiting non-person mothers.

    The router still chooses evidence-backed structures.  This projection only
    prevents a five-script wearable batch from spending two or more mother
    slots on static/product-only carriers.  It creates no new structure and
    leaves hand-first accessories untouched.
    """

    if not _is_creator_wearable_batch(top_category, product_type):
        return list(directions)
    creator = [item for item in directions if _is_creator_direction(item)]
    support = [item for item in directions if not _is_creator_direction(item)]
    if not creator:
        return list(directions)
    max_support = 0 if requested_count <= 1 else max(1, (requested_count + 4) // 5)
    resolved = normalize_product_type(product_type, top_category)
    if resolved.canonical_type in {
        "scarf", "winter_scarf", "silk_scarf", "headscarf",
    }:
        # A hand-held observation is usually a more native supplementary
        # direction for a wearable scarf than a mannequin/static display.
        # Keep this as a stable soft sort: static remains available when the
        # router has no compatible hand structure.
        support.sort(
            key=lambda item: {
                "HAND_ONLY": 0,
                "HANDS_ONLY": 0,
                "STATIC_PRODUCT": 1,
            }.get(_direction_carrier(item), 2)
        )
    selected_support_ids = {
        _text(item.get("direction_assignment_id"))
        for item in support[:max_support]
    }
    return [
        item for item in directions
        if _is_creator_direction(item)
        or _text(item.get("direction_assignment_id")) in selected_support_ids
    ]


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
    dependency = normalized_carrier_requirement(argument)
    proof_subject = normalized_proof_subject(argument)
    carrier = _direction_carrier(direction)
    compatible = compatible_structure_carriers(argument)
    proof_carriers = {
        "ON_BODY_RESULT": {"WEARER_ACTIVE", "PERSON_ON_CAMERA", "MIXED"},
        "SCENE_USAGE": {"WEARER_ACTIVE", "PERSON_ON_CAMERA", "MIXED"},
        "PRODUCT_DETAIL": {"WEARER_ACTIVE", "PERSON_ON_CAMERA", "MIXED", "HAND_ONLY", "HANDS_ONLY", "STATIC_PRODUCT"},
        "GENERAL_EXPRESSION": set(),
    }.get(proof_subject, set())
    if not compatible and proof_carriers:
        compatible = sorted(proof_carriers)
    is_mismatch = bool(compatible) and carrier not in set(compatible)
    result["carrier_fit_status"] = "DEFERRED" if is_mismatch else "MATCHED"
    result["carrier_fit_reason"] = (
        "WEARER_VISUAL_REQUIRED"
        if is_mismatch and (
            dependency == "WEARER_REQUIRED"
            or proof_subject in {"ON_BODY_RESULT", "SCENE_USAGE"}
        )
        else f"{dependency}_STRUCTURE_MISMATCH"
        if is_mismatch
        else "NOT_APPLICABLE"
    )
    result["recommended_carriers"] = compatible
    result["proof_subject"] = proof_subject
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
    if (
        _text(bundle.get("hook_tension_authority")).upper()
        == "CENTRAL_CONCEPT"
    ):
        tension_available = True
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
    scene_reference_contexts: Optional[Dict[str, Dict[str, Any]]] = None,
    category_execution_extension: Optional[Dict[str, Any]] = None,
) -> Tuple[List[PlanItem], Dict[str, Any]]:
    """Three-round deterministic allocation returning items and allocation summary."""
    recent = list(recent_creative_usage or [])
    rng = random.Random(random_seed)
    items: List[PlanItem] = []
    reserved_visual_signatures: List[str] = []
    reserved_creative_contracts: List[Dict[str, Any]] = []
    deferred_content: List[Dict[str, Any]] = []
    if scene_reference_contexts is None:
        # Best-effort and read-only.  The adapter returns an empty mapping when
        # disabled, so normal planning stays fully offline by default.
        from core.scene_reference_adapter import load_scene_reference_contexts
        scene_reference_contexts = load_scene_reference_contexts(directions)

    # ── Prepare candidate pools ────────────────────────────────────────
    planning_directions = _creator_weighted_directions(
        directions,
        requested_count=requested_count,
        top_category=top_category,
        product_type=product_type,
    )
    structure_pool: List[Dict[str, Any]] = []
    content_pools: Dict[int, List[Dict[str, Any]]] = {}
    for idx, d in enumerate(planning_directions):
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
            _annotate_carrier_fit(
                _annotate_variant_fit(bundle, anchor_card=anchor_card),
                direction=d,
            )
            for bundle in raw_candidates
        ]
        candidates: List[Dict[str, Any]] = []
        for bundle in all_candidates:
            if bundle.get("variant_fit_status") == "DEFERRED":
                deferred_content.append(
                    {
                        "direction_assignment_id": d.get("direction_assignment_id", ""),
                        "output_slot": d.get("output_slot", ""),
                        "content_bundle_id": bundle.get("content_bundle_id", ""),
                        "content_mode": bundle.get("content_mode", "FACTUAL_OBSERVATION"),
                        "argument_readiness": bundle.get("argument_readiness", "NOT_APPLICABLE"),
                        "downgrade_reason": "VARIANT_MISMATCH",
                        "argument_variant_tokens": bundle.get("argument_variant_tokens", []),
                        "product_variant_tokens": bundle.get("product_variant_tokens", []),
                        "recommended_flow": "MATCHING_PRODUCT_VARIANT",
                    }
                )
                continue
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
    relationship_devices: List[str] = []
    mother_bundle_indices: Dict[int, int] = {}

    # ── Round 1: STRUCTURE_MOTHER ──────────────────────────────────────
    for struct_idx, direction in enumerate(structure_pool):
        eligible_pairs = [
            (bundle_index, bundle)
            for bundle_index, bundle in enumerate(content_pools[struct_idx])
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
        relationship_device = _relationship_device_for_hook(
            hook_id, relationship_devices
        )

        creative = _allocate_creative(
            product_code=product_code, direction=direction,
            bundle=bundle, recent_usage=recent,
            reserved_signatures=reserved_visual_signatures,
            reserved_creatives=reserved_creative_contracts,
            creative_policy_version=creative_policy_version,
            rng=rng,
            scene_reference_context=scene_reference_contexts.get(
                _text(direction.get("direction_assignment_id"))
            ),
            product_type=product_type,
            top_category=top_category,
            category_execution_extension=category_execution_extension,
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
            category_execution_extension=category_execution_extension,
        )
        if item:
            items.append(item)
            reserved_visual_signatures.append(visual_sig)
            reserved_creative_contracts.append(creative)
            used_signatures.add(item.allocation_signature)
            struct_usage[struct_idx] += 1
            angle_usage[angle_key] += 1
            argument_usage[_bundle_argument_key(bundle)] += 1
            hook_usage[hook_id] += 1
            visual_usage[visual_sig] += 1
            mother_bundle_indices[struct_idx] = bundle_index
            relationship_devices.append(relationship_device)

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
    ]
    while pending and len(items) < requested_count:
        # Prefer a selling argument not yet represented in the batch, then the
        # less-used structure. There is no fixed per-argument cap: the finite
        # structure × argument pool and allocation signature still prevent
        # duplicate plans, while least-used-first keeps quotas balanced.
        pending.sort(
            key=lambda entry: (
                (
                    0
                    if (
                        _is_creator_wearable_batch(top_category, product_type)
                        and sum(
                            1
                            for existing in items
                            if _text(existing.carrier_mode).upper()
                            in {"WEARER_ACTIVE", "MIXED", "PERSON_ON_CAMERA"}
                        )
                        < max(1, requested_count - max(1, (requested_count + 4) // 5))
                        and _is_creator_direction(entry[2])
                    )
                    else 1
                ),
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
        relationship_device = _relationship_device_for_hook(
            hook_id, relationship_devices
        )

        creative = _allocate_creative(
            product_code=product_code, direction=direction,
            bundle=bundle, recent_usage=recent,
            reserved_signatures=reserved_visual_signatures,
            reserved_creatives=reserved_creative_contracts,
            creative_policy_version=creative_policy_version,
            rng=rng,
            scene_reference_context=scene_reference_contexts.get(
                _text(direction.get("direction_assignment_id"))
            ),
            product_type=product_type,
            top_category=top_category,
            category_execution_extension=category_execution_extension,
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
            category_execution_extension=category_execution_extension,
        )
        if item:
            items.append(item)
            reserved_visual_signatures.append(visual_sig)
            reserved_creative_contracts.append(creative)
            used_signatures.add(item.allocation_signature)
            struct_usage[struct_idx] += 1
            angle_usage[angle_key] += 1
            argument_usage[_bundle_argument_key(bundle)] += 1
            hook_usage[hook_id] += 1
            visual_usage[visual_sig] += 1
            relationship_devices.append(relationship_device)

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
    reserved_creatives: Optional[List[Dict[str, Any]]],
    creative_policy_version: str,
    rng: random.Random,
    scene_reference_context: Optional[Dict[str, Any]] = None,
    product_type: str = "",
    top_category: str = "",
    category_execution_extension: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Call build_creative_diversity_contract, incorporating batch-reserved patterns."""
    from core.complete_script_v3 import build_creative_diversity_contract

    contract = direction.get("structure_contract", {}) or {}
    hard = contract.get("hard_constraints", {}) or {}
    carrier = hard.get("content_carrier", "WEARER_ACTIVE")

    contract_country = _text(direction.get("country") or "泰国")
    contract_category = _text(direction.get("category") or "女装")

    augmented = [
        *recent_usage,
        *[
            {**contract, "_batch_reserved": True}
            for contract in (reserved_creatives or [])
        ],
    ]
    # Legacy callers may only provide visual signatures. New batch planning
    # carries the full frozen creative contract so outfit and scene histories
    # both participate in same-batch rotation without extra storage writes.
    if not reserved_creatives:
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
    # Scene diversity remains owned by the existing creative allocator, but
    # the already-selected selling argument may softly influence which scene
    # candidate ranks first.  No content authority is created here.
    enriched["content_bundle_brief"] = bundle
    if category_execution_extension:
        enriched["category_execution_extension"] = dict(
            category_execution_extension
        )
    try:
        creative = build_creative_diversity_contract(
            product_code=product_code,
            country=contract_country,
            category=_text(top_category or contract_category),
            product_type=_text(product_type or bundle.get("product_type") or "外套"),
            direction=enriched,
            recent_usage=augmented,
            scene_reference_context=scene_reference_context,
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
    category_execution_extension: Optional[Dict[str, Any]] = None,
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
        "outfit_scene_affinity_contract": creative.get(
            "outfit_scene_affinity_contract", {}
        ),
        "outfit_persona_affinity_contract": creative.get(
            "outfit_persona_affinity_contract", {}
        ),
        "persona_selection_contract": creative.get("persona_selection_contract", {}),
        "scene_reference_contract": creative.get("scene_reference_contract", {}),
        "requested_hook_id": hook_id,
        "content_angle_key": angle_key,
        "selling_argument_id": selling_argument_id,
    }
    from core.category_execution import (
        compile_category_execution_extension,
        resolve_category_argument_execution,
    )

    if category_execution_extension is None:
        category_execution_extension = compile_category_execution_extension(
            product_type=product_type,
            top_category=top_category,
            anchor_card=dict(anchor_card or {}),
        )
    else:
        category_execution_extension = dict(category_execution_extension or {})
    if category_execution_extension:
        category_execution_extension = resolve_category_argument_execution(
            category_execution_extension,
            selling_argument=selling_argument,
        )
    if category_execution_extension:
        frozen_package["category_execution_extension"] = (
            category_execution_extension
        )
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
        category_execution_extension=category_execution_extension,
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
    argument_counts: Counter = Counter()
    capture_counts: Counter = Counter()
    scene_family_counts: Counter = Counter()
    surface_profile_counts: Counter = Counter()
    outfit_silhouette_counts: Counter = Counter()
    outfit_source_counts: Counter = Counter()
    outfit_template_counts: Counter = Counter()
    persona_template_counts: Counter = Counter()
    outfit_persona_affinity_counts: Counter = Counter()
    reference_strategy_counts: Counter = Counter()
    for it in items:
        struct_counts[it.direction_assignment_id] += 1
        try:
            bundle = json.loads(it.content_bundle_json or "{}")
        except (TypeError, json.JSONDecodeError):
            bundle = {}
        argument = (
            bundle.get("selling_argument")
            if isinstance(bundle.get("selling_argument"), dict)
            else {}
        )
        argument_id = _text(argument.get("argument_id"))
        if argument_id:
            argument_counts[argument_id] += 1
        try:
            frozen = json.loads(it.frozen_direction_package_json or "{}")
        except (TypeError, json.JSONDecodeError):
            frozen = {}
        capture_mode = _text(
            frozen.get("simplified_creative_seed", {})
            .get("creative_direction", {})
            .get("capture_mode")
        )
        if capture_mode:
            capture_counts[capture_mode] += 1
        creative = frozen.get("creative_diversity_contract", {})
        if isinstance(creative, dict):
            scene_family = _text(creative.get("scene_family_key"))
            outfit_contract = (
                creative.get("outfit_selection_contract")
                if isinstance(creative.get("outfit_selection_contract"), dict)
                else {}
            )
            surface_key = _text(
                (creative.get("surface_profile") or {}).get("surface_profile_key")
                if isinstance(creative.get("surface_profile"), dict)
                else ""
            )
            silhouette_key = _text(
                outfit_contract.get("silhouette_key") or surface_key
            )
            outfit_source = _text(outfit_contract.get("source_type"))
            outfit_template = _text(outfit_contract.get("template_id"))
            persona_contract = (
                creative.get("persona_selection_contract")
                if isinstance(creative.get("persona_selection_contract"), dict)
                else {}
            )
            outfit_persona_affinity = (
                creative.get("outfit_persona_affinity_contract")
                if isinstance(
                    creative.get("outfit_persona_affinity_contract"), dict
                )
                else {}
            )
            persona_id = _text(persona_contract.get("persona_id"))
            reference_strategy = _text(persona_contract.get("reference_strategy"))
            if scene_family:
                scene_family_counts[scene_family] += 1
            if surface_key and surface_key != "PRODUCT_LED":
                surface_profile_counts[surface_key] += 1
            if silhouette_key and silhouette_key != "PRODUCT_LED":
                outfit_silhouette_counts[silhouette_key] += 1
            if outfit_source:
                outfit_source_counts[outfit_source] += 1
            if outfit_template:
                outfit_template_counts[outfit_template] += 1
            if persona_id:
                persona_template_counts[persona_id] += 1
            if reference_strategy:
                reference_strategy_counts[reference_strategy] += 1
            affinity_status = _text(
                outfit_persona_affinity.get("match_status")
            )
            if affinity_status:
                outfit_persona_affinity_counts[affinity_status] += 1

    requested = int(requested_count) if requested_count is not None else len(items)
    planned = len(items)
    allocation_status = (
        "COMPLETE" if planned >= requested else "PARTIAL_CONTENT_CAPACITY"
    )
    outfit_provider_snapshot = get_outfit_template_provider_snapshot()
    persona_provider_snapshot = load_persona_templates()
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
        "selling_argument_distribution": dict(argument_counts),
        "used_selling_argument_count": len(argument_counts),
        "selling_argument_usage_spread": (
            max(argument_counts.values()) - min(argument_counts.values())
            if argument_counts else 0
        ),
        "capture_mode_distribution": dict(capture_counts),
        "scene_family_distribution": dict(scene_family_counts),
        "surface_profile_distribution": dict(surface_profile_counts),
        "outfit_silhouette_distribution": dict(outfit_silhouette_counts),
        "outfit_source_distribution": dict(outfit_source_counts),
        "outfit_template_distribution": dict(outfit_template_counts),
        "outfit_template_provider_snapshot": outfit_provider_snapshot,
        "persona_template_distribution": dict(persona_template_counts),
        "outfit_persona_affinity_distribution": dict(
            outfit_persona_affinity_counts
        ),
        "reference_strategy_distribution": dict(reference_strategy_counts),
        "persona_template_provider_snapshot": {
            key: persona_provider_snapshot.get(key)
            for key in (
                "provider_version", "status", "enabled_count",
                "approved_asset_count", "soft_warnings",
            )
        },
        "soft_warnings": [
            *list(outfit_provider_snapshot.get("soft_warnings") or []),
            *list(persona_provider_snapshot.get("soft_warnings") or []),
        ],
        "scene_family_diversity_target_met": (
            len(scene_family_counts) >= min(3, planned) if planned else False
        ),
        "surface_profile_diversity_target_met": (
            len(surface_profile_counts) >= min(3, sum(surface_profile_counts.values()))
            if surface_profile_counts else False
        ),
        "outfit_silhouette_target_met": (
            len(outfit_silhouette_counts)
            >= min(3, sum(outfit_silhouette_counts.values()))
            if outfit_silhouette_counts else False
        ),
        "families": families,
        "carriers": carriers,
        "hooks": hooks,
        "angles": angles,
    }
