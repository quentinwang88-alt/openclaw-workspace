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
from core.semantic_spine import (
    build_context_bridge,
    build_product_market_context,
    build_script_semantic_spine,
    semantic_spine_enabled,
)


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


_HOOK_FAMILY_BY_ID = {
    "PAIN_REFRAME": "NEED_TENSION",
    "AUDIENCE_NEED_CALLOUT": "NEED_TENSION",
    "DISCOVERY_RESULT_PROMISE": "DISCOVERY_DETAIL",
    "DETAIL_SURPRISE": "DISCOVERY_DETAIL",
    "NOVELTY_NEW_ARRIVAL": "DISCOVERY_DETAIL",
    "VISUAL_RESULT_DIRECT": "RESULT_COMPARE",
    "BINARY_COMPARISON": "RESULT_COMPARE",
    "USER_ADVOCACY_STANCE": "CREATOR_RELATION",
    "GENERAL_PRODUCT_SHARE": "CREATOR_RELATION",
    "SOCIAL_VALIDATION": "SOCIAL_PROOF",
    "PARTICIPATION_CHOICE": "PARTICIPATION",
    "LIVE_SCARCITY": "SCARCITY",
}


def _hook_family(hook_id: str) -> str:
    value = _text(hook_id).upper()
    return _HOOK_FAMILY_BY_ID.get(value, value or "UNAVAILABLE")


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
    argument_text = _semantic_text({
        "operator_expression": argument.get("operator_expression"),
        "core_value": argument.get("core_value"),
        "creative_core_value": argument.get("creative_core_value"),
    }).lower()
    excluded_current_color = any(
        phrase in argument_text
        for phrase in (
            "不只有黑白", "不止黑白", "区别于黑白", "不是黑白",
            "不只是纯黑白", "不只有纯黑白", "不止纯黑白", "区别于纯黑白",
            "不是纯黑白", "不局限于黑白", "不局限于纯黑白",
            "not just black and white", "not only black and white",
            "ไม่ใช่แค่สีดำและสีขาว", "ไม่ใช่แค่ดำขาว",
        )
    ) and bool(product_colors & {"BLACK", "WHITE"})
    mismatch = bool(
        (argument_colors and product_colors and argument_colors.isdisjoint(product_colors))
        or excluded_current_color
    )
    result["variant_fit_status"] = "DEFERRED" if mismatch else "MATCHED"
    result["variant_fit_reason"] = "VARIANT_MISMATCH" if mismatch else "NOT_APPLICABLE"
    result["argument_variant_tokens"] = sorted(argument_colors)
    result["product_variant_tokens"] = sorted(product_colors)
    return result


def _authoritative_selling_catalog(catalog: Iterable[Dict[str, Any]]) -> bool:
    """Whether the batch is using the governed operator/central catalog.

    Legacy tests and old frozen inputs may contain plain value rows without
    lineage metadata.  They remain compatible; once an authoritative catalog
    is present, every selling argument must retain at least one stable source
    identifier.
    """

    for row in catalog:
        if not isinstance(row, dict):
            continue
        if (
            row.get("source_argument_id")
            or row.get("source_claim_ids")
        ):
            return True
    return False


def _annotate_selling_argument_lineage(
    bundle: Dict[str, Any], *, authoritative_catalog: bool
) -> Dict[str, Any]:
    """Freeze source IDs without treating an unmapped human value as invalid."""

    result = copy.deepcopy(bundle)
    argument = result.get("selling_argument") if isinstance(result.get("selling_argument"), dict) else {}
    if _text(result.get("content_mode")).upper() != "SELLING_ARGUMENT":
        result["selling_argument_lineage"] = {
            "policy_version": "selling-argument-lineage-v1",
            "status": "NOT_APPLICABLE",
            "hard_required": False,
        }
        return result
    source_argument_id = _text(argument.get("source_argument_id"))
    source_claim_ids = [
        _text(value) for value in (argument.get("source_claim_ids") or [])
        if _text(value)
    ]
    confirmed = bool(source_argument_id or source_claim_ids)
    status = "CONFIRMED" if confirmed else (
        "DEFERRED" if authoritative_catalog else "LEGACY_COMPATIBLE"
    )
    result["selling_argument_lineage"] = {
        "policy_version": "selling-argument-lineage-v1",
        "status": status,
        "source_argument_id": source_argument_id,
        "source_claim_ids": source_claim_ids,
        "authority_source": _text(
            argument.get("authorization_source") or argument.get("source")
        ),
        "mapping_status": _text(argument.get("mapping_status")),
        "hard_required": bool(authoritative_catalog),
    }
    return result


def _build_proof_execution_intent(
    bundle: Dict[str, Any], creative: Dict[str, Any]
) -> Dict[str, Any]:
    """Compile existing governed semantics into retrieval preferences only."""

    argument = bundle.get("selling_argument") if isinstance(bundle.get("selling_argument"), dict) else {}
    subject = normalized_proof_subject(argument)
    claim_theme = _text(argument.get("claim_theme")).lower()
    scene_request = creative.get("scene_request_contract") if isinstance(creative.get("scene_request_contract"), dict) else {}
    scene_intent = _text(scene_request.get("scene_intent")).upper()
    scene_preferences = {
        _text(value).upper()
        for value in (creative.get("selling_scene_affinity_preferences") or [])
        if _text(value)
    }
    if subject == "GENERAL_EXPRESSION" and scene_intent in {
        "SCENE_USAGE", "MULTI_OCCASION", "DAYTIME_USE",
    }:
        subject = "SCENE_USAGE"
    if subject == "GENERAL_EXPRESSION" and scene_preferences.intersection({
        "COMMUTE", "MULTI_OCCASION", "DAYTIME_USE",
    }):
        subject = "SCENE_USAGE"
    if subject == "GENERAL_EXPRESSION" and claim_theme == "style":
        subject = "STYLE_RELATION"
    spec = {
        "ON_BODY_RESULT": {
            "preferred_parts": ["opening", "proof"],
            "required_any_parts": ["proof"],
            "preferred_action_tokens": ["WEAR", "RESULT_SHOW", "TURN", "WALK"],
            "hard_required_part_match": True,
        },
        "PRODUCT_DETAIL": {
            "preferred_parts": ["proof"],
            "required_any_parts": ["proof"],
            "preferred_action_tokens": ["DETAIL_SHOW", "HOLD", "ADJUST"],
            "hard_required_part_match": True,
        },
        "SCENE_USAGE": {
            "preferred_parts": ["opening", "use_process"],
            "required_any_parts": ["use_process"],
            "preferred_action_tokens": ["WEAR", "WALK", "TRY_ON"],
            "hard_required_part_match": True,
        },
        "STYLE_RELATION": {
            "preferred_parts": ["opening", "proof"],
            "required_any_parts": [],
            "preferred_action_tokens": ["WEAR", "RESULT_SHOW", "ADJUST"],
            "hard_required_part_match": False,
        },
        "GENERAL_EXPRESSION": {
            "preferred_parts": ["opening", "proof"],
            "required_any_parts": [],
            "preferred_action_tokens": [],
            "hard_required_part_match": False,
        },
    }[subject]
    return {
        "policy_version": "proof-execution-intent-v1",
        "proof_subject": subject,
        "scene_intent": scene_intent or "UNAVAILABLE",
        "claim_theme": claim_theme or "UNAVAILABLE",
        "scene_affinity_preferences": sorted(scene_preferences),
        **spec,
    }


def _build_argument_context_alignment(
    bundle: Dict[str, Any], creative: Dict[str, Any]
) -> Dict[str, Any]:
    """Describe the current outfit example without judging the selling point.

    Operator-maintained selling points remain authoritative.  This small
    contract only stops the utterance from claiming that an unselected shirt,
    collar or colour is visible in the current script.
    """

    argument = (
        bundle.get("selling_argument")
        if isinstance(bundle.get("selling_argument"), dict) else {}
    )
    argument_text = _semantic_text({
        "operator_expression": argument.get("operator_expression"),
        "core_value": argument.get("core_value"),
        "creative_core_value": argument.get("creative_core_value"),
    })
    outfit = (
        creative.get("outfit_selection_contract")
        if isinstance(creative.get("outfit_selection_contract"), dict) else {}
    )
    recipe = outfit.get("outfit_recipe") if isinstance(outfit.get("outfit_recipe"), dict) else {}
    selected_top = _text(recipe.get("top") or recipe.get("one_piece"))
    needs_shirt_example = any(
        term in argument_text.lower()
        for term in ("衬衫", "领型", "shirt collar", "collar type", "ปกเสื้อเชิ้ต")
    )
    current_has_shirt = any(
        term in selected_top.lower()
        for term in ("衬衫", "shirt", "เสื้อเชิ้ต", "翻领", "collar")
    )
    status = (
        "GENERALIZE_UNSELECTED_STYLING_EXAMPLE"
        if needs_shirt_example and not current_has_shirt
        else "CURRENT_OUTFIT_COMPATIBLE"
    )
    return {
        "policy_version": "argument-context-alignment-v1",
        "status": status,
        "selected_inner_top": selected_top,
        "instruction": (
            "保留当前卖点，但把衬衫、领型等未在本条穿搭中出现的例子概括为不同内搭；"
            "不得说成当前画面已经展示了这些单品。"
            if status == "GENERALIZE_UNSELECTED_STYLING_EXAMPLE"
            else "按当前冻结穿搭自然表达，不要求口播逐句对应画面。"
        ),
        "hard_required": False,
    }


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
    # Several central concepts may come from one numbered operator selling
    # point.  Balance the human-authored point first, then rotate its concept
    # variants after other selling points have had a turn.
    source_argument_id = _text(argument.get("source_argument_id"))
    if source_argument_id:
        return source_argument_id
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
    product_reference_assets: Optional[Iterable[Dict[str, Any]]] = None,
    top_category: str = "",
    max_candidates: int = 3,
) -> List[Dict[str, Any]]:
    """Generate up to max_candidates distinct content directions.

    Value arguments are diversified before hooks, structures or visual facts.
    Facts can repeat as proof across different arguments; rotating "five
    buttons" into the thesis is deliberately not a diversity mechanism.

    ``product_reference_assets`` / ``top_category`` only feed the per-argument
    fact-evidence record (plan §4 / C1): which product-image version the argument
    was reviewed against, and which part registry its wording should be checked
    against.  They change no existing field.
    """
    # First bundle: the existing logic
    primary = build_content_bundle_brief(
        anchor_card,
        execution_reference,
        product_type=product_type,
        selling_point_catalog=selling_point_catalog,
        product_selling_note=product_selling_note,
        product_reference_assets=product_reference_assets,
        top_category=top_category,
    )
    primary_argument = primary.get("selling_argument") if isinstance(primary.get("selling_argument"), dict) else {}
    primary_argument_id = _text(primary_argument.get("argument_id"))
    primary_source_key = _text(
        primary_argument.get("source_argument_id") or primary_argument_id
    )
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

    # Preserve operator-level breadth before consuming several normalized
    # concepts from the same numbered selling point.
    first_by_source: List[Dict[str, Any]] = []
    additional_concepts: List[Dict[str, Any]] = []
    seen_source_keys: set[str] = (
        {primary_source_key} if primary_source_key else set()
    )
    for value_angle in value_angles:
        source_key = _text(
            value_angle.get("source_argument_id") or value_angle.get("value_id")
        )
        if source_key and source_key not in seen_source_keys:
            seen_source_keys.add(source_key)
            first_by_source.append(value_angle)
        else:
            additional_concepts.append(value_angle)

    seen_argument_ids = {primary_argument_id} if primary_argument_id else set()
    for value_angle in [*first_by_source, *additional_concepts]:
        if len(candidates) >= max_candidates:
            break
        variant = build_content_bundle_brief(
            anchor_card,
            execution_reference,
            product_type=product_type,
            selling_point_catalog=[value_angle],
            product_selling_note=product_selling_note,
            product_reference_assets=product_reference_assets,
            top_category=top_category,
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
    selling_argument = (
        bundle.get("selling_argument")
        if isinstance(bundle.get("selling_argument"), dict)
        else {}
    )
    audience_need_authorized = (
        _text(
            bundle.get("audience_need_authority")
            or selling_argument.get("audience_need_authority")
        ).upper()
        == "APPROVED_SELLING_SCENARIO"
    )
    suppressed = []
    result = []
    for hid in eligible:
        hid = _text(hid)
        if not hid:
            continue
        needs_tension = hid in {"PAIN_REFRAME", "USER_ADVOCACY_STANCE"}
        needs_need_authority = hid == "AUDIENCE_NEED_CALLOUT"
        if (
            (needs_tension and not tension_available)
            or (
                needs_need_authority
                and not tension_available
                and not audience_need_authorized
            )
        ):
            suppressed.append(hid)
            continue
        if hid in active_hook_ids:
            result.append(hid)
    if not result and active_hook_ids:
        result = [h for h in active_hook_ids if h not in HOOK_ID_BLACKLIST_FOR_NO_TENSION][:2]
    return result, suppressed


# ── Three-round allocation ─────────────────────────────────────────────


def _mixed_scope_status(
    product_type: str,
    top_category: str,
    execution_scope: Optional[Dict[str, Any]],
) -> str:
    """Whether the accessory mixed mode governs this batch at all.

    ``IN_SCOPE`` / ``OUT_OF_SCOPE`` (feature off or another category) /
    ``SCOPE_UNSUPPORTED`` (this is a long-form build, a remake or a resume).
    The distinction matters: three zeroed coverage counters on an out-of-scope
    batch must never read as "compared history, found no duplicate".
    """

    try:
        from core.accessory_mixed_templates import (
            accessory_mixed_template_enabled,
            mixed_scope_decision,
            resolve_mixed_zone,
        )
    except Exception:  # noqa: BLE001 - a report label must never break planning
        return "OUT_OF_SCOPE"
    if not accessory_mixed_template_enabled():
        return "OUT_OF_SCOPE"
    if not mixed_scope_decision(execution_scope).get("eligible"):
        return "SCOPE_UNSUPPORTED"
    zone, _canonical = resolve_mixed_zone(product_type, top_category)
    return "IN_SCOPE" if zone else "OUT_OF_SCOPE"


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
    target_country: str = "",
    target_language: str = "",
    scene_reference_contexts: Optional[Dict[str, Dict[str, Any]]] = None,
    multidim_reference_contexts: Optional[Dict[str, Dict[str, Any]]] = None,
    category_execution_extension: Optional[Dict[str, Any]] = None,
    execution_scope: Optional[Dict[str, Any]] = None,
    mixed_history_exclusions: Optional[Dict[str, Any]] = None,
    product_reference_assets: Optional[Iterable[Dict[str, Any]]] = None,
) -> Tuple[List[PlanItem], Dict[str, Any]]:
    """Three-round deterministic allocation returning items and allocation summary.

    ``execution_scope`` is the caller's declaration of the *real* task this
    planning request belongs to (short-original vs long-form vs remake, its real
    target duration, whether this is a fresh plan).  The authored mixed template
    is a 15-second short-original mode, and the long-form direct-product builder
    borrows this same entry point with a 15-second duration while owning a
    different parent task, so the category and the environment flag alone cannot
    decide it.  ``None`` keeps the historical behaviour for direct callers.

    ``mixed_history_exclusions`` names the ledger rows that belong to the batch
    being planned *now* (``batch_ids`` / ``usage_ids`` / ``batch_item_ids`` /
    ``script_ids``).  A resumed batch must not be compared against its own first
    attempt (R3).  ``None`` -- a fresh plan, every other category, every existing
    caller -- filters nothing.
    """
    recent = list(recent_creative_usage or [])
    rng = random.Random(random_seed)
    items: List[PlanItem] = []
    reserved_visual_signatures: List[str] = []
    reserved_creative_contracts: List[Dict[str, Any]] = []
    # Final-shot references for the mixed montage.  Kept separate from the
    # legacy scene signatures above: they measure a different axis and mixing
    # them would let a rotating scene label hide a repeated montage.
    reserved_mixed_references: List[Dict[str, Any]] = []
    mixed_history_references, mixed_history_skipped = _mixed_history_references(
        recent, product_code, exclusions=mixed_history_exclusions
    )
    # 历史比较覆盖范围: a reader must be able to see how much history was
    # actually available before believing a "no duplicate found" statement.
    # ``scope_status`` says whether this mechanism applied to the batch at all,
    # so an all-zero coverage on a women's-wear batch is never read as a dedup
    # verdict.  ``history_other_product`` is listed separately from
    # ``history_incomplete`` on purpose: those rows *were* complete, they simply
    # belong to another product and are therefore not a duplicate verdict (I2).
    # ``history_excluded_own`` is a third bucket again: those rows were written
    # by the batch being planned right now, so they were never history to begin
    # with (R3).  It is reported because a resume that silently compared a batch
    # against itself is indistinguishable from a resume that found nothing.
    mixed_scope_status = _mixed_scope_status(
        product_type, top_category, execution_scope
    )
    mixed_history_coverage: Dict[str, Any] = {
        "scope_status": mixed_scope_status,
        "product_code": _text(product_code),
        "history_compared": len(mixed_history_references),
        "history_incomplete": mixed_history_skipped.get("incomplete", 0),
        "history_other_product": mixed_history_skipped.get("other_product", 0),
        "history_excluded_own": mixed_history_skipped.get("own_record", 0),
        "history_rows_seen": (
            len(mixed_history_references)
            + mixed_history_skipped.get("incomplete", 0)
            + mixed_history_skipped.get("other_product", 0)
            + mixed_history_skipped.get("own_record", 0)
        ),
    }
    # Kept as a plain int for the per-item difference report, which records how
    # much of the history could not be compared at all.
    mixed_history_incomplete = int(mixed_history_coverage["history_incomplete"])
    mixed_difference_tally: Dict[str, int] = {
        "distinct_theme": 0,
        "execution_variant": 0,
        "duplicate_rejected": 0,
        "insufficient_evidence": 0,
        "uncompared": 0,
    }
    reference_video_usage: Counter = Counter()
    deferred_content: List[Dict[str, Any]] = []
    catalog_rows = list(selling_point_catalog or [])
    authoritative_catalog = _authoritative_selling_catalog(catalog_rows)
    product_market_context = (
        build_product_market_context(
            product_code=product_code,
            target_country=target_country,
            target_language=target_language,
            product_type=product_type,
            selling_point_catalog=catalog_rows,
        )
        if semantic_spine_enabled()
        else {}
    )
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
            selling_point_catalog=catalog_rows,
            product_selling_note=product_selling_note,
            product_reference_assets=product_reference_assets,
            top_category=top_category,
            # A test batch should see the available selling-point breadth.
            # The previous fixed limit of three candidates per carrier made a
            # six-point product repeat two arguments before reaching the rest.
            max_candidates=max(3, requested_count),
        )
        all_candidates = [
            _annotate_carrier_fit(
                _annotate_variant_fit(
                    _annotate_selling_argument_lineage(
                        bundle, authoritative_catalog=authoritative_catalog
                    ),
                    anchor_card=anchor_card,
                ),
                direction=d,
            )
            for bundle in raw_candidates
        ]
        if semantic_spine_enabled():
            for bundle in all_candidates:
                bundle["semantic_spine_contract"] = build_script_semantic_spine(
                    product_code=product_code,
                    content_bundle=bundle,
                    market_context=product_market_context,
                )
        candidates: List[Dict[str, Any]] = []
        for bundle in all_candidates:
            lineage = bundle.get("selling_argument_lineage") or {}
            if lineage.get("status") == "DEFERRED":
                deferred_content.append({
                    "direction_assignment_id": d.get("direction_assignment_id", ""),
                    "output_slot": d.get("output_slot", ""),
                    "content_bundle_id": bundle.get("content_bundle_id", ""),
                    "content_mode": bundle.get("content_mode", "FACTUAL_OBSERVATION"),
                    "downgrade_reason": "SELLING_ARGUMENT_LINEAGE_MISSING",
                    "recommended_flow": "REFRESH_SELLING_POINT_CATALOG",
                })
                continue
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
        rejection = _mixed_structure_rejection(
            direction=d,
            product_type=product_type,
            top_category=top_category,
            execution_scope=execution_scope,
        )
        if rejection:
            # This structure cannot host the frozen four-shot template: it needs
            # more beats than the template has shots and there is no defined
            # mapping for the surplus beat.  Exclude the candidate while
            # structures are still being chosen, and say why -- truncating the
            # extra beat later in the compile step would delete narrative
            # authority, and merging two beats would invent one.
            deferred_content.append(
                {
                    "direction_assignment_id": d.get("direction_assignment_id", ""),
                    "output_slot": d.get("output_slot", ""),
                    "content_bundle_id": "",
                    "content_mode": "FACTUAL_OBSERVATION",
                    "argument_readiness": "NOT_APPLICABLE",
                    "downgrade_reason": rejection,
                    "recommended_flow": "REPLAN_WITH_MIXED_COMPATIBLE_STRUCTURE",
                }
            )
            continue
        content_pools[idx] = candidates
        structure_pool.append(d)

    # Usage counters
    struct_usage: Counter = Counter()
    angle_usage: Counter = Counter()
    argument_usage: Counter = Counter()
    hook_usage: Counter = Counter()
    hook_family_usage: Counter = Counter()
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
                argument_usage[_bundle_argument_key(pair[1])],
                angle_usage[_text(pair[1].get("content_angle_key", "FACT_DISCOVERY"))],
                pair[0],
            ),
        )
        angle_key = _text(bundle.get("content_angle_key", "FACT_DISCOVERY"))
        eligible_hooks, _ = _eligible_hooks_for_bundle(bundle, active_hook_ids)
        if not eligible_hooks:
            continue
        hook_id = _pick_least_used(
            eligible_hooks, hook_usage, rng, family_usage=hook_family_usage
        )
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
            multidim_reference_context=(multidim_reference_contexts or {}).get(
                _text(direction.get("direction_assignment_id"))
            ),
            reference_video_usage=reference_video_usage,
            category_execution_extension=category_execution_extension,
            execution_scope=execution_scope,
            # The very list the summary already reports as deferred content: a
            # rejected mixed contract *is* a candidate that could not be
            # delivered, so its reason code belongs with the others.
            planning_rejections=deferred_content,
            reserved_mixed_references=reserved_mixed_references,
            mixed_history_references=mixed_history_references,
            mixed_history_incomplete=mixed_history_incomplete,
            mixed_difference_tally=mixed_difference_tally,
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
            hook_family_usage[_hook_family(hook_id)] += 1
            visual_usage[visual_sig] += 1
            mother_bundle_indices[struct_idx] = bundle_index
            relationship_devices.append(relationship_device)

    if len(items) >= requested_count:
        return items, _build_summary(
            items, structure_pool, "ROUND1_COMPLETE",
            requested_count=requested_count,
            deferred_content=deferred_content,
            mixed_difference_tally=mixed_difference_tally,
            mixed_history_coverage=mixed_history_coverage,
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
                argument_usage[_bundle_argument_key(entry[3])],
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
        hook_id = _pick_least_used(
            eligible_hooks, hook_usage, rng, family_usage=hook_family_usage
        )
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
            multidim_reference_context=(multidim_reference_contexts or {}).get(
                _text(direction.get("direction_assignment_id"))
            ),
            reference_video_usage=reference_video_usage,
            category_execution_extension=category_execution_extension,
            execution_scope=execution_scope,
            # The very list the summary already reports as deferred content: a
            # rejected mixed contract *is* a candidate that could not be
            # delivered, so its reason code belongs with the others.
            planning_rejections=deferred_content,
            reserved_mixed_references=reserved_mixed_references,
            mixed_history_references=mixed_history_references,
            mixed_history_incomplete=mixed_history_incomplete,
            mixed_difference_tally=mixed_difference_tally,
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
            hook_family_usage[_hook_family(hook_id)] += 1
            visual_usage[visual_sig] += 1
            relationship_devices.append(relationship_device)

    if len(items) >= requested_count:
        return items, _build_summary(
            items, structure_pool, "ROUND2_COMPLETE",
            requested_count=requested_count,
            deferred_content=deferred_content,
            mixed_difference_tally=mixed_difference_tally,
            mixed_history_coverage=mixed_history_coverage,
        )

    return items[:requested_count], _build_summary(
        items, structure_pool, "PARTIAL_CONTENT_CAPACITY",
        requested_count=requested_count,
        deferred_content=deferred_content,
        mixed_difference_tally=mixed_difference_tally,
        mixed_history_coverage=mixed_history_coverage,
    )


# ── Helpers ────────────────────────────────────────────────────────────


def _pick_least_used(
    candidates: List[str],
    usage: Counter,
    rng: random.Random,
    *,
    family_usage: Optional[Counter] = None,
) -> str:
    """Pick a compatible hook with soft family rotation.

    The allocator never introduces a hook outside ``candidates``.  Within that
    governed pool it prefers an under-used rhetorical family, then an
    under-used hook ID.  GENERAL_PRODUCT_SHARE loses only an exact tie so it
    remains a safe fallback without swallowing every batch.
    """

    family_usage = family_usage or Counter()

    def score(candidate: str) -> Tuple[int, int, int, int]:
        return (
            family_usage.get(_hook_family(candidate), 0),
            usage.get(candidate, 0),
            1 if _text(candidate).upper() == "GENERAL_PRODUCT_SHARE" else 0,
            candidates.index(candidate),
        )

    minimum = min(score(candidate) for candidate in candidates)
    least = [candidate for candidate in candidates if score(candidate) == minimum]
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
            {
                **contract,
                "product_code": product_code,
                "_batch_reserved": True,
            }
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
    multidim_reference_context: Optional[Dict[str, Any]] = None,
    reference_video_usage: Optional[Counter] = None,
    category_execution_extension: Optional[Dict[str, Any]] = None,
    execution_scope: Optional[Dict[str, Any]] = None,
    planning_rejections: Optional[List[Dict[str, Any]]] = None,
    reserved_mixed_references: Optional[List[Dict[str, Any]]] = None,
    mixed_history_references: Optional[List[Dict[str, Any]]] = None,
    mixed_history_incomplete: int = 0,
    mixed_difference_tally: Optional[Dict[str, int]] = None,
) -> Optional[PlanItem]:
    da_id = direction.get("direction_assignment_id", "")
    atoms = bundle.get("claim_atoms", [])
    claim_keys = [_text(a.get("claim_key")) for a in atoms if _text(a.get("claim_key"))]

    frozen_bundle = copy.deepcopy(bundle)
    frozen_bundle["argument_context_alignment"] = _build_argument_context_alignment(
        frozen_bundle, creative
    )
    frozen_bundle["proof_execution_intent"] = _build_proof_execution_intent(
        frozen_bundle, creative
    )
    selling_argument = frozen_bundle.get("selling_argument") if isinstance(frozen_bundle.get("selling_argument"), dict) else {}
    semantic_spine = copy.deepcopy(
        frozen_bundle.get("semantic_spine_contract") or {}
    )
    context_bridge = (
        build_context_bridge(semantic_spine, creative)
        if semantic_spine_enabled() and semantic_spine
        else {}
    )
    if semantic_spine:
        frozen_bundle["semantic_spine_contract"] = semantic_spine
    if context_bridge:
        frozen_bundle["context_bridge_contract"] = context_bridge
    selling_argument_id = _text(selling_argument.get("argument_id"))
    claim_action_contract = {
        "policy_version": "small-accessory-claim-action-v1",
        "selling_argument_id": selling_argument_id,
        "proof_action_intent": _text(
            selling_argument.get("proof_action_intent")
        ),
        "preferred_action_mode": _text(
            selling_argument.get("preferred_action_mode")
        ).upper(),
        "required_proof_relation": _text(
            selling_argument.get("required_proof_relation")
        ),
        "carrier_fit_status": _text(
            frozen_bundle.get("carrier_fit_status")
        ) or "MATCHED",
        "hard_required": False,
        "may_trigger_retry": False,
    }
    frozen_bundle["claim_action_contract"] = claim_action_contract
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
        "content_bundle_brief": frozen_bundle,
        "semantic_spine_contract": semantic_spine,
        "context_bridge_contract": context_bridge,
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
        "hook_allocation_contract": {
            "policy_version": "hook-allocation-v2-compatible-family-rotation",
            "requested_hook_id": hook_id,
            "hook_family": _hook_family(hook_id),
            "eligible_hook_ids": list(eligible_hooks),
            "eligible_hook_families": list(dict.fromkeys(
                _hook_family(item) for item in eligible_hooks
            )),
            "allocation_status": (
                "GENERAL_FALLBACK_ONLY"
                if eligible_hooks == ["GENERAL_PRODUCT_SHARE"]
                else "COMPATIBLE_ROTATION"
                if len(eligible_hooks) > 1
                else "SINGLE_COMPATIBLE"
            ),
        },
        "content_angle_key": angle_key,
        "selling_argument_id": selling_argument_id,
        "selling_argument_lineage": copy.deepcopy(
            frozen_bundle.get("selling_argument_lineage") or {}
        ),
        "proof_execution_intent": copy.deepcopy(
            frozen_bundle.get("proof_execution_intent") or {}
        ),
        "claim_action_contract": copy.deepcopy(claim_action_contract),
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
    from core.multidim_reference_adapter import (
        select_retrieval_reference_contract,
    )

    retrieval_creative = dict(creative)
    retrieval_creative.setdefault(
        "carrier_mode", _text(hard.get("content_carrier"))
    )
    retrieval_reference_contract = select_retrieval_reference_contract(
        multidim_reference_context,
        creative_contract=retrieval_creative,
        content_bundle=frozen_bundle,
        requested_hook_id=hook_id,
        used_video_ids=reference_video_usage,
        category_execution_extension=category_execution_extension,
    )
    frozen_package["retrieval_reference_contract"] = (
        retrieval_reference_contract
    )
    from core.multidim_reference_adapter import (
        legacy_execution_reference_projection,
    )
    projected_execution_reference = legacy_execution_reference_projection(
        retrieval_reference_contract
    )
    if projected_execution_reference:
        frozen_package["route_execution_reference"] = dict(
            direction.get("execution_reference", {}) or {}
        )
        frozen_package["execution_reference"] = projected_execution_reference
    # The simplified path consumes the same frozen plan without changing the
    # allocator or adding another database.  It is a compact input contract,
    # not a second creative decision layer.
    from core.simplified_complete_script import build_simplified_creative_seed
    from core.simplified_complete_script import build_creator_recording_profile
    recording_profile = build_creator_recording_profile(
        top_category=top_category,
        product_type=product_type,
        content_carrier=_text(hard.get("content_carrier")),
    )
    # ── Authored mixed accessory template (feature-gated) ─────────────
    # Adds one frozen per-shot display contract inside the existing
    # category_execution_extension.  When the gate is off this writes nothing,
    # so frozen packages for every other product stay byte-for-byte identical.
    #
    # Ordering is load-bearing: this must run *before* the creative seed is
    # built.  The seed deep-copies the extension it is handed, and the script
    # stage consumes the frozen seed verbatim (it never rebuilds it when the
    # batch already carries one).  Injecting after the seed therefore wrote the
    # contract into the frozen package but not into the copy the generator
    # actually reads, so the template silently vanished between planning and
    # generation -- the blueprint fell back to the legacy single-carrier
    # framing advice.
    mixed_injection = _build_mixed_template_injection(
        product_type=product_type,
        top_category=top_category,
        item_index=item_index,
        item_role=item_role,
        content_angle_key=angle_key,
        audience_tension_text=bundle.get("audience_tension_text", ""),
        claim_keys=claim_keys,
        product_code=product_code,
        execution_scope=execution_scope,
        reserved_references=reserved_mixed_references,
        history_references=mixed_history_references,
        history_incomplete=mixed_history_incomplete,
        identity=f"{da_id}#{int(item_index):02d}",
        # The theme the contract carries must be a sentence a reviewer can read
        # back, not the angle key.  Resolution stays here because this is where
        # the content bundle is in scope.
        theme_proposition=_mixed_theme_proposition(bundle, fallback_theme_id=angle_key),
    )
    if mixed_injection.get("contract"):
        mixed_extension = dict(category_execution_extension or {})
        mixed_extension["mixed_template_contract"] = mixed_injection["contract"]
        category_execution_extension = mixed_extension
        frozen_package["category_execution_extension"] = mixed_extension
        frozen_package["mixed_template_contract_status"] = "FROZEN"
        _record_mixed_difference(
            mixed_difference_tally,
            mixed_injection["contract"].get("difference_report"),
        )
        if reserved_mixed_references is not None:
            # Reserve the *final shots* this candidate owns, so the next item in
            # this same batch is compared against what was actually accepted
            # rather than against a plan that never shipped.
            reserved_mixed_references.append(
                {
                    "identity": f"{da_id}#{int(item_index):02d}",
                    "signature": mixed_injection["contract"].get(
                        "final_shot_signature"
                    )
                    or {},
                }
            )
        # C2: freeze one mainline per film and let it carry the observation tasks.
        # This has to happen *before* ``build_simplified_creative_seed`` below,
        # otherwise the frozen package would hold the mainline while the seed the
        # script stage consumes verbatim would not.
        from core.mixed_mainline_contract import (
            build_mixed_mainline_contract,
            mixed_mainline_enabled,
        )

        if mixed_mainline_enabled():
            mainline = build_mixed_mainline_contract(
                selling_argument=selling_argument,
                fact_evidence=selling_argument.get("fact_evidence") or {},
                semantic_spine=semantic_spine,
                mixed_contract=mixed_injection["contract"],
                audience_tension_text=bundle.get("audience_tension_text", ""),
                requested_hook_id=hook_id,
            )
            if mainline:
                frozen_bundle["mixed_mainline_contract"] = mainline
                frozen_package["mixed_mainline_contract"] = mainline
    elif mixed_injection.get("rejected"):
        # A valid contract whose four final shots repeat one this batch already
        # owns.  Delivering it anyway is exactly what Review #3 found: the batch
        # claimed N distinct scripts while showing the same montage N times.
        rejection = mixed_injection["rejected"]
        verdict = _text(rejection.get("review_status")).upper()
        tally = mixed_difference_tally
        if tally is not None:
            key = (
                "insufficient_evidence"
                if verdict == "NEEDS_REVIEW"
                else "duplicate_rejected"
            )
            tally[key] = tally.get(key, 0) + 1
        if planning_rejections is not None:
            planning_rejections.append(
                {
                    "direction_assignment_id": da_id,
                    "output_slot": direction.get("output_slot", f"S{int(item_index)}"),
                    "content_bundle_id": bundle.get("content_bundle_id", ""),
                    "content_mode": bundle.get("content_mode", "FACTUAL_OBSERVATION"),
                    "argument_readiness": bundle.get(
                        "argument_readiness", "NOT_APPLICABLE"
                    ),
                    "downgrade_reason": _text(rejection.get("reason"))
                    or "MIXED_DUPLICATE_CANDIDATE",
                    "difference_report": rejection.get("difference_report") or {},
                    "recommended_flow": "REPLAN_MIXED_THEME",
                }
            )
        return None
    elif mixed_injection.get("errors"):
        # A contract that failed hard validation must never look like a
        # successful plan, and must never reach paid generation.  Return no
        # executable item at all: a "PLANNED" item carrying a REJECTED contract
        # is exactly the hole this closes.  The reason codes travel back to the
        # caller so the batch report can say *why* the candidate was dropped.
        errors = [str(item) for item in mixed_injection["errors"] if str(item)]
        # A missing readable theme is reported *by name*.  "There was no buying
        # reason to build this video on" is a different operational problem from
        # "the contract contradicted itself", and a reader must be able to tell
        # them apart from the report alone (package B3).
        theme_gap = next(
            (item for item in errors if item.startswith("MIXED_THEME_INPUT_GAP")), ""
        )
        if planning_rejections is not None:
            planning_rejections.append(
                {
                    "direction_assignment_id": da_id,
                    "output_slot": direction.get("output_slot", f"S{int(item_index)}"),
                    "content_bundle_id": bundle.get("content_bundle_id", ""),
                    "content_mode": bundle.get("content_mode", "FACTUAL_OBSERVATION"),
                    "argument_readiness": bundle.get(
                        "argument_readiness", "NOT_APPLICABLE"
                    ),
                    "downgrade_reason": theme_gap or "MIXED_CONTRACT_REJECTED",
                    "contract_errors": errors,
                    "recommended_flow": (
                        "REPLAN_MIXED_THEME"
                        if theme_gap
                        else "REPLAN_MIXED_CONTRACT"
                    ),
                }
            )
        return None

    frozen_package["simplified_creative_seed"] = build_simplified_creative_seed(
        anchor_card=dict(anchor_card or {}),
        structure_contract=contract,
        content_bundle=frozen_bundle,
        creative_contract=creative,
        execution_reference=frozen_package.get("execution_reference", {}) or {},
        requested_hook_id=hook_id,
        content_angle_key=angle_key,
        relationship_device=relationship_device,
        product_type=product_type,
        top_category=top_category,
        retrieval_reference_contract=retrieval_reference_contract,
        category_execution_extension=category_execution_extension,
        creator_recording_profile=recording_profile,
    )
    # C2: carry the mainline into the seed as well.  The seed is what the script
    # stage consumes verbatim, so a contract living only on the frozen package
    # would look present in the plan and be missing from generation -- the exact
    # failure the mixed template contract already taught us (see
    # ``_repair_frozen_seed_mixed_extension``).  It goes beside the template
    # contract, in ``category_execution_extension``, so a reviewer can check both
    # frozen hand-offs at the same place.
    mainline_contract = frozen_package.get("mixed_mainline_contract")
    if isinstance(mainline_contract, dict) and mainline_contract:
        seed_payload = frozen_package.get("simplified_creative_seed")
        if isinstance(seed_payload, dict):
            seed_payload["mixed_mainline_contract"] = dict(mainline_contract)
            seed_extension = dict(seed_payload.get("category_execution_extension") or {})
            seed_extension["mixed_mainline_contract"] = dict(mainline_contract)
            seed_payload["category_execution_extension"] = seed_extension

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


def _routed_beats(contract: Dict[str, Any]) -> List[str]:
    """Routed beat order for one direction, via the single existing authority."""

    try:
        from core.simplified_complete_script import _macro_structure

        return list(_macro_structure(contract or {}))
    except Exception:  # noqa: BLE001 - a structure we cannot read is not a filter
        return []


def _mixed_structure_rejection(
    *,
    direction: Dict[str, Any],
    product_type: str,
    top_category: str,
    execution_scope: Optional[Dict[str, Any]],
) -> str:
    """Reason code when this structure cannot host the mixed template.

    Empty string means "keep the candidate".  Only consulted when the mixed mode
    is genuinely on for this execution scope and this product, so every other
    request returns immediately and no existing path changes.
    """

    try:
        from core.accessory_mixed_templates import (
            accessory_mixed_template_enabled,
            map_structure_beats_to_units,
            mixed_scope_decision,
            mixed_template_shot_limit,
            resolve_mixed_zone,
        )
    except Exception:  # noqa: BLE001 - never break planning
        return ""
    if not accessory_mixed_template_enabled():
        return ""
    if not mixed_scope_decision(execution_scope).get("eligible"):
        return ""
    zone, _canonical = resolve_mixed_zone(product_type, top_category)
    if zone is None:
        return ""

    contract = (
        direction.get("structure_contract")
        if isinstance(direction.get("structure_contract"), dict)
        else {}
    )
    mapping = map_structure_beats_to_units(
        _routed_beats(contract), mixed_template_shot_limit()
    )
    if mapping.get("compatible"):
        return ""
    return _text(mapping.get("reason")) or "MIXED_STRUCTURE_INCOMPATIBLE"


def _mixed_history_references(
    recent_usage: Optional[List[Dict[str, Any]]],
    product_code: str = "",
    exclusions: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Comparable mixed references from the historical usage ledger.

    Returns ``(references, skipped)``.

    Three kinds of row are deliberately left out, and all three are **counted**
    rather than silently dropped -- reading a skip as "compared and found
    nothing" is how a dedup pass gets reported that never happened:

    * ``incomplete`` -- the row predates the final-shot signatures.  Treating a
      missing record as "nothing similar found" would overstate the comparison,
      and rebuilding a signature from the legacy scene id would fabricate a
      reference outright.
    * ``other_product`` -- the row belongs to a different SKU.  Reusing a
      template across products is an account-style / scheduling question, not a
      duplicate video: the same A/B/C montage is *supposed* to recur across a
      catalogue.  Content-level hard de-duplication only applies within one
      product (I2).
    * ``own_record`` -- the row was written by the batch being planned right
      now.  Resuming a batch re-plans it, and its first attempt already reserved
      ledger rows; without this filter every re-planned candidate is compared
      against its own earlier incarnation, reads as a duplicate, and the batch
      delivers nothing (Review R3).  Excluding these rows loses no comparison:
      sibling comparison inside a run happens through the reserved-reference
      list, so the same row would otherwise be compared twice.

    A row whose product code is missing cannot be attributed, so it is treated as
    ``incomplete``: an unattributable row must not silently become a hard filter
    for whichever product happens to be planning.
    """

    try:
        from core.accessory_mixed_templates import (
            ledger_row_is_own_record,
            mixed_reference_signature,
        )
    except Exception:  # noqa: BLE001 - never break planning
        return [], {"incomplete": 0, "other_product": 0, "own_record": 0}

    declared = exclusions if isinstance(exclusions, dict) else {}
    own = {
        "batch_ids": declared.get("batch_ids") or (),
        "usage_ids": declared.get("usage_ids") or (),
        "batch_item_ids": declared.get("batch_item_ids") or (),
        "script_ids": declared.get("script_ids") or (),
    }

    wanted = _text(product_code)
    references: List[Dict[str, Any]] = []
    skipped = {"incomplete": 0, "other_product": 0, "own_record": 0}
    for row in recent_usage or []:
        if not isinstance(row, dict):
            continue
        if ledger_row_is_own_record(row, **own):
            skipped["own_record"] += 1
            continue
        row_product = _text(row.get("product_code"))
        # Product attribution is decided *before* readability.  ``history_incomplete``
        # must mean "same product, but the record cannot be compared" -- if rows
        # of other products were counted here, an unreadable foreign row would
        # make a same-product candidate look uncomparable when it is not.
        if wanted and row_product and row_product != wanted:
            skipped["other_product"] += 1
            continue
        reference = mixed_reference_signature(row)
        if not reference.get("complete"):
            skipped["incomplete"] += 1
            continue
        if not row_product:
            # An unattributable row cannot be filtered by product above, so it
            # must not become a reference either.
            skipped["incomplete"] += 1
            continue
        references.append(reference)
    return references, skipped


def _record_mixed_difference(
    tally: Optional[Dict[str, int]],
    report: Optional[Dict[str, Any]],
) -> None:
    """Count one accepted candidate into the report's independent-content tally.

    A ``DISTINCT_THEME`` reached with **no comparable reference** is not a
    finding -- it is the absence of one.  ``comparison_scope ==
    "HISTORY_INCOMPARABLE"`` says exactly that: same-product history existed but
    could not be read (written before the final-shot signatures, or under an
    older version).  Counting those as independent themes is how an unperformed
    comparison gets reported as a verified difference, so they go to their own
    bucket and the summary names them.
    """

    if not isinstance(tally, dict) or not isinstance(report, dict):
        return
    verdict = _text(report.get("review_status")).upper()
    scope = _text(report.get("comparison_scope")).upper()
    if scope == "HISTORY_INCOMPARABLE":
        tally["uncompared"] = tally.get("uncompared", 0) + 1
        return
    if verdict == "DISTINCT_THEME":
        tally["distinct_theme"] = tally.get("distinct_theme", 0) + 1
    elif verdict == "EXECUTION_VARIANT":
        tally["execution_variant"] = tally.get("execution_variant", 0) + 1


# ── Readable theme proposition (package B3) ────────────────────────────────
# The mixed contract's ``content_theme.thesis`` used to be
# ``audience_tension_text or content_angle_key or theme_id``.  In a real run all
# three are unusable as a *theme sentence*: ``audience_tension.status`` comes
# back ``UNAVAILABLE`` with empty text, and the two fallbacks are internal IDs.
# Measured on the four real scripts of 2026-09-17, every plan wrote
# ``"thesis": "ARGUMENT_OPERATOR_PCS_..._PCL_..."`` -- an opaque key with a
# theme's name on it.  A reader cannot tell two buying reasons apart from it,
# and the difference judge was comparing exactly that shape (Review R4).
#
# The readable proposition does exist; it simply lives further down the bundle.
# Order below is most-authoritative first, and every rung names the *field it
# was copied from*, so "why does this video have this theme?" is answerable by
# opening the bundle rather than by decoding an ID.
_MIXED_THESIS_SOURCES: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    (
        "SPINE_CORE_BUYING_REASON",
        ("semantic_spine_contract", "script_thesis", "core_buying_reason"),
    ),
    ("SELLING_ARGUMENT_CORE_VALUE", ("selling_argument", "core_value")),
    ("CONTENT_MAINLINE", ("content_mainline",)),
    ("SELLING_ARGUMENT_OPERATOR_EXPRESSION", ("selling_argument", "operator_expression")),
    (
        "SELLING_ARGUMENT_CREATIVE_CORE_VALUE",
        ("selling_argument", "creative_core_value"),
    ),
    (
        "SELLING_ARGUMENT_SOURCE_OPERATOR_EXPRESSION",
        ("selling_argument", "source_operator_expression"),
    ),
    ("AUDIENCE_SITUATION", ("audience_situation",)),
    # Legacy projection of the same block; only consulted last, and only for
    # the fields it actually carries.
    ("VALUE_PROPOSITION_OPERATOR_EXPRESSION", ("value_proposition", "operator_expression")),
    ("VALUE_PROPOSITION_SOURCE_OPERATOR_EXPRESSION", ("value_proposition", "source_operator_expression")),
)

# Where the *argument ID* is kept once the readable text takes over ``thesis``.
_MIXED_ARGUMENT_ID_SOURCES: Tuple[Tuple[str, ...], ...] = (
    ("selling_argument", "argument_id"),
    ("value_proposition", "argument_id"),
    ("selling_argument", "source_argument_id"),
    ("value_proposition", "source_argument_id"),
    ("selling_argument_lineage", "source_argument_id"),
)


def _mixed_bundle_path(bundle: Any, path: Sequence[str]) -> str:
    """Read a dotted bundle path, tolerating every intermediate being absent."""

    node: Any = bundle
    for key in path:
        if not isinstance(node, dict):
            return ""
        node = node.get(key)
    return _text(node)


def _mixed_argument_id(bundle: Any) -> str:
    for path in _MIXED_ARGUMENT_ID_SOURCES:
        value = _mixed_bundle_path(bundle, path)
        if value:
            return value
    return ""


def _mixed_theme_proposition(
    bundle: Any,
    *,
    fallback_theme_id: str = "",
) -> Dict[str, str]:
    """Resolve the readable proposition this item's video is built around.

    Returns ``thesis`` / ``thesis_source`` / ``thesis_source_ref`` /
    ``thesis_input_gap`` plus ``argument_id``.  When nothing readable exists the
    ``thesis`` is empty and ``thesis_input_gap`` names the gap -- the internal
    angle key is deliberately *not* promoted into the theme slot, because doing
    exactly that is what made four different buying reasons look alike.
    """

    angle = _text(fallback_theme_id)
    try:
        from core.accessory_mixed_templates import resolve_theme_proposition
    except Exception:  # noqa: BLE001 - a missing module must not break planning
        resolve_theme_proposition = None  # type: ignore[assignment]

    candidates: List[Dict[str, str]] = []
    for label, path in _MIXED_THESIS_SOURCES:
        text = _mixed_bundle_path(bundle, path)
        if not text:
            continue
        candidates.append(
            {
                "source": label,
                "ref": "bundle." + ".".join(path),
                "text": text,
            }
        )
    # The angle key is offered last and *labelled as an ID*: if the readability
    # guard ever let it through, the source field would still say where it came
    # from, so it could never be mistaken for an authored proposition.
    if angle:
        candidates.append(
            {
                "source": "CONTENT_ANGLE_KEY",
                "ref": "original_content_item.content_angle_key",
                "text": angle,
            }
        )

    if resolve_theme_proposition is None:
        resolved = {"thesis": "", "thesis_source": "", "thesis_source_ref": "", "thesis_input_gap": "THESIS_NOT_READABLE"}
    else:
        resolved = resolve_theme_proposition(candidates)
    resolved["argument_id"] = _mixed_argument_id(bundle)
    return resolved


def _build_mixed_template_injection(
    *,
    product_type: str,
    top_category: str,
    item_index: int,
    item_role: str,
    content_angle_key: str,
    audience_tension_text: str,
    claim_keys: List[str],
    product_code: str,
    execution_scope: Optional[Dict[str, Any]] = None,
    reserved_references: Optional[List[Dict[str, Any]]] = None,
    history_references: Optional[List[Dict[str, Any]]] = None,
    history_incomplete: int = 0,
    identity: str = "",
    theme_proposition: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Compile the authored mixed-accessory contract for one plan item.

    Returns one of five shapes:

    * ``{}``                       -- the feature is off, or the product is not
      one of the four supported accessory families.  Nothing is injected, so
      every existing path stays byte-for-byte unchanged.
    * ``{"scope_rejected": ...}``  -- the mode is on and the product qualifies,
      but the *real* parent task is not a 15-second short original (a long-form
      source build borrowing this entry point, a remake, a resumed plan).  No
      contract is injected and no failure is recorded: this request simply is
      not the mixed mode's business.
    * ``{"contract": {...}}``      -- a validated contract that won its
      final-shot comparison, with ``difference_report`` filled in.
    * ``{"errors": [...]}``        -- the contract was built but failed hard
      validation.  It is *not* injected, and the failure is recorded so it can
      never be mistaken for a successful plan.
    * ``{"rejected": {...}}``      -- the contract is valid but its four final
      shots repeat a candidate this batch (or history) already owns.  The
      reason and the comparison report travel back so the batch report can show
      *why* fewer scripts were delivered instead of quietly minting a synonym.

    The theme reuses the existing content semantics (``content_angle_key`` /
    ``audience_tension_text`` / ``claim_keys``) and the existing item role, so no
    parallel theme registry or creative-history database is introduced.
    """

    try:
        from core.accessory_mixed_templates import (
            MIXED_SIGNATURE_KEY,
            accessory_mixed_template_enabled,
            compile_mixed_template_contract,
            judge_mixed_candidate,
            mixed_scope_decision,
            mixed_template_ids,
            resolve_mixed_zone,
            select_environment_recipe_id,
            select_template_id,
            validate_mixed_template_contract,
        )
    except Exception:  # noqa: BLE001 - never break planning
        return {}
    if not accessory_mixed_template_enabled():
        return {}
    scope_decision = mixed_scope_decision(execution_scope)
    if not scope_decision.get("eligible"):
        return {"scope_rejected": str(scope_decision.get("reason") or "")}
    zone, canonical = resolve_mixed_zone(product_type, top_category)
    if zone is None:
        return {}

    role = _text(item_role).upper()
    candidate_role = "PRIMARY" if role == "STRUCTURE_MOTHER" else "EXECUTION_VARIANT"
    angle = _text(content_angle_key)
    theme_id = angle or f"TH_ITEM_{int(item_index)}"

    # 先选本条要让观众看懂什么，再选择能展示这个主题的模板。
    # The observation vocabulary is owned by the physical subtype; the template
    # decides which of those observations open the film and in what order.  So
    # the template is the axis that has to vary when the index-driven choice
    # would repeat a montage this batch already owns.
    recipe_id = select_environment_recipe_id(int(item_index) - 1)
    identifier = _text(identity) or f"{_text(product_code)}#{int(item_index):02d}"

    try:
        template_ids = list(mixed_template_ids())
    except Exception:  # noqa: BLE001
        template_ids = []
    if not template_ids:
        return {"errors": ["MIXED_CONTRACT_NO_TEMPLATE"]}

    preferred = select_template_id(int(item_index) - 1)
    ordered = [preferred] + [tid for tid in template_ids if tid != preferred]

    # The theme written into the contract is a *readable proposition*.  Callers
    # that own the content bundle resolve it there and hand it over; callers
    # that only have the legacy argument text are still guarded, so an internal
    # ID can never be promoted into the theme slot (package B3 / Review R4).
    declared = dict(theme_proposition or {})
    if declared:
        thesis = _text(declared.get("thesis"))
        thesis_source = _text(declared.get("thesis_source"))
        thesis_source_ref = _text(declared.get("thesis_source_ref"))
        thesis_input_gap = _text(declared.get("thesis_input_gap"))
        argument_id = _text(declared.get("argument_id"))
    else:
        gap_code = "THESIS_NOT_READABLE"
        try:
            from core.accessory_mixed_templates import (
                MIXED_THESIS_INPUT_GAP_READABILITY,
                is_readable_theme_proposition,
            )

            gap_code = MIXED_THESIS_INPUT_GAP_READABILITY
            readable = is_readable_theme_proposition(
                audience_tension_text,
                {"theme_id": theme_id, "parent_theme_id": angle or theme_id},
            )
        except Exception:  # noqa: BLE001 - never break planning on a missing guard
            readable = bool(_text(audience_tension_text))
        if readable:
            thesis = _text(audience_tension_text).strip()
            thesis_source = "AUDIENCE_TENSION_TEXT"
            thesis_source_ref = "bundle.audience_tension.text"
            thesis_input_gap = ""
        else:
            thesis = ""
            thesis_source = ""
            thesis_source_ref = ""
            thesis_input_gap = gap_code
        argument_id = ""

    attempted: List[Dict[str, Any]] = []
    first_contract: Dict[str, Any] = {}
    for template_id in ordered:
        try:
            contract = compile_mixed_template_contract(
                product_type=product_type,
                top_category=top_category,
                template_id=template_id,
                environment_recipe_id=recipe_id,
                product_identity_ref=_text(product_code),
                content_theme={
                    "theme_id": theme_id,
                    "parent_theme_id": angle or theme_id,
                    "candidate_role": candidate_role,
                    "thesis": thesis,
                    "argument_id": argument_id,
                    "thesis_source": thesis_source,
                    "thesis_source_ref": thesis_source_ref,
                    "thesis_input_gap": thesis_input_gap,
                    "approved_claim_refs": [
                        _text(item) for item in (claim_keys or []) if _text(item)
                    ],
                    "evidence_refs": [
                        _text(item) for item in (claim_keys or []) if _text(item)
                    ],
                },
            )
        except Exception as exc:  # noqa: BLE001
            return {"errors": [f"MIXED_CONTRACT_COMPILE_FAILED:{type(exc).__name__}"]}

        errors = validate_mixed_template_contract(contract)
        if errors:
            return {"errors": errors}

        if not first_contract:
            first_contract = contract
        report = judge_mixed_candidate(
            contract,
            [
                *(reserved_references or []),
                *(history_references or []),
            ],
            history_compared=len(history_references or []),
            history_incomplete=int(history_incomplete),
        )
        contract["difference_report"] = report
        if report.get("counts_as_independent", True):
            return {"contract": contract}
        attempted.append(report)

    # Every authored montage repeats something already accepted.  Report the
    # first (the index-driven choice, i.e. the one the batch actually asked for)
    # so the reason matches what a reader would expect.
    blocking = attempted[0] if attempted else {}
    if not blocking:
        return {"contract": first_contract}
    verdict = _text(blocking.get("review_status")).upper()
    reason = (
        "MIXED_EVIDENCE_UNVERIFIED"
        if verdict == "NEEDS_REVIEW"
        else "MIXED_DUPLICATE_CANDIDATE"
    )
    return {
        "rejected": {
            "reason": reason,
            "review_status": verdict,
            "difference_report": blocking,
            "identity": identifier,
            "template_id": _text(first_contract.get("template_id")),
            "signature": first_contract.get(MIXED_SIGNATURE_KEY) or {},
        }
    }


def _mixed_rejection_key(row: Dict[str, Any]) -> Tuple[str, ...]:
    """One rejected mixed candidate's identity inside the deferred report."""

    report = row.get("difference_report")
    report = report if isinstance(report, dict) else {}
    return (
        _text(row.get("direction_assignment_id")),
        _text(row.get("output_slot")),
        _text(row.get("content_bundle_id")),
        _text(row.get("downgrade_reason")),
        _text(report.get("review_status")).upper(),
        "|".join(_text(item) for item in (report.get("difference_dimensions") or [])),
    )


def _dedup_mixed_rejections(
    deferred: Optional[List[Dict[str, Any]]],
    tally: Optional[Dict[str, int]],
) -> None:
    """同一候选被多轮重复尝试时只计一次。

    A round-1 candidate that lost its final-shot comparison leaves no accepted
    item behind, so the next round can put the very same candidate back on the
    table and lose it again for the very same reason.  Recording that twice
    would inflate "重复剔除数" with a single candidate -- exactly the
    double-counting the report is required to avoid.  The list is rewritten in
    place (it is the same object the summary publishes) and the two rejection
    counters are recomputed from the deduplicated rows.
    """

    if not isinstance(deferred, list) or not deferred:
        return
    seen: set = set()
    deduped: List[Dict[str, Any]] = []
    for row in deferred:
        if not isinstance(row, dict) or not _text(
            row.get("downgrade_reason")
        ).startswith("MIXED_"):
            # Non-mixed deferrals keep their own semantics untouched.
            deduped.append(row)
            continue
        key = _mixed_rejection_key(row)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    if len(deduped) != len(deferred):
        deferred[:] = deduped
    if isinstance(tally, dict) and seen:
        tally["duplicate_rejected"] = sum(
            1 for key in seen if key[4] != "NEEDS_REVIEW"
        )
        tally["insufficient_evidence"] = sum(
            1 for key in seen if key[4] == "NEEDS_REVIEW"
        )


def _build_summary(
    items: List[PlanItem],
    structures: List[Dict[str, Any]],
    round_label: str,
    *,
    requested_count: Optional[int] = None,
    deferred_content: Optional[List[Dict[str, Any]]] = None,
    mixed_difference_tally: Optional[Dict[str, int]] = None,
    mixed_history_coverage: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    families = list(set(it.macro_family_key for it in items if it.macro_family_key))
    carriers = list(set(it.carrier_mode for it in items if it.carrier_mode))
    hooks = list(set(it.requested_hook_id for it in items if it.requested_hook_id))
    hook_family_counts = Counter(
        _hook_family(it.requested_hook_id)
        for it in items
        if it.requested_hook_id
    )
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
    retrieval_status_counts: Counter = Counter()
    retrieval_scene_alignment_counts: Counter = Counter()
    retrieval_primary_eligibility_counts: Counter = Counter()
    retrieval_fallback_reason_counts: Counter = Counter()
    retrieval_cross_run_resolution_counts: Counter = Counter()
    retrieval_primary_video_ids: set = set()
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
        retrieval = (
            frozen.get("retrieval_reference_contract")
            if isinstance(frozen.get("retrieval_reference_contract"), dict)
            else {}
        )
        retrieval_status = _text(retrieval.get("status")) or "UNAVAILABLE"
        retrieval_status_counts[retrieval_status] += 1
        scene_alignment = _text(retrieval.get("scene_alignment_status"))
        if scene_alignment:
            retrieval_scene_alignment_counts[scene_alignment] += 1
        primary_case = (
            retrieval.get("primary_case")
            if isinstance(retrieval.get("primary_case"), dict)
            else {}
        )
        primary_eligibility = _text(primary_case.get("eligibility_status"))
        if primary_eligibility:
            retrieval_primary_eligibility_counts[primary_eligibility] += 1
        fallback_reason = _text(retrieval.get("fallback_reason"))
        if fallback_reason:
            retrieval_fallback_reason_counts[fallback_reason] += 1
        cross_run_resolution = _text(retrieval.get("cross_run_resolution"))
        if cross_run_resolution:
            retrieval_cross_run_resolution_counts[cross_run_resolution] += 1
        primary_video_id = _text(
            primary_case.get("video_id")
        )
        if primary_video_id:
            retrieval_primary_video_ids.add(primary_video_id)
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
    # 数量报告必须区分请求数、独立主题数、执行变体数、重复剔除数、证据不足数和实际可用数；
    # 计数不要重复累计同一候选（每条候选只落进一个桶，且只在通过比较后计入）。
    _dedup_mixed_rejections(deferred_content, mixed_difference_tally)
    tally = mixed_difference_tally or {}
    distinct_theme = int(tally.get("distinct_theme", 0))
    execution_variant = int(tally.get("execution_variant", 0))
    duplicate_rejected = int(tally.get("duplicate_rejected", 0))
    insufficient_evidence = int(tally.get("insufficient_evidence", 0))
    uncompared = int(tally.get("uncompared", 0))
    mixed_usable = distinct_theme + execution_variant
    outfit_provider_snapshot = get_outfit_template_provider_snapshot()
    persona_provider_snapshot = load_persona_templates()
    return {
        "policy_version": MODEL_POLICY_VERSION,
        "allocation_round": round_label,
        "allocation_status": allocation_status,
        "requested_count": requested,
        "planned_count": planned,
        "shortage_count": max(0, requested - planned),
        "mixed_distinct_theme_count": distinct_theme,
        "mixed_execution_variant_count": execution_variant,
        "mixed_duplicate_rejected_count": duplicate_rejected,
        "mixed_insufficient_evidence_count": insufficient_evidence,
        # 同商品历史存在但读不出可比签名（版本不同 / 未写最终镜头）时的条数。
        # 单列出来是因为它既不是"独立主题"，也不是"重复"，更不是"没历史"。
        "mixed_uncompared_count": uncompared,
        "mixed_usable_count": mixed_usable,
        "mixed_history_coverage": dict(mixed_history_coverage or {}),
        "mixed_shortage_reason": (
            "DIFFERENCE_INSUFFICIENT"
            if planned < requested and (duplicate_rejected or insufficient_evidence)
            else "CONTENT_CAPACITY"
        ),
        "deferred_content": list(deferred_content or []),
        "structure_count": len(structures),
        "unique_families": len(families),
        "unique_carriers": len(carriers),
        "unique_hooks": len(hooks),
        "unique_hook_families": len(hook_family_counts),
        "hook_family_distribution": dict(hook_family_counts),
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
        "retrieval_reference_status_distribution": dict(
            retrieval_status_counts
        ),
        "retrieval_scene_alignment_distribution": dict(
            retrieval_scene_alignment_counts
        ),
        "retrieval_primary_eligibility_distribution": dict(
            retrieval_primary_eligibility_counts
        ),
        "retrieval_fallback_reason_distribution": dict(
            retrieval_fallback_reason_counts
        ),
        "retrieval_cross_run_resolution_distribution": dict(
            retrieval_cross_run_resolution_counts
        ),
        "unique_retrieval_primary_videos": len(retrieval_primary_video_ids),
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
