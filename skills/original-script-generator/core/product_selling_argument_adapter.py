"""Read-only adapter for governed product selling arguments.

Feishu-confirmed operator segments are the semantic authority for whether a
selling argument may be used.  Central claim concepts enrich those segments
with type, strength and carrier hints; a missing concept must not silently drop
an operator-confirmed selling point from original-video planning.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

from core.product_claim_storage import (
    claim_store_descriptor,
    connect_claim_store,
    table_columns,
)
from core.product_type_resolution import normalize_product_type


DEFAULT_VOICEOVER_ROOT = Path("/Users/likeu3/voiceover_copy_engine")
SELLING_ARGUMENT_CATALOG_VERSION = "selling-argument-catalog-v10-operator-argument-v2"
ARGUMENT_CLAIM_TYPES = frozenset(
    {"benefit", "visual_result", "scenario", "audience"}
)
FEISHU_OPERATOR_SOURCE_PREFIX = "feishu-product-claims:"
FLEXIBLE_CARRIER_REQUIREMENT = "FLEXIBLE"
KNOWN_CARRIER_REQUIREMENTS = frozenset(
    {"WEARER_REQUIRED", "HAND_REQUIRED", "STATIC_REQUIRED", FLEXIBLE_CARRIER_REQUIREMENT}
)

_SCARF_TYPES = {"scarf", "winter_scarf", "silk_scarf", "headscarf"}
_WRIST_TYPES = {"bracelet", "bangle", "slim_bangle"}
_FINGER_TYPES = {"ring"}
_HAIR_TYPES = {
    "hair_accessory_generic", "claw_clip", "hair_clip", "headband",
    "scrunchie", "hair_tie", "ribbon", "hair_pin",
}
_SCARF_USAGE_CONCEPTS = {
    "CCP_SCARF_HAIR_RESCUE": {
        "argument_theme": "HAIR_RESCUE",
        "preferred_hook_ids": [
            "PAIN_REFRAME", "AUDIENCE_NEED_CALLOUT",
            "DISCOVERY_RESULT_PROMISE", "GENERAL_PRODUCT_SHARE",
        ],
        "hook_tension_authority": "CENTRAL_CONCEPT",
        "primary_demonstration_mode": "HAIR_TIE",
        "supported_demonstration_modes": ["HAIR_TIE", "HEAD_WORN", "NECK_WORN"],
        "evidence_mode": "VISUAL_RESULT_WITH_AUTHORIZED_VOICEOVER",
    },
    "CCP_HEADSCARF_SUN_SHADE": {
        "argument_theme": "SUN_SHADE",
        "preferred_hook_ids": [
            "AUDIENCE_NEED_CALLOUT", "USER_ADVOCACY_STANCE",
            "GENERAL_PRODUCT_SHARE",
        ],
        "hook_tension_authority": "CENTRAL_CONCEPT",
        "primary_demonstration_mode": "HEAD_WORN",
        "supported_demonstration_modes": ["HEAD_WORN"],
        "evidence_mode": "AUTHORIZED_VOICEOVER_WITH_VISIBLE_USAGE",
    },
    "CCP_SCARF_MULTI_USE": {
        "argument_theme": "MULTI_USE",
        "preferred_hook_ids": [
            "USER_ADVOCACY_STANCE", "DISCOVERY_RESULT_PROMISE",
            "GENERAL_PRODUCT_SHARE",
        ],
        "evidence_mode": "AUTHORIZED_VOICEOVER_WITH_ONE_VISIBLE_USAGE",
    },
    "CCP_SCARF_SUMMER_COMFORT": {
        "argument_theme": "SUMMER_COMFORT",
        "preferred_hook_ids": [
            "AUDIENCE_NEED_CALLOUT", "USER_ADVOCACY_STANCE",
            "GENERAL_PRODUCT_SHARE",
        ],
        "hook_tension_authority": "CENTRAL_CONCEPT",
        "evidence_mode": "AUTHORIZED_VOICEOVER",
    },
    "CCP_SCARF_COLOR_MOOD": {
        "argument_theme": "COLOR_MOOD",
        "preferred_hook_ids": [
            "VISUAL_RESULT_DIRECT", "DETAIL_SURPRISE",
            "GENERAL_PRODUCT_SHARE",
        ],
        "evidence_mode": "VISUAL_RESULT",
    },
    "CCP_SCARF_SURFACE_GLOSS": {
        "argument_theme": "SURFACE_GLOSS",
        "preferred_hook_ids": [
            "DETAIL_SURPRISE", "VISUAL_RESULT_DIRECT",
            "GENERAL_PRODUCT_SHARE",
        ],
        "evidence_mode": "PRODUCT_DETAIL",
    },
    "CCP_SCARF_LOW_STATIC": {
        "argument_theme": "LOW_STATIC",
        "preferred_hook_ids": [
            "USER_ADVOCACY_STANCE", "DISCOVERY_RESULT_PROMISE",
            "GENERAL_PRODUCT_SHARE",
        ],
        "evidence_mode": "AUTHORIZED_VOICEOVER",
    },
}

_SCARF_WORN_USAGE_THEMES = frozenset({
    "HAIR_RESCUE",
    "SUN_SHADE",
    "MULTI_USE",
    "SUMMER_COMFORT",
    "LOW_STATIC",
})
_SCARF_WORN_DEMONSTRATION_MODES = frozenset({
    "HEAD_WORN", "NECK_WORN", "HAIR_TIE",
})
_SCARF_MULTI_USE_SCOPED_VALUES = {
    "NECK_WORN": "一条丝巾可以灵活变化用法，这次用作颈部点缀",
    "HEAD_WORN": "一条丝巾可以灵活变化用法，这次用作头部造型",
    "HAIR_TIE": "一条丝巾可以灵活变化用法，这次用作头发点缀",
    "BAG_ACCENT": "一条丝巾可以灵活变化用法，这次用作包袋点缀",
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _stable_hash(material: Any) -> str:
    raw = json.dumps(material, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


_RESPECTFUL_REFRAME_MARKERS = (
    "农村妇女", "乡下人", "乡巴佬", "土包子", "村妇", "土鳖",
    "虚荣心", "爱慕虚荣", "穷人", "穷酸", "穷鬼", "贵妇脸",
    "肥婆", "死胖子", "丑女", "丑八怪",
)


def _requires_respectful_reframe(*values: Any) -> bool:
    """Flag explicit stigmatizing language, not ordinary selling points.

    Terms such as 遮肉, 显瘦, 空调房外搭 or 显贵 remain operator-authorised
    semantics.  Reframing is reserved for an actual insulting/classist label.
    """

    def flatten(value: Any) -> str:
        if isinstance(value, Mapping):
            return " ".join(flatten(item) for item in value.values())
        if isinstance(value, (list, tuple, set)):
            return " ".join(flatten(item) for item in value)
        return _text(value)

    material = " ".join(flatten(value) for value in values).lower()
    return any(marker.lower() in material for marker in _RESPECTFUL_REFRAME_MARKERS)


def _claims_db_path(voiceover_root: str = "") -> Path:
    configured = _text(os.environ.get("ORIGINAL_SCRIPT_CLAIMS_DB_PATH"))
    if configured:
        return Path(configured).expanduser()
    root = Path(voiceover_root).expanduser() if _text(voiceover_root) else DEFAULT_VOICEOVER_ROOT
    return root / "var" / "voiceover.sqlite"


def _carrier_policy(claim_type: str, claim_theme: str = "") -> Dict[str, Any]:
    """Describe how an authorised value is best shown, not whether it is true.

    A visual result (fit, proportion or silhouette) needs a worn execution for
    the visual to carry that result. A benefit remains flexible: its authority
    comes from the central library and static presentation may still suit an
    ordinary use-case message. Keeping just these two states avoids creating a
    second claim-review system in the original-script workflow.
    """

    normalized_claim_type = _text(claim_type).lower()
    if normalized_claim_type == "visual_result":
        return {
            "proof_subject": "ON_BODY_RESULT",
            "visual_dependency": "WEARER_REQUIRED",
            "compatible_carriers": ["WEARER_ACTIVE", "MIXED"],
        }
    if normalized_claim_type == "scenario":
        return {
            "proof_subject": "SCENE_USAGE",
            "visual_dependency": "WEARER_REQUIRED",
            "compatible_carriers": ["WEARER_ACTIVE", "MIXED"],
        }
    normalized_theme = _text(claim_theme).lower()
    if normalized_theme in {
        "scene_usage", "usage_scene", "usage_scenario", "occasion", "multi_occasion",
        "commute", "travel", "photo_scene",
    }:
        return {
            "proof_subject": "SCENE_USAGE",
            "visual_dependency": "WEARER_REQUIRED",
            "compatible_carriers": ["WEARER_ACTIVE", "MIXED"],
        }
    if normalized_theme in {"product_detail", "detail", "construction", "hardware", "material"}:
        return {
            "proof_subject": "PRODUCT_DETAIL",
            "visual_dependency": "FLEXIBLE",
            "compatible_carriers": [],
        }
    return {
        "proof_subject": "GENERAL_EXPRESSION",
        "visual_dependency": "FLEXIBLE",
        # Empty means the message remains authorised across carriers; it does
        # not mean the argument is unavailable.
        "compatible_carriers": [],
    }


def normalized_proof_subject(argument: Mapping[str, Any] | None) -> str:
    """Return the governed proof subject without guessing from sales copy."""

    value = _text((argument or {}).get("proof_subject")).upper()
    if value in {"ON_BODY_RESULT", "SCENE_USAGE", "PRODUCT_DETAIL", "GENERAL_EXPRESSION"}:
        return value
    claim_type = _text((argument or {}).get("claim_type")).lower()
    if claim_type == "visual_result":
        return "ON_BODY_RESULT"
    return "GENERAL_EXPRESSION"


def normalized_carrier_requirement(argument: Mapping[str, Any] | None) -> str:
    """Return the central semantic carrier requirement without reading copy.

    The original-video workflow is not allowed to infer a carrier from words
    such as "显瘦" or "上镜".  It consumes only the structured requirement
    emitted by the central claim adapter.  Missing/legacy metadata stays
    flexible so an unknown mapping cannot silently remove an operator-approved
    selling argument.
    """

    value = _text((argument or {}).get("visual_dependency")).upper()
    return value if value in KNOWN_CARRIER_REQUIREMENTS else FLEXIBLE_CARRIER_REQUIREMENT


def compatible_structure_carriers(argument: Mapping[str, Any] | None) -> List[str]:
    """Return the normalized structure carriers for a known requirement."""

    requirement = normalized_carrier_requirement(argument)
    defaults = {
        "WEARER_REQUIRED": ["WEARER_ACTIVE", "MIXED"],
        "HAND_REQUIRED": ["HAND_ONLY", "HANDS_ONLY", "MIXED"],
        "STATIC_REQUIRED": ["STATIC_PRODUCT", "MIXED"],
        FLEXIBLE_CARRIER_REQUIREMENT: [],
    }
    if requirement == FLEXIBLE_CARRIER_REQUIREMENT:
        # Legacy rows sometimes carry a preference list without an explicit
        # dependency.  Preference is not a hard requirement and must not make
        # an unknown/flexible operator value disappear from planning.
        return []
    explicit = [
        _text(value).upper()
        for value in (argument or {}).get("compatible_carriers", [])
        if _text(value)
    ]
    return list(dict.fromkeys(explicit or defaults[requirement]))


def _operator_expression(source_span: Any) -> str:
    """Return the reviewed operator sentence without spreadsheet numbering."""

    value = _text(source_span)
    value = re.sub(r"^\s*(?:\d+\s*[、./.)）:-]\s*)+", "", value)
    return value.strip(" \t\r\n；;。")


def _operator_source_order(source_span: Any) -> int:
    value = _text(source_span)
    matched = re.search(r"#argument-v2-(\d+)$", value)
    if matched:
        return int(matched.group(1))
    matched = re.search(r"#segment-(\d+)$", value)
    if matched:
        return int(matched.group(1))
    matched = re.match(r"^\s*(\d+)\s*[、./.)）:-]", value)
    return int(matched.group(1)) if matched else 9999


def _prefer_segmented_operator_sources(
    rows: Iterable[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Consume the newest, most specific Feishu argument generation.

    Feishu sync used to store one row containing every numbered selling point.
    It later stored ``#segment-N`` and now stores the more capable
    ``#argument-v2-N`` sources.  Once a V2 generation exists for a Feishu row,
    older whole-cell and numbered generations must not re-enter the catalog.
    Re-syncs can also leave two source ids under the same source_ref after the
    operator edits text; only the newest source id is active.  Independent
    Feishu rows remain independent even when they map to the same concept.
    """

    material = [dict(row) for row in rows]
    if not material:
        return []

    def base_ref(value: Any) -> str:
        return re.sub(
            r"#(?:argument-v2|segment)-\d+$", "", _text(value)
        )

    generation_by_base: Dict[str, str] = {}
    for row in material:
        ref = _text(row.get("source_ref"))
        base = base_ref(ref)
        generation = (
            "V2" if re.search(r"#argument-v2-\d+$", ref)
            else "SEGMENT" if re.search(r"#segment-\d+$", ref)
            else "WHOLE"
        )
        previous = generation_by_base.get(base, "WHOLE")
        rank = {"WHOLE": 0, "SEGMENT": 1, "V2": 2}
        if rank[generation] > rank[previous]:
            generation_by_base[base] = generation
        else:
            generation_by_base.setdefault(base, previous)

    filtered: List[Dict[str, Any]] = []
    for row in material:
        ref = _text(row.get("source_ref"))
        wanted = generation_by_base.get(base_ref(ref), "WHOLE")
        current = (
            "V2" if re.search(r"#argument-v2-\d+$", ref)
            else "SEGMENT" if re.search(r"#segment-\d+$", ref)
            else "WHOLE"
        )
        if current == wanted:
            filtered.append(row)

    newest_source_by_ref: Dict[str, tuple[str, str]] = {}
    for row in filtered:
        ref = _text(row.get("source_ref"))
        candidate = (
            _text(row.get("source_created_at")),
            _text(row.get("claim_source_id")),
        )
        if candidate > newest_source_by_ref.get(ref, ("", "")):
            newest_source_by_ref[ref] = candidate
    return [
        row for row in filtered
        if _text(row.get("claim_source_id"))
        == newest_source_by_ref.get(_text(row.get("source_ref")), ("", ""))[1]
    ]


def _operator_catalog_entries(source_id: str, source_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return one or more speech-safe operator argument rows.

    A single reviewed Feishu segment can normalize to several central
    concepts.  For a 15-second original video those concepts must rotate as
    separate arguments; otherwise the central voiceover sees the full raw
    sentence and naturally speaks multiple unrelated benefits in one script.
    """

    if not source_rows:
        return []
    first = source_rows[0]
    operator_expression = _operator_expression(first.get("raw_text"))
    if not operator_expression:
        return []
    normalized_rows = [row for row in source_rows if _text(row.get("claim_id"))]
    mapped_rows = [
        row for row in normalized_rows
        if _text(row.get("concept_id"))
        and _text(row.get("claim_type")).lower() in ARGUMENT_CLAIM_TYPES
    ]
    distinct_concepts = {
        _text(row.get("concept_id")) for row in mapped_rows if _text(row.get("concept_id"))
    }
    if len(distinct_concepts) <= 1:
        best = max(
            normalized_rows,
            key=lambda row: (
                1 if _text(row.get("concept_id")) else 0,
                1 if _text(row.get("verification_status")) == "VERIFIED" else 0,
                1 if _text(row.get("claim_type")).lower() in ARGUMENT_CLAIM_TYPES else 0,
                float(row.get("normalizer_confidence") or 0.0),
                _text(row.get("created_at")),
            ),
            default={},
        )
        return [{
            "row": best,
            "value_suffix": "",
            "operator_expression": operator_expression,
            "source_operator_expression": operator_expression,
            "source_scope_concept_count": max(1, len(distinct_concepts)),
            "primary_selling_point": operator_expression,
            "source_claim_ids": [
                _text(row.get("claim_id"))
                for row in normalized_rows
                if _text(row.get("verification_status")) == "VERIFIED"
                and _text(row.get("claim_id"))
            ],
            "normalization_claim_ids": [
                _text(row.get("claim_id"))
                for row in normalized_rows
                if _text(row.get("claim_id"))
            ],
            "concept_ids": list(dict.fromkeys(
                _text(row.get("concept_id"))
                for row in normalized_rows
                if _text(row.get("concept_id"))
            )),
        }]

    entries: List[Dict[str, Any]] = []
    for row in mapped_rows:
        claim_id = _text(row.get("claim_id"))
        canonical = _text(row.get("canonical_claim_zh"))
        if not claim_id or not canonical:
            continue
        entries.append({
            "row": row,
            "value_suffix": "_" + claim_id,
            "operator_expression": canonical,
            # Keep the complete, separately confirmed operator point as
            # rhetoric context.  The canonical expression remains the only
            # one-argument mainline, so this cannot recombine several claims.
            "source_operator_expression": operator_expression,
            "source_scope_concept_count": max(1, len(distinct_concepts)),
            "primary_selling_point": canonical,
            "source_claim_ids": [claim_id] if _text(row.get("verification_status")) == "VERIFIED" else [],
            "normalization_claim_ids": [claim_id],
            "concept_ids": [_text(row.get("concept_id"))],
        })
    return entries


def _scarf_execution_semantics(
    product_type: str,
    concept_ids: Iterable[Any],
) -> Dict[str, Any]:
    """Translate central taxonomy into one 15-second execution decision.

    This reads concept IDs only.  Raw operator copy stays authoritative for
    wording, while the original-video flow consumes the structured semantic
    result without keyword guessing or a second claim-review system.
    """

    resolved = normalize_product_type(product_type, "配饰")
    if resolved.canonical_type not in _SCARF_TYPES:
        return {}
    default_mode = "HEAD_WORN" if resolved.canonical_type == "headscarf" else "NECK_WORN"
    ordered = [str(value or "").strip() for value in concept_ids if str(value or "").strip()]
    selected: Dict[str, Any] = {}
    for concept_id in (
        "CCP_SCARF_HAIR_RESCUE",
        "CCP_HEADSCARF_SUN_SHADE",
        "CCP_SCARF_MULTI_USE",
        "CCP_SCARF_SUMMER_COMFORT",
        "CCP_SCARF_COLOR_MOOD",
        "CCP_SCARF_SURFACE_GLOSS",
        "CCP_SCARF_LOW_STATIC",
    ):
        if concept_id in ordered:
            selected = dict(_SCARF_USAGE_CONCEPTS[concept_id])
            break
    if not selected:
        selected = {
            "argument_theme": "GENERAL_SCARF_VALUE",
            "preferred_hook_ids": [
                "VISUAL_RESULT_DIRECT", "USER_ADVOCACY_STANCE",
                "GENERAL_PRODUCT_SHARE",
            ],
            "primary_demonstration_mode": default_mode,
            "supported_demonstration_modes": [default_mode],
            "evidence_mode": "AUTHORIZED_VOICEOVER_OR_VISIBLE_RESULT",
            "demonstration_policy": "ONE_PRIMARY_MODE_PER_15S",
        }
    elif selected.get("argument_theme") == "MULTI_USE":
        selected["primary_demonstration_mode"] = default_mode
        selected["supported_demonstration_modes"] = (
            ["HEAD_WORN", "HAIR_TIE", "NECK_WORN"]
            if resolved.canonical_type == "headscarf"
            else ["NECK_WORN", "HAIR_TIE", "BAG_ACCENT"]
        )
    elif (
        selected.get("argument_theme") == "HAIR_RESCUE"
        and resolved.canonical_type == "headscarf"
    ):
        # A headscarf solves the same hair-state need through an already worn
        # head look; do not silently reinterpret the product as a thin hair tie.
        selected["primary_demonstration_mode"] = "HEAD_WORN"
        selected["supported_demonstration_modes"] = ["HEAD_WORN", "HAIR_TIE"]
    else:
        selected.setdefault("primary_demonstration_mode", default_mode)
        selected.setdefault("supported_demonstration_modes", [default_mode])

    primary_mode = _text(selected.get("primary_demonstration_mode")).upper()
    argument_theme = _text(selected.get("argument_theme")).upper()
    if (
        argument_theme in _SCARF_WORN_USAGE_THEMES
        and primary_mode in _SCARF_WORN_DEMONSTRATION_MODES
    ):
        # A use-case that has already selected an on-body demonstration cannot
        # be fulfilled by a product-only mother structure.  This is a planning
        # semantic, not a new copy validator or a claim-authority decision.
        selected.update({
            "proof_subject": (
                "SCENE_USAGE"
                if argument_theme in {"SUN_SHADE", "SUMMER_COMFORT", "MULTI_USE"}
                else "ON_BODY_RESULT"
            ),
            "visual_dependency": "WEARER_REQUIRED",
            "compatible_carriers": [
                "WEARER_ACTIVE", "PERSON_ON_CAMERA", "MIXED",
            ],
        })
    if argument_theme == "MULTI_USE":
        scoped_value = _SCARF_MULTI_USE_SCOPED_VALUES.get(primary_mode, "")
        if scoped_value:
            # Preserve the reviewed source text in operator_expression, while
            # giving this one 15-second item a single executable speech scope.
            selected["voiceover_core_value"] = scoped_value
            selected["scoped_creative_core_value"] = scoped_value
            selected["voiceover_scope_policy"] = (
                "PRIMARY_DEMONSTRATION_MODE_ONLY"
            )
    selected["demonstration_policy"] = "ONE_PRIMARY_MODE_PER_15S"
    return selected


def _explicit_wrist_stack_quantity(operator_expression: str) -> Dict[str, Any]:
    """Return a same-SKU display quantity only when the operator wrote one.

    ``叠戴`` by itself does not authorize the generator to duplicate a product.
    A nearby explicit two/three count does.  The highest authorized count is
    frozen for the whole clip so the video never changes quantity mid-shot.
    """

    text = _text(operator_expression)
    marker_at = text.find("叠戴")
    if marker_at < 0:
        return {}
    window = text[max(0, marker_at - 8): marker_at + 10]
    range_match = re.search(
        r"(?:两|二|2)(?:到|至|[-—~～、]?)(?:三|3)个?",
        window,
    )
    if range_match:
        minimum, maximum = 2, 3
    elif re.search(r"(?:三|3)个?", window):
        minimum = maximum = 3
    elif re.search(r"(?:两|二|2)个?", window):
        minimum = maximum = 2
    else:
        return {}
    return {
        "status": "AUTHORIZED",
        "mode": "SAME_SKU_STACK",
        "min_display_count": minimum,
        "max_display_count": maximum,
        "required_display_count": maximum,
        "continuity": "SAME_COUNT_THROUGHOUT_VIDEO",
        "authority": "EXPLICIT_OPERATOR_QUANTITY",
    }


def _accessory_operator_execution_semantics(
    product_type: str,
    operator_expression: str,
) -> Dict[str, Any]:
    """Convert explicit wrist-use wording into production semantics.

    The operator sentence remains the content authority.  This narrow adapter
    does not judge whether the benefit is true and does not rewrite it; it only
    prevents an explicitly described wearing result or put-on process from
    being assigned to a static-product structure that cannot show it.  Unknown
    or detail-only wording remains flexible.
    """

    resolved = normalize_product_type(product_type, "配饰")
    if resolved.canonical_type not in _WRIST_TYPES:
        return {}
    text = _text(operator_expression)
    if not text:
        return {}
    process_markers = (
        "一滑就", "滑进去", "套进去", "套入", "戴进去", "戴上很方便",
        "容易戴", "方便佩戴", "开合佩戴",
    )
    worn_result_markers = (
        "佩戴", "戴着", "戴起来", "单戴", "叠戴", "上腕", "手腕",
        "日常干活", "日常做事",
    )
    if any(marker in text for marker in process_markers):
        return {
            "proof_subject": "ON_BODY_RESULT",
            "visual_dependency": "HAND_REQUIRED",
            "compatible_carriers": ["HAND_ONLY", "HANDS_ONLY", "MIXED"],
            "preferred_action_mode": "SIMPLE_WEAR_PROCESS",
            "execution_semantics_source": "EXPLICIT_OPERATOR_WRIST_PROCESS",
        }
    if any(marker in text for marker in worn_result_markers):
        result = {
            "proof_subject": "ON_BODY_RESULT",
            "visual_dependency": "HAND_REQUIRED",
            "compatible_carriers": [
                "HAND_ONLY", "HANDS_ONLY", "WEARER_ACTIVE",
                "PERSON_ON_CAMERA", "MIXED",
            ],
            "preferred_action_mode": "RESULT_SHOW",
            "execution_semantics_source": "EXPLICIT_OPERATOR_WRIST_RESULT",
        }
        quantity_contract = _explicit_wrist_stack_quantity(text)
        if quantity_contract:
            result["display_quantity_contract"] = quantity_contract
        return result
    return {}


def _small_accessory_claim_action_semantics(
    product_type: str,
    operator_expression: str,
    claim_theme: str = "",
) -> Dict[str, Any]:
    """Compile an approved small-accessory value into a proof-action intent.

    This narrow planning adapter never approves, rejects or rewrites the
    operator's selling point. It only prevents values such as ``可调节`` or
    ``拿在手里有分量`` from reaching an execution that cannot show them.
    Unknown wording stays on the existing flexible path.
    """

    enabled = _text(
        os.environ.get("ORIGINAL_SCRIPT_SMALL_ACCESSORY_CLAIM_ACTION_V1", "1")
    ).lower()
    if enabled in {"0", "false", "off", "no"}:
        return {}

    resolved = normalize_product_type(product_type, "配饰")
    canonical = resolved.canonical_type
    if canonical not in {*_FINGER_TYPES, *_HAIR_TYPES, "earring"}:
        return {}
    text = _text(operator_expression).lower()
    theme = _text(claim_theme).lower()
    if not text and not theme:
        return {}

    if canonical in _FINGER_TYPES and any(marker in text for marker in (
        "可调节", "调节大小", "调节尺寸", "开口戒", "开口设计",
        "粗细手指", "不同手指", "戒围", "松紧", "adjustable", "open ring",
    )):
        return {
            "proof_action_intent": "SIZE_ADJUSTMENT",
            "preferred_action_mode": "ADJUST_THEN_WEAR",
            "required_proof_relation": "先清楚展示一次小幅调节，再保留稳定的手指佩戴结果",
            "proof_subject": "ON_BODY_RESULT",
            "visual_dependency": "HAND_REQUIRED",
            "compatible_carriers": [
                "HAND_ONLY", "HANDS_ONLY", "PERSON_ON_CAMERA",
                "WEARER_ACTIVE", "MIXED",
            ],
            "execution_semantics_source": "EXPLICIT_OPERATOR_SMALL_ACCESSORY_ACTION",
        }

    if any(marker in text for marker in (
        "重量", "分量", "压手", "拿在手", "手感", "金属感",
        "weight", "weighted", "in hand", "metal feel",
    )):
        return {
            "proof_action_intent": "HANDHELD_MATERIAL_FEEL",
            "preferred_action_mode": "HANDHELD_PRODUCT",
            "required_proof_relation": "商品先由同一人物自然拿在手中看清，再进入佩戴或使用结果",
            "proof_subject": "PRODUCT_DETAIL",
            "visual_dependency": "HAND_REQUIRED",
            "compatible_carriers": [
                "HAND_ONLY", "HANDS_ONLY", "PERSON_ON_CAMERA",
                "WEARER_ACTIVE", "MIXED",
            ],
            "execution_semantics_source": "EXPLICIT_OPERATOR_SMALL_ACCESSORY_ACTION",
        }

    scene_theme = theme in {
        "scene_usage", "usage_scene", "usage_scenario", "occasion",
        "multi_occasion", "commute", "travel", "photo_scene",
    }
    scene_wording = any(marker in text for marker in (
        "通勤", "上班", "办公室", "聚会", "约会", "出门", "日常",
        "多场景", "多种场合", "work", "office", "commute", "party", "daily",
    ))
    if scene_theme or scene_wording:
        return {
            "proof_action_intent": "SCENE_USAGE",
            "preferred_action_mode": "SCENE_USE",
            "required_proof_relation": "让商品佩戴结果与一个真实使用场景及相应造型同时成立",
            "proof_subject": "SCENE_USAGE",
            "visual_dependency": "WEARER_REQUIRED",
            "compatible_carriers": [
                "PERSON_ON_CAMERA", "WEARER_ACTIVE", "MIXED",
            ],
            "execution_semantics_source": "EXPLICIT_OPERATOR_SMALL_ACCESSORY_ACTION",
        }

    if any(marker in text for marker in (
        "显白", "提亮", "闪", "光泽", "修饰脸型", "精致", "上手效果",
        "佩戴效果", "好看", "brighten", "sparkle", "worn result",
    )):
        visual_carriers = [
            "PERSON_ON_CAMERA", "WEARER_ACTIVE", "MIXED",
        ]
        if canonical in _FINGER_TYPES:
            visual_carriers.extend(["HAND_ONLY", "HANDS_ONLY"])
        return {
            "proof_action_intent": "WORN_VISUAL_RESULT",
            "preferred_action_mode": "RESULT_SHOW",
            "required_proof_relation": "从已经佩戴完成的状态展示商品与对应身体区域的视觉结果",
            "proof_subject": "ON_BODY_RESULT",
            "visual_dependency": "WEARER_REQUIRED",
            "compatible_carriers": visual_carriers,
            "execution_semantics_source": "EXPLICIT_OPERATOR_SMALL_ACCESSORY_ACTION",
        }
    return {}


def load_verified_selling_point_catalog(
    product_code: str,
    *,
    voiceover_root: str = "",
    product_type: str = "",
) -> Dict[str, Any]:
    """Return a deterministic catalog of operator-confirmed selling arguments.

    Every numbered Feishu segment that has already passed operator confirmation
    becomes one selling argument.  A mapped central claim contributes optional
    governance metadata.  Legacy non-operator VERIFIED benefit/visual-result
    rows remain available as a backward-compatible fallback.
    """

    db_path = _claims_db_path(voiceover_root)
    descriptor = claim_store_descriptor(
        db_path,
        explicit_sqlite=bool(_text(os.environ.get("ORIGINAL_SCRIPT_CLAIMS_DB_PATH"))),
    )
    base = {
        "catalog_version": SELLING_ARGUMENT_CATALOG_VERSION,
        "source": "CENTRAL_VOICEOVER_OPERATOR_ARGUMENTS",
        "provider": descriptor["provider"],
        "storage_backend": descriptor["backend"],
        "db_path": str(db_path) if descriptor["backend"] == "sqlite" else "",
        "product_code": product_code,
        "status": "UNAVAILABLE",
        "catalog": [],
        "evidence_claims": [],
        "confirmed_argument_count": 0,
        "mapped_argument_count": 0,
        "unmapped_argument_count": 0,
        "available_argument_count": 0,
    }
    if descriptor["backend"] == "unavailable":
        base["error"] = descriptor.get("reason", "CLAIM_STORE_UNAVAILABLE")
        base["snapshot_hash"] = _stable_hash(base)
        return base

    try:
        with connect_claim_store(descriptor) as conn:
            source_columns = table_columns(conn, "product_claim_sources")
            claim_columns = table_columns(conn, "product_claims")
            operator_rows = []
            if {"claim_source_id", "product_id", "raw_text", "source_type", "source_ref"}.issubset(source_columns):
                source_priority = (
                    "s.operator_priority" if "operator_priority" in source_columns else "'normal'"
                )
                source_created = "s.created_at" if "created_at" in source_columns else "''"
                confidence = (
                    "c.normalizer_confidence" if "normalizer_confidence" in claim_columns else "0.0"
                )
                operator_rows = conn.execute(
                    f"""
                    SELECT s.claim_source_id, s.raw_text, s.source_type,
                           s.source_ref, {source_priority} AS source_operator_priority,
                           {source_created} AS source_created_at,
                           c.claim_id, c.concept_id, c.source_span,
                           c.canonical_claim_zh, c.claim_type, c.claim_theme,
                           c.verification_status, c.evidence_requirement,
                           c.allowed_strength, c.operator_priority,
                           c.updated_at, c.created_at,
                           {confidence} AS normalizer_confidence
                    FROM product_claim_sources s
                    LEFT JOIN product_claims c
                      ON c.claim_source_id=s.claim_source_id
                    WHERE s.product_id=?
                      AND s.source_type='operator_input'
                      AND s.source_ref LIKE ?
                    ORDER BY s.claim_source_id, c.created_at, c.claim_id
                    """,
                    (product_code, FEISHU_OPERATOR_SOURCE_PREFIX + "%"),
                ).fetchall()
            rows = conn.execute(
                """
                SELECT c.claim_id, c.claim_source_id, c.concept_id, c.source_span,
                       c.canonical_claim_zh, c.claim_type, c.claim_theme,
                       c.verification_status, c.evidence_requirement,
                       c.allowed_strength, c.operator_priority, c.updated_at,
                       c.created_at, s.source_type, s.source_ref
                FROM product_claims c
                JOIN product_claim_sources s
                  ON s.claim_source_id=c.claim_source_id
                WHERE c.product_id=? AND c.verification_status='VERIFIED'
                ORDER BY
                  CASE s.source_type WHEN 'operator_input' THEN 0 ELSE 1 END,
                  CASE c.operator_priority WHEN 'core' THEN 0 WHEN 'normal' THEN 1 ELSE 2 END,
                  c.created_at ASC, c.claim_id ASC
                """,
                (product_code,),
            ).fetchall()
    except Exception as exc:
        base["status"] = "READ_ERROR"
        base["error"] = f"{type(exc).__name__}:{exc}"[:240]
        base["snapshot_hash"] = _stable_hash(base)
        return base

    operator_material = [dict(row) for row in operator_rows]
    active_operator_rows = _prefer_segmented_operator_sources(operator_material)
    active_operator_source_ids = {
        _text(row.get("claim_source_id")) for row in active_operator_rows
        if _text(row.get("claim_source_id"))
    }
    # Superseded whole-cell sources must also be excluded from the later
    # generic VERIFIED-claim pass; otherwise they re-enter without operator
    # lineage and recreate the same duplicate catalog entries.
    superseded_operator_source_ids = {
        _text(row.get("claim_source_id")) for row in operator_material
        if _text(row.get("claim_source_id")) not in active_operator_source_ids
    }
    operator_groups: Dict[str, List[Dict[str, Any]]] = {}
    for material in active_operator_rows:
        operator_groups.setdefault(_text(material.get("claim_source_id")), []).append(material)

    included_operator_sources = set(superseded_operator_source_ids)
    ordered_operator_groups = sorted(
        operator_groups.items(),
        key=lambda pair: (
            _operator_source_order(pair[1][0].get("source_ref") if pair[1] else ""),
            _operator_source_order(pair[1][0].get("raw_text") if pair[1] else ""),
            _text(pair[1][0].get("source_created_at") if pair[1] else ""),
            pair[0],
        ),
    )
    for source_id, source_rows in ordered_operator_groups:
        if not source_id:
            continue
        included_operator_sources.add(source_id)
        first = source_rows[0]
        for entry in _operator_catalog_entries(source_id, source_rows):
            best = entry["row"]
            mapped = bool(_text(best.get("concept_id")))
            claim_type = _text(best.get("claim_type")).lower() if mapped else "benefit"
            concept_ids = list(entry.get("concept_ids") or [])
            carrier_policy = _carrier_policy(claim_type, _text(best.get("claim_theme")))
            base["catalog"].append(
                {
                    "value_id": "OPERATOR_" + source_id + _text(entry.get("value_suffix")),
                    "source_argument_id": source_id,
                    "source_ref": _text(first.get("source_ref")),
                    "primary_selling_point": _text(entry.get("primary_selling_point")),
                    "canonical_selling_point": (
                        _text(best.get("canonical_claim_zh")) if mapped else ""
                    ),
                    "operator_expression": _text(entry.get("operator_expression")),
                    "source_operator_expression": _text(
                        entry.get("source_operator_expression")
                    ),
                    "source_scope_concept_count": int(
                        entry.get("source_scope_concept_count") or 1
                    ),
                    "dominant_user_question": "",
                    "proof_thesis": "",
                    "decision_thesis": "",
                    "script_role": (
                        "result_delivery" if claim_type == "visual_result" else "benefit_delivery"
                    ),
                    "argument_kind": "SELLING_ARGUMENT",
                    "source": "FEISHU_OPERATOR_CONFIRMED_ARGUMENT",
                    "authority": "FEISHU_OPERATOR_CONFIRMED",
                    "source_claim_ids": list(dict.fromkeys(entry.get("source_claim_ids") or [])),
                    "normalization_claim_ids": list(
                        dict.fromkeys(entry.get("normalization_claim_ids") or [])
                    ),
                    "concept_id": _text(best.get("concept_id")) if mapped else "",
                    "concept_ids": concept_ids,
                    "claim_type": claim_type,
                    "claim_theme": _text(best.get("claim_theme")) if mapped else "operator_value",
                    "allowed_strength": (
                        _text(best.get("allowed_strength")) if mapped else "soft_only"
                    ) or "soft_only",
                    "verification_status": "OPERATOR_CONFIRMED",
                    "mapping_status": "MAPPED" if mapped else "UNMAPPED",
                    "evidence_requirement": (
                        _text(best.get("evidence_requirement")) if mapped else "source_only"
                    ) or "source_only",
                    "operator_priority": _text(first.get("source_operator_priority")) or "normal",
                    "source_type": "operator_input",
                    "source_ref": _text(first.get("source_ref")),
                    "expression_policy": "SEMANTIC_AUTHORITY_NOT_VERBATIM",
                    "respectful_reframe_required": _requires_respectful_reframe(
                        entry.get("operator_expression"),
                        entry.get("primary_selling_point"),
                        best.get("canonical_claim_zh"),
                        best.get("risk_tags"),
                    ),
                    **carrier_policy,
                    **_scarf_execution_semantics(product_type, concept_ids),
                    **_accessory_operator_execution_semantics(
                        product_type,
                        _text(entry.get("operator_expression")),
                    ),
                    **_small_accessory_claim_action_semantics(
                        product_type,
                        _text(entry.get("operator_expression")),
                        _text(best.get("claim_theme")),
                    ),
                }
            )

    base["confirmed_argument_count"] = len(base["catalog"])
    base["mapped_argument_count"] = sum(
        item.get("mapping_status") == "MAPPED" for item in base["catalog"]
    )
    base["unmapped_argument_count"] = sum(
        item.get("mapping_status") == "UNMAPPED" for item in base["catalog"]
    )

    seen_concepts = set()
    ordered_rows = [dict(row) for row in rows]
    ordered_rows.sort(
        key=lambda claim: (
            0 if _text(claim.get("source_type")) == "operator_input" else 1,
            _operator_source_order(claim.get("source_span")),
            _text(claim.get("created_at")),
            _text(claim.get("claim_id")),
        )
    )
    for claim in ordered_rows:
        if _text(claim.get("claim_source_id")) in included_operator_sources:
            continue
        text = _text(claim.get("canonical_claim_zh"))
        claim_type = _text(claim.get("claim_type")).lower()
        if not text:
            continue
        concept_key = _text(claim.get("concept_id")) or f"{claim_type}:{text}"
        if concept_key in seen_concepts:
            continue
        seen_concepts.add(concept_key)
        source_type = _text(claim.get("source_type"))
        operator_expression = (
            _operator_expression(claim.get("source_span"))
            if source_type == "operator_input"
            else ""
        )
        common = {
            "claim_id": _text(claim.get("claim_id")),
            "concept_id": _text(claim.get("concept_id")),
            "canonical_claim_zh": text,
            "operator_expression": operator_expression,
            "claim_type": claim_type,
            "claim_theme": _text(claim.get("claim_theme")),
            "verification_status": _text(claim.get("verification_status")),
            "evidence_requirement": _text(claim.get("evidence_requirement")),
            "allowed_strength": _text(claim.get("allowed_strength")) or "soft_only",
            "operator_priority": _text(claim.get("operator_priority")) or "normal",
            "updated_at": _text(claim.get("updated_at")),
            "source_type": source_type,
            "source_ref": _text(claim.get("source_ref")),
        }
        if claim_type in ARGUMENT_CLAIM_TYPES:
            carrier_policy = _carrier_policy(claim_type, common["claim_theme"])
            # An explicitly confirmed operator sentence carries more useful
            # audience/scene semantics than the short taxonomy label.  The
            # canonical value remains attached for governance and deduping.
            argument_text = operator_expression or text
            base["catalog"].append(
                {
                    "value_id": "CENTRAL_" + common["claim_id"],
                    "primary_selling_point": argument_text,
                    "canonical_selling_point": text,
                    "operator_expression": operator_expression,
                    "dominant_user_question": "",
                    "proof_thesis": "",
                    "decision_thesis": "",
                    "script_role": "result_delivery" if claim_type == "visual_result" else "benefit_delivery",
                    "argument_kind": "SELLING_ARGUMENT",
                    "source": "CENTRAL_VOICEOVER_VERIFIED_CLAIM",
                    "source_claim_ids": [common["claim_id"]],
                    "claim_type": claim_type,
                    "claim_theme": common["claim_theme"],
                    "allowed_strength": common["allowed_strength"],
                    "verification_status": common["verification_status"],
                    "evidence_requirement": common["evidence_requirement"],
                    "operator_priority": common["operator_priority"],
                    "source_type": source_type,
                    "source_ref": common["source_ref"],
                    **carrier_policy,
                    **_scarf_execution_semantics(
                        product_type, [common["concept_id"]]
                    ),
                    **_accessory_operator_execution_semantics(
                        product_type,
                        operator_expression or text,
                    ),
                    **_small_accessory_claim_action_semantics(
                        product_type,
                        operator_expression or text,
                        common["claim_theme"],
                    ),
                }
            )
        else:
            base["evidence_claims"].append(common)

    base["available_argument_count"] = len(base["catalog"])
    base["status"] = "AVAILABLE" if base["catalog"] else "NO_SELLING_ARGUMENT"
    base["snapshot_hash"] = _stable_hash(
        {
            "catalog_version": SELLING_ARGUMENT_CATALOG_VERSION,
            "product_code": product_code,
            "catalog": base["catalog"],
            "evidence_claims": base["evidence_claims"],
        }
    )
    return base
