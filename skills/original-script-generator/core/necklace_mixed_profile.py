"""Necklace single-layer / single-pendant mixed display profile (V1).

This module owns one concern only: the *necklace delta* on top of the authored
mixed-accessory template.  It is deliberately additive and side-effect free:

* Importing it touches no database, no network and no environment write.
* Nothing here changes an existing behaviour: every entry point is opt-in
  through ``ORIGINAL_SCRIPT_NECKLACE_MIXED_V1_ENABLED`` (default *off*) and the
  shared template definition is never mutated -- the overlay is a deep copy
  rebuilt on demand.
* The four existing families (ear / wrist / finger / hair) stay byte-for-byte
  unchanged.  ``necklace`` is a registered canonical type that simply had no
  mixed zone; this module supplies one *locally* instead of appending the new
  template to the shared A/B/C rotation (which would have let other categories
  rotate onto a necklace template).

Two separations are load-bearing and are the reason this is a separate module:

1. **Registry opinion vs instance evidence.**  The physical subtype registry
   declares ``has_chain`` / ``has_pendant`` as ``UNKNOWN``: whether a given
   product is a *single*-layer chain with a *single* pendant is instance
   evidence, not a property of the word "necklace".  A choker, a bracelet or a
   bare chain must not be able to borrow this profile by resolving to the same
   canonical type, and the title must never be allowed to promote either part
   to ``VERIFIED``.
2. **Applicability vs eligibility.**  ``applicable`` answers "is this product a
   necklace at all?"; ``eligible`` answers "may it use this profile?".  A
   caller that collapses both into "empty result, take the old path" would let
   an explicitly requested necklace production run silently fall back to the
   earring / womenwear template.  They are reported separately, always.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from core.accessory_mixed_templates import (
    EVIDENCE_ABSENT,
    EVIDENCE_UNKNOWN,
    EVIDENCE_VERIFIED,
    load_mixed_template_definition,
)
from core.product_type_resolution import load_type_registry, normalize_product_type


NECKLACE_MIXED_V1_ENV = "ORIGINAL_SCRIPT_NECKLACE_MIXED_V1_ENABLED"
NECKLACE_MIXED_V1_PROFILE = "NECKLACE_MIXED_V1"
NECKLACE_MIXED_V1_FEATURE_VERSION = 1
NECKLACE_MIXED_V1_SCHEMA = "necklace-mixed-v1"
NECKLACE_MIXED_V1_CANONICAL = "necklace"
NECKLACE_MIXED_V1_ZONE = "NECK"
NECKLACE_MIXED_V1_SUBTYPE = "SINGLE_LAYER_SINGLE_PENDANT"
NECKLACE_MIXED_V1_INTERACTION_MODE = "NONE"
NECKLACE_MIXED_V1_TEMPLATE_ID = "NMX_01_WEAR_DETAIL_STATIC"
NECKLACE_MIXED_V1_RECIPE_ID = "NMX_WARM_NEUTRAL_WINDOW_V1"

#: Contract namespace the necklace profile adds to the shared
#: ``mixed_template_contract``.  Kept *additional*: the shared contract keeps
#: its existing shape, so every existing consumer is unaffected.
NECKLACE_CONTRACT_KEY = "necklace_contract"

#: Recorded inside the overlay so a downstream reader can prove which profile
#: produced a contract without having to diff two definitions.
NECKLACE_OVERLAY_MARKER = "necklace_profile_overlay"

_DEFINITION_PATH = Path(__file__).resolve().parent.parent / "config" / "necklace_mixed_v1.json"
_TRUE_TOKENS = {"1", "true", "yes", "on"}

# ── Refusal vocabulary ──────────────────────────────────────────────────────
# Mapped onto the existing rejection reports; deliberately *not* a new batch
# status enum.  The suffix after ``:`` names the concrete cause so a reviewer
# can tell "not a necklace" from "a necklace we refuse to certify".
NECKLACE_SCOPE_UNSUPPORTED = "NECKLACE_SCOPE_UNSUPPORTED"
NECKLACE_EVIDENCE_INCOMPLETE = "NECKLACE_EVIDENCE_INCOMPLETE"
NECKLACE_TEMPLATE_MISMATCH = "NECKLACE_TEMPLATE_MISMATCH"
NECKLACE_MAINLINE_UNAVAILABLE = "NECKLACE_MAINLINE_UNAVAILABLE"

NECKLACE_V1_ELIGIBLE = "NECKLACE_MIXED_V1_ELIGIBLE"

#: Ordered shot contract of the single V1 template.  Frozen into the contract
#: so a later config edit can never silently re-choreograph a frozen script.
NECKLACE_V1_SHOT_ORDER: Tuple[Tuple[str, int], ...] = (
    ("WORN_DETAIL", 4),
    ("WORN_RELATION", 3),
    ("HANDHELD_PRODUCT", 4),
    ("STATIC_PRODUCT", 4),
)

#: Body zones whose vocabulary must never reappear in a necklace shot.  The
#: check runs against the *positive* instruction fields only (``action`` /
#: ``observation_job``): ``forbidden_framing`` has to name what is banned, and
#: a blanket substring search over the whole contract would report the correct
#: prohibition as the defect.
_FOREIGN_ZONE_TERMS: Tuple[str, ...] = (
    "耳侧",
    "耳廓",
    "耳垂",
    "耳饰",
    "耳部",
    "手腕",
    "前臂",
    "手镯",
    "手链",
    "发梢",
    "发型",
    "发夹",
    "发饰",
    "耳针",
    "耳夹",
)

#: Generic expressions that turn "show one detail" into a promise about the
#: whole product.  They are refused in an observation job the same way the
#: mainline contract refuses them in the core value (semantic reversal: after
#: removing the specific part, only a broader claim is left).
_GENERIC_OBSERVATION_TERMS: Tuple[str, ...] = (
    "核心卖点",
    "展示卖点",
    "展示核心",
    "整体展示",
)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _flag_enabled(value: Any) -> bool:
    return _text(value).lower() in _TRUE_TOKENS


def necklace_mixed_v1_enabled(enabled: Optional[bool] = None) -> bool:
    """Return whether the necklace V1 profile is switched on.

    ``enabled`` is an explicit override for callers that already resolved the
    request-level switch.  When omitted the environment is consulted and the
    default is off, so no existing request changes behaviour.
    """

    if enabled is not None:
        return bool(enabled)
    return _flag_enabled(os.environ.get(NECKLACE_MIXED_V1_ENV, "0"))


@lru_cache(maxsize=1)
def load_necklace_v1_definition() -> Dict[str, Any]:
    """Load and cache the *delta* definition (necklace-only keys)."""

    with _DEFINITION_PATH.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("necklace_mixed_v1.json 顶层必须是对象")
    return payload


def necklace_profile_config_hash(definition: Mapping[str, Any] | None = None) -> str:
    """Stable hash of the necklace delta, frozen into every V1 contract.

    Hashes the *delta* rather than the merged overlay: the overlay inherits
    large shared blocks that this profile does not own, and a shared edit must
    not be able to masquerade as a necklace-profile change.
    """

    data = dict(definition or load_necklace_v1_definition())
    blob = json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def necklace_v1_template() -> Dict[str, Any]:
    return copy.deepcopy(load_necklace_v1_definition().get("template") or {})


def necklace_v1_zone_rule() -> Dict[str, Any]:
    return copy.deepcopy(load_necklace_v1_definition().get("zone_rule") or {})


def necklace_v1_subtype_rule() -> Dict[str, Any]:
    return copy.deepcopy(load_necklace_v1_definition().get("subtype_rule") or {})


def necklace_v1_environment_recipe() -> Dict[str, Any]:
    return copy.deepcopy(load_necklace_v1_definition().get("environment_recipe") or {})


def necklace_v1_eligibility_rule() -> Dict[str, Any]:
    return copy.deepcopy(load_necklace_v1_definition().get("eligibility") or {})


def select_necklace_template_id(index: int = 0) -> str:
    """The necklace profile owns exactly one template, so it never rotates.

    ``index`` is accepted (and ignored) only so call sites can keep the same
    shape as the shared ``select_template_id(index)``.
    """

    return _text((load_necklace_v1_definition().get("template") or {}).get("template_id")) or (
        NECKLACE_MIXED_V1_TEMPLATE_ID
    )


def select_necklace_environment_recipe_id(index: int = 0) -> str:
    """Same as :func:`select_necklace_template_id`: one recipe, no rotation."""

    return (
        _text(load_necklace_v1_definition().get("environment_recipe_id"))
        or NECKLACE_MIXED_V1_RECIPE_ID
    )


# ---------------------------------------------------------------------------
# Local definition overlay
# ---------------------------------------------------------------------------


def build_necklace_profile_overlay(
    product_evidence: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    """Return a *fresh* shared definition with the necklace delta applied.

    The shared definition is ``lru_cache``d and shared by every caller, so the
    overlay is built by deep copy and the cached object is never touched.  The
    three shared templates and three shared recipes are intentionally *not*
    carried over:

    * ``templates`` becomes the single NMX template, so this branch can never
      rotate onto an AMX montage;
    * ``environment_recipes`` becomes the single NMX recipe, so this branch can
      never inherit a shared light setup;
    * ``canonical_type_to_zone`` gains ``necklace`` only -- ``choker`` and every
      other canonical type keep resolving exactly as before, which is what
      keeps the new template unreachable for them.

    ``product_evidence`` is accepted for callers that want the instance
    evidence recorded alongside the overlay; it never changes which template or
    recipe is selected.
    """

    overlay = copy.deepcopy(load_mixed_template_definition())
    delta = load_necklace_v1_definition()

    zones = dict(overlay.get("canonical_type_to_zone") or {})
    zones[NECKLACE_MIXED_V1_CANONICAL] = NECKLACE_MIXED_V1_ZONE
    overlay["canonical_type_to_zone"] = zones

    overlay["templates"] = [necklace_v1_template()]
    overlay["environment_recipes"] = {
        select_necklace_environment_recipe_id(): necklace_v1_environment_recipe()
    }
    overlay["default_environment_recipe"] = select_necklace_environment_recipe_id()

    category_rules = dict(overlay.get("category_rules") or {})
    category_rules[NECKLACE_MIXED_V1_ZONE] = necklace_v1_zone_rule()
    overlay["category_rules"] = category_rules

    subtype_rules = dict(overlay.get("physical_subtype_rules") or {})
    subtype_rules[NECKLACE_MIXED_V1_CANONICAL] = necklace_v1_subtype_rule()
    overlay["physical_subtype_rules"] = subtype_rules

    # The template total must equal the shared 15-second requirement.  Taken
    # from the delta rather than assumed, so a config edit that breaks the
    # timeline fails loudly at compile time instead of producing a 12-second
    # "15-second" contract.
    overlay["duration_seconds"] = int(delta.get("duration_seconds") or 15)
    overlay["unit_count"] = int(delta.get("unit_count") or len(NECKLACE_V1_SHOT_ORDER))

    overlay[NECKLACE_OVERLAY_MARKER] = {
        "feature_profile": NECKLACE_MIXED_V1_PROFILE,
        "feature_version": NECKLACE_MIXED_V1_FEATURE_VERSION,
        "schema_version": NECKLACE_MIXED_V1_SCHEMA,
        "profile_config_hash": necklace_profile_config_hash(delta),
        "canonical_type": NECKLACE_MIXED_V1_CANONICAL,
        "zone": NECKLACE_MIXED_V1_ZONE,
        "subtype": NECKLACE_MIXED_V1_SUBTYPE,
        "interaction_mode": NECKLACE_MIXED_V1_INTERACTION_MODE,
        "eligibility_evidence_refs": [
            dict(item)
            for item in (_evidence_refs(product_evidence) or [])
        ],
    }
    return overlay


# ---------------------------------------------------------------------------
# Applicability and eligibility
# ---------------------------------------------------------------------------


def _canonical_of(product_context: Mapping[str, Any] | None) -> str:
    """Resolve the canonical type of a request without guessing.

    Three shapes are trusted, in this order:

    1. an explicit ``canonical_type`` (which upstream stages produce) -- but
       only when it is a registered canonical spelling;
    2. a raw value that *is* a registered canonical spelling;
    3. the shared registry's own resolution of an operator-facing value, and
       only when the registry actually recognised it.

    Step 2 is not redundant with step 3 and skipping it is a live trap:
    ``normalize_product_type("necklace", "")`` does **not** recognise a bare
    English canonical name -- it silently falls back to ``womenwear``.  A
    necklace request carrying ``product_type="necklace"`` would therefore be
    refused as "not a necklace" by a resolver that only did step 3.

    Step 3's ``recognized_by_registry`` guard is the mirror image: an
    unrecognised value also falls back to ``womenwear``, and turning that
    fallback into an answer is how an unknown accessory gets treated as
    apparel.  Unresolved returns ``""``, which callers read as not-applicable.

    Deliberately *not* used here: a ``"项链"`` substring test, the ``neck``
    slot, or the ``jewelry`` family.  A choker shares the first two and a
    bracelet shares the third, so any of them would let a non-qualifying
    product borrow this profile.
    """

    data = dict(product_context or {})
    try:
        known = set(load_type_registry().type_by_canonical)
    except Exception:  # noqa: BLE001 - a broken registry must not guess
        known = set()

    declared = _text(data.get("canonical_type")).lower()
    if declared and (not known or declared in known):
        return declared

    raw = _text(
        data.get("product_type_raw")
        or data.get("product_type")
        or data.get("canonical_product_type")
    )
    if not raw:
        return ""
    lowered = raw.lower()
    if lowered in known:
        return lowered

    resolved = normalize_product_type(raw, _text(data.get("top_category")))
    if not getattr(resolved, "recognized_by_registry", False):
        return ""
    return _text(getattr(resolved, "canonical_type", "")).lower()


def _part_state(part_evidence: Mapping[str, Any] | None, part_key: str) -> str:
    """Read one part state out of an evidence map, tolerating both shapes.

    ``resolve_part_evidence_map`` produces ``{part_key: {"state": ...}}``; a
    hand-held ``{part_key: "VERIFIED"}`` is also accepted.  An unknown shape
    yields ``UNKNOWN`` -- never a silent default that would look like proof.
    """

    if not isinstance(part_evidence, Mapping):
        return EVIDENCE_UNKNOWN
    entry = part_evidence.get(part_key)
    if isinstance(entry, Mapping):
        state = _text(entry.get("state")).upper()
    else:
        state = _text(entry).upper()
    return state or EVIDENCE_UNKNOWN


def _evidence_refs(product_evidence: Mapping[str, Any] | None) -> List[Dict[str, Any]]:
    if not isinstance(product_evidence, Mapping):
        return []
    refs = product_evidence.get("evidence_refs")
    if isinstance(refs, Sequence) and not isinstance(refs, (str, bytes)):
        return [dict(item) for item in refs if isinstance(item, Mapping)]
    return []


def _count_of(counts: Mapping[str, Any] | None, key: str) -> Optional[int]:
    """Read an integer count, or ``None`` when it is simply not established.

    ``None`` is the whole point: "no source said how many layers" must not be
    readable as "there is one layer".
    """

    if not isinstance(counts, Mapping):
        return None
    raw = counts.get(key)
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Instance evidence production
# ---------------------------------------------------------------------------
# The eligibility judge reads *instance* evidence, so something has to produce
# it.  Two shapes are consulted, in this order:
#
# 1. an explicit structured count on the anchor card (``layer_count`` /
#    ``pendant_count``) -- the authoritative shape once an upstream stage can
#    supply one;
# 2. a count the approved anchor text *states outright* ("单层", "双吊坠").
#
# Nothing here promotes a value out of the product name.  That is the whole
# point of separating the two: "项链" proves neither a chain nor a pendant, and
# a coiled chain in a photo is not evidence of several layers.  An unstated
# count stays ``None`` and the judge reports ``UNKNOWN`` and refuses -- never a
# guessed 1.


def resolve_necklace_part_evidence(
    *,
    structure_facts: Mapping[str, Any] | None = None,
    anchor_texts: Iterable[Any] | None = None,
) -> Dict[str, Dict[str, str]]:
    """Evidence for exactly the parts this profile's eligibility reads.

    The enumeration source is the necklace profile's own
    ``eligibility.required_part_states``, deliberately *not* the shared
    :func:`resolve_part_evidence_map`.  The shared helper enumerates
    ``optional_actions[].requires``, and V1 declares no optional action at all
    (the two worn shots add no chain-pulling move).  Calling it here would
    therefore return an empty map -- and an empty map is indistinguishable from
    "no source confirmed anything", so a *missing enumeration entry* would read
    as "there is no chain" and refuse every genuine necklace.  That silent
    conflation is what this function exists to avoid.

    The per-part judgement is still the shared, already-authoritative
    :func:`resolve_part_evidence`: registry fact first, then approved anchor
    text with negatives scanned before positives, else ``UNKNOWN``.  The shared
    ``part_evidence_terms`` table already carries ``has_chain`` /
    ``has_pendant``, so no second fact table is introduced.
    """

    from core.accessory_mixed_templates import resolve_part_evidence

    rule = necklace_v1_eligibility_rule()
    keys: List[str] = []
    for part_key in (rule.get("required_part_states") or {}):
        name = _text(part_key)
        if name and name not in keys:
            keys.append(name)

    facts = dict(
        structure_facts
        if isinstance(structure_facts, Mapping) and structure_facts
        else (necklace_v1_subtype_rule().get("structure_facts") or {})
    )
    return {
        key: resolve_part_evidence(key, structure_facts=facts, anchor_texts=anchor_texts)
        for key in keys
    }


def _declared_count(
    anchor_card: Mapping[str, Any] | None,
    keys: Sequence[str],
) -> Optional[int]:
    """Read an explicit structured count, or ``None`` when it is not stated."""

    card = anchor_card if isinstance(anchor_card, Mapping) else {}
    for key in keys:
        raw = card.get(key)
        if raw is None or raw == "":
            continue
        try:
            value = int(raw)
        except (TypeError, ValueError):
            continue
        if value > 0:
            return value
    return None


def _count_from_anchor_text(
    anchor_texts: Iterable[Any] | None,
    term_groups: Mapping[str, Any] | None,
) -> Tuple[Optional[int], str]:
    """Read an *explicitly stated* count out of the approved anchor text.

    Returns ``(count, matched_term)``.  Groups are scanned from the largest
    count down, so a "双层" statement can never be read as a single layer just
    because a one-layer spelling also appears somewhere in the same text.
    """

    texts = [_text(item) for item in (anchor_texts or []) if _text(item)]
    blob = "；".join(texts)
    if not blob or not isinstance(term_groups, Mapping):
        return None, ""

    groups: List[Tuple[int, List[str]]] = []
    for label, terms in term_groups.items():
        try:
            count = int(_text(label))
        except (TypeError, ValueError):
            continue
        terms_list = [_text(term) for term in (terms or []) if _text(term)]
        if terms_list:
            groups.append((count, terms_list))

    for count, terms_list in sorted(groups, key=lambda item: -item[0]):
        for term in terms_list:
            if term in blob:
                return count, term
    return None, ""


def resolve_necklace_structure_counts(
    *,
    anchor_card: Mapping[str, Any] | None = None,
    anchor_texts: Iterable[Any] | None = None,
) -> Dict[str, Any]:
    """Layer / pendant counts, each carrying the source it was read from.

    The trailing ``*_source`` keys are what make the number reviewable: a
    reviewer can open the anchor card and see the exact field or phrase the
    count came from instead of trusting a bare integer.
    """

    rule = necklace_v1_eligibility_rule()
    card = anchor_card if isinstance(anchor_card, Mapping) else {}

    out: Dict[str, Any] = {}
    for key, term_field in (
        (_text(rule.get("layer_count_key")) or "layer_count", "layer_count_terms"),
        (_text(rule.get("pendant_count_key")) or "pendant_count", "pendant_count_terms"),
    ):
        value = _declared_count(card, (key, f"structure_{key}"))
        source = "ANCHOR_CARD_FIELD" if value is not None else ""
        if value is None:
            value, matched = _count_from_anchor_text(
                anchor_texts, rule.get(term_field)
            )
            source = f"ANCHOR_TEXT:{matched}" if value is not None else ""
        if value is not None:
            out[key] = value
            out[f"{key}_source"] = source
    return out


def build_necklace_product_evidence(
    *,
    anchor_card: Mapping[str, Any] | None = None,
    counts: Mapping[str, Any] | None = None,
    evidence_ref: str = "",
    structure_facts: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    """Assemble the instance evidence ``resolve_necklace_v1_scope`` consumes.

    Only authoritative anchor text confirms a part: ``anchor_evidence_texts``
    reads ``hard_anchors`` and deliberately excludes ``display_anchors``, which
    may hold a model-authored presentation idea ("手持展示吊坠") that must never
    be promoted into a product fact.

    An explicit ``counts`` argument wins outright; otherwise the anchor card and
    the approved anchor text are consulted.  A count that nobody states simply
    does not appear in the map, and the judge reports ``UNKNOWN`` and refuses.
    """

    from core.accessory_mixed_templates import anchor_evidence_texts

    card = anchor_card if isinstance(anchor_card, Mapping) else {}
    anchor_texts = anchor_evidence_texts(card)
    resolved_counts: Dict[str, Any] = dict(counts or {})
    if not resolved_counts:
        resolved_counts = resolve_necklace_structure_counts(
            anchor_card=card, anchor_texts=anchor_texts
        )
    return {
        "part_evidence": resolve_necklace_part_evidence(
            structure_facts=structure_facts,
            anchor_texts=anchor_texts,
        ),
        "counts": resolved_counts,
        "evidence_ref": _text(evidence_ref) or _text(card.get("product_code")),
        "anchor_texts": anchor_texts,
    }


def judge_necklace_eligibility(
    *,
    part_evidence: Mapping[str, Any] | None = None,
    counts: Mapping[str, Any] | None = None,
    evidence_ref: str = "",
) -> Dict[str, Any]:
    """Judge single-layer / single-pendant eligibility from instance evidence.

    Returns ``{"eligible", "reason", "evidence_refs", "layer_count",
    "pendant_count", "unresolved"}``.  Nothing is inferred from the product
    name: chain and pendant must be *confirmed*, and both counts must have a
    traceable source.  Anything unestablished is reported as ``UNKNOWN`` and
    refuses the profile -- an explicit, reviewable refusal instead of a coin
    flip that later ships three stacked layers under a "single layer" contract.
    """

    rule = necklace_v1_eligibility_rule()
    required = dict(rule.get("required_part_states") or {})
    forbidden = dict(rule.get("forbidden_part_states") or {})

    refs: List[Dict[str, Any]] = []
    unresolved: List[str] = []

    for part_key, wanted in required.items():
        state = _part_state(part_evidence, part_key)
        refs.append({
            "part": _text(part_key),
            "state": state,
            "required": _text(wanted).upper(),
            "source": _text(
                ((part_evidence or {}).get(part_key) or {}).get("source")
                if isinstance((part_evidence or {}).get(part_key), Mapping)
                else ""
            ),
            "ref": _text(evidence_ref),
        })
        if state == EVIDENCE_UNKNOWN:
            unresolved.append(f"{part_key}:UNKNOWN")
        elif _text(forbidden.get(part_key)).upper() == state:
            unresolved.append(f"{part_key}:{state}")
        elif state != _text(wanted).upper():
            unresolved.append(f"{part_key}:{state}")

    layer_key = _text(rule.get("layer_count_key")) or "layer_count"
    pendant_key = _text(rule.get("pendant_count_key")) or "pendant_count"
    max_layers = int(rule.get("max_layer_count") or 1)
    max_pendants = int(rule.get("max_pendant_count") or 1)

    layer_count = _count_of(counts, layer_key)
    pendant_count = _count_of(counts, pendant_key)
    refs.append({
        "part": "layer_count",
        "state": EVIDENCE_UNKNOWN if layer_count is None else str(layer_count),
        "required": max_layers,
        "source": "STRUCTURE_COUNTS",
        "ref": _text(evidence_ref),
    })
    refs.append({
        "part": "pendant_count",
        "state": EVIDENCE_UNKNOWN if pendant_count is None else str(pendant_count),
        "required": max_pendants,
        "source": "STRUCTURE_COUNTS",
        "ref": _text(evidence_ref),
    })
    if layer_count is None:
        unresolved.append("layer_count:UNKNOWN")
    elif layer_count != max_layers:
        unresolved.append(f"layer_count:{layer_count}")
    if pendant_count is None:
        unresolved.append("pendant_count:UNKNOWN")
    elif pendant_count != max_pendants:
        unresolved.append(f"pendant_count:{pendant_count}")

    if unresolved:
        return {
            "eligible": False,
            "reason": f"{NECKLACE_EVIDENCE_INCOMPLETE}:{','.join(unresolved)}",
            "evidence_refs": refs,
            "layer_count": layer_count,
            "pendant_count": pendant_count,
            "unresolved": unresolved,
        }
    return {
        "eligible": True,
        "reason": NECKLACE_V1_ELIGIBLE,
        "evidence_refs": refs,
        "layer_count": layer_count,
        "pendant_count": pendant_count,
        "unresolved": [],
    }


def resolve_necklace_v1_scope(
    product_context: Mapping[str, Any] | None,
    execution_scope: Mapping[str, Any] | None = None,
    *,
    enabled: Optional[bool] = None,
) -> Dict[str, Any]:
    """Decide whether one request may plan under the necklace V1 profile.

    ``applies``-style collapsing is refused by design: ``applicable`` and
    ``eligible`` are independent, and the caller is expected to branch on both.
    A non-necklace product is ``applicable=False``; a necklace that fails the
    switch, the parent-task scope, the part evidence or the mainline test is
    ``applicable=True, eligible=False`` with a named reason.  Only the two
    eligibility flags together mean "use NMX".
    """

    data = dict(product_context or {})
    canonical = _canonical_of(data)
    decision: Dict[str, Any] = {
        "applicable": canonical == NECKLACE_MIXED_V1_CANONICAL,
        "eligible": False,
        "reason": "",
        "canonical_type": canonical,
        "evidence_refs": [],
        "product_code": _text(data.get("product_code")),
        "feature_profile": NECKLACE_MIXED_V1_PROFILE,
        "feature_version": NECKLACE_MIXED_V1_FEATURE_VERSION,
        "template_id": select_necklace_template_id(),
        "environment_recipe_id": select_necklace_environment_recipe_id(),
    }

    if not decision["applicable"]:
        decision["reason"] = f"{NECKLACE_SCOPE_UNSUPPORTED}:not_necklace"
        return decision

    if not necklace_mixed_v1_enabled(enabled):
        decision["reason"] = f"{NECKLACE_SCOPE_UNSUPPORTED}:switch=off"
        return decision

    # The necklace profile rides on the shared mixed scope: a long-form source
    # build, a remake, a resumed plan or a non-15-second request must not enter
    # here either.  Reusing the shared decision keeps one authority for "which
    # task is this really?".
    from core.accessory_mixed_templates import mixed_scope_decision

    scope_decision = mixed_scope_decision(execution_scope)
    decision["scope_eligible"] = bool(scope_decision.get("eligible"))
    if not decision["scope_eligible"]:
        decision["reason"] = (
            f"{NECKLACE_SCOPE_UNSUPPORTED}:scope="
            f"{_text(scope_decision.get('reason'))}"
        )
        return decision

    eligible = judge_necklace_eligibility(
        part_evidence=data.get("part_evidence"),
        counts=data.get("counts") or data.get("structure_counts"),
        evidence_ref=_text(data.get("evidence_ref") or data.get("product_code")),
    )
    decision["evidence_refs"] = list(eligible.get("evidence_refs") or [])
    decision["layer_count"] = eligible.get("layer_count")
    decision["pendant_count"] = eligible.get("pendant_count")
    if not eligible.get("eligible"):
        decision["reason"] = _text(eligible.get("reason"))
        return decision

    mainline = data.get("mainline") or data.get("mainline_contract")
    if not isinstance(mainline, Mapping) or not _text(mainline.get("core_value")):
        decision["reason"] = f"{NECKLACE_MAINLINE_UNAVAILABLE}:core_value_missing"
        return decision

    decision["eligible"] = True
    decision["reason"] = NECKLACE_V1_ELIGIBLE
    return decision


# ---------------------------------------------------------------------------
# Contract namespace
# ---------------------------------------------------------------------------


def build_necklace_contract_block(
    *,
    eligibility: Mapping[str, Any] | None = None,
    chain_identity: Mapping[str, Any] | None = None,
    pendant_identity: Mapping[str, Any] | None = None,
    wearing_relation: Mapping[str, Any] | None = None,
    interaction_mode: str = "",
) -> Dict[str, Any]:
    """Build the additional namespace appended to the shared contract.

    Only fields that a real consumer reads are stored: the subtype, the
    traceable eligibility evidence, the two identity references and the
    interaction mode.  The identity blocks *reference* the existing product
    identity rather than restating chain / pendant facts, so there is no second
    fact table to drift out of sync.
    """

    return {
        "subtype": NECKLACE_MIXED_V1_SUBTYPE,
        "eligibility_evidence_refs": [
            dict(item) for item in ((eligibility or {}).get("evidence_refs") or [])
            if isinstance(item, Mapping)
        ],
        "chain_identity": dict(chain_identity or {}),
        "pendant_identity": dict(pendant_identity or {}),
        "wearing_relation": dict(wearing_relation or {}),
        "interaction_mode": _text(interaction_mode) or NECKLACE_MIXED_V1_INTERACTION_MODE,
        "profile_config_hash": necklace_profile_config_hash(),
        "feature_profile": NECKLACE_MIXED_V1_PROFILE,
        "feature_version": NECKLACE_MIXED_V1_FEATURE_VERSION,
    }


def attach_necklace_contract(
    contract: Mapping[str, Any] | None,
    *,
    eligibility: Mapping[str, Any] | None = None,
    chain_identity: Mapping[str, Any] | None = None,
    pendant_identity: Mapping[str, Any] | None = None,
    wearing_relation: Mapping[str, Any] | None = None,
    interaction_mode: str = "",
) -> Dict[str, Any]:
    """Return ``contract`` with the necklace namespace and profile fields set.

    A *copy* is returned with the shared keys untouched: the shared contract
    consumers keep reading exactly the keys they read today.
    """

    out = dict(contract or {})
    block = build_necklace_contract_block(
        eligibility=eligibility,
        chain_identity=chain_identity,
        pendant_identity=pendant_identity,
        wearing_relation=wearing_relation,
        interaction_mode=interaction_mode,
    )
    out["feature_profile"] = NECKLACE_MIXED_V1_PROFILE
    out["feature_version"] = NECKLACE_MIXED_V1_FEATURE_VERSION
    out["profile_config_hash"] = block["profile_config_hash"]
    out[NECKLACE_CONTRACT_KEY] = block
    return out


def frozen_necklace_contract(container: Mapping[str, Any] | None) -> Dict[str, Any]:
    """Read the necklace namespace back out of a frozen contract/extension.

    Mirrors ``frozen_mixed_contract``: the shared ``mixed_template_contract``
    key is checked first, its own ``category_execution_extension`` second.  An
    absent namespace returns ``{}``, which is the authority for "this is not a
    necklace V1 task" -- never the environment switch, which only governs *new*
    plans.
    """

    if not isinstance(container, Mapping):
        return {}
    from core.accessory_mixed_templates import MIXED_TEMPLATE_CONTRACT_KEY

    for candidate in (
        container.get(MIXED_TEMPLATE_CONTRACT_KEY),
        (container.get("category_execution_extension") or {}).get(
            MIXED_TEMPLATE_CONTRACT_KEY
        ) if isinstance(container.get("category_execution_extension"), Mapping) else None,
    ):
        if isinstance(candidate, Mapping) and isinstance(
            candidate.get(NECKLACE_CONTRACT_KEY), Mapping
        ):
            return dict(candidate.get(NECKLACE_CONTRACT_KEY))
    return {}


# ---------------------------------------------------------------------------
# Necklace-specific contract validation
# ---------------------------------------------------------------------------


def validate_necklace_v1_contract(contract: Mapping[str, Any] | None) -> List[str]:
    """Return hard blocking errors for a necklace V1 contract.

    Complements the shared ``validate_mixed_template_contract``: the shared
    validator owns the generic promises (4 shots, 15 seconds, MIXED carrier,
    NO_FACE), this one owns the necklace promises.  Empty list means usable.

    The foreign-zone check deliberately inspects only the positive instruction
    fields.  ``forbidden_framing`` must *name* what is banned ("眼睛入画"), so a
    substring sweep over the whole contract would report the correct
    prohibition as the defect -- the exact false positive this project has
    already paid for once.
    """

    errors: List[str] = []
    data = dict(contract or {})
    if not data:
        return ["NECKLACE_CONTRACT_MISSING"]

    if _text(data.get("canonical_product_type")).lower() != NECKLACE_MIXED_V1_CANONICAL:
        errors.append("NECKLACE_TEMPLATE_MISMATCH:canonical_product_type")
    if _text(data.get("template_id")) != NECKLACE_MIXED_V1_TEMPLATE_ID:
        errors.append(f"{NECKLACE_TEMPLATE_MISMATCH}:template_id")
    if _text(data.get("category_zone")) != NECKLACE_MIXED_V1_ZONE:
        errors.append(f"{NECKLACE_TEMPLATE_MISMATCH}:category_zone")
    if _text(data.get("environment_recipe_id")) != NECKLACE_MIXED_V1_RECIPE_ID:
        errors.append(f"{NECKLACE_TEMPLATE_MISMATCH}:environment_recipe_id")

    block = data.get(NECKLACE_CONTRACT_KEY)
    if not isinstance(block, Mapping) or not block:
        errors.append(f"{NECKLACE_TEMPLATE_MISMATCH}:necklace_contract_missing")
    else:
        if _text(block.get("subtype")) != NECKLACE_MIXED_V1_SUBTYPE:
            errors.append(f"{NECKLACE_TEMPLATE_MISMATCH}:subtype")
        if _text(block.get("interaction_mode")) != NECKLACE_MIXED_V1_INTERACTION_MODE:
            errors.append(f"{NECKLACE_TEMPLATE_MISMATCH}:interaction_mode")

    units = [item for item in (data.get("capture_units") or []) if isinstance(item, Mapping)]
    if len(units) != len(NECKLACE_V1_SHOT_ORDER):
        errors.append(f"NECKLACE_CONTRACT_SHOT_COUNT:{len(units)}")
        return errors

    total = 0
    for position, (module, seconds) in enumerate(NECKLACE_V1_SHOT_ORDER):
        unit = units[position]
        unit_id = _text(unit.get("unit_id")) or f"#{position + 1}"
        if _text(unit.get("module")) != module:
            errors.append(f"NECKLACE_CONTRACT_SHOT_ORDER:{unit_id}:{unit.get('module')}")
        try:
            got = int(unit.get("duration_seconds") or 0)
        except (TypeError, ValueError):
            got = -1
        if got != seconds:
            errors.append(f"NECKLACE_CONTRACT_SHOT_DURATION:{unit_id}:{got}!={seconds}")
        total += max(got, 0)
        for field in ("action", "observation_job"):
            blob = _text(unit.get(field))
            hits = [term for term in _FOREIGN_ZONE_TERMS if term in blob]
            for term in hits:
                errors.append(f"NECKLACE_CONTRACT_FOREIGN_ZONE_ACTION:{unit_id}:{term}")
            generic = [term for term in _GENERIC_OBSERVATION_TERMS if term in blob]
            for term in generic:
                errors.append(f"NECKLACE_CONTRACT_GENERIC_OBSERVATION:{unit_id}:{term}")

    if total != int(load_necklace_v1_definition().get("duration_seconds") or 15):
        errors.append(f"NECKLACE_CONTRACT_TIMELINE:{total}")

    # The two non-worn shots are where this project's earlier defects landed:
    # a handheld shot inheriting a body-zone entry requirement, and a static
    # shot still asking for a person.  Assert them here so a config edit cannot
    # reintroduce either without failing loudly.
    handheld = units[2]
    if _text(handheld.get("carrier_mode")) != "HAND_ONLY":
        errors.append("NECKLACE_CONTRACT_HANDHELD_CARRIER")
    if _text(handheld.get("product_state")) != "HELD":
        errors.append("NECKLACE_CONTRACT_HANDHELD_STATE")
    static = units[3]
    if _text(static.get("carrier_mode")) != "STATIC_PRODUCT":
        errors.append("NECKLACE_CONTRACT_STATIC_CARRIER")
    if _text(static.get("product_state")) != "RESTING_ON_SURFACE":
        errors.append("NECKLACE_CONTRACT_STATIC_STATE")
    if _text(static.get("body_zone")):
        errors.append("NECKLACE_CONTRACT_STATIC_BODY_ZONE")

    worn_zones = {_text(units[index].get("body_zone")) for index in (0, 1)}
    if worn_zones != {NECKLACE_MIXED_V1_ZONE}:
        errors.append(f"NECKLACE_CONTRACT_WORN_BODY_ZONE:{sorted(worn_zones)}")

    return errors


__all__ = [
    "NECKLACE_MIXED_V1_ENV",
    "NECKLACE_MIXED_V1_PROFILE",
    "NECKLACE_MIXED_V1_FEATURE_VERSION",
    "NECKLACE_MIXED_V1_SCHEMA",
    "NECKLACE_MIXED_V1_CANONICAL",
    "NECKLACE_MIXED_V1_ZONE",
    "NECKLACE_MIXED_V1_SUBTYPE",
    "NECKLACE_MIXED_V1_INTERACTION_MODE",
    "NECKLACE_MIXED_V1_TEMPLATE_ID",
    "NECKLACE_MIXED_V1_RECIPE_ID",
    "NECKLACE_CONTRACT_KEY",
    "NECKLACE_OVERLAY_MARKER",
    "NECKLACE_SCOPE_UNSUPPORTED",
    "NECKLACE_EVIDENCE_INCOMPLETE",
    "NECKLACE_TEMPLATE_MISMATCH",
    "NECKLACE_MAINLINE_UNAVAILABLE",
    "NECKLACE_V1_ELIGIBLE",
    "NECKLACE_V1_SHOT_ORDER",
    "necklace_mixed_v1_enabled",
    "load_necklace_v1_definition",
    "necklace_profile_config_hash",
    "necklace_v1_template",
    "necklace_v1_zone_rule",
    "necklace_v1_subtype_rule",
    "necklace_v1_environment_recipe",
    "necklace_v1_eligibility_rule",
    "select_necklace_template_id",
    "select_necklace_environment_recipe_id",
    "build_necklace_profile_overlay",
    "judge_necklace_eligibility",
    "resolve_necklace_v1_scope",
    "build_necklace_contract_block",
    "attach_necklace_contract",
    "frozen_necklace_contract",
    "validate_necklace_v1_contract",
]
