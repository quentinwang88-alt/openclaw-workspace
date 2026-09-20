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

#: Wording that only belongs in a *worn* shot.  The hand-held and static shots
#: must not inherit it: a hand-only shot that still asks for a body zone is the
#: exact defect this project has already paid for once (a handheld unit
#: inheriting a worn framing), and the last static shot asking for a person is
#: the other half of the same failure.
_WORN_ONLY_TERMS: Tuple[str, ...] = (
    "锁骨",
    "颈部",
    "脖颈",
    "领口",
    "肩上",
    "肩部",
    "已经佩戴",
    "已佩戴",
    "佩戴完成",
    "佩戴中",
)

#: The delivered shot fields that *ask* for something.  The sweeps over
#: foreign-zone words and worn body-zone words read only these, never the
#: 不得出现 lists or the 本段手机构图 line: those have to name the banned thing
#: in order to ban it, so including them turns a correct prohibition into a
#: reported defect.
_NECKLACE_POSITIVE_FIELDS: Tuple[str, ...] = (
    "画面事件",
    "人物动作",
    "自然反应",
    "视线关系",
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

# ── Final-prompt audit vocabulary ───────────────────────────────────────────
# Section 8's necklace assertions, checked on the text that is actually
# delivered to the video model rather than on the frozen contract.  R1 lived in
# the last step (splice + compaction), so a contract that was right at planning
# time is not evidence that the delivered prompt is still right.  The shared
# audit already covers the generic promises; these codes are the necklace ones.
NECKLACE_PROMPT_SHOT_COUNT = "NECKLACE_PROMPT_SHOT_COUNT"
NECKLACE_PROMPT_SHOT_ORDER = "NECKLACE_PROMPT_SHOT_ORDER"
NECKLACE_PROMPT_TIMELINE = "NECKLACE_PROMPT_TIMELINE"
NECKLACE_PROMPT_WORN_ZONE = "NECKLACE_PROMPT_WORN_ZONE"
NECKLACE_PROMPT_HANDHELD_CARRIER = "NECKLACE_PROMPT_HANDHELD_CARRIER"
NECKLACE_PROMPT_STATIC_CARRIER = "NECKLACE_PROMPT_STATIC_CARRIER"
NECKLACE_PROMPT_IDENTITY_REF = "NECKLACE_PROMPT_IDENTITY_REF"
NECKLACE_PROMPT_FOREIGN_ZONE = "NECKLACE_PROMPT_FOREIGN_ZONE"
#: v2 changes *what the audit asks*, not just its wording: v1 required the
#: frozen ``action`` clause verbatim in the delivered shot text and required the
#: four ``商品必须可见`` lines to be identical.  Both are impossible for a real
#: film (the 画面事件 is written by the generation model; the visible-anchor line
#: is per-shot by construction), so v1 reported every correct necklace film as
#: ``FAIL``.  The version is stamped into the delivered ``render_validation`` and
#: pinned by the sync consumer, so a v1 audit must not be read as a v2 one -- a
#: consumer that accepted v1 would be accepting a check that never ran.
NECKLACE_PROMPT_AUDIT_VERSION = "necklace-final-prompt-audit-v2"

#: The necklace profile's audit status is deliberately the same three-value
#: vocabulary the shared audit uses, so callers can merge the two reports
#: without learning a second state machine.
AUDIT_NOT_APPLICABLE = "NOT_APPLICABLE"
AUDIT_PASS = "PASS"
AUDIT_FAIL = "FAIL"


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


# ---------------------------------------------------------------------------
# Count evidence (F3)
# ---------------------------------------------------------------------------
# A count is a *claim*, and a claim needs a source and a polarity.  The judge
# used to ask "does the word 双层 appear anywhere in the approved anchor text?",
# which reads a negation as an affirmation: "不是单层" contains 单层 and was
# therefore admitted as a *single-layer* necklace.  That is the dangerous
# direction -- it lets a multi-layer product into the single-layer contract.
#
# Each count now carries local evidence shaped as the review asked for:
#
#     value      明确正整数或 null
#     status     VERIFIED / UNKNOWN / CONFLICT
#     source_ref 结构化字段路径或硬锚点位置
#     source_text 支持该数量的原句
#     polarity   AFFIRMED / NEGATED / UNCERTAIN
#
# ``polarity`` is decided *locally* -- the marker has to sit immediately next to
# the term it applies to -- so one 不是 at the far end of a sentence cannot
# invalidate every count in the paragraph.
#
# ``term`` and ``candidates`` are extra: the first says which spelling produced
# the count, the second names both sides of a conflict.  A reviewer can then
# open the anchor text and see the sentence instead of trusting a bare integer.

COUNT_STATUS_VERIFIED = "VERIFIED"
COUNT_STATUS_UNKNOWN = "UNKNOWN"
COUNT_STATUS_CONFLICT = "CONFLICT"

COUNT_POLARITY_AFFIRMED = "AFFIRMED"
COUNT_POLARITY_NEGATED = "NEGATED"
COUNT_POLARITY_UNCERTAIN = "UNCERTAIN"

#: What the judge reports as a count's provenance.  ``UNSOURCED`` is the point:
#: a bare integer handed in by a caller is never dressed up as
#: ``STRUCTURE_COUNTS`` evidence it does not have.  A number nobody sourced is
#: reported as a number nobody sourced.
COUNT_SOURCE_ANCHOR_FIELD = "ANCHOR_CARD_FIELD"
COUNT_SOURCE_ANCHOR_TEXT = "ANCHOR_TEXT"
COUNT_SOURCE_UNSOURCED = "UNSOURCED"

#: Markers that *end* exactly where the counted term begins.  Multi-character
#: on purpose: a bare 不 would fire on 不错 / 不少, so only real negations are
#: listed, and adjacency keeps "非常规双层" from being read as a negation.
_COUNT_NEGATION_MARKERS: Tuple[str, ...] = (
    "不是",
    "并非",
    "而不是",
    "并不是",
    "不为",
    "不属于",
    "算不上",
    "不算",
    "没有",
    "无",
    "未",
    "非",
)

#: Checked before the negations above: they answer a different question ("we do
#: not know" rather than "it is not"), and a hedge must not be read as a denial
#: -- nor as an affirmation.
_COUNT_UNCERTAINTY_MARKERS: Tuple[str, ...] = (
    "不能确认",
    "无法确认",
    "不能确定",
    "无法确定",
    "不能认定",
    "无法判断",
    "无法辨别",
    "尚不清楚",
    "不得而知",
    "不确定",
    "看不清",
)


def _strict_positive_int(raw: Any) -> Optional[int]:
    """A strictly positive integer, or ``None`` when the literal is not one.

    ``int(1.8)`` is 1, so a bare ``int()`` silently truncates a decimal into a
    plausible-looking count.  ``True`` is even worse: it *is* an ``int`` in
    Python, so a JSON ``true`` used to arrive at the judge as the count 1.
    Both are refused here, along with zero, negatives and anything non-numeric.
    """

    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return raw if raw > 0 else None
    if isinstance(raw, float):
        if raw != raw or raw in (float("inf"), float("-inf")):
            return None
        # An integral float states the value exactly; a decimal would be a
        # truncation, and a truncated count is a guessed count.
        return int(raw) if raw > 0 and raw.is_integer() else None
    if isinstance(raw, str):
        text = raw.strip()
        return int(text) if text.isdigit() and int(text) > 0 else None
    return None


def _unknown_count_evidence(key: str, *, ref: str = "", text: str = "") -> Dict[str, Any]:
    return {
        "value": None,
        "status": COUNT_STATUS_UNKNOWN,
        "source_ref": ref or f"counts.{key}",
        "source_text": _text(text),
        "polarity": COUNT_POLARITY_UNCERTAIN,
        "term": "",
        "candidates": [],
    }


def _count_provenance_label(evidence: Mapping[str, Any] | None) -> str:
    """Name where a count came from, without inventing a source."""

    ref = _text((evidence or {}).get("source_ref"))
    if ref.startswith("anchor_card."):
        return COUNT_SOURCE_ANCHOR_FIELD
    if ref.startswith("hard_anchors"):
        return COUNT_SOURCE_ANCHOR_TEXT
    return COUNT_SOURCE_UNSOURCED


def _looks_like_count_evidence(value: Any) -> bool:
    """Whether a mapping is one count's local evidence rather than a count."""

    return isinstance(value, Mapping) and _text(value.get("status")) in {
        COUNT_STATUS_VERIFIED,
        COUNT_STATUS_UNKNOWN,
        COUNT_STATUS_CONFLICT,
    }


def _normalise_count_evidence(recorded: Mapping[str, Any]) -> Dict[str, Any]:
    """A copy with every field present, so callers never see a ragged shape."""

    return {
        "value": recorded.get("value"),
        "status": _text(recorded.get("status")),
        "source_ref": _text(recorded.get("source_ref")),
        "source_text": _text(recorded.get("source_text")),
        "polarity": _text(recorded.get("polarity")) or COUNT_POLARITY_UNCERTAIN,
        "term": _text(recorded.get("term")),
        "candidates": [dict(item) for item in (recorded.get("candidates") or [])],
    }


def _count_evidence_of(counts: Mapping[str, Any] | None, key: str) -> Dict[str, Any]:
    """Read one count's evidence out of a ``counts`` mapping.

    Three shapes arrive here.  The necklace profile produces the evidence itself
    and stores it beside the legacy integer (``{key}_count_evidence``); a caller
    may hand in the evidence map straight from
    :func:`resolve_necklace_count_evidence` (``{key: evidence}``); and a caller
    may still hand in the legacy bare integer, which stays readable at the
    boundary but is labelled ``UNSOURCED`` rather than being given provenance it
    never had.
    """

    if not isinstance(counts, Mapping):
        return _unknown_count_evidence(key)

    for holder in (f"{key}_count_evidence", f"{key}_evidence"):
        recorded = counts.get(holder)
        if _looks_like_count_evidence(recorded):
            return _normalise_count_evidence(recorded)

    inline = counts.get(key)
    if _looks_like_count_evidence(inline):
        return _normalise_count_evidence(inline)

    if key not in counts:
        return _unknown_count_evidence(key)

    value = _strict_positive_int(counts.get(key))
    if value is None:
        # An illegal literal is not a count, and it is not a silent zero either.
        return _unknown_count_evidence(
            key, ref=f"counts.{key}", text=_text(counts.get(key))
        )

    legacy_source = _text(counts.get(f"{key}_source"))
    if legacy_source.startswith("ANCHOR_TEXT"):
        source_ref = "hard_anchors"
    elif legacy_source.startswith("ANCHOR_CARD_FIELD"):
        source_ref = f"anchor_card.{key}"
    else:
        source_ref = f"counts.{key}"
    return {
        "value": value,
        "status": COUNT_STATUS_VERIFIED,
        "source_ref": source_ref,
        "source_text": legacy_source,
        "polarity": COUNT_POLARITY_AFFIRMED,
        "term": "",
        "candidates": [],
    }


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
#
# F3 adds one more separation on top: a count is read as a *local assertion*
# with a polarity, not as a keyword hit.  "有吊坠" proves a pendant exists and
# says nothing about how many, and "不是双层" is a denial of two layers rather
# than a statement of two.  Both facts are recorded on the evidence so the
# refusal (or the admission) can be audited sentence by sentence.


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


def _polarity_at(text: str, start: int, term: str) -> str:
    """Whether the term at ``start`` is affirmed, negated or hedged.

    Only *adjacent* markers count.  A negation three characters upstream (or a
    clause later) belongs to a different object, and treating it as applying
    here is how "不是银色的，单层链条" would lose its single-layer reading.
    """

    head = text[:start]
    tail = text[start + len(term):]
    for marker in _COUNT_UNCERTAINTY_MARKERS:
        if head.endswith(marker) or tail.startswith(marker):
            return COUNT_POLARITY_UNCERTAIN
    for marker in _COUNT_NEGATION_MARKERS:
        if head.endswith(marker):
            return COUNT_POLARITY_NEGATED
    return COUNT_POLARITY_AFFIRMED


def _count_assertions(
    texts: Sequence[str],
    term_groups: Mapping[str, Any] | None,
) -> List[Dict[str, Any]]:
    """Every counted term the approved anchor text states, with its polarity.

    Every occurrence is recorded rather than only the largest, because the
    question is no longer "which number is biggest" but "what does the text
    actually claim", and a text can claim two different things.
    """

    if not isinstance(term_groups, Mapping):
        return []

    assertions: List[Dict[str, Any]] = []
    for index, raw_text in enumerate(texts):
        text = _text(raw_text)
        if not text:
            continue
        for label, terms in term_groups.items():
            try:
                value = int(_text(label))
            except (TypeError, ValueError):
                continue
            for raw_term in terms or []:
                term = _text(raw_term)
                if not term:
                    continue
                start = text.find(term)
                while start != -1:
                    assertions.append(
                        {
                            "value": value,
                            "term": term,
                            "polarity": _polarity_at(text, start, term),
                            "source_ref": f"hard_anchors[{index}]",
                            "source_text": text,
                        }
                    )
                    start = text.find(term, start + 1)
    return assertions


def _one_count_evidence(
    *,
    card: Mapping[str, Any],
    texts: Sequence[str],
    key: str,
    terms: Mapping[str, Any] | None,
) -> Dict[str, Any]:
    """One field's count, read from the structured field and the anchor text."""

    field_value = None
    field_key = ""
    field_raw: Any = ""
    for candidate in (key, f"structure_{key}"):
        if candidate not in card:
            continue
        value = _strict_positive_int(card.get(candidate))
        if value is not None:
            field_value, field_key, field_raw = value, candidate, card.get(candidate)
            break

    assertions = _count_assertions(texts, terms)
    affirmed: List[Tuple[int, Dict[str, Any]]] = []
    negated_values = set()
    for item in assertions:
        if item["polarity"] == COUNT_POLARITY_AFFIRMED:
            if item["value"] not in [value for value, _ in affirmed]:
                affirmed.append((item["value"], item))
        elif item["polarity"] == COUNT_POLARITY_NEGATED:
            negated_values.add(item["value"])

    affirmed_values = {value for value, _ in affirmed}

    def conflict(candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
        return {
            "value": None,
            "status": COUNT_STATUS_CONFLICT,
            "source_ref": " + ".join(item["source_ref"] for item in candidates),
            "source_text": " ｜ ".join(
                item["source_text"] for item in candidates if item["source_text"]
            ),
            "polarity": COUNT_POLARITY_UNCERTAIN,
            "term": "",
            "candidates": candidates,
        }

    if len(affirmed_values) > 1 or (affirmed_values & negated_values):
        # The text claims two different numbers for one field (or claims and
        # retracts the same one).  Which the product is cannot be decided here,
        # so it is reported as a conflict instead of being resolved by an
        # ordering rule nobody can see.
        return conflict(
            [
                {
                    "value": value,
                    "source_ref": item["source_ref"],
                    "source_text": item["source_text"],
                    "term": item["term"],
                }
                for value, item in affirmed
            ]
        )

    text_value = affirmed[0][0] if affirmed else None
    text_item = affirmed[0][1] if affirmed else None

    if field_value is not None:
        if text_value is not None and text_value != field_value:
            # §4.6: a structured field and an affirmative anchor must not be
            # silently ranked.  Keeping the conflict is what stops "字段为1、
            # 锚点明确双层" from shipping as a single-layer film.
            return conflict(
                [
                    {
                        "value": field_value,
                        "source_ref": f"anchor_card.{field_key}",
                        "source_text": _text(field_raw),
                        "term": "",
                    },
                    {
                        "value": text_value,
                        "source_ref": text_item["source_ref"],
                        "source_text": text_item["source_text"],
                        "term": text_item["term"],
                    },
                ]
            )
        return {
            "value": field_value,
            "status": COUNT_STATUS_VERIFIED,
            "source_ref": f"anchor_card.{field_key}",
            "source_text": _text(field_raw),
            "polarity": COUNT_POLARITY_AFFIRMED,
            "term": "",
            "candidates": [],
        }

    if text_value is not None:
        return {
            "value": text_value,
            "status": COUNT_STATUS_VERIFIED,
            "source_ref": text_item["source_ref"],
            "source_text": text_item["source_text"],
            "polarity": COUNT_POLARITY_AFFIRMED,
            "term": text_item["term"],
            "candidates": [],
        }

    # Nothing affirmatively states a count.  Distinguish "the text talked about
    # it and denied it" from "the text said nothing": a reviewer needs to see
    # that 不是双层 was *read*, not missed.
    if assertions and all(
        item["polarity"] == COUNT_POLARITY_NEGATED for item in assertions
    ):
        polarity = COUNT_POLARITY_NEGATED
    else:
        polarity = COUNT_POLARITY_UNCERTAIN
    first = assertions[0] if assertions else {}
    return {
        "value": None,
        "status": COUNT_STATUS_UNKNOWN,
        "source_ref": _text(first.get("source_ref")),
        "source_text": _text(first.get("source_text")),
        "polarity": polarity,
        "term": _text(first.get("term")),
        "candidates": [],
    }


def resolve_necklace_count_evidence(
    *,
    anchor_card: Mapping[str, Any] | None = None,
    anchor_texts: Iterable[Any] | None = None,
) -> Dict[str, Dict[str, Any]]:
    """Local evidence for each structure count, keyed by count field.

    Reads only the two shapes §4.1 allows: the anchor card's structured field
    (with its field path as ``source_ref``) and the *approved* hard anchors
    (with the anchor index).  A title or a display anchor never takes part --
    ``anchor_evidence_texts`` is the filter, and it excludes display anchors by
    construction, so omitting ``anchor_texts`` reads the card's approved
    anchors rather than reading nothing.
    """

    rule = necklace_v1_eligibility_rule()
    card = anchor_card if isinstance(anchor_card, Mapping) else {}
    if anchor_texts is None:
        from core.accessory_mixed_templates import anchor_evidence_texts

        anchor_texts = anchor_evidence_texts(card)
    texts = [_text(item) for item in (anchor_texts or []) if _text(item)]

    return {
        key: _one_count_evidence(
            card=card, texts=texts, key=key, terms=rule.get(term_field)
        )
        for key, term_field in (
            (
                _text(rule.get("layer_count_key")) or "layer_count",
                "layer_count_terms",
            ),
            (
                _text(rule.get("pendant_count_key")) or "pendant_count",
                "pendant_count_terms",
            ),
        )
    }


def resolve_necklace_structure_counts(
    *,
    anchor_card: Mapping[str, Any] | None = None,
    anchor_texts: Iterable[Any] | None = None,
) -> Dict[str, Any]:
    """Layer / pendant counts, each carrying the source it was read from.

    Two representations, on purpose.  ``{key}`` stays a bare integer when the
    count is ``VERIFIED`` so existing callers keep working unchanged; the
    trailing ``*_source`` keys make the number reviewable; and
    ``{key}_count_evidence`` carries the full local evidence -- value, status,
    source, sentence and polarity -- which is what the judge actually reads.
    A count that is not established gets no bare integer at all.

    When ``anchor_texts`` is omitted the card's own *approved* anchors are read
    (``anchor_evidence_texts``, which excludes display anchors).  Asking "what
    counts does this card support?" and getting ``UNKNOWN`` back because the
    caller did not also pass the card's own text would be a trap, not a
    safeguard.
    """

    rule = necklace_v1_eligibility_rule()
    card = anchor_card if isinstance(anchor_card, Mapping) else {}
    evidence = resolve_necklace_count_evidence(
        anchor_card=card, anchor_texts=anchor_texts
    )

    out: Dict[str, Any] = {}
    for key in (
        _text(rule.get("layer_count_key")) or "layer_count",
        _text(rule.get("pendant_count_key")) or "pendant_count",
    ):
        item = evidence[key]
        if item["status"] == COUNT_STATUS_VERIFIED and item["value"] is not None:
            out[key] = item["value"]
            if _text(item.get("term")):
                out[f"{key}_source"] = f"ANCHOR_TEXT:{item['term']}"
            else:
                out[f"{key}_source"] = COUNT_SOURCE_ANCHOR_FIELD
        out[f"{key}_count_evidence"] = item
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
    "pendant_count", "layer_count_evidence", "pendant_count_evidence",
    "unresolved"}``.  Nothing is inferred from the product name: chain and
    pendant must be *confirmed*, and both counts must have a traceable source.
    Anything unestablished is reported as ``UNKNOWN`` and refuses the profile --
    an explicit, reviewable refusal instead of a coin flip that later ships
    three stacked layers under a "single layer" contract.

    A count that two sources disagree about is reported as ``CONFLICT`` and also
    refuses.  Resolving it by precedence would look identical in the output to
    having a single agreeing source, which is exactly the kind of quiet
    promotion this judge exists to prevent.
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

    layer_evidence = _count_evidence_of(counts, layer_key)
    pendant_evidence = _count_evidence_of(counts, pendant_key)

    for key, evidence, maximum in (
        (layer_key, layer_evidence, max_layers),
        (pendant_key, pendant_evidence, max_pendants),
    ):
        value = evidence.get("value")
        if evidence["status"] == COUNT_STATUS_CONFLICT:
            state = COUNT_STATUS_CONFLICT
        elif value is None:
            state = EVIDENCE_UNKNOWN
        else:
            state = str(value)
        refs.append({
            "part": key,
            "state": state,
            "required": maximum,
            # The provenance is read off the evidence, so a bare integer with
            # no recorded source reports ``UNSOURCED`` instead of borrowing a
            # ``STRUCTURE_COUNTS`` label it never earned.
            "source": _count_provenance_label(evidence),
            "ref": _text(evidence_ref),
            "polarity": _text(evidence.get("polarity")),
            "term": _text(evidence.get("term")),
            "source_text": _text(evidence.get("source_text")),
            "source_ref": _text(evidence.get("source_ref")),
        })

    layer_count = layer_evidence.get("value")
    pendant_count = pendant_evidence.get("value")

    for key, evidence, maximum in (
        (layer_key, layer_evidence, max_layers),
        (pendant_key, pendant_evidence, max_pendants),
    ):
        if evidence["status"] == COUNT_STATUS_CONFLICT:
            # A conflict is not a value and must not be resolved by precedence.
            unresolved.append(f"{key}:{COUNT_STATUS_CONFLICT}")
        elif evidence.get("value") is None:
            unresolved.append(f"{key}:UNKNOWN")
        elif evidence["value"] != maximum:
            unresolved.append(f"{key}:{evidence['value']}")

    if unresolved:
        return {
            "eligible": False,
            "reason": f"{NECKLACE_EVIDENCE_INCOMPLETE}:{','.join(unresolved)}",
            "evidence_refs": refs,
            "layer_count": layer_count,
            "pendant_count": pendant_count,
            "layer_count_evidence": layer_evidence,
            "pendant_count_evidence": pendant_evidence,
            "unresolved": unresolved,
        }
    return {
        "eligible": True,
        "reason": NECKLACE_V1_ELIGIBLE,
        "evidence_refs": refs,
        "layer_count": layer_count,
        "pendant_count": pendant_count,
        "layer_count_evidence": layer_evidence,
        "pendant_count_evidence": pendant_evidence,
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


# ---------------------------------------------------------------------------
# Final-prompt audit (section 8)
# ---------------------------------------------------------------------------


def audit_necklace_final_prompt(
    prompt: Any,
    contract: Mapping[str, Any] | None,
    *,
    renderer_version: str = "",
) -> Dict[str, Any]:
    """Audit the *delivered* prompt against the frozen necklace contract.

    Runs on the text the video model actually receives -- after splicing and
    after compaction -- because a contract that was correct at planning time is
    no evidence about the prompt.  The shared
    ``audit_mixed_final_execution`` already owns the generic promises (four
    shots, 15 seconds, MIXED carrier, no face); this one owns the necklace ones:

    * shot order and shot timeline,
    * the two worn shots staying on the neck zone,
    * the hand-held shot carrying no body-zone wording,
    * the static last shot carrying no person,
    * one unchanged product across all shots,
    * no ear / wrist / hair action coming back.

    Every assertion is made against something the *renderer* owns -- the frozen
    contract, the header splice, or the lines the renderer writes from the
    frozen unit.  Nothing here demands that the generation model copy frozen
    wording into its own prose; see the two markers below for why that
    distinction is the whole ballgame.

    Returns the same shape as the shared audit so callers can merge the two
    without learning a second state machine.  A non-necklace contract reports
    ``NOT_APPLICABLE`` and costs nothing.

    Only the delivered *text* is judged here.  Whether the chain physically
    breaks or clips through the neck in the finished video stays a human review
    item, and this audit must never be reported as having verified it.
    """

    from core.accessory_mixed_templates import (
        format_mixed_shot_time_range,
        frozen_unit_timeline,
        mixed_shot_camera_line,
        parse_final_shot_blocks,
    )

    text = _text(prompt)
    report: Dict[str, Any] = {
        "version": NECKLACE_PROMPT_AUDIT_VERSION,
        "status": AUDIT_NOT_APPLICABLE,
        "issues": [],
        "issue_count": 0,
        "checked_shots": 0,
        "prompt_hash": hashlib.sha256(text.encode("utf-8")).hexdigest()[:16],
        "renderer_version": _text(renderer_version),
        "reason": "",
    }

    data = contract if isinstance(contract, Mapping) else {}
    block = frozen_necklace_contract({"mixed_template_contract": data})
    if not block:
        report["reason"] = "NOT_NECKLACE_V1"
        return report

    # Which contract revision this audit actually cleared.  A consumer that only
    # sees the finished row has to tell "this prompt was audited against *this*
    # contract" from "this prompt carries an audit of some other revision", and
    # the audit is the only place that statement can live.  The values are read
    # from the *frozen* contract on purpose: the audit asserts things about the
    # frozen film, so a later config edit must not make an unchanged film look
    # like it needs re-auditing -- and re-exporting the same frozen contract has
    # to keep clearing it.
    #
    # Each value is copied with its own JSON type (``template_version`` and
    # ``feature_version`` are integers in the contract).  Stringifying one here
    # would make a consumer's equality check fail against the contract it is
    # supposed to agree with.
    report["feature_version"] = data.get("feature_version")
    report["profile_config_hash"] = _text(block.get("profile_config_hash"))
    report["template_version"] = data.get("template_version")

    units = [
        unit for unit in (data.get("capture_units") or []) if isinstance(unit, Mapping)
    ]

    def _issue(
        position: int,
        unit_id: str,
        field: str,
        code: str,
        actual: Any,
        detail: str,
    ) -> Dict[str, Any]:
        return {
            "shot_id": _text(unit_id) or f"CU_{position:02d}",
            "field": field,
            "module": "",
            "code": code,
            "actual": _text(actual)[:120],
            "detail": detail,
            "source": "RENDERER",
        }

    issues: List[Dict[str, Any]] = []
    blocks = parse_final_shot_blocks(text)
    if not units or not blocks:
        report["status"] = AUDIT_FAIL
        report["reason"] = "NO_DELIVERED_SHOTS"
        report["issues"] = [
            _issue(0, "", "镜块", NECKLACE_PROMPT_SHOT_COUNT, len(blocks),
                   "最终提示词里没有可核对的镜块")
        ]
        report["issue_count"] = 1
        return report

    if len(blocks) != len(units):
        issues.append(
            _issue(len(units), "", "镜块", NECKLACE_PROMPT_SHOT_COUNT, len(blocks),
                   f"交付镜块数 {len(blocks)} 与冻结合同的 {len(units)} 不一致")
        )

    timeline, _total = frozen_unit_timeline(data)
    identity_clause_sets: List[List[str]] = []

    for position, unit in enumerate(units, start=1):
        if position > len(blocks):
            break
        block = blocks[position - 1]
        unit_id = _text(unit.get("unit_id"))
        module = _text(unit.get("module"))
        lines = [_text(line) for line in (block.get("lines") or [])]
        fields = block.get("fields") or {}

        # Order, read as "is the shot this position owns actually the shot that
        # shipped here?".
        #
        # The marker is the shot's own 本段手机构图 line, which the renderer
        # writes from ``mixed_shot_camera_line(unit)`` -- "逐字取自该镜自己冻结
        # 的取景范围" -- so the delivered value can be compared byte for byte
        # with the frozen unit instead of being fuzzy-matched.  The four NMX
        # framings share no text, so this is a per-shot answer; the header is not,
        # because it lists every module of the film.
        #
        # The frozen ``action`` deliberately is *not* the marker.  The delivered
        # 画面事件 / 人物动作 are written by the generation model from the frozen
        # package: they realise the action semantically and never copy it word for
        # word.  Demanding the clause verbatim failed all four shots of the first
        # real production-shaped film (2026-09-19) while the film itself was
        # correct -- the same over-strictness the earlier compaction fix had
        # already hit once, one level deeper.  Whether an action was *respected*
        # is a separate question, answered on the contract by
        # ``validate_necklace_v1_contract`` and on the delivery by the shared
        # module-boundary audit -- not by string equality against model prose.
        framing_line = _text(fields.get("本段手机构图")) or _text(fields.get("手机机位"))
        expected_framing = _text(mixed_shot_camera_line(unit))
        if expected_framing and framing_line != expected_framing:
            issues.append(
                _issue(position, unit_id, "本段手机构图", NECKLACE_PROMPT_SHOT_ORDER,
                       framing_line,
                       f"第{position}镜（{module}）的本段手机构图与该镜冻结取景不一致"
                       f"（应为：{expected_framing}）")
            )

        expected_range = _text(format_mixed_shot_time_range(timeline.get(unit_id)))
        delivered_range = _text(block.get("time_range"))
        if expected_range and delivered_range != expected_range:
            issues.append(
                _issue(position, unit_id, "时间范围", NECKLACE_PROMPT_TIMELINE,
                       delivered_range, f"应为 {expected_range}")
            )

        # Foreign vocabulary must not come back through any shot.
        # Both sweeps read the *positive* instruction fields only.  The
        # 本段手机构图 line and every 不得出现 list must *name* what is banned
        # ("嘴部入画", "佩戴部位入画"), so sweeping the whole block would report a
        # correct prohibition as the defect -- section 8's own warning, and the
        # false positive this project has already paid for once.  Nothing is
        # lost by narrowing: that line is byte-compared against the frozen unit
        # above, so a foreign term spliced into it fails NECKLACE_PROMPT_SHOT_ORDER
        # anyway.
        positive = " ".join(
            [_text(block.get("header_role"))]
            + [_text(fields.get(label)) for label in _NECKLACE_POSITIVE_FIELDS]
        )
        for term in _FOREIGN_ZONE_TERMS:
            if term in positive:
                issues.append(
                    _issue(position, unit_id, "画面事件",
                           NECKLACE_PROMPT_FOREIGN_ZONE, term,
                           f"第{position}镜出现不属于本类目的动作/部位：{term}")
                )

        # The two non-worn shots must not inherit worn body-zone wording.
        if module in ("HANDHELD_PRODUCT", "STATIC_PRODUCT"):
            for term in _WORN_ONLY_TERMS:
                if term in positive:
                    code = (
                        NECKLACE_PROMPT_HANDHELD_CARRIER
                        if module == "HANDHELD_PRODUCT"
                        else NECKLACE_PROMPT_STATIC_CARRIER
                    )
                    issues.append(
                        _issue(position, unit_id, "画面事件", code, term,
                               f"{module} 镜不应出现佩戴身体区表述：{term}")
                    )

        # One product across every shot.
        #
        # The delivered line is ``商品必须可见：`` + the *shot's own* visible
        # anchors (``production_script_renderer`` reads them from the shot's
        # ``product_anchors_visible``), and those anchors are chosen per shot:
        # the worn shots state the落点, the hand-held shot states the pendant
        # detail, the static shot states the chain layout.  Requiring the four
        # lines to be *equal* therefore failed the first real film for the right
        # reason (they differ by design).  What "商品身份引用一致" actually means
        # is that the four shots are still talking about one product, so the
        # check is on the clause every shot still states -- the intersection.
        for line in lines:
            if line.startswith("商品必须可见：") or line.startswith("每段商品必须可见："):
                clauses = [
                    _text(part)
                    for part in line.split("：", 1)[-1].split("；")
                    if _text(part)
                ]
                if clauses:
                    identity_clause_sets.append(clauses)

    # Fewer than two declaring shots means compaction hoisted the shared line to
    # film level and left one copy (the I6 promotion the shared audit already
    # documents) -- there is nothing to compare and nothing to accuse.
    if len(identity_clause_sets) >= 2:
        shared_identity = set(identity_clause_sets[0])
        for clause_set in identity_clause_sets[1:]:
            shared_identity &= set(clause_set)
        if not shared_identity:
            issues.append(
                _issue(0, "", "商品必须可见", NECKLACE_PROMPT_IDENTITY_REF,
                       " | ".join(
                           "；".join(clause_set) for clause_set in identity_clause_sets
                       ),
                       "各镜声明的商品可见内容没有任何一条相同，无法确认全片是同一件商品")
            )

    report["issues"] = issues
    report["issue_count"] = len(issues)
    report["checked_shots"] = min(len(blocks), len(units))
    report["status"] = AUDIT_FAIL if issues else AUDIT_PASS
    report["reason"] = "NECKLACE_EXECUTION_CONFLICT" if issues else "OK"
    return report


def merge_prompt_audits(
    shared: Mapping[str, Any] | None,
    necklace: Mapping[str, Any] | None,
) -> Dict[str, Any]:
    """Fold the necklace audit into the shared one, keeping one status.

    The shared report keeps its own keys; the necklace issues are appended and
    counted separately so a reviewer can still tell which layer refused.  A
    ``NOT_APPLICABLE`` necklace audit changes nothing at all, which is what
    keeps every other category byte-identical.
    """

    base = dict(shared or {})
    extra = dict(necklace or {})
    if _text(extra.get("status")) == AUDIT_NOT_APPLICABLE:
        return base

    issues = list(base.get("issues") or []) + list(extra.get("issues") or [])
    base["issues"] = issues
    base["issue_count"] = len(issues)
    base["necklace_audit"] = extra
    if _text(extra.get("status")) == AUDIT_FAIL:
        base["status"] = AUDIT_FAIL
    return base


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
    "resolve_necklace_count_evidence",
    "resolve_necklace_structure_counts",
    "judge_necklace_eligibility",
    "COUNT_STATUS_VERIFIED",
    "COUNT_STATUS_UNKNOWN",
    "COUNT_STATUS_CONFLICT",
    "COUNT_POLARITY_AFFIRMED",
    "COUNT_POLARITY_NEGATED",
    "COUNT_POLARITY_UNCERTAIN",
    "COUNT_SOURCE_ANCHOR_FIELD",
    "COUNT_SOURCE_ANCHOR_TEXT",
    "COUNT_SOURCE_UNSOURCED",
    "resolve_necklace_v1_scope",
    "build_necklace_contract_block",
    "attach_necklace_contract",
    "frozen_necklace_contract",
    "validate_necklace_v1_contract",
    "audit_necklace_final_prompt",
    "merge_prompt_audits",
    "NECKLACE_PROMPT_AUDIT_VERSION",
]
