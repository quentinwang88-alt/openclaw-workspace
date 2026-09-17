"""Authored mixed-display template contract for short accessory originals.

This module owns one concern only: a fixed, human-authored 15-second shooting
template that guarantees every video contains handheld, static and worn (face
free) modules.  It is deliberately additive:

* Importing it has no side effects and touches no database.
* All behaviour is behind ``ORIGINAL_SCRIPT_ACCESSORY_MIXED_TEMPLATE_V1_ENABLED``
  which defaults to *off*, so existing apparel / scarf / long-form paths stay
  byte-for-byte unchanged.
* Product type resolution reuses ``normalize_product_type`` -- the single
  existing authority -- instead of any local Chinese keyword table.

The template owns *physical display choreography* only.  Narrative structure,
product facts and selling arguments keep their existing authorities.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from core.product_type_resolution import normalize_product_type


ACCESSORY_MIXED_TEMPLATE_ENV = "ORIGINAL_SCRIPT_ACCESSORY_MIXED_TEMPLATE_V1_ENABLED"
ACCESSORY_MIXED_TEMPLATE_PROFILE = "ACCESSORY_MIXED_TEMPLATE_V1"
ACCESSORY_MIXED_TEMPLATE_SCHEMA = "accessory-mixed-template-v1"
ACCESSORY_MIXED_TEMPLATE_SOURCE_MODE = "AUTHORED_TEMPLATE"
MIXED_TEMPLATE_CONTRACT_KEY = "mixed_template_contract"

# Single source for the pairing vocabulary.  ``category_execution.accessory``
# imports these for its script validator, so the guard and the guidance can never
# drift apart.
#
# Why the guidance cares at all: the validator hard-fails an earring script whose
# *authored* content asserts a pairing relation the anchor card never granted.
# A guard sentence that names those terms is therefore unsafe to paraphrase into
# the blueprint prompt -- a model that copies it into the storyboard destroys an
# otherwise valid generation.  Guidance must state the relation, or state the
# prohibition, without ever spelling the forbidden words.
PAIRING_OUTPUT_TERMS: Tuple[str, ...] = ("一对", "成对", "一副", "双耳", "两只", "这对")
SINGLE_OUTPUT_TERMS: Tuple[str, ...] = ("单只", "单个", "单耳", "单边", "这一只")
_PAIRING_OUTPUT_TERMS_ALL: Tuple[str, ...] = PAIRING_OUTPUT_TERMS + SINGLE_OUTPUT_TERMS

_DEFINITION_PATH = (
    Path(__file__).resolve().parents[1] / "config" / "accessory_mixed_templates.json"
)

_TRUE_TOKENS = {"1", "true", "yes", "on"}

# Required per-unit fields for a valid compiled contract.
_UNIT_REQUIRED_FIELDS = (
    "unit_id",
    "duration_seconds",
    "module",
    "carrier_mode",
    "product_state",
    "body_zone",
    "action",
    "observation_job",
    "edit_before",
    "continuity_group",
)

# ``body_zone`` only exists for the two worn modules.  A handheld shot is
# hand-and-product and a static shot is product-and-surface, so neither has a
# body zone to name -- requiring one is what used to force the wrist zone (or
# the ear) onto every shot of the video.
_BODY_ZONE_MODULES = ("WORN_DETAIL", "WORN_RELATION")


def _unit_uses_body_zone(unit: Mapping[str, Any]) -> bool:
    scope = _text(unit.get("view_scope")).upper()
    if scope:
        return scope.startswith("BODY_ZONE")
    return _text(unit.get("module")).upper() in _BODY_ZONE_MODULES

_THEME_REQUIRED_FIELDS = (
    "theme_id",
    "candidate_role",
    "thesis",
)

# ── Hard error vocabulary ───────────────────────────────────────────────────
# One place for the codes that must *stop* a mixed item instead of being logged.
# Callers persist them verbatim (planning deferral reason, execution failure
# code), so a reviewer can tell which boundary refused the item.
ERR_MIXED_CONTRACT_MISSING = "MIXED_CONTRACT_MISSING"
ERR_MIXED_SCOPE_UNSUPPORTED = "MIXED_SCOPE_UNSUPPORTED"
ERR_MIXED_STRUCTURE_INCOMPATIBLE = "MIXED_STRUCTURE_INCOMPATIBLE"
ERR_MIXED_STRUCTURE_BEATS_MISSING = "MIXED_STRUCTURE_BEATS_MISSING"
ERR_MIXED_UNIT_ID_MISMATCH = "MIXED_UNIT_ID_MISMATCH"
ERR_MIXED_UNIT_COUNT_MISMATCH = "MIXED_UNIT_COUNT_MISMATCH"
ERR_MIXED_TIMELINE_MISMATCH = "MIXED_TIMELINE_MISMATCH"
ERR_MIXED_CARRIER_MISMATCH = "MIXED_CARRIER_MISMATCH"
ERR_MIXED_STATE_MISMATCH = "MIXED_STATE_MISMATCH"
ERR_MIXED_UNIT_DURATION_INVALID = "MIXED_UNIT_DURATION_INVALID"
ERR_MIXED_PART_CONTRADICTION = "MIXED_PART_CONTRADICTION"
ERR_MIXED_THEME_INPUT_GAP = "MIXED_THEME_INPUT_GAP"

# Where a readable ``thesis`` may legitimately come from, most authoritative
# first.  These are *source field paths*, not IDs: the point of the axis is that
# a reviewer can go read the sentence the theme was copied from.
MIXED_THESIS_INPUT_GAP_READABILITY = "THESIS_NOT_READABLE"
_MIXED_THESIS_UNREADABLE_SENTINELS = frozenset(
    {"UNAVAILABLE", "UNKNOWN", "N/A", "NA", "NONE", "NULL", "TBD"}
)
_MIXED_THESIS_ID_PREFIXES = (
    "ARGUMENT_OPERATOR_",
    "OPERATOR_",
    "PCL_",
    "PCS_",
    "CLM_",
    "CBR_",
    "TH_",
)

# ---------------------------------------------------------------------------
# Part evidence (Review #6)
# ---------------------------------------------------------------------------
# An authored action may only ask for a part the product actually has.  Three
# states are distinguished because they license different behaviour:
#
# ``VERIFIED`` -- the registry guarantees the form, or an approved anchor names
#   the part.  Displaying it is authorised.
# ``ABSENT``   -- the registry excludes the form, or an approved anchor negates
#   it.  Naming it in an action is a factual error (a solid bangle has no chain
#   links), so it is a hard validation error.
# ``UNKNOWN``  -- nothing authoritative has been said either way.  This is
#   deliberately not ABSENT: a missing statement must not authorise the part's
#   display, but it must not forbid observing the part's plain appearance
#   either, so the action degrades to silhouette / colour / surface texture.
EVIDENCE_VERIFIED = "VERIFIED"
EVIDENCE_ABSENT = "ABSENT"
EVIDENCE_UNKNOWN = "UNKNOWN"
EVIDENCE_STATES: Tuple[str, ...] = (
    EVIDENCE_VERIFIED,
    EVIDENCE_ABSENT,
    EVIDENCE_UNKNOWN,
)
EVIDENCE_SOURCE_ANCHOR_UNCERTAIN = "ANCHOR_UNCERTAIN"

# How a text says "I cannot tell".  A hedged statement must never be read as a
# confirmation: "搭扣结构无法确认" contains the positive term "搭扣", so keyword
# matching alone recorded the clasp as VERIFIED and then authorised the gated
# action that displays it.  A hedge resolves to UNKNOWN -- never ABSENT, because
# "cannot tell" is not "there is none", and the two are different instructions to
# the camera.
UNCERTAINTY_TERMS_KEY = "evidence_uncertainty_terms"
DEFAULT_UNCERTAINTY_TERMS: Tuple[str, ...] = (
    "无法确认",
    "不能确认",
    "没法确认",
    "难以确认",
    "无法判断",
    "不能判断",
    "无法核实",
    "有待确认",
    "待确认",
    "看不清",
    "看不出来",
    "看不到",
    "不清楚",
    "不明确",
    "不确定",
    "不详",
    "未提供",
    "未说明",
    "没有提供",
    "未见",
    "未知",
)

STRUCTURE_FACTS_KEY = "structure_facts"
PART_EVIDENCE_KEY = "part_evidence"
PART_GATED_ACTIONS_KEY = "part_gated_actions"

# The projection is the one place that decides whether a compiled script may
# move on to the voiceover / first-frame / production stages.
PROJECTION_STATUS_APPLIED = "APPLIED"
PROJECTION_STATUS_SKIPPED = "SKIPPED"
PROJECTION_STATUS_MISMATCH = "MISMATCH"

# Version stamped next to the projected per-shot timeline, so a request made
# against an older mapping can be told apart from a current one.
TIMELINE_PROJECTION_VERSION = "mixed-timeline-v1"


def _text(value: Any) -> str:
    return str(value or "").strip()


def _flag_enabled(value: Any) -> bool:
    return _text(value).lower() in _TRUE_TOKENS


def accessory_mixed_template_enabled(enabled: Optional[bool] = None) -> bool:
    """Return whether the authored mixed-accessory mode is active.

    ``enabled`` is an explicit override used by callers that already resolved
    the request level switch.  When omitted the environment is consulted and
    the default is off.
    """

    if enabled is not None:
        return bool(enabled)
    return _flag_enabled(os.environ.get(ACCESSORY_MIXED_TEMPLATE_ENV, "0"))


@lru_cache(maxsize=1)
def load_mixed_template_definition() -> Dict[str, Any]:
    """Load and cache the authored template definition."""

    with _DEFINITION_PATH.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("accessory_mixed_templates.json 顶层必须是对象")
    return payload


def mixed_supported_canonical_types() -> set:
    """Canonical product types that may use the mixed template mode."""

    definition = load_mixed_template_definition()
    return set((definition.get("canonical_type_to_zone") or {}).keys())


def resolve_mixed_zone(
    product_type: str, top_category: str = ""
) -> Tuple[Optional[str], str]:
    """Resolve the mixed-display body zone for one product.

    Returns ``(zone, canonical_type)``.  ``zone`` is ``None`` when the product
    is not one of the four supported accessory families.

    Two input shapes are accepted, because callers upstream hold different
    things:

    * an operator-facing value (``"耳饰"``, ``"鲨鱼夹"``) which must go through
      the shared registry, and
    * an already-normalised ``canonical_type`` (``"earring"``, ``"claw_clip"``)
      as produced by earlier stages.

    The direct canonical lookup is required because ``normalize_product_type``
    does *not* treat bare English canonical names as aliases -- it silently
    falls back to ``womenwear`` for them.  A direct hit is only trusted for the
    exact registered spellings, so unknown products are still refused rather
    than guessed.
    """

    zones = load_mixed_template_definition().get("canonical_type_to_zone") or {}
    raw = _text(product_type).lower()
    if raw in zones:
        return zones[raw], raw
    if not _text(product_type) and not _text(top_category):
        return None, ""
    resolved = normalize_product_type(product_type, top_category)
    canonical = _text(resolved.canonical_type).lower()
    if canonical in zones:
        return zones[canonical], canonical
    return None, canonical


def is_accessory_mixed_type(product_type: str, top_category: str = "") -> bool:
    zone, _ = resolve_mixed_zone(product_type, top_category)
    return zone is not None


def list_mixed_templates() -> List[Dict[str, Any]]:
    definition = load_mixed_template_definition()
    templates = definition.get("templates") or []
    return [dict(item) for item in templates if isinstance(item, Mapping)]


def mixed_template_ids() -> List[str]:
    return [str(item.get("template_id") or "") for item in list_mixed_templates()]


def default_template_id() -> str:
    ids = mixed_template_ids()
    if not ids:
        raise ValueError("模板定义未包含任何 template_id")
    return ids[0]


def get_mixed_template(template_id: str) -> Dict[str, Any]:
    wanted = _text(template_id) or default_template_id()
    for item in list_mixed_templates():
        if _text(item.get("template_id")) == wanted:
            return item
    raise ValueError(f"未知的饰品混合模板: {wanted}")


def select_template_id(index: int) -> str:
    """Deterministically rotate templates so a batch spreads across A/B/C."""

    ids = mixed_template_ids()
    if not ids:
        raise ValueError("模板定义未包含任何 template_id")
    return ids[int(index) % len(ids)]


def select_environment_recipe_id(index: int) -> str:
    definition = load_mixed_template_definition()
    recipes = definition.get("environment_recipes") or {}
    ids = [str(key) for key in recipes.keys()]
    if not ids:
        raise ValueError("模板定义未包含任何 environment_recipe")
    return ids[int(index) % len(ids)]


def get_environment_recipe(recipe_id: str) -> Dict[str, Any]:
    definition = load_mixed_template_definition()
    recipes = definition.get("environment_recipes") or {}
    recipe = recipes.get(_text(recipe_id))
    if not isinstance(recipe, Mapping):
        raise ValueError(f"未知的饰品光影配方: {recipe_id}")
    return dict(recipe)


def _category_rule(zone: str) -> Dict[str, Any]:
    rules = load_mixed_template_definition().get("category_rules") or {}
    rule = rules.get(zone)
    if not isinstance(rule, Mapping):
        raise ValueError(f"缺少类目局部规则: {zone}")
    return dict(rule)


def _action_for_module(rule: Mapping[str, Any], module: str) -> str:
    actions = rule.get("action_by_module") or {}
    return _text(actions.get(module)) or _text(rule.get("base_action"))


# ---------------------------------------------------------------------------
# Module-level framing (Review #5)
# ---------------------------------------------------------------------------
# The category rule used to be copied verbatim onto every shot module, so a
# bracelet's static shot still inherited the wrist zone, and the shared no-face
# prose was written as if every product hung off an ear.  Framing is therefore
# split into two layers:
#
# * the *module* layer says what kind of frame this is -- hand-and-product,
#   product-and-surface, or body zone -- and owns its own allowed/forbidden
#   lists for the two non-body modules;
# * the *category* layer supplies the body zone, and only the two worn modules
#   read it.
#
# Everything that describes a shot reads its framing from
# ``project_module_framing`` so a shot cannot be described with one body zone
# while being executed as another module.


def mixed_first_frame_facts(contract: Mapping[str, Any] | None) -> Dict[str, Any]:
    """First-frame-relevant slice of a frozen mixed contract.

    Kept deliberately small: only the opening shot can affect a still frame, so
    only that shot enters the first-frame fingerprint.  Hashing the whole
    contract would invalidate every cached first frame whenever an unrelated
    later shot's prose changed.
    """

    source = contract if isinstance(contract, Mapping) else {}
    units = [item for item in (source.get("capture_units") or []) if isinstance(item, Mapping)]
    unit = dict(units[0]) if units else {}
    return {
        "template_id": _text(source.get("template_id")),
        "face_policy": _text(source.get("face_policy")).upper(),
        "environment_recipe_id": _text(source.get("environment_recipe_id")),
        "opening_unit": {
            "unit_id": _text(unit.get("unit_id")),
            "module": _text(unit.get("module")),
            "view_scope": _text(unit.get("view_scope")),
            "view_label": _text(unit.get("view_label")),
            "carrier_mode": _text(unit.get("carrier_mode")),
            "allowed_framing": [
                _text(item)
                for item in (unit.get("allowed_framing") or [])
                if _text(item)
            ],
            "forbidden_framing": [
                _text(item)
                for item in (unit.get("forbidden_framing") or [])
                if _text(item)
            ],
        },
    }


def frozen_mixed_contract(container: Mapping[str, Any] | None) -> Dict[str, Any]:
    """The frozen mixed contract carried by a script, brief, or extension.

    Accepts a mapping that either *is* the category-execution extension or holds
    one under ``category_execution_extension``.  Returns ``{}`` when there is no
    contract, and callers must treat that as "the mixed mode owns nothing here" --
    it has to keep every legacy accessory path byte-for-byte unchanged.

    Existence of this dict, never an environment variable, is what makes a task a
    mixed-mode task.  Reading the switch instead would let an async first-frame
    worker with a different environment change the framing of an already frozen
    task, and would change legacy accessory tasks the moment the switch flips on.
    """

    data = container if isinstance(container, Mapping) else {}
    candidate: Any = data.get(MIXED_TEMPLATE_CONTRACT_KEY)
    if not isinstance(candidate, Mapping) or not candidate:
        extension = data.get("category_execution_extension")
        candidate = (
            extension.get(MIXED_TEMPLATE_CONTRACT_KEY)
            if isinstance(extension, Mapping)
            else None
        )
    return dict(candidate) if isinstance(candidate, Mapping) and candidate else {}


def physical_subtype_rule(canonical_type: str) -> Dict[str, Any]:
    """The physical-form rule for one canonical registry type (may be empty)."""

    rules = load_mixed_template_definition().get("physical_subtype_rules") or {}
    rule = rules.get(_text(canonical_type))
    return dict(rule) if isinstance(rule, Mapping) else {}


def subtype_structure_facts(canonical_type: str) -> Dict[str, str]:
    """Registry-level structural facts, normalised onto the three evidence states.

    The subtype registry is authoritative about its own form: a ``bangle`` is a
    rigid ring and genuinely has no chain.  Anything the registry does not
    settle stays ``UNKNOWN`` rather than being guessed in either direction.
    """

    facts: Dict[str, str] = {}
    raw = physical_subtype_rule(canonical_type).get(STRUCTURE_FACTS_KEY) or {}
    for key, value in raw.items():
        name = _text(key)
        state = _text(value).upper()
        if name and state in EVIDENCE_STATES:
            facts[name] = state
    return facts


def _part_terminology(part_key: str) -> Dict[str, Any]:
    terms = load_mixed_template_definition().get("part_evidence_terms") or {}
    entry = terms.get(_text(part_key))
    return dict(entry) if isinstance(entry, Mapping) else {}


def uncertainty_terms() -> Tuple[str, ...]:
    """How the approved anchors say "cannot tell" (config-first, code fallback)."""

    raw = load_mixed_template_definition().get(UNCERTAINTY_TERMS_KEY)
    terms = tuple(_text(item) for item in (raw or []) if _text(item))
    return terms or DEFAULT_UNCERTAINTY_TERMS


def _hedged_about_part(
    texts: Sequence[str],
    part_terms: Sequence[str],
    uncertainty: Sequence[str],
) -> bool:
    """True when one text hedges about *this* part.

    Scoped to a single text on purpose: an unrelated "结构无法确认" elsewhere in
    the anchor card must not withdraw evidence for a part that another line
    states plainly.
    """

    for text in texts:
        if any(term in text for term in part_terms) and any(
            marker in text for marker in uncertainty
        ):
            return True
    return False


def project_module_framing(canonical_type: str, module: str) -> Dict[str, Any]:
    """The single projection for one ``(category, shot module)`` framing rule.

    Returns ``{}`` -- never a half-filled default -- in two cases:

    * the module is unknown, and
    * the module is ``CATEGORY_ZONE`` sourced but the category is unknown.

    The second case matters: a worn shot has no framing of its own to fall back
    on, so answering with an empty allowed-range list would hand consumers a
    "rule" that silently permits nothing.  Callers must decide explicitly
    instead of inheriting a guess.
    """

    module = _text(module).upper()
    rules = load_mixed_template_definition().get("module_framing_rules") or {}
    spec = rules.get(module)
    if not isinstance(spec, Mapping):
        return {}

    zone, _canonical = resolve_mixed_zone(canonical_type, "")
    category = _category_rule(zone) if zone else {}
    source = _text(spec.get("framing_source")).upper()

    if source == "CATEGORY_ZONE" and not zone:
        return {}

    out: Dict[str, Any] = {
        "module": module,
        "view_scope": _text(spec.get("view_scope")),
        "view_label": (
            _text(spec.get("view_label")) or _MODULE_LABELS.get(module) or module
        ),
        "framing_source": source,
        "state_boundary": _text(spec.get("state_boundary")),
        "visible_quantity_rule": _text(spec.get("visible_quantity_rule")),
        "zone_label": "",
        "body_zone": "",
        "allowed_framing": [],
        "forbidden_framing": [],
    }

    if source == "CATEGORY_ZONE":
        key = _text(spec.get("framing_key")) or module
        per_module = category.get("module_framing")
        per_module = per_module if isinstance(per_module, Mapping) else {}
        chosen = per_module.get(key)
        if not isinstance(chosen, list) or not chosen:
            chosen = list(category.get("allowed_framing") or [])
        out["allowed_framing"] = [_text(item) for item in chosen if _text(item)]
        # The face ban is a category guarantee covering both worn modules; only
        # the *allowed* list differs between a detail and a relation shot.
        out["forbidden_framing"] = [
            _text(item)
            for item in (category.get("forbidden_framing") or [])
            if _text(item)
        ]
        out["zone_label"] = _text(category.get("zone_label"))
        out["body_zone"] = _text(category.get("body_zone"))
        # A category may sharpen the shared crop rule (a paired earring must not
        # be required to show both in one crop).  More specific wins.
        category_rule = _text(category.get("visible_quantity_rule"))
        if category_rule:
            out["visible_quantity_rule"] = category_rule
        return out

    out["allowed_framing"] = [
        _text(item) for item in (spec.get("allowed_framing") or []) if _text(item)
    ]
    out["forbidden_framing"] = [
        _text(item) for item in (spec.get("forbidden_framing") or []) if _text(item)
    ]
    return out


def module_framing_projection(
    contract: Mapping[str, Any] | None,
) -> Dict[str, Dict[str, Any]]:
    """Per-module framing actually frozen into a contract, keyed by module."""

    out: Dict[str, Dict[str, Any]] = {}
    for unit in (contract or {}).get("capture_units") or []:
        if not isinstance(unit, Mapping):
            continue
        module = _text(unit.get("module"))
        if not module or module in out:
            continue
        out[module] = {
            "module": module,
            "view_label": (
                _text(unit.get("view_label")) or _MODULE_LABELS.get(module) or module
            ),
            "zone_label": _text(unit.get("framing")),
            "allowed_framing": [
                _text(item) for item in (unit.get("allowed_framing") or []) if _text(item)
            ],
            "forbidden_framing": [
                _text(item) for item in (unit.get("forbidden_framing") or []) if _text(item)
            ],
        }
    return out


def worn_body_framing(contract: Mapping[str, Any] | None) -> List[str]:
    """Body-zone framing union across the two worn modules.

    This is what replaces the hard-coded "耳侧、耳廓、耳垂、颈侧" sentence: the
    set is read off the frozen contract, so a wrist product is described with
    its own wrist framing instead of an ear.
    """

    out: List[str] = []
    for unit in (contract or {}).get("capture_units") or []:
        if not isinstance(unit, Mapping):
            continue
        if _text(unit.get("module")) not in {"WORN_DETAIL", "WORN_RELATION"}:
            continue
        for item in unit.get("allowed_framing") or []:
            text = _text(item)
            if text and text not in out:
                out.append(text)
    return out


def module_framing_legend_lines(contract: Mapping[str, Any] | None) -> List[str]:
    """Compact per-module framing statement: allowed range *and* bans.

    Stated per module so a reader can map 镜头 N -> module -> framing without
    guessing.  The two non-body modules carry their own bans -- a static shot
    may not show a hand, which is not a face rule -- while the worn modules
    share the category's face ban, which the no-face block states.
    """

    framing = module_framing_projection(contract)
    if not framing:
        return []

    face_bans: List[str] = []
    for module in ("WORN_DETAIL", "WORN_RELATION"):
        for item in (framing.get(module) or {}).get("forbidden_framing") or []:
            if item and item not in face_bans:
                face_bans.append(item)

    worn_parts: List[str] = []
    scope_parts: List[str] = []
    for module in ("WORN_DETAIL", "WORN_RELATION", "HANDHELD_PRODUCT", "STATIC_PRODUCT"):
        item = framing.get(module)
        if not item:
            continue
        label = item["view_label"]
        allowed = "、".join(item["allowed_framing"])
        if module in {"WORN_DETAIL", "WORN_RELATION"}:
            # Name the body zone so the worn row is self-describing.
            zone_label = _text(item.get("zone_label"))
            head = f"{label}（{zone_label}）" if zone_label else label
            worn_parts.append(f"{head}={allowed}" if allowed else head)
            continue
        scope_parts.append(f"{label}={allowed}" if allowed else label)
        bans = [
            value
            for value in item["forbidden_framing"]
            if value and value not in face_bans
        ]
        if bans:
            scope_parts.append(f"{label}不得出现" + "、".join(bans))

    lines: List[str] = []
    if worn_parts or scope_parts:
        lines.append(
            "镜头取景按模块执行（每镜只用自己那一行的范围）："
            + "；".join([*worn_parts, *scope_parts])
        )
    return lines


def resolve_part_evidence(
    part_key: str,
    *,
    structure_facts: Mapping[str, Any] | None = None,
    anchor_texts: Iterable[Any] | None = None,
) -> Dict[str, str]:
    """Tri-state evidence for one optional part.

    Resolution order -- each step exists because the one after it would otherwise
    over-claim:

    1. the structure registry, when it has an opinion (it is authoritative about
       its own physical form);
    2. a **hedge about this part** -> ``UNKNOWN``.  "搭扣结构无法确认" contains the
       positive term "搭扣", so keyword matching alone recorded the clasp as
       VERIFIED and then authorised the gated action that displays it.  A hedge is
       never promoted to a fact, and never demoted to ABSENT either: "cannot tell"
       is a different shooting instruction from "there is none";
    3. a negation such as "无搭扣" -> ``ABSENT`` (it contains the positive term as
       a substring, so negatives are scanned first);
    4. a plain positive term -> ``VERIFIED``;
    5. otherwise ``UNKNOWN``.
    """

    key = _text(part_key)
    facts = {
        _text(name): _text(state).upper()
        for name, state in (structure_facts or {}).items()
        if _text(name)
    }
    base = facts.get(key, EVIDENCE_UNKNOWN)
    if base not in EVIDENCE_STATES:
        base = EVIDENCE_UNKNOWN
    if base != EVIDENCE_UNKNOWN:
        return {"part_key": key, "state": base, "source": "STRUCTURE_REGISTRY"}

    texts = [_text(item) for item in (anchor_texts or []) if _text(item)]
    blob = "；".join(texts)
    if not blob:
        return {"part_key": key, "state": EVIDENCE_UNKNOWN, "source": "NO_EVIDENCE"}

    terms = _part_terminology(key)
    negatives = [_text(item) for item in (terms.get("negative_terms") or []) if _text(item)]
    positives = [_text(item) for item in (terms.get("positive_terms") or []) if _text(item)]

    # Every spelling that names this part, including those that only occur inside
    # a negated term ("无搭扣" names the clasp in order to deny it).
    part_terms = tuple(dict.fromkeys(positives + negatives))
    if _hedged_about_part(texts, part_terms, uncertainty_terms()):
        return {
            "part_key": key,
            "state": EVIDENCE_UNKNOWN,
            "source": EVIDENCE_SOURCE_ANCHOR_UNCERTAIN,
        }

    if any(term in blob for term in negatives):
        return {"part_key": key, "state": EVIDENCE_ABSENT, "source": "APPROVED_ANCHORS"}
    if any(term in blob for term in positives):
        return {"part_key": key, "state": EVIDENCE_VERIFIED, "source": "APPROVED_ANCHORS"}
    return {"part_key": key, "state": EVIDENCE_UNKNOWN, "source": "NO_EVIDENCE"}


def resolve_part_evidence_map(
    canonical_type: str,
    *,
    structure_facts: Mapping[str, Any] | None = None,
    anchor_texts: Iterable[Any] | None = None,
) -> Dict[str, Dict[str, str]]:
    """Evidence for every part that this subtype's gated actions depend on."""

    facts = dict(structure_facts or subtype_structure_facts(canonical_type))
    wanted: List[str] = []
    for candidate in physical_subtype_rule(canonical_type).get("optional_actions") or []:
        if not isinstance(candidate, Mapping):
            continue
        for part in candidate.get("requires") or {}:
            name = _text(part)
            if name and name not in wanted:
                wanted.append(name)
    return {
        part: resolve_part_evidence(part, structure_facts=facts, anchor_texts=anchor_texts)
        for part in wanted
    }


def resolve_part_gated_actions(
    canonical_type: str,
    *,
    evidence: Mapping[str, Mapping[str, Any]] | None = None,
) -> List[Dict[str, Any]]:
    """Gated action candidates whose required parts are all ``VERIFIED``.

    ``UNKNOWN`` is not treated as ``ABSENT``: the candidate is simply not
    issued, and the base subtype action (which never names the part) stands in
    its place.
    """

    out: List[Dict[str, Any]] = []
    for candidate in physical_subtype_rule(canonical_type).get("optional_actions") or []:
        if not isinstance(candidate, Mapping):
            continue
        requires = {
            _text(name): _text(state).upper()
            for name, state in (candidate.get("requires") or {}).items()
            if _text(name)
        }
        if not requires:
            continue
        resolved = {
            part: _text((evidence or {}).get(part, {}).get("state")).upper()
            for part in requires
        }
        if all(resolved.get(part) == want for part, want in requires.items()):
            out.append({
                "module": _text(candidate.get("module")).upper(),
                "action": _text(candidate.get("action")),
                "requires": dict(requires),
                "resolved": resolved,
            })
    return out


def anchor_evidence_texts(anchor_card: Mapping[str, Any] | None) -> List[str]:
    """Authoritative anchor text used to confirm an optional part exists.

    Only ``hard_anchors`` are read.  ``display_anchors`` may legitimately hold a
    model-authored presentation suggestion such as "手持展示吊坠", and treating
    that as proof of a pendant would turn a display idea into a product fact --
    the exact fabrication this guard exists to prevent.  Nothing is inferred
    from the product type or from the reference image alone.
    """

    card = anchor_card if isinstance(anchor_card, Mapping) else {}
    values: List[str] = []
    for item in card.get("hard_anchors") or []:
        if isinstance(item, Mapping):
            text = (
                item.get("anchor")
                or item.get("anchor_text")
                or item.get("name")
                or item.get("value")
            )
        else:
            text = item
        value = _text(text)
        if value and value not in values:
            values.append(value)
    return values


def attach_mixed_part_evidence(
    extension: Mapping[str, Any] | None,
    *,
    anchor_texts: Iterable[Any] | None = None,
) -> Dict[str, Any]:
    """Freeze anchor-derived part evidence next to the frozen contract.

    Called once at the seed boundary -- the only place that holds both the
    frozen contract and the approved anchors -- and written in place so the
    blueprint guidance and the renderer read the same answer.  A no-op without
    a mixed contract, which keeps every legacy extension unchanged.
    """

    target = extension if isinstance(extension, dict) else None
    if target is None:
        return {}
    contract = target.get(MIXED_TEMPLATE_CONTRACT_KEY)
    if not isinstance(contract, Mapping) or not contract:
        return {}
    if _text(contract.get("execution_profile")) != ACCESSORY_MIXED_TEMPLATE_PROFILE:
        return {}
    canonical = _text(contract.get("canonical_product_type"))
    facts = contract.get(STRUCTURE_FACTS_KEY)
    evidence = resolve_part_evidence_map(
        canonical,
        structure_facts=facts if isinstance(facts, Mapping) else None,
        anchor_texts=anchor_texts,
    )
    target[PART_EVIDENCE_KEY] = evidence
    return evidence


def _absent_part_terms(canonical_type: str) -> List[str]:
    """Part names that the registry has ruled out for this subtype.

    Their vocabulary must not appear in any authored action, or the model is
    told to display something the product does not have.
    """

    facts = subtype_structure_facts(canonical_type)
    blocked: List[str] = []
    for part, state in facts.items():
        if state != EVIDENCE_ABSENT:
            continue
        terms = _part_terminology(part)
        for term in terms.get("positive_terms") or []:
            text = _text(term)
            if text and text not in blocked:
                blocked.append(text)
    return blocked


def _normalize_theme(content_theme: Mapping[str, Any] | None) -> Dict[str, Any]:
    theme = dict(content_theme or {})
    theme_id = _text(theme.get("theme_id"))
    parent_theme_id = _text(theme.get("parent_theme_id")) or theme_id
    normalized = {
        "theme_id": theme_id,
        "parent_theme_id": parent_theme_id,
        "candidate_role": (_text(theme.get("candidate_role")) or "PRIMARY").upper(),
        "thesis": _text(theme.get("thesis")),
        "approved_claim_refs": [
            _text(item) for item in (theme.get("approved_claim_refs") or []) if _text(item)
        ],
        "evidence_refs": [
            _text(item) for item in (theme.get("evidence_refs") or []) if _text(item)
        ],
    }
    # ``thesis`` is a *readable proposition* about why this product is worth
    # buying; the internal argument IDs that used to be pasted into it now live
    # only in the source fields below.  Keeping them means "which argument was
    # this?" stays auditable, while the theme text stops being an ID wearing a
    # theme's clothes (Review R4: two different themes were declared identical
    # because both carried the same opaque operator ID shape).
    provenance = {
        "argument_id": _text(theme.get("argument_id")),
        "thesis_source": _text(theme.get("thesis_source")),
        "thesis_source_ref": _text(theme.get("thesis_source_ref")),
        "thesis_input_gap": _text(theme.get("thesis_input_gap")),
    }
    return {**normalized, **provenance}


def compile_mixed_template_contract(
    *,
    product_type: str,
    top_category: str = "",
    template_id: str = "",
    content_theme: Mapping[str, Any] | None = None,
    product_identity_ref: str = "",
    local_body_style_ref: str = "",
    environment_recipe_id: str = "",
    structure_role_by_module: Mapping[str, str] | None = None,
) -> Dict[str, Any]:
    """Compile one frozen ``mixed_template_contract``.

    Raises ``ValueError`` for products outside the four supported families so a
    misrouted request fails loudly *before* any paid generation instead of
    silently falling back to the legacy self-shot path.
    """

    zone, canonical = resolve_mixed_zone(product_type, top_category)
    if zone is None:
        raise ValueError(
            f"商品类型 {canonical or product_type!r} 不属于饰品混合模板支持范围"
        )

    definition = load_mixed_template_definition()
    template = get_mixed_template(template_id)
    rule = _category_rule(zone)
    structural = definition.get("structural") or {}
    carrier_by_module = structural.get("carrier_by_module") or {}
    state_by_module = structural.get("product_state_by_module") or {}
    labels = structural.get("module_labels") or {}
    recipe_id = _text(environment_recipe_id) or _text(
        definition.get("default_environment_recipe")
    )
    recipe = get_environment_recipe(recipe_id)
    theme = _normalize_theme(content_theme)
    role_map = {
        _text(module): _text(role)
        for module, role in (structure_role_by_module or {}).items()
        if _text(module)
    }

    subtype = physical_subtype_rule(canonical)
    structure_facts = subtype_structure_facts(canonical)
    subtype_actions = subtype.get("action_by_module")
    subtype_actions = subtype_actions if isinstance(subtype_actions, Mapping) else {}
    subtype_jobs = subtype.get("distinct_jobs")
    subtype_jobs = subtype_jobs if isinstance(subtype_jobs, Mapping) else {}

    capture_units: List[Dict[str, Any]] = []
    for position, item in enumerate(template.get("sequence") or [], start=1):
        module = _text(item.get("module"))
        unit_id = f"CU_{position:02d}"
        observation_job = _text(item.get("observation_job"))
        distinct = (
            _text(subtype_jobs.get(module))
            or _text((rule.get("distinct_jobs") or {}).get(module))
        )
        framing = project_module_framing(canonical, module)
        capture_units.append({
            "unit_id": unit_id,
            "unit_index": position,
            "duration_seconds": int(item.get("duration_seconds") or 0),
            "module": module,
            "module_label": _text(labels.get(module)),
            "view_scope": _text(framing.get("view_scope")),
            "view_label": _text(framing.get("view_label")) or _text(labels.get(module)),
            "carrier_mode": _text(carrier_by_module.get(module)),
            "structure_role": role_map.get(module, ""),
            "framing": _text(framing.get("zone_label")),
            "allowed_framing": list(framing.get("allowed_framing") or []),
            "forbidden_framing": list(framing.get("forbidden_framing") or []),
            "face_policy": _text(structural.get("face_policy")),
            "observation_job": distinct or observation_job,
            "product_state": _text(state_by_module.get(module)),
            "body_zone": _text(framing.get("body_zone")),
            "action": _text(subtype_actions.get(module)) or _action_for_module(rule, module),
            "action_boundary": list(rule.get("forbidden_actions") or []),
            "state_boundary": _text(framing.get("state_boundary")),
            "quantity_rule": _text(rule.get("quantity_rule")),
            "visible_quantity_rule": _text(framing.get("visible_quantity_rule")),
            "evidence_refs": list(theme.get("evidence_refs") or []),
            "edit_before": (
                _text(structural.get("edit_before_first_unit"))
                if position == 1
                else _text(structural.get("edit_before_default"))
            ),
            "continuity_group": unit_id,
        })

    contract = {
        "schema_version": ACCESSORY_MIXED_TEMPLATE_SCHEMA,
        "source_mode": ACCESSORY_MIXED_TEMPLATE_SOURCE_MODE,
        "execution_profile": ACCESSORY_MIXED_TEMPLATE_PROFILE,
        "template_id": _text(template.get("template_id")),
        "template_version": int(template.get("template_version") or 1),
        "template_display_name": _text(template.get("display_name")),
        "content_theme": theme,
        "global_carrier": _text(structural.get("global_carrier")),
        "face_policy": _text(structural.get("face_policy")),
        "audio_route": _text(structural.get("audio_route")),
        "environment_recipe_id": _text(recipe_id),
        "environment_recipe_version": int(recipe.get("recipe_version") or 1),
        "environment_recipe": recipe,
        "total_duration_seconds": sum(
            int(unit.get("duration_seconds") or 0) for unit in capture_units
        ),
        "product_identity_ref": _text(product_identity_ref),
        "local_body_style_ref": _text(local_body_style_ref),
        "category_zone": zone,
        "canonical_product_type": canonical,
        "physical_family": _text(subtype.get("family")),
        "observation_focus": [
            _text(item) for item in (subtype.get("observation_focus") or []) if _text(item)
        ],
        "fallback_observation": _text(subtype.get("fallback_observation")),
        "structure_facts": structure_facts,
        "part_gated_actions": [
            {
                "module": _text(candidate.get("module")).upper(),
                "action": _text(candidate.get("action")),
                "requires": {
                    _text(name): _text(state).upper()
                    for name, state in (candidate.get("requires") or {}).items()
                    if _text(name)
                },
            }
            for candidate in (subtype.get("optional_actions") or [])
            if isinstance(candidate, Mapping)
            and _text(candidate.get("action"))
            and _text(candidate.get("module"))
        ],
        "cut_policy": dict(structural.get("cut_policy") or {}),
        "capture_units": capture_units,
        "difference_report": {
            "nearest_script_id": "",
            "difference_dimensions": [],
            "difference_summary": "",
            "review_status": "PLANNED_ONLY",
        },
    }
    # The final-shot signature is a pure function of the contract, so it is
    # frozen *with* it: a later batch and the post-generation re-check must
    # compare against the same numbers this contract was accepted under.
    contract[MIXED_SIGNATURE_KEY] = mixed_signature_bundle(contract)
    return contract


def validate_mixed_template_contract(contract: Mapping[str, Any] | None) -> List[str]:
    """Return hard blocking errors.  Empty list means the contract is usable."""

    errors: List[str] = []
    data = dict(contract or {})
    if not data:
        return ["MIXED_CONTRACT_MISSING"]
    if _text(data.get("schema_version")) != ACCESSORY_MIXED_TEMPLATE_SCHEMA:
        errors.append("MIXED_CONTRACT_SCHEMA_MISMATCH")
    if _text(data.get("execution_profile")) != ACCESSORY_MIXED_TEMPLATE_PROFILE:
        errors.append("MIXED_CONTRACT_PROFILE_MISMATCH")
    if _text(data.get("global_carrier")).upper() != "MIXED":
        errors.append("MIXED_CONTRACT_GLOBAL_CARRIER_NOT_MIXED")
    if _text(data.get("face_policy")).upper() != "NO_FACE":
        errors.append("MIXED_CONTRACT_FACE_POLICY_NOT_NO_FACE")

    structural = load_mixed_template_definition().get("structural") or {}
    required_modules = [str(item) for item in structural.get("required_modules") or []]
    carrier_by_module = structural.get("carrier_by_module") or {}

    units = [item for item in (data.get("capture_units") or []) if isinstance(item, Mapping)]
    if not units:
        errors.append("MIXED_CONTRACT_UNITS_MISSING")
        return errors

    seen_unit_ids: List[str] = []
    total_seconds = 0
    for unit in units:
        for field in _UNIT_REQUIRED_FIELDS:
            if field == "body_zone" and not _unit_uses_body_zone(unit):
                continue
            if field not in unit or unit.get(field) in (None, ""):
                errors.append(
                    f"MIXED_CONTRACT_UNIT_FIELD_MISSING:{unit.get('unit_id') or '?'}:{field}"
                )
        unit_id = _text(unit.get("unit_id"))
        if unit_id in seen_unit_ids:
            errors.append(f"MIXED_CONTRACT_UNIT_ID_DUPLICATE:{unit_id}")
        seen_unit_ids.append(unit_id)

        module = _text(unit.get("module"))
        expected_carrier = _text(carrier_by_module.get(module))
        if not expected_carrier:
            errors.append(f"MIXED_CONTRACT_UNKNOWN_MODULE:{module or '?'}")
        elif _text(unit.get("carrier_mode")) != expected_carrier:
            errors.append(
                f"MIXED_CONTRACT_UNIT_CARRIER_CONFLICT:{unit_id}:"
                f"{unit.get('carrier_mode')}!={expected_carrier}"
            )
        try:
            total_seconds += int(unit.get("duration_seconds") or 0)
        except (TypeError, ValueError):
            errors.append(f"MIXED_CONTRACT_UNIT_DURATION_INVALID:{unit_id}")

    present_modules = {_text(unit.get("module")) for unit in units}
    for module in required_modules:
        if module not in present_modules:
            errors.append(f"MIXED_CONTRACT_MODULE_MISSING:{module}")

    # The three display methods are the product requirement: every video must
    # contain handheld + static + worn.  A missing method is a hard error.
    display_groups = {
        "WORN": {"WORN_DETAIL", "WORN_RELATION"},
        "HANDHELD": {"HANDHELD_PRODUCT"},
        "STATIC": {"STATIC_PRODUCT"},
    }
    for label, modules in display_groups.items():
        if not (present_modules & modules):
            errors.append(f"MIXED_CONTRACT_DISPLAY_METHOD_MISSING:{label}")

    expected_total = int(load_mixed_template_definition().get("duration_seconds") or 15)
    if total_seconds != expected_total:
        errors.append(
            f"MIXED_CONTRACT_TIMELINE_MISMATCH:{total_seconds}!={expected_total}"
        )

    theme = data.get("content_theme") or {}
    for field in _THEME_REQUIRED_FIELDS:
        if not _text(theme.get(field)):
            errors.append(f"MIXED_CONTRACT_THEME_FIELD_MISSING:{field}")
    # A declared input gap is reported *by name* so the batch report can say
    # "there was no readable buying reason to theme this video on" instead of
    # the generic missing-field code.  It is still a hard error for the mixed
    # mode: a contract whose theme is an opaque operator ID can be planned but
    # never judged, and shipping it would re-create Review R4.
    gap = _text(theme.get("thesis_input_gap"))
    if gap:
        errors.append(f"{ERR_MIXED_THEME_INPUT_GAP}:{gap}")

    # A subtype may not be asked to display a part its own form excludes.  This
    # is checked here, at compile time, so the promise holds regardless of what
    # the anchors say later -- "手链与手镯不得互相冒充" as a negative rule could
    # not cancel a positive instruction to show a solid bangle's chain links.
    canonical = _text(data.get("canonical_product_type"))
    blocked = _absent_part_terms(canonical)
    if blocked:
        for unit in units:
            action = _text(unit.get("action"))
            if not action:
                continue
            hits = [term for term in blocked if term in action]
            for term in hits:
                errors.append(
                    f"{ERR_MIXED_PART_CONTRADICTION}:"
                    f"{_text(unit.get('unit_id')) or '?'}:{term}"
                )

    return errors


# ── Execution scope ────────────────────────────────────────────────────────
# The mixed template is a *15-second short-original* mode.  Whether it applies
# cannot be decided from the category plus an environment flag: the long-form
# direct-product source builder borrows this very planning entry point with
# ``duration_seconds=15.0`` while owning a completely different parent task, and
# a long-form or remake run must never receive accessory mixed shots.
#
# ``task_branch`` is the authoritative answer to "which task is this really?",
# resolved once at the planning entry point from the real request and parent
# task context.  The other scope fields are guard rails, not the source of
# truth.

MIXED_ELIGIBLE_DURATION_SECONDS = 15.0
SCOPE_BRANCH_SHORT_VIDEO_ORIGINAL = "SHORT_VIDEO_ORIGINAL"
SCOPE_BRANCH_LONGFORM = "LONGFORM"
SCOPE_BRANCH_REMAKE = "REMAKE"
SCOPE_BRANCH_UNKNOWN = "UNKNOWN"
SCOPE_ELIGIBLE_SCRIPT_MODES: Tuple[str, ...] = ("simplified_v1",)


def mixed_scope_decision(scope: Mapping[str, Any] | None) -> Dict[str, Any]:
    """Decide whether ``scope`` may use the accessory mixed template.

    ``scope`` is ``None`` when a caller declared none -- direct library callers
    and existing tests -- which keeps the historical category+flag behaviour.
    The real planning entry point always declares a scope, so every production
    request is judged on its true parent task instead.
    """

    if scope is None:
        return {"eligible": True, "reason": "MIXED_SCOPE_UNDECLARED", "scope": {}}

    data = {str(key): value for key, value in dict(scope).items()}
    branch = _text(data.get("task_branch")).upper() or SCOPE_BRANCH_UNKNOWN
    try:
        duration = float(data.get("target_duration_seconds") or 0)
    except (TypeError, ValueError):
        duration = 0.0
    is_new_plan = bool(data.get("is_new_plan", True))
    script_mode = _text(data.get("script_mode"))

    def _refuse(reason: str) -> Dict[str, Any]:
        return {"eligible": False, "reason": reason, "scope": data}

    if branch != SCOPE_BRANCH_SHORT_VIDEO_ORIGINAL:
        return _refuse(f"{ERR_MIXED_SCOPE_UNSUPPORTED}:branch={branch}")
    if not is_new_plan:
        return _refuse(f"{ERR_MIXED_SCOPE_UNSUPPORTED}:not_new_plan")
    if abs(duration - MIXED_ELIGIBLE_DURATION_SECONDS) > 1e-6:
        return _refuse(f"{ERR_MIXED_SCOPE_UNSUPPORTED}:duration={duration:g}")
    if script_mode and script_mode not in SCOPE_ELIGIBLE_SCRIPT_MODES:
        return _refuse(f"{ERR_MIXED_SCOPE_UNSUPPORTED}:script_mode={script_mode}")
    return {"eligible": True, "reason": "MIXED_SCOPE_ELIGIBLE", "scope": data}


# ── Structure compatibility ────────────────────────────────────────────────
# The authored template owns the *physical* shot choreography and therefore the
# clip count.  A routed narrative structure only owns the viewing order, so it
# may map onto the template when it does not need more beats than the template
# has shots.  A structure with *more* beats has no defined mapping: the extra
# beat cannot be dropped (that deletes narrative authority) and it cannot be
# merged (that invents a beat pair the router never proposed).  Such a candidate
# is excluded while structures are still being selected, with a reason, rather
# than being truncated later in the compile step.


def mixed_template_shot_limit() -> int:
    """Smallest shot count across the authored templates.

    Structure compatibility is decided while structures are still being chosen,
    before the item index -- and therefore the rotating template -- is known.
    The conservative bound is therefore the minimum, so a structure accepted
    here fits whichever template the rotation later hands the item.
    """

    counts = [
        len([item for item in (entry.get("sequence") or []) if isinstance(item, Mapping)])
        for entry in list_mixed_templates()
    ]
    counts = [count for count in counts if count > 0]
    return min(counts) if counts else 0


def mixed_template_unit_count(contract: Mapping[str, Any] | None) -> int:
    """Number of frozen shots the contract requires.  0 when unusable."""

    units = (contract or {}).get("capture_units") or []
    return len([item for item in units if isinstance(item, Mapping)])


def map_structure_beats_to_units(
    beats: Iterable[Any],
    unit_count: int,
) -> Dict[str, Any]:
    """Decide whether one routed structure maps onto ``unit_count`` shots.

    Returns ``{"compatible": bool, "reason": str, "beats": [...], ...}``.  A
    shorter structure is compatible: the existing compile step already expands
    it by repeating an existing proof/use function, which adds no new beat.
    A longer structure is not, and says so by code.
    """

    sequence = [_text(value) for value in (beats or []) if _text(value)]
    result: Dict[str, Any] = {
        "mapping_version": "mixed-structure-mapping-v1",
        "unit_count": int(unit_count or 0),
        "beat_count": len(sequence),
        "beats": sequence,
        "compatible": False,
        "reason": "",
    }
    if not sequence:
        result["reason"] = ERR_MIXED_STRUCTURE_BEATS_MISSING
        return result
    if int(unit_count or 0) <= 0:
        result["reason"] = f"{ERR_MIXED_STRUCTURE_INCOMPATIBLE}:template_units=0"
        return result
    if len(sequence) > int(unit_count):
        result["reason"] = (
            f"{ERR_MIXED_STRUCTURE_INCOMPATIBLE}:"
            f"beats={len(sequence)}>units={int(unit_count)}"
        )
        return result
    result["compatible"] = True
    result["reason"] = "MIXED_STRUCTURE_MAPPED"
    return result


def unit_carrier_map(contract: Mapping[str, Any] | None) -> Dict[str, str]:
    """Project ``unit_id -> carrier_mode`` for downstream shot-level consumers."""

    units = (contract or {}).get("capture_units") or []
    return {
        _text(unit.get("unit_id")): _text(unit.get("carrier_mode"))
        for unit in units
        if isinstance(unit, Mapping) and _text(unit.get("unit_id"))
    }


def summarize_mixed_contract(contract: Mapping[str, Any] | None) -> Dict[str, Any]:
    """Compact projection used by batch reports and review packets."""

    data = dict(contract or {})
    units = [
        item for item in (data.get("capture_units") or []) if isinstance(item, Mapping)
    ]
    theme = data.get("content_theme") or {}
    return {
        "template_id": _text(data.get("template_id")),
        "template_version": data.get("template_version"),
        "execution_profile": _text(data.get("execution_profile")),
        "source_mode": _text(data.get("source_mode")),
        "theme_id": _text(theme.get("theme_id")),
        "candidate_role": _text(theme.get("candidate_role")),
        "environment_recipe_id": _text(data.get("environment_recipe_id")),
        "total_duration_seconds": data.get("total_duration_seconds"),
        "modules": [_text(unit.get("module")) for unit in units],
        "carriers": [_text(unit.get("carrier_mode")) for unit in units],
        "units": [
            {
                "unit_id": _text(unit.get("unit_id")),
                "module": _text(unit.get("module")),
                "carrier_mode": _text(unit.get("carrier_mode")),
                "duration_seconds": unit.get("duration_seconds"),
                "body_zone": _text(unit.get("body_zone")),
                "product_state": _text(unit.get("product_state")),
            }
            for unit in units
        ],
    }


def iter_supported_canonical_types() -> Iterable[str]:
    return sorted(mixed_supported_canonical_types())


_MODULE_LABELS = {
    "WORN_DETAIL": "佩戴近景",
    "HANDHELD_PRODUCT": "手持商品",
    "STATIC_PRODUCT": "商品静物",
    "WORN_RELATION": "佩戴关系",
}

_CARRIER_LABELS = {
    "WEARER_ACTIVE": "真人局部佩戴",
    "HAND_ONLY": "纯手部承载",
    "STATIC_PRODUCT": "静物承载",
}


def contains_pairing_claim(text: Any) -> bool:
    """True when ``text`` spells out a pairing relation.

    Used to decide whether an authored rule may be handed to the blueprint model
    as prose.  A rule that names the relation is only safe when the anchor card
    authorised that exact relation; otherwise paraphrasing it into the script
    trips the accessory identity gate.
    """

    blob = _text(text)
    return any(term in blob for term in _PAIRING_OUTPUT_TERMS_ALL)


def sanitize_pairing_claims(text: Any) -> str:
    """Remove pairing wording from descriptive prose.

    The relation may only ever be stated by the frozen authority, so any pairing
    word inside a descriptive field (a per-shot action, a framing note) is
    dropped before the line reaches the model.  Contracts planned before the
    template text was made neutral still carry such wording, and the identity
    gate would otherwise reject every item built from them.

    Only ever removes; it never substitutes a relation, so it cannot introduce
    a claim the anchor card did not authorise.
    """

    out = _text(text)
    for term in _PAIRING_OUTPUT_TERMS_ALL:
        if term in out:
            out = out.replace(term, "")
    return out


# ---------------------------------------------------------------------------
# NO_FACE prose scrubbing
# ---------------------------------------------------------------------------
# The ``NO_FACE`` contract forbids eyes / nose / mouth in frame, but the EAR
# zone's authored action text used to describe a head-and-shoulders shift
# ("小幅头肩变化").  ``render_mixed_blueprint_guidance`` copies that action
# verbatim into the blueprint prompt, so the same prompt both *instructed* a
# head-and-shoulders shot and *banned* the wording -- the model was told to do
# the thing it was told not to do.
#
# The authored text is fixed at source, but a contract planned before that fix
# is frozen and keeps the old wording.  Exactly like ``sanitize_pairing_claims``
# above, the render path therefore scrubs it on the way out.
#
# The rewrite table itself stays single-sourced in
# ``category_execution.accessory`` (it is entangled with the accessory profile's
# framing vocabulary), so this module only borrows it -- importing at call time
# because ``accessory`` imports *this* module at import time.
NO_FACE_POLICY = "NO_FACE"

# Terms that name the face.  "头肩" is included even though it never literally
# says "face": the EAR zone's ``forbidden_framing`` bans eyes / nose / mouth in
# frame, so head-and-shoulders is a face shot by another name.
FACE_WORDING_TERMS: Tuple[str, ...] = ("半脸", "侧脸", "正脸", "全脸", "头肩", "自拍")


def contains_face_wording(text: Any) -> bool:
    """True when ``text`` names the face, so it may not be shown to the model."""

    blob = _text(text)
    return any(term in blob for term in FACE_WORDING_TERMS)


def sanitize_face_wording(text: Any) -> str:
    """Rewrite face-naming prose into the contract's authorised vocabulary.

    Reuses the accessory adapter's single rewrite table so the render path and
    the adapter can never drift apart.  Only ever runs for a ``NO_FACE``
    contract; every other face policy keeps its authored wording verbatim.
    """

    out = _text(text)
    if not out:
        return out
    from core.category_execution.accessory import _no_face_safe_text

    return _no_face_safe_text(out)


def scrub_no_face_prose_in_place(node: Any) -> None:
    """Rewrite face-naming prose inside an assembled script, in place.

    ``_no_face_deep_clean`` guards the projection the adapter *owns*.  A compiled
    script also carries model-authored prose -- ``production_design.scene
    .subject_position``, ``storyboard[].camera`` -- which no adapter ever sees
    and which the renderer copies verbatim into the video prompt.  This is that
    second boundary: the one that catches a model paraphrasing
    ``CREATOR_SELF_SHOT`` into "人物在同一自拍范围内轻微调整位置".

    Two deliberate differences from ``_no_face_deep_clean``:

    * **Nothing is dropped.**  The compiled script has a fixed schema and the
      renderer walks it positionally, so removing a list entry that still names
      the face would corrupt the script instead of cleaning it.  Only string
      leaves are rewritten, through the adapter's single replacement table.
    * **It mutates instead of returning.**  ``video_generation_brief`` is built
      later from these *same objects* (references, not copies), so swapping in a
      cleaned copy would leave the brief holding the pre-scrub text.
    """

    def _walk(current: Any) -> None:
        if isinstance(current, dict):
            for key, value in current.items():
                if isinstance(value, str):
                    cleaned = sanitize_face_wording(value)
                    if cleaned != value:
                        current[key] = cleaned
                else:
                    _walk(value)
        elif isinstance(current, list):
            for index, value in enumerate(current):
                if isinstance(value, str):
                    cleaned = sanitize_face_wording(value)
                    if cleaned != value:
                        current[index] = cleaned
                else:
                    _walk(value)

    _walk(node)


def _quantity_identity_lines(
    units: List[Mapping[str, Any]],
    identity_authority: Mapping[str, Any] | None,
) -> List[str]:
    """Quantity guard that cannot leak an unauthorised pairing claim.

    The relation itself is stated from the frozen authority (which is the only
    thing allowed to state it).  The zone rule is appended only when it names no
    pairing term at all -- that keeps rules about physical form (bangle vs
    bracelet, ring face vs band) while dropping prose that would be copied into
    authored content as a forbidden claim.
    """

    mode = _text((identity_authority or {}).get("pairing_mode")).upper()
    if mode == "PAIR":
        head = "数量与身份：授权锚点已明确成对关系，全片保持成对出现，不得改写成单件商品"
    elif mode == "SINGLE":
        head = "数量与身份：授权锚点已明确单件关系，全片保持单件商品出现，不得改写成多件"
    else:
        head = (
            "数量与身份：分镜与文案都不得声明商品的件数与左右佩戴方式"
            "（授权锚点没有提供这一信息），只描述每一镜当前实际可见的状态"
        )

    lines: List[str] = [head]
    for unit in units:
        rule = _text(unit.get("quantity_rule"))
        if not rule or contains_pairing_claim(rule):
            continue
        lines.append("数量与身份（类目规则）：" + rule)
        break
    # The authorised quantity and the quantity visible inside one crop are two
    # different things.  Without this line a paired product reads as a demand
    # that the pair be visible in a single-part crop.
    for unit in units:
        rule = _text(unit.get("visible_quantity_rule"))
        if not rule or contains_pairing_claim(rule):
            continue
        lines.append("本镜可见数量（不改变授权数量）：" + rule)
        break
    return lines


def render_mixed_blueprint_guidance(
    contract: Mapping[str, Any] | None,
    identity_authority: Mapping[str, Any] | None = None,
    evidence: Mapping[str, Mapping[str, Any]] | None = None,
) -> List[str]:
    """Deterministic per-shot guidance lines for the visual blueprint model.

    Only called when a compiled ``mixed_template_contract`` exists.  The lines
    replace the legacy accessory framing advice, which assumes one single
    carrier for the whole clip and is therefore wrong for a mixed montage.

    ``evidence`` is the frozen part-evidence map (see
    :func:`attach_mixed_part_evidence`).  It only ever *withholds* a
    part-specific action candidate; the base subtype action always stands.
    """

    data = dict(contract or {})
    if _text(data.get("execution_profile")) != ACCESSORY_MIXED_TEMPLATE_PROFILE:
        return []
    units = [u for u in (data.get("capture_units") or []) if isinstance(u, Mapping)]
    if not units:
        return []

    lines: List[str] = []
    template_name = _text(data.get("template_display_name")) or _text(data.get("template_id"))
    total = data.get("total_duration_seconds")
    lines.append(
        f"本片使用饰品混合展示模板「{template_name}」（{total}秒，{len(units)}个镜头）："
        "镜头顺序、每镜承载方式与时长已冻结，不得改动、不得增删镜头、不得把某一镜换成另一种承载"
    )

    face_policy = _text(data.get("face_policy"))
    face_free = face_policy == NO_FACE_POLICY

    def _unit_prose(value: Any) -> str:
        """Per-shot prose as the model may see it.

        Pairing wording is always sanitised.  Face wording is additionally
        rewritten only under ``NO_FACE``: without this, a contract frozen before
        the authored action text was fixed would still hand the model a
        head-and-shoulders instruction in the same breath as the ban on it.
        """

        text = sanitize_pairing_claims(value)
        return sanitize_face_wording(text) if face_free else text

    # Part-gated candidates: only the ones the evidence actually authorises are
    # issued.  Everything else falls back to the subtype's base action, which
    # never names the unconfirmed part.
    canonical = _text(data.get("canonical_product_type"))
    gated = resolve_part_gated_actions(canonical, evidence=evidence)
    gated_by_module: Dict[str, str] = {}
    for candidate in gated:
        module = _text(candidate.get("module"))
        if module and module not in gated_by_module:
            gated_by_module[module] = _text(candidate.get("action"))
    declared_gated = [
        item
        for item in (data.get(PART_GATED_ACTIONS_KEY) or [])
        if isinstance(item, Mapping)
    ]
    withheld = len(declared_gated) - len(gated)

    elapsed = 0
    for unit in units:
        duration = unit.get("duration_seconds") or 0
        start, end = elapsed, elapsed + int(duration or 0)
        elapsed = end
        module = _text(unit.get("module"))
        carrier = _text(unit.get("carrier_mode"))
        action = gated_by_module.get(module) or unit.get("action")
        # Use the same label the framing legend uses, so 镜头 N can be mapped
        # onto its module row without translating vocabulary.
        label = _text(unit.get("view_label")) or _MODULE_LABELS.get(module, module)
        lines.append(
            f"- 镜头{unit.get('unit_index')}（{start}-{end}秒）"
            f"{label}｜承载：{_CARRIER_LABELS.get(carrier, carrier)}"
            f"｜本镜只做这一件事：{_unit_prose(unit.get('observation_job'))}"
            f"｜动作：{_unit_prose(action)}"
        )

    lines.append(
        "全片承载为 MIXED：同一支视频内必须同时出现佩戴、手持、静物三类画面，"
        "允许用直接切换在 佩戴→手持→静物→佩戴 之间转换；"
        "同一连续镜头内不得发生物理状态瞬间转移，也不得拍出摘戴过程"
    )

    # Framing is per module: the worn shots use the category's body zone, the
    # handheld shot is hand-and-product, the static shot is product-and-surface.
    # This replaces the old single "允许的取景范围" line, which merged all four
    # into one body-zone list.
    lines.extend(module_framing_legend_lines(data))
    for unit in units:
        boundary = _text(unit.get("state_boundary"))
        if not boundary:
            continue
        lines.append(
            f"镜头{unit.get('unit_index')}状态边界（{_text(unit.get('view_label'))}）：{boundary}"
        )

    if face_free:
        worn_bans: List[str] = []
        for unit in units:
            if _text(unit.get("module")) not in {"WORN_DETAIL", "WORN_RELATION"}:
                continue
            for item in unit.get("forbidden_framing") or []:
                text = sanitize_pairing_claims(item)
                if text and text not in worn_bans:
                    worn_bans.append(text)
        lines.append(
            "全片不露脸：每一镜都不得出现"
            + "、".join(worn_bans or ["眼睛", "鼻子", "嘴部", "正面全脸", "镜面反射露脸"])
            + "；镜面中的反射同样算露脸"
        )
        # Name the wording ban explicitly.  "半脸耳侧近景" is the accessory
        # profile's historical way of describing this framing and it leaks back
        # into the 分镜文案 whenever the model paraphrases the legacy vocabulary, so
        # the ban has to be stated in the same words the model would reuse.
        # "头肩" is listed too: it never literally says "face", but the body-zone
        # rule bans eyes / nose / mouth in frame, so a head-and-shoulders shot is
        # a face shot by another name.
        #
        # The allowed vocabulary is read off the frozen contract instead of
        # being written out as ear wording: this line used to tell a bracelet to
        # describe itself in terms of "耳侧、耳廓、耳垂、颈侧".
        worn_allowed = "、".join(worn_body_framing(data)) or "本镜自己的局部身体关系"
        lines.append(
            "不露脸写法：分镜的 visual_content、camera、production_design 与人物动作里"
            "都不得出现“半脸”“侧脸”“正脸”“全脸”“镜面头肩”“头肩”“自拍”"
            "这类把脸写进画面的措辞；"
            f"佩戴类镜头只用“{worn_allowed}”描述"
        )
        # ``CREATOR_SELF_SHOT`` is a capture-organisation token, but it reads like
        # "selfie" and a model will happily paraphrase it into "人物在同一自拍范围内
        # 轻微调整位置" -- a face framing smuggled in through the capture mode.
        # Naming the token and its real meaning is what stops that paraphrase;
        # the bare wording ban above only bans the *word*.
        lines.append(
            "拍摄关系是 CREATOR_SELF_SHOT：同一个人用同一部手机分多段录制后直接剪切成片。"
            "它只说明“谁在拍、素材怎么组织”，不是取景方式——"
            "不得据此在画面描述里写成“自拍”，也不得出现对着镜头、看镜头这类暗示画面里有脸的构图"
        )

    focus = [
        _text(item) for item in (data.get("observation_focus") or []) if _text(item)
    ]
    if focus:
        lines.append("本片可观察重点（只到这里为止）：" + "、".join(focus))
    fallback = _text(data.get("fallback_observation"))
    # ``withheld`` starts as the gated candidates the evidence refused to issue.
    # It must also cover the other half of the same problem: a part the registry
    # declares ``UNKNOWN`` is exactly the kind of structure a model invents when
    # nothing tells it to stop.  ``ABSENT`` needs no instruction -- the part does
    # not exist and no action names it -- but ``UNKNOWN`` is "we could not confirm
    # it", which is the state that produced the fabricated back-of-clip detail in
    # the first place.  A subtype that declares no structure facts at all is
    # wholly unconfirmed and gets the same instruction.
    facts = data.get(STRUCTURE_FACTS_KEY)
    facts = facts if isinstance(facts, Mapping) else {}
    unconfirmed = not facts or any(
        _text(state).upper() == EVIDENCE_UNKNOWN for state in facts.values()
    )
    if fallback and (withheld > 0 or unconfirmed):
        lines.append("未确认部件不要展示：" + fallback)

    forbidden_actions: List[str] = []
    for unit in units:
        for item in unit.get("action_boundary") or []:
            text = sanitize_pairing_claims(item)
            if text and text not in forbidden_actions:
                forbidden_actions.append(text)
    if forbidden_actions:
        lines.append("全片禁止动作：" + "、".join(forbidden_actions))

    worn_jobs = [
        (u.get("unit_index"), sanitize_pairing_claims(u.get("observation_job")))
        for u in units
        if _text(u.get("module")) in {"WORN_DETAIL", "WORN_RELATION"}
    ]
    if len(worn_jobs) >= 2:
        first, last = worn_jobs[0], worn_jobs[-1]
        lines.append(
            f"首段与末段的佩戴必须是两个不同的观察任务：镜头{first[0]}={first[1]}；"
            f"镜头{last[0]}={last[1]}；不得用同一素材重复裁切伪装成两个镜头"
        )

    lines.extend(_quantity_identity_lines(units, identity_authority))

    recipe = data.get("environment_recipe") if isinstance(data.get("environment_recipe"), dict) else {}
    if recipe:
        lines.append(
            "全片唯一环境配方（"
            + (_text(recipe.get("label")) or _text(data.get("environment_recipe_id")))
            + "）："
            + _text(recipe.get("environment"))
            + "；目标是"
            + _text(recipe.get("goal"))
            + "。光向、白平衡、肤色与主要背景材质全片一致，只允许正常角度变化带来的高光变化；"
            "不要磨皮成塑料感，不要加星芒或虚构钻石火彩，不要靠抖动、曝光跳变或低画质伪造真实感"
        )

    if _text(data.get("audio_route")) == "VOICEOVER_POST":
        lines.append(
            "声音为画外旁白：不需要对镜讲话、不需要口型，人物也不需要看镜头"
        )

    cut_policy = data.get("cut_policy") if isinstance(data.get("cut_policy"), dict) else {}
    if _text(cut_policy.get("note")):
        lines.append("剪辑约束：" + _text(cut_policy.get("note")))

    return lines


# Shot-level carrier projection -------------------------------------------------

PROJECTION_STATUS_KEY = "mixed_template_shot_projection_status"
PROJECTION_ERRORS_KEY = "mixed_template_shot_projection_errors"


def _unit_id_of(unit: Mapping[str, Any]) -> str:
    """The single per-shot id authority.

    ``capture_unit_id`` is what the compile step and the storyboard both use.
    ``unit_id`` is the authored contract's own spelling and is accepted only as
    a read-boundary alias, so an older frozen contract still resolves without
    anybody guessing a shot by its position.
    """

    return _text(unit.get("capture_unit_id")) or _text(unit.get("unit_id"))


def frozen_unit_timeline(
    contract: Mapping[str, Any] | None,
) -> Tuple[Dict[str, Dict[str, int]], int]:
    """Derive ``unit_id -> {start,end,duration}`` from the frozen durations.

    This is the *only* timeline authority: start times are accumulated from the
    frozen per-shot durations rather than kept in a second place, so the sum
    can never drift away from the clip list.
    """

    units = [
        item
        for item in ((contract or {}).get("capture_units") or [])
        if isinstance(item, Mapping)
    ]
    timeline: Dict[str, Dict[str, int]] = {}
    elapsed = 0
    for unit in units:
        unit_id = _text(unit.get("unit_id"))
        try:
            duration = int(unit.get("duration_seconds") or 0)
        except (TypeError, ValueError):
            duration = 0
        timeline[unit_id] = {
            "start_seconds": elapsed,
            "end_seconds": elapsed + duration,
            "duration_seconds": duration,
        }
        elapsed += duration
    return timeline, elapsed


def format_mixed_shot_time_range(span: Mapping[str, Any] | None) -> str:
    """The one string form of a frozen span, for storyboards and prompts.

    Matches the production 15-second convention (``structure_execution_compiler``
    emits ``"0-3s"``), so a projected storyboard reads like every other one.
    """

    data = span if isinstance(span, Mapping) else {}
    try:
        start = float(data.get("start_seconds") or 0)
        end = float(data.get("end_seconds") or 0)
    except (TypeError, ValueError):
        return ""

    def _fmt(value: float) -> str:
        return str(int(value)) if float(value).is_integer() else f"{value:g}"

    return f"{_fmt(start)}-{_fmt(end)}s"


def validate_mixed_shot_projection(
    units: Any,
    contract: Mapping[str, Any] | None,
) -> List[str]:
    """Hard errors for one compiled clip list against the frozen contract.

    Checks identity (every shot resolves by id, exactly once, with no missing
    and no extra shot) and the per-shot execution fields the template owns
    (carrier, product state, positive duration).  Position is never used as a
    fallback: a shot whose id does not resolve is an error, not the n-th shot.
    """

    errors: List[str] = []
    data = dict(contract or {})
    contract_units = [
        item
        for item in (data.get("capture_units") or [])
        if isinstance(item, Mapping)
    ]
    if not contract_units:
        return [f"{ERR_MIXED_UNIT_COUNT_MISMATCH}:contract_units=0"]

    by_id = {
        _text(item.get("unit_id")): item
        for item in contract_units
        if _text(item.get("unit_id"))
    }
    unit_list = [item for item in (units or []) if isinstance(item, dict)]

    if len(unit_list) != len(contract_units):
        errors.append(
            f"{ERR_MIXED_UNIT_COUNT_MISMATCH}:"
            f"compiled={len(unit_list)} contract={len(contract_units)}"
        )

    seen: List[str] = []
    for index, unit in enumerate(unit_list):
        unit_id = _unit_id_of(unit)
        if not unit_id:
            errors.append(f"{ERR_MIXED_UNIT_ID_MISMATCH}:missing_id:index={index}")
            continue
        if unit_id in seen:
            errors.append(f"{ERR_MIXED_UNIT_ID_MISMATCH}:duplicate:{unit_id}")
            continue
        seen.append(unit_id)
        source = by_id.get(unit_id)
        if source is None:
            errors.append(f"{ERR_MIXED_UNIT_ID_MISMATCH}:unknown:{unit_id}")
            continue

        for field, code in (
            ("carrier_mode", ERR_MIXED_CARRIER_MISMATCH),
            ("product_state", ERR_MIXED_STATE_MISMATCH),
        ):
            expected = _text(source.get(field))
            actual = _text(unit.get(field))
            # Only compare once the value exists: before projection the compile
            # step has not stamped it yet, and that is not a conflict.
            if actual and expected and actual != expected:
                errors.append(f"{code}:{unit_id}:{field}:{actual}!={expected}")

        # Before projection the compile step has not stamped a duration yet, so
        # fall back to the frozen value; after projection the stamped value is
        # the one that has to hold up.
        raw_duration = unit.get("duration_seconds")
        if raw_duration is None:
            raw_duration = source.get("duration_seconds")
        try:
            duration = int(raw_duration or 0)
        except (TypeError, ValueError):
            duration = 0
        if duration <= 0:
            errors.append(f"{ERR_MIXED_UNIT_DURATION_INVALID}:{unit_id}")

    for unit_id in by_id:
        if unit_id not in seen:
            errors.append(f"{ERR_MIXED_UNIT_ID_MISMATCH}:absent:{unit_id}")

    timeline, total = frozen_unit_timeline(data)
    expected_total = int(data.get("total_duration_seconds") or 0)
    if expected_total and total != expected_total:
        errors.append(
            f"{ERR_MIXED_TIMELINE_MISMATCH}:sum={total}!={expected_total}"
        )
    for unit_id, span in timeline.items():
        if span["duration_seconds"] <= 0:
            errors.append(f"{ERR_MIXED_UNIT_DURATION_INVALID}:contract:{unit_id}")
        if span["end_seconds"] <= span["start_seconds"]:
            errors.append(
                f"{ERR_MIXED_TIMELINE_MISMATCH}:non_positive_span:{unit_id}"
            )
    ordered = [timeline[key] for key in timeline]
    for previous, current in zip(ordered, ordered[1:]):
        if current["start_seconds"] != previous["end_seconds"]:
            errors.append(
                f"{ERR_MIXED_TIMELINE_MISMATCH}:gap_or_overlap@"
                f"{current['start_seconds']}"
            )
    return errors


def project_mixed_template_onto_units(
    units: Any,
    storyboard: Any = None,
    contract: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    """Stamp the frozen per-shot contract onto compiled capture units.

    Code already owns the contract; the model is never asked to restate it, so
    the projection is deterministic and cannot drift.  Matching is by shot id
    only.  A compiled clip list that does not line up with the template is
    reported as ``MISMATCH`` with hard errors and is *not* partially projected,
    so no caller can mistake a half-applied contract for a usable script.

    Returns ``{"status", "errors", "applied_units", "timeline"}`` where status
    is one of ``SKIPPED`` / ``APPLIED`` / ``MISMATCH``.
    """

    data = dict(contract or {})
    if _text(data.get("execution_profile")) != ACCESSORY_MIXED_TEMPLATE_PROFILE:
        return {
            "status": PROJECTION_STATUS_SKIPPED,
            "errors": [],
            "applied_units": 0,
            "timeline": {},
        }

    contract_units = [
        item
        for item in (data.get("capture_units") or [])
        if isinstance(item, Mapping)
    ]
    if not contract_units:
        return {
            "status": PROJECTION_STATUS_SKIPPED,
            "errors": [],
            "applied_units": 0,
            "timeline": {},
        }

    by_id = {
        _text(item.get("unit_id")): item
        for item in contract_units
        if _text(item.get("unit_id"))
    }
    unit_list = [item for item in (units or []) if isinstance(item, dict)]
    timeline, _ = frozen_unit_timeline(data)

    errors = validate_mixed_shot_projection(unit_list, data)
    if errors:
        return {
            "status": PROJECTION_STATUS_MISMATCH,
            "errors": errors,
            "applied_units": 0,
            "timeline": timeline,
        }

    applied = 0
    for unit in unit_list:
        source = by_id.get(_unit_id_of(unit))
        if source is None:
            continue
        for key in (
            "module",
            "module_label",
            "view_scope",
            "view_label",
            "carrier_mode",
            "face_policy",
            "product_state",
            "body_zone",
            "duration_seconds",
            "observation_job",
            "allowed_framing",
            "forbidden_framing",
            "action_boundary",
            "state_boundary",
            "quantity_rule",
            "visible_quantity_rule",
            "continuity_group",
            "structure_role",
            "action",
        ):
            if key in source:
                unit[key] = source[key]
        span = timeline.get(_text(source.get("unit_id")))
        if span:
            unit["time_range"] = dict(span)
            unit["timeline_projection_version"] = TIMELINE_PROJECTION_VERSION
        applied += 1

    # Mirror the same carrier onto the storyboard so every downstream reader sees
    # one consistent per-shot answer.
    #
    # The timeline is mirrored too, and that is not cosmetic: the storyboard's own
    # ``time_range`` is what the renderer puts in the prompt header
    # (``production_script_renderer`` reads the storyboard, not the units), so
    # mirroring only the carrier left a 15-second contract able to render as a
    # 12-second video -- projection reported APPLIED and every check passed,
    # because the checks only ever looked at ``capture_units`` (I4).
    undated_shots: List[str] = []
    if isinstance(storyboard, list):
        for shot in storyboard:
            if not isinstance(shot, dict):
                continue
            source = by_id.get(_unit_id_of(shot))
            if source is None:
                undated_shots.append(_unit_id_of(shot) or "<no id>")
                continue
            for key in (
                "module",
                "carrier_mode",
                "face_policy",
                "product_state",
                "body_zone",
                "observation_job",
                # Review #8: the central voiceover inherits the per-shot carrier
                # and continuity group.  Without mirroring it here the voiceover
                # would fall back to "the whole film is one worn shot", which is
                # the opposite of what the frozen montage says.
                "continuity_group",
            ):
                if key in source:
                    shot[key] = source[key]
            span = timeline.get(_text(source.get("unit_id")))
            shot_range = format_mixed_shot_time_range(span)
            if shot_range:
                shot["time_range"] = shot_range
                shot["timeline_projection_version"] = TIMELINE_PROJECTION_VERSION
            else:
                undated_shots.append(_unit_id_of(shot) or "<no id>")

    # The timeline is derived one way only, so a storyboard shot that could not be
    # derived must fail loudly rather than keep a timeline of its own.
    if undated_shots:
        return {
            "status": PROJECTION_STATUS_MISMATCH,
            "errors": [
                f"{ERR_MIXED_TIMELINE_MISMATCH}:storyboard 第 {position} 镜未能从"
                f"冻结合同派生时间轴（{shot_id}）"
                for position, shot_id in enumerate(undated_shots, start=1)
            ],
            "applied_units": 0,
            "timeline": timeline,
        }

    # Re-check the fields that only exist *after* projection (carrier, state).
    post_errors = validate_mixed_shot_projection(unit_list, data)
    if post_errors:
        return {
            "status": PROJECTION_STATUS_MISMATCH,
            "errors": post_errors,
            "applied_units": 0,
            "timeline": timeline,
        }

    return {
        "status": PROJECTION_STATUS_APPLIED if applied else PROJECTION_STATUS_SKIPPED,
        "errors": [],
        "applied_units": applied,
        "timeline": timeline,
    }


# ── Difference judgement on the final shots ────────────────────────────
#
# Review #3: the batch/history comparison ran on the *legacy* scene signature
# (``persona_role|scene_motif|opening_action|action_grammar|outfit|persona_id``)
# and the mixed contract was never compared at all.  Templates and environment
# recipes rotated together on the item index, so every third candidate repeated
# the same pair, and two candidates whose four final shots were byte-identical
# were accepted as different content because their scene signature differed.
#
# The signatures below are computed from what the shots actually are.  That is
# what makes "只有环境差异不计新内容" checkable and what stops a rotating theme
# label from manufacturing difference the shots do not have.
#
# Relationship to ``creative_diversity_contract.visual_signature``: that one
# stays exactly as it is.  It measures persona/scene/outfit rotation, which is a
# different axis and still governs the non-accessory categories.  This module
# adds the final-shot axis *in addition*, and the two must not be mixed -- hence
# ``signature_version`` below.

MIXED_SIGNATURE_VERSION = "mixed-signature-v1"
MIXED_HISTORY_METADATA_KEY = "mixed_signature"
MIXED_SIGNATURE_KEY = "final_shot_signature"

# Which side of the plan/render boundary a signature was computed on.  A
# planned signature answers "what was this item asked to shoot"; a rendered one
# answers "what did the model actually write".  They are never interchangeable:
# comparing across the two reports plan differences as if they were delivered
# differences, which is precisely the confusion this axis exists to remove.
MIXED_SIGNATURE_KIND_PLANNED = "PLANNED"
MIXED_SIGNATURE_KIND_RENDERED = "RENDERED"

# The ledger keeps both, under separate keys.  A reservation has a planned
# signature from the moment it is created and a rendered one only once the model
# has written the script, so they cannot share one slot.
MIXED_RENDERED_HISTORY_METADATA_KEY = "mixed_rendered_signature"

# Comparison outcomes.  Names are the review's own vocabulary; no new
# production state machine is introduced for them.
MIXED_VERDICT_EXACT_DUPLICATE = "EXACT_DUPLICATE"
MIXED_VERDICT_SURFACE_ONLY = "SURFACE_ONLY"
MIXED_VERDICT_EXECUTION_VARIANT = "EXECUTION_VARIANT"
MIXED_VERDICT_DISTINCT_THEME = "DISTINCT_THEME"
MIXED_VERDICT_NEEDS_REVIEW = "NEEDS_REVIEW"

# When several references disagree about one candidate, a single duplicate
# reference is enough to refuse it, so the most duplicate-like verdict wins.
_MIXED_VERDICT_SEVERITY = {
    MIXED_VERDICT_EXACT_DUPLICATE: 5,
    MIXED_VERDICT_SURFACE_ONLY: 4,
    MIXED_VERDICT_NEEDS_REVIEW: 3,
    MIXED_VERDICT_EXECUTION_VARIANT: 2,
    MIXED_VERDICT_DISTINCT_THEME: 1,
}

# Verdicts that refuse a candidate.  Independence has to be *demonstrated*: a
# difference we cannot verify is not accepted either, or the batch report would
# claim a dedup pass it never performed.
MIXED_BLOCKING_VERDICTS = frozenset(
    {
        MIXED_VERDICT_EXACT_DUPLICATE,
        MIXED_VERDICT_SURFACE_ONLY,
        MIXED_VERDICT_NEEDS_REVIEW,
    }
)

# Why a candidate was refused, for the batch report and the planning rejection
# list.  Duplicate and evidence reasons stay separate counts on purpose.
MIXED_REJECT_DUPLICATE = "MIXED_DUPLICATE_CANDIDATE"
MIXED_REJECT_EVIDENCE = "MIXED_EVIDENCE_UNVERIFIED"

# Verdicts that also bar a *generated* script from the independent delivery set.
# Both assert that the shots themselves are a repeat of something already
# delivered -- only the light/tabletop differs in the second one -- so counting
# the item as a finished, distinct script would overstate what the batch
# produced.
#
# NEEDS_REVIEW is deliberately absent.  An uncertain similarity is not a repeat;
# the plan asks for a human to look at those, not for an unbounded model
# rewrite, so those items are delivered *flagged* instead of withheld.
MIXED_NON_DELIVERABLE_VERDICTS = frozenset(
    {
        MIXED_VERDICT_EXACT_DUPLICATE,
        MIXED_VERDICT_SURFACE_ONLY,
    }
)


def mixed_delivery_decision(report: Mapping[str, Any] | None) -> Dict[str, Any]:
    """May this item be delivered as an *independent* script?

    ``usable`` False means the item must not enter the automatic production
    candidate set (首帧任务 / 生产脚本表 / 生产交接), because a "完成数" that
    counted it would not be a count of distinct content.

    ``requires_review`` marks the opposite case: an item that is delivered but
    whose rendered similarity to something else could not be resolved
    mechanically.
    """

    data = report if isinstance(report, Mapping) else {}
    if not data:
        return {
            "usable": True,
            "verdict": "",
            "reject_code": "",
            "requires_review": False,
            "reason": "",
        }

    verdict = _text(data.get("review_status")).upper()
    summary = _text(data.get("difference_summary"))
    nearest = _text(data.get("nearest_script_id"))
    if verdict in MIXED_NON_DELIVERABLE_VERDICTS:
        reason = f"{verdict}｜与 {nearest} 重复：{summary}" if nearest else f"{verdict}｜{summary}"
        return {
            "usable": False,
            "verdict": verdict,
            "reject_code": MIXED_REJECT_DUPLICATE,
            "requires_review": False,
            "nearest_script_id": nearest,
            "reason": reason,
        }

    collapsed = bool(data.get("montage_collapsed"))
    flagged = verdict == MIXED_VERDICT_NEEDS_REVIEW or collapsed
    return {
        "usable": True,
        "verdict": verdict,
        "reject_code": "",
        "requires_review": flagged,
        "nearest_script_id": nearest,
        "reason": summary if flagged else "",
    }


def _signature_atom(value: Any) -> str:
    """Normalize one comparable token.

    Punctuation, spacing and case are wording, not meaning: ``整体轮廓`` and
    ``整体轮廓。`` describe the same requirement and must not read as a
    difference.  Only the alphanumeric/CJK skeleton survives.
    """

    text = _text(value)
    if not text:
        return ""
    return "".join(
        char
        for char in text
        if char.isalnum() or "\u4e00" <= char <= "\u9fff"
    ).lower()


def _signature_atoms(values: Any) -> List[str]:
    items = values if isinstance(values, (list, tuple, set)) else [values]
    return sorted({atom for atom in (_signature_atom(item) for item in items) if atom})


def _signature_digest(payload: Any) -> str:
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()[:20]


# ── 成稿特征：版本化 + 可回溯（开发包 B1）──────────────────────────────
#
# 为什么单独版本化：旧签名把成稿压成一个 ``visual_content`` 全文 hash，于是
# "背景换词"会移动 digest，被读成"镜头变了"。这里改成**结构字段 + 表层分离**，
# 并给特征加版本号，使新→旧签名比较能直接判"不可比"（Review R2 的第二个口子：
# 旧版不可比时默认成不同）。旧签名没有该字段，一律按 v0 对待。
MIXED_RENDERED_FEATURE_VERSION = "mixed-rendered-features-v1"
MIXED_RENDERED_FEATURE_VERSION_LEGACY = "mixed-rendered-features-v0"
MIXED_RENDERED_FEATURE_SOURCE = "RULE_BASED_V1"

# 可见主体：由 module/carrier_mode 决定，不由措辞决定。
_VISIBLE_SUBJECT_BY_MODULE = {
    "HANDHELD_PRODUCT": "PRODUCT_ONLY",
    "STATIC_PRODUCT": "PRODUCT_AND_SURFACE",
    "WORN_DETAIL": "PRODUCT_AND_WEARER",
    "WORN_RELATION": "PRODUCT_AND_WEARER",
}

# 表层词表（闭环）。只有出现在这里的差异才允许被判成 SURFACE_ONLY；表外的
# 差异一律不下"只是换了说法"的结论 —— 判错成一个独立变体比判成待复核更贵。
_SURFACE_HEADS = (
    "背景", "台面", "桌面", "底布", "衬布", "布料", "毯", "垫",
    "光线", "光影", "光斑", "光比", "亮度", "曝光", "色温", "色调", "影调",
    "冷调", "暖调", "氛围", "景深", "虚化",
    "BGM", "音乐", "配乐", "节奏点",
)
_SURFACE_MODIFIERS = (
    "灰", "灰色", "白", "白色", "米", "米白", "米色", "木", "木色", "原木",
    "暖", "冷", "柔", "硬", "亮", "暗", "深", "浅", "淡", "素", "净",
    "逆光", "侧光", "顺光", "顶光", "自然光", "柔光", "窗光", "冷光", "暖光",
    # 光源与反射类措辞：环境配方会写"侧窗柔光""轻微反射补亮"这类短语，
    # 词表漏一个，"背景换词"就会掉进"结构差异"。
    "侧窗", "天然光", "主光", "辅光", "补光", "高光", "反光", "反射光", "漫射光",
    "反射", "补亮", "明暗", "阴影", "投影", "亮部", "暗部", "层次", "哑光",
    # 近义措辞：同一套环境配方在实际分镜里会被写成不同说法（"侧窗柔光" 会写成
    # "侧向窗光"，"浅木台面" 会写成 "木质台面"）。词表只认一种写法，"近义改写"
    # 就会掉进"结构差异"。这里补的是**布景与光线**的同义词，不含商品名词、不含
    # 动作词 —— 商品名词与装饰数量另由 ``_PRODUCT_NOUNS`` / ``_COUNT_RE`` 保护。
    # 表外的新说法不倒向"独立变体"，而是走 NEEDS_REVIEW（提取不确定）。
    "侧向", "同方向", "窗边", "窗台", "墙面", "地面", "地板", "室内光", "环境光",
    "自然光线", "环境", "布景", "场景", "木纹", "木质", "原木色", "中性光",
    "低饱和", "高饱和", "背景色", "环境色", "留白",
)

# 受保护内容：这些差异**不许**被当成表层措辞删掉。
#  - 商品原色：颜色词与商品名词的共现（"灰色夹体" vs "灰色背景" 必须分开）
#  - 装饰数量：数量 + 部件量词
#  - 已验证部件名
_PRODUCT_NOUNS = (
    "商品", "本体", "夹体", "夹子", "发夹", "抓夹", "发饰", "耳饰", "耳钉", "耳环",
    "手链", "手镯", "腕饰", "戒指", "指环", "发圈", "发带", "头绳", "丝带", "缎带",
    "花朵", "花瓣", "蝶", "蝴蝶", "珍珠", "水钻", "钻", "链条", "链节", "搭扣",
    "圈口", "弧架", "簪身", "齿口", "吊坠", "镶嵌", "金属",
    # 装饰部件与表面的通称。实测成稿会写"乳白色半透明层叠装饰"，而颜色与最近
    # 的商品名之间隔着 5 个字；不把这些通称算进来，"商品原色"就保护不住 ——
    # 方案明确要求结构、装饰数量与商品原色不得被当成无关词删掉。
    "装饰", "图案", "纹样", "花纹", "镶边", "部件", "表面",
)
_COLOR_TERMS = (
    "灰", "灰色", "白", "白色", "米白", "米色", "棕", "棕色", "金", "金色",
    "银", "银色", "粉", "粉色", "红", "红色", "蓝", "蓝色", "绿", "绿色",
    "黑", "黑色", "紫", "紫色", "透明", "半透明",
)
_COUNT_RE = re.compile(
    r"(?:[一二两三两三四五六七八九十百千单双半\d]+)\s*"
    r"(?:层|个|片|只|对|排|枚|颗|条|圈|瓣|支|根|束)"
)
# 颜色词与商品名词之间允许隔多少个字才算"在说商品颜色"。
#
# 判据不是"附近出现过颜色字"，也不是固定窗口：还要**中间不能夹表层中心词**。
# 否则 "米白背景下的手与商品画面" 里的 "米白" 会被误判成商品颜色（它离"商品"
# 只有几个字），而 "商品整体呈灰色" 里的 "灰色" 反而漏判。两个方向都必须看：
# 中文既可写"灰色夹体"，也可写"商品整体呈灰色"。
#
# 窗口取 6：中文里"乳白色半透明层叠装饰"这种修饰串正好 5 个字，取 4 会漏判。
# 放宽窗口靠的是**表层中心词**继续挡住误判（"米白背景下的手与商品"仍被挡住），
# 而不是靠窗口窄 —— 误判成"受保护"只会把结果推向待复核，不会变成表层豁免。
_COLOR_ADJACENCY = 6
_SURFACE_HEADS_PATTERN = (
    "背景", "台面", "桌面", "底布", "衬布",
    "墙面", "墙", "地面", "地板", "布景", "环境",
)


@lru_cache(maxsize=1)
def surface_terms() -> Tuple[str, ...]:
    """表层词汇：静态表 ∪ 环境配方里实际使用的措辞。

    环境配方是这条线真正会写进分镜的布景词来源（浅木台面 / 米白背景 /
    侧窗柔光 …）。手工词表漏一个词，"背景换词"就会掉进"结构差异"，
    所以这里直接从配置采集，按长度降序供最长匹配使用。
    """

    terms = set(_SURFACE_HEADS) | set(_SURFACE_MODIFIERS)
    try:
        definition = load_mixed_template_definition()
    except Exception:  # noqa: BLE001 - 读不到配置就用静态表
        definition = {}
    recipes = definition.get("environment_recipes")
    if isinstance(recipes, Mapping):
        for recipe in recipes.values():
            if not isinstance(recipe, Mapping):
                continue
            for key in ("environment", "label"):
                value = recipe.get(key)
                if not isinstance(value, str):
                    continue
                for chunk in re.split(r"[、，,；;。/\s]+", value):
                    chunk = chunk.strip()
                    if 2 <= len(chunk) <= 12:
                        terms.add(chunk)
    return tuple(sorted(terms, key=len, reverse=True))


def _feature_text(value: Any) -> str:
    return _text(value)


def rendered_visible_subject(module: Any) -> str:
    return _VISIBLE_SUBJECT_BY_MODULE.get(_text(module).upper(), "UNKNOWN")


def rendered_action_state(product_state: Any) -> str:
    """冻结物理状态 → 可比较的机械状态（不含任何措辞）。"""

    state = _text(product_state).upper()
    return {
        "ALREADY_WORN": "WORN",
        "WORN": "WORN",
        "HELD": "HELD",
        "RESTING_ON_SURFACE": "RESTING",
        "PLACED_ON_SURFACE": "RESTING",
    }.get(state, state or "UNSPECIFIED")


def protected_phrases(text: Any) -> List[str]:
    """不受表层豁免保护的片段：商品原色、装饰数量、已验证部件名。

    ``灰色背景`` 与 ``灰色夹体`` 必须分开：前者是布景措辞，后者是商品原色。
    判据是颜色词是否**紧邻商品名词**，不是"出现过某个颜色字"。
    """

    source = _text(text)
    if not source:
        return []
    found: List[str] = []
    for match in _COUNT_RE.finditer(source):
        found.append(_signature_atom(match.group(0)))
    for noun in _PRODUCT_NOUNS:
        start = 0
        while True:
            index = source.find(noun, start)
            if index < 0:
                break
            noun_end = index + len(noun)
            for color in _COLOR_TERMS:
                for color_index in _find_all(source, color):
                    if _color_binds_to_product(source, color_index, len(color), index, noun_end):
                        found.append(_signature_atom(f"{color}{noun}"))
            start = noun_end
    return sorted({item for item in found if item})


def _find_all(source: str, needle: str) -> List[int]:
    out: List[int] = []
    cursor = 0
    while True:
        index = source.find(needle, cursor)
        if index < 0:
            return out
        out.append(index)
        cursor = index + 1


def _color_binds_to_product(
    source: str,
    color_index: int,
    color_len: int,
    noun_index: int,
    noun_end: int,
) -> bool:
    """这个颜色词是在说商品，还是在说布景？

    三种情况都算"说商品"：紧邻、隔少量字（"整体呈灰色"）、或写成"灰色夹体"。
    只要颜色与商品之间存在表层中心词（"米白**背景**下的手与商品"），就不算 ——
    那是布景颜色，属于表层措辞。
    """

    if color_index + color_len <= noun_index:
        between = source[color_index + color_len: noun_index]
    elif color_index >= noun_end:
        between = source[noun_end: color_index]
    else:
        return True  # 与商品名词重叠，必然在修饰它
    if len(between) > _COLOR_ADJACENCY:
        return False
    return not any(head in between for head in _SURFACE_HEADS_PATTERN)


# 表层词按长度降序：先用最长匹配，避免"柔光"被"光"抢先切碎。
# 实际词表由 ``surface_terms()`` 组装（静态表 ∪ 环境配方措辞）。


def _surface_spans(source: str, protected: Sequence[str]) -> List[Tuple[int, int]]:
    """表层措辞的字符区间。

    从最长匹配出发，再向左吸收紧邻的表层修饰词 —— 这样 ``米白背景``、
    ``浅木台面``、``侧窗柔光`` 各自成一个整体区间，而不是把词切碎。
    与受保护短语重叠的区间一律不返回（那句话在说商品，不是在说布景）。
    """

    terms = surface_terms()
    spans: List[Tuple[int, int]] = []
    cursor = 0
    while cursor < len(source):
        matched = None
        for term in terms:
            if source.startswith(term, cursor):
                matched = term
                break
        if not matched:
            cursor += 1
            continue
        start, end = cursor, cursor + len(matched)
        # 向左吸收紧邻的表层修饰词
        extended = True
        while extended:
            extended = False
            for term in terms:
                if start - len(term) >= 0 and source.startswith(term, start - len(term)):
                    start -= len(term)
                    extended = True
                    break
        atom = _signature_atom(source[start:end])
        if atom and not any(
            atom and (atom in item or item in atom) for item in protected
        ):
            spans.append((start, end))
        cursor = end
    # 去掉被更长区间包住的碎片（"米白" 被 "米白背景" 包住），避免短语表被噪声撑满。
    kept: List[Tuple[int, int]] = []
    for span in sorted(spans, key=lambda item: (item[0], -(item[1] - item[0]))):
        if any(span[0] >= outer[0] and span[1] <= outer[1] and span != outer for outer in spans):
            continue
        if span not in kept:
            kept.append(span)
    return sorted(kept)


def surface_phrases(text: Any) -> List[str]:
    """表层片段：环境、光线、台面、音乐等近义措辞所在的短语。"""

    source = _text(text)
    if not source:
        return []
    protected = protected_phrases(source)
    return sorted(
        {
            atom
            for start, end in _surface_spans(source, protected)
            for atom in (_signature_atom(source[start:end]),)
            if atom
        }
    )


def strip_surface(text: Any) -> str:
    """去掉表层片段后剩下的骨架 —— 用于判断"差异是否全在表层"。"""

    source = _text(text)
    if not source:
        return ""
    protected = protected_phrases(source)
    spans = _surface_spans(source, protected)
    if not spans:
        return _signature_atom(source)
    out: List[str] = []
    cursor = 0
    for start, end in sorted(spans):
        if start < cursor:
            continue
        out.append(source[cursor:start])
        cursor = end
    out.append(source[cursor:])
    return _signature_atom("".join(out))


def _shot_framing_phrase(shot: Mapping[str, Any]) -> str:
    """成稿自己声明的取景尺度。

    成稿不写 ``view_scope``（那是冻结合同的取值域），但每一镜都写了 ``camera``，
    开头就是取景描述（"竖屏佩戴近景，手机固定在鞋柜边，从后脑与侧后方记录…"）。
    取**第一小句**：既是成稿自己的声明，又不会因为后文的机位与剪切措辞变化而
    让取景尺度跟着漂移。成稿什么都没写才回落到冻结合同的 ``view_scope``。

    这一步很关键：若取景一律以冻结合同为准，计划里的模板就会决定成稿 digest，
    I3 修好的"计划抵消正文相同"会原地复现（同一条正文配不同模板 → 签名不同）。
    """

    for key in ("view_scope", "framing"):
        value = _text(shot.get(key))
        if value:
            return value.upper() if key == "view_scope" else _signature_atom(value)
    camera = _text(shot.get("camera"))
    if not camera:
        return ""
    for boundary in ("，", "；", "。", ",", ";"):
        index = camera.find(boundary)
        if index > 0:
            camera = camera[:index]
            break
    return _signature_atom(camera)[:24]


def rendered_shot_features(
    shot: Mapping[str, Any],
    unit: Mapping[str, Any],
    position: int,
    *,
    bound_by_id: bool = False,
) -> Dict[str, Any]:
    """一镜的成稿特征：结构字段 + 两个原句 + 回溯路径。

    ``observed_part_or_relation`` / ``action_or_state`` / ``framing_scale`` /
    ``actual_module`` 取冻结单元：它们是**硬边界**，成稿必须在该边界内执行，
    而 A 包的一致性检查已经证明交付提示词确实执行了它们。措辞差异不进结构字段。

    ``bound_by_id`` 记的是这一镜的冻结单元是**按 ``capture_unit_id`` 配到的**
    还是按位置配到的。位置配对会把"换顺序"这件事抹掉（结构字段读的是单元列表，
    而单元列表没变），于是真实换序看起来跟没动过一样。``position`` 始终是交付
    顺序里的位置，用于回溯原文；结构字段的来源则按实际配对方式标注。

    **字段来源：成稿先声明，冻结单元只补位。** 观众收到的是这一镜*实际写了*
    什么，所以 ``module`` / ``observation_job`` / ``product_state`` 先读成稿
    自己的声明；成稿没声明（旧稿、夹具）才回落到冻结单元，并用 ``feature_source``
    如实标注。成稿不声明取景边界（``view_scope``），所以 ``framing_scale`` 由
    冻结单元提供 —— 那是允许范围，不是措辞。若反过来一律以冻结单元为准，
    计划里的模板顺序就会决定成稿 digest，I3 修的"计划抵消正文相同"会原地复现。
    """

    unit = unit if isinstance(unit, Mapping) else {}
    shot = shot if isinstance(shot, Mapping) else {}
    module = _text(unit.get("module")).upper()
    job = _text(unit.get("observation_job"))
    unit_ref = (
        f"capture_units[capture_unit_id={_text(unit.get('unit_id'))}]"
        if bound_by_id
        else f"capture_units[{position - 1}]"
    )
    shot_ref = f"storyboard[{position - 1}]"

    def _prefer(field: str, normalise) -> Tuple[str, str]:
        """成稿声明优先，成稿没声明才回落到冻结单元；返回 (值, 来源)。"""

        raw = shot.get(field)
        if _text(raw):
            return normalise(raw), shot_ref
        return normalise(unit.get(field)), unit_ref

    module, module_ref = _prefer("module", lambda value: _text(value).upper())
    job, job_ref = _prefer("observation_job", _text)
    state, state_ref = _prefer("product_state", rendered_action_state)
    # 取景尺度先读成稿自己的声明（view_scope/framing/camera 首句），成稿什么都没
    # 写才回落到冻结合同的 view_scope —— 那是允许范围，不是这镜实际拍了什么。
    framing = _shot_framing_phrase(shot)
    if framing:
        framing_ref = shot_ref
    else:
        framing = _text(unit.get("view_scope")).upper()
        framing_ref = unit_ref
    return {
        "sequence_position": int(position),
        "actual_module": module,
        "visible_subject": rendered_visible_subject(module),
        "observed_part_or_relation": job,
        "observation_supported": bool(job) and _signature_atom(job) in authored_observation_jobs(),
        "action_or_state": state,
        "framing_scale": framing,
        "unit_binding": "CAPTURE_UNIT_ID" if bound_by_id else "POSITION",
        # 每个特征出自成稿还是出自冻结边界。报告要能回答"这句话/这个模块是谁定的"。
        "feature_source": {
            "actual_module": "SHOT" if module_ref == shot_ref else "CONTRACT",
            "observed_part_or_relation": "SHOT" if job_ref == shot_ref else "CONTRACT",
            "action_or_state": "SHOT" if state_ref == shot_ref else "CONTRACT",
            "framing_scale": "SHOT" if framing_ref == shot_ref else "CONTRACT",
        },
        # 原句保留：报告要能回答"这句话出自哪里"，审计要能回到原文。
        "rendered_visual_content": _text(shot.get("visual_content")),
        "rendered_character_action": _text(shot.get("character_action")),
        "source_refs": {
            "actual_module": f"{module_ref}.module",
            "visible_subject": f"{module_ref}.module",
            "observed_part_or_relation": f"{job_ref}.observation_job",
            "action_or_state": f"{state_ref}.product_state",
            "framing_scale": f"{framing_ref}.camera" if framing_ref == shot_ref
            else f"{framing_ref}.view_scope",
            "rendered_visual_content": f"{shot_ref}.visual_content",
            "rendered_character_action": f"{shot_ref}.character_action",
        },
    }


_STRUCTURAL_FEATURE_KEYS = (
    "actual_module",
    "visible_subject",
    "observed_part_or_relation",
    "action_or_state",
    "framing_scale",
)


def structural_shot_tuple(shot: Mapping[str, Any]) -> List[str]:
    """结构指纹：与顺序无关地取该镜的结构字段。

    同时接受成稿特征与计划特征（``_mixed_unit_visual``）两种形状 —— 计划侧
    用的是 ``module`` / ``observation_job`` / ``product_state``，成稿侧用的是
    ``actual_*`` 前缀。归一化在这里做一次，比较层就不再关心版本。
    """

    data = shot if isinstance(shot, Mapping) else {}
    module = _text(data.get("actual_module") or data.get("module")).upper()
    subject = _text(data.get("visible_subject")) or rendered_visible_subject(module)
    job = _signature_atom(
        data.get("observed_part_or_relation") or data.get("observation_job")
    )
    state = _text(data.get("action_or_state")) or rendered_action_state(
        data.get("product_state")
    )
    framing = _text(data.get("framing_scale") or data.get("view_scope")).upper()
    return [module, subject, job, state, framing]


def feature_version_of(visual: Mapping[str, Any]) -> str:
    """该签名用的是哪一版特征提取。缺字段 = 旧版（v0），不是"未知"。"""

    data = visual if isinstance(visual, Mapping) else {}
    return _text(data.get("feature_version")) or MIXED_RENDERED_FEATURE_VERSION_LEGACY


def rendered_text_delta(
    candidate_visual: Mapping[str, Any],
    reference_visual: Mapping[str, Any],
) -> Dict[str, Any]:
    """成稿措辞差异的归因：全在表层，还是碰到了受保护内容。

    比较必须区分"灰色背景"与"灰色商品"：前者是表层措辞，后者是商品原色。
    受保护片段（商品原色 / 装饰数量 / 已确认部件）一旦不同，就不允许判成
    SURFACE_ONLY。
    """

    def _texts(visual: Mapping[str, Any]) -> List[str]:
        out: List[str] = []
        for shot in (visual.get("shots") or []):
            if not isinstance(shot, Mapping):
                continue
            out.append(_text(shot.get("rendered_visual_content")))
            out.append(_text(shot.get("rendered_character_action")))
        return out

    cand_texts = _texts(candidate_visual)
    ref_texts = _texts(reference_visual)
    if not cand_texts or not ref_texts or not any(cand_texts + ref_texts):
        # 两边都没有可比原文（计划签名、旧签名、只剩结构字段的夹具）。这里的
        # `attributable=False` 与"原文相同"必须分开：否则空文本互相比等会被读成
        # "正文一字未改"，进而把只改了口播卖点的稿判成"完全相同"。
        return {
            "surface_only": False,
            "protected_differ": [],
            "skeleton_differ": [],
            "attributable": False,
            "texts_identical": None,
        }
    # None = 无原文可比（计划签名/旧签名）。这与 False（有原文且不同）必须分开：
    # 前者退回签名摘要比较，后者才允许下"只是换说法"的结论。
    texts_identical = cand_texts == ref_texts
    cand_protected: List[str] = []
    ref_protected: List[str] = []
    cand_skeleton: List[str] = []
    ref_skeleton: List[str] = []
    for text in cand_texts:
        cand_protected.extend(protected_phrases(text))
        cand_skeleton.append(strip_surface(text))
    for text in ref_texts:
        ref_protected.extend(protected_phrases(text))
        ref_skeleton.append(strip_surface(text))
    protected_differ = sorted(set(cand_protected) ^ set(ref_protected))
    skeleton_differ = sorted(
        {
            item
            for left, right in zip(cand_skeleton, ref_skeleton)
            if left != right
            for item in (left, right)
            if item
        }
    )
    surface_only = not protected_differ and not skeleton_differ
    return {
        "surface_only": surface_only,
        "protected_differ": protected_differ,
        "skeleton_differ": skeleton_differ,
        "texts_identical": texts_identical,
        # 能否把这次差异归因到表层。归因不了就不许下"只是换了说法"的结论。
        "attributable": True,
    }


@lru_cache(maxsize=1)
def authored_observation_jobs() -> frozenset:
    """Every observation job a human wrote for this template family.

    Used as the evidence test for a *new* observation point.  A job the author
    wrote has already been bounded by the parts the subtype is allowed to claim;
    anything outside that vocabulary is planner-invented and cannot be certified
    as evidenced offline, so it is routed to review instead of being accepted.
    """

    authored: set = set()
    definition = load_mixed_template_definition()

    def _collect(value: Any) -> None:
        if isinstance(value, Mapping):
            for key, item in value.items():
                if _text(key) in {"observation_job", "fallback_observation"}:
                    if isinstance(item, str) and _text(item):
                        authored.add(_signature_atom(item))
                _collect(item)
        elif isinstance(value, (list, tuple)):
            for item in value:
                _collect(item)
        elif isinstance(value, str):
            pass

    _collect(definition.get("templates"))
    _collect(definition.get("physical_subtype_rules"))
    _collect(definition.get("category_rules"))
    # ``observation_focus`` is a list of plain strings under a known key, so the
    # generic walker above would drop it.
    for bucket in (
        definition.get("physical_subtype_rules"),
        definition.get("category_rules"),
    ):
        if isinstance(bucket, Mapping):
            for rule in bucket.values():
                if isinstance(rule, Mapping):
                    for key in ("observation_focus", "distinct_jobs"):
                        value = rule.get(key)
                        if isinstance(value, Mapping):
                            for item in value.values():
                                if _text(item):
                                    authored.add(_signature_atom(item))
                        elif isinstance(value, (list, tuple)):
                            for item in value:
                                if _text(item):
                                    authored.add(_signature_atom(item))
    authored.discard("")
    return frozenset(authored)


def _first_sentence(text: Any) -> str:
    source = _text(text)
    if not source:
        return ""
    for boundary in ("。", "！", "？", ".", "!", "?", "\n"):
        index = source.find(boundary)
        if index > 0:
            return source[: index + 1]
    return source


def mixed_rendered_semantic_signature(
    script: Mapping[str, Any] | None,
    base: Mapping[str, Any] | None,
    rendered_shots: Sequence[Mapping[str, Any]] = (),
) -> Dict[str, Any]:
    """主题语义：读**成稿**，不读计划。

    R4 的两个口子都在这里堵：

    1. 旧 digest 对 ``approved_claim_refs`` / ``evidence_refs`` 求 hash。那两个
       列表是**商品事实**引用，不同主题只要引用同一组商品事实就会算出同一个
       digest —— 于是"不同主题"被判成相同。商品事实只该回答"用了哪些商品事实"。
    2. ``thesis`` 在规划期会退化成内部 ID（实测为
       ``ARGUMENT_OPERATOR_PCS_..._PCL_...``）。内部 ID 不是主题文案，不能当作
       观众问题，也不能成为主题判定的依据。

    现在 digest 只由**成稿里实际说出口的购买理由**（``selling_argument_realization_zh``）
    与结构化的观众问题（``hook_id`` + ``voiceover_context_mode``）决定；
    ``selected_selling_argument_id`` 只作为来源保留，不参与 digest。
    对不上的取值一律记 ``input_gap``，不用内部 ID 冒充主题文案。
    """

    data = script if isinstance(script, Mapping) else {}
    contract = base if isinstance(base, Mapping) else {}
    theme = (
        contract.get("content_theme")
        if isinstance(contract.get("content_theme"), Mapping)
        else {}
    )
    voice = (
        data.get("continuous_voiceover")
        if isinstance(data.get("continuous_voiceover"), Mapping)
        else {}
    )
    core_value = _text(
        voice.get("selling_argument_realization_zh")
        or voice.get("selling_argument_realization")
    )
    audience_question = _first_sentence(voice.get("chinese_translation"))
    hook_id = _text(voice.get("hook_id"))
    context_mode = _text(voice.get("voiceover_context_mode")).upper()
    claim_source_id = _text(voice.get("selected_selling_argument_id"))

    # 读得出来就写读出来的；读不出来就明确记缺口，不用内部 ID 顶上。
    thesis = _text(theme.get("thesis"))
    input_gap = ""
    if not is_readable_theme_proposition(thesis, theme):
        input_gap = MIXED_THESIS_INPUT_GAP_READABILITY
        thesis = core_value
    if not core_value:
        input_gap = "|".join(filter(None, (input_gap, "CORE_VALUE_UNAVAILABLE")))
    if not hook_id and not context_mode and not audience_question:
        input_gap = "|".join(filter(None, (input_gap, "AUDIENCE_QUESTION_UNAVAILABLE")))

    # 结构化的观众问题 + 实际说出口的购买理由 = 语义。措辞归一，标点与大小写
    # 不算差异（"看着柔美又梦幻。" 与 "看着柔美又梦幻" 是同一句）。
    meaning = {
        "hook_id": hook_id,
        "voiceover_context_mode": context_mode,
        "core_value": _signature_atom(core_value),
    }
    visible_answer = sorted(
        {
            _text(shot.get("observed_part_or_relation"))
            for shot in rendered_shots
            if isinstance(shot, Mapping) and _text(shot.get("observed_part_or_relation"))
        }
    )
    return {
        "proposition": thesis,
        "audience_question": audience_question,
        "core_value": core_value,
        "visible_answer": visible_answer,
        "claim_type": hook_id or context_mode,
        "claim_source_id": claim_source_id,
        "input_gap": input_gap,
        "theme_label": _text(theme.get("theme_id")),
        "digest": _signature_digest(meaning),
        "source_refs": {
            "core_value": "script.continuous_voiceover.selling_argument_realization_zh",
            "audience_question": "script.continuous_voiceover.chinese_translation",
            "hook_id": "script.continuous_voiceover.hook_id",
            "voiceover_context_mode": "script.continuous_voiceover.voiceover_context_mode",
            "claim_source_id": "script.continuous_voiceover.selected_selling_argument_id",
            "visible_answer": "storyboard[*] × capture_units[*].observation_job",
        },
        # 计划侧只留作审计：它回答"原本要被安排成什么主题"，不参与判定。
        "planned": {
            "theme_id": _text(theme.get("theme_id")),
            "thesis": _text(theme.get("thesis")),
            "approved_claim_refs": _signature_atoms(theme.get("approved_claim_refs")),
            "evidence_refs": _signature_atoms(theme.get("evidence_refs")),
            "candidate_role": _text(theme.get("candidate_role")),
        },
    }


def is_readable_theme_proposition(
    value: Any, theme: Mapping[str, Any] | None = None
) -> bool:
    """Can ``value`` be shown to a reviewer as *what this video claims*?

    Not readable: empty text, the ``UNAVAILABLE``-family sentinels the upstream
    books use for "no value", anything shaped like an internal ID, and any
    string that merely repeats one of the theme's own identifiers.
    """

    text = _text(value).strip()
    if not text:
        return False
    if text.upper() in _MIXED_THESIS_UNREADABLE_SENTINELS:
        return False
    if text.upper().startswith(_MIXED_THESIS_ID_PREFIXES):
        return False
    data = theme if isinstance(theme, Mapping) else {}
    markers = {
        _text(data.get("theme_id")),
        _text(data.get("parent_theme_id")),
        _text(data.get("candidate_role")),
        _text(data.get("argument_id")),
    }
    markers.discard("")
    return text not in markers


def resolve_theme_proposition(
    candidates: Any,
) -> Dict[str, str]:
    """从候选来源里挑第一条**可读**命题，来源一并留下。

    ``candidates`` 是 ``[{"source": 短标签, "ref": 来源路径, "text": 文本}, ...]``，
    **列表顺序即优先级**。返回 ``thesis`` / ``thesis_source`` /
    ``thesis_source_ref`` / ``thesis_input_gap`` 四项。全部读不出来时 ``thesis``
    为空并给出缺口码 —— 宁可口径上少一条主题，也不拿内部 ID 冒充主题文案
    （Review R4）。
    """

    for entry in candidates if isinstance(candidates, (list, tuple)) else []:
        if not isinstance(entry, Mapping):
            continue
        text = _text(entry.get("text")).strip()
        if not is_readable_theme_proposition(text):
            continue
        return {
            "thesis": text,
            "thesis_source": _text(entry.get("source")),
            "thesis_source_ref": _text(entry.get("ref")),
            "thesis_input_gap": "",
        }
    return {
        "thesis": "",
        "thesis_source": "",
        "thesis_source_ref": "",
        "thesis_input_gap": MIXED_THESIS_INPUT_GAP_READABILITY,
    }


def theme_proposition(theme: Mapping[str, Any] | None) -> Tuple[str, str]:
    """主题命题：只接受**可读文案**，内部 ID 不算命题。

    规划期的 ``thesis`` 曾退化成 ``ARGUMENT_OPERATOR_PCS_..._PCL_...``（实测四条
    真实稿全部如此）。那是内部 ID，既不是观众问题也不是购买理由；把它当主题
    文案，报告看起来"有主题"，实际上什么也没说（Review R4）。返回
    ``(命题, 输入缺口)``：读不出来就明确记缺口，不用内部 ID 冒充。

    规划期的写入侧现在由 ``resolve_theme_proposition`` 负责，本函数是读取侧的
    同一套判据 —— 历史包里已经写坏的 ``thesis`` 依然会被读成缺口。
    """

    data = theme if isinstance(theme, Mapping) else {}
    if is_readable_theme_proposition(data.get("thesis"), data):
        return _text(data.get("thesis")).strip(), ""
    return "", MIXED_THESIS_INPUT_GAP_READABILITY


def mixed_product_fact_signature(contract: Mapping[str, Any] | None) -> Dict[str, Any]:
    """这一条**用了哪些商品事实** —— 与主题语义分开的一支。

    它回答的只是"引用了哪些 claim/evidence、商品有哪些已确认结构"，因此允许
    按 ID 比较。旧实现把 claim/evidence 的 ID 哈希当成主题语义，于是两个*不同*
    主题只要引用同一组商品事实，就被判成同一个主题（Review R4）—— 商品事实
    不是主题，主题是观众问题与购买理由。
    """

    data = contract if isinstance(contract, Mapping) else {}
    theme = (
        data.get("content_theme")
        if isinstance(data.get("content_theme"), Mapping)
        else {}
    )
    payload = {
        "claims": _signature_atoms(theme.get("approved_claim_refs")),
        "evidence": _signature_atoms(theme.get("evidence_refs")),
        "observations": _signature_atoms(data.get("observation_focus")),
        "structure": {
            _text(key): _text(state).upper()
            for key, state in (data.get("structure_facts") or {}).items()
            if _text(key)
        },
        "verified_parts": _mixed_verified_parts(data),
    }
    return {
        **payload,
        "digest": _signature_digest(payload),
        "source_refs": {
            "claims": "content_theme.approved_claim_refs",
            "evidence": "content_theme.evidence_refs",
            "structure": "structure_facts",
            "verified_parts": "structure_facts（三态：VERIFIED/ABSENT/UNKNOWN）",
        },
        "note": "商品事实轴：只回答用了哪些商品事实，不参与主题语义判定",
    }


def mixed_semantic_signature(contract: Mapping[str, Any] | None) -> Dict[str, Any]:
    """主题语义 = 观众问题 + 购买理由（**可读文案**）。

    The digest excludes the theme id and the claim/evidence ids: ``theme_id``
    rotates per item, and the claim set only says which product facts were
    quoted.  Two different buying reasons built on the same product facts
    would otherwise collapse into one theme -- exactly the hole Review R4
    found.
    """

    data = contract if isinstance(contract, Mapping) else {}
    theme = data.get("content_theme") if isinstance(data.get("content_theme"), Mapping) else {}
    proposition, gap = theme_proposition(theme)
    core_value = _text(theme.get("core_value") or theme.get("value_proposition"))
    audience_question = _text(theme.get("audience_question"))
    if not proposition:
        proposition = core_value
    if not core_value:
        core_value = proposition
    if not proposition:
        gap = "|".join(filter(None, (gap, "THEME_PROPOSITION_UNAVAILABLE")))
    if not audience_question:
        gap = "|".join(filter(None, (gap, "AUDIENCE_QUESTION_UNAVAILABLE")))
    meaning = {
        "proposition": _signature_atom(proposition),
        "audience_question": _signature_atom(audience_question),
    }
    return {
        "proposition": proposition,
        "core_value": core_value,
        "audience_question": audience_question,
        "theme_label": _text(theme.get("theme_id")),
        "input_gap": gap,
        "claims": _signature_atoms(theme.get("approved_claim_refs")),
        "evidence": _signature_atoms(theme.get("evidence_refs")),
        "observations": _signature_atoms(data.get("observation_focus")),
        # 商品事实与 claim/evidence 的 ID 属于**商品事实轴**
        # （``mixed_product_fact_signature``），不再参与主题语义 digest。
        "product_fact_digest_source": "mixed_product_fact_signature",
        "digest": _signature_digest(meaning),
    }


def _mixed_unit_visual(unit: Mapping[str, Any]) -> Dict[str, Any]:
    # Deliberately *not* here: the unit's ``evidence_refs``.  Those are claim
    # ids, i.e. what the shot is being used to prove -- semantic content.  A
    # visual signature that moved when the claim set changed would report "the
    # picture changed" for a re-worded argument, which is the mirror image of
    # the defect being fixed here.  Verified *parts* enter at contract level
    # (see ``verified_parts``).
    return {
        "unit_id": _text(unit.get("unit_id")),
        "module": _text(unit.get("module")).upper(),
        "view_scope": _text(unit.get("view_scope")).upper(),
        "framing": _signature_atom(unit.get("framing")),
        "allowed_framing": _signature_atoms(unit.get("allowed_framing")),
        "forbidden_framing": _signature_atoms(unit.get("forbidden_framing")),
        "observation_job": _signature_atom(unit.get("observation_job")),
        "action": _signature_atom(unit.get("action")),
        "action_boundary": _signature_atoms(unit.get("action_boundary")),
        "product_state": _text(unit.get("product_state")).upper(),
        "body_zone": _text(unit.get("body_zone")).upper(),
        "carrier_mode": _text(unit.get("carrier_mode")).upper(),
    }


def _mixed_verified_parts(contract: Mapping[str, Any]) -> Dict[str, str]:
    """The parts this product may actually show, from the frozen evidence.

    ABSENT/UNKNOWN entries stay in the map: "the chain is confirmed absent" is
    a different shooting instruction from "the chain was never confirmed", and
    the two must not collapse into the same signature.
    """

    facts = contract.get("structure_facts")
    facts = facts if isinstance(facts, Mapping) else {}
    return {
        _text(key): _text(state).upper()
        for key, state in facts.items()
        if _text(key)
    }


def mixed_visual_signature(contract: Mapping[str, Any] | None) -> Dict[str, Any]:
    """按最终镜头记录 module、观察重点、已验证部件、动作、裁切、佩戴关系和首镜任务。"""

    data = contract if isinstance(contract, Mapping) else {}
    units = [
        unit
        for unit in (data.get("capture_units") or [])
        if isinstance(unit, Mapping)
    ]
    shots: List[Dict[str, Any]] = []
    for position, unit in enumerate(units, start=1):
        shot = _mixed_unit_visual(unit)
        shot["is_opening"] = position == 1
        # 首镜任务: the opening shot's job and its framing decide the still frame,
        # so they are recorded as a dedicated field rather than left implicit in
        # the ordered list.
        shot["opening_job"] = shot["observation_job"] if position == 1 else ""
        shots.append(shot)
    verified_parts = _mixed_verified_parts(data)
    payload = {
        "signature_kind": MIXED_SIGNATURE_KIND_PLANNED,
        "template_id": _text(data.get("template_id")),
        "face_policy": _text(data.get("face_policy")).upper(),
        "verified_parts": verified_parts,
        "shots": shots,
    }
    return {
        "signature_kind": MIXED_SIGNATURE_KIND_PLANNED,
        "template_id": payload["template_id"],
        "face_policy": payload["face_policy"],
        "verified_parts": verified_parts,
        "shots": shots,
        "digest": _signature_digest(payload),
    }


def mixed_surface_signature(contract: Mapping[str, Any] | None) -> Dict[str, Any]:
    """台面、光线配方、颜色风格、BGM 等辅助变化 —— 仅用于排期/轮换。"""

    data = contract if isinstance(contract, Mapping) else {}
    recipe = data.get("environment_recipe") if isinstance(data.get("environment_recipe"), Mapping) else {}
    payload = {
        "environment_recipe_id": _text(data.get("environment_recipe_id")),
        "environment_recipe_version": int(data.get("environment_recipe_version") or 0),
        "audio_route": _text(data.get("audio_route")).upper(),
        "total_duration_seconds": int(data.get("total_duration_seconds") or 0),
        "cut_policy": {
            _text(key): _text(value)
            for key, value in (data.get("cut_policy") or {}).items()
            if _text(key)
        },
        "recipe_label": _signature_atom(recipe.get("label") or recipe.get("display_name")),
    }
    return {**payload, "digest": _signature_digest(payload)}


def mixed_signature_bundle(contract: Mapping[str, Any] | None) -> Dict[str, Any]:
    """The three signatures plus the version they were computed under."""

    data = contract if isinstance(contract, Mapping) else {}
    if not data or not (data.get("capture_units") or []):
        return {}
    return {
        "signature_version": MIXED_SIGNATURE_VERSION,
        "semantic": mixed_semantic_signature(data),
        "visual": mixed_visual_signature(data),
        "surface": mixed_surface_signature(data),
        # 商品事实单独一支：它回答"用了哪些商品事实"，与"主题是什么"是两件事。
        "product_fact": mixed_product_fact_signature(data),
    }


def _ledger_reference(
    record: Mapping[str, Any] | None,
    metadata_key: str,
    *,
    missing_reason: str,
) -> Dict[str, Any]:
    """A comparable reference built from one historical usage record.

    ``complete`` is False when the record carries no bundle under
    ``metadata_key``, when the bundle predates these signatures, or when its
    version is not the one this module computes.  A missing final-shot set is
    *not* reconstructed from the legacy scene signature: a fabricated reference
    would silently mark real repeats as new content, which is the failure this
    whole mechanism exists to prevent.
    """

    data = record if isinstance(record, Mapping) else {}
    # The ledger persists metadata as JSON text and ``SELECT *`` hands back the
    # raw column, while in-memory callers pass the parsed dict.  Both shapes
    # have to work, or every historical row would silently look incomplete.
    metadata = data.get("metadata")
    if metadata is None:
        metadata = data.get("metadata_json")
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (TypeError, ValueError):
            metadata = {}
    metadata = metadata if isinstance(metadata, Mapping) else {}
    identity = (
        _text(metadata.get("batch_item_id"))
        or _text(data.get("usage_id"))
        or _text(data.get("direction_id"))
    )
    bundle = metadata.get(metadata_key)
    if not isinstance(bundle, Mapping) or not bundle:
        return {
            "identity": identity,
            "complete": False,
            "reason": missing_reason,
            "signature": {},
            "signature_version": "",
        }
    if _text(bundle.get("signature_version")) != MIXED_SIGNATURE_VERSION:
        return {
            "identity": identity,
            "complete": False,
            "reason": "HISTORY_SIGNATURE_VERSION_MISMATCH",
            "signature": dict(bundle),
            "signature_version": _text(bundle.get("signature_version")),
        }
    if not (bundle.get("visual") or {}).get("shots"):
        return {
            "identity": identity,
            "complete": False,
            "reason": missing_reason,
            "signature": dict(bundle),
            "signature_version": MIXED_SIGNATURE_VERSION,
        }
    return {
        "identity": identity,
        "complete": True,
        "reason": "",
        "signature": dict(bundle),
        "signature_version": MIXED_SIGNATURE_VERSION,
    }


def ledger_row_identity(record: Mapping[str, Any] | None) -> Dict[str, str]:
    """The identifying fields of one usage-ledger row.

    ``batch_id`` / ``batch_item_id`` are written into ``metadata_json`` at
    reservation time, ``usage_id`` is the primary key, and ``script_id`` appears
    once a script was produced.  Reading all four in one place keeps the "is
    this row mine?" test from drifting between the planner and the executor.
    """

    row = record if isinstance(record, Mapping) else {}
    # The ledger persists metadata as one JSON column and ``SELECT *`` hands back
    # ``metadata_json``, while in-memory callers pass the parsed ``metadata``
    # dict.  Both shapes must work: reading only one of them makes every row
    # look like it belongs to nobody, and the "my own row" filter then silently
    # stops filtering.
    metadata = row.get("metadata")
    if metadata is None:
        metadata = row.get("metadata_json")
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (TypeError, ValueError):
            metadata = {}
    metadata = metadata if isinstance(metadata, Mapping) else {}
    return {
        "usage_id": _text(row.get("usage_id")) or _text(metadata.get("usage_id")),
        "batch_id": _text(row.get("batch_id")) or _text(metadata.get("batch_id")),
        "batch_item_id": _text(row.get("batch_item_id"))
        or _text(metadata.get("batch_item_id")),
        "script_id": _text(row.get("script_id")) or _text(metadata.get("script_id")),
    }


def ledger_row_is_own_record(
    record: Mapping[str, Any] | None,
    *,
    batch_ids: Any = (),
    usage_ids: Any = (),
    batch_item_ids: Any = (),
    script_ids: Any = (),
) -> bool:
    """Does this ledger row belong to the batch being processed *right now*?

    Any single match is enough: the four keys are written at different moments
    (usage id and batch id at reservation, script id at generation), so a row
    that survived a partial run may carry only some of them.  When every key set
    is empty this always returns ``False``, so a fresh plan filters nothing it
    did not filter before.

    Why the question matters: resuming a batch re-plans (or re-runs) it, and its
    own earlier rows come back as "history".  Every candidate would then be
    compared against its own previous incarnation, read as a duplicate, and the
    batch would deliver nothing -- Review R3.
    """

    wanted = {
        "batch_id": {_text(item) for item in (batch_ids or ()) if _text(item)},
        "usage_id": {_text(item) for item in (usage_ids or ()) if _text(item)},
        "batch_item_id": {
            _text(item) for item in (batch_item_ids or ()) if _text(item)
        },
        "script_id": {_text(item) for item in (script_ids or ()) if _text(item)},
    }
    if not any(wanted.values()):
        return False
    identity = ledger_row_identity(record)
    for key, values in wanted.items():
        value = identity.get(key) or ""
        if value and value in values:
            return True
    return False


def mixed_reference_signature(record: Mapping[str, Any] | None) -> Dict[str, Any]:
    """The *planned* signature of one historical usage record.

    This is the reference planning compares against: it is the reservation the
    ledger made, which is all that exists before anything has been generated.
    """

    return _ledger_reference(
        record, MIXED_HISTORY_METADATA_KEY, missing_reason="HISTORY_INCOMPLETE"
    )


def mixed_rendered_reference_signature(
    record: Mapping[str, Any] | None,
) -> Dict[str, Any]:
    """The *rendered* signature of one historical usage record.

    Kept on its own key with its own reason: a reservation that never generated
    has a planned signature and no rendered one, which is a different fact from
    a history row that could not be read -- and neither may be counted as "this
    product delivered that already".
    """

    return _ledger_reference(
        record,
        MIXED_RENDERED_HISTORY_METADATA_KEY,
        missing_reason="RENDERED_HISTORY_MISSING",
    )


def _signature_kind(visual: Mapping[str, Any] | None) -> str:
    """Which side of the plan/render boundary one visual signature was built on.

    Bundles written before the kinds existed only ever held planned signatures,
    so a missing kind reads as PLANNED rather than "unknown".  The alternative
    would declare the entire existing ledger uncomparable, and the one thing
    this field must never do is let a plan difference pass as a rendered one.
    """

    data = visual if isinstance(visual, Mapping) else {}
    return _text(data.get("signature_kind")).upper() or MIXED_SIGNATURE_KIND_PLANNED


def compare_mixed_signatures(
    candidate: Mapping[str, Any] | None,
    reference: Mapping[str, Any] | None,
) -> Dict[str, Any]:
    """Explain how ``candidate`` differs from one ``reference``.

    Order matters and mirrors the review's table: identical final shots are a
    duplicate even when the surrounding theme label differs, because the shots
    are what the viewer receives.  A differing shot set is only *independent*
    when the new observation point is authored evidence; otherwise the verdict
    is NEEDS_REVIEW and the candidate does not claim a dedup pass.
    """

    cand = candidate if isinstance(candidate, Mapping) else {}
    ref = reference if isinstance(reference, Mapping) else {}
    cand_visual = cand.get("visual") or {}
    ref_visual = ref.get("visual") or {}
    if not cand_visual or not ref_visual:
        return {}

    # Two signatures only describe the same object when they sit on the same
    # side of the plan/render boundary.  A planned template difference must
    # never be allowed to veto a rendered script, and vice versa; the honest
    # outcome for an incomparable pair is "needs review", not "different".
    cand_kind = _signature_kind(cand_visual)
    ref_kind = _signature_kind(ref_visual)
    if cand_kind != ref_kind:
        return {
            "review_status": MIXED_VERDICT_NEEDS_REVIEW,
            "difference_dimensions": ["SIGNATURE_KIND_MISMATCH"],
            "difference_summary": (
                f"{cand_kind} 签名与 {ref_kind} 签名不可直接比较，"
                "计划差异不得当成成稿差异"
            ),
            "counts_as_independent": False,
            "counts_as_theme": False,
            "counts_as_variant": False,
            "order_only": False,
            "semantic_identical": False,
            "visual_identical": False,
            "surface_identical": False,
        }

    # Two different feature versions describe the same script differently.
    # Treating that as "different" would book a version bump as original
    # content, so an incomparable pair is routed to review instead.
    cand_feature = feature_version_of(cand_visual)
    ref_feature = feature_version_of(ref_visual)
    if cand_feature != ref_feature:
        return {
            "review_status": MIXED_VERDICT_NEEDS_REVIEW,
            "difference_dimensions": ["FEATURE_VERSION_MISMATCH"],
            "difference_summary": (
                f"成稿特征版本 {cand_feature} 与 {ref_feature} 不可直接比较；"
                "旧签名需先转换或明确按不可比处理"
            ),
            "counts_as_independent": False,
            "counts_as_theme": False,
            "counts_as_variant": False,
            "order_only": False,
            "candidate_feature_version": cand_feature,
            "reference_feature_version": ref_feature,
            "semantic_identical": False,
            "visual_identical": False,
            "surface_identical": False,
        }

    cand_shots = [
        shot for shot in (cand_visual.get("shots") or []) if isinstance(shot, Mapping)
    ]
    ref_shots = [
        shot for shot in (ref_visual.get("shots") or []) if isinstance(shot, Mapping)
    ]
    cand_struct = [structural_shot_tuple(shot) for shot in cand_shots]
    ref_struct = [structural_shot_tuple(shot) for shot in ref_shots]
    # 逐位比较：结构相同 = 同样的模块/主体/观察点/状态/取景，且顺序一致。
    same_visual = bool(cand_struct) and cand_struct == ref_struct
    # ``SHOT_ORDER_DIFFERS`` 只在**实际比较过序列**之后才允许输出。
    # 不能把它当成"没找到新观察点"的默认落点 —— R2 的误判正是这么来的：
    # 背景换词让 digest 变了，代码找不到新观察点，就把结论写成"顺序不同"。
    order_only = (
        not same_visual
        and bool(cand_struct)
        and len(cand_struct) == len(ref_struct)
        and sorted(cand_struct) == sorted(ref_struct)
    )
    same_surface = _text((cand.get("surface") or {}).get("digest")) == _text(
        (ref.get("surface") or {}).get("digest")
    )
    cand_semantic_digest = _text((cand.get("semantic") or {}).get("digest"))
    ref_semantic_digest = _text((ref.get("semantic") or {}).get("digest"))
    cand_fact_digest = _text((cand.get("product_fact") or {}).get("digest"))
    ref_fact_digest = _text((ref.get("product_fact") or {}).get("digest"))
    # 商品事实轴单独比较：只有两边都算得出来才算"可比且相同"。
    same_product_fact = bool(
        cand_fact_digest and cand_fact_digest == ref_fact_digest
    )
    delta = rendered_text_delta(cand_visual, ref_visual)
    # 只有两边都算得出来、且**有可比原文**时才能谈"语义变了但画面没变"。
    # 计划签名没有成稿原文，它这里只能回答"计划安排的观察点是否相同"，不能
    # 回答"说出口的卖点变了没有" —— 那要靠成稿口播字段。
    semantic_changed = bool(
        delta.get("attributable")
        and cand_semantic_digest
        and ref_semantic_digest
        and cand_semantic_digest != ref_semantic_digest
    )
    same_semantic = bool(
        cand_semantic_digest and cand_semantic_digest == ref_semantic_digest
    )
    texts_identical = delta.get("texts_identical")
    surface_only_delta = bool(delta.get("attributable")) and bool(delta.get("surface_only"))
    protected_delta = bool(delta.get("attributable")) and bool(delta.get("protected_differ"))

    dimensions: List[str] = []
    order_only_flag = False
    new_jobs: List[str] = []
    if same_visual:
        # 四镜一模一样，但说出口的购买理由变了：这不是"同一个视频"。画面没有
        # 对应支撑，所以既不能算独立内容，也不该被报成"完全相同"——那会把
        # "口播换了卖点、画面没跟上"这件事从报告里抹掉（§B2 同一格）。
        if protected_delta:
            # 结构一致但商品原色/装饰数量/已确认部件变了：这不是措辞差异，
            # 不得用"表层变化"把它抹掉，也不能当成独立变体直接放行。
            verdict = MIXED_VERDICT_NEEDS_REVIEW
            dimensions = ["FINAL_SHOTS_IDENTICAL", "PROTECTED_CONTENT_DIFFERS"]
            summary = (
                "结构相同，但商品原色/装饰数量/已确认部件出现差异，需人工确认："
                + "、".join(delta.get("protected_differ") or [])[:120]
            )
        elif semantic_changed:
            verdict = MIXED_VERDICT_NEEDS_REVIEW
            dimensions = ["FINAL_SHOTS_IDENTICAL", "THEME_CHANGED_WITHOUT_VISIBLE_SUPPORT"]
            summary = (
                "四镜画面完全相同，但购买理由/观众问题变了且画面未给出对应支撑，"
                "需人工复核（仅口播换卖点不自动计为独立视频，也不宣布为完全相同）"
            )
        elif same_surface and texts_identical is not False:
            verdict = MIXED_VERDICT_EXACT_DUPLICATE
            dimensions = ["FINAL_SHOTS_IDENTICAL", "SURFACE_IDENTICAL"]
            summary = "四镜最终约束与台面光线完全一致，仅序号/旧场景签名不同"
        elif surface_only_delta:
            verdict = MIXED_VERDICT_SURFACE_ONLY
            dimensions = ["FINAL_SHOTS_IDENTICAL", "SURFACE_DIFFERENT"]
            summary = "四镜结构相同，差异全部落在背景/光线等表层措辞"
        elif texts_identical is False:
            # 原文确实不同，但既归因不到表层、也没碰到受保护内容 —— 分类器
            # 说不清这是什么差异。此时**不能**说"正文完全一致"（那是漏报重复），
            # 也不能说"新增了独立内容"（那是虚报原创）。按不可确定处理。
            verdict = MIXED_VERDICT_NEEDS_REVIEW
            dimensions = ["FINAL_SHOTS_IDENTICAL", "UNATTRIBUTABLE_TEXT_DIFFERENCE"]
            summary = (
                "结构相同但成稿措辞存在无法机械归因的差异，需人工复核"
                "（不据此宣布为独立内容，也不宣布为完全相同）"
            )
        else:
            verdict = MIXED_VERDICT_SURFACE_ONLY
            dimensions = ["FINAL_SHOTS_IDENTICAL", "SURFACE_DIFFERENT"]
            summary = "最终镜头约束相同，只有环境/光线等辅助变化"
    else:
        dimensions = ["FINAL_SHOTS_DIFFERENT"]
        new_jobs = _new_observation_jobs(cand_visual, ref_visual)
        if order_only:
            # 同一组镜头、只换了顺序：是执行变体，但**默认不增有效变体**。
            # 这条分支必须排在"新观察点"之前 —— 顺序变化不会带来新观察点，
            # 但重复出现的观察点会让 new_jobs 看起来非空。
            order_only_flag = True
            dimensions.append("SHOT_ORDER_DIFFERS")
        elif new_jobs:
            dimensions.append("NEW_OBSERVATION_POINT")
        elif surface_only_delta and texts_identical is False:
            # 结构指纹不同（例如为了换环境换了模板），但**成稿措辞**的差异全在
            # 表层，且没有受保护内容变化 —— 这正是 R2 的"背景换词被判独立变体"
            # 应落的位置。要求原文确实不同：措辞一字未改而结构声明不同，是另一
            # 回事（见下一支）。
            verdict = MIXED_VERDICT_SURFACE_ONLY
            dimensions.append("SURFACE_ONLY_REWORDED")
            summary = "结构相同范围内仅表层措辞变化，不计为独立变体"
            return _mixed_comparison_payload(
                verdict, dimensions, summary, same_semantic, same_visual,
                same_surface, order_only_flag, delta, same_product_fact=same_product_fact,
            )
        elif surface_only_delta:
            # 成稿原文一字未改，却声明了不同的模块/观察点/物理状态：这不是重复
            # （执行契约不同），也不是有证据的新观察点（没有任何新观察内容）。
            # 归因不了就不许自动计为独立变体，交给人工（§B2 提取不确定）。
            verdict = MIXED_VERDICT_NEEDS_REVIEW
            dimensions.append("STRUCTURE_DIVERGES_WITH_IDENTICAL_TEXT")
            summary = (
                "成稿正文一字未改，但交付的模块/观察点声明与参照不同，"
                "需人工复核后再决定是否独立"
            )
            return _mixed_comparison_payload(
                verdict, dimensions, summary, same_semantic, same_visual,
                same_surface, order_only_flag, delta, same_product_fact=same_product_fact,
            )
        elif cand_feature == MIXED_RENDERED_FEATURE_VERSION_LEGACY:
            # 旧签名没有原文可归因，保持历史语义；但不再假称"顺序不同"。
            dimensions.append("SHOT_SET_DIFFERS")
        else:
            # 差异解释不了：既没有新的有证据观察点，也不是顺序变化，也归因不到
            # 表层。不许自动当成独立变体 —— 那会把"看起来有点不一样"变成
            # 原创质量指标。交给人工看。
            verdict = MIXED_VERDICT_NEEDS_REVIEW
            dimensions.append("UNEXPLAINED_DIFFERENCE")
            summary = "镜头结构存在无法机械归因的差异，需人工复核后再决定是否独立"
            return _mixed_comparison_payload(
                verdict, dimensions, summary, same_semantic, same_visual,
                same_surface, order_only_flag, delta, same_product_fact=same_product_fact,
            )

        if same_semantic:
            dimensions.append("SEMANTIC_IDENTICAL")
        else:
            dimensions.append("SEMANTIC_DIFFERENT")
        unsupported = sorted(job for job in new_jobs if job not in authored_observation_jobs())
        legacy_features = (
            cand_feature == MIXED_RENDERED_FEATURE_VERSION_LEGACY
        )
        if unsupported:
            verdict = MIXED_VERDICT_NEEDS_REVIEW
            dimensions.append("UNVERIFIED_NEW_OBSERVATION")
            summary = "新增观察点缺少可核查证据：" + "、".join(unsupported[:3])
        elif order_only:
            verdict = MIXED_VERDICT_EXECUTION_VARIANT
            summary = "同主题，相同观察重点以不同镜头顺序执行（默认不计为新增有效变体）"
        elif new_jobs and same_semantic:
            # 同主题 + 新增有证据的观察内容 → 执行变体（增有效变体，不增主题）
            verdict = MIXED_VERDICT_EXECUTION_VARIANT
            summary = "同主题下增加有证据支撑的新观察重点"
        elif new_jobs:
            # 购买理由也不同，且新增观察点有成稿画面支撑 → 独立主题。
            # 这一支必须排在"执行变体"之前：否则"不同购买理由"永远被
            # 吸收成变体，主题数恒为 1，报告就再也看不出真实差异（B2 表）。
            verdict = MIXED_VERDICT_DISTINCT_THEME
            summary = "购买理由有实质变化，并安排了有证据支撑的新观察镜头"
        elif same_semantic:
            verdict = MIXED_VERDICT_EXECUTION_VARIANT
            summary = "同主题，镜头集合与顺序与参照不同"
        elif legacy_features:
            # 旧签名没有原文可归因，保持历史语义，避免把版本差异当内容差异。
            verdict = MIXED_VERDICT_DISTINCT_THEME
            summary = "主题问题有实质变化（旧签名无可归因原文，按历史口径处理）"
        else:
            # 购买理由不同，但成稿里没有一条新的可见观察点来支撑它 ——
            # 只有口播换了卖点、画面没跟上。这正是方案点名的情形：
            # "只有口播换卖点、四镜完全相同，不自动计为独立视频"。
            verdict = MIXED_VERDICT_NEEDS_REVIEW
            dimensions.append("THEME_CHANGED_WITHOUT_VISIBLE_SUPPORT")
            summary = (
                "购买理由变化但成稿未给出对应的新观察镜头，需人工复核"
                "（仅口播换卖点不自动计为独立视频）"
            )

    return _mixed_comparison_payload(
        verdict, dimensions, summary, same_semantic, same_visual, same_surface,
        order_only_flag, delta, new_jobs_locals=new_jobs,
        same_product_fact=same_product_fact,
    )


def _mixed_comparison_payload(
    verdict: str,
    dimensions: List[str],
    summary: str,
    same_semantic: bool,
    same_visual: bool,
    same_surface: bool,
    order_only: bool,
    delta: Mapping[str, Any],
    *,
    new_jobs_locals: Any = None,
    same_product_fact: Optional[bool] = None,
) -> Dict[str, Any]:
    """One comparison result, with the counting axes the batch report needs.

    ``counts_as_independent`` keeps its original meaning (may this be delivered
    as usable content) so the delivery gate is unchanged.  Two narrower axes are
    added because "producible" and "new" are different claims:

    * ``counts_as_theme`` — does this establish a *distinct buying reason*.
    * ``counts_as_variant`` — is this a *new effective variant* of the theme.
      An order-only re-shuffle is executed differently but adds nothing new, so
      it counts as neither.

    ``NEEDS_REVIEW`` counts as neither: an unresolved similarity must not be
    spent as original content in either column.
    """

    new_jobs = [
        job for job in (new_jobs_locals or []) if isinstance(job, str) and job
    ]
    supported_new = [
        job for job in new_jobs if job in authored_observation_jobs()
    ]
    counts_as_theme = verdict == MIXED_VERDICT_DISTINCT_THEME
    counts_as_variant = bool(
        verdict == MIXED_VERDICT_EXECUTION_VARIANT
        and not order_only
        and supported_new
    )
    return {
        "review_status": verdict,
        "difference_dimensions": dimensions,
        "difference_summary": summary,
        # Only a demonstrated, evidenced difference may be counted as
        # independent content.  NEEDS_REVIEW is included on purpose: claiming a
        # dedup pass we could not verify is the same defect one layer down.
        "counts_as_independent": verdict not in MIXED_BLOCKING_VERDICTS,
        "counts_as_theme": counts_as_theme,
        "counts_as_variant": counts_as_variant,
        "order_only": bool(order_only),
        "new_observation_points": supported_new,
        "protected_content_differ": list(delta.get("protected_differ") or []),
        # 商品事实轴：与主题语义分开汇报。两者可以同时不同（用了不同商品事实
        # 支撑不同主题），也可以只差一项 —— 混在一起就无法解释裁决。
        "product_fact_identical": same_product_fact,
        "semantic_identical": same_semantic,
        "visual_identical": same_visual,
        "surface_identical": same_surface,
    }


def _new_observation_jobs(
    candidate_visual: Mapping[str, Any],
    reference_visual: Mapping[str, Any],
) -> List[str]:
    """观察点差异，按归一化的结构指纹取，不按措辞取。

    旧实现读的是 ``observation_job`` 这个键，而成稿特征用的是
    ``observed_part_or_relation``；于是成稿比较时这里恒为空 —— 那句"没有新观察点"
    就成了 ``SHOT_ORDER_DIFFERS`` 的默认落点（R2 的误判链条）。
    统一走 ``structural_shot_tuple`` 后，同一个观察点被重新措辞不再算作新观察点。
    """

    def _jobs(visual: Mapping[str, Any]) -> List[str]:
        return [
            structural_shot_tuple(shot)[2]
            for shot in (visual.get("shots") or [])
            if isinstance(shot, Mapping) and structural_shot_tuple(shot)[2]
        ]

    existing = set(_jobs(reference_visual))
    return sorted({job for job in _jobs(candidate_visual) if job not in existing})


def judge_mixed_candidate(
    contract: Mapping[str, Any] | None,
    references: Iterable[Mapping[str, Any]] | None = None,
    *,
    history_compared: int = 0,
    history_incomplete: int = 0,
) -> Dict[str, Any]:
    """Fill a contract's ``difference_report`` from the final-shot comparison.

    ``references`` are comparable candidates: this batch's already-accepted
    items plus the history rows.  Incomplete history rows are counted but never
    used as references, so a missing record can never be mistaken for "no
    duplicate found".
    """

    signature = mixed_signature_bundle(contract)
    report: Dict[str, Any] = {
        "signature_version": MIXED_SIGNATURE_VERSION,
        "nearest_script_id": "",
        "difference_dimensions": [],
        "difference_summary": "本批与历史中暂无同类混合候选可比对",
        "review_status": MIXED_VERDICT_DISTINCT_THEME,
        "counts_as_independent": True,
        "comparison_scope": "CANDIDATE_ONLY",
        "history_compared": int(history_compared),
        "history_incomplete": int(history_incomplete),
        # 与成稿侧同口径：没有可比参照时三项一律 False。规划的批次报告按唯一
        # script_id 汇总时只能读这里，不能从"规划成功"或"换了模板"推。
        "counts_as_theme": False,
        "counts_as_variant": False,
        "order_only": False,
    }
    if not signature:
        report["review_status"] = MIXED_VERDICT_NEEDS_REVIEW
        report["counts_as_independent"] = False
        report["difference_summary"] = "合同缺少最终镜头，无法计算差异签名"
        report["difference_dimensions"] = ["NO_FINAL_SHOTS"]
        return report

    best: Dict[str, Any] = {}
    for reference in references or []:
        ref_signature = reference.get("signature") if isinstance(reference, Mapping) else None
        if not isinstance(ref_signature, Mapping) or not ref_signature:
            continue
        outcome = compare_mixed_signatures(signature, ref_signature)
        if not outcome:
            continue
        outcome["identity"] = _text(reference.get("identity"))
        if (
            not best
            or _MIXED_VERDICT_SEVERITY.get(outcome["review_status"], 0)
            > _MIXED_VERDICT_SEVERITY.get(best["review_status"], 0)
        ):
            best = outcome

    if not best:
        # Nothing comparable existed.  Two very different situations produce
        # this, and the report must not let them collapse into one:
        #
        # * no history at all  -> "nothing to compare against" is the honest
        #   statement, and ``CANDIDATE_ONLY`` already says so.
        # * same-product history that could not be read (written before the
        #   final-shot signatures, or under an older version) -> the comparison
        #   did *not* happen.  Reporting a bare ``DISTINCT_THEME`` would book an
        #   inferior record as "checked and found different".
        #
        # The candidate still ships: an unreadable *historical* record must not
        # block new content (that rule predates this change).  What changes is
        # that the pass is now named, so a reader can tell "compared" from
        # "could not compare".
        if int(history_incomplete) > 0:
            report["comparison_scope"] = "HISTORY_INCOMPARABLE"
            report["difference_dimensions"] = ["HISTORY_INCOMPARABLE"]
            report["difference_summary"] = (
                f"同商品历史有 {int(history_incomplete)} 条缺少可比签名"
                "（版本不同或未写最终镜头），本次**未完成**跨批比对；"
                "不得读成『已比对且确认不同』"
            )
            report["history_incomparable"] = int(history_incomplete)
        return report

    verdict = best["review_status"]
    report.update(
        {
            "nearest_script_id": best.get("identity", ""),
            "difference_dimensions": list(best.get("difference_dimensions") or []),
            "difference_summary": _text(best.get("difference_summary")),
            "review_status": verdict,
            "counts_as_independent": bool(best.get("counts_as_independent")),
            "comparison_scope": "BATCH_AND_HISTORY",
            # B2 的计数轴随裁决一起落盘；规划期汇总只能读这里。
            "counts_as_theme": bool(best.get("counts_as_theme")),
            "counts_as_variant": bool(best.get("counts_as_variant")),
            "order_only": bool(best.get("order_only")),
        }
    )
    return report


def mixed_signature_from_script(
    script: Mapping[str, Any] | None,
    contract: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    """Final-shot signature of what the model *actually wrote*.

    This is the second check the plan asks for, so it must not smuggle the plan
    back in.  An earlier revision folded ``template_id`` and the frozen
    per-shot requirements into the digest, which made two scripts with
    byte-identical 正文 look different merely because they had been planned onto
    different templates -- the plan silently cancelled the 正文 comparison it was
    supposed to be checked against.

    The digest is therefore computed from the rendered shots alone.  Everything
    the frozen contract contributed stays under ``planned``, for audit only: it
    answers a different question ("what was asked") than the digest does
    ("what was delivered").

    ``contract`` is the frozen contract to read the requirements from.  It is
    passed explicitly because the assembled script carries the storyboard but
    not always the extension the storyboard was generated from.
    """

    data = script if isinstance(script, Mapping) else {}
    base = frozen_mixed_contract(data)
    if not base and isinstance(contract, Mapping):
        base = dict(contract)
    storyboard: List[Dict[str, Any]] = []
    for shot in data.get("storyboard") or []:
        if isinstance(shot, Mapping):
            storyboard.append(shot)
    if not base or not storyboard:
        return {}
    units = [
        unit
        for unit in (base.get("capture_units") or [])
        if isinstance(unit, Mapping)
    ]
    if len(storyboard) != len(units):
        return {}
    # 一镜的冻结单元按它自己的 ``capture_unit_id`` 找。位置配对在"只换了顺序"
    # 的情形下会给出与参照完全相同的结构指纹 —— 因为结构字段读的是单元列表，
    # 而单元列表没动 —— 于是真实换序看起来像没变过。按 id 配对后顺序才是
    # 可比较的：换序会体现在结构指纹的序列上，才能输出 SHOT_ORDER_DIFFERS。
    # 没有 id 的旧稿退回位置配对，行为与之前一致。
    units_by_id = {
        _text(unit.get("unit_id")): unit
        for unit in units
        if _text(unit.get("unit_id"))
    }
    rendered: List[Dict[str, Any]] = []
    planned: List[Dict[str, Any]] = []
    for position, (unit, shot) in enumerate(zip(units, storyboard), start=1):
        # The generated shot's own visible event is what the viewer receives.
        # Nothing derived from the frozen unit may *replace* it: that is the
        # whole point of keeping the two apart.  The frozen unit supplies the
        # structural boundary the shot must execute inside; the two delivered
        # sentences are kept verbatim so every feature can be traced back to the
        # text that produced it (``source_refs``).
        bound = units_by_id.get(_text(shot.get("capture_unit_id")))
        bound_by_id = bound is not None
        rendered.append(
            rendered_shot_features(
                shot, bound if bound_by_id else unit, position, bound_by_id=bound_by_id
            )
        )
        requirement = _mixed_unit_visual(unit)
        requirement["is_opening"] = position == 1
        requirement["opening_job"] = (
            requirement["observation_job"] if position == 1 else ""
        )
        planned.append(requirement)
    content_payload = [
        structural_shot_tuple(shot) for shot in rendered
    ]
    surface_text = " ".join(
        _text(shot.get("rendered_visual_content"))
        + " "
        + _text(shot.get("rendered_character_action"))
        for shot in rendered
    )
    return {
        "signature_version": MIXED_SIGNATURE_VERSION,
        "visual": {
            "signature_kind": MIXED_SIGNATURE_KIND_RENDERED,
            "feature_version": MIXED_RENDERED_FEATURE_VERSION,
            "feature_source": MIXED_RENDERED_FEATURE_SOURCE,
            "shots": rendered,
            # ``digest`` 保持存在（旧消费者仍在读它），但它现在由**结构指纹 +
            # 去表层后的骨架**决定，不再由全文 hash 决定 —— 换背景措辞不会移动它。
            "digest": _signature_digest(
                {
                    "signature_kind": MIXED_SIGNATURE_KIND_RENDERED,
                    "feature_version": MIXED_RENDERED_FEATURE_VERSION,
                    "content": content_payload,
                    "skeleton": [strip_surface(text) for text in (
                        _text(shot.get("rendered_visual_content")) for shot in rendered
                    )],
                }
            ),
            "content_digest": _signature_digest(content_payload),
            "surface_text_digest": _signature_digest(surface_text),
            "surface_phrases": sorted(
                {phrase for phrase in surface_phrases(surface_text) if phrase}
            ),
            "protected_phrases": sorted(
                {phrase for phrase in protected_phrases(surface_text) if phrase}
            ),
            "planned": {
                "template_id": _text(base.get("template_id")),
                "face_policy": _text(base.get("face_policy")).upper(),
                "verified_parts": _mixed_verified_parts(base),
                "shots": planned,
            },
        },
        # The theme is read from the delivered script, not from the plan.  The
        # frozen thesis/theme_id stay under ``semantic.planned``: a theme claim
        # derived from an id would let a plan difference pass as delivered
        # meaning (Review R4).
        "semantic": mixed_rendered_semantic_signature(script, base, rendered),
        "surface": mixed_surface_signature(base),
        # 商品事实轴读的是冻结合同（商品事实在规划期就冻结了），与成稿措辞无关。
        "product_fact": mixed_product_fact_signature(base),
    }


def judge_rendered_script(
    script: Mapping[str, Any] | None,
    contract: Mapping[str, Any] | None = None,
    references: Iterable[Mapping[str, Any]] | None = None,
) -> Dict[str, Any]:
    """Post-generation re-check of one generated script.

    The verdict is computed from the shots the model actually wrote.  It is
    reported alongside the planned verdict rather than replacing it: a
    disagreement between the two is exactly the signal a reviewer needs
    ("planned as a variant, generated as a repeat").

    Only *rendered* signatures are usable as references.  A sibling that has
    not generated yet is represented by its frozen shots, and those were
    already compared at planning time; folding them back in here would let a
    planned template difference veto a rendered script, which is the confusion
    this re-check exists to remove.  Such references are counted, not ignored.
    """

    signature = mixed_signature_from_script(script, contract)
    if not signature:
        return {}
    reference_list: List[Mapping[str, Any]] = []
    skipped_not_rendered = 0
    for reference in references or []:
        if not isinstance(reference, Mapping) or not reference.get("signature"):
            continue
        reference_visual = (reference.get("signature") or {}).get("visual") or {}
        if _signature_kind(reference_visual) != MIXED_SIGNATURE_KIND_RENDERED:
            skipped_not_rendered += 1
            continue
        reference_list.append(reference)
    from_history = any(
        _text(reference.get("source")) == "RENDERED_HISTORY"
        for reference in reference_list
    )
    report: Dict[str, Any] = {
        "signature_version": MIXED_SIGNATURE_VERSION,
        "nearest_script_id": "",
        "difference_dimensions": [],
        "difference_summary": "本批暂无其它已生成镜头可比对",
        "review_status": MIXED_VERDICT_DISTINCT_THEME,
        "counts_as_independent": True,
        # 没有可比参照时，这三项一律为 False：批次报告要能把"没比对"与
        # "比对后确认是独立主题"分开，否则"没有历史"会被读成"全是新主题"。
        "counts_as_theme": False,
        "counts_as_variant": False,
        "order_only": False,
        "comparison_scope": "RENDERED_ONLY",
        "references_compared": len(reference_list),
        "references_skipped_not_rendered": skipped_not_rendered,
        "history_rendered_compared": sum(
            1
            for reference in reference_list
            if _text(reference.get("source")) == "RENDERED_HISTORY"
        ),
    }
    best: Dict[str, Any] = {}
    for reference in reference_list:
        outcome = compare_mixed_signatures(signature, reference.get("signature") or {})
        if not outcome:
            continue
        outcome["identity"] = _text(reference.get("identity"))
        if (
            not best
            or _MIXED_VERDICT_SEVERITY.get(outcome["review_status"], 0)
            > _MIXED_VERDICT_SEVERITY.get(best["review_status"], 0)
        ):
            best = outcome
    if best:
        report.update(
            {
                "nearest_script_id": best.get("identity", ""),
                "difference_dimensions": list(best.get("difference_dimensions") or []),
                "difference_summary": _text(best.get("difference_summary")),
                "review_status": best["review_status"],
                "counts_as_independent": bool(best.get("counts_as_independent")),
                # Which axis produced the verdict.  Without these a reviewer
                # cannot tell "the shots really differ" from "only the light
                # differed", and the two call for opposite actions.
                "visual_identical": bool(best.get("visual_identical")),
                "surface_identical": bool(best.get("surface_identical")),
                "semantic_identical": bool(best.get("semantic_identical")),
                # B2 的两个计数轴随成稿一起落盘：批次报告按唯一 script_id
                # 汇总时只能读这里，不能从"生成成功"或"模板排列"推。
                "counts_as_theme": bool(best.get("counts_as_theme")),
                "counts_as_variant": bool(best.get("counts_as_variant")),
                "order_only": bool(best.get("order_only")),
                "new_observation_points": list(best.get("new_observation_points") or []),
                "protected_content_differ": list(best.get("protected_content_differ") or []),
                "comparison_scope": (
                    "RENDERED_BATCH_AND_HISTORY"
                    if from_history
                    else "RENDERED_BATCH"
                ),
            }
        )

    # The generated montage still has to *be* a montage.  Four shots whose
    # visible contents are identical are one shot delivered four times, whatever
    # the frozen requirements said.
    #
    # 读的是这一镜**实际交付的画面描述**。B1 把每镜特征换成
    # ``rendered_visual_content`` 之后，旧键 ``rendered_event`` 已不存在 ——
    # 继续读旧键会让 ``events`` 恒为全空，"画面重复"守卫变成恒真，把每一份
    # 成稿都拦成待复核（离线夹具里就是这样被发现的）。空文本也不算"重复"：
    # 那是缺内容，由别的检查报告。
    events = [
        _signature_atom(shot.get("rendered_visual_content"))
        for shot in (signature.get("visual") or {}).get("shots") or []
    ]
    distinct_events = {event for event in events if event}
    report["rendered_shot_count"] = len(events)
    report["distinct_rendered_events"] = len(distinct_events)
    report["montage_collapsed"] = bool(distinct_events) and len(distinct_events) <= 1
    if report["montage_collapsed"]:
        report["review_status"] = MIXED_VERDICT_NEEDS_REVIEW
        report["counts_as_independent"] = False
        report["difference_dimensions"] = [
            *report["difference_dimensions"],
            "RENDERED_SHOTS_COLLAPSED",
        ]
        report["difference_summary"] = "各镜实际画面内容相同，未形成混合展示"
    return report


def final_shot_reference(
    identity: str,
    contract: Mapping[str, Any] | None,
) -> Dict[str, Any]:
    """A reference a later batch can compare against, from a frozen contract."""

    return {
        "identity": _text(identity),
        "signature": mixed_signature_bundle(contract),
    }


# ---------------------------------------------------------------------------
# 每镜执行对象（A 包：让最终渲染服从混合模块自己的边界）
# ---------------------------------------------------------------------------
# R1 的根因不是某个词，而是一条**按位置索引**的旧角色弧：
# ``accessory.py:_deduplicate_framing`` / ``renderer:_apply_small_accessory_*``
# 把 ``unit_role`` 按 0/1/2/last 赋成
# ``PRODUCT_RESULT_CLOSE -> NATURAL_MOTION_RELATION -> PRODUCT_DETAIL_RELATION
# -> PRODUCT_REACQUISITION``，再由 ``_capture_unit_passages`` 用它取出
# ``core_action`` / ``movement_guidance`` / ``ending_guidance``。
# 混合模板的四镜是 **模块**（佩戴近景 / 手持 / 静物 / 佩戴关系），与旧弧毫无
# 关系，于是手持镜与静物镜被套上"人物在颈肩关系内改变重心""录制上半身"这类
# 佩戴动作，并且**违反本镜自己的 forbidden_framing**。
#
# 下面这组纯函数把"每镜该执行什么"收敛成单一出口。判据只有两个来源：
#   1. 冻结合同（硬边界：时间 / 模块 / 载体 / 不露脸范围 / 允许部位 / 物理状态）
#   2. 已通过审查的成稿（创意内容：本镜观察什么、实际做什么、怎么运镜）
# 旧 ``unit_role`` 只作为 ``legacy_role`` 保留作审计，**不再授权身体动作**。

SHOT_EXECUTION_VERSION = "mixed-shot-execution-v1"
EXECUTION_AUDIT_VERSION = "mixed-execution-audit-v1"

ERR_MIXED_EXECUTION_MODULE_UNKNOWN = "MIXED_EXECUTION_MODULE_UNKNOWN"
ERR_MIXED_EXECUTION_TIMELINE = "MIXED_EXECUTION_TIMELINE"
ERR_MIXED_EXECUTION_ACTION_BOUNDARY = "MIXED_EXECUTION_ACTION_BOUNDARY"
ERR_MIXED_EXECUTION_CAMERA_BOUNDARY = "MIXED_EXECUTION_CAMERA_BOUNDARY"
ERR_MIXED_EXECUTION_ROLE_MISMATCH = "MIXED_EXECUTION_ROLE_MISMATCH"
ERR_MIXED_EXECUTION_ANCHOR_MISSING = "MIXED_EXECUTION_ANCHOR_MISSING"

WORN_MODULES: Tuple[str, ...] = ("WORN_DETAIL", "WORN_RELATION")
NON_BODY_MODULES: Tuple[str, ...] = ("HANDHELD_PRODUCT", "STATIC_PRODUCT")
MIXED_MODULES: Tuple[str, ...] = (
    "WORN_DETAIL",
    "HANDHELD_PRODUCT",
    "STATIC_PRODUCT",
    "WORN_RELATION",
)

# 提示词里"商品执行关系"那一行在混合模式下显示什么。旧 ``unit_role`` 是内部
# 叙事标签，对拍摄者没有意义；模块 + 载体才是本镜真正的执行关系。
_MODULE_EXECUTION_ROLE = {
    "WORN_DETAIL": "佩戴局部近景（真人局部佩戴）",
    "HANDHELD_PRODUCT": "手持商品（纯手部承载）",
    "STATIC_PRODUCT": "商品静物（静物承载）",
    "WORN_RELATION": "佩戴关系（真人局部佩戴）",
}

# 模块边界词表。来源是与 ``module_framing_rules`` 已经写死的
# ``forbidden_framing`` 同一套语义：手持/静物镜不得出现人物与佩戴部位，静物镜
# 连手都不许有。这里刻意用固定词表而不是解析 forbidden 文案——forbidden 是给
# 人读的句子，拆词比列词更脆。
# 刻意**不含**"创作者 / 她 / 模特"这类泛称：它们出现在
# "保持普通创作者展示随身物件的自然节奏" 这类风格句里，并不要求人物入画。
# R1 的旧动作全部由身体部位与镜面词命中（颈肩 / 肩部 / 头部 / 上半身 /
# 重心 / 侧移 / 转头 / 人物 / 佩戴部位），所以去掉泛称不会削弱检出，只会少
# 一类误报。
_BODY_PRESENCE_TERMS: Tuple[str, ...] = (
    "人物", "上半身", "肩部", "肩膀", "肩颈", "颈肩", "颈部",
    "头部", "转头", "侧脸", "半脸", "正脸", "全脸", "脸", "眼部", "眼", "鼻",
    "嘴", "面部", "头肩", "镜中", "镜面", "重心", "侧移", "站姿", "站定",
    "坐下", "坐着", "走动", "走出", "走向", "佩戴部位", "全身", "穿搭展示",
)
_HAND_PRESENCE_TERMS: Tuple[str, ...] = (
    "手", "手指", "手臂", "手腕", "承托", "举起", "握", "拿", "掌心",
)
_STATIC_MOTION_TERMS: Tuple[str, ...] = (
    "转动", "移动", "翻转", "摇晃", "位移", "改变角度",
)

# NO_FACE 合同下任何一镜都不得**正向**要求出现脸/镜面。旧角色弧的
# "先观察镜中或侧后方的发饰位置"正是靠这条被抓住。
_NO_FACE_POSITIVE_BAN_TERMS: Tuple[str, ...] = (
    "镜中", "镜面", "侧脸", "半脸", "正脸", "全脸", "头肩", "自拍",
    "眼部", "眼", "鼻", "嘴", "面部",
)

# 否定/禁止措辞：命中即视为"禁止说明"而不是"正向动作"。
# 这是方案要求的判据 —— 不能因为出现"头部"就判定违规，
# "不出现头部""无人物入画"必须被认成禁令。
_PROHIBITION_MARKERS: Tuple[str, ...] = (
    "不得", "不能", "不可", "禁止", "不要", "不许", "勿", "避免",
    "不", "无", "没有", "未", "非", "严禁", "不出现", "不入画",
)

_CLAUSE_SPLIT_RE = re.compile(r"[；;。，,、\n]")
# 中文分句。"，"、"、" 是句内枚举，；。\n 才是句界——这个区别决定了禁令能否
# 正确覆盖它后面枚举出来的整串条目。
_CLAUSE_TOKEN_RE = re.compile(r"([^；;。，,、\n]*)([；;。，,、\n]?)")
_SENTENCE_BOUNDARIES = "；;。\n"

# 显式禁令引导词。**刻意不含单独的"不"**：它是 `_clause_is_prohibitive` 的
# 判据之一，但不足以把后续枚举一起染成禁令
# （"人物不入画，商品全程静置"的第二句是正向要求）。
_BAN_LEAD_INS: Tuple[str, ...] = (
    "不得出现", "不得再", "不得", "禁止", "不要", "严禁", "避免", "不出现", "不入画",
)


def _is_no_face(face_policy: Any) -> bool:
    return _text(face_policy).upper() == NO_FACE_POLICY


def _clause_is_prohibitive(clause: str) -> bool:
    """True when a clause states a ban rather than a positive instruction."""

    return any(marker in clause for marker in _PROHIBITION_MARKERS)


def execution_clauses(text: Any) -> List[Dict[str, Any]]:
    """Split one action/framing sentence into polarity-tagged clauses.

    Two things are deliberate here:

    * ``手机`` is stripped before term matching — it contains ``手``, and a phone
      reposition is not a hand instruction.
    * A ban *enumerates*: ``不得出现：脸与头部入画、正面全脸、镜面反射露脸`` is
      one prohibition with three items, not one prohibition followed by two
      requirements.  A per-clause keyword test would read the second and third
      items as instructions and report the prompt's own ban list as a
      violation, so an enumeration lead-in carries its polarity across the
      ``、``/``，`` list until the next ``；``/``。``/newline.
    """

    out: List[Dict[str, Any]] = []
    enumerated = False
    for clause, separator in _CLAUSE_TOKEN_RE.findall(_text(text)):
        clause = clause.strip()
        if not clause:
            if separator in _SENTENCE_BOUNDARIES:
                enumerated = False
            continue
        prohibitive = _clause_is_prohibitive(clause) or enumerated
        out.append(
            {
                "clause": clause,
                "probe": clause.replace("手机", ""),
                "prohibitive": prohibitive,
            }
        )
        if separator in _SENTENCE_BOUNDARIES:
            enumerated = False
        elif any(lead in clause for lead in _BAN_LEAD_INS):
            enumerated = True
    return out


def _module_boundary_terms(module: str) -> Tuple[str, ...]:
    module = _text(module).upper()
    if module == "HANDHELD_PRODUCT":
        return _BODY_PRESENCE_TERMS
    if module == "STATIC_PRODUCT":
        return (*_BODY_PRESENCE_TERMS, *_HAND_PRESENCE_TERMS, *_STATIC_MOTION_TERMS)
    return ()


def module_action_violations(
    module: str,
    text: Any,
    *,
    face_policy: str = "",
) -> List[Dict[str, Any]]:
    """Positive clauses that cross this module's own boundary.

    Order matters: polarity first, then terms.  A ban is not an instruction, and
    the single most common false positive for this check is treating
    "人物不入画" as "the person is in frame".
    """

    module = _text(module).upper()
    terms = list(_module_boundary_terms(module))
    if _is_no_face(face_policy):
        terms.extend(_NO_FACE_POSITIVE_BAN_TERMS)
    if not terms:
        return []

    out: List[Dict[str, Any]] = []
    for item in execution_clauses(text):
        if item["prohibitive"]:
            continue
        hits = [term for term in terms if term in item["probe"]]
        if not hits:
            continue
        out.append(
            {
                "module": module,
                "clause": item["clause"],
                "terms": hits,
                "code": ERR_MIXED_EXECUTION_ACTION_BOUNDARY,
            }
        )
    return out


def mixed_module_execution_role(module: str) -> str:
    module = _text(module).upper()
    return _MODULE_EXECUTION_ROLE.get(module, "") or _MODULE_LABELS.get(module, module)


def mixed_shot_camera_line(unit: Mapping[str, Any] | None) -> str:
    """本段手机构图，逐字取自该镜自己冻结的取景范围。

    Derived rather than authored so the two non-body modules cannot inherit the
    worn arc's "上半身 / 重新放置手机" wording: whatever the frozen unit allows
    is what the shot gets, and whatever it bans is stated as a ban.
    """

    data = unit if isinstance(unit, Mapping) else {}
    module = _text(data.get("module")).upper()
    if not module:
        return ""
    label = (
        _text(data.get("view_label"))
        or _MODULE_LABELS.get(module)
        or module
    )
    allowed = [_text(item) for item in (data.get("allowed_framing") or []) if _text(item)]
    forbidden = [
        _text(item) for item in (data.get("forbidden_framing") or []) if _text(item)
    ]
    parts: List[str] = []
    if allowed:
        parts.append(f"{label}取景只保留：{'、'.join(allowed)}")
    else:
        parts.append(f"{label}取景")
    if forbidden:
        parts.append("不得出现：" + "、".join(forbidden))
    return "；".join(parts)


def _execution_environment(contract: Mapping[str, Any]) -> Dict[str, Any]:
    """单片冻结光影配方：镜头可换角度，不得换另一套配方。"""

    recipe = contract.get("environment_recipe")
    recipe = recipe if isinstance(recipe, Mapping) else {}
    return {
        "environment_recipe_id": _text(contract.get("environment_recipe_id")),
        "environment_recipe_version": int(
            contract.get("environment_recipe_version") or 0
        ),
        "label": _text(recipe.get("label") or recipe.get("display_name")),
        "light_direction": _text(recipe.get("light_direction")),
        "white_balance": _text(recipe.get("white_balance")),
        "surface": _text(recipe.get("surface") or recipe.get("table_surface")),
        "background": _text(recipe.get("background")),
    }


def _execution_product_identity(contract: Mapping[str, Any]) -> Dict[str, Any]:
    """全片一致的商品身份与已知结构；未知部件不补造。"""

    facts = contract.get("structure_facts")
    facts = facts if isinstance(facts, Mapping) else {}
    return {
        "product_code": _text(contract.get("product_code")),
        "canonical_type": _text(contract.get("canonical_type") or contract.get("product_type")),
        "zone": _text(contract.get("zone")),
        "visible_quantity_rule": _text(contract.get("visible_quantity_rule")),
        "structure_facts": {
            _text(key): _text(state).upper()
            for key, state in facts.items()
            if _text(key)
        },
    }


def mixed_shot_execution_object(
    unit: Mapping[str, Any] | None,
    shot: Mapping[str, Any] | None = None,
    contract: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    """The single per-shot execution object the final renderer must obey.

    Hard boundary (time / module / carrier / face range / allowed body parts /
    physical state) comes from the frozen unit.  Creative content (what this
    shot observes, what it actually does, how it is framed) comes from the
    approved storyboard.  When the approved action crosses the module boundary
    the frozen requirement replaces it and the substitution is recorded, so the
    failure is visible instead of silently shipped.
    """

    frozen = unit if isinstance(unit, Mapping) else {}
    approved = shot if isinstance(shot, Mapping) else {}
    base = contract if isinstance(contract, Mapping) else {}
    module = _text(frozen.get("module")).upper()
    face_policy = _text(frozen.get("face_policy")).upper()
    issues: List[Dict[str, Any]] = []

    if not module:
        issues.append(
            {
                "code": ERR_MIXED_EXECUTION_MODULE_UNKNOWN,
                "field": "module",
                "actual": "",
                "detail": "冻结合同的该镜没有模块，无法确定执行边界",
            }
        )

    approved_action = _text(approved.get("character_action"))
    required_action = _text(frozen.get("action"))
    violations = (
        module_action_violations(module, approved_action, face_policy=face_policy)
        if approved_action
        else []
    )
    if approved_action and not violations:
        action, action_source = approved_action, "APPROVED_SCRIPT"
    elif required_action:
        action, action_source = required_action, "MODULE_REQUIREMENT"
        if approved_action:
            action_source = "MODULE_REQUIREMENT_REPLACED_APPROVED"
    else:
        action, action_source = approved_action, "APPROVED_SCRIPT"
    if violations and action_source != "APPROVED_SCRIPT":
        issues.append(
            {
                "code": ERR_MIXED_EXECUTION_ACTION_BOUNDARY,
                "field": "character_action",
                "module": module,
                "actual": approved_action,
                "detail": "成稿动作越出本镜模块边界，已替换为该镜冻结要求",
                "clauses": violations,
            }
        )

    approved_visual = _text(approved.get("visual_content"))
    camera_line = mixed_shot_camera_line(frozen)
    for clause in execution_clauses(approved_visual):
        if clause["prohibitive"]:
            continue
        hits = [
            term
            for term in _module_boundary_terms(module)
            if term in clause["probe"]
        ]
        if hits:
            issues.append(
                {
                    "code": ERR_MIXED_EXECUTION_CAMERA_BOUNDARY,
                    "field": "visual_content",
                    "module": module,
                    "actual": clause["clause"],
                    "terms": hits,
                    "detail": "成稿画面事件越出本镜模块取景范围",
                }
            )

    # 视线与自然反响：只用成稿自己的，拿不到就留空。旧角色弧的
    # "先观察镜中或侧后方的发饰位置" 在 NO_FACE 下会直接违反本镜禁令。
    gaze = _text(approved.get("gaze_target"))
    reaction = _text(
        approved.get("natural_emotion") or approved.get("natural_reaction")
    )
    for field, value in (("gaze_target", gaze), ("natural_reaction", reaction)):
        if not value:
            continue
        hits = module_action_violations(module, value, face_policy=face_policy)
        if hits:
            issues.append(
                {
                    "code": ERR_MIXED_EXECUTION_ACTION_BOUNDARY,
                    "field": field,
                    "module": module,
                    "actual": value,
                    "detail": "成稿自由文本越出本镜模块边界，已丢弃",
                    "clauses": hits,
                }
            )
            if field == "gaze_target":
                gaze = ""
            else:
                reaction = ""

    return {
        "execution_version": SHOT_EXECUTION_VERSION,
        "shot_id": _text(frozen.get("unit_id") or approved.get("capture_unit_id")),
        "time_range": _text(approved.get("time_range")),
        "module": module,
        "module_label": _text(frozen.get("view_label"))
        or _MODULE_LABELS.get(module)
        or module,
        "execution_role": mixed_module_execution_role(module),
        # Audit only.  The old role arc keeps its narrative label but owns no
        # body action any more.
        "legacy_role": _text(frozen.get("unit_role")).upper(),
        "carrier_mode": _text(frozen.get("carrier_mode")).upper(),
        "face_policy": face_policy,
        "body_zone": _text(frozen.get("body_zone")),
        "view_scope": _text(frozen.get("view_scope")),
        "allowed_framing": [
            _text(item) for item in (frozen.get("allowed_framing") or []) if _text(item)
        ],
        "forbidden_framing": [
            _text(item) for item in (frozen.get("forbidden_framing") or []) if _text(item)
        ],
        "action_boundary": [
            _text(item) for item in (frozen.get("action_boundary") or []) if _text(item)
        ],
        "state_boundary": _text(frozen.get("state_boundary")),
        "product_state": _text(frozen.get("product_state")).upper(),
        "quantity_rule": _text(
            frozen.get("visible_quantity_rule") or frozen.get("quantity_rule")
        ),
        "observation_job": _text(frozen.get("observation_job")),
        "continuity_group": _text(frozen.get("continuity_group")),
        "action": action,
        "action_source": action_source,
        "camera_guidance": camera_line,
        "gaze_target": gaze,
        "micro_reaction": reaction,
        "product_identity": _execution_product_identity({**base, **frozen}),
        "environment": _execution_environment(base),
        "issues": issues,
    }


def mixed_execution_objects(
    contract: Mapping[str, Any] | None,
    storyboard: Any = None,
) -> List[Dict[str, Any]]:
    """One execution object per frozen shot, matched to the approved shot."""

    data = contract if isinstance(contract, Mapping) else {}
    units = [
        unit for unit in (data.get("capture_units") or []) if isinstance(unit, Mapping)
    ]
    by_id: Dict[str, Mapping[str, Any]] = {}
    for shot in storyboard or []:
        if not isinstance(shot, Mapping):
            continue
        key = _text(shot.get("capture_unit_id"))
        if key and key not in by_id:
            by_id[key] = shot
    out: List[Dict[str, Any]] = []
    for index, unit in enumerate(units, start=1):
        shot = by_id.get(_text(unit.get("unit_id")))
        obj = mixed_shot_execution_object(unit, shot, data)
        obj["position"] = index
        if not obj["time_range"]:
            span = (frozen_unit_timeline(data)[0] or {}).get(_text(unit.get("unit_id")))
            obj["time_range"] = format_mixed_shot_time_range(span)
        out.append(obj)
    return out


# ---------------------------------------------------------------------------
# 最终执行一致性检查（A 包：跑在拼接与压缩之后）
# ---------------------------------------------------------------------------
# 只检查冻结合同是不够的：上一轮的冲突正是发生在合同检查**之后**的最后一步
# 投影。所以这一节读的是**最终提示词文本**，逐镜回比冻结合同与成稿。

_FINAL_SHOT_HEADER_RE = re.compile(
    r"^【(?:拍摄片段|连续内容段|片段)(?P<index>\d{2})｜(?P<time>[^｜]+)｜(?P<role>[^】]*)】$"
)
_FINAL_SHOT_FIELDS = (
    "画面事件",
    "人物动作",
    "商品执行关系",
    "视线关系",
    "自然反应",
    "本段手机构图",
    "手机机位",
    "每段商品必须可见",
    "商品必须可见",
)


def parse_final_shot_blocks(prompt: Any) -> List[Dict[str, Any]]:
    """Read back the per-shot blocks actually delivered to the video model."""

    blocks: List[Dict[str, Any]] = []
    current: Optional[Dict[str, Any]] = None
    for raw in _text(prompt).splitlines():
        line = raw.strip()
        if not line:
            continue
        header = _FINAL_SHOT_HEADER_RE.match(line)
        if header:
            current = {
                "index": int(header.group("index")),
                "time_range": header.group("time").strip(),
                "header_role": header.group("role").strip(),
                "fields": {},
                "lines": [],
            }
            blocks.append(current)
            continue
        if current is None:
            continue
        if line.startswith("【") and line.endswith("】"):
            # Any other section header closes the shot run.
            current = None
            continue
        current["lines"].append(line)
        for label in _FINAL_SHOT_FIELDS:
            prefix = label + "："
            if line.startswith(prefix):
                current["fields"].setdefault(label, line[len(prefix):].strip())
                break
    return blocks


def _prompt_hash(prompt: Any) -> str:
    return hashlib.sha1(_text(prompt).encode("utf-8")).hexdigest()[:20]


def audit_mixed_final_execution(
    prompt: Any,
    contract: Mapping[str, Any] | None,
    *,
    storyboard: Any = None,
    renderer_version: str = "",
    pre_compaction_prompt: Any = None,
) -> Dict[str, Any]:
    """Structural audit of the *final* prompt against the frozen mixed contract.

    Runs after splicing and compaction, because that is where R1 lived.  Every
    issue names the shot, the field, the module boundary it crossed, the actual
    delivered value, and which stage introduced it.

    ``status`` is ``PASS`` / ``FAIL`` / ``NOT_APPLICABLE``.  A ``FAIL`` means the
    prompt asks the video model for two contradictory things at once and must
    not be submitted.
    """

    data = contract if isinstance(contract, Mapping) else {}
    if _text(data.get("execution_profile")) != ACCESSORY_MIXED_TEMPLATE_PROFILE:
        return {
            "version": EXECUTION_AUDIT_VERSION,
            "status": "NOT_APPLICABLE",
            "issues": [],
            "issue_count": 0,
            "checked_shots": 0,
            "block_count": 0,
            "anchored_blocks": 0,
            "prompt_hash": _prompt_hash(prompt),
            "renderer_version": renderer_version,
            "reason": "NOT_MIXED_MODE",
        }

    objects = mixed_execution_objects(data, storyboard)
    if not objects:
        return {
            "version": EXECUTION_AUDIT_VERSION,
            "status": "NOT_APPLICABLE",
            "issues": [],
            "issue_count": 0,
            "checked_shots": 0,
            "block_count": 0,
            "anchored_blocks": 0,
            "prompt_hash": _prompt_hash(prompt),
            "renderer_version": renderer_version,
            "reason": "NO_FROZEN_SHOTS",
        }

    blocks = parse_final_shot_blocks(prompt)
    storyboard_text = json.dumps(storyboard or [], ensure_ascii=False, default=str)
    pre_text = _text(pre_compaction_prompt)

    def _stage_of(actual: str, field: str) -> str:
        needle = _text(actual)
        if needle and needle in storyboard_text:
            return "GENERATION"
        if needle and pre_text and needle in pre_text and needle not in _text(prompt):
            return "COMPACTION"
        return "RENDERER"

    issues: List[Dict[str, Any]] = []
    anchored_blocks = 0
    for position, obj in enumerate(objects, start=1):
        shot_id = obj.get("shot_id") or f"CU_{position:02d}"
        block = blocks[position - 1] if position - 1 < len(blocks) else {}
        fields = block.get("fields") or {}
        if not block:
            issues.append(
                {
                    "shot_id": shot_id,
                    "field": "<block>",
                    "module": obj.get("module", ""),
                    "code": ERR_MIXED_EXECUTION_TIMELINE,
                    "actual": "",
                    "detail": "最终提示词里找不到该镜的片段块",
                    "source": "RENDERER",
                }
            )
            continue
        expected_time = _text(obj.get("time_range"))
        actual_time = _text(block.get("time_range"))
        if expected_time and actual_time and expected_time != actual_time:
            issues.append(
                {
                    "shot_id": shot_id,
                    "field": "time_range",
                    "module": obj.get("module", ""),
                    "code": ERR_MIXED_EXECUTION_TIMELINE,
                    "expected": expected_time,
                    "actual": actual_time,
                    "detail": "片段表头时间轴与冻结合同不一致",
                    "source": "RENDERER",
                }
            )
        for label in ("画面事件", "人物动作", "本段手机构图", "手机机位",
                      "视线关系", "自然反应"):
            value = _text(fields.get(label))
            if not value:
                continue
            for hit in module_action_violations(
                _text(obj.get("module")),
                value,
                face_policy=_text(obj.get("face_policy")),
            ):
                issues.append(
                    {
                        "shot_id": shot_id,
                        "field": label,
                        "module": obj.get("module", ""),
                        "code": ERR_MIXED_EXECUTION_ACTION_BOUNDARY,
                        "expected": _text(obj.get("camera_guidance")),
                        "actual": hit["clause"],
                        "terms": hit["terms"],
                        "detail": f"{label}正向要求越出本镜模块边界",
                        "source": _stage_of(value, label),
                    }
                )
        role_line = _text(fields.get("商品执行关系"))
        if role_line and obj.get("module"):
            expected_role = _text(obj.get("execution_role"))
            if role_line not in expected_role:
                issues.append(
                    {
                        "shot_id": shot_id,
                        "field": "商品执行关系",
                        "module": obj.get("module", ""),
                        "code": ERR_MIXED_EXECUTION_ROLE_MISMATCH,
                        "expected": expected_role,
                        "actual": role_line,
                        "detail": "该镜仍写着旧的按位置索引角色，不是本镜模块",
                        "source": "RENDERER",
                    }
                )
        anchors = [
            _text(item)
            for item in (block.get("lines") or [])
            if item.startswith("每段商品必须可见：")
            or item.startswith("商品必须可见：")
        ]
        if anchors:
            anchored_blocks += 1

    # 锚点可以（也应该）被提升成全片一行：四镜声明同一个锚点时，压缩会把整行
    # 留在首镜并只保留一份（I6）。所以这里判的是"全片是否还有商品可见锚点"，
    # 不是"每一块都有一行"——后者会把正确的提升判成丢失。
    if objects and anchored_blocks == 0:
        issues.append(
            {
                "shot_id": "<all>",
                "field": "商品必须可见",
                "module": "",
                "code": ERR_MIXED_EXECUTION_ANCHOR_MISSING,
                "actual": "",
                "detail": "最终提示词里没有任何商品可见锚点",
                "source": "COMPACTION",
            }
        )

    return {
        "version": EXECUTION_AUDIT_VERSION,
        "status": "FAIL" if issues else "PASS",
        "issues": issues,
        "issue_count": len(issues),
        "checked_shots": len(objects),
        "block_count": len(blocks),
        "anchored_blocks": anchored_blocks,
        "prompt_hash": _prompt_hash(prompt),
        "renderer_version": renderer_version,
        "execution_version": SHOT_EXECUTION_VERSION,
    }
