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
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

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

    The structure registry wins when it has an opinion; otherwise the approved
    anchors are scanned.  A negation such as "无吊坠" contains the positive
    term as a substring, so negatives are scanned first.
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

    blob = "；".join(_text(item) for item in (anchor_texts or []) if _text(item))
    if not blob:
        return {"part_key": key, "state": EVIDENCE_UNKNOWN, "source": "NO_EVIDENCE"}

    terms = _part_terminology(key)
    negatives = [_text(item) for item in (terms.get("negative_terms") or []) if _text(item)]
    positives = [_text(item) for item in (terms.get("positive_terms") or []) if _text(item)]
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
    return {
        "theme_id": _text(theme.get("theme_id")),
        "parent_theme_id": _text(theme.get("parent_theme_id")) or _text(theme.get("theme_id")),
        "candidate_role": (_text(theme.get("candidate_role")) or "PRIMARY").upper(),
        "thesis": _text(theme.get("thesis")),
        "approved_claim_refs": [
            _text(item) for item in (theme.get("approved_claim_refs") or []) if _text(item)
        ],
        "evidence_refs": [
            _text(item) for item in (theme.get("evidence_refs") or []) if _text(item)
        ],
    }


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
    if isinstance(storyboard, list):
        for shot in storyboard:
            if not isinstance(shot, dict):
                continue
            source = by_id.get(_unit_id_of(shot))
            if source is None:
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


def mixed_semantic_signature(contract: Mapping[str, Any] | None) -> Dict[str, Any]:
    """主题命题 / 观众问题 / 核心证据所对应的具体意义。

    The *digest* deliberately excludes the thesis prose.  ``theme_id`` rotates
    per item and a thesis can be paraphrased without changing what the viewer
    learns, so neither may create difference on its own -- that is exactly the
    hole Review #3 found.  Only the claim/evidence the shots are allowed to
    assert, plus the observation vocabulary the product can actually show,
    counts as meaning.  The prose is still recorded for the report.
    """

    data = contract if isinstance(contract, Mapping) else {}
    theme = data.get("content_theme") if isinstance(data.get("content_theme"), Mapping) else {}
    meaning = {
        "claims": _signature_atoms(theme.get("approved_claim_refs")),
        "evidence": _signature_atoms(theme.get("evidence_refs")),
        "observations": _signature_atoms(data.get("observation_focus")),
        "structure": {
            _text(key): _text(state).upper()
            for key, state in (data.get("structure_facts") or {}).items()
            if _text(key)
        },
    }
    return {
        "proposition": _text(theme.get("thesis")),
        "audience_question": _text(theme.get("thesis")),
        "theme_label": _text(theme.get("theme_id")),
        "claims": meaning["claims"],
        "evidence": meaning["evidence"],
        "observations": meaning["observations"],
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
    return {
        "template_id": _text(data.get("template_id")),
        "face_policy": _text(data.get("face_policy")).upper(),
        "verified_parts": verified_parts,
        "shots": shots,
        "digest": _signature_digest(
            {
                "template_id": _text(data.get("template_id")),
                "face_policy": _text(data.get("face_policy")).upper(),
                "verified_parts": verified_parts,
                "shots": shots,
            }
        ),
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
    }


def mixed_reference_signature(record: Mapping[str, Any] | None) -> Dict[str, Any]:
    """A comparable reference built from one historical usage record.

    ``complete`` is False when the record predates these signatures.  A missing
    final-shot set is *not* reconstructed from the legacy scene signature: a
    fabricated reference would silently mark real repeats as new content, which
    is the failure this whole mechanism exists to prevent.
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
    bundle = metadata.get(MIXED_HISTORY_METADATA_KEY)
    if not isinstance(bundle, Mapping) or not bundle:
        return {
            "identity": identity,
            "complete": False,
            "reason": "HISTORY_INCOMPLETE",
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
            "reason": "HISTORY_INCOMPLETE",
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

    same_visual = (
        _text(cand_visual.get("digest")) == _text(ref_visual.get("digest"))
        and bool(_text(cand_visual.get("digest")))
    )
    same_surface = _text((cand.get("surface") or {}).get("digest")) == _text(
        (ref.get("surface") or {}).get("digest")
    )
    same_semantic = (
        _text((cand.get("semantic") or {}).get("digest"))
        == _text((ref.get("semantic") or {}).get("digest"))
        and bool(_text((cand.get("semantic") or {}).get("digest")))
    )

    dimensions: List[str] = []
    if same_visual and same_surface:
        verdict = MIXED_VERDICT_EXACT_DUPLICATE
        dimensions = ["FINAL_SHOTS_IDENTICAL", "SURFACE_IDENTICAL"]
        summary = "四镜最终约束与台面光线完全一致，仅序号/旧场景签名不同"
    elif same_visual:
        verdict = MIXED_VERDICT_SURFACE_ONLY
        dimensions = ["FINAL_SHOTS_IDENTICAL", "SURFACE_DIFFERENT"]
        summary = "最终镜头约束相同，只有环境/光线等辅助变化"
    else:
        new_jobs = _new_observation_jobs(cand_visual, ref_visual)
        dimensions = ["FINAL_SHOTS_DIFFERENT"]
        if new_jobs:
            dimensions.append("NEW_OBSERVATION_POINT")
        else:
            # The same evidenced observations, arranged into a different
            # montage.  The three authored templates exist precisely for this,
            # so it is a legitimate execution variant -- not a duplicate.
            dimensions.append("SHOT_ORDER_DIFFERS")
        if same_semantic:
            dimensions.append("SEMANTIC_IDENTICAL")
        else:
            dimensions.append("SEMANTIC_DIFFERENT")
        authored = authored_observation_jobs()
        unsupported = sorted(job for job in new_jobs if job not in authored)
        if unsupported:
            verdict = MIXED_VERDICT_NEEDS_REVIEW
            dimensions.append("UNVERIFIED_NEW_OBSERVATION")
            summary = "新增观察点缺少可核查证据：" + "、".join(unsupported[:3])
        elif same_semantic and new_jobs:
            verdict = MIXED_VERDICT_EXECUTION_VARIANT
            summary = "同主题下增加有证据支撑的新观察重点"
        elif same_semantic:
            verdict = MIXED_VERDICT_EXECUTION_VARIANT
            summary = "同主题，相同观察重点以不同镜头顺序执行"
        else:
            verdict = MIXED_VERDICT_DISTINCT_THEME
            summary = "主题问题有实质变化，并安排了相应可见镜头"

    return {
        "review_status": verdict,
        "difference_dimensions": dimensions,
        "difference_summary": summary,
        # Only a demonstrated, evidenced difference may be counted as
        # independent content.  NEEDS_REVIEW is included on purpose: claiming a
        # dedup pass we could not verify is the same defect one layer down.
        "counts_as_independent": verdict not in MIXED_BLOCKING_VERDICTS,
        "semantic_identical": same_semantic,
        "visual_identical": same_visual,
        "surface_identical": same_surface,
    }


def _new_observation_jobs(
    candidate_visual: Mapping[str, Any],
    reference_visual: Mapping[str, Any],
) -> List[str]:
    def _jobs(visual: Mapping[str, Any]) -> List[str]:
        return [
            _text(shot.get("observation_job"))
            for shot in (visual.get("shots") or [])
            if isinstance(shot, Mapping) and _text(shot.get("observation_job"))
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
        }
    )
    return report


def mixed_signature_from_script(
    script: Mapping[str, Any] | None,
    contract: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    """Final-shot signature read back from a generated script.

    This is the second check the plan asks for: once the 正文 exists, the shots
    the model actually wrote are compared again, so a candidate that looked
    distinct while planned cannot stay accepted if the model collapsed it into
    the same shots as something already delivered.

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
    projected: List[Dict[str, Any]] = []
    for position, (unit, shot) in enumerate(zip(units, storyboard), start=1):
        entry = _mixed_unit_visual(unit)
        entry["is_opening"] = position == 1
        entry["opening_job"] = entry["observation_job"] if position == 1 else ""
        # The generated shot's own visible event is what the viewer receives;
        # the frozen job is the requirement it was asked to satisfy.
        entry["rendered_event"] = _signature_atom(shot.get("visual_content"))
        projected.append(entry)
    return {
        "signature_version": MIXED_SIGNATURE_VERSION,
        "visual": {
            "template_id": _text(base.get("template_id")),
            "face_policy": _text(base.get("face_policy")).upper(),
            "verified_parts": _mixed_verified_parts(base),
            "shots": projected,
            "digest": _signature_digest(
                {
                    "template_id": _text(base.get("template_id")),
                    "face_policy": _text(base.get("face_policy")).upper(),
                    "verified_parts": _mixed_verified_parts(base),
                    "shots": projected,
                }
            ),
        },
        "semantic": mixed_semantic_signature(base),
        "surface": mixed_surface_signature(base),
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
    """

    signature = mixed_signature_from_script(script, contract)
    if not signature:
        return {}
    reference_list = [
        reference
        for reference in (references or [])
        if isinstance(reference, Mapping) and reference.get("signature")
    ]
    report: Dict[str, Any] = {
        "signature_version": MIXED_SIGNATURE_VERSION,
        "nearest_script_id": "",
        "difference_dimensions": [],
        "difference_summary": "本批暂无其它已生成镜头可比对",
        "review_status": MIXED_VERDICT_DISTINCT_THEME,
        "counts_as_independent": True,
        "comparison_scope": "RENDERED_ONLY",
        "references_compared": len(reference_list),
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
                "comparison_scope": "RENDERED_BATCH",
            }
        )

    # The generated montage still has to *be* a montage.  Four shots whose
    # visible contents are identical are one shot delivered four times, whatever
    # the frozen requirements said.
    events = [
        _text(shot.get("rendered_event"))
        for shot in (signature.get("visual") or {}).get("shots") or []
    ]
    distinct_events = {event for event in events if event}
    report["rendered_shot_count"] = len(events)
    report["distinct_rendered_events"] = len(distinct_events)
    report["montage_collapsed"] = bool(events) and len(distinct_events) <= 1
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
