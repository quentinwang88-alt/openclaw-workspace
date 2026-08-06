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
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Mapping


DEFAULT_VOICEOVER_ROOT = Path("/Users/likeu3/voiceover_copy_engine")
ARGUMENT_CLAIM_TYPES = frozenset({"benefit", "visual_result"})
FEISHU_OPERATOR_SOURCE_PREFIX = "feishu-product-claims:"
FLEXIBLE_CARRIER_REQUIREMENT = "FLEXIBLE"
KNOWN_CARRIER_REQUIREMENTS = frozenset(
    {"WEARER_REQUIRED", "HAND_REQUIRED", "STATIC_REQUIRED", FLEXIBLE_CARRIER_REQUIREMENT}
)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _stable_hash(material: Any) -> str:
    raw = json.dumps(material, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


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

    if _text(claim_type).lower() == "visual_result":
        return {
            "proof_subject": "ON_BODY_RESULT",
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
    matched = re.match(r"^\s*(\d+)\s*[、./.)）:-]", _text(source_span))
    return int(matched.group(1)) if matched else 9999


def load_verified_selling_point_catalog(
    product_code: str,
    *,
    voiceover_root: str = "",
) -> Dict[str, Any]:
    """Return a deterministic catalog of operator-confirmed selling arguments.

    Every numbered Feishu segment that has already passed operator confirmation
    becomes one selling argument.  A mapped central claim contributes optional
    governance metadata.  Legacy non-operator VERIFIED benefit/visual-result
    rows remain available as a backward-compatible fallback.
    """

    db_path = _claims_db_path(voiceover_root)
    base = {
        "source": "CENTRAL_VOICEOVER_OPERATOR_ARGUMENTS",
        "db_path": str(db_path),
        "product_code": product_code,
        "status": "UNAVAILABLE",
        "catalog": [],
        "evidence_claims": [],
        "confirmed_argument_count": 0,
        "mapped_argument_count": 0,
        "unmapped_argument_count": 0,
        "available_argument_count": 0,
    }
    if not db_path.exists():
        base["snapshot_hash"] = _stable_hash(base)
        return base

    try:
        with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
            conn.row_factory = sqlite3.Row
            source_columns = {
                str(row[1]) for row in conn.execute("PRAGMA table_info(product_claim_sources)")
            }
            claim_columns = {
                str(row[1]) for row in conn.execute("PRAGMA table_info(product_claims)")
            }
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
    except sqlite3.Error as exc:
        base["status"] = "READ_ERROR"
        base["error"] = str(exc)[:240]
        base["snapshot_hash"] = _stable_hash(base)
        return base

    operator_groups: Dict[str, List[Dict[str, Any]]] = {}
    for row in operator_rows:
        material = dict(row)
        operator_groups.setdefault(_text(material.get("claim_source_id")), []).append(material)

    included_operator_sources = set()
    ordered_operator_groups = sorted(
        operator_groups.items(),
        key=lambda pair: (
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
        operator_expression = _operator_expression(first.get("raw_text"))
        if not operator_expression:
            continue
        normalized_rows = [row for row in source_rows if _text(row.get("claim_id"))]
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
        mapped = bool(_text(best.get("concept_id")))
        claim_type = _text(best.get("claim_type")).lower() if mapped else "benefit"
        carrier_policy = _carrier_policy(claim_type, _text(best.get("claim_theme")))
        verified_claim_ids = [
            _text(row.get("claim_id"))
            for row in normalized_rows
            if _text(row.get("verification_status")) == "VERIFIED"
            and _text(row.get("claim_id"))
        ]
        base["catalog"].append(
            {
                "value_id": "OPERATOR_" + source_id,
                "source_argument_id": source_id,
                "primary_selling_point": operator_expression,
                "canonical_selling_point": (
                    _text(best.get("canonical_claim_zh")) if mapped else ""
                ),
                "operator_expression": operator_expression,
                "dominant_user_question": "",
                "proof_thesis": "",
                "decision_thesis": "",
                "script_role": (
                    "result_delivery" if claim_type == "visual_result" else "benefit_delivery"
                ),
                "argument_kind": "SELLING_ARGUMENT",
                "source": "FEISHU_OPERATOR_CONFIRMED_ARGUMENT",
                "authority": "FEISHU_OPERATOR_CONFIRMED",
                "source_claim_ids": list(dict.fromkeys(verified_claim_ids)),
                "normalization_claim_ids": [
                    _text(row.get("claim_id")) for row in normalized_rows if _text(row.get("claim_id"))
                ],
                "concept_id": _text(best.get("concept_id")) if mapped else "",
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
                "respectful_reframe_required": True,
                **carrier_policy,
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
                }
            )
        else:
            base["evidence_claims"].append(common)

    base["available_argument_count"] = len(base["catalog"])
    base["status"] = "AVAILABLE" if base["catalog"] else "NO_SELLING_ARGUMENT"
    base["snapshot_hash"] = _stable_hash(
        {
            "product_code": product_code,
            "catalog": base["catalog"],
            "evidence_claims": base["evidence_claims"],
        }
    )
    return base
