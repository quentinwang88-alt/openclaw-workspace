"""Read-only adapter for governed product selling arguments.

The original-script workflow must not normalize or review product claims on its
own.  It consumes the central voiceover engine's VERIFIED claims once during
batch planning, turns them into a compact catalog, and freezes that catalog in
the batch snapshot.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
from pathlib import Path
from typing import Any, Dict, List


DEFAULT_VOICEOVER_ROOT = Path("/Users/likeu3/voiceover_copy_engine")
ARGUMENT_CLAIM_TYPES = frozenset({"benefit", "visual_result"})


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


def _carrier_policy(claim_type: str) -> Dict[str, Any]:
    """Describe how an authorised value is best shown, not whether it is true.

    A visual result (fit, proportion or silhouette) needs a worn execution for
    the visual to carry that result. A benefit remains flexible: its authority
    comes from the central library and static presentation may still suit an
    ordinary use-case message. Keeping just these two states avoids creating a
    second claim-review system in the original-script workflow.
    """

    if _text(claim_type).lower() == "visual_result":
        return {
            "visual_dependency": "WEARER_REQUIRED",
            "compatible_carriers": ["WEARER_ACTIVE", "MIXED"],
        }
    return {
        "visual_dependency": "FLEXIBLE",
        # Empty means the message remains authorised across carriers; it does
        # not mean the argument is unavailable.
        "compatible_carriers": [],
    }


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
    """Return a deterministic, read-only catalog from central VERIFIED claims.

    Only benefit / visual-result claims become *selling arguments*.  Verified
    feature claims remain in the snapshot for lineage but are evidence facts,
    not permission to make "five buttons" the content thesis.
    """

    db_path = _claims_db_path(voiceover_root)
    base = {
        "source": "CENTRAL_VOICEOVER_VERIFIED_CLAIMS",
        "db_path": str(db_path),
        "product_code": product_code,
        "status": "UNAVAILABLE",
        "catalog": [],
        "evidence_claims": [],
    }
    if not db_path.exists():
        base["snapshot_hash"] = _stable_hash(base)
        return base

    try:
        with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT c.claim_id, c.concept_id, c.source_span,
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
            carrier_policy = _carrier_policy(claim_type)
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

    base["status"] = "AVAILABLE" if base["catalog"] else "NO_SELLING_ARGUMENT"
    base["snapshot_hash"] = _stable_hash(
        {
            "product_code": product_code,
            "catalog": base["catalog"],
            "evidence_claims": base["evidence_claims"],
        }
    )
    return base
