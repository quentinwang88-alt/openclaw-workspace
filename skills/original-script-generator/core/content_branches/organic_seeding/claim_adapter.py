"""Read-only central claim projection for the organic-seeding branch.

The central database owns product truth and evidence governance.  This module
only decides how an already-governed claim may participate in an organic story;
it deliberately does not import direct-response argument bundles, CTA logic or
sales scoring.
"""
from __future__ import annotations

import json
import hashlib
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence

from core.product_claim_storage import claim_store_descriptor, connect_claim_store


ACTIVE_STATUSES = {"VERIFIED", "UNRESOLVED"}


def _text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _json_list(value: Any) -> List[str]:
    try:
        parsed = json.loads(str(value or "[]"))
    except (TypeError, ValueError):
        return []
    if not isinstance(parsed, list):
        return []
    return [_text(item) for item in parsed if _text(item)]


def default_claim_db_path() -> Path:
    explicit = _text(
        os.environ.get("ORGANIC_SEEDING_CLAIM_DB_PATH")
        or os.environ.get("VOICEOVER_DB_PATH")
    )
    if explicit:
        return Path(explicit).expanduser()
    return Path.home() / "voiceover_copy_engine" / "var" / "voiceover.sqlite"


class CentralClaimProvider:
    """Load a stable product-claim snapshot from the central authority."""

    def __init__(self, db_path: str | Path | None = None) -> None:
        self.db_path = Path(db_path).expanduser() if db_path else default_claim_db_path()
        self.descriptor = claim_store_descriptor(
            self.db_path,
            explicit_sqlite=db_path is not None,
        )

    def _empty(self, product_code: str, *, status: str, reason: str = "") -> Dict[str, Any]:
        return {
            "schema_version": "central-claim-snapshot-v1",
            "provider": self.descriptor["provider"],
            "storage_backend": self.descriptor["backend"],
            "product_code": product_code,
            "status": status,
            "db_fingerprint": "",
            "snapshot_at": "",
            "claims": [],
            "reason": reason,
        }

    def snapshot(self, product_code: str) -> Dict[str, Any]:
        product = _text(product_code)
        if not product:
            return self._empty(product, status="UNAVAILABLE", reason="PRODUCT_CODE_REQUIRED")
        if self.descriptor["backend"] == "unavailable":
            return self._empty(
                product,
                status="UNAVAILABLE",
                reason=str(self.descriptor.get("reason") or "CLAIM_STORE_UNAVAILABLE"),
            )
        try:
            with connect_claim_store(self.descriptor) as connection:
                rows = connection.execute(
                    """
                SELECT
                    c.claim_id,
                    c.product_id,
                    c.claim_source_id,
                    c.source_span,
                    c.canonical_claim_zh,
                    c.claim_type,
                    c.claim_theme,
                    c.verification_status,
                    c.evidence_requirement,
                    c.allowed_strength,
                    c.operator_priority,
                    c.risk_tags_json,
                    c.normalizer_confidence,
                    c.created_at,
                    c.updated_at,
                    s.raw_text AS source_raw_text,
                    s.source_type,
                    s.source_ref
                FROM product_claims c
                JOIN product_claim_sources s
                  ON s.claim_source_id = c.claim_source_id
                WHERE c.product_id = ?
                  AND c.verification_status IN ('VERIFIED', 'UNRESOLVED')
                ORDER BY c.created_at ASC, c.claim_id ASC
                """,
                    (product,),
                ).fetchall()
        except Exception as exc:
            return self._empty(
                product,
                status="UNAVAILABLE",
                reason=f"CLAIM_DB_READ_FAILED:{type(exc).__name__}:{exc}",
            )

        # Once operator-argument-v2 exists, legacy whole-cell/segment rows are
        # superseded.  This mirrors the central catalogue boundary and prevents
        # the organic branch from seeing the same statement twice.
        has_argument_v2 = any("#argument-v2-" in _text(row["source_ref"]) for row in rows)
        if has_argument_v2:
            rows = [row for row in rows if "#argument-v2-" in _text(row["source_ref"])]

        claims: List[Dict[str, Any]] = []
        seen: set[tuple[str, str, str]] = set()
        for row in reversed(rows):
            key = (
                _text(row["canonical_claim_zh"]),
                _text(row["claim_type"]).lower(),
                _text(row["verification_status"]).upper(),
            )
            if key in seen:
                continue
            seen.add(key)
            claims.append(
                {
                    "claim_id": _text(row["claim_id"]),
                    "claim_source_id": _text(row["claim_source_id"]),
                    "canonical_claim": _text(row["canonical_claim_zh"]),
                    "operator_expression": _text(row["source_span"]),
                    "source_operator_expression": _text(row["source_raw_text"]),
                    "claim_type": _text(row["claim_type"]).lower(),
                    "claim_theme": _text(row["claim_theme"]).lower(),
                    "verification_status": _text(row["verification_status"]).upper(),
                    "evidence_requirement": _text(row["evidence_requirement"]).lower(),
                    "allowed_strength": _text(row["allowed_strength"]).lower(),
                    "operator_priority": _text(row["operator_priority"]).lower(),
                    "risk_tags": _json_list(row["risk_tags_json"]),
                    "normalizer_confidence": float(row["normalizer_confidence"] or 0),
                    "source_type": _text(row["source_type"]),
                    "source_ref": _text(row["source_ref"]),
                    "created_at": _text(row["created_at"]),
                    "updated_at": _text(row["updated_at"]),
                }
            )
        claims.reverse()
        fingerprint_material = [
            {
                key: claim.get(key)
                for key in (
                    "claim_id", "claim_source_id", "canonical_claim", "operator_expression",
                    "claim_type", "claim_theme", "verification_status",
                    "evidence_requirement", "allowed_strength", "operator_priority",
                    "risk_tags", "source_ref", "updated_at",
                )
            }
            for claim in claims
        ]
        fingerprint = hashlib.sha256(
            json.dumps(
                fingerprint_material, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
        ).hexdigest() if claims else ""
        snapshot_at = max(
            (_text(claim.get("updated_at") or claim.get("created_at")) for claim in claims),
            default="",
        )
        return {
            "schema_version": "central-claim-snapshot-v1",
            "provider": self.descriptor["provider"],
            "storage_backend": self.descriptor["backend"],
            "product_code": product,
            "status": "READY" if claims else "EMPTY",
            "db_fingerprint": fingerprint,
            "snapshot_at": snapshot_at,
            "claims": claims,
            "reason": "",
        }


class OrganicClaimAdapter:
    """Project central truth into organic roles without importing sales logic."""

    def project(self, snapshot: Mapping[str, Any] | None) -> List[Dict[str, Any]]:
        output: List[Dict[str, Any]] = []
        for raw in (snapshot or {}).get("claims") or []:
            if not isinstance(raw, Mapping):
                continue
            claim = dict(raw)
            status = _text(claim.get("verification_status")).upper()
            strength = _text(claim.get("allowed_strength")).lower()
            claim_type = _text(claim.get("claim_type")).lower()
            evidence = _text(claim.get("evidence_requirement")).lower()
            if status not in ACTIVE_STATUSES or strength == "forbidden":
                eligibility = "DEFERRED"
                roles = ["DEFERRED"]
            elif status == "UNRESOLVED":
                # Unresolved product effects remain visible for audit.  Only a
                # centrally typed audience/scenario statement may seed context;
                # free-text feature claims are never reinterpreted here.
                if claim_type in {"audience", "scenario"}:
                    eligibility = "CONTEXT_ONLY"
                    roles = ["CONTEXT_SEED", "SOFT_PERSONAL_ONLY"]
                else:
                    eligibility = "DEFERRED"
                    roles = ["DEFERRED"]
            else:
                eligibility = "ELIGIBLE"
                roles = ["VALUE_TRIGGER"]
                if claim_type in {"visual_result", "feature"} and evidence in {
                    "video_positive", "source_plus_video"
                }:
                    roles.append("VISIBLE_REASON")
                if claim_type in {"audience", "scenario"}:
                    roles.append("CONTEXT_SEED")
                if strength == "soft_only" or claim_type == "visual_result":
                    roles.append("SOFT_PERSONAL_ONLY")
            claim["organic_roles"] = list(dict.fromkeys(roles))
            claim["organic_eligibility"] = eligibility
            claim["visual_dependency"] = (
                "WEARER_REQUIRED" if claim_type == "visual_result" else "FLEXIBLE"
            )
            claim["compatible_carriers"] = (
                ["WEARER_ACTIVE", "MIXED"]
                if claim["visual_dependency"] == "WEARER_REQUIRED"
                else []
            )
            output.append(claim)
        return output

    def enrich_product_context(
        self,
        product_context: Mapping[str, Any],
        snapshot: Mapping[str, Any] | None,
    ) -> Dict[str, Any]:
        context = dict(product_context)
        catalogue = self.project(snapshot)
        facts: List[Any] = list(context.get("facts") or [])
        existing_ids = {
            _text(item.get("fact_id") or item.get("claim_key"))
            for item in facts
            if isinstance(item, Mapping)
        }
        existing_texts = {
            _text(item.get("text") or item.get("fact_text"))
            if isinstance(item, Mapping) else _text(item)
            for item in facts
        }
        for claim in catalogue:
            if claim.get("organic_eligibility") != "ELIGIBLE":
                continue
            claim_id = _text(claim.get("claim_id"))
            text = _text(claim.get("canonical_claim"))
            if not claim_id or not text or claim_id in existing_ids or text in existing_texts:
                continue
            facts.append(
                {
                    "fact_id": claim_id,
                    "text": text,
                    "source": "CENTRAL_CLAIM_CATALOG",
                    "claim_source_id": claim.get("claim_source_id"),
                    "claim_type": claim.get("claim_type"),
                    "claim_theme": claim.get("claim_theme"),
                    "allowed_strength": claim.get("allowed_strength"),
                    "evidence_requirement": claim.get("evidence_requirement"),
                    "verification_status": claim.get("verification_status"),
                    "organic_roles": claim.get("organic_roles") or [],
                    "operator_expression": claim.get("operator_expression"),
                }
            )
            existing_ids.add(claim_id)
            existing_texts.add(text)
        context["facts"] = facts
        context["governed_claim_snapshot"] = dict(snapshot or {})
        context["organic_claim_catalog"] = catalogue
        return context


def eligible_claims(catalogue: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    return [dict(item) for item in catalogue if item.get("organic_eligibility") == "ELIGIBLE"]
