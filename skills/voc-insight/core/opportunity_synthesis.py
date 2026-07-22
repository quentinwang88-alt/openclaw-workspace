"""Build evidence-bound product opportunity cards and reusable spec/risk items."""
from __future__ import annotations

import hashlib
from collections import defaultdict
from typing import Any, Dict, List, Sequence

from .opportunity_aggregation import confidence_for_metrics, metrics_for_rows


FORBIDDEN_FIELDS = {
    "recommended_product_id", "product_potential_score", "final_action",
    "recommended_test", "need_lookup", "procurement_conclusion", "margin_conclusion",
}

REQUIRED_CARD_FIELDS = {
    "opportunity_id", "run_id", "market", "category_key", "product_forms",
    "opportunity_type", "title_zh", "user_job", "unmet_need", "opportunity_hypothesis",
    "must_have_specs", "optional_specs", "avoid_specs", "supporting_signal_ids",
    "contradicting_signal_ids", "metrics", "confidence", "evidence_refs", "limitations",
    "status", "generated_at",
}


def forbidden_paths(value: Any, prefix: str = "") -> List[str]:
    paths: List[str] = []
    if isinstance(value, dict):
        for key, nested in value.items():
            path = "{}.{}".format(prefix, key) if prefix else str(key)
            if key in FORBIDDEN_FIELDS:
                paths.append(path)
            paths.extend(forbidden_paths(nested, path))
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            paths.extend(forbidden_paths(nested, "{}[{}]".format(prefix, index)))
    return paths


def stable_opportunity_id(market: str, category: str, semantic_key: str) -> str:
    raw = "{}|{}|{}".format(market, category, semantic_key)
    return "vocopp_{}".format(hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20])


def rows_for_mapping(rows: Sequence[Dict[str, Any]], mapping: Dict[str, Any]) -> List[Dict[str, Any]]:
    forms = set(mapping.get("product_forms") or [])
    tags = {tag for group in (mapping.get("required_signal_groups") or []) for tag in group}
    return [
        row for row in rows
        if row.get("signal_tag") in tags and (not forms or row.get("product_form") in forms)
    ]


def required_groups_present(rows: Sequence[Dict[str, Any]], mapping: Dict[str, Any]) -> bool:
    present = {str(row.get("signal_tag") or "") for row in rows}
    groups = mapping.get("required_signal_groups") or []
    return bool(groups) and all(any(tag in present for tag in group) for group in groups)


def limitations_for(metrics: Dict[str, Any], rows: Sequence[Dict[str, Any]]) -> List[str]:
    limitations: List[str] = []
    pool_types = {str(row.get("pool_type") or "") for row in rows if row.get("pool_type")}
    if pool_types == {"classic"}:
        limitations.append("classic_only_evidence")
    if metrics.get("proxy_evidence_count"):
        limitations.append("contains_category_proxy_evidence")
    if float(metrics.get("max_product_contribution") or 0) > 0.3:
        limitations.append("single_product_bias")
    if not metrics.get("natural_sample_count"):
        limitations.append("diagnostic_sample_only")
    if int(metrics.get("direct_product_count") or 0) < 5:
        limitations.append("limited_product_coverage")
    return limitations


def build_opportunity_cards(
    atomic_rows: Sequence[Dict[str, Any]], adapter_payload: Dict[str, Any],
    run_id: str, market: str, category_key: str, generated_at: str,
) -> List[Dict[str, Any]]:
    category = ((adapter_payload.get("categories") or {}).get(category_key) or {})
    cards: List[Dict[str, Any]] = []
    for mapping in category.get("opportunities") or []:
        members = rows_for_mapping(atomic_rows, mapping)
        if not required_groups_present(members, mapping):
            continue
        metrics = metrics_for_rows(members)
        confidence = confidence_for_metrics(metrics)
        negative_tags = sorted({str(row.get("signal_tag") or "") for row in members if row.get("polarity") == "negative"})
        positive_tags = sorted({str(row.get("signal_tag") or "") for row in members if row.get("polarity") == "positive"})
        semantic_key = str(mapping.get("semantic_key") or "")
        cards.append({
            "opportunity_id": stable_opportunity_id(market, category_key, semantic_key),
            "semantic_key": semantic_key,
            "run_id": run_id,
            "market": market,
            "category_key": category_key,
            "product_forms": mapping.get("product_forms") or sorted({str(row.get("product_form") or "") for row in members}),
            "opportunity_type": mapping.get("opportunity_type") or "signal_only",
            "title_zh": mapping.get("title_zh") or semantic_key,
            "user_job": mapping.get("user_job") or "",
            "unmet_need": mapping.get("unmet_need") or "",
            "opportunity_hypothesis": mapping.get("opportunity_hypothesis") or "",
            "must_have_specs": mapping.get("must_have_specs") or [],
            "optional_specs": mapping.get("optional_specs") or [],
            "avoid_specs": mapping.get("avoid_specs") or [],
            "inspection_checks": mapping.get("inspection_checks") or [],
            "usage_scenarios": mapping.get("usage_scenarios") or [],
            "supporting_signal_ids": positive_tags,
            "contradicting_signal_ids": negative_tags,
            "metrics": metrics,
            "confidence": confidence,
            "evidence_refs": metrics["evidence_refs"],
            "source_evidence_refs": metrics["source_evidence_refs"],
            "limitations": limitations_for(metrics, members),
            "status": "first_observation",
            "generated_at": generated_at,
        })
    return sorted(cards, key=lambda card: (
        {"stable_reference": 0, "direction_candidate": 1, "emerging_opportunity": 2, "signal_only": 3}.get(card["confidence"], 4),
        -int((card.get("metrics") or {}).get("direct_product_count") or 0),
        card["title_zh"],
    ))


def validate_cards(cards: Sequence[Dict[str, Any]], atomic_rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    valid_refs = {str(row.get("atomic_evidence_id") or "") for row in atomic_rows}
    errors: List[Dict[str, Any]] = []
    for card in cards:
        card_errors: List[str] = []
        for path in forbidden_paths(card):
            card_errors.append("forbidden_field:{}".format(path))
        missing_fields = sorted(REQUIRED_CARD_FIELDS - set(card))
        if missing_fields:
            card_errors.append("missing_required_fields:{}".format(",".join(missing_fields)))
        if not card.get("evidence_refs"):
            card_errors.append("missing_evidence_refs")
        missing = [ref for ref in card.get("evidence_refs") or [] if ref not in valid_refs]
        if missing:
            card_errors.append("unknown_evidence_refs:{}".format(len(missing)))
        if not card.get("user_job") or not card.get("unmet_need"):
            card_errors.append("missing_claim_fields")
        if card_errors:
            errors.append({"opportunity_id": card.get("opportunity_id"), "errors": card_errors})
    return {"passed": not errors, "card_count": len(cards), "errors": errors}


def build_spec_risk_library(cards: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[tuple, Dict[str, Any]] = {}
    field_types = (
        ("must_have_specs", "must_have"),
        ("optional_specs", "optional_upgrade"),
        ("avoid_specs", "avoid"),
        ("inspection_checks", "inspection_check"),
    )
    for card in cards:
        for field, item_type in field_types:
            for requirement in card.get(field) or []:
                key = (card.get("market"), card.get("category_key"), tuple(card.get("product_forms") or []), item_type, requirement)
                if key not in grouped:
                    grouped[key] = {
                        "item_id": "vocspec_{}".format(hashlib.sha256(repr(key).encode("utf-8")).hexdigest()[:20]),
                        "market": card.get("market"),
                        "category_key": card.get("category_key"),
                        "product_forms": card.get("product_forms") or [],
                        "item_type": item_type,
                        "requirement_zh": requirement,
                        "consumer_reason": card.get("unmet_need") or card.get("user_job") or "",
                        "opportunity_ids": [],
                        "evidence_refs": [],
                        "confidence": card.get("confidence"),
                    }
                item = grouped[key]
                item["opportunity_ids"].append(card.get("opportunity_id"))
                item["evidence_refs"].extend(card.get("evidence_refs") or [])
    for item in grouped.values():
        item["opportunity_ids"] = sorted(set(item["opportunity_ids"]))
        item["evidence_refs"] = sorted(set(item["evidence_refs"]))
    return sorted(grouped.values(), key=lambda item: (item["item_type"], item["requirement_zh"]))
