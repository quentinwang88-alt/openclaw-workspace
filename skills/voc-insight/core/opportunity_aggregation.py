"""Deterministic aggregation and confidence grading for VOC opportunity evidence."""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


CONFIDENCE_ORDER = {
    "signal_only": 0,
    "emerging_opportunity": 1,
    "direction_candidate": 2,
    "stable_reference": 3,
}


def confidence_for_metrics(metrics: Dict[str, Any]) -> str:
    products = int(metrics.get("direct_product_count") or 0)
    evidence = int(metrics.get("direct_evidence_count") or 0)
    batches = int(metrics.get("direct_batch_count") or 0)
    max_product = float(metrics.get("max_product_contribution") or 0.0)
    natural = int(metrics.get("natural_sample_count") or 0)
    if products >= 8 and evidence >= 40 and batches >= 2 and max_product <= 0.3 and natural > 0:
        return "stable_reference"
    if products >= 5 and evidence >= 20 and max_product <= 0.3 and natural > 0:
        return "direction_candidate"
    if products >= 3 and evidence >= 10:
        return "emerging_opportunity"
    return "signal_only"


def metrics_for_rows(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    direct = [row for row in rows if row.get("evidence_scope") != "category_proxy"]
    direct_basis = direct or []
    product_counter = Counter(str(row.get("product_id") or "") for row in direct_basis)
    form_counter = Counter(str(row.get("product_form") or "unknown") for row in direct_basis)
    evidence_count = len(rows)
    direct_count = len(direct)
    return {
        "evidence_count": evidence_count,
        "product_count": len({str(row.get("product_id") or "") for row in rows}),
        "batch_count": len({str(row.get("source_batch_id") or row.get("batch_id") or "") for row in rows}),
        "direct_evidence_count": direct_count,
        "direct_product_count": len(product_counter),
        "direct_batch_count": len({str(row.get("source_batch_id") or row.get("batch_id") or "") for row in direct}),
        "proxy_evidence_count": evidence_count - direct_count,
        "positive_count": sum(1 for row in rows if row.get("polarity") == "positive"),
        "negative_count": sum(1 for row in rows if row.get("polarity") == "negative"),
        "neutral_count": sum(1 for row in rows if row.get("polarity") == "neutral"),
        "mixed_count": sum(1 for row in rows if row.get("polarity") == "mixed"),
        "natural_sample_count": sum(1 for row in rows if row.get("sample_pool") == "natural_distribution"),
        "diagnostic_sample_count": sum(1 for row in rows if row.get("sample_pool") == "diagnostic_risk"),
        "max_product_contribution": round((product_counter.most_common(1)[0][1] / direct_count) if product_counter and direct_count else 0.0, 4),
        "max_form_contribution": round((form_counter.most_common(1)[0][1] / direct_count) if form_counter and direct_count else 0.0, 4),
        "evidence_refs": sorted({str(row.get("atomic_evidence_id") or "") for row in rows}),
        "source_evidence_refs": sorted({str(row.get("evidence_id") or "") for row in rows}),
        "contradicting_evidence_refs": [],
    }


def aggregate_signal_summaries(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    groups: Dict[Tuple[str, str, Optional[str], str], List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        market = str(row.get("market") or "")
        category = str(row.get("category_key") or "")
        form = str(row.get("product_form") or "unknown")
        tag = str(row.get("signal_tag") or "")
        groups[(market, category, form, tag)].append(row)
        groups[(market, category, None, tag)].append(row)
    summaries: List[Dict[str, Any]] = []
    for (market, category, form, tag), members in sorted(groups.items(), key=lambda item: str(item[0])):
        metrics = metrics_for_rows(members)
        summaries.append({
            "signal_summary_id": "{}:{}:{}:{}".format(market, category, form or "all", tag),
            "market": market,
            "category_key": category,
            "product_form": form,
            "signal_tag": tag,
            "metrics": metrics,
            "confidence": confidence_for_metrics(metrics),
            "evidence_refs": metrics["evidence_refs"],
            "contradicting_evidence_refs": [],
        })
    return summaries
