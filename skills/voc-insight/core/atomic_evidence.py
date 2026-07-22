"""Convert enriched VOC reviews into traceable aspect-level atomic evidence."""
from __future__ import annotations

import re
import unicodedata
from typing import Any, Dict, Iterable, List, Sequence, Tuple


def normalize(text: Any) -> str:
    raw = unicodedata.normalize("NFKD", str(text or "").lower())
    ascii_text = "".join(ch for ch in raw if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", ascii_text).strip()


def split_clauses_with_offsets(text: str) -> List[Tuple[str, int, int]]:
    """Split conservatively while retaining offsets in the original review."""
    source = str(text or "")
    if not source.strip():
        return []
    boundaries = re.compile(r"[.!?;\n]+|\s+(?:pero|aunque|sin embargo)\s+", re.IGNORECASE)
    result: List[Tuple[str, int, int]] = []
    start = 0
    for match in boundaries.finditer(source):
        raw = source[start:match.start()]
        stripped = raw.strip(" ,")
        if stripped:
            offset = raw.find(stripped)
            result.append((stripped, start + offset, start + offset + len(stripped)))
        start = match.end()
    raw = source[start:]
    stripped = raw.strip(" ,")
    if stripped:
        offset = raw.find(stripped)
        result.append((stripped, start + offset, start + offset + len(stripped)))
    return result or [(source.strip(), source.find(source.strip()), len(source.rstrip()))]


def source_clause(text: str, evidence_text: str) -> Tuple[str, int, int]:
    evidence_norm = normalize(evidence_text)
    clauses = split_clauses_with_offsets(text)
    for clause, start, end in clauses:
        if evidence_norm and evidence_norm in normalize(clause):
            return clause, start, end
    return (text.strip(), 0, len(text.strip()))


def polarity_for(tag: str, segment: Dict[str, Any]) -> str:
    value = str(segment.get("polarity") or "").lower()
    if value in {"positive", "negative", "neutral", "mixed"}:
        return value
    return "negative" if tag.endswith("_issue") else "positive"


def build_atomic_evidence(records: Sequence[Dict[str, Any]], taxonomy: Dict[str, Any]) -> List[Dict[str, Any]]:
    signal_meta = taxonomy.get("signals") or {}
    default_pool = taxonomy.get("default_sample_pool") or "natural_distribution"
    default_scope = taxonomy.get("default_evidence_scope") or "direct"
    output: List[Dict[str, Any]] = []
    for record in records:
        if not record.get("is_valid_voc"):
            continue
        review_text = str(record.get("voc_text") or "")
        if not review_text.strip():
            continue
        segments = record.get("attribute_segments") or []
        seen = set()
        index = 0
        for segment in segments:
            tag = str(segment.get("signal_tag") or "")
            if not tag or tag in seen:
                continue
            seen.add(tag)
            index += 1
            meta = signal_meta.get(tag) or {}
            evidence_text = str(segment.get("evidence_text") or "")
            clause, start, end = source_clause(review_text, evidence_text)
            polarity = polarity_for(tag, segment)
            severity = meta.get("severity") or ("medium" if polarity == "negative" else "unknown")
            evidence_id = str(record.get("evidence_id") or "")
            output.append({
                "atomic_evidence_id": "{}:seg:{:02d}".format(evidence_id, index),
                "evidence_id": evidence_id,
                "segment_index": index,
                "batch_id": record.get("batch_id"),
                "source_batch_id": record.get("source_batch_id") or record.get("batch_id"),
                "market": record.get("market") or "",
                "category_key": record.get("category_key") or "",
                "product_id": str(record.get("fastmoss_product_id") or record.get("product_id") or ""),
                "product_title": record.get("product_title") or "",
                "product_form": record.get("product_form") or "unknown",
                "product_form_label": record.get("product_form_label") or record.get("product_form") or "unknown",
                "pool_type": record.get("pool_type") or "unknown",
                "sample_pool": record.get("sample_pool") or default_pool,
                "evidence_scope": record.get("evidence_scope") or default_scope,
                "aspect_group": meta.get("aspect_group") or "other",
                "signal_tag": tag,
                "polarity": polarity,
                "severity": severity,
                "opinion_target": meta.get("opinion_target") or tag,
                "opinion_text": evidence_text or clause,
                "desired_outcome": meta.get("desired_outcome") or "",
                "usage_scenario": record.get("usage_scenario") or "",
                "controllability": meta.get("controllability") or "unknown",
                "source_text": clause,
                "source_start": start,
                "source_end": end,
                "review_text": review_text,
                "extraction_method": "rule",
                "extractor_version": taxonomy.get("version") or "unknown",
                "quality_flags": [],
            })
    return output
