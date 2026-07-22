#!/usr/bin/env python3
"""Run the standalone, selection-decoupled VOC opportunity-reference MVP."""
from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


SKILL_DIR = Path(__file__).resolve().parents[1]
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from core.atomic_evidence import build_atomic_evidence
from core.opportunity_aggregation import aggregate_signal_summaries
from core.opportunity_synthesis import build_opportunity_cards, build_spec_risk_library, validate_cards


ENRICH_PATH = SKILL_DIR / "scripts" / "enrich_wigs_voc.py"
REPORT_PATH = SKILL_DIR / "scripts" / "build_voc_opportunity_report.py"
DEFAULT_ASPECT_TAXONOMY = SKILL_DIR / "references" / "voc_aspect_taxonomy_v1.json"
DEFAULT_ADAPTERS = SKILL_DIR / "references" / "category_opportunity_adapters.json"


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load {}".format(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_json(path: Path) -> Dict[str, Any]:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def now_compact() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def records_from_input(path: Path) -> Dict[str, Any]:
    payload = load_json(path)
    records = payload.get("records")
    if not isinstance(records, list):
        raise RuntimeError("input JSON must contain records[]")
    return {
        "batch_id": payload.get("batch_id"),
        "market": payload.get("market"),
        "category_key": payload.get("category_key"),
        "source_batches": payload.get("source_batches") or [payload.get("batch_id")],
        "source_quality": payload.get("source_quality") or {},
        "records": records,
    }


def records_from_db(batch_id: str, database_url: Optional[str]) -> Dict[str, Any]:
    enrich = load_module("enrich_wigs_voc_opportunity", ENRICH_PATH)
    taxonomy = enrich.load_json(enrich.TAXONOMY_PATH)
    rows = enrich.fetch_rows(batch_id, database_url)
    records = [enrich.enrich_row(row, taxonomy) for row in rows]
    return {
        "batch_id": batch_id,
        "market": taxonomy.get("market"),
        "category_key": taxonomy.get("category_key"),
        "source_batches": [batch_id],
        "source_quality": enrich.fetch_batch_quality(batch_id, database_url),
        "records": records,
    }


def build_guidance_assessment(
    source_quality: Dict[str, Any], summary: Dict[str, Any], cards: List[Dict[str, Any]],
    records: List[Dict[str, Any]],
) -> Dict[str, Any]:
    export = source_quality.get("export_batch") or {}
    raw = source_quality.get("raw_quality") or {}
    manifest = export.get("manifest_json") or {}
    raw_count = int(raw.get("raw_voc_count") or summary.get("raw_review_count") or 0)
    distinct_count = int(raw.get("distinct_voc_count") or 0)
    blank_count = int(raw.get("blank_voc_count") or 0)
    voc_products = int(raw.get("voc_product_count") or 0)
    target_products = int(export.get("product_count") or manifest.get("target_products") or 0)
    new_target = int(export.get("new_product_count") or manifest.get("new_target") or 0)
    new_voc_products = int(raw.get("new_voc_products") or 0)
    target_voc = int(manifest.get("form_supplement_target_voc") or 80)
    valid_count = int(summary.get("valid_review_count") or 0)
    tagged_count = int(summary.get("tagged_valid_review_count") or 0)
    relevant_products = len({
        str(row.get("fastmoss_product_id") or "") for row in records
        if row.get("is_valid_voc") and row.get("fastmoss_product_id")
    })
    contaminated_products = len({
        str(row.get("fastmoss_product_id") or "") for row in records
        if str(row.get("invalid_reason") or "").startswith("non_core_")
    })
    ratios = {
        "nonblank_rate": round((raw_count - blank_count) / raw_count, 4) if raw_count else 0.0,
        "dedup_rate": round(distinct_count / raw_count, 4) if raw_count else 0.0,
        "product_voc_coverage": round(voc_products / target_products, 4) if target_products else 0.0,
        "voc_depth_coverage": round(min(raw_count / target_voc, 1.0), 4) if target_voc else 0.0,
        "new_pool_voc_coverage": round(new_voc_products / new_target, 4) if new_target else 0.0,
        "category_purity": round(relevant_products / voc_products, 4) if voc_products else 0.0,
        "valid_review_rate": round(valid_count / raw_count, 4) if raw_count else 0.0,
        "tagged_valid_rate": round(tagged_count / valid_count, 4) if valid_count else 0.0,
    }
    maturity = max(
        ({"signal_only": 0.25, "emerging_opportunity": 0.55, "direction_candidate": 0.8, "stable_reference": 1.0}
         .get(str(card.get("confidence") or ""), 0.0) for card in cards),
        default=0.0,
    )
    score = round(
        10 * ratios["nonblank_rate"] + 10 * ratios["dedup_rate"]
        + 15 * ratios["product_voc_coverage"] + 10 * ratios["voc_depth_coverage"]
        + 10 * ratios["new_pool_voc_coverage"] + 15 * ratios["category_purity"]
        + 15 * ratios["valid_review_rate"] + 10 * ratios["tagged_valid_rate"] + 5 * maturity,
        1,
    )
    blockers = []
    if ratios["product_voc_coverage"] < 0.6:
        blockers.append("抓取成功商品覆盖不足60%")
    if new_target and new_voc_products < 2:
        blockers.append("新品池没有形成可用VOC证据")
    if ratios["category_purity"] < 0.85:
        blockers.append("存在明显非核心假发/接发商品污染")
    if not cards or all(card.get("confidence") == "signal_only" for card in cards):
        blockers.append("全部机会仍停留在线索级")
    can_support = not blockers and score >= 75
    label = "高" if score >= 85 else "中高" if score >= 75 else "中低" if score >= 60 else "低"
    return {
        "overall_score": score,
        "overall_confidence": label,
        "can_support_selection_guidance": can_support,
        "guidance_level": "可进入选品验证" if can_support else "仅支持方向参考，不支持独立商品级选品决策",
        "ratios": ratios,
        "counts": {
            "target_products": target_products, "voc_products": voc_products,
            "new_target_products": new_target, "new_voc_products": new_voc_products,
            "raw_voc": raw_count, "target_voc": target_voc, "relevant_products": relevant_products,
            "contaminated_products": contaminated_products,
        },
        "blockers": blockers,
        "allowed_uses": ["消费者痛点与偏好参考", "产品规格和验收条件设计", "候选方向假设生成"],
        "not_allowed_uses": ["商品级排序", "单独决定上新/淘汰", "新品趋势强结论"],
        "source_quality_status": export.get("quality_status") or manifest.get("quality_status") or "unknown",
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Standalone VOC product-opportunity reference pipeline")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--input-json", help="existing enriched/dry-run JSON containing records[]")
    source.add_argument("--batch-id", help="read one batch from the configured VOC RDS tables")
    parser.add_argument("--market", default=None)
    parser.add_argument("--category-key", default=None)
    parser.add_argument("--database-url", default=None)
    parser.add_argument("--aspect-taxonomy", default=str(DEFAULT_ASPECT_TAXONOMY))
    parser.add_argument("--opportunity-adapters", default=str(DEFAULT_ADAPTERS))
    parser.add_argument("--output-dir", default=str(SKILL_DIR / "output"))
    parser.add_argument("--write", action="store_true", help="reserved for WP5; MVP refuses database writes")
    parser.add_argument("--pretty", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.write:
        raise SystemExit("--write is intentionally unavailable in the WP0-WP4 MVP")
    source = records_from_input(Path(args.input_json)) if args.input_json else records_from_db(args.batch_id, args.database_url)
    batch_id = str(source.get("batch_id") or args.batch_id or "")
    market = str(args.market or source.get("market") or "")
    category_key = str(args.category_key or source.get("category_key") or "")
    if not batch_id or not market or not category_key:
        raise SystemExit("batch_id, market and category_key are required")
    if args.market and source.get("market") and args.market != source.get("market"):
        raise SystemExit("market override conflicts with input scope")
    if args.category_key and source.get("category_key") and args.category_key != source.get("category_key"):
        raise SystemExit("category override conflicts with input scope")

    aspect_taxonomy = load_json(Path(args.aspect_taxonomy))
    adapters = load_json(Path(args.opportunity_adapters))
    generated_at = now_iso()
    run_id = "{}__opportunity_reference__{}".format(batch_id, now_compact())
    records: List[Dict[str, Any]] = source["records"]
    atomic = build_atomic_evidence(records, aspect_taxonomy)
    summaries = aggregate_signal_summaries(atomic)
    cards = build_opportunity_cards(atomic, adapters, run_id, market, category_key, generated_at)
    card_validation = validate_cards(cards, atomic)
    spec_library = build_spec_risk_library(cards)
    valid_records = [record for record in records if record.get("is_valid_voc")]
    tagged_valid = [record for record in valid_records if record.get("signal_tags")]
    summary = {
        "raw_review_count": len(records),
        "valid_review_count": len(valid_records),
        "tagged_valid_review_count": len(tagged_valid),
        "untagged_valid_review_count": len(valid_records) - len(tagged_valid),
        "atomic_evidence_count": len(atomic),
        "evidence_product_count": len({row.get("product_id") for row in atomic}),
        "signal_summary_count": len(summaries),
        "opportunity_card_count": len(cards),
        "spec_risk_item_count": len(spec_library),
    }
    guidance_assessment = build_guidance_assessment(
        source.get("source_quality") or {}, summary, cards, records
    )
    quality_gate = {
        "passed": bool(card_validation.get("passed")) and bool(atomic) and bool(cards),
        "card_validation": card_validation,
        "has_atomic_evidence": bool(atomic),
        "has_opportunity_cards": bool(cards),
        "forbidden_selection_outputs": 0,
        "database_written": False,
    }
    payload = {
        "contract_version": "voc_opportunity_output_contract_v2",
        "mode": "opportunity_reference",
        "run_id": run_id,
        "batch_id": batch_id,
        "source_batches": source.get("source_batches") or [batch_id],
        "market": market,
        "category_key": category_key,
        "generated_at": generated_at,
        "dry_run": True,
        "written": False,
        "summary": summary,
        "source_quality": source.get("source_quality") or {},
        "selection_guidance_assessment": guidance_assessment,
        "quality_gate": quality_gate,
        "atomic_evidence": atomic,
        "signal_summaries": summaries,
        "opportunity_cards": cards,
        "spec_risk_library": spec_library,
    }
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    result_json = output_dir / "{}_voc_opportunity_result.json".format(batch_id)
    result_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    report = load_module("build_voc_opportunity_report_runtime", REPORT_PATH)
    artifacts = report.write_artifacts(payload, output_dir)
    artifacts["result_json"] = str(result_json)
    payload["artifacts"] = artifacts
    result_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    compact = {
        "success": quality_gate["passed"], "run_id": run_id, "batch_id": batch_id,
        "market": market, "category_key": category_key, "summary": summary,
        "quality_gate": quality_gate, "artifacts": artifacts,
    }
    print(json.dumps(payload if args.pretty else compact, ensure_ascii=False, indent=2))
    return 0 if quality_gate["passed"] else 3


if __name__ == "__main__":
    raise SystemExit(main())
