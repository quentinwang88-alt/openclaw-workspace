#!/usr/bin/env python3
"""Combine several MX wigs crawl batches into one local, deduplicated analysis."""
from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
from typing import Any, Dict, List


SKILL_DIR = Path(__file__).resolve().parents[1]
ENRICH_PATH = SKILL_DIR / "scripts" / "enrich_wigs_voc.py"


def load_enricher():
    spec = importlib.util.spec_from_file_location("enrich_wigs_voc_combined", str(ENRICH_PATH))
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load {}".format(ENRICH_PATH))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> int:
    parser = argparse.ArgumentParser(description="Combine MX wigs VOC batches for local analysis")
    parser.add_argument("--batch-id", required=True, help="synthetic ID for the combined analysis")
    parser.add_argument("--source-batch", action="append", required=True)
    parser.add_argument("--database-url", default=None)
    parser.add_argument("--taxonomy", default=str(SKILL_DIR / "references" / "wigs_voc_taxonomy_v1.json"))
    parser.add_argument("--gold-sample", default=str(SKILL_DIR / "references" / "wigs_voc_gold_sample_v1.json"))
    parser.add_argument("--output-dir", default=str(SKILL_DIR / "output"))
    args = parser.parse_args()

    enrich = load_enricher()
    taxonomy = enrich.load_json(Path(args.taxonomy))
    gold = enrich.load_json(Path(args.gold_sample))
    unique: Dict[tuple, Dict[str, Any]] = {}
    source_counts: Dict[str, int] = {}
    for source_batch in args.source_batch:
        rows = enrich.fetch_rows(source_batch, args.database_url)
        source_counts[source_batch] = len(rows)
        for row in rows:
            normalized = enrich.normalize(str(row.get("voc_text") or ""))
            key = (str(row.get("fastmoss_product_id") or ""), normalized)
            if key not in unique:
                copied = dict(row)
                copied["source_batch_id"] = source_batch
                copied["batch_id"] = args.batch_id
                unique[key] = copied

    records: List[Dict[str, Any]] = []
    for row in unique.values():
        record = enrich.enrich_row(row, taxonomy)
        record["source_batch_id"] = row["source_batch_id"]
        records.append(record)
    records.sort(key=lambda item: (item["fastmoss_product_id"], item["voc_rank"], item["source_batch_id"]))

    result = {
        "batch_id": args.batch_id,
        "source_batches": args.source_batch,
        "source_raw_counts": source_counts,
        "deduplicated_count": len(records),
        "market": taxonomy.get("market"),
        "category_key": taxonomy.get("category_key"),
        "taxonomy_version": taxonomy.get("version"),
        "dry_run": True,
        "written": False,
        "summary": enrich.summarize(records),
        "gold_validation": enrich.validate_gold(records, gold, taxonomy),
        "draft_insights": enrich.build_draft_insights(records),
        "records": records,
    }
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "{}_wigs_voc_dryrun.json".format(args.batch_id)
    report_path = output_dir / "{}_wigs_voc_validation.md".format(args.batch_id)
    json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    report_path.write_text(enrich.markdown_report(result), encoding="utf-8")
    print(json.dumps({
        "success": result["gold_validation"]["passed"],
        "batch_id": args.batch_id,
        "source_raw_counts": source_counts,
        "summary": result["summary"],
        "category_confidence": result["draft_insights"]["category_artifact"]["confidence_level"],
        "json": str(json_path),
        "report": str(report_path),
    }, ensure_ascii=False, indent=2))
    return 0 if result["gold_validation"]["passed"] else 3


if __name__ == "__main__":
    raise SystemExit(main())
