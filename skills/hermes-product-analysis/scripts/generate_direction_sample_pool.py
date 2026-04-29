#!/usr/bin/env python3
"""Generate direction top/new sample pool artifact for an existing latest run."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.reaggregate_latest_market_insight import _build_scored_item  # noqa: E402
from src.market_insight_sample_pool import build_direction_sample_pool, build_sample_pool_diagnostics  # noqa: E402
from src.market_insight_table_adapter import MarketInsightTableAdapter  # noqa: E402


DEFAULT_ARTIFACTS_ROOT = ROOT / "artifacts" / "market_insight"


def _load_config(table_id: str, config_dir: Path):
    adapter = MarketInsightTableAdapter()
    configs = adapter.load_table_configs(config_dir, validate_source=False)
    for config in configs:
        if config.table_id == table_id:
            return config
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate direction sample pool for latest market-insight index.")
    parser.add_argument("--country", required=True)
    parser.add_argument("--category", required=True)
    parser.add_argument("--artifacts-root", default=str(DEFAULT_ARTIFACTS_ROOT))
    parser.add_argument("--config-dir", default=str(ROOT / "configs" / "market_insight_table_configs"))
    args = parser.parse_args()

    artifacts_root = Path(args.artifacts_root)
    latest_path = artifacts_root / "latest" / f"{args.country}__{args.category}.json"
    latest_payload = json.loads(latest_path.read_text(encoding="utf-8"))
    cards_path = Path(str(latest_payload.get("cards_path") or ""))
    run_dir = cards_path.parent
    product_tags_path = Path(str(latest_payload.get("product_tags_path") or run_dir / "market_insight_product_tags.json"))
    cards_payload = json.loads(cards_path.read_text(encoding="utf-8"))
    scored_payload = json.loads(product_tags_path.read_text(encoding="utf-8"))
    config = _load_config(str(latest_payload.get("table_id") or ""), Path(args.config_dir))
    scored_items = [_build_scored_item(item) for item in scored_payload]
    rows = build_direction_sample_pool(scored_items=scored_items, direction_cards=cards_payload, config=config)
    diagnostics = build_sample_pool_diagnostics(scored_items=scored_items, direction_cards=cards_payload, rows=rows)
    sample_pool_path = run_dir / "direction_sample_pool.json"
    diagnostics_path = run_dir / "direction_sample_pool_diagnostics.json"
    sample_pool_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    diagnostics_path.write_text(json.dumps(diagnostics, ensure_ascii=False, indent=2), encoding="utf-8")
    latest_payload["sample_pool_path"] = str(sample_pool_path)
    latest_payload["sample_pool_diagnostics_path"] = str(diagnostics_path)
    latest_payload["product_tags_path"] = str(product_tags_path)
    latest_path.write_text(json.dumps(latest_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"sample_pool_path": str(sample_pool_path), "diagnostics_path": str(diagnostics_path), "rows": len(rows), "diagnostics": diagnostics, "latest_path": str(latest_path)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
