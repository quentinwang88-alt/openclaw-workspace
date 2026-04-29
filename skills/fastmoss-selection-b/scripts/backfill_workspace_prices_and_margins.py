#!/usr/bin/env python3
"""回填选品工作台的人民币价格区间与最新毛利。"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.config import get_settings  # noqa: E402
from app.models import RuleConfig  # noqa: E402
from app.pipeline import FastMossPipeline, _calculate_margins  # noqa: E402
from app.utils import safe_float, to_rmb  # noqa: E402


def _fallback_rule_config(row: Dict[str, Any]) -> RuleConfig:
    return RuleConfig(
        config_id="fallback",
        country=str(row.get("country") or ""),
        category=str(row.get("category") or ""),
        enabled=True,
        fx_rate_to_rmb=safe_float(row.get("fx_rate_to_rmb")) or 1.0,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="回填工作台的人民币价格区间与毛利")
    parser.add_argument("--batch-id", help="只回填指定 batch_id")
    parser.add_argument("--dry-run", action="store_true", help="只输出统计，不回写飞书和 SQLite")
    args = parser.parse_args()

    settings = get_settings()
    pipeline = FastMossPipeline.from_settings(settings)
    rule_configs = pipeline.load_rule_configs() if pipeline.config_client else {}
    selection_rows = pipeline.db.list_selection_records(args.batch_id)

    local_updates = []
    workspace_updates = []
    repriced_count = 0
    margin_count = 0

    for row in selection_rows:
        work_id = str(row.get("work_id") or "").strip()
        if not work_id:
            continue
        price_low_rmb = safe_float(row.get("price_low_rmb"))
        if price_low_rmb is None:
            price_low_rmb = to_rmb(safe_float(row.get("price_low_local")), safe_float(row.get("fx_rate_to_rmb")))
        price_high_rmb = safe_float(row.get("price_high_rmb"))
        if price_high_rmb is None:
            price_high_rmb = to_rmb(safe_float(row.get("price_high_local")), safe_float(row.get("fx_rate_to_rmb")))

        local_update = {
            "work_id": work_id,
            "price_low_rmb": price_low_rmb,
            "price_high_rmb": price_high_rmb,
        }
        workspace_update = {
            "work_id": work_id,
            "最低价_rmb": price_low_rmb,
            "最高价_rmb": price_high_rmb,
        }
        repriced_count += 1

        procurement_price_rmb = safe_float(row.get("procurement_price_rmb"))
        if procurement_price_rmb is not None and price_low_rmb is not None:
            rule_config = rule_configs.get(
                (str(row.get("country") or "").strip(), str(row.get("category") or "").strip())
            ) or _fallback_rule_config(row)
            margins = _calculate_margins(
                price_low_rmb,
                procurement_price_rmb,
                row.get("commission_rate"),
                row.get("category"),
                row.get("product_name"),
                rule_config,
            )
            local_update.update(
                {
                    "pricing_reference_rmb": margins["pricing_reference_rmb"],
                    "platform_fee_rate": margins["platform_fee_rate"],
                    "platform_fee_amount": margins["platform_fee_amount"],
                    "head_shipping_rmb": margins["head_shipping_rmb"],
                    "head_shipping_rule": margins["head_shipping_rule"],
                    "gross_margin_amount": margins["gross_margin_amount"],
                    "gross_margin_rate": margins["gross_margin_rate"],
                    "distribution_margin_amount": margins["distribution_margin_amount"],
                    "distribution_margin_rate": margins["distribution_margin_rate"],
                }
            )
            workspace_update.update(
                {
                    "商品粗毛利率": margins["gross_margin_rate"],
                    "分销后毛利率": margins["distribution_margin_rate"],
                }
            )
            margin_count += 1

        local_updates.append(local_update)
        workspace_updates.append(workspace_update)

    if not args.dry_run:
        pipeline.db.upsert_selection_records(local_updates)
        pipeline._upsert_workspace_partial_updates(workspace_updates)

    print(
        json.dumps(
            {
                "batch_id": args.batch_id or "ALL",
                "dry_run": args.dry_run,
                "selection_rows": len(selection_rows),
                "repriced_rows": repriced_count,
                "margin_rows": margin_count,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
