#!/usr/bin/env python3
"""
补货建议核心逻辑测试。
"""

import sys
from pathlib import Path

SKILLS_DIR = Path(__file__).parent
if str(SKILLS_DIR) not in sys.path:
    sys.path.insert(0, str(SKILLS_DIR))

from restock_abc_classifier import classify_skus_by_abc
from restock_skill_adapter import _build_restock_item, _resolve_restock_settings


def test_double_priority_and_risk_flag():
    settings = _resolve_restock_settings(
        {"restock": {"priority_strategy": "both", "enable_abc_layering": False}},
        purchase_cycle_days=17,
    )

    dependent_on_transit = _build_restock_item(
        {
            "sku": "SKU-RISK",
            "available": 50,
            "avg_daily_sales": 10,
            "purchase_sale_days": 5,
        },
        in_transit_qty=400,
        threshold_days=10,
        settings=settings,
        abc_map={},
    )
    assert dependent_on_transit is not None
    assert dependent_on_transit["suggested_qty"] == 0
    assert dependent_on_transit["stock_priority"][0] == "🟠 高"
    assert dependent_on_transit["effective_priority"][0] == "🟢 低"
    assert dependent_on_transit["risk_flag"] == "依赖在途"

    advance_restock = _build_restock_item(
        {
            "sku": "SKU-ADVANCE",
            "available": 120,
            "avg_daily_sales": 10,
            "purchase_sale_days": 12,
        },
        in_transit_qty=0,
        threshold_days=10,
        settings=settings,
        abc_map={},
    )
    assert advance_restock is not None
    assert advance_restock["suggested_qty"] > 0
    assert advance_restock["is_advance_restock"] is True


def test_abc_classifier_edge_cases():
    single = classify_skus_by_abc([
        {"sku": "ONLY", "avg_daily_sales": 8},
    ])
    assert single["ONLY"] == "A"

    zero_sales = classify_skus_by_abc([
        {"sku": "ZERO-A", "avg_daily_sales": 0},
        {"sku": "ZERO-B", "avg_daily_sales": 0},
    ])
    assert zero_sales["ZERO-A"] == "C"
    assert zero_sales["ZERO-B"] == "C"

    with_new = classify_skus_by_abc([
        {"sku": "NEW-ITEM", "avg_daily_sales": 0, "sku_age_days": 3},
        {"sku": "STABLE", "avg_daily_sales": 20, "sku_age_days": 20},
        {"sku": "LONGTAIL", "avg_daily_sales": 2, "sku_age_days": 40},
    ])
    assert with_new["NEW-ITEM"] == "NEW"
    assert with_new["STABLE"] == "A"
    assert with_new["LONGTAIL"] in {"B", "C"}


def test_abc_layered_lead_time():
    settings = _resolve_restock_settings(
        {"restock": {"priority_strategy": "both", "enable_abc_layering": True}},
        purchase_cycle_days=17,
    )
    abc_map = {"SKU-A": "A", "SKU-B": "B", "SKU-C": "C", "SKU-N": "NEW"}

    sku_a = _build_restock_item(
        {
            "sku": "SKU-A",
            "available": 100,
            "avg_daily_sales": 10,
            "purchase_sale_days": 10,
        },
        in_transit_qty=0,
        threshold_days=10,
        settings=settings,
        abc_map=abc_map,
    )
    sku_c = _build_restock_item(
        {
            "sku": "SKU-C",
            "available": 100,
            "avg_daily_sales": 10,
            "purchase_sale_days": 10,
        },
        in_transit_qty=0,
        threshold_days=10,
        settings=settings,
        abc_map=abc_map,
    )
    sku_new = _build_restock_item(
        {
            "sku": "SKU-N",
            "available": 100,
            "avg_daily_sales": 10,
            "purchase_sale_days": 10,
        },
        in_transit_qty=0,
        threshold_days=10,
        settings=settings,
        abc_map=abc_map,
    )

    assert sku_a is not None and sku_a["lead_time_used"] == 20 and sku_a["safety_days_used"] == 5
    assert sku_c is not None and sku_c["lead_time_used"] == 16 and sku_c["safety_days_used"] == 1
    assert sku_new is not None and sku_new["lead_time_used"] == 18 and sku_new["safety_days_used"] == 3


def main():
    print("\n🧪 补货建议逻辑测试\n")
    test_double_priority_and_risk_flag()
    print("✅ 双优先级与风险标记通过")
    test_abc_classifier_edge_cases()
    print("✅ ABC 分类边界通过")
    test_abc_layered_lead_time()
    print("✅ ABC 分层补货周期通过")
    print("\n🎉 补货建议逻辑测试通过")


if __name__ == "__main__":
    main()
