#!/usr/bin/env python3
"""
补货建议 ABC 分类工具。

基于全量 SKU 的日均销量做帕累托分层，用于动态安全天数计算。
"""

from typing import Dict, Iterable, Optional


def _get_sku_code(item: dict) -> Optional[str]:
    """兼容不同字段命名的 SKU 标识。"""
    return item.get("sku") or item.get("sku_code") or item.get("SKU编码")


def classify_skus_by_abc(
    sku_list: Iterable[dict],
    a_threshold: float = 0.70,
    b_threshold: float = 0.90,
    new_sku_age_days: int = 7,
    unknown_age_default_class: str = "B",
) -> Dict[str, str]:
    """
    基于日均销量累计占比做 ABC 分类。

    规则：
    - 有明确年龄且 < new_sku_age_days 的 SKU 归为 NEW
    - 有销量的 SKU 按日均销量降序做帕累托分层
    - 无销量 SKU 默认归为 C
    - 缺失年龄不会阻断 ABC 分类；只有当完全无法分类时才用 unknown_age_default_class 兜底
    """
    normalized_default_class = (unknown_age_default_class or "B").upper()
    if normalized_default_class not in {"A", "B", "C", "NEW"}:
        normalized_default_class = "B"

    valid_skus = []
    zero_sales_skus = []
    new_skus = []
    result: Dict[str, str] = {}

    for raw_item in sku_list:
        sku_code = _get_sku_code(raw_item)
        if not sku_code:
            continue

        avg_sales = float(raw_item.get("avg_daily_sales", 0) or 0)
        sku_age_days = raw_item.get("sku_age_days")

        if sku_age_days is not None and sku_age_days < new_sku_age_days:
            new_skus.append((sku_code, raw_item))
            continue

        if avg_sales > 0:
            valid_skus.append((sku_code, avg_sales))
        else:
            zero_sales_skus.append((sku_code, raw_item))

    if len(valid_skus) == 1:
        result[valid_skus[0][0]] = "A"
    elif valid_skus:
        sorted_skus = sorted(valid_skus, key=lambda item: (-item[1], item[0]))
        total_sales = sum(avg_sales for _, avg_sales in sorted_skus)

        if total_sales > 0:
            cumulative_sales = 0.0
            for sku_code, avg_sales in sorted_skus:
                previous_ratio = cumulative_sales / total_sales if total_sales else 0
                if previous_ratio < a_threshold:
                    result[sku_code] = "A"
                elif previous_ratio < b_threshold:
                    result[sku_code] = "B"
                else:
                    result[sku_code] = "C"
                cumulative_sales += avg_sales
        else:
            for sku_code, _ in sorted_skus:
                result[sku_code] = "C"

    for sku_code, _ in zero_sales_skus:
        result.setdefault(sku_code, "C")

    for sku_code, _ in new_skus:
        result[sku_code] = "NEW"

    if not result:
        for raw_item in sku_list:
            sku_code = _get_sku_code(raw_item)
            if sku_code:
                result[sku_code] = normalized_default_class

    return result
