#!/usr/bin/env python3
"""FastMoss Excel 导入与标准化。"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from app.models import ImportResult, RuleConfig
from app.utils import (
    calc_listing_days,
    extract_product_id,
    first_number,
    json_dumps,
    parse_datetime_value,
    parse_percent,
    parse_price_range,
    safe_text,
    to_rmb,
    utc_now_iso,
)


COLUMN_ALIASES = {
    "product_id": ["product_id", "商品ID", "产品ID"],
    "product_url": [
        "TikTok商品落地页地址",
        "商品落地页地址",
        "商品详情页",
        "商品链接",
        "产品链接",
        "product_url",
        "url",
    ],
    "product_name": ["商品名称", "产品名称", "标题", "商品标题", "product_name", "title"],
    "shop_name": ["店铺名称", "店铺", "shop_name", "shop"],
    "product_image": ["商品图片", "商品主图", "主图", "product_image", "image"],
    "listing_time": ["预估商品上架时间", "商品上架时间", "上架时间", "listing_time", "estimated_listing_time"],
    "price_raw": ["售价", "售价区间", "价格", "price", "price_range"],
    "sales_7d": ["7天销量", "近7天销量", "7d销量", "sales_7d"],
    "revenue_7d": ["7天销售额", "近7天销售额", "7d销售额", "revenue_7d"],
    "total_sales": ["总销量", "累计销量", "total_sales"],
    "total_revenue": ["总销售额", "累计销售额", "total_revenue"],
    "creator_count": ["带货达人总数", "达人总数", "creator_count"],
    "creator_order_rate": ["达人出单率", "达人成交率", "creator_order_rate"],
    "video_count": ["带货视频总数", "视频总数", "video_count"],
    "live_count": ["带货直播总数", "直播总数", "live_count"],
    "commission_rate": ["佣金比例", "佣金率", "commission_rate"],
}

REQUIRED_LOGICAL_FIELDS = [
    "product_name",
    "price_raw",
    "sales_7d",
    "revenue_7d",
    "total_sales",
    "total_revenue",
    "creator_count",
    "video_count",
    "live_count",
    "commission_rate",
]


def read_fastmoss_file(source_file_path: str) -> List[Dict[str, Any]]:
    path = Path(source_file_path)
    if not path.exists():
        raise FileNotFoundError("FastMoss 文件不存在: {path}".format(path=path))
    if path.suffix.lower() == ".csv":
        frame = pd.read_csv(path)
    else:
        frame = pd.read_excel(path)
    frame.columns = [safe_text(column) for column in frame.columns]
    frame = frame.fillna("")
    return frame.to_dict(orient="records")


def resolve_field_mapping(columns: List[str]) -> Dict[str, str]:
    mapping = {}
    normalized = {safe_text(column): column for column in columns}
    for logical_name, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            candidate = normalized.get(alias)
            if candidate:
                mapping[logical_name] = candidate
                break
    return mapping


def _validate_required_columns(mapping: Dict[str, str]) -> None:
    missing = []
    for logical_name in REQUIRED_LOGICAL_FIELDS:
        if logical_name not in mapping:
            missing.append(logical_name)
    if "product_id" not in mapping and "product_url" not in mapping:
        missing.append("product_id/product_url")
    if missing:
        raise ValueError("FastMoss 文件缺少必需列: {fields}".format(fields=", ".join(missing)))


def _field(row: Dict[str, Any], mapping: Dict[str, str], logical_name: str) -> Any:
    column = mapping.get(logical_name)
    return row.get(column) if column else ""


def normalize_fastmoss_rows(
    source_file_path: str,
    batch_id: str,
    snapshot_time: Any,
    rule_config: RuleConfig,
) -> ImportResult:
    rows = read_fastmoss_file(source_file_path)
    columns = list(rows[0].keys()) if rows else []
    mapping = resolve_field_mapping(columns)
    _validate_required_columns(mapping)

    snapshot_dt = parse_datetime_value(snapshot_time)
    warnings = []  # type: List[str]
    records = []  # type: List[Dict[str, Any]]
    seen_product_ids = set()

    for index, row in enumerate(rows, start=1):
        explicit_product_id = _field(row, mapping, "product_id")
        product_url = safe_text(_field(row, mapping, "product_url"))
        product_id = extract_product_id(product_url, explicit_product_id=explicit_product_id)
        if not product_id:
            warnings.append("第 {index} 行缺少 product_id，已跳过".format(index=index))
            continue
        if product_id in seen_product_ids:
            warnings.append("第 {index} 行 product_id={product_id} 在批次内重复，已跳过".format(index=index, product_id=product_id))
            continue
        seen_product_ids.add(product_id)

        listing_time = parse_datetime_value(_field(row, mapping, "listing_time"))
        listing_days = calc_listing_days(snapshot_dt, listing_time)
        price_raw, price_low_local, price_high_local, price_mid_local = parse_price_range(_field(row, mapping, "price_raw"))
        sales_7d = first_number(_field(row, mapping, "sales_7d"))
        revenue_7d = first_number(_field(row, mapping, "revenue_7d"))
        total_sales = first_number(_field(row, mapping, "total_sales"))
        total_revenue = first_number(_field(row, mapping, "total_revenue"))
        creator_count = first_number(_field(row, mapping, "creator_count"))
        creator_order_rate = parse_percent(_field(row, mapping, "creator_order_rate"))
        video_count = first_number(_field(row, mapping, "video_count"))
        live_count = first_number(_field(row, mapping, "live_count"))
        commission_rate = parse_percent(_field(row, mapping, "commission_rate"))

        avg_price_7d_rmb = None
        if sales_7d and sales_7d > 0 and revenue_7d is not None:
            avg_price_7d_rmb = to_rmb(revenue_7d / sales_7d, rule_config.fx_rate_to_rmb)

        avg_price_total_rmb = None
        if total_sales and total_sales > 0 and total_revenue is not None:
            avg_price_total_rmb = to_rmb(total_revenue / total_sales, rule_config.fx_rate_to_rmb)

        video_density = None
        creator_density = None
        if total_sales and total_sales > 0:
            if video_count is not None:
                video_density = round(video_count / total_sales * 1000.0, 4)
            if creator_count is not None:
                creator_density = round(creator_count / total_sales * 1000.0, 4)

        record = {
            "batch_id": batch_id,
            "product_id": product_id,
            "product_name": safe_text(_field(row, mapping, "product_name")),
            "shop_name": safe_text(_field(row, mapping, "shop_name")),
            "product_image": safe_text(_field(row, mapping, "product_image")),
            "product_url": product_url,
            "listing_time": listing_time.isoformat() if listing_time else "",
            "listing_days": listing_days,
            "price_raw": price_raw,
            "price_low_local": price_low_local,
            "price_high_local": price_high_local,
            "price_mid_local": price_mid_local,
            "fx_rate_to_rmb": rule_config.fx_rate_to_rmb,
            "price_low_rmb": to_rmb(price_low_local, rule_config.fx_rate_to_rmb) if price_low_local is not None else None,
            "price_high_rmb": to_rmb(price_high_local, rule_config.fx_rate_to_rmb) if price_high_local is not None else None,
            "price_mid_rmb": to_rmb(price_mid_local, rule_config.fx_rate_to_rmb) if price_mid_local is not None else None,
            "sales_7d": sales_7d,
            "revenue_7d": revenue_7d,
            "avg_price_7d_rmb": avg_price_7d_rmb,
            "total_sales": total_sales,
            "total_revenue": total_revenue,
            "avg_price_total_rmb": avg_price_total_rmb,
            "creator_count": creator_count,
            "creator_order_rate": creator_order_rate,
            "video_count": video_count,
            "live_count": live_count,
            "commission_rate": commission_rate,
            "video_competition_density": video_density,
            "creator_competition_density": creator_density,
            "import_warnings": "",
            "raw_row_json": json_dumps(row),
            "updated_at": utc_now_iso(),
        }
        records.append(record)

    return ImportResult(
        records=records,
        warnings=warnings,
        field_mapping=mapping,
        total_rows=len(rows),
        skipped_rows=max(len(rows) - len(records), 0),
    )
