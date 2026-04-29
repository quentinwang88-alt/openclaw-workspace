#!/usr/bin/env python3
"""Reader for upstream standardized product snapshots.

Layer 1/2 own crawling and basic normalization.  This module only consumes the
ready snapshot rows that already exist in Feishu, preserving the normalized
fields as the shared input for Market Insight and Product Selection agents.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional


READY_STATUS = "ready_for_agents"


FIELD_ALIASES = {
    "crawl_batch_id": ["crawl_batch_id", "抓取批次ID", "批次ID", "batch_id"],
    "product_snapshot_id": ["product_snapshot_id", "商品快照ID"],
    "product_id": ["product_id", "商品ID"],
    "market_id": ["market_id", "市场ID"],
    "market_name": ["market_name", "市场"],
    "category_id": ["category_id", "类目ID"],
    "category_name": ["category_name", "类目"],
    "title": ["title", "商品标题", "商品名称"],
    "main_image_url": ["main_image_url", "商品主图URL", "商品图片"],
    "price": ["price", "价格"],
    "currency": ["currency", "币种"],
    "price_rmb": ["price_rmb", "折算人民币价格", "价格"],
    "sales_7d": ["sales_7d", "7天销量", "7日销量"],
    "sales_30d": ["sales_30d", "30天销量", "30日销量"],
    "video_count": ["video_count", "视频数量", "视频数"],
    "creator_count": ["creator_count", "达人数量", "达人数"],
    "listing_datetime": ["listing_datetime", "上架日期", "上架时间"],
    "product_age_days": ["product_age_days", "上架天数"],
    "age_bucket": ["age_bucket", "商品年龄分桶"],
    "fastmoss_url": ["fastmoss_url", "FastMoss链接"],
    "source": ["source", "来源"],
    "data_status": ["data_status", "数据状态"],
    "is_valid": ["is_valid", "是否有效"],
    "data_quality_flags": ["data_quality_flags", "数据质量标记"],
    "shop_name": ["shop_name", "店铺名称"],
    "platform_product_url": ["platform_product_url", "平台商品链接", "商品链接"],
    "image_urls": ["image_urls", "商品图片组URL"],
    "comments_count": ["comments_count", "评论数"],
    "total_sales": ["total_sales", "累计销量", "总销量"],
    "product_tags": ["product_tags", "商品标签"],
    "manual_notes": ["manual_notes", "人工备注"],
}


@dataclass
class StandardizedProductSnapshot:
    crawl_batch_id: str
    product_snapshot_id: str
    product_id: str
    market_id: str
    market_name: str
    category_id: str
    category_name: str
    title: str
    main_image_url: str
    price: Optional[float]
    currency: str
    price_rmb: Optional[float]
    sales_7d: Optional[float]
    sales_30d: Optional[float]
    video_count: Optional[float]
    creator_count: Optional[float]
    listing_datetime: str
    product_age_days: Optional[int]
    age_bucket: str
    fastmoss_url: str
    source: str
    data_status: str
    is_valid: bool
    data_quality_flags: List[str] = field(default_factory=list)
    shop_name: str = ""
    platform_product_url: str = ""
    image_urls: List[str] = field(default_factory=list)
    comments_count: Optional[float] = None
    total_sales: Optional[float] = None
    product_tags: List[str] = field(default_factory=list)
    manual_notes: str = ""
    source_record_id: str = ""
    raw_fields: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def skip_reason(self) -> str:
        if self.data_status != READY_STATUS:
            return "data_status_not_ready"
        if not self.is_valid:
            return "is_valid_false"
        if not self.market_id:
            return "market_id_missing"
        if not self.category_id:
            return "category_id_missing"
        if not self.crawl_batch_id:
            return "crawl_batch_id_missing"
        return ""

    @property
    def ready_for_agents(self) -> bool:
        return self.skip_reason() == ""


@dataclass
class StandardizedSnapshotLoadResult:
    snapshots: List[StandardizedProductSnapshot]
    skipped: List[Dict[str, str]]
    source_row_count: int


def load_standard_product_snapshots_from_feishu(
    client: Any,
    crawl_batch_id: str = "",
    market_id: str = "",
    category_id: str = "",
    limit: Optional[int] = None,
) -> StandardizedSnapshotLoadResult:
    records = client.list_records(limit=limit)
    snapshots: List[StandardizedProductSnapshot] = []
    skipped: List[Dict[str, str]] = []
    for record in records:
        snapshot = snapshot_from_fields(dict(record.fields), source_record_id=str(record.record_id or ""))
        reason = snapshot.skip_reason()
        if crawl_batch_id and snapshot.crawl_batch_id != crawl_batch_id:
            reason = "crawl_batch_id_not_requested"
        if market_id and snapshot.market_id != market_id:
            reason = "market_id_not_requested"
        if category_id and snapshot.category_id != category_id:
            reason = "category_id_not_requested"
        if reason:
            skipped.append({"record_id": snapshot.source_record_id, "reason": reason})
            continue
        snapshots.append(snapshot)
    return StandardizedSnapshotLoadResult(snapshots=snapshots, skipped=skipped, source_row_count=len(records))


def snapshot_from_fields(fields: Dict[str, Any], source_record_id: str = "") -> StandardizedProductSnapshot:
    def value(key: str) -> Any:
        for name in FIELD_ALIASES.get(key, [key]):
            if name in fields and fields.get(name) not in (None, "", []):
                return fields.get(name)
        return None

    product_id = _text(value("product_id"))
    snapshot_id = _text(value("product_snapshot_id")) or product_id or source_record_id
    return StandardizedProductSnapshot(
        crawl_batch_id=_text(value("crawl_batch_id")),
        product_snapshot_id=snapshot_id,
        product_id=product_id or snapshot_id,
        market_id=_text(value("market_id")).upper(),
        market_name=_text(value("market_name")),
        category_id=_text(value("category_id")),
        category_name=_text(value("category_name")),
        title=_text(value("title")),
        main_image_url=_first_url(value("main_image_url")),
        price=_number(value("price")),
        currency=_text(value("currency")),
        price_rmb=_number(value("price_rmb")),
        sales_7d=_number(value("sales_7d")),
        sales_30d=_number(value("sales_30d")),
        video_count=_number(value("video_count")),
        creator_count=_number(value("creator_count")),
        listing_datetime=_text(value("listing_datetime")),
        product_age_days=_int_or_none(value("product_age_days")),
        age_bucket=_text(value("age_bucket")),
        fastmoss_url=_first_url(value("fastmoss_url")),
        source=_text(value("source")),
        data_status=_text(value("data_status")),
        is_valid=_bool(value("is_valid")),
        data_quality_flags=_list_text(value("data_quality_flags")),
        shop_name=_text(value("shop_name")),
        platform_product_url=_first_url(value("platform_product_url")),
        image_urls=_list_text(value("image_urls")),
        comments_count=_number(value("comments_count")),
        total_sales=_number(value("total_sales")),
        product_tags=_list_text(value("product_tags")),
        manual_notes=_text(value("manual_notes")),
        source_record_id=source_record_id,
        raw_fields=fields,
    )


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        if not value:
            return ""
        first = value[0]
        if isinstance(first, dict):
            return str(first.get("text") or first.get("name") or first.get("url") or "").strip()
        return str(first or "").strip()
    if isinstance(value, dict):
        return str(value.get("text") or value.get("name") or value.get("url") or value.get("link") or "").strip()
    return str(value).strip()


def _first_url(value: Any) -> str:
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                candidate = str(item.get("url") or item.get("link") or item.get("tmp_url") or "").strip()
            else:
                candidate = str(item or "").strip()
            if candidate:
                return candidate
        return ""
    if isinstance(value, dict):
        return str(value.get("url") or value.get("link") or value.get("tmp_url") or "").strip()
    return _text(value)


def _number(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    try:
        return float(str(value).replace(",", "").replace("%", "").strip())
    except (TypeError, ValueError):
        return None


def _int_or_none(value: Any) -> Optional[int]:
    number = _number(value)
    return int(number) if number is not None else None


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = _text(value).lower()
    return text in {"true", "1", "yes", "y", "有效", "是", "勾选"}


def _list_text(value: Any) -> List[str]:
    if value is None or value == "":
        return []
    if isinstance(value, list):
        return [item for item in (_text(v) for v in value) if item]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        if text.startswith("["):
            try:
                parsed = json.loads(text)
                return _list_text(parsed)
            except ValueError:
                pass
        return [item.strip() for item in text.replace("，", ",").split(",") if item.strip()]
    return [_text(value)] if _text(value) else []
