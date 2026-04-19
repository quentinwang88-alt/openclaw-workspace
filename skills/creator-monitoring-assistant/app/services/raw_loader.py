#!/usr/bin/env python3
"""原始数据入库。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional
from uuid import uuid4

from app.db import Database
from app.services.excel_reader import read_creator_weekly_excel
from app.utils.hash_utils import sha256_text


def load_raw_excel(
    stat_week: str,
    source_file_path: str,
    platform: str,
    country: str,
    store: str = "",
    db: Optional[Database] = None,
) -> str:
    database = db or Database()
    rows = read_creator_weekly_excel(source_file_path)
    import_batch_id = f"{stat_week}:{uuid4().hex[:10]}"
    source_file_name = Path(source_file_path).name

    payloads = []
    for row in rows:
        creator_name = str(row.get("达人名称", "")).strip()
        if not creator_name:
            continue
        serialized = json.dumps(row, ensure_ascii=False, sort_keys=True)
        payloads.append(
            {
                "import_batch_id": import_batch_id,
                "stat_week": stat_week,
                "source_file_name": source_file_name,
                "creator_name_raw": creator_name,
                "platform": platform,
                "country": country,
                "store": store,
                "gmv_raw": str(row.get("联盟归因 GMV", "")),
                "refund_amount_raw": str(row.get("退款金额", "")),
                "order_count_raw": str(row.get("归因订单数", "")),
                "sold_item_count_raw": str(row.get("联盟归因成交件数", "")),
                "refunded_item_count_raw": str(row.get("已退款的商品件数", "")),
                "avg_order_value_raw": str(row.get("平均订单金额", "")),
                "avg_daily_sold_item_count_raw": str(row.get("日均商品成交件数", "")),
                "video_count_raw": str(row.get("视频数", "")),
                "live_count_raw": str(row.get("直播数", "")),
                "estimated_commission_raw": str(row.get("预计佣金", "")),
                "shipped_sample_count_raw": str(row.get("已发货样品数", "")),
                "row_hash": sha256_text(serialized),
            }
        )

    database.executemany(
        """
        INSERT INTO creator_weekly_raw (
            import_batch_id, stat_week, source_file_name, creator_name_raw,
            platform, country, store, gmv_raw, refund_amount_raw, order_count_raw,
            sold_item_count_raw, refunded_item_count_raw, avg_order_value_raw,
            avg_daily_sold_item_count_raw, video_count_raw, live_count_raw,
            estimated_commission_raw, shipped_sample_count_raw, row_hash
        ) VALUES (
            :import_batch_id, :stat_week, :source_file_name, :creator_name_raw,
            :platform, :country, :store, :gmv_raw, :refund_amount_raw, :order_count_raw,
            :sold_item_count_raw, :refunded_item_count_raw, :avg_order_value_raw,
            :avg_daily_sold_item_count_raw, :video_count_raw, :live_count_raw,
            :estimated_commission_raw, :shipped_sample_count_raw, :row_hash
        )
        """,
        payloads,
    )
    return import_batch_id
