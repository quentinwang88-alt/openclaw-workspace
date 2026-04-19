#!/usr/bin/env python3
"""监控有效达人过滤规则。"""

from __future__ import annotations

from typing import Dict, Optional

from app.config import Settings, get_settings


def is_valid_creator_for_monitoring(
    row: Dict[str, object],
    settings: Optional[Settings] = None,
) -> bool:
    settings = settings or get_settings()
    gmv = float(row.get("gmv") or 0)
    order_count = int(row.get("order_count") or 0)
    content_action_count = int(row.get("content_action_count") or 0)
    shipped_sample_count = int(row.get("shipped_sample_count") or 0)

    return any(
        [
            gmv >= settings.min_valid_gmv,
            order_count >= settings.min_valid_order_count,
            content_action_count >= settings.min_valid_content_action_count,
            shipped_sample_count >= settings.min_valid_sample_count,
        ]
    )
