#!/usr/bin/env python3
"""飞书多维表格 URL 解析。"""

import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import parse_qs, urlparse


@dataclass
class FeishuBitableInfo:
    app_token: str
    table_id: str
    view_id: Optional[str] = None
    original_url: str = ""


def parse_feishu_bitable_url(url: str) -> Optional[FeishuBitableInfo]:
    if not url:
        return None

    cleaned = url.strip()

    match = re.search(r"/base/([a-zA-Z0-9]+)", cleaned)
    if match:
        params = parse_qs(urlparse(cleaned).query)
        table_id = params.get("table", [None])[0]
        view_id = params.get("view", [None])[0]
        if table_id:
            return FeishuBitableInfo(
                app_token=match.group(1),
                table_id=table_id,
                view_id=view_id,
                original_url=cleaned,
            )

    match = re.search(r"/wiki/([a-zA-Z0-9]+)", cleaned)
    if match:
        params = parse_qs(urlparse(cleaned).query)
        table_id = params.get("table", [None])[0]
        view_id = params.get("view", [None])[0]
        if table_id:
            return FeishuBitableInfo(
                app_token=match.group(1),
                table_id=table_id,
                view_id=view_id,
                original_url=cleaned,
            )

    match = re.search(r"/apps/([a-zA-Z0-9]+)/tables/([a-zA-Z0-9]+)", cleaned)
    if match:
        return FeishuBitableInfo(
            app_token=match.group(1),
            table_id=match.group(2),
            original_url=cleaned,
        )

    return None

