#!/usr/bin/env python3
"""
飞书 URL 解析工具。

支持解析飞书多维表格链接，提取 app_token 和 table_id。
"""

import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import parse_qs, urlparse


@dataclass
class FeishuBitableInfo:
    """飞书多维表格信息。"""

    app_token: str
    table_id: str
    view_id: Optional[str] = None
    original_url: str = ""


def parse_feishu_bitable_url(url: str) -> Optional[FeishuBitableInfo]:
    """解析飞书多维表格 URL。"""
    if not url:
        return None

    url = url.strip()

    match = re.search(r"/base/([a-zA-Z0-9]+)", url)
    if match:
        app_token = match.group(1)
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        table_id = params.get("table", [None])[0]
        view_id = params.get("view", [None])[0]
        if table_id:
            return FeishuBitableInfo(
                app_token=app_token,
                table_id=table_id,
                view_id=view_id,
                original_url=url,
            )

    match = re.search(r"/wiki/([a-zA-Z0-9]+)", url)
    if match:
        wiki_id = match.group(1)
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        table_id = params.get("table", [None])[0]
        view_id = params.get("view", [None])[0]
        if table_id:
            return FeishuBitableInfo(
                app_token=wiki_id,
                table_id=table_id,
                view_id=view_id,
                original_url=url,
            )

    match = re.search(r"/apps/([a-zA-Z0-9]+)/tables/([a-zA-Z0-9]+)", url)
    if match:
        return FeishuBitableInfo(
            app_token=match.group(1),
            table_id=match.group(2),
            original_url=url,
        )

    match = re.match(r"^([a-zA-Z0-9]+)[/,]([a-zA-Z0-9]+)$", url)
    if match:
        return FeishuBitableInfo(
            app_token=match.group(1),
            table_id=match.group(2),
            original_url=url,
        )

    return None
