#!/usr/bin/env python3
"""
飞书 URL 解析工具

支持解析飞书多维表格的 URL，提取 app_token 和 table_id。
用于支持动态传入飞书文档链接。

飞书多维表格 URL 格式示例：
1. https://gcngopvfvo0q.feishu.cn/base/ES8dbWo9FaXmaVs6jA7cgMURnQe?table=tblk1IHpVAvv2nWc&view=vewPrkWWaW
2. https://gcngopvfvo0q.feishu.cn/wiki/JLNQwHpx4imPzfk67ghck4N5n8e?table=tblk1IHpVAvv2nWc&view=vewPrkWWaW
3. https://open.feishu.cn/open-apis/bitable/v1/apps/ES8dbWo9FaXmaVs6jA7cgMURnQe/tables/tblk1IHpVAvv2nWc
"""

import re
from urllib.parse import urlparse, parse_qs
from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass
class FeishuBitableInfo:
    """飞书多维表格信息"""
    app_token: str
    table_id: str
    view_id: Optional[str] = None
    original_url: str = ""
    
    def __str__(self) -> str:
        return f"FeishuBitableInfo(app_token={self.app_token[:8]}..., table_id={self.table_id})"


def parse_feishu_bitable_url(url: str) -> Optional[FeishuBitableInfo]:
    """
    解析飞书多维表格 URL
    
    支持的 URL 格式：
    - https://{domain}.feishu.cn/base/{app_token}?table={table_id}&view={view_id}
    - https://{domain}.feishu.cn/wiki/{wiki_id}?table={table_id}&view={view_id}
    - https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}
    
    Args:
        url: 飞书多维表格 URL
    
    Returns:
        FeishuBitableInfo 或 None（解析失败）
    """
    if not url:
        return None
    
    url = url.strip()
    
    # 尝试不同的 URL 格式
    
    # 格式1: /base/{app_token}?table={table_id}
    # 例如: https://gcngopvfvo0q.feishu.cn/base/ES8dbWo9FaXmaVs6jA7cgMURnQe?table=tblk1IHpVAvv2nWc
    match = re.search(r'/base/([a-zA-Z0-9]+)', url)
    if match:
        app_token = match.group(1)
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        table_id = params.get('table', [None])[0]
        view_id = params.get('view', [None])[0]
        
        if table_id:
            return FeishuBitableInfo(
                app_token=app_token,
                table_id=table_id,
                view_id=view_id,
                original_url=url
            )
    
    # 格式2: /wiki/{wiki_id}?table={table_id}
    # 例如: https://gcngopvfvo0q.feishu.cn/wiki/JLNQwHpx4imPzfk67ghck4N5n8e?table=tblk1IHpVAvv2nWc
    match = re.search(r'/wiki/([a-zA-Z0-9]+)', url)
    if match:
        wiki_id = match.group(1)
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        table_id = params.get('table', [None])[0]
        view_id = params.get('view', [None])[0]
        
        if table_id:
            # wiki 格式中，wiki_id 实际上就是 app_token
            return FeishuBitableInfo(
                app_token=wiki_id,
                table_id=table_id,
                view_id=view_id,
                original_url=url
            )
    
    # 格式3: API URL /apps/{app_token}/tables/{table_id}
    # 例如: https://open.feishu.cn/open-apis/bitable/v1/apps/ES8dbWo9FaXmaVs6jA7cgMURnQe/tables/tblk1IHpVAvv2nWc
    match = re.search(r'/apps/([a-zA-Z0-9]+)/tables/([a-zA-Z0-9]+)', url)
    if match:
        return FeishuBitableInfo(
            app_token=match.group(1),
            table_id=match.group(2),
            original_url=url
        )
    
    # 格式4: 直接传入 app_token 和 table_id（用 / 或 , 分隔）
    # 例如: ES8dbWo9FaXmaVs6jA7cgMURnQe/tblk1IHpVAvv2nWc
    match = re.match(r'^([a-zA-Z0-9]+)[/,]([a-zA-Z0-9]+)$', url)
    if match:
        return FeishuBitableInfo(
            app_token=match.group(1),
            table_id=match.group(2),
            original_url=url
        )
    
    return None


def extract_from_feishu_url(url: str) -> Tuple[Optional[str], Optional[str]]:
    """
    从飞书 URL 中提取 app_token 和 table_id
    
    简化的接口，返回元组 (app_token, table_id)
    
    Args:
        url: 飞书多维表格 URL
    
    Returns:
        (app_token, table_id) 元组，解析失败返回 (None, None)
    """
    info = parse_feishu_bitable_url(url)
    if info:
        return (info.app_token, info.table_id)
    return (None, None)


def test_parse():
    """测试 URL 解析"""
    test_urls = [
        # 标准格式
        "https://gcngopvfvo0q.feishu.cn/base/ES8dbWo9FaXmaVs6jA7cgMURnQe?table=tblk1IHpVAvv2nWc&view=vewPrkWWaW",
        "https://gcngopvfvo0q.feishu.cn/wiki/JLNQwHpx4imPzfk67ghck4N5n8e?table=tblk1IHpVAvv2nWc",
        # API 格式
        "https://open.feishu.cn/open-apis/bitable/v1/apps/ES8dbWo9FaXmaVs6jA7cgMURnQe/tables/tblk1IHpVAvv2nWc",
        # 简化格式
        "ES8dbWo9FaXmaVs6jA7cgMURnQe/tblk1IHpVAvv2nWc",
        "ES8dbWo9FaXmaVs6jA7cgMURnQe,tblk1IHpVAvv2nWc",
        # 无效格式
        "https://example.com/invalid",
        "",
    ]
    
    print("=" * 70)
    print("🧪 飞书 URL 解析测试")
    print("=" * 70)
    
    for url in test_urls:
        info = parse_feishu_bitable_url(url)
        if info:
            print(f"\n✅ 成功解析:")
            print(f"   URL: {url[:50]}{'...' if len(url) > 50 else ''}")
            print(f"   app_token: {info.app_token}")
            print(f"   table_id: {info.table_id}")
            if info.view_id:
                print(f"   view_id: {info.view_id}")
        else:
            print(f"\n❌ 解析失败: {url[:50]}{'...' if len(url) > 50 else ''}")


if __name__ == "__main__":
    test_parse()