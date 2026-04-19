#!/usr/bin/env python3
"""
调试 API 响应 - 查看实际返回的数据
"""

import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from inventory_api import InventoryAPI


def debug_query(sku: str):
    """调试查询，显示完整的 API 响应"""
    print(f"调试查询 SKU: {sku}")
    print("=" * 80)
    
    api = InventoryAPI()
    
    # 构建请求
    api_config = api.config['api']
    url = api_config['base_url'] + api_config['endpoints']['query']
    payload = api._build_payload(sku, 'thailand')
    
    print(f"\n📤 请求信息:")
    print(f"URL: {url}")
    print(f"Payload: {json.dumps(payload, indent=2, ensure_ascii=False)}")
    
    # 发送请求
    print(f"\n⏳ 发送请求...")
    response = api.session.post(url, json=payload, timeout=api.timeout)
    
    print(f"\n📥 响应状态: {response.status_code}")
    
    # 解析响应
    response_data = response.json()
    print(f"\n📊 响应数据结构:")
    print(f"  - code: {response_data.get('code')}")
    print(f"  - msg: {response_data.get('msg')}")
    
    # 检查数据路径
    data = response_data.get('data', {})
    page = data.get('page', {})
    rows = page.get('rows', [])
    
    print(f"  - data.page.total: {page.get('total', 0)}")
    print(f"  - data.page.rows 数量: {len(rows)}")
    
    if rows:
        print(f"\n📋 返回的所有 SKU 列表:")
        for i, row in enumerate(rows, 1):
            sku_code = row.get('sku', 'N/A')
            available = row.get('available', 0)
            onhand = row.get('onhand', 0)
            print(f"  {i}. {sku_code} - 可用: {available}, 总库存: {onhand}")
        
        # 检查是否有匹配的 SKU
        print(f"\n🔍 查找包含 '{sku}' 的 SKU:")
        sku_lower = sku.lower()
        matched = [row for row in rows if sku_lower in row.get('sku', '').lower()]
        
        if matched:
            print(f"  找到 {len(matched)} 个匹配:")
            for row in matched:
                print(f"    - {row.get('sku')} (可用: {row.get('available')})")
        else:
            print(f"  ❌ 没有找到包含 '{sku}' 的 SKU")
            print(f"\n💡 建议:")
            print(f"  1. 检查 SKU 拼写是否正确")
            print(f"  2. 尝试使用更短的关键词（如 'SL' 而不是 'SL002'）")
            print(f"  3. 查看上面列出的 SKU，确认系统中是否存在该 SKU")
    else:
        print(f"\n❌ API 没有返回任何数据")
    
    print("\n" + "=" * 80)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python debug_response.py SKU")
        print("示例: python debug_response.py SL002")
        sys.exit(1)
    
    sku = sys.argv[1]
    debug_query(sku)
