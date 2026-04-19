#!/usr/bin/env python3
"""
详细调试 - 跟踪 SKU 参数传递
"""

import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from inventory_api import InventoryAPI


def trace_query(sku: str):
    """跟踪查询过程"""
    print(f"=" * 80)
    print(f"跟踪查询: {sku}")
    print(f"=" * 80)
    
    api = InventoryAPI()
    
    # 1. 检查配置
    print(f"\n1️⃣ 配置检查:")
    payload_template = api.config['api'].get('payload_template', {})
    print(f"   payload_template: {json.dumps(payload_template, indent=4, ensure_ascii=False)}")
    
    # 2. 构建 payload
    print(f"\n2️⃣ 构建 payload:")
    payload = api._build_payload(sku, 'thailand')
    print(f"   实际 payload: {json.dumps(payload, indent=4, ensure_ascii=False)}")
    
    # 3. 发送请求
    print(f"\n3️⃣ 发送请求:")
    api_config = api.config['api']
    url = api_config['base_url'] + api_config['endpoints']['query']
    print(f"   URL: {url}")
    print(f"   Method: POST")
    
    response = api.session.post(url, json=payload, timeout=api.timeout)
    print(f"   响应状态: {response.status_code}")
    
    # 4. 解析响应
    print(f"\n4️⃣ 解析响应:")
    response_data = response.json()
    rows = response_data.get('data', {}).get('page', {}).get('rows', [])
    print(f"   返回记录数: {len(rows)}")
    
    if rows:
        print(f"   前 5 个 SKU:")
        for i, row in enumerate(rows[:5], 1):
            print(f"      {i}. {row.get('sku')}")
        
        # 5. 匹配检查
        print(f"\n5️⃣ 匹配检查:")
        print(f"   查询的 SKU: '{sku}'")
        print(f"   SKU (lower): '{sku.lower()}'")
        
        sku_lower = sku.lower()
        matched = []
        for row in rows:
            row_sku = row.get('sku', '')
            if sku_lower in row_sku.lower():
                matched.append(row_sku)
                print(f"   ✅ 匹配: '{row_sku}' (包含 '{sku}')")
        
        if not matched:
            print(f"   ❌ 没有找到包含 '{sku}' 的 SKU")
            print(f"\n   💡 可能的原因:")
            print(f"      1. API 的 searchContent 参数没有起作用")
            print(f"      2. API 返回的是固定的前 50 条记录，而不是根据搜索内容过滤")
            print(f"      3. SKU '{sku}' 在系统中不存在")
    
    print(f"\n" + "=" * 80)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python trace_query.py SKU")
        print("示例: python trace_query.py SL002")
        sys.exit(1)
    
    sku = sys.argv[1]
    trace_query(sku)
