#!/usr/bin/env python3
"""对比测试：已知 SKU vs 不存在的 SKU"""
import sys
import json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from inventory_api import InventoryAPI

api = InventoryAPI()

# 测试两个 SKU
test_cases = [
    ("BU0010", "已知存在的 SKU"),
    ("wj", "可能不存在的 SKU"),
]

for sku, description in test_cases:
    print(f"\n{'=' * 60}")
    print(f"测试: {description} - {sku}")
    print('=' * 60)
    
    url = api.config['api']['base_url'] + api.config['api']['endpoints']['query']
    payload = api._build_payload(sku, "thailand")
    
    try:
        response = api.session.post(url, json=payload, timeout=api.timeout)
        response_data = response.json()
        
        code = response_data.get('code')
        msg = response_data.get('msg', '')
        data = response_data.get('data')
        
        print(f"\n状态码: {code}")
        print(f"错误信息: '{msg}'")
        print(f"data 是否为 null: {data is None}")
        
        if data:
            page = data.get('page', {})
            rows = page.get('rows', [])
            print(f"返回记录数: {len(rows)}")
            
            if rows:
                for row in rows:
                    print(f"  - SKU: {row.get('sku')}, 可用: {row.get('available')}")
        
        # 测试解析结果
        result = api._parse_response(response_data, sku)
        print(f"\n解析结果:")
        if 'error' in result:
            print(f"  ✗ 错误: {result['error']}")
        else:
            print(f"  ✓ 可用库存: {result['available']}")
            
    except Exception as e:
        print(f"\n✗ 请求失败: {e}")

print(f"\n{'=' * 60}")
print("结论:")
print('=' * 60)
print("""
如果 BU0010 返回 code=0 且有数据，而 wj 返回 code=2001 且 data=null，
说明 'wj' 这个 SKU 在系统中不存在或者搜索条件不匹配。

可能的原因:
1. SKU 'wj' 不存在于库存系统中
2. 搜索类型 'skuName' 需要更精确的匹配
3. 该 SKU 已被删除或归档
""")
