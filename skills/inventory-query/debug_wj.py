#!/usr/bin/env python3
"""调试 wj SKU 的 API 响应"""
import sys
import json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from inventory_api import InventoryAPI

api = InventoryAPI()
sku = "wj"

print(f"测试查询 SKU: {sku}")
print("=" * 60)

# 构建请求
url = api.config['api']['base_url'] + api.config['api']['endpoints']['query']
payload = api._build_payload(sku, "thailand")

print(f"\nRequest URL: {url}")
print(f"Request Method: {api.config['api']['method']}")
print(f"\nPayload:")
print(json.dumps(payload, ensure_ascii=False, indent=2))

# 发送请求
try:
    response = api.session.post(url, json=payload, timeout=api.timeout)
    print(f"\nResponse Status: {response.status_code}")
    print(f"\nResponse Data:")
    response_data = response.json()
    print(json.dumps(response_data, ensure_ascii=False, indent=2))
    
    # 分析响应
    print("\n" + "=" * 60)
    print("响应分析:")
    print("=" * 60)
    
    # 检查状态码
    status = response_data.get('code')
    print(f"\n状态码 (code): {status}")
    
    if status != 0:
        msg = response_data.get('msg', '未知错误')
        print(f"错误信息 (msg): {msg}")
        print(f"\n⚠️  API 返回非成功状态码")
    
    # 检查数据路径
    data = response_data.get('data', {})
    print(f"\ndata 字段存在: {bool(data)}")
    
    if data:
        page = data.get('page', {})
        print(f"data.page 字段存在: {bool(page)}")
        
        if page:
            rows = page.get('rows', [])
            print(f"data.page.rows 数组长度: {len(rows)}")
            
            if rows:
                print(f"\n找到 {len(rows)} 条记录:")
                for i, row in enumerate(rows):
                    row_sku = row.get('sku', 'N/A')
                    available = row.get('available', 0)
                    print(f"  [{i}] SKU: {row_sku}, 可用库存: {available}")
                    
                # 检查是否有匹配的 SKU
                matched = [r for r in rows if r.get('sku', '').lower() == sku.lower()]
                if matched:
                    print(f"\n✓ 找到匹配的 SKU: {sku}")
                    print(json.dumps(matched[0], ensure_ascii=False, indent=2))
                else:
                    print(f"\n✗ 未找到匹配的 SKU: {sku}")
            else:
                print("\n✗ rows 数组为空")
        else:
            print("\n✗ page 字段不存在或为空")
    else:
        print("\n✗ data 字段不存在或为空")
    
    # 测试解析函数
    print("\n" + "=" * 60)
    print("测试解析函数:")
    print("=" * 60)
    result = api._parse_response(response_data, sku)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    
except Exception as e:
    print(f"\n✗ 请求失败: {e}")
    import traceback
    traceback.print_exc()
