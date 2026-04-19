#!/usr/bin/env python3
"""
直接测试 API 请求 - 完全模拟浏览器
"""

import sys
import json
import requests
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from inventory_api import InventoryAPI


def test_direct_request():
    """直接发送请求，完全模拟浏览器"""
    api = InventoryAPI()
    
    url = "https://www.bigseller.pro/api/v1/inventory/pageList.json"
    
    # 完全按照浏览器的格式
    payload = {
        "pageNo": 1,
        "pageSize": 50,
        "searchType": "skuName",
        "searchContent": "SL002",
        "inquireType": 0,
        "stockStatus": "",
        "isGroup": "",
        "orderBy": "desc",
        "fullCid": "",
        "queryDistribution": 1,
        "saleState": "",
        "zoneId": "",
        "openFlag": False,
        "hideZeroInventorySku": 0,
        "warehouseIds": [54952]
    }
    
    print("发送请求...")
    print(f"URL: {url}")
    print(f"Payload: {json.dumps(payload, indent=2, ensure_ascii=False)}")
    print()
    
    response = api.session.post(url, json=payload, timeout=10)
    
    print(f"响应状态: {response.status_code}")
    print()
    
    data = response.json()
    
    # 检查返回的数据
    rows = data.get('data', {}).get('page', {}).get('rows', [])
    total_size = data.get('data', {}).get('page', {}).get('totalSize', 0)
    
    print(f"totalSize: {total_size}")
    print(f"返回记录数: {len(rows)}")
    print()
    
    if rows:
        print("返回的 SKU:")
        for i, row in enumerate(rows, 1):
            sku = row.get('sku')
            available = row.get('available')
            print(f"  {i}. {sku} - 可用: {available}")
    else:
        print("没有返回任何记录")
    
    # 保存完整响应
    with open('debug_response_SL002.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print("\n完整响应已保存到 debug_response_SL002.json")


if __name__ == "__main__":
    test_direct_request()
