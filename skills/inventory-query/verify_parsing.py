#!/usr/bin/env python3
"""验证代码能否正确解析成功的响应"""
import sys
import json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from inventory_api import InventoryAPI

# 用户提供的成功响应示例
success_response = {
    "code": 0,
    "errorType": 0,
    "msg": "操作成功",
    "msgObjStr": "",
    "data": {
        "currency": "THB",
        "viewCostPremission": False,
        "page": {
            "pageNo": 1,
            "pageToken": None,
            "pageSize": 50,
            "totalPage": 1,
            "totalSize": 1,
            "rows": [
                {
                    "sku": "BU0010",
                    "warehouseName": "Default Warehouse",
                    "title": "金色钉子手镯",
                    "onhand": 112,
                    "allocated": 0,
                    "available": 112,
                    "saleState": 1
                }
            ]
        }
    }
}

print("=" * 60)
print("测试解析成功的 API 响应")
print("=" * 60)

api = InventoryAPI()

# 测试解析
result = api._parse_response(success_response, "BU0010", fuzzy=True)

print("\n解析结果:")
print(json.dumps(result, ensure_ascii=False, indent=2))

print("\n" + "=" * 60)
print("验证:")
print("=" * 60)

if 'error' in result:
    print(f"✗ 解析失败: {result['error']}")
else:
    print(f"✓ 解析成功！")
    print(f"  SKU: {result['sku']}")
    print(f"  可用库存: {result['available']}")
    print(f"  总库存: {result['total']}")
    print(f"  预留: {result['reserved']}")
    print(f"  状态: {result['status']}")

# 测试模糊查询（查询 "bu" 应该匹配到 "BU0010"）
print("\n" + "=" * 60)
print("测试模糊匹配:")
print("=" * 60)

result_fuzzy = api._parse_response(success_response, "bu", fuzzy=True)
print(f"\n查询 'bu' (模糊匹配):")
print(json.dumps(result_fuzzy, ensure_ascii=False, indent=2))

# 测试精确查询（查询 "bu" 不应该匹配到 "BU0010"）
result_exact = api._parse_response(success_response, "bu", fuzzy=False)
print(f"\n查询 'bu' (精确匹配):")
print(json.dumps(result_exact, ensure_ascii=False, indent=2))
