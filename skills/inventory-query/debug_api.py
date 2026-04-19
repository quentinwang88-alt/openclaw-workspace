#!/usr/bin/env python3
"""调试 API 响应"""
import sys
import json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from inventory_api import InventoryAPI

api = InventoryAPI()
print("测试查询 SKU: BU0011")
print("=" * 60)

# 构建请求
url = api.config['api']['base_url'] + api.config['api']['endpoints']['query']
payload = api._build_payload("BU0011", "thailand")

print(f"\nRequest URL: {url}")
print(f"Request Method: {api.config['api']['method']}")
print(f"\nPayload:")
print(json.dumps(payload, ensure_ascii=False, indent=2))

# 发送请求
response = api.session.post(url, json=payload, timeout=api.timeout)
print(f"\nResponse Status: {response.status_code}")
print(f"\nResponse Data:")
print(json.dumps(response.json(), ensure_ascii=False, indent=2))
