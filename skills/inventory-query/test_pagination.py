#!/usr/bin/env python3
"""
测试分页查询
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from inventory_api import InventoryAPI


def test_pagination():
    """测试分页"""
    api = InventoryAPI()
    
    print("测试分页查询")
    print("=" * 80)
    
    for page in range(1, 4):
        print(f"\n📄 第 {page} 页:")
        
        payload = api._build_payload("test", "thailand", page_no=page)
        url = api.config['api']['base_url'] + api.config['api']['endpoints']['query']
        
        response = api.session.post(url, json=payload, timeout=api.timeout)
        data = response.json()
        
        rows = data.get('data', {}).get('page', {}).get('rows', [])
        print(f"   返回记录数: {len(rows)}")
        
        if rows:
            print(f"   前 5 个 SKU:")
            for i, row in enumerate(rows[:5], 1):
                print(f"      {i}. {row.get('sku')}")
        
        # 检查是否有不同的数据
        if page == 1:
            first_page_skus = [row.get('sku') for row in rows]
        else:
            current_skus = [row.get('sku') for row in rows]
            if current_skus == first_page_skus:
                print(f"   ⚠️  与第 1 页数据相同！")
            else:
                print(f"   ✅ 数据不同，分页有效")


if __name__ == "__main__":
    test_pagination()
