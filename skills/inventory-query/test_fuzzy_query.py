#!/usr/bin/env python3
"""测试模糊查询功能"""
import sys
import json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from inventory_api import query_inventory

print("=" * 60)
print("测试模糊查询功能")
print("=" * 60)

# 测试 SKU
test_sku = "wj"

print(f"\n查询 SKU: {test_sku}")
print(f"使用模糊匹配（fuzzy=True）")
print("-" * 60)

try:
    results = query_inventory([test_sku], fuzzy=True)
    result = results[test_sku]
    
    print(f"\n查询结果:")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    
    print(f"\n" + "=" * 60)
    print("结果分析:")
    print("=" * 60)
    
    if 'error' in result:
        print(f"\n✗ 查询失败: {result['error']}")
        print("\n可能的原因:")
        print("  1. Cookie 已过期，需要重新登录")
        print("  2. SKU 不存在于系统中")
        print("  3. API 认证失败")
    elif 'matched_count' in result:
        print(f"\n✓ 模糊匹配成功！")
        print(f"  找到 {result['matched_count']} 个匹配的 SKU:")
        for i, sku in enumerate(result['matched_skus'], 1):
            print(f"    {i}. {sku}")
        print(f"\n  总库存统计:")
        print(f"    可用库存: {result['available']}")
        print(f"    总库存: {result['total']}")
        print(f"    仓库锁定待发库存: {result['reserved']}")
    else:
        print(f"\n✓ 精确匹配成功！")
        print(f"  SKU: {result['sku']}")
        print(f"  可用库存: {result['available']}")
        print(f"  总库存: {result['total']}")
        print(f"  仓库锁定待发库存: {result['reserved']}")
        print(f"  状态: {result['status']}")
    
except Exception as e:
    print(f"\n✗ 测试失败: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 60)
