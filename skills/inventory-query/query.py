#!/usr/bin/env python3
"""
快速查询库存 - OpenClaw 便捷接口
用法: python query.py SKU1 SKU2 SKU3 ...
"""

import sys
from pathlib import Path

# 添加当前目录到 Python 路径
sys.path.insert(0, str(Path(__file__).parent))

from inventory_api import query_inventory


def main():
    args = sys.argv[1:]
    profile = None

    if args[:2] and args[0] == "--profile":
        if len(args) < 3:
            print("用法: python query.py --profile 店铺A SKU1 [SKU2 SKU3 ...]")
            sys.exit(1)
        profile = args[1]
        args = args[2:]

    if len(args) < 1:
        print("用法: python query.py [--profile 店铺A] SKU1 [SKU2 SKU3 ...]")
        print("\n示例:")
        print("  python query.py SL002")
        print("  python query.py --profile 店铺A SL002")
        print("  python query.py wj MWJ-BP bu0020")
        sys.exit(1)
    
    skus = args
    print(f"查询 SKU: {', '.join(skus)}")
    if profile:
        print(f"使用 profile: {profile}")
    print("=" * 60)
    
    results = query_inventory(skus, profile=profile)
    
    for sku, data in results.items():
        print(f"\n【{sku}】")
        if 'error' in data:
            print(f"  ❌ {data['error']}")
        else:
            print(f"  ✅ 可用库存: {data['available']}")
            print(f"  📦 总库存: {data['total']}")
            print(f"  🔒 仓库锁定待发库存: {data['reserved']}")
            if data.get('title'):
                print(f"  🏷️ 前台名称: {data['title']}")
            if data.get('matched_by') == 'title':
                print(f"  🔁 匹配方式: 前台名称回退匹配")
            if data.get('fallback_used'):
                print(f"  🔎 原始查询: {data.get('query_input', sku)}")
                if data.get('fallback_type') == 'alias_map':
                    print(f"  🗂️ 回退方式: 本地别名映射")
                    print(f"  🔗 映射库存编码: {data.get('alias_sku', data.get('sku', sku))}")
                else:
                    print(f"  🔗 实际库存编码: {data.get('sku', sku)}")
            if 'matched_count' in data and data['matched_count'] > 1:
                print(f"  🔍 匹配了 {data['matched_count']} 个 SKU:")
                for matched_sku in data.get('matched_skus', []):
                    print(f"     - {matched_sku}")


if __name__ == "__main__":
    main()
