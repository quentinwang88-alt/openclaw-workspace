#!/usr/bin/env python3
"""
测试库存查询 API
"""

import sys
from pathlib import Path

# 添加当前目录到 Python 路径
sys.path.insert(0, str(Path(__file__).parent))

from inventory_api import InventoryAPI, query_inventory


def test_connection():
    """测试 API 连接"""
    print("=" * 60)
    print("库存查询 API 连接测试")
    print("=" * 60)
    
    try:
        api = InventoryAPI()
        print("\n✓ 配置文件加载成功")
        print(f"  API 地址: {api.config['api']['base_url']}")
        print(f"  查询端点: {api.config['api']['endpoints']['query']}")
        
        # 测试查询
        test_sku = api.config.get('test_sku', 'bu0010')
        print(f"\n正在测试查询 SKU: {test_sku}")
        
        result = api.query_single(test_sku)
        
        print("\n查询结果:")
        print("-" * 60)
        if 'error' in result:
            print(f"⚠️  返回错误: {result['error']}")
            print("\n这可能是正常的（如果测试 SKU 不存在）")
            print("只要能连接到 API 并返回响应就说明配置正确")
        else:
            print(f"✓ 查询成功！")
            print(f"  SKU: {result['sku']}")
            print(f"  可用库存: {result['available']}")
            print(f"  总库存: {result['total']}")
            print(f"  预留: {result['reserved']}")
            print(f"  状态: {result['status']}")
        
        print("\n" + "=" * 60)
        print("✓ API 连接测试完成")
        print("=" * 60)
        return True
        
    except FileNotFoundError as e:
        print(f"\n✗ {e}")
        print("\n请先配置 API:")
        print("  1. 复制 config/api_config.example.json 为 config/api_config.json")
        print("  2. 按照 docs/API_DISCOVERY_GUIDE.md 找到 API 端点")
        print("  3. 更新配置文件中的 API 信息")
        print("  4. 重新运行此测试")
        return False
        
    except Exception as e:
        print(f"\n✗ 测试失败: {e}")
        print("\n可能的原因:")
        print("  1. API 地址或端点配置错误")
        print("  2. Authorization Token 无效或过期")
        print("  3. 网络连接问题")
        print("  4. API 响应格式与配置不匹配")
        print("\n请检查配置文件并参考 docs/API_QUICKSTART.md")
        
        import traceback
        print("\n详细错误信息:")
        traceback.print_exc()
        return False


def test_multiple_query(skus=None):
    """测试批量查询"""
    print("\n" + "=" * 60)
    print("批量查询测试")
    print("=" * 60)
    
    try:
        if skus is None:
            test_skus = ["bu0010", "wj", "bu0020"]
        else:
            test_skus = skus
            
        print(f"\n测试查询 {len(test_skus)} 个 SKU: {', '.join(test_skus)}")
        
        import time
        start_time = time.time()
        
        results = query_inventory(test_skus)
        
        elapsed_time = time.time() - start_time
        
        print(f"\n查询完成，耗时: {elapsed_time:.2f} 秒")
        print(f"平均每个 SKU: {elapsed_time/len(test_skus):.2f} 秒")
        
        print("\n结果:")
        print("-" * 60)
        for sku, data in results.items():
            if 'error' in data:
                print(f"  {sku}: ❌ {data['error']}")
            else:
                status_info = f"可用 {data['available']}"
                if 'matched_count' in data:
                    status_info += f" (匹配 {data['matched_count']} 个 SKU)"
                print(f"  {sku}: ✓ {status_info}")
        
        print("\n✓ 批量查询测试完成")
        return True
        
    except Exception as e:
        print(f"\n✗ 批量查询测试失败: {e}")
        return False


if __name__ == "__main__":
    success = True
    
    # 检查命令行参数
    if len(sys.argv) > 1:
        # 如果提供了命令行参数，直接使用这些 SKU 进行批量查询
        test_skus = sys.argv[1:]
        print(f"使用命令行参数进行查询: {', '.join(test_skus)}")
        success = test_multiple_query(test_skus)
    else:
        # 否则运行标准测试流程
        print("提示: 可以通过命令行参数指定 SKU，例如:")
        print("  python test_api.py wj bu0020 TH-DR-8801\n")
        
        success = test_connection()
        
        if success:
            print("\n是否继续测试批量查询？(y/n): ", end="")
            try:
                choice = input().strip().lower()
                if choice == 'y':
                    success = test_multiple_query()
            except KeyboardInterrupt:
                print("\n\n测试中断")
    
    sys.exit(0 if success else 1)
