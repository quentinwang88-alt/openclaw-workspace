#!/usr/bin/env python3
"""
性能诊断工具 - 找出库存查询慢的原因
"""

import json
import time
from pathlib import Path
from inventory_api import InventoryAPI

def diagnose():
    """诊断性能问题"""
    print("=" * 70)
    print("库存查询性能诊断")
    print("=" * 70)
    
    # 加载配置
    config_path = Path(__file__).parent / "config" / "api_config.json"
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    print("\n📋 当前配置:")
    print("-" * 70)
    
    # 检查限流设置
    rate_limit = config.get('rate_limit', {})
    if rate_limit.get('enabled', False):
        delay = rate_limit.get('delay_between_queries', 0)
        print(f"⚠️  限流已启用: 每次查询间隔 {delay} 秒")
        print(f"   → 查询 10 个 SKU 需要额外等待: {delay * 10:.1f} 秒")
        print(f"   → 查询 100 个 SKU 需要额外等待: {delay * 100:.1f} 秒")
    else:
        print("✓ 限流未启用")
    
    # 检查超时设置
    timeout = config['api'].get('timeout', 10)
    print(f"\n⏱️  请求超时: {timeout} 秒")
    
    # 检查重试设置
    retry = config.get('retry', {})
    if retry.get('enabled', False):
        max_retries = retry.get('max_retries', 3)
        backoff = retry.get('backoff_factor', 1)
        print(f"\n🔄 重试已启用: 最多 {max_retries} 次，退避系数 {backoff}")
    else:
        print("\n🔄 重试未启用")
    
    # 性能测试
    print("\n" + "=" * 70)
    print("性能测试")
    print("=" * 70)
    
    test_sku = config.get('test_sku', 'BU0010')
    
    # 测试1: 单次查询
    print(f"\n测试 1: 单次查询 ({test_sku})")
    print("-" * 70)
    api = InventoryAPI()
    
    start = time.time()
    result = api.query_single(test_sku)
    elapsed = time.time() - start
    
    print(f"耗时: {elapsed:.3f} 秒")
    if 'error' in result:
        print(f"结果: ❌ {result['error']}")
    else:
        print(f"结果: ✓ 可用库存 {result['available']}")
    
    # 测试2: 连续查询 3 次
    print(f"\n测试 2: 连续查询 3 次")
    print("-" * 70)
    
    start = time.time()
    for i in range(3):
        result = api.query_single(test_sku)
        print(f"  第 {i+1} 次: {time.time() - start:.3f} 秒")
    total_elapsed = time.time() - start
    
    print(f"\n总耗时: {total_elapsed:.3f} 秒")
    print(f"平均每次: {total_elapsed/3:.3f} 秒")
    
    # 分析瓶颈
    print("\n" + "=" * 70)
    print("性能分析")
    print("=" * 70)
    
    actual_api_time = elapsed  # 第一次查询的实际时间
    rate_limit_delay = rate_limit.get('delay_between_queries', 0) if rate_limit.get('enabled') else 0
    
    print(f"\n实际 API 响应时间: ~{actual_api_time:.3f} 秒")
    print(f"限流等待时间: {rate_limit_delay} 秒")
    print(f"总时间: ~{actual_api_time + rate_limit_delay:.3f} 秒/次")
    
    # 给出建议
    print("\n" + "=" * 70)
    print("优化建议")
    print("=" * 70)
    
    if rate_limit.get('enabled') and rate_limit_delay > 0.1:
        print("\n⚠️  主要瓶颈: 限流等待时间过长")
        print(f"\n当前设置: 每次查询等待 {rate_limit_delay} 秒")
        print(f"查询 10 个 SKU 需要: ~{(actual_api_time + rate_limit_delay) * 10:.1f} 秒")
        print(f"查询 100 个 SKU 需要: ~{(actual_api_time + rate_limit_delay) * 100:.1f} 秒")
        
        print("\n💡 优化方案:")
        print(f"   1. 降低限流延迟到 0.1 秒（推荐）")
        print(f"      → 10 个 SKU: ~{(actual_api_time + 0.1) * 10:.1f} 秒")
        print(f"      → 100 个 SKU: ~{(actual_api_time + 0.1) * 100:.1f} 秒")
        
        print(f"\n   2. 完全禁用限流（如果 API 允许）")
        print(f"      → 10 个 SKU: ~{actual_api_time * 10:.1f} 秒")
        print(f"      → 100 个 SKU: ~{actual_api_time * 100:.1f} 秒")
        
        print("\n   3. 使用批量查询 API（如果支持）")
        print(f"      → 可能将 100 个 SKU 的查询时间降低到 1-5 秒")
    
    elif actual_api_time > 1.0:
        print("\n⚠️  主要瓶颈: API 响应时间较慢")
        print(f"\n当前 API 响应时间: {actual_api_time:.3f} 秒")
        print("\n💡 优化方案:")
        print("   1. 检查网络连接")
        print("   2. 确认 API 服务器状态")
        print("   3. 考虑使用更快的 API 端点")
        print("   4. 检查是否有批量查询接口")
    
    else:
        print("\n✓ 性能良好！")
        print(f"\n当前配置下:")
        print(f"  单次查询: ~{actual_api_time + rate_limit_delay:.3f} 秒")
        print(f"  10 个 SKU: ~{(actual_api_time + rate_limit_delay) * 10:.1f} 秒")
        print(f"  100 个 SKU: ~{(actual_api_time + rate_limit_delay) * 100:.1f} 秒")
    
    print("\n" + "=" * 70)
    print("诊断完成")
    print("=" * 70)

if __name__ == "__main__":
    try:
        diagnose()
    except FileNotFoundError as e:
        print(f"\n✗ {e}")
        print("\n请先配置 API:")
        print("  1. 复制 config/api_config.example.json 为 config/api_config.json")
        print("  2. 更新配置文件中的 API 信息")
    except Exception as e:
        print(f"\n✗ 诊断失败: {e}")
        import traceback
        traceback.print_exc()
