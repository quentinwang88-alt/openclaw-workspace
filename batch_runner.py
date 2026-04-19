#!/usr/bin/env python3
"""
批处理运行器 - 统一入口
使用方法:
    python batch_runner.py batch_5           # 运行第5批
    python batch_runner.py batch_26_30       # 运行第26-30批
    python batch_runner.py all               # 运行所有批次
"""
import sys
import json
from pathlib import Path
from batch_processor_core import run_batch


def load_batch_data(batch_name: str = None):
    """
    加载批处理数据

    Args:
        batch_name: 批次名称，如 'batch_5', 'batch_26_30'。如果为 None，返回所有批次

    Returns:
        批次数据字典或列表
    """
    data_file = Path(__file__).parent / "batch_data.json"

    if not data_file.exists():
        print(f"❌ 批处理数据文件不存在: {data_file}")
        sys.exit(1)

    with open(data_file, 'r', encoding='utf-8') as f:
        all_data = json.load(f)

    if batch_name is None:
        return all_data

    if batch_name not in all_data:
        print(f"❌ 批次 '{batch_name}' 不存在")
        print(f"可用批次: {', '.join(all_data.keys())}")
        sys.exit(1)

    return all_data[batch_name]


def main():
    """主函数"""
    if len(sys.argv) < 2:
        print("批处理运行器")
        print("\n用法:")
        print("  python batch_runner.py <batch_name>")
        print("\n可用批次:")

        all_data = load_batch_data()
        for batch_name, creators in all_data.items():
            print(f"  - {batch_name}: {len(creators)} 个达人")

        print("\n示例:")
        print("  python batch_runner.py batch_5")
        print("  python batch_runner.py batch_26_30")
        print("  python batch_runner.py all  # 运行所有批次")
        sys.exit(0)

    batch_arg = sys.argv[1]

    if batch_arg == "all":
        # 运行所有批次
        all_data = load_batch_data()
        all_results = []

        for batch_name, creators in all_data.items():
            print(f"\n{'='*70}")
            print(f"开始处理批次: {batch_name}")
            print(f"{'='*70}\n")

            result = run_batch(creators, batch_name)
            all_results.append(result)

        # 打印总体汇总
        print("\n" + "="*70)
        print("所有批次处理完成")
        print("="*70)

        total_creators = sum(r['total'] for r in all_results)
        total_success = sum(r['success'] for r in all_results)
        total_failed = sum(r['failed'] for r in all_results)

        print(f"✅ 总成功: {total_success}")
        print(f"❌ 总失败: {total_failed}")
        print(f"📊 总计: {total_creators}")

    else:
        # 运行单个批次
        creators = load_batch_data(batch_arg)
        run_batch(creators, batch_arg)


if __name__ == "__main__":
    main()
