#!/usr/bin/env python3
"""
批处理核心模块使用示例

演示如何使用改进后的批处理功能：
1. 基本使用（仅视频ID）
2. 高级使用（包含播放量和带货金额）
"""

import sys
from pathlib import Path

# 添加路径
workspace_root = Path.home() / ".openclaw/workspace"
sys.path.insert(0, str(workspace_root))

from batch_processor_core import process_creator, run_batch


def example_basic():
    """示例1: 基本使用（仅视频ID）"""
    print("\n" + "="*70)
    print("示例1: 基本使用（仅视频ID）")
    print("="*70)
    
    creator_data = {
        'record_id': 'recvdmcVxL2Q1m',
        'tk_handle': 'soe..moe..kyi',
        'video_ids': [
            '7615214987761454354', '7615174225762012424', '7615171800246275346',
            '7615166157330337032', '7615162682148179208', '7615157483857775880',
            '7615150168308059399', '7615148832443157768', '7615145934657178888',
            '7615142976729337095', '7615132946638376210', '7615129098171485448'
        ]
    }
    
    print("\n处理达人（不包含播放量和带货金额数据）...")
    result = process_creator(creator_data)
    
    print("\n处理结果:")
    print(f"  状态: {result['status']}")
    if result['status'] == 'success':
        print(f"  生成宫图数: {result['grids_generated']}")
        print(f"  宫图路径: {result['grid_paths']}")


def example_advanced():
    """示例2: 高级使用（包含播放量和带货金额）"""
    print("\n" + "="*70)
    print("示例2: 高级使用（包含播放量和带货金额）")
    print("="*70)
    
    creator_data = {
        'record_id': 'recvdmcVxL2Q1m',
        'tk_handle': 'soe..moe..kyi',
        'video_ids': [
            '7615214987761454354', '7615174225762012424', '7615171800246275346',
            '7615166157330337032', '7615162682148179208', '7615157483857775880',
            '7615150168308059399', '7615148832443157768', '7615145934657178888',
            '7615142976729337095', '7615132946638376210', '7615129098171485448',
            '7615214987761454355', '7615174225762012425', '7615171800246275347',
            '7615166157330337033', '7615162682148179209', '7615157483857775881',
            '7615150168308059400', '7615148832443157769', '7615145934657178889',
            '7615142976729337096', '7615132946638376211', '7615129098171485449'
        ],
        'video_data': [
            {'views': 1500000, 'revenue': 25000.0},
            {'views': 800000, 'revenue': 15000.0},
            {'views': 1200000, 'revenue': 20000.0},
            {'views': 950000, 'revenue': 18000.0},
            {'views': 1100000, 'revenue': 22000.0},
            {'views': 750000, 'revenue': 12000.0},
            {'views': 1300000, 'revenue': 24000.0},
            {'views': 900000, 'revenue': 16000.0},
            {'views': 1050000, 'revenue': 19000.0},
            {'views': 850000, 'revenue': 14000.0},
            {'views': 1150000, 'revenue': 21000.0},
            {'views': 700000, 'revenue': 11000.0},
            {'views': 1400000, 'revenue': 26000.0},
            {'views': 950000, 'revenue': 17000.0},
            {'views': 1250000, 'revenue': 23000.0},
            {'views': 800000, 'revenue': 13000.0},
            {'views': 1350000, 'revenue': 25000.0},
            {'views': 900000, 'revenue': 15000.0},
            {'views': 1100000, 'revenue': 20000.0},
            {'views': 850000, 'revenue': 14000.0},
            {'views': 1200000, 'revenue': 22000.0},
            {'views': 750000, 'revenue': 12000.0},
            {'views': 1300000, 'revenue': 24000.0},
            {'views': 900000, 'revenue': 16000.0}
        ]
    }
    
    print("\n处理达人（包含播放量和带货金额数据）...")
    result = process_creator(creator_data)
    
    print("\n处理结果:")
    print(f"  状态: {result['status']}")
    if result['status'] == 'success':
        print(f"  生成宫图数: {result['grids_generated']}")
        print(f"  宫图路径: {result['grid_paths']}")
        print("\n  封面上将显示:")
        print("    - 播放量（如 1.5M、800K）")
        print("    - 带货金额（如 25.0K、15.0K）")


def example_batch():
    """示例3: 批量处理多个达人"""
    print("\n" + "="*70)
    print("示例3: 批量处理多个达人")
    print("="*70)
    
    creators = [
        {
            'record_id': 'recvdmcVxL2Q1m',
            'tk_handle': 'soe..moe..kyi',
            'video_ids': [
                '7615214987761454354', '7615174225762012424', '7615171800246275346',
                '7615166157330337032', '7615162682148179208', '7615157483857775880',
                '7615150168308059399', '7615148832443157768', '7615145934657178888',
                '7615142976729337095', '7615132946638376210', '7615129098171485448'
            ]
        },
        {
            'record_id': 'recvdmcW2lzIvC',
            'tk_handle': 'fluke_0171',
            'video_ids': [
                '7591095075665415444', '7585539516694334728', '7577184916513852680',
                '7615220675459943700', '7615208925880732949', '7615198242724154644',
                '7615049244067499284', '7614867455936040213', '7614854989197905172',
                '7614847096738434325', '7614833990385618197', '7614825786238930196'
            ]
        }
    ]
    
    print(f"\n批量处理 {len(creators)} 个达人...")
    summary = run_batch(creators, batch_name="示例批次")
    
    print("\n批处理结果:")
    print(f"  成功: {summary['success']}")
    print(f"  失败: {summary['failed']}")
    print(f"  总计: {summary['total']}")


def main():
    """主函数"""
    print("="*70)
    print("批处理核心模块使用示例")
    print("="*70)
    print("\n选择示例:")
    print("  1. 基本使用（仅视频ID）")
    print("  2. 高级使用（包含播放量和带货金额）")
    print("  3. 批量处理多个达人")
    print("  4. 运行所有示例")
    
    choice = input("\n请输入选项 (1-4): ").strip()
    
    if choice == '1':
        example_basic()
    elif choice == '2':
        example_advanced()
    elif choice == '3':
        example_batch()
    elif choice == '4':
        example_basic()
        example_advanced()
        example_batch()
    else:
        print("无效选项")
        return
    
    print("\n" + "="*70)
    print("示例完成")
    print("="*70)


if __name__ == "__main__":
    main()
