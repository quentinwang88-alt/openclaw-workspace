#!/usr/bin/env python3
"""
测试批处理核心模块的改进功能

测试场景：
1. 24 个视频（生成 2 张宫图）
2. 18 个视频（生成 1 张宫图）
3. 15 个视频（生成 1 张宫图）
4. 10 个视频（失败，封面不足）
"""

import sys
from pathlib import Path

# 添加路径
workspace_root = Path.home() / ".openclaw/workspace"
sys.path.insert(0, str(workspace_root))

from batch_processor_core import process_creator

# 测试数据
TEST_CASES = [
    {
        "name": "场景1: 24个视频（应生成2张宫图）",
        "data": {
            "record_id": "test_rec_001",
            "tk_handle": "test_user_24",
            "video_ids": [
                "7615214987761454354", "7615174225762012424", "7615171800246275346",
                "7615166157330337032", "7615162682148179208", "7615157483857775880",
                "7615150168308059399", "7615148832443157768", "7615145934657178888",
                "7615142976729337095", "7615132946638376210", "7615129098171485448",
                "7615214987761454355", "7615174225762012425", "7615171800246275347",
                "7615166157330337033", "7615162682148179209", "7615157483857775881",
                "7615150168308059400", "7615148832443157769", "7615145934657178889",
                "7615142976729337096", "7615132946638376211", "7615129098171485449"
            ],
            "video_data": [
                {"views": 1500000, "revenue": 25000.0},
                {"views": 800000, "revenue": 15000.0},
                {"views": 1200000, "revenue": 20000.0},
                {"views": 950000, "revenue": 18000.0},
                {"views": 1100000, "revenue": 22000.0},
                {"views": 750000, "revenue": 12000.0},
                {"views": 1300000, "revenue": 24000.0},
                {"views": 900000, "revenue": 16000.0},
                {"views": 1050000, "revenue": 19000.0},
                {"views": 850000, "revenue": 14000.0},
                {"views": 1150000, "revenue": 21000.0},
                {"views": 700000, "revenue": 11000.0},
                {"views": 1400000, "revenue": 26000.0},
                {"views": 950000, "revenue": 17000.0},
                {"views": 1250000, "revenue": 23000.0},
                {"views": 800000, "revenue": 13000.0},
                {"views": 1350000, "revenue": 25000.0},
                {"views": 900000, "revenue": 15000.0},
                {"views": 1100000, "revenue": 20000.0},
                {"views": 850000, "revenue": 14000.0},
                {"views": 1200000, "revenue": 22000.0},
                {"views": 750000, "revenue": 12000.0},
                {"views": 1300000, "revenue": 24000.0},
                {"views": 900000, "revenue": 16000.0}
            ]
        },
        "expected_grids": 2
    },
    {
        "name": "场景2: 18个视频（应生成1张宫图）",
        "data": {
            "record_id": "test_rec_002",
            "tk_handle": "test_user_18",
            "video_ids": [
                "7615214987761454354", "7615174225762012424", "7615171800246275346",
                "7615166157330337032", "7615162682148179208", "7615157483857775880",
                "7615150168308059399", "7615148832443157768", "7615145934657178888",
                "7615142976729337095", "7615132946638376210", "7615129098171485448",
                "7615214987761454355", "7615174225762012425", "7615171800246275347",
                "7615166157330337033", "7615162682148179209", "7615157483857775881"
            ],
            "video_data": [
                {"views": 1500000, "revenue": 25000.0},
                {"views": 800000, "revenue": 15000.0},
                {"views": 1200000, "revenue": 20000.0},
                {"views": 950000, "revenue": 18000.0},
                {"views": 1100000, "revenue": 22000.0},
                {"views": 750000, "revenue": 12000.0},
                {"views": 1300000, "revenue": 24000.0},
                {"views": 900000, "revenue": 16000.0},
                {"views": 1050000, "revenue": 19000.0},
                {"views": 850000, "revenue": 14000.0},
                {"views": 1150000, "revenue": 21000.0},
                {"views": 700000, "revenue": 11000.0},
                {"views": 1400000, "revenue": 26000.0},
                {"views": 950000, "revenue": 17000.0},
                {"views": 1250000, "revenue": 23000.0},
                {"views": 800000, "revenue": 13000.0},
                {"views": 1350000, "revenue": 25000.0},
                {"views": 900000, "revenue": 15000.0}
            ]
        },
        "expected_grids": 1
    },
    {
        "name": "场景3: 15个视频（应生成1张宫图）",
        "data": {
            "record_id": "test_rec_003",
            "tk_handle": "test_user_15",
            "video_ids": [
                "7615214987761454354", "7615174225762012424", "7615171800246275346",
                "7615166157330337032", "7615162682148179208", "7615157483857775880",
                "7615150168308059399", "7615148832443157768", "7615145934657178888",
                "7615142976729337095", "7615132946638376210", "7615129098171485448",
                "7615214987761454355", "7615174225762012425", "7615171800246275347"
            ]
        },
        "expected_grids": 1
    },
    {
        "name": "场景4: 10个视频（应失败）",
        "data": {
            "record_id": "test_rec_004",
            "tk_handle": "test_user_10",
            "video_ids": [
                "7615214987761454354", "7615174225762012424", "7615171800246275346",
                "7615166157330337032", "7615162682148179208", "7615157483857775880",
                "7615150168308059399", "7615148832443157768", "7615145934657178888",
                "7615142976729337095"
            ]
        },
        "expected_grids": 0
    }
]


def run_test(test_case):
    """运行单个测试用例"""
    print("\n" + "="*70)
    print(f"测试: {test_case['name']}")
    print("="*70)
    
    result = process_creator(test_case['data'])
    
    print("\n测试结果:")
    print(f"  状态: {result['status']}")
    
    if result['status'] == 'success':
        grids_count = result.get('grids_generated', 0)
        print(f"  生成宫图数: {grids_count}")
        print(f"  预期宫图数: {test_case['expected_grids']}")
        
        if grids_count == test_case['expected_grids']:
            print("  ✅ 测试通过")
            return True
        else:
            print("  ❌ 测试失败：宫图数量不符")
            return False
    else:
        if test_case['expected_grids'] == 0:
            print("  ✅ 测试通过（预期失败）")
            return True
        else:
            print("  ❌ 测试失败：预期成功但实际失败")
            return False


def main():
    """运行所有测试"""
    print("="*70)
    print("批处理核心模块改进测试")
    print("="*70)
    print("\n⚠️  注意：此测试需要网络连接来获取真实的视频封面")
    print("如果网络不可用或视频ID无效，测试可能会失败\n")
    
    input("按 Enter 键开始测试...")
    
    results = []
    for test_case in TEST_CASES:
        try:
            success = run_test(test_case)
            results.append({
                'name': test_case['name'],
                'success': success
            })
        except Exception as e:
            print(f"\n❌ 测试异常: {e}")
            results.append({
                'name': test_case['name'],
                'success': False
            })
    
    # 打印汇总
    print("\n" + "="*70)
    print("测试汇总")
    print("="*70)
    
    passed = sum(1 for r in results if r['success'])
    total = len(results)
    
    for result in results:
        status = "✅ 通过" if result['success'] else "❌ 失败"
        print(f"{status} - {result['name']}")
    
    print(f"\n总计: {passed}/{total} 通过")
    
    if passed == total:
        print("\n🎉 所有测试通过！")
    else:
        print(f"\n⚠️  {total - passed} 个测试失败")


if __name__ == "__main__":
    main()
