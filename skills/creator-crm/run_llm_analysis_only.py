#!/usr/bin/env python3
"""
LLM 分析补全脚本（数据补偿机制）

专门用于补全飞书表格中未完成的 LLM 分析任务：
- 视频质量评分（支持：未打分 或 打分为0 的数据）
- 带货标签打标（支持：未打标的数据）
- 达人风格打标（支持：未打标的数据）

这三个任务彼此独立，无先后依赖关系。
脚本会自动检查每条记录的完成情况，只执行未完成的任务。

本脚本作为数据补偿机制，与 skill 中的品类打标、达人风格打标、达人打分
共用相同的方法、传参和模型调用逻辑。

使用方法：
    # 补全所有未完成的 LLM 分析任务
    python3 skills/creator-crm/run_llm_analysis_only.py
    
    # 只补全视频评分（包括打分为0的数据）
    python3 skills/creator-crm/run_llm_analysis_only.py --task scoring
    
    # 只补全带货标签
    python3 skills/creator-crm/run_llm_analysis_only.py --task category
    
    # 只补全风格标签
    python3 skills/creator-crm/run_llm_analysis_only.py --task vibe
    
    # 限制处理数量
    python3 skills/creator-crm/run_llm_analysis_only.py --limit 10
"""

import os
import sys
import json
import argparse
import time
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional

# 确保从任意目录运行都能找到模块
SKILL_DIR = Path(__file__).parent.absolute()
REPO_ROOT = SKILL_DIR.parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(SKILL_DIR))

from workspace_support import load_repo_env

load_repo_env()

from core.feishu_reader import FeishuBitableReader
from core.llm_analyzer import (
    VideoScoringAgent, CategoryTaggingAgent, VibeTaggingAgent,
    FeishuFieldUpdater, TaskCompletionChecker, CombinedScoringVibeAgent
)


# ============================================================================
# 配置
# ============================================================================

# 飞书多维表格配置
FEISHU_APP_TOKEN = os.environ.get("FEISHU_APP_TOKEN", "")
FEISHU_TABLE_ID = os.environ.get("FEISHU_TABLE_ID", "")

# 宫格图存储目录
OUTPUT_DIR = SKILL_DIR / "output" / "grids"

# 断点续传状态文件
STATE_FILE = SKILL_DIR / "output" / "llm_analysis_state.json"


# ============================================================================
# 核心逻辑
# ============================================================================

class LLMAnalysisCompleter:
    """LLM 分析补全器（数据补偿机制）
    
    与 skill 中的品类打标、达人风格打标、达人打分共用相同的方法和模型调用逻辑。
    支持：
    - 视频评分：未打分 或 打分为0 的数据
    - 带货标签：未打标的数据
    - 达人风格：未打标的数据
    """
    
    def __init__(self):
        # 使用与 skill 相同的智能体实例
        # 合并智能体：打分 + 风格（一次 LLM 调用，节省 token）
        self.combined_scoring_vibe_agent = CombinedScoringVibeAgent()
        # 带货标签打标智能体（使用独立的 Responses API）
        self.category_tagging_agent = CategoryTaggingAgent()
        # 飞书字段更新器
        self.feishu_updater = FeishuFieldUpdater()
        # 任务完成检查器
        self.task_checker = TaskCompletionChecker()
        
        self.stats = {
            'total_checked': 0,
            'scoring_completed': 0,      # 评分完成数
            'scoring_failed': 0,         # 评分失败数
            'combined_completed': 0,     # 评分+风格合并完成
            'combined_failed': 0,
            'category_completed': 0,     # 带货标签完成数
            'category_failed': 0,        # 带货标签失败数
            'vibe_completed': 0,         # 风格标签完成数
            'vibe_failed': 0             # 风格标签失败数
        }
        
        # 已处理的记录 ID 集合（用于断点续传）
        self.processed_ids = self._load_state()
    
    def _load_state(self) -> set:
        """加载断点续传状态"""
        if STATE_FILE.exists():
            try:
                with open(STATE_FILE, 'r') as f:
                    state = json.load(f)
                    processed = set(state.get('processed_ids', []))
                    print(f"📂 加载断点续传状态: {len(processed)} 条记录已处理")
                    return processed
            except Exception as e:
                print(f"⚠️ 加载状态文件失败: {e}")
        return set()
    
    def _save_state(self):
        """保存断点续传状态"""
        try:
            state = {
                'processed_ids': list(self.processed_ids),
                'stats': self.stats,
                'last_update': datetime.now().isoformat()
            }
            with open(STATE_FILE, 'w') as f:
                json.dump(state, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"⚠️ 保存状态文件失败: {e}")
    
    def get_records_with_grids(self) -> List[Dict[str, Any]]:
        """
        获取所有有宫格图的记录
        
        Returns:
            有宫格图的记录列表
        """
        print("\n📖 从飞书读取有宫格图的记录...")
        
        reader = FeishuBitableReader(
            app_token=FEISHU_APP_TOKEN,
            table_id=FEISHU_TABLE_ID
        )
        
        records = reader.read_records(page_size=500)
        
        # 过滤出有宫格图（视频截图）的记录
        records_with_grids = []
        for record in records:
            if hasattr(record, 'video_screenshots') and record.video_screenshots:
                records_with_grids.append({
                    'record_id': record.record_id,
                    'tk_handle': record.tk_handle,
                    'video_screenshots': record.video_screenshots
                })
        
        print(f"✅ 找到 {len(records_with_grids)} 条有宫格图的记录")
        return records_with_grids
    
    def check_record_status(
        self,
        record_id: str,
        app_token: str,
        table_id: str
    ) -> Dict[str, Any]:
        """
        检查单条记录的任务完成状态（扩展版）
        
        与 TaskCompletionChecker.get_incomplete_tasks 不同，
        此方法额外检查评分是否为0的情况。
        
        Returns:
            {
                'has_grid': bool,           # 是否有宫格图
                'has_valid_score': bool,    # 是否有有效评分（非空且非0）
                'has_category': bool,       # 是否有带货标签
                'has_vibe': bool,           # 是否有风格标签
                'score_value': float,       # 当前评分值
                'incomplete': List[str]     # 未完成任务列表
            }
        """
        import requests
        
        access_token = self.feishu_updater._get_access_token()
        
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"
        
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        response = requests.get(url, headers=headers)
        result = response.json()
        
        if result.get('code') != 0:
            raise Exception(f"获取记录失败: {result.get('msg')}")
        
        fields = result.get('data', {}).get('record', {}).get('fields', {})
        
        # 检查各字段状态
        has_grid = bool(fields.get('视频截图'))
        
        # 评分检查：未打分 或 打分为0 都需要重新处理
        score_value = fields.get('视频最终评分')
        has_valid_score = False
        if score_value is not None and score_value != '':
            try:
                score_float = float(score_value)
                # 评分大于0才视为有效
                has_valid_score = score_float > 0
            except (ValueError, TypeError):
                has_valid_score = False
        
        has_category = bool(fields.get('主大类'))
        has_vibe = bool(fields.get('达人风格标签'))
        
        # 构建未完成任务列表
        incomplete = []
        if has_grid:
            if not has_valid_score:
                incomplete.append('scoring')
            if not has_category:
                incomplete.append('category')
            if not has_vibe:
                incomplete.append('vibe')
        
        return {
            'has_grid': has_grid,
            'has_valid_score': has_valid_score,
            'has_category': has_category,
            'has_vibe': has_vibe,
            'score_value': score_value,
            'incomplete': incomplete
        }
    
    def check_and_complete(
        self,
        record: Dict[str, Any],
        tasks: List[str] = None
    ) -> Dict[str, Any]:
        """
        检查并补全单条记录的 LLM 分析任务
        
        与 skill 中的方法共用相同的调用逻辑：
        - 使用 CombinedScoringVibeAgent 进行评分+风格分析
        - 使用 CategoryTaggingAgent 进行带货标签打标
        - 使用 FeishuFieldUpdater 更新飞书字段
        
        Args:
            record: 达人记录
            tasks: 要执行的任务列表 ['scoring', 'category', 'vibe']，None 表示全部
        
        Returns:
            执行结果
        """
        record_id = record['record_id']
        tk_handle = record['tk_handle']
        
        # 默认执行所有任务
        if tasks is None:
            tasks = ['scoring', 'category', 'vibe']
        
        result = {
            'record_id': record_id,
            'tk_handle': tk_handle,
            'tasks_completed': [],
            'tasks_failed': [],
            'status_before': {},
            'status_after': {}
        }
        
        print(f"\n{'='*60}")
        print(f"检查达人: {tk_handle}")
        print(f"{'='*60}")
        
        # 使用扩展的状态检查方法
        try:
            status = self.check_record_status(
                record_id=record_id,
                app_token=FEISHU_APP_TOKEN,
                table_id=FEISHU_TABLE_ID
            )
            result['status_before'] = status
            
            # 获取未完成任务（考虑评分是否为0）
            incomplete = status['incomplete']
            
            print(f"  当前状态:")
            print(f"    - 有宫格图: {status['has_grid']}")
            print(f"    - 有效评分: {status['has_valid_score']} (当前值: {status['score_value']})")
            print(f"    - 有带货标签: {status['has_category']}")
            print(f"    - 有风格标签: {status['has_vibe']}")
            print(f"  未完成任务: {incomplete}")
            
        except Exception as e:
            print(f"  ⚠️ 检查任务状态失败: {e}")
            # 如果检查失败，尝试执行所有请求的任务
            incomplete = tasks
            result['status_before'] = {'error': str(e)}
        
        self.stats['total_checked'] += 1
        
        # 获取宫格图路径
        grid_paths = self._get_grid_paths(record)
        if not grid_paths:
            print(f"  ❌ 无法获取宫格图路径，跳过")
            result['error'] = "无法获取宫格图路径"
            return result
        
        # ============ 合并执行：评分 + 风格打标 ============
        # 使用与 skill 相同的 CombinedScoringVibeAgent
        # 如果需要评分或风格打标，使用合并智能体
        needs_combined = ('scoring' in incomplete or 'vibe' in incomplete)
        if needs_combined:
            print(f"\n  🎯 执行: 视频评分 + 风格打标（合并调用，与 skill 共用逻辑）")
            try:
                # 使用与 skill 完全相同的调用方式和传参
                combined_result = self.combined_scoring_vibe_agent.execute({
                    'tk_handle': tk_handle,
                    'grid_paths': grid_paths,
                    'views_list': []  # 可选参数，暂不传入播放量
                })
                
                # 更新评分（如果需要）
                if 'scoring' in incomplete:
                    # 使用与 skill 相同的更新逻辑
                    scoring_result = {
                        'analysis_reason': combined_result.get('analysis_reason', ''),
                        'score_traffic': combined_result.get('score_traffic', 3),
                        'score_presence': combined_result.get('score_presence', 3),
                        'score_consistency': combined_result.get('score_consistency', 3),
                        'score_lighting': combined_result.get('score_lighting', 3),
                        'score_background': combined_result.get('score_background', 3),
                        'total_score': combined_result.get('total_score', 15),
                        'final_star_rating': combined_result.get('final_star_rating', 3.0)
                    }
                    self.feishu_updater.update_scoring_result(
                        record_id=record_id,
                        app_token=FEISHU_APP_TOKEN,
                        table_id=FEISHU_TABLE_ID,
                        scoring_result=scoring_result
                    )
                    print(f"     ✅ 评分完成: {scoring_result['final_star_rating']} 星")
                    result['tasks_completed'].append('scoring')
                    self.stats['scoring_completed'] += 1
                
                # 更新风格（如果需要）
                if 'vibe' in incomplete:
                    vibe_result = {
                        'vibe_reason': combined_result.get('vibe_reason', ''),
                        'vibe_tag': combined_result.get('vibe_tag', 'Unknown')
                    }
                    self.feishu_updater.update_vibe_result(
                        record_id=record_id,
                        app_token=FEISHU_APP_TOKEN,
                        table_id=FEISHU_TABLE_ID,
                        vibe_result=vibe_result
                    )
                    print(f"     ✅ 风格完成: {vibe_result['vibe_tag']}")
                    result['tasks_completed'].append('vibe')
                    self.stats['vibe_completed'] += 1
                
                self.stats['combined_completed'] += 1
                
            except Exception as e:
                print(f"     ❌ 评分/风格分析失败: {e}")
                result['tasks_failed'].extend([t for t in ['scoring', 'vibe'] if t in incomplete])
                if 'scoring' in incomplete:
                    self.stats['scoring_failed'] += 1
                if 'vibe' in incomplete:
                    self.stats['vibe_failed'] += 1
                self.stats['combined_failed'] += 1
        
        # ============ 独立执行：带货标签打标 ============
        # 使用与 skill 相同的 CategoryTaggingAgent
        if 'category' in tasks and 'category' in incomplete:
            print(f"\n  🏷️ 执行: 带货标签打标（与 skill 共用逻辑）")
            try:
                # 使用与 skill 完全相同的调用方式和传参
                category_result = self.category_tagging_agent.execute({
                    'tk_handle': tk_handle,
                    'grid_paths': grid_paths
                })
                
                # 使用与 skill 相同的更新逻辑
                self.feishu_updater.update_category_result(
                    record_id=record_id,
                    app_token=FEISHU_APP_TOKEN,
                    table_id=FEISHU_TABLE_ID,
                    category_result=category_result
                )
                print(f"     ✅ 打标完成: {category_result['main_category_1']}")
                result['tasks_completed'].append('category')
                self.stats['category_completed'] += 1
                
            except Exception as e:
                print(f"     ❌ 打标失败: {e}")
                result['tasks_failed'].append('category')
                self.stats['category_failed'] += 1
        
        return result
    
    def _get_grid_paths(self, record: Dict[str, Any]) -> List[str]:
        """
        获取宫格图本地路径
        
        首先尝试从本地目录获取，如果不存在则返回空列表
        """
        tk_handle = record['tk_handle']
        
        # 尝试在本地目录查找
        grid_files = list(OUTPUT_DIR.glob(f"{tk_handle}*.png"))
        
        if grid_files:
            return [str(f) for f in grid_files]
        
        return []


# ============================================================================
# 命令行入口
# ============================================================================

def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='LLM 分析补全脚本（数据补偿机制）- 补全未完成的评分、标签任务',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 补全所有未完成的 LLM 分析任务
  python3 run_llm_analysis_only.py
  
  # 只补全视频评分（包括打分为0的数据）
  python3 run_llm_analysis_only.py --task scoring
  
  # 只补全带货标签
  python3 run_llm_analysis_only.py --task category
  
  # 只补全风格标签
  python3 run_llm_analysis_only.py --task vibe
  
  # 限制处理数量
  python3 run_llm_analysis_only.py --limit 10

补偿机制说明:
  - 视频评分：对未打分 或 打分为0 的数据进行补偿
  - 带货标签：对未打标的数据进行补偿
  - 达人风格：对未打标的数据进行补偿
  
  本脚本与 skill 中的品类打标、达人风格打标、达人打分共用相同的方法和模型调用逻辑。
        """
    )
    
    parser.add_argument(
        '--task', '-t',
        choices=['scoring', 'category', 'vibe', 'all'],
        default='all',
        help='要执行的任务类型（默认: all）'
    )
    
    parser.add_argument(
        '--limit', '-n',
        type=int,
        default=None,
        help='最大处理数量（默认处理全部）'
    )
    
    parser.add_argument(
        '--reset', '-r',
        action='store_true',
        help='重置断点续传状态（从头开始处理）'
    )
    
    args = parser.parse_args()
    
    # 重置状态
    if args.reset and STATE_FILE.exists():
        STATE_FILE.unlink()
        print("🗑️ 已重置断点续传状态")
    
    print("="*70)
    print("LLM 分析补全脚本（数据补偿机制）")
    print("="*70)
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"任务类型: {args.task}")
    print("="*70)
    print("\n📋 补偿机制说明:")
    print("  - 视频评分：未打分 或 打分为0 的数据")
    print("  - 带货标签：未打标的数据")
    print("  - 达人风格：未打标的数据")
    print("  - 使用与 skill 相同的方法和模型调用逻辑")
    print("="*70)
    
    # 创建补全器
    completer = LLMAnalysisCompleter()
    
    # 获取有宫格图的记录
    records = completer.get_records_with_grids()
    
    if not records:
        print("\n⚠️ 没有找到有宫格图的记录")
        return 0
    
    # 限制数量
    if args.limit:
        records = records[:args.limit]
        print(f"📌 限制处理前 {args.limit} 条")
    
    # 确定要执行的任务
    if args.task == 'all':
        tasks = ['scoring', 'category', 'vibe']
    else:
        tasks = [args.task]
    
    # 过滤掉已处理的记录
    records = [r for r in records if r['record_id'] not in completer.processed_ids]
    print(f"\n🚀 开始处理 {len(records)} 条记录（已跳过 {len(completer.processed_ids)} 条）...")
    
    # 处理每条记录
    results = []
    last_save_time = time.time()
    for i, record in enumerate(records, 1):
        print(f"\n[{i}/{len(records)}]", end="")
        result = completer.check_and_complete(record, tasks)
        results.append(result)
        
        # 记录已处理
        completer.processed_ids.add(record['record_id'])
        
        # 每 30 秒保存一次状态
        if time.time() - last_save_time > 30:
            completer._save_state()
            print(f"\n💾 状态已保存（已处理 {len(completer.processed_ids)} 条）")
            last_save_time = time.time()
    
    # 最终保存状态
    completer._save_state()
    
    # 打印摘要
    print("\n" + "="*70)
    print("执行摘要")
    print("="*70)
    print(f"检查记录数: {completer.stats['total_checked']}")
    print(f"\n📊 视频评分:")
    print(f"  完成: {completer.stats['scoring_completed']}")
    print(f"  失败: {completer.stats['scoring_failed']}")
    print(f"\n🎭 达人风格:")
    print(f"  完成: {completer.stats['vibe_completed']}")
    print(f"  失败: {completer.stats['vibe_failed']}")
    print(f"\n🎯 评分+风格（合并调用）:")
    print(f"  完成: {completer.stats['combined_completed']}")
    print(f"  失败: {completer.stats['combined_failed']}")
    print(f"\n🏷️ 带货标签:")
    print(f"  完成: {completer.stats['category_completed']}")
    print(f"  失败: {completer.stats['category_failed']}")
    print("="*70)
    print(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
