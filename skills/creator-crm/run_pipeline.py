#!/usr/bin/env python3
"""
Creator CRM 流水线入口脚本
专为 OpenClaw 设计，支持从任意目录运行

功能：
1. 从飞书多维表格读取待处理达人（状态为"待开始"的记录）
2. 对每个达人执行完整流水线：
   - 从 Kalodata 获取视频数据
   - 生成 3x4 宫格图
   - 上传到飞书多维表格
   - 视频质量评分（LLM）
   - 带货标签打标（LLM）
3. 支持命令行参数控制
4. ⭐ 支持动态传入飞书文档链接（多文档场景）

使用方法：
    # 处理所有待开始的达人（含 LLM 分析）
    python3 skills/creator-crm/run_pipeline.py
    
    # ⭐ 使用指定的飞书文档链接
    python3 skills/creator-crm/run_pipeline.py --feishu-url "https://xxx.feishu.cn/base/xxx?table=xxx"
    
    # 只处理前5个
    python3 skills/creator-crm/run_pipeline.py --limit 5
    
    # 处理指定达人
    python3 skills/creator-crm/run_pipeline.py --handles pimrypie tingjasale
    
    # 测试模式（不实际上传）
    python3 skills/creator-crm/run_pipeline.py --dry-run --limit 1
    
    # 跳过 LLM 分析（只生成宫格图）
    python3 skills/creator-crm/run_pipeline.py --no-llm
"""

import os
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

# 确保从任意目录运行都能找到模块
SKILL_DIR = Path(__file__).parent.absolute()
REPO_ROOT = SKILL_DIR.parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(SKILL_DIR))

from workspace_support import load_repo_env

load_repo_env()

# 导入核心模块
from automation_v2 import AutomationOrchestrator
from core.feishu_reader import FeishuBitableReader, CreatorRecord
from core.feishu_url_parser import parse_feishu_bitable_url, FeishuBitableInfo


# ============================================================================
# 配置
# ============================================================================

# 飞书多维表格配置（默认值，可通过 --feishu-url 覆盖）
FEISHU_APP_TOKEN = os.environ.get("FEISHU_APP_TOKEN", "")
FEISHU_TABLE_ID = os.environ.get("FEISHU_TABLE_ID", "")

# 状态字段名（飞书表格中的字段）
# 实际字段名：视频宫图是否已生成
STATUS_FIELD = "视频宫图是否已生成"
STATUS_PENDING = "待开始"
STATUS_PROCESSING = "处理中"
STATUS_DONE = "已完成"


# ============================================================================
# 飞书 URL 解析工具
# ============================================================================

def resolve_feishu_config(feishu_url: Optional[str] = None) -> Tuple[str, str]:
    """
    解析飞书配置，支持 URL 参数覆盖默认配置
    
    Args:
        feishu_url: 飞书多维表格 URL（可选）
    
    Returns:
        (app_token, table_id) 元组
    """
    if feishu_url:
        info = parse_feishu_bitable_url(feishu_url)
        if info:
            print(f"🔗 从 URL 解析飞书配置:")
            print(f"   app_token: {info.app_token}")
            print(f"   table_id: {info.table_id}")
            return (info.app_token, info.table_id)
        else:
            print(f"⚠️ 无法解析飞书 URL，使用默认配置")

    if not FEISHU_APP_TOKEN or not FEISHU_TABLE_ID:
        raise RuntimeError(
            "缺少飞书配置。请先填写仓库根目录 .env 中的 FEISHU_APP_TOKEN / FEISHU_TABLE_ID，"
            "或运行时使用 --feishu-url。"
        )

    return (FEISHU_APP_TOKEN, FEISHU_TABLE_ID)


# ============================================================================
# 飞书数据读取（增强版）
# ============================================================================

def load_creators_from_feishu(
    limit: Optional[int] = None,
    filter_status: str = STATUS_PENDING,
    feishu_url: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    从飞书多维表格读取待处理达人
    
    Args:
        limit: 最大处理数量（None 表示全部）
        filter_status: 过滤状态（默认"待开始"）
        feishu_url: 飞书多维表格 URL（可选，覆盖默认配置）
    
    Returns:
        达人列表
    """
    # 解析飞书配置
    app_token, table_id = resolve_feishu_config(feishu_url)
    
    print(f"\n📖 从飞书读取达人数据...")
    print(f"   表格: {app_token}")
    print(f"   过滤状态: {filter_status}")
    
    try:
        reader = FeishuBitableReader(
            app_token=app_token,
            table_id=table_id
        )
        
        # 读取所有记录
        records = reader.read_records(page_size=100)
        
        print(f"✅ 读取到 {len(records)} 条记录")
        
        # 过滤出有 Kalodata 链接且状态为"待开始"的记录
        creators = []
        status_counts = {}
        for record in records:
            # 统计状态
            status = getattr(record, '_status', '未知')
            status_counts[status] = status_counts.get(status, 0) + 1
            
            # 过滤条件：有 Kalodata 链接 且 状态为"待开始"
            if record.kalodata_url and status == filter_status:
                creators.append({
                    'record_id': record.record_id,
                    'tk_handle': record.tk_handle,
                    'tk_url': record.tk_url,
                    'kalodata_url': record.kalodata_url
                })
        
        print(f"📊 状态统计: {status_counts}")
        print(f"✅ 找到 {len(creators)} 个状态为'{filter_status}'且有 Kalodata 链接的达人")
        
        # 限制数量
        if limit:
            creators = creators[:limit]
            print(f"📌 限制处理前 {limit} 个")
        
        return creators
        
    except Exception as e:
        print(f"⚠️ 从飞书读取失败: {e}")
        print("   将使用本地测试数据...")
        return []


def load_creators_by_handles(
    handles: List[str],
    feishu_url: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    根据 TikTok 账号名加载达人数据
    
    Args:
        handles: TikTok 账号名列表
        feishu_url: 飞书多维表格 URL（可选，覆盖默认配置）
    
    Returns:
        达人列表
    """
    # 解析飞书配置
    app_token, table_id = resolve_feishu_config(feishu_url)
    
    print(f"\n📖 根据账号名加载达人: {handles}")
    
    try:
        reader = FeishuBitableReader(
            app_token=app_token,
            table_id=table_id
        )
        
        all_records = reader.read_records(page_size=100)
        
        # 过滤指定账号
        creators = []
        for record in all_records:
            if record.tk_handle in handles and record.kalodata_url:
                creators.append({
                    'record_id': record.record_id,
                    'tk_handle': record.tk_handle,
                    'tk_url': record.tk_url,
                    'kalodata_url': record.kalodata_url
                })
        
        print(f"✅ 找到 {len(creators)} 个匹配的达人")
        return creators
        
    except Exception as e:
        print(f"⚠️ 加载失败: {e}")
        return []


# ============================================================================
# 主流水线
# ============================================================================

def run_pipeline(
    creators: List[Dict[str, Any]],
    max_workers: int = 2,
    dry_run: bool = False,
    enable_llm: bool = True,
    app_token: str = None,
    table_id: str = None
) -> Dict[str, Any]:
    """
    运行完整流水线
    
    Args:
        creators: 达人列表
        max_workers: 并发数
        dry_run: 测试模式（不实际上传）
        enable_llm: 是否启用 LLM 分析（视频评分和带货标签）
        app_token: 飞书 app_token（可选，覆盖默认配置）
        table_id: 飞书 table_id（可选，覆盖默认配置）
    
    Returns:
        执行结果统计
    """
    if not creators:
        print("⚠️ 没有待处理的达人")
        return {'total': 0, 'success': 0, 'failed': 0}
    
    print(f"\n{'='*70}")
    print(f"🚀 Creator CRM 流水线")
    print(f"{'='*70}")
    print(f"待处理达人: {len(creators)}")
    print(f"并发数: {max_workers}")
    print(f"测试模式: {'是' if dry_run else '否'}")
    print(f"LLM 分析: {'启用' if enable_llm else '禁用'}")
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")
    
    if dry_run:
        print("🔍 测试模式 - 仅显示将要处理的达人：")
        for i, creator in enumerate(creators, 1):
            print(f"  {i}. @{creator['tk_handle']} (record: {creator['record_id']})")
            print(f"     Kalodata: {creator.get('kalodata_url', '无')}")
        return {'total': len(creators), 'success': 0, 'failed': 0, 'dry_run': True}
    
    # 创建编排器（使用新的状态文件，避免加载历史任务）
    import time as _time
    run_id = int(_time.time())
    state_file = str(SKILL_DIR / "output" / f"task_queue_run_{run_id}.json")
    automation = AutomationOrchestrator(
        state_file=state_file,
        enable_llm_analysis=enable_llm,
        app_token=app_token,
        table_id=table_id
    )
    automation.executor.max_workers = max_workers
    
    try:
        # 添加任务
        task_ids = automation.add_batch_tasks(creators)
        print(f"✅ 已添加 {len(task_ids)} 个任务到队列\n")
        
        # 启动执行
        automation.start()
        
        # 等待完成
        print("⏳ 等待任务完成...\n")
        automation.wait_completion()
        
        # 停止
        automation.stop()
        
        # 获取结果
        stats = automation.task_queue.get_stats()
        
        # 打印摘要
        automation.print_summary()
        
        print(f"\n结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return {
            'total': stats['total'],
            'success': stats['success'],
            'failed': stats['failed'],
            'pending': stats['pending'],
            'running': stats['running']
        }
        
    except KeyboardInterrupt:
        print("\n\n⚠️ 用户中断")
        automation.stop()
        automation.print_summary()
        return automation.task_queue.get_stats()
        
    except Exception as e:
        print(f"\n❌ 流水线错误: {e}")
        import traceback
        traceback.print_exc()
        automation.stop()
        raise e


# ============================================================================
# 命令行入口
# ============================================================================

def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='Creator CRM 流水线 - 处理达人视频截图',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 处理所有待开始的达人（含 LLM 分析）
  python3 run_pipeline.py
  
  # ⭐ 使用指定的飞书文档链接（多文档场景）
  python3 run_pipeline.py --feishu-url "https://xxx.feishu.cn/base/xxx?table=xxx"
  
  # 只处理前5个
  python3 run_pipeline.py --limit 5
  
  # 处理指定达人
  python3 run_pipeline.py --handles pimrypie tingjasale
  
  # 测试模式（不实际上传）
  python3 run_pipeline.py --dry-run --limit 3
  
  # 跳过 LLM 分析（只生成宫格图）
  python3 run_pipeline.py --no-llm
  
  # 调整并发数
  python3 run_pipeline.py --workers 3
        """
    )
    
    parser.add_argument(
        '--feishu-url', '-u',
        type=str,
        default=None,
        help='飞书多维表格 URL（支持多种格式，自动解析 app_token 和 table_id）'
    )
    
    parser.add_argument(
        '--limit', '-n',
        type=int,
        default=None,
        help='最大处理数量（默认处理全部）'
    )
    
    parser.add_argument(
        '--handles',
        nargs='+',
        default=None,
        help='指定处理的 TikTok 账号名（空格分隔）'
    )
    
    parser.add_argument(
        '--workers', '-w',
        type=int,
        default=2,
        help='并发数（默认2）'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='测试模式，只显示将要处理的达人，不实际执行'
    )
    
    parser.add_argument(
        '--no-llm',
        action='store_true',
        help='禁用 LLM 分析（跳过视频评分和带货标签）'
    )
    
    parser.add_argument(
        '--status',
        default=STATUS_PENDING,
        help=f'过滤飞书记录状态（默认: {STATUS_PENDING}）'
    )
    
    args = parser.parse_args()
    
    # 加载达人数据
    if args.handles:
        creators = load_creators_by_handles(args.handles, feishu_url=args.feishu_url)
    else:
        creators = load_creators_from_feishu(
            limit=args.limit,
            filter_status=args.status,
            feishu_url=args.feishu_url
        )
    
    if not creators:
        print("\n⚠️ 没有找到待处理的达人")
        print("请检查：")
        print("  1. 飞书表格中是否有达人记录")
        print("  2. 达人记录是否有 Kalodata 链接")
        print("  3. 飞书 API 配置是否正确")
        if args.feishu_url:
            print("  4. 飞书文档链接格式是否正确")
        return 1
    
    # 解析飞书配置（用于传递给流水线）
    app_token, table_id = resolve_feishu_config(args.feishu_url)
    
    # 运行流水线
    result = run_pipeline(
        creators=creators,
        max_workers=args.workers,
        dry_run=args.dry_run,
        enable_llm=not args.no_llm,
        app_token=app_token,
        table_id=table_id
    )
    
    # 输出结果
    print(f"\n{'='*70}")
    print(f"执行结果:")
    print(f"  总数: {result.get('total', 0)}")
    print(f"  成功: {result.get('success', 0)}")
    print(f"  失败: {result.get('failed', 0)}")
    print(f"{'='*70}")
    
    return 0 if result.get('failed', 0) == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
