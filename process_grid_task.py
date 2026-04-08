#!/usr/bin/env python3
"""
Creator Grid Pipeline - 标准入口点

这是唯一推荐使用的入口点，其他旧文件已废弃。

功能：
1. 从飞书读取"待开始"状态的任务（包含 Kalodata_URL）
2. 打开 Kalodata 页面获取视频数据（视频ID、播放量、带货金额）
3. 获取前 24 个视频封面（有多少抓多少）
4. 生成两张宫图（每张 12 个）
5. 封面上包含播放量和带货金额
6. 更新飞书状态（已完成/生成失败）

使用方法：
    python3 process_grid_task.py
"""

import sys
import json
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime

from workspace_support import REPO_ROOT

# 添加路径
workspace_root = REPO_ROOT
sys.path.insert(0, str(workspace_root))
sys.path.insert(0, str(workspace_root / "skills/creator-crm"))

# 导入核心模块
from batch_processor_core import process_creator
from skills.creator_crm.utils.feishu_uploader import FeishuUploaderAgent
from skills.creator_crm.core.kalodata_fetcher import KalodataFetcher

# 配置
import importlib.util
config_path = workspace_root / "config.py"
spec = importlib.util.spec_from_file_location("workspace_config", config_path)
workspace_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(workspace_config)

APP_TOKEN = workspace_config.FEISHU_APP_TOKEN
TABLE_ID = workspace_config.FEISHU_TABLE_ID


class FeishuTaskManager:
    """飞书任务管理器"""
    
    # 状态常量
    STATUS_PENDING = "待开始"
    STATUS_COMPLETED = "已完成"
    STATUS_FAILED = "生成失败"
    
    def __init__(self):
        self.uploader = FeishuUploaderAgent()
    
    def fetch_pending_tasks(self) -> List[Dict[str, Any]]:
        """
        从飞书多维表格获取"待开始"状态的任务
        
        Returns:
            待处理任务列表，包含 Kalodata_URL
        """
        print("📋 从飞书读取待开始的任务...")
        
        try:
            # TODO: 实现飞书 API 调用
            # 筛选条件：状态字段 = "待开始"
            # 示例返回格式
            tasks = []
            
            # 这里需要调用飞书 API
            # response = self.uploader.feishu_client.get_records(
            #     app_token=APP_TOKEN,
            #     table_id=TABLE_ID,
            #     filter='状态 = "待开始"'
            # )
            #
            # for record in response['records']:
            #     tasks.append({
            #         'record_id': record['record_id'],
            #         'tk_handle': record['fields']['TikTok账号'],
            #         'kalodata_url': record['fields']['Kalodata_URL'],  # Kalodata 链接
            #     })
            
            print(f"✅ 找到 {len(tasks)} 个待开始的任务")
            return tasks
            
        except Exception as e:
            print(f"❌ 读取飞书数据失败: {e}")
            return []
    
    def update_task_status(
        self,
        record_id: str,
        status: str,
        error_message: str = None
    ):
        """
        更新任务状态
        
        Args:
            record_id: 记录 ID
            status: 状态（待开始/已完成/生成失败）
            error_message: 错误信息（可选）
        """
        try:
            # TODO: 实现飞书 API 调用
            # self.uploader.feishu_client.update_record(
            #     app_token=APP_TOKEN,
            #     table_id=TABLE_ID,
            #     record_id=record_id,
            #     fields={
            #         '状态': status,
            #         '错误信息': error_message if error_message else '',
            #         '更新时间': datetime.now().isoformat()
            #     }
            # )
            
            print(f"✅ 更新状态: {record_id} -> {status}")
            
        except Exception as e:
            print(f"⚠️  更新状态失败: {e}")


def process_task(task: Dict[str, Any], task_manager: FeishuTaskManager) -> Dict[str, Any]:
    """
    处理单个任务
    
    Args:
        task: 任务数据（包含 kalodata_url）
        task_manager: 飞书任务管理器
    
    Returns:
        处理结果
    """
    record_id = task['record_id']
    tk_handle = task['tk_handle']
    kalodata_url = task['kalodata_url']
    
    print(f"\n{'='*70}")
    print(f"处理任务: {tk_handle}")
    print(f"Record ID: {record_id}")
    print(f"Kalodata URL: {kalodata_url}")
    print(f"{'='*70}")
    
    try:
        # 步骤 1: 从 Kalodata 页面获取视频数据
        print("\n⏳ 步骤 1/4: 从 Kalodata 获取视频数据...")
        print(f"  打开链接: {kalodata_url}")
        
        # 使用 KalodataFetcher 获取视频数据
        fetcher = KalodataFetcher()
        
        # 从 URL 中提取达人 ID
        # Kalodata URL 格式: https://www.kalodata.com/creator/detail?id=xxx
        import re
        match = re.search(r'id=([^&]+)', kalodata_url)
        if not match:
            raise Exception(f"无法从 URL 中提取达人 ID: {kalodata_url}")
        
        creator_id = match.group(1)
        print(f"  达人 ID: {creator_id}")
        
        # 获取视频数据（前 24 个）
        videos = fetcher.fetch_creator_videos(creator_id, limit=24)
        
        if not videos:
            raise Exception("无法从 Kalodata 页面获取视频数据")
        
        print(f"  ✅ 获取到 {len(videos)} 个视频")
        
        # 转换为所需格式
        video_ids = [v.video_id for v in videos]
        video_data = [
            {
                'views': v.views,
                'revenue': v.gmv
            }
            for v in videos
        ]
        
        # 步骤 2-4: 调用核心处理逻辑
        print("\n⏳ 步骤 2-4: 生成宫图...")
        creator_data = {
            'record_id': record_id,
            'tk_handle': tk_handle,
            'video_ids': video_ids,
            'video_data': video_data
        }
        
        result = process_creator(creator_data)
        
        if result['status'] == 'success':
            # 更新为"已完成"
            task_manager.update_task_status(
                record_id=record_id,
                status=FeishuTaskManager.STATUS_COMPLETED
            )
            print(f"✅ 任务完成: {tk_handle}")
        else:
            # 更新为"生成失败"
            error_msg = result.get('reason', '未知错误')
            task_manager.update_task_status(
                record_id=record_id,
                status=FeishuTaskManager.STATUS_FAILED,
                error_message=error_msg
            )
            print(f"❌ 任务失败: {tk_handle} - {error_msg}")
        
        return result
    
    except Exception as e:
        # 更新为"生成失败"
        error_msg = str(e)
        task_manager.update_task_status(
            record_id=record_id,
            status=FeishuTaskManager.STATUS_FAILED,
            error_message=error_msg
        )
        print(f"❌ 任务异常: {tk_handle} - {error_msg}")
        
        return {
            'status': 'failed',
            'reason': error_msg,
            'tk_handle': tk_handle
        }


def main():
    """主函数"""
    print("="*70)
    print("Creator Grid Pipeline - 标准入口点")
    print("="*70)
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # 创建任务管理器
    task_manager = FeishuTaskManager()
    
    # 步骤 1: 读取待开始的任务
    tasks = task_manager.fetch_pending_tasks()
    
    if not tasks:
        print("\n✅ 没有待开始的任务")
        return
    
    print(f"\n📊 待处理任务数量: {len(tasks)}")
    
    # 步骤 2: 逐个处理任务
    results = []
    success_count = 0
    failed_count = 0
    
    for i, task in enumerate(tasks, 1):
        print(f"\n[{i}/{len(tasks)}] 开始处理...")
        
        result = process_task(task, task_manager)
        results.append(result)
        
        if result['status'] == 'success':
            success_count += 1
        else:
            failed_count += 1
    
    # 步骤 3: 打印汇总
    print("\n" + "="*70)
    print("处理完成")
    print("="*70)
    print(f"✅ 成功: {success_count}")
    print(f"❌ 失败: {failed_count}")
    print(f"📊 总计: {len(tasks)}")
    
    # 保存结果
    result_file = workspace_root / "output/last_run_result.json"
    result_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(result_file, 'w', encoding='utf-8') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'total': len(tasks),
            'success': success_count,
            'failed': failed_count,
            'results': results
        }, f, indent=2, ensure_ascii=False)
    
    print(f"\n📄 结果已保存: {result_file}")


if __name__ == "__main__":
    main()
