#!/usr/bin/env python3
"""
优化后的自动化脚本 v2.1
采用子智能体架构 + 任务队列 + 限流熔断机制

特性:
- 子智能体分工处理专业任务
- 异步非阻塞执行
- 限流和熔断保护
- 断点续传
- 完整的状态追踪
- 视频质量评分 (LLM)
- 带货标签打标 (LLM)

使用方法:
    # 前台运行
    python3 automation_v2.py
    
    # 后台运行
    nohup python3 automation_v2.py > automation_v2.log 2>&1 &
    
    # 查看进度
    tail -f automation_v2.log
    
    # 查看状态
    cat output/task_queue_state.json
"""

import os
import sys
import json
import time
import signal
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any

# 添加路径
sys.path.insert(0, str(Path(__file__).parent))

from core.task_queue import (
    Task, TaskQueue, TaskStatus, TaskPriority,
    AsyncTaskExecutor
)
from core.sub_agents import (
    VideoFetcherAgent, CoverFetcherAgent,
    GridGeneratorAgent, FeishuUploaderAgent,
    AgentOrchestrator
)
from core.llm_analyzer import (
    VideoScoringAgent, CategoryTaggingAgent, FeishuFieldUpdater,
    VibeTaggingAgent, TaskCompletionChecker, CombinedScoringVibeAgent
)


# 配置
APP_TOKEN = os.environ.get("FEISHU_APP_TOKEN", "ES8dbWo9FaXmaVs6jA7cgMURnQe")
TABLE_ID = "tblk1IHpVAvv2nWc"
# 使用绝对路径，确保从任何目录运行都能找到输出目录
SKILL_DIR = Path(__file__).parent
OUTPUT_DIR = SKILL_DIR / "output" / "grids"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# 批量处理配置（避免风控）
BATCH_SIZE = int(os.environ.get("CREATOR_BATCH_SIZE", "20"))  # 每批处理达人数量
BATCH_INTERVAL = int(os.environ.get("CREATOR_BATCH_INTERVAL", "300"))  # 批次间等待时间（秒）


class AutomationOrchestrator:
    """自动化编排器 - 主控制器"""
    
    def __init__(self, state_file: str = None, enable_llm_analysis: bool = True,
                 app_token: str = None, table_id: str = None):
        self.task_queue = TaskQueue(state_file=state_file)
        self.executor = AsyncTaskExecutor(self.task_queue, max_workers=3)
        self.orchestrator = AgentOrchestrator()
        
        # 飞书配置（允许动态传入，覆盖默认值）
        self.app_token = app_token or APP_TOKEN
        self.table_id = table_id or TABLE_ID
        
        # 是否启用 LLM 分析（视频评分、带货标签、风格标签）
        self.enable_llm_analysis = enable_llm_analysis
        
        # 注册子智能体
        self.orchestrator.register_agent('video_fetcher', VideoFetcherAgent())
        self.orchestrator.register_agent('cover_fetcher', CoverFetcherAgent())
        self.orchestrator.register_agent('grid_generator', GridGeneratorAgent())
        self.orchestrator.register_agent('feishu_uploader', FeishuUploaderAgent())
        
        # 注册 LLM 分析智能体（三个独立的任务）
        if self.enable_llm_analysis:
            # 合并智能体：打分 + 风格打标（一次 LLM 调用）
            self.combined_scoring_vibe_agent = CombinedScoringVibeAgent()
            self.category_tagging_agent = CategoryTaggingAgent()
            self.feishu_updater = FeishuFieldUpdater()
            self.task_checker = TaskCompletionChecker()
        
        # 注册任务处理器
        self._register_handlers()
        
        self.running = False
    
    def _register_handlers(self):
        """注册任务处理器"""
        
        def handle_fetch_videos(payload: Dict[str, Any]) -> Dict[str, Any]:
            """处理视频获取任务"""
            agent = self.orchestrator.get_agent('video_fetcher')
            return agent.execute(payload)
        
        def handle_fetch_covers(payload: Dict[str, Any]) -> Dict[str, Any]:
            """处理封面获取任务"""
            agent = self.orchestrator.get_agent('cover_fetcher')
            return agent.execute(payload)
        
        def handle_generate_grid(payload: Dict[str, Any]) -> Dict[str, Any]:
            """处理宫图生成任务"""
            agent = self.orchestrator.get_agent('grid_generator')
            return agent.execute(payload)
        
        def handle_upload_feishu(payload: Dict[str, Any]) -> Dict[str, Any]:
            """处理飞书上传任务"""
            agent = self.orchestrator.get_agent('feishu_uploader')
            return agent.execute(payload)
        
        def handle_full_pipeline(payload: Dict[str, Any]) -> Dict[str, Any]:
            """处理完整流程任务（含 LLM 分析）"""
            tk_handle = payload['tk_handle']
            kalodata_url = payload.get('kalodata_url')  # 使用kalodata_url替代tk_url
            record_id = payload['record_id']
            
            # 根据是否启用 LLM 分析调整步骤数（打分+风格合并为1步，带货标签1步，共6步）
            total_steps = 6 if self.enable_llm_analysis else 4
            
            print(f"\n{'='*70}")
            print(f"处理达人: {tk_handle}")
            print(f"{'='*70}")
            
            try:
                # 步骤1: 获取视频ID、封面URL、播放量和成交金额（从Kalodata）
                print(f"步骤 1/{total_steps}: 从Kalodata获取视频数据")
                video_agent = self.orchestrator.get_agent('video_fetcher')
                video_result = video_agent.execute({
                    'tk_handle': tk_handle,
                    'kalodata_url': kalodata_url,
                    'max_videos': 48
                })
                video_ids = video_result['video_ids']
                cover_urls = video_result.get('cover_urls', [])
                views_list = video_result.get('views_list', [])
                revenue_list = video_result.get('revenue_list', [])
                print(f"  ✅ 获取到 {len(video_ids)} 个视频，{len(cover_urls)} 个封面")
                
                # 步骤2: 确保封面URL（如果VideoFetcher已返回则跳过）
                if not cover_urls or len(cover_urls) < 12:
                    print(f"步骤 2/{total_steps}: 补充获取封面")
                    cover_agent = self.orchestrator.get_agent('cover_fetcher')
                    cover_result = cover_agent.execute({
                        'tk_handle': tk_handle,
                        'video_ids': video_ids,
                        'cover_urls': cover_urls,
                        'views_list': views_list,
                        'revenue_list': revenue_list
                    })
                    cover_urls = cover_result['cover_urls']
                    views_list = cover_result.get('views_list', views_list)
                    revenue_list = cover_result.get('revenue_list', revenue_list)
                    print(f"  ✅ 获取到 {len(cover_urls)} 个封面")
                else:
                    print(f"步骤 2/{total_steps}: 跳过（已有封面URL）")
                
                if len(cover_urls) < 6:
                    raise Exception(f"封面数量不足: {len(cover_urls)}/6（最少需要6张）")
                
                # 步骤3: 生成宫图（多张，每12张一组，带播放量和成交金额）
                print(f"步骤 3/{total_steps}: 生成宫图")
                grid_agent = self.orchestrator.get_agent('grid_generator')
                grid_result = grid_agent.execute({
                    'tk_handle': tk_handle,
                    'cover_urls': cover_urls,
                    'views_list': views_list,
                    'revenue_list': revenue_list,
                    'output_dir': str(OUTPUT_DIR)
                })
                grid_paths = grid_result.get('grid_paths', [grid_result['grid_path']])
                grid_count = grid_result.get('grid_count', 1)
                print(f"  ✅ 生成 {grid_count} 张宫格图，使用 {grid_result['image_count']} 张封面")
                
                # 步骤4: 上传到飞书（支持多张宫格图）
                print(f"步骤 4/{total_steps}: 上传到飞书")
                upload_agent = self.orchestrator.get_agent('feishu_uploader')
                upload_result = upload_agent.execute({
                    'tk_handle': tk_handle,
                    'grid_paths': grid_paths,  # 传递多张宫格图
                    'grid_path': grid_paths[0],  # 兼容旧接口
                    'record_id': record_id,
                    'app_token': self.app_token,
                    'table_id': self.table_id
                })
                print(f"  ✅ 上传成功（{upload_result.get('grid_count', 1)} 张宫格图）")
                
                # ============ LLM 分析步骤（合并调用节省 token） ============
                # 打分 + 风格打标 合并为一次 LLM 调用
                # 带货标签打标 保持独立（不同的分析逻辑）
                scoring_result = None
                category_result = None
                vibe_result = None
                
                if self.enable_llm_analysis:
                    # ============ 步骤5: 视频评分 + 风格打标（合并 LLM 调用） ============
                    print(f"步骤 5/{total_steps}: 视频评分 + 风格打标（LLM 合并调用）")
                    try:
                        combined_result = self.combined_scoring_vibe_agent.execute({
                            'tk_handle': tk_handle,
                            'grid_paths': grid_paths,
                            'views_list': views_list
                        })
                        
                        # 提取评分结果并更新飞书
                        # 使用 safe_int/safe_float 确保类型正确
                        def safe_int(val, default=3, min_val=1, max_val=5):
                            if val is None:
                                return default
                            try:
                                int_val = int(float(val))
                                if min_val <= int_val <= max_val:
                                    return int_val
                                return default
                            except (ValueError, TypeError):
                                return default
                        
                        def safe_float(val, default=3.0, min_val=1.0, max_val=5.0):
                            if val is None:
                                return default
                            try:
                                float_val = float(val)
                                if min_val <= float_val <= max_val:
                                    return round(float_val, 1)
                                return default
                            except (ValueError, TypeError):
                                return default
                        
                        scoring_result = {
                            'analysis_reason': combined_result.get('analysis_reason', ''),
                            'score_traffic': safe_int(combined_result.get('score_traffic')),
                            'score_presence': safe_int(combined_result.get('score_presence')),
                            'score_consistency': safe_int(combined_result.get('score_consistency')),
                            'score_lighting': safe_int(combined_result.get('score_lighting')),
                            'score_background': safe_int(combined_result.get('score_background')),
                            'total_score': safe_int(combined_result.get('total_score'), 15, 5, 25),
                            'final_star_rating': safe_float(combined_result.get('final_star_rating'))
                        }
                        self.feishu_updater.update_scoring_result(
                            record_id=record_id,
                            app_token=self.app_token,
                            table_id=self.table_id,
                            scoring_result=scoring_result
                        )
                        print(f"     ✅ 评分已更新: {scoring_result['final_star_rating']} 星")
                        
                        # 提取风格结果并更新飞书
                        vibe_result = {
                            'vibe_reason': combined_result.get('vibe_reason', ''),
                            'vibe_tag': combined_result.get('vibe_tag', 'Unknown')
                        }
                        self.feishu_updater.update_vibe_result(
                            record_id=record_id,
                            app_token=self.app_token,
                            table_id=self.table_id,
                            vibe_result=vibe_result
                        )
                        print(f"     ✅ 风格已更新: {vibe_result['vibe_tag']}")
                        
                    except Exception as e:
                        print(f"     ⚠️ 评分/风格分析失败（不影响后续流程）: {e}")
                    
                    # ============ 步骤6: 带货标签打标（独立 LLM 调用） ============
                    print(f"步骤 6/{total_steps}: 带货标签打标（LLM）")
                    try:
                        # 添加线程超时保护（60秒）
                        import concurrent.futures
                        with concurrent.futures.ThreadPoolExecutor() as executor:
                            future = executor.submit(
                                self.category_tagging_agent.execute,
                                {'tk_handle': tk_handle, 'grid_paths': grid_paths}
                            )
                            try:
                                category_result = future.result(timeout=60)
                            except concurrent.futures.TimeoutError:
                                print(f"     ⚠️ 带货标签打标超时（60秒），跳过此步骤")
                                category_result = None
                        
                        if category_result:
                            # 更新飞书标签字段
                            self.feishu_updater.update_category_result(
                                record_id=record_id,
                                app_token=self.app_token,
                                table_id=self.table_id,
                                category_result=category_result
                            )
                            print(f"     ✅ 标签已更新: {category_result['main_category_1']}")
                        
                    except Exception as e:
                        print(f"     ⚠️ 打标失败（不影响后续流程）: {e}")
                
                return {
                    'tk_handle': tk_handle,
                    'video_count': len(video_ids),
                    'cover_count': len(cover_urls),
                    'grid_paths': grid_paths,
                    'grid_path': grid_paths[0],
                    'grid_count': grid_count,
                    'file_token': upload_result['file_token'],
                    'scoring_result': scoring_result,
                    'category_result': category_result,
                    'vibe_result': vibe_result,
                    'status': 'success'
                }
                
            except Exception as e:
                print(f"  ❌ 失败: {e}")
                raise e
        
        # 注册所有处理器
        self.executor.register_handler('fetch_videos', handle_fetch_videos)
        self.executor.register_handler('fetch_covers', handle_fetch_covers)
        self.executor.register_handler('generate_grid', handle_generate_grid)
        self.executor.register_handler('upload_feishu', handle_upload_feishu)
        self.executor.register_handler('full_pipeline', handle_full_pipeline)
    
    def add_creator_task(self, creator: Dict[str, Any], priority: TaskPriority = TaskPriority.NORMAL):
        """添加达人处理任务"""
        task = Task(
            task_id=f"creator_{creator['tk_handle']}_{int(time.time())}",
            task_type='full_pipeline',
            payload=creator,
            priority=priority
        )
        self.task_queue.add_task(task)
        return task.task_id
    
    def add_batch_tasks(self, creators: List[Dict[str, Any]], batch_size: int = None, batch_interval: int = None):
        """
        批量添加任务
        
        注意：批次控制已移到任务执行层面（通过限流器控制），
        这里直接添加所有任务到队列，避免阻塞。
        
        Args:
            creators: 达人列表
            batch_size: 已废弃，保留参数兼容性
            batch_interval: 已废弃，保留参数兼容性
        
        Returns:
            task_ids: 任务 ID 列表
        """
        task_ids = []
        total = len(creators)
        
        print(f"\n📦 添加 {total} 个任务到队列...")
        
        for i, creator in enumerate(creators):
            task_id = self.add_creator_task(creator)
            task_ids.append(task_id)
            
            # 每 50 个任务打印一次进度
            if (i + 1) % 50 == 0:
                print(f"   已添加 {i + 1}/{total} 个任务...")
        
        print(f"✅ 已添加 {len(task_ids)} 个任务到队列")
        print(f"   限流策略: Kalodata API 3请求/10秒（反爬保护）")
        print(f"   LLM API: 无限流（付费接口，全速执行）")
        return task_ids
        
        print(f"\n✅ 共添加 {len(task_ids)} 个任务到队列")
        return task_ids
    
    def start(self):
        """启动自动化"""
        if self.running:
            return
        
        self.running = True
        self.executor.start()
        print("🚀 自动化系统启动")
    
    def stop(self):
        """停止自动化"""
        if not self.running:
            return
        
        self.running = False
        self.executor.stop()
        print("🛑 自动化系统停止")
    
    def wait_completion(self):
        """等待所有任务完成"""
        self.executor.wait_completion()
    
    def print_summary(self):
        """打印摘要"""
        stats = self.task_queue.get_stats()
        
        print("\n" + "="*70)
        print("任务执行摘要")
        print("="*70)
        print(f"总任务数: {stats['total']}")
        print(f"成功: {stats['success']}")
        print(f"失败: {stats['failed']}")
        print(f"待处理: {stats['pending']}")
        print(f"运行中: {stats['running']}")
        print(f"重试: {stats['retry']}")
        print("="*70)
        
        # 打印子智能体统计
        self.orchestrator.print_stats()
        
        # 打印 LLM 分析统计
        if self.enable_llm_analysis:
            print("\n" + "-"*70)
            print("LLM 分析统计")
            print("-"*70)
            
            # 合并智能体统计（打分 + 风格）
            combined_stats = self.combined_scoring_vibe_agent.get_stats()
            print(f"\n评分+风格（合并调用）:")
            print(f"  处理: {combined_stats['processed']}")
            print(f"  成功: {combined_stats['success']}")
            print(f"  失败: {combined_stats['failed']}")
            if 'avg_time' in combined_stats:
                print(f"  平均耗时: {combined_stats['avg_time']:.2f}s")
            
            category_stats = self.category_tagging_agent.get_stats()
            print(f"\n带货标签:")
            print(f"  处理: {category_stats['processed']}")
            print(f"  成功: {category_stats['success']}")
            print(f"  失败: {category_stats['failed']}")
            if 'avg_time' in category_stats:
                print(f"  平均耗时: {category_stats['avg_time']:.2f}s")
            
            print("="*70)


def load_creators_from_feishu() -> List[Dict[str, Any]]:
    """从飞书加载待处理达人列表"""
    # TODO: 实现从飞书API读取
    # 这里使用硬编码数据作为示例
    
    creators = [
        {
            "record_id": "recvdmcV3UawMI", 
            "tk_handle": "anuchittum1", 
            "kalodata_url": "https://www.kalodata.com/creator/detail?id=7207725887499846682&language=zh-CN&currency=CNY&region=TH"
        },
        {
            "record_id": "recvdmcVxL2Q1m", 
            "tk_handle": "soe..moe..kyi", 
            "kalodata_url": "https://www.kalodata.com/creator/detail?id=7207725887499846682&language=zh-CN&currency=CNY&region=TH"
        },
        {
            "record_id": "recvdmcW2lzIvC", 
            "tk_handle": "fluke_0171", 
            "kalodata_url": "https://www.kalodata.com/creator/detail?id=7207725887499846682&language=zh-CN&currency=CNY&region=TH"
        },
        {
            "record_id": "recvdmeQnHq3xE", 
            "tk_handle": "beholiday.official", 
            "kalodata_url": "https://www.kalodata.com/creator/detail?id=7207725887499846682&language=zh-CN&currency=CNY&region=TH"
        },
        {
            "record_id": "recvdmeQQOzExE", 
            "tk_handle": "narikapuy", 
            "kalodata_url": "https://www.kalodata.com/creator/detail?id=7207725887499846682&language=zh-CN&currency=CNY&region=TH"
        },
    ]
    
    return creators


def main():
    """主函数"""
    
    print("="*70)
    print("自动化脚本 v2.0 - 子智能体架构")
    print("="*70)
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    # 创建编排器
    automation = AutomationOrchestrator()
    
    # 信号处理
    def signal_handler(sig, frame):
        print("\n\n⚠️ 收到中断信号，正在停止...")
        automation.stop()
        automation.print_summary()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # 加载待处理达人
        print("\n📋 加载待处理达人...")
        creators = load_creators_from_feishu()
        print(f"✅ 加载 {len(creators)} 个达人")
        
        # 添加任务
        print("\n📝 添加任务到队列...")
        task_ids = automation.add_batch_tasks(creators)
        print(f"✅ 添加 {len(task_ids)} 个任务")
        
        # 启动执行
        print("\n🚀 启动任务执行...")
        automation.start()
        
        # 等待完成
        print("\n⏳ 等待任务完成...\n")
        automation.wait_completion()
        
        # 停止
        automation.stop()
        
        # 打印摘要
        automation.print_summary()
        
        print(f"\n结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)
        
    except Exception as e:
        print(f"\n❌ 错误: {e}")
        import traceback
        traceback.print_exc()
        automation.stop()
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
