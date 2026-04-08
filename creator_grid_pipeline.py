#!/usr/bin/env python3
"""
自动化流水线主程序 - Creator CRM 宫图生成流水线

功能：
1. 从飞书多维表格读取待处理达人列表
2. 获取视频封面和播放数据
3. 生成宫图（每个达人最多2张）
4. 上传到飞书多维表格

触发方式：
- 定时触发（cron）
- 手动触发（HTTP API）
- 命令行触发

使用方法：
    # 命令行触发
    python3 creator_grid_pipeline.py run
    
    # 启动 HTTP 服务（支持手动触发）
    python3 creator_grid_pipeline.py serve
    
    # 查看状态
    python3 creator_grid_pipeline.py status
"""

import sys
import json
import time
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import threading

from workspace_support import REPO_ROOT

# 添加路径
workspace_root = REPO_ROOT
sys.path.insert(0, str(workspace_root))
sys.path.insert(0, str(workspace_root / "skills/creator-crm"))

# 导入核心模块
from batch_processor_core import process_creator
from skills.creator_crm.utils.feishu_uploader import FeishuUploaderAgent

# 配置
PIPELINE_PORT = 8766
STATE_FILE = workspace_root / "output/pipeline_state.json"
LOG_FILE = workspace_root / "output/pipeline.log"


class PipelineLogger:
    """流水线日志记录器"""
    
    def __init__(self, log_file: Path):
        self.log_file = log_file
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
    
    def log(self, level: str, message: str):
        """记录日志"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] [{level}] {message}\n"
        
        # 打印到控制台
        print(log_entry.strip())
        
        # 写入文件
        try:
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(log_entry)
        except Exception as e:
            print(f"⚠️ 写入日志失败: {e}")
    
    def info(self, message: str):
        self.log("INFO", message)
    
    def error(self, message: str):
        self.log("ERROR", message)
    
    def success(self, message: str):
        self.log("SUCCESS", message)


class PipelineState:
    """流水线状态管理"""
    
    def __init__(self, state_file: Path):
        self.state_file = state_file
        self.state = self.load_state()
    
    def load_state(self) -> Dict[str, Any]:
        """加载状态"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                pass
        
        return {
            'last_run': None,
            'total_processed': 0,
            'total_success': 0,
            'total_failed': 0,
            'runs': []
        }
    
    def save_state(self):
        """保存状态"""
        try:
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"⚠️ 保存状态失败: {e}")
    
    def add_run(self, run_data: Dict[str, Any]):
        """添加运行记录"""
        self.state['runs'].append(run_data)
        self.state['last_run'] = run_data['started_at']
        self.state['total_processed'] += run_data.get('total', 0)
        self.state['total_success'] += run_data.get('success', 0)
        self.state['total_failed'] += run_data.get('failed', 0)
        
        # 只保留最近 100 次运行记录
        if len(self.state['runs']) > 100:
            self.state['runs'] = self.state['runs'][-100:]
        
        self.save_state()


class FeishuDataReader:
    """飞书数据读取器"""
    
    def __init__(self, logger: PipelineLogger):
        self.logger = logger
        self.uploader = FeishuUploaderAgent()
    
    def fetch_pending_creators(self) -> List[Dict[str, Any]]:
        """
        从飞书多维表格获取待处理的达人列表
        
        筛选条件：
        - 视频截图字段为空
        - 或者需要更新的达人
        
        Returns:
            待处理达人列表
        """
        self.logger.info("从飞书多维表格读取待处理达人...")
        
        try:
            # TODO: 实现飞书 API 调用
            # 这里需要根据实际的飞书 API 实现
            # 示例返回格式
            creators = []
            
            self.logger.success(f"成功读取 {len(creators)} 个待处理达人")
            return creators
            
        except Exception as e:
            self.logger.error(f"读取飞书数据失败: {e}")
            return []


class VideoDataFetcher:
    """视频数据获取器"""
    
    def __init__(self, logger: PipelineLogger):
        self.logger = logger
    
    def fetch_video_data(
        self,
        tk_handle: str,
        video_ids: List[str]
    ) -> Optional[List[Dict[str, Any]]]:
        """
        获取视频数据（播放量、带货金额等）
        
        Args:
            tk_handle: TikTok 用户名
            video_ids: 视频 ID 列表
        
        Returns:
            视频数据列表，包含 views 和 revenue
        """
        self.logger.info(f"获取 @{tk_handle} 的视频数据...")
        
        try:
            # TODO: 实现视频数据获取
            # 可以从 Kalodata API 或其他数据源获取
            # 示例返回格式
            video_data = []
            for vid in video_ids:
                video_data.append({
                    'video_id': vid,
                    'views': 0,  # 从数据源获取
                    'revenue': 0.0  # 从数据源获取
                })
            
            return video_data
            
        except Exception as e:
            self.logger.error(f"获取视频数据失败: {e}")
            return None


class CreatorGridPipeline:
    """达人宫图生成流水线"""
    
    def __init__(self):
        self.logger = PipelineLogger(LOG_FILE)
        self.state = PipelineState(STATE_FILE)
        self.feishu_reader = FeishuDataReader(self.logger)
        self.video_fetcher = VideoDataFetcher(self.logger)
    
    def run(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        运行流水线
        
        Args:
            limit: 限制处理数量（用于测试）
        
        Returns:
            运行结果统计
        """
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        started_at = datetime.now().isoformat()
        
        self.logger.info("="*70)
        self.logger.info(f"流水线开始运行 (ID: {run_id})")
        self.logger.info("="*70)
        
        # 步骤 1: 读取待处理达人
        self.logger.info("步骤 1/4: 读取待处理达人列表")
        creators = self.feishu_reader.fetch_pending_creators()
        
        if not creators:
            self.logger.info("没有待处理的达人，流水线结束")
            return {
                'run_id': run_id,
                'started_at': started_at,
                'completed_at': datetime.now().isoformat(),
                'total': 0,
                'success': 0,
                'failed': 0,
                'results': []
            }
        
        if limit:
            creators = creators[:limit]
            self.logger.info(f"限制处理数量: {limit}")
        
        self.logger.info(f"待处理达人数量: {len(creators)}")
        
        # 步骤 2-4: 处理每个达人
        results = []
        success_count = 0
        failed_count = 0
        
        for i, creator in enumerate(creators, 1):
            self.logger.info(f"\n[{i}/{len(creators)}] 处理达人: {creator.get('tk_handle')}")
            
            try:
                # 步骤 2: 获取视频数据
                video_data = self.video_fetcher.fetch_video_data(
                    creator['tk_handle'],
                    creator['video_ids']
                )
                
                # 步骤 3: 生成宫图
                creator_data = {
                    'record_id': creator['record_id'],
                    'tk_handle': creator['tk_handle'],
                    'video_ids': creator['video_ids'],
                    'video_data': video_data
                }
                
                result = process_creator(creator_data)
                results.append(result)
                
                if result['status'] == 'success':
                    success_count += 1
                    self.logger.success(f"✅ 成功处理: {creator['tk_handle']}")
                else:
                    failed_count += 1
                    self.logger.error(f"❌ 处理失败: {creator['tk_handle']} - {result.get('reason')}")
                
            except Exception as e:
                failed_count += 1
                self.logger.error(f"❌ 处理异常: {creator['tk_handle']} - {e}")
                results.append({
                    'status': 'failed',
                    'reason': str(e),
                    'tk_handle': creator['tk_handle']
                })
        
        # 汇总结果
        completed_at = datetime.now().isoformat()
        run_data = {
            'run_id': run_id,
            'started_at': started_at,
            'completed_at': completed_at,
            'total': len(creators),
            'success': success_count,
            'failed': failed_count,
            'results': results
        }
        
        # 保存运行记录
        self.state.add_run(run_data)
        
        # 打印汇总
        self.logger.info("\n" + "="*70)
        self.logger.info("流水线运行完成")
        self.logger.info("="*70)
        self.logger.info(f"✅ 成功: {success_count}")
        self.logger.info(f"❌ 失败: {failed_count}")
        self.logger.info(f"📊 总计: {len(creators)}")
        self.logger.info(f"⏱️  耗时: {self._calculate_duration(started_at, completed_at)}")
        
        return run_data
    
    def _calculate_duration(self, start: str, end: str) -> str:
        """计算耗时"""
        try:
            start_dt = datetime.fromisoformat(start)
            end_dt = datetime.fromisoformat(end)
            duration = (end_dt - start_dt).total_seconds()
            
            if duration < 60:
                return f"{duration:.1f}秒"
            elif duration < 3600:
                return f"{duration/60:.1f}分钟"
            else:
                return f"{duration/3600:.1f}小时"
        except:
            return "未知"
    
    def get_status(self) -> Dict[str, Any]:
        """获取流水线状态"""
        return {
            'last_run': self.state.state.get('last_run'),
            'total_processed': self.state.state.get('total_processed', 0),
            'total_success': self.state.state.get('total_success', 0),
            'total_failed': self.state.state.get('total_failed', 0),
            'recent_runs': self.state.state.get('runs', [])[-10:]  # 最近 10 次
        }


class PipelineHTTPHandler(BaseHTTPRequestHandler):
    """HTTP 请求处理器"""
    
    pipeline = None  # 类变量，由外部设置
    
    def do_GET(self):
        """处理 GET 请求"""
        parsed_path = urlparse(self.path)
        
        if parsed_path.path == '/status':
            # 查询状态
            status = self.pipeline.get_status()
            self._send_json_response(200, status)
        
        elif parsed_path.path == '/health':
            # 健康检查
            self._send_json_response(200, {'status': 'ok'})
        
        else:
            self._send_json_response(404, {'error': 'Not found'})
    
    def do_POST(self):
        """处理 POST 请求"""
        parsed_path = urlparse(self.path)
        
        if parsed_path.path == '/run':
            # 触发运行
            try:
                # 读取请求体
                content_length = int(self.headers.get('Content-Length', 0))
                body = self.rfile.read(content_length).decode('utf-8')
                params = json.loads(body) if body else {}
                
                # 运行流水线
                limit = params.get('limit')
                result = self.pipeline.run(limit=limit)
                
                self._send_json_response(200, result)
            
            except Exception as e:
                self._send_json_response(500, {'error': str(e)})
        
        else:
            self._send_json_response(404, {'error': 'Not found'})
    
    def _send_json_response(self, status_code: int, data: Dict[str, Any]):
        """发送 JSON 响应"""
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8'))
    
    def log_message(self, format, *args):
        """禁用默认日志"""
        pass


def serve(port: int = PIPELINE_PORT):
    """启动 HTTP 服务"""
    pipeline = CreatorGridPipeline()
    PipelineHTTPHandler.pipeline = pipeline
    
    server = HTTPServer(('0.0.0.0', port), PipelineHTTPHandler)
    
    print("="*70)
    print("Creator Grid Pipeline HTTP 服务")
    print("="*70)
    print(f"监听端口: {port}")
    print(f"\nAPI 端点:")
    print(f"  GET  http://localhost:{port}/status  - 查询状态")
    print(f"  GET  http://localhost:{port}/health  - 健康检查")
    print(f"  POST http://localhost:{port}/run     - 触发运行")
    print(f"\n按 Ctrl+C 停止服务")
    print("="*70)
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n\n服务已停止")
        server.shutdown()


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='Creator CRM 宫图生成流水线',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest='command', help='命令')
    
    # run 命令
    run_parser = subparsers.add_parser('run', help='运行流水线')
    run_parser.add_argument('--limit', type=int, help='限制处理数量（用于测试）')
    
    # serve 命令
    serve_parser = subparsers.add_parser('serve', help='启动 HTTP 服务')
    serve_parser.add_argument('--port', type=int, default=PIPELINE_PORT, help='监听端口')
    
    # status 命令
    subparsers.add_parser('status', help='查看状态')
    
    args = parser.parse_args()
    
    if args.command == 'run':
        # 运行流水线
        pipeline = CreatorGridPipeline()
        pipeline.run(limit=args.limit)
    
    elif args.command == 'serve':
        # 启动 HTTP 服务
        serve(port=args.port)
    
    elif args.command == 'status':
        # 查看状态
        pipeline = CreatorGridPipeline()
        status = pipeline.get_status()
        
        print("="*70)
        print("流水线状态")
        print("="*70)
        print(f"上次运行: {status['last_run'] or '从未运行'}")
        print(f"累计处理: {status['total_processed']}")
        print(f"累计成功: {status['total_success']}")
        print(f"累计失败: {status['total_failed']}")
        print(f"\n最近 {len(status['recent_runs'])} 次运行:")
        
        for run in status['recent_runs']:
            print(f"  - {run['run_id']}: {run['success']}/{run['total']} 成功")
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
