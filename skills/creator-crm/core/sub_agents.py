#!/usr/bin/env python3
"""
子智能体系统
专门处理特定类型的任务，减轻主控Agent负担
"""

import sys
import json
import time
from pathlib import Path
from typing import Dict, Any, Optional, List
from abc import ABC, abstractmethod

# 添加路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.task_queue import Task, TaskStatus
from core.rate_limiter import (
    RateLimiter, RateLimiterConfig,
    CircuitBreaker, CircuitBreakerConfig
)


class SubAgent(ABC):
    """子智能体基类"""
    
    def __init__(self, agent_id: str, rate_limiter: Optional[RateLimiter] = None,
                 circuit_breaker: Optional[CircuitBreaker] = None):
        self.agent_id = agent_id
        self.rate_limiter = rate_limiter
        self.circuit_breaker = circuit_breaker
        self.stats = {
            'processed': 0,
            'success': 0,
            'failed': 0,
            'total_time': 0.0
        }
    
    @abstractmethod
    def process(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """处理任务"""
        pass
    
    def execute(self, payload: Dict[str, Any], rate_limit_timeout: float = 60.0) -> Dict[str, Any]:
        """执行任务（带限流和熔断）
        
        Args:
            payload: 任务参数
            rate_limit_timeout: 限流器获取令牌的超时时间（秒），默认60秒
        """
        start_time = time.time()
        
        try:
            # 限流（使用明确的超时时间，防止无限等待）
            if self.rate_limiter:
                acquired = self.rate_limiter.acquire(timeout=rate_limit_timeout)
                if not acquired:
                    raise Exception(f"[{self.agent_id}] 限流超时（等待超过{rate_limit_timeout}秒）")
            
            # 熔断
            if self.circuit_breaker:
                result = self.circuit_breaker.call(self.process, payload)
            else:
                result = self.process(payload)
            
            # 统计
            self.stats['processed'] += 1
            self.stats['success'] += 1
            self.stats['total_time'] += time.time() - start_time
            
            return result
            
        except Exception as e:
            self.stats['processed'] += 1
            self.stats['failed'] += 1
            self.stats['total_time'] += time.time() - start_time
            # 添加更详细的错误信息
            error_msg = f"[{self.agent_id}] 任务执行失败: {e}"
            print(f"❌ {error_msg}")
            raise e
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        stats = self.stats.copy()
        if stats['processed'] > 0:
            stats['avg_time'] = stats['total_time'] / stats['processed']
            stats['success_rate'] = stats['success'] / stats['processed']
        return stats


class VideoFetcherAgent(SubAgent):
    """视频获取子智能体 - 负责从TikTok获取视频ID"""
    
    def __init__(self):
        # 配置限流：每10秒最多3个请求（更保守，避免风控）
        rate_limiter = RateLimiter(RateLimiterConfig(
            max_requests=3,
            time_window=10.0,
            burst_size=1
        ))
        
        # 配置熔断：3次失败后熔断120秒（更保守）
        circuit_breaker = CircuitBreaker(CircuitBreakerConfig(
            failure_threshold=3,
            success_threshold=2,
            timeout=120.0
        ))
        
        super().__init__("video_fetcher", rate_limiter, circuit_breaker)
    
    def process(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        获取视频ID
        
        Args:
            payload: {
                'tk_handle': str,
                'kalodata_url': str,  # Kalodata达人详情页URL
                'max_videos': int (default: 48)
            }
        
        Returns:
            {
                'tk_handle': str,
                'video_ids': List[str],
                'cover_urls': List[str],  # 同时返回封面URL
                'video_count': int
            }
        """
        from core.kalodata_fetcher import KalodataFetcher
        
        tk_handle = payload['tk_handle']
        kalodata_url = payload.get('kalodata_url')
        max_videos = payload.get('max_videos', 48)
        
        print(f"  🎬 [{self.agent_id}] 获取视频: {tk_handle}")
        
        if not kalodata_url:
            raise ValueError(f"缺少 kalodata_url 参数")
        
        try:
            # 使用 KalodataFetcher 获取视频数据
            fetcher = KalodataFetcher()
            videos = fetcher.fetch_videos_from_url(kalodata_url, max_videos=max_videos)
            
            if not videos:
                raise Exception(f"未获取到视频数据")
            
            # 提取视频ID、封面URL、播放量和成交金额
            video_ids = [v.video_id for v in videos]
            cover_urls = [v.cover_url for v in videos]
            views_list = [v.views for v in videos]
            revenue_list = [v.gmv for v in videos]
            
            print(f"    ✅ 获取到 {len(video_ids)} 个视频，{len(cover_urls)} 个封面")
            
            return {
                'tk_handle': tk_handle,
                'video_ids': video_ids,
                'cover_urls': cover_urls,  # 直接返回封面URL，避免CoverFetcher重复获取
                'views_list': views_list,   # 播放量列表
                'revenue_list': revenue_list,  # 成交金额列表
                'video_count': len(video_ids)
            }
            
        except Exception as e:
            print(f"    ❌ 获取失败: {e}")
            raise e


class CoverFetcherAgent(SubAgent):
    """封面获取子智能体 - 负责获取视频封面URL"""
    
    def __init__(self):
        # 配置限流：每秒最多10个请求
        rate_limiter = RateLimiter(RateLimiterConfig(
            max_requests=10,
            time_window=1.0,
            burst_size=5
        ))
        
        # 配置熔断
        circuit_breaker = CircuitBreaker(CircuitBreakerConfig(
            failure_threshold=10,
            success_threshold=3,
            timeout=30.0
        ))
        
        super().__init__("cover_fetcher", rate_limiter, circuit_breaker)
    
    def process(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        获取封面URL
        
        Args:
            payload: {
                'tk_handle': str,
                'video_ids': List[str],
                'cover_urls': List[str] (optional) - 如果已有封面URL则直接使用
            }
        
        Returns:
            {
                'tk_handle': str,
                'cover_urls': List[str],
                'cover_count': int
            }
        """
        tk_handle = payload['tk_handle']
        video_ids = payload.get('video_ids', [])
        cover_urls = payload.get('cover_urls', [])
        views_list = payload.get('views_list', [])
        revenue_list = payload.get('revenue_list', [])
        
        print(f"  🖼️  [{self.agent_id}] 获取封面: {tk_handle}")
        
        # 如果已经有封面URL（从VideoFetcherAgent返回），直接使用（不截取，保留全部）
        if cover_urls:
            print(f"    ✅ 使用已有封面 {len(cover_urls)} 个")
            return {
                'tk_handle': tk_handle,
                'cover_urls': cover_urls,  # 保留全部封面，不截取
                'views_list': views_list,
                'revenue_list': revenue_list,
                'cover_count': len(cover_urls)
            }
        
        # 否则使用OEmbed方式获取（备用方案）
        from core.data_fetchers import OEmbedFetcher
        
        print(f"    ⚠️ 使用OEmbed备用方案获取封面 ({len(video_ids)} 个视频)")
        
        fetcher = OEmbedFetcher(cache_enabled=True, timeout=10)
        cover_urls = []
        
        for video_id in video_ids:  # 获取全部
            try:
                url = fetcher.fetch_cover_url(tk_handle, video_id)
                if url:
                    cover_urls.append(url)
            except Exception as e:
                print(f"    ⚠️ 封面获取失败: {video_id} - {e}")
        
        return {
            'tk_handle': tk_handle,
            'cover_urls': cover_urls,
            'views_list': views_list,
            'revenue_list': revenue_list,
            'cover_count': len(cover_urls)
        }


class GridGeneratorAgent(SubAgent):
    """宫图生成子智能体 - 负责生成宫格图片"""
    
    def __init__(self):
        # 配置限流：每5秒最多3个请求（图片处理较慢）
        rate_limiter = RateLimiter(RateLimiterConfig(
            max_requests=3,
            time_window=5.0,
            burst_size=1
        ))
        
        # 配置熔断
        circuit_breaker = CircuitBreaker(CircuitBreakerConfig(
            failure_threshold=3,
            success_threshold=2,
            timeout=60.0
        ))
        
        super().__init__("grid_generator", rate_limiter, circuit_breaker)
    
    def process(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        生成宫图（支持多张，每12张封面一组）
        
        Args:
            payload: {
                'tk_handle': str,
                'cover_urls': List[str],       # 全部封面URL（最多48张）
                'views_list': List[int],        # 播放量列表（可选）
                'revenue_list': List[float],    # 成交金额列表（可选）
                'output_dir': str (optional)
            }
        
        Returns:
            {
                'tk_handle': str,
                'grid_paths': List[str],  # 所有宫格图路径
                'grid_path': str,         # 第一张宫格图路径（兼容旧接口）
                'image_count': int,
                'grid_count': int
            }
        """
        import re
        from core.image_processor import GridCanvasGenerator
        
        tk_handle = payload['tk_handle']
        cover_urls = payload['cover_urls']
        views_list = payload.get('views_list', [])
        revenue_list = payload.get('revenue_list', [])
        output_dir = Path(payload.get('output_dir', 'output/grids'))
        
        print(f"  🎨 [{self.agent_id}] 生成宫图: {tk_handle} ({len(cover_urls)} 张图)")
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        generator = GridCanvasGenerator()
        
        # 清理文件名（移除特殊字符，避免文件系统问题）
        safe_handle = re.sub(r'[^\w\-_\. ]', '_', tk_handle)
        safe_handle = safe_handle.strip('_').strip()[:50]  # 限制长度
        if not safe_handle:
            safe_handle = f"creator_{hash(tk_handle) % 100000}"
        
        # 将封面URL按每12张分组，生成多张宫格图
        grid_paths = []
        total_downloaded = 0
        
        # 最多处理48张（4组）
        max_covers = min(len(cover_urls), 48)
        chunks = [cover_urls[i:i+12] for i in range(0, max_covers, 12)]
        
        for chunk_idx, chunk_urls in enumerate(chunks):
            if len(chunk_urls) < 6:
                print(f"  ⚠️ 第{chunk_idx+1}组封面不足6张（{len(chunk_urls)}张），跳过")
                break
            
            # 对应的播放量和成交金额（按原始索引对应）
            start_idx = chunk_idx * 12
            chunk_views_all = views_list[start_idx:start_idx+len(chunk_urls)] if views_list else []
            chunk_revenue_all = revenue_list[start_idx:start_idx+len(chunk_urls)] if revenue_list else []
            
            # 下载图片（保持顺序，失败的跳过）
            # 需要跟踪哪些图片成功下载，以便正确对应 views/revenue
            images_with_data = []
            for url_idx, url in enumerate(chunk_urls):
                img = generator.downloader.download_image(url)
                if img:
                    v = chunk_views_all[url_idx] if url_idx < len(chunk_views_all) else 0
                    r = chunk_revenue_all[url_idx] if url_idx < len(chunk_revenue_all) else 0.0
                    images_with_data.append((img, v, r))
            
            if len(images_with_data) < 6:
                print(f"  ⚠️ 第{chunk_idx+1}组图片下载不足6张（{len(images_with_data)}张），跳过")
                break
            
            images = [item[0] for item in images_with_data]
            chunk_views = [item[1] for item in images_with_data]
            chunk_revenue = [item[2] for item in images_with_data]
            
            # 创建画板（传入播放量和成交金额）
            canvas = generator.create_canvas(
                images,
                max_images=12,
                views_list=chunk_views if any(v > 0 for v in chunk_views) else None,
                revenue_list=chunk_revenue if any(r > 0 for r in chunk_revenue) else None
            )
            
            # 保存（多张时加序号）
            if len(chunks) > 1:
                output_path = output_dir / f"{safe_handle}_grid_{chunk_idx+1}.png"
            else:
                output_path = output_dir / f"{safe_handle}_grid.png"
            
            canvas.save(output_path, format='PNG')
            grid_paths.append(str(output_path))
            total_downloaded += len(images)
            
            print(f"  ✅ 第{chunk_idx+1}张宫格图已保存: {output_path.name} ({len(images)}张封面)")
        
        if not grid_paths:
            raise Exception(f"宫格图生成失败：封面下载不足")
        
        print(f"  ✅ 共生成 {len(grid_paths)} 张宫格图，使用 {total_downloaded} 张封面")
        
        return {
            'tk_handle': tk_handle,
            'grid_paths': grid_paths,
            'grid_path': grid_paths[0],  # 兼容旧接口
            'image_count': total_downloaded,
            'grid_count': len(grid_paths)
        }


class FeishuUploaderAgent(SubAgent):
    """飞书上传子智能体 - 负责上传图片到飞书"""
    
    def __init__(self):
        # 配置限流：每秒最多2个请求（API限制）
        rate_limiter = RateLimiter(RateLimiterConfig(
            max_requests=2,
            time_window=1.0,
            burst_size=1
        ))
        
        # 配置熔断
        circuit_breaker = CircuitBreaker(CircuitBreakerConfig(
            failure_threshold=5,
            success_threshold=2,
            timeout=120.0  # 飞书API可能较慢
        ))
        
        super().__init__("feishu_uploader", rate_limiter, circuit_breaker)
        
        self.access_token: Optional[str] = None
        self.token_expires_at: float = 0
    
    def _get_access_token(self) -> str:
        """获取飞书access_token"""
        import requests
        
        # 检查token是否过期
        if self.access_token and time.time() < self.token_expires_at:
            return self.access_token
        
        # 获取新token
        config_file = Path.home() / ".openclaw/openclaw.json"
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        app_id = config['channels']['feishu']['appId']
        app_secret = config['channels']['feishu']['appSecret']
        
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        response = requests.post(url, json={'app_id': app_id, 'app_secret': app_secret})
        result = response.json()
        
        if result.get('code') != 0:
            raise Exception(f"获取access_token失败: {result.get('msg')}")
        
        self.access_token = result['tenant_access_token']
        self.token_expires_at = time.time() + result.get('expire', 7200) - 300  # 提前5分钟刷新
        
        return self.access_token
    
    def _upload_single_file(self, grid_path: Path, app_token: str, access_token: str) -> str:
        """上传单个文件，返回 file_token"""
        import requests
        
        upload_url = "https://open.feishu.cn/open-apis/drive/v1/medias/upload_all"
        
        with open(grid_path, 'rb') as f:
            files = {'file': (grid_path.name, f, 'image/png')}
            data = {
                'file_name': grid_path.name,
                'parent_type': 'bitable_image',
                'parent_node': app_token,
                'size': str(grid_path.stat().st_size)
            }
            headers = {'Authorization': f'Bearer {access_token}'}
            
            response = requests.post(upload_url, files=files, data=data, headers=headers)
            result = response.json()
            
            if result.get('code') != 0:
                raise Exception(f"上传文件失败: {result.get('msg')}")
            
            return result['data']['file_token']
    
    def process(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        上传到飞书（支持多张宫格图）
        
        Args:
            payload: {
                'tk_handle': str,
                'grid_path': str,           # 单张宫格图路径（兼容旧接口）
                'grid_paths': List[str],    # 多张宫格图路径（优先使用）
                'record_id': str,
                'app_token': str,
                'table_id': str
            }
        
        Returns:
            {
                'tk_handle': str,
                'file_tokens': List[str],
                'file_token': str,   # 第一张的token（兼容旧接口）
                'uploaded': bool,
                'grid_count': int
            }
        """
        import requests
        
        tk_handle = payload['tk_handle']
        # 优先使用 grid_paths（多张），兼容旧的 grid_path（单张）
        grid_paths_raw = payload.get('grid_paths') or [payload['grid_path']]
        grid_paths = [Path(p) for p in grid_paths_raw]
        record_id = payload['record_id']
        app_token = payload['app_token']
        table_id = payload['table_id']
        
        print(f"  ☁️  [{self.agent_id}] 上传到飞书: {tk_handle} ({len(grid_paths)} 张宫格图)")
        
        access_token = self._get_access_token()
        
        # 1. 上传所有宫格图文件
        file_tokens = []
        for i, grid_path in enumerate(grid_paths):
            file_token = self._upload_single_file(grid_path, app_token, access_token)
            file_tokens.append(file_token)
            print(f"    ✅ 第{i+1}张宫格图上传成功: {grid_path.name}")
        
        # 2. 更新记录（将所有宫格图附件写入视频截图字段，同时更新状态）
        update_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"
        
        # 构建附件列表（多张图片）
        attachments = [
            {
                'file_token': ft,
                'name': grid_paths[i].name,
                'type': 'image/png'
            }
            for i, ft in enumerate(file_tokens)
        ]
        
        payload_data = {
            'fields': {
                '视频截图': attachments,
                # 更新状态为"已完成"
                '视频宫图是否已生成': '已完成'
            }
        }
        
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        response = requests.put(update_url, json=payload_data, headers=headers)
        result = response.json()
        
        if result.get('code') != 0:
            raise Exception(f"更新记录失败: {result.get('msg')}")
        
        print(f"    ✅ 已更新状态为'已完成'（{len(file_tokens)} 张宫格图）")
        
        return {
            'tk_handle': tk_handle,
            'file_tokens': file_tokens,
            'file_token': file_tokens[0] if file_tokens else '',  # 兼容旧接口
            'uploaded': True,
            'grid_count': len(file_tokens)
        }


class AgentOrchestrator:
    """智能体编排器 - 协调多个子智能体"""
    
    def __init__(self):
        self.agents: Dict[str, SubAgent] = {}
    
    def register_agent(self, agent_type: str, agent: SubAgent):
        """注册子智能体"""
        self.agents[agent_type] = agent
        print(f"✅ 注册子智能体: {agent_type} ({agent.agent_id})")
    
    def get_agent(self, agent_type: str) -> Optional[SubAgent]:
        """获取子智能体"""
        return self.agents.get(agent_type)
    
    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """获取所有子智能体的统计信息"""
        return {
            agent_type: agent.get_stats()
            for agent_type, agent in self.agents.items()
        }
    
    def print_stats(self):
        """打印统计信息"""
        print("\n" + "="*70)
        print("子智能体统计")
        print("="*70)
        
        for agent_type, stats in self.get_all_stats().items():
            print(f"\n{agent_type}:")
            print(f"  处理: {stats['processed']}")
            print(f"  成功: {stats['success']}")
            print(f"  失败: {stats['failed']}")
            if 'avg_time' in stats:
                print(f"  平均耗时: {stats['avg_time']:.2f}s")
            if 'success_rate' in stats:
                print(f"  成功率: {stats['success_rate']*100:.1f}%")
        
        print("="*70)
