"""
数据获取器 - 统一的视频数据获取接口

支持多种数据源：
1. Kalodata API
2. Kalodata 页面数据
3. 本地缓存
"""

import os
import sys
import json
import hashlib
import requests
from pathlib import Path
from typing import List, Dict, Optional, Protocol
from concurrent.futures import ThreadPoolExecutor, as_completed

# 添加父目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.exceptions import (
    KalodataAPIError,
    DataExtractionError,
    InsufficientDataError,
    CacheError
)


# ============================================================================
# 数据结构
# ============================================================================

class VideoData:
    """视频数据结构"""
    
    def __init__(
        self,
        video_id: str,
        description: str,
        revenue: float,
        cover_url: Optional[str] = None,
        publish_date: Optional[str] = None,
        views: int = 0
    ):
        self.video_id = video_id
        self.description = description
        self.revenue = revenue
        self.cover_url = cover_url
        self.publish_date = publish_date
        self.views = views  # 视频播放量
    
    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            "video_id": self.video_id,
            "description": self.description,
            "revenue": self.revenue,
            "cover_url": self.cover_url,
            "publish_date": self.publish_date,
            "views": self.views
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> "VideoData":
        """从字典创建"""
        return cls(
            video_id=data["video_id"],
            description=data.get("description", ""),
            revenue=data.get("revenue", 0.0),
            cover_url=data.get("cover_url"),
            publish_date=data.get("publish_date"),
            views=data.get("views", 0)
        )
    
    def __repr__(self) -> str:
        return f"VideoData(id={self.video_id}, revenue={self.revenue}, views={self.views}, has_cover={self.cover_url is not None})"


# ============================================================================
# 数据获取器接口
# ============================================================================

class DataFetcher(Protocol):
    """数据获取器接口"""
    
    def fetch_videos(
        self,
        tk_handle: str,
        max_videos: int = 48
    ) -> List[VideoData]:
        """
        获取视频数据（包含封面）
        
        Args:
            tk_handle: TikTok 账号名
            max_videos: 最大视频数量
            
        Returns:
            List[VideoData]: 视频数据列表
            
        Raises:
            DataExtractionError: 数据提取失败
            InsufficientDataError: 数据不足
        """
        ...


# ============================================================================
# oEmbed 缓存管理器
# ============================================================================

class OEmbedCache:
    """oEmbed 封面缓存管理器"""
    
    def __init__(self, cache_dir: str = "cache/oembed"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_cache_key(self, tk_handle: str, video_id: str) -> str:
        """生成缓存键"""
        return hashlib.md5(f"{tk_handle}_{video_id}".encode()).hexdigest()
    
    def get(self, tk_handle: str, video_id: str) -> Optional[str]:
        """获取缓存的封面 URL"""
        cache_key = self._get_cache_key(tk_handle, video_id)
        cache_file = self.cache_dir / f"{cache_key}.json"
        
        if cache_file.exists():
            try:
                data = json.loads(cache_file.read_text())
                return data.get("cover_url")
            except Exception:
                return None
        
        return None
    
    def set(self, tk_handle: str, video_id: str, cover_url: str):
        """设置缓存"""
        cache_key = self._get_cache_key(tk_handle, video_id)
        cache_file = self.cache_dir / f"{cache_key}.json"
        
        try:
            cache_file.write_text(json.dumps({
                "cover_url": cover_url,
                "tk_handle": tk_handle,
                "video_id": video_id
            }))
        except Exception as e:
            raise CacheError(f"缓存写入失败: {str(e)}")
    
    def clear(self):
        """清空缓存"""
        for cache_file in self.cache_dir.glob("*.json"):
            cache_file.unlink()


# ============================================================================
# oEmbed 封面补全器
# ============================================================================

class OEmbedFetcher:
    """TikTok oEmbed 封面获取器（带缓存）"""
    
    def __init__(self, cache_enabled: bool = True, timeout: int = 5):
        self.cache_enabled = cache_enabled
        self.timeout = timeout
        self.cache = OEmbedCache() if cache_enabled else None
    
    def fetch_cover_url(
        self,
        tk_handle: str,
        video_id: str
    ) -> Optional[str]:
        """
        获取单个视频的封面 URL（带缓存）
        
        Args:
            tk_handle: TikTok 账号名
            video_id: 视频 ID
            
        Returns:
            Optional[str]: 封面 URL，失败返回 None
        """
        # 尝试从缓存读取
        if self.cache_enabled:
            cached_url = self.cache.get(tk_handle, video_id)
            if cached_url:
                return cached_url
        
        # 缓存未命中，调用 oEmbed API
        oembed_url = f"https://www.tiktok.com/oembed?url=https://www.tiktok.com/@{tk_handle}/video/{video_id}"
        
        try:
            resp = requests.get(oembed_url, timeout=self.timeout)
            resp.raise_for_status()
            oembed_data = resp.json()
            
            cover_url = oembed_data.get("thumbnail_url")
            
            if cover_url and self.cache_enabled:
                # 写入缓存
                self.cache.set(tk_handle, video_id, cover_url)
            
            return cover_url
            
        except Exception:
            return None
    
    def fetch_covers_batch(
        self,
        tk_handle: str,
        video_ids: List[str],
        max_workers: int = 5
    ) -> Dict[str, Optional[str]]:
        """
        批量获取封面 URL（并发 + 缓存）
        
        Args:
            tk_handle: TikTok 账号名
            video_ids: 视频 ID 列表
            max_workers: 并发数
            
        Returns:
            Dict[str, Optional[str]]: video_id -> cover_url 映射
        """
        results = {}
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self.fetch_cover_url, tk_handle, vid): vid
                for vid in video_ids
            }
            
            for future in as_completed(futures):
                video_id = futures[future]
                try:
                    cover_url = future.result()
                    results[video_id] = cover_url
                except Exception:
                    results[video_id] = None
        
        return results


# ============================================================================
# Kalodata API 数据获取器
# ============================================================================

class KalodataAPIFetcher:
    """Kalodata API 数据获取器"""
    
    def __init__(
        self,
        api_url: str,
        api_key: str,
        oembed_fetcher: Optional[OEmbedFetcher] = None
    ):
        self.api_url = api_url
        self.api_key = api_key
        self.oembed_fetcher = oembed_fetcher or OEmbedFetcher()
    
    def fetch_videos(
        self,
        tk_handle: str,
        max_videos: int = 48
    ) -> List[VideoData]:
        """
        从 Kalodata API 获取视频数据
        
        Args:
            tk_handle: TikTok 账号名
            max_videos: 最大视频数量
            
        Returns:
            List[VideoData]: 视频数据列表
            
        Raises:
            KalodataAPIError: API 调用失败
            InsufficientDataError: 数据不足
        """
        # 1. 调用 Kalodata API
        payload = {
            "creator_handle": tk_handle,
            "pageSize": max_videos,
            "sort": [{"field": "revenue", "type": "DESC"}]
        }
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.post(
                self.api_url,
                json=payload,
                headers=headers,
                timeout=10
            )
            response.raise_for_status()
            
        except requests.exceptions.Timeout:
            raise KalodataAPIError(408, "请求超时")
        except requests.exceptions.HTTPError as e:
            raise KalodataAPIError(
                e.response.status_code,
                f"HTTP 错误: {str(e)}"
            )
        except requests.exceptions.RequestException as e:
            raise KalodataAPIError(0, f"网络错误: {str(e)}")
        
        # 2. 解析响应
        try:
            data = response.json()
            videos_raw = data.get("data", {}).get("videos", [])
        except (json.JSONDecodeError, KeyError) as e:
            raise DataExtractionError(f"响应解析失败: {str(e)}")
        
        if not videos_raw:
            raise DataExtractionError("Kalodata API 返回空视频列表")
        
        # 3. 构建 VideoData 对象
        videos = []
        for v in videos_raw[:max_videos]:
            video = VideoData(
                video_id=v.get("id"),
                description=v.get("description", ""),
                revenue=v.get("revenue", 0.0),
                cover_url=v.get("cover_url") or v.get("thumbnail_url"),  # 优先使用 API 返回的封面
                publish_date=v.get("publish_date")
            )
            videos.append(video)
        
        # 4. 补全缺失的封面（并发）
        videos_without_cover = [v for v in videos if not v.cover_url]
        
        if videos_without_cover:
            video_ids = [v.video_id for v in videos_without_cover]
            cover_urls = self.oembed_fetcher.fetch_covers_batch(
                tk_handle,
                video_ids,
                max_workers=5
            )
            
            for video in videos_without_cover:
                video.cover_url = cover_urls.get(video.video_id)
        
        # 5. 过滤出有封面的视频
        valid_videos = [v for v in videos if v.cover_url]
        
        if len(valid_videos) < 12:
            raise InsufficientDataError(
                required=12,
                actual=len(valid_videos),
                data_type="有效视频（含封面）"
            )
        
        return valid_videos


# ============================================================================
# Kalodata 页面数据获取器
# ============================================================================

class KalodataPageFetcher:
    """Kalodata 页面数据获取器"""
    
    def __init__(self, oembed_fetcher: Optional[OEmbedFetcher] = None):
        self.oembed_fetcher = oembed_fetcher or OEmbedFetcher()
    
    def fetch_videos(
        self,
        tk_handle: str,
        max_videos: int = 48,
        page_data: Optional[Dict] = None
    ) -> List[VideoData]:
        """
        从 Kalodata 页面数据获取视频
        
        Args:
            tk_handle: TikTok 账号名
            max_videos: 最大视频数量
            page_data: 页面提取的数据（包含 videos 列表）
            
        Returns:
            List[VideoData]: 视频数据列表
            
        Raises:
            DataExtractionError: 数据提取失败
            InsufficientDataError: 数据不足
        """
        if not page_data:
            raise DataExtractionError("未提供页面数据")
        
        videos_raw = page_data.get("videos", [])
        
        if not videos_raw:
            raise DataExtractionError("页面数据中无视频列表")
        
        # 构建 VideoData 对象
        videos = []
        for v in videos_raw[:max_videos]:
            video = VideoData(
                video_id=v.get("video_id"),
                description=v.get("description", ""),
                revenue=v.get("revenue", 0.0),
                cover_url=v.get("cover_url"),  # 页面可能已有封面
                publish_date=v.get("publish_date")
            )
            videos.append(video)
        
        # 补全缺失的封面
        videos_without_cover = [v for v in videos if not v.cover_url]
        
        if videos_without_cover:
            video_ids = [v.video_id for v in videos_without_cover]
            cover_urls = self.oembed_fetcher.fetch_covers_batch(
                tk_handle,
                video_ids,
                max_workers=5
            )
            
            for video in videos_without_cover:
                video.cover_url = cover_urls.get(video.video_id)
        
        # 过滤出有封面的视频
        valid_videos = [v for v in videos if v.cover_url]
        
        if len(valid_videos) < 12:
            raise InsufficientDataError(
                required=12,
                actual=len(valid_videos),
                data_type="有效视频（含封面）"
            )
        
        return valid_videos
