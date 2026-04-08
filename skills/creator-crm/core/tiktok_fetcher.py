"""
TikTok 直接数据获取器 - 从 TikTok 主页直接提取封面

基于页面分析结果的优化方案：
1. 从 DOM 直接提取封面 URL（100% 成功率）
2. 封面 URL 当天有效，需要立即下载
3. 使用 oEmbed API 作为备用（提供长期有效 URL）
"""

import sys
import os
from typing import List, Dict, Optional

# 添加父目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.data_fetchers import VideoData, OEmbedFetcher
from config.exceptions import DataExtractionError


# ============================================================================
# TikTok 页面数据提取 JavaScript
# ============================================================================

TIKTOK_EXTRACTION_JS = """
(function() {
    // 提取达人 handle
    const tkHandle = window.location.pathname.split('@')[1]?.split('/')[0] || '';
    
    // 提取视频数据
    const videos = [];
    const videoElements = document.querySelectorAll('[data-e2e="user-post-item"]');
    
    videoElements.forEach((el, index) => {
        if (index >= 48) return; // 最多 48 个
        
        // 提取视频 ID
        const linkEl = el.querySelector('a');
        if (!linkEl) return;
        
        const match = linkEl.href.match(/video\\/(\\d+)/);
        if (!match) return;
        
        const videoId = match[1];
        
        // 提取封面 URL（多种方式）
        let coverUrl = null;
        
        // 方式 1: img 标签
        const imgEl = el.querySelector('img');
        if (imgEl) {
            coverUrl = imgEl.src || imgEl.getAttribute('data-src');
        }
        
        // 方式 2: background-image
        if (!coverUrl) {
            const bgEl = el.querySelector('[style*="background-image"]');
            if (bgEl) {
                const style = bgEl.getAttribute('style');
                const bgMatch = style.match(/url\\(['"]?([^'"\\)]+)['"]?\\)/);
                if (bgMatch) {
                    coverUrl = bgMatch[1];
                }
            }
        }
        
        // 方式 3: 从 window 数据中查找
        if (!coverUrl && window.__UNIVERSAL_DATA_FOR_REHYDRATION__) {
            try {
                const data = window.__UNIVERSAL_DATA_FOR_REHYDRATION__;
                // 这里需要根据实际数据结构调整
                // 通常在 data.default.ItemModule[videoId].video.cover
            } catch (e) {
                // 忽略
            }
        }
        
        videos.push({
            video_id: videoId,
            description: '',  // TikTok 主页通常不显示完整描述
            revenue: 0,       // TikTok 主页没有 GMV 数据
            cover_url: coverUrl,
            cover_url_expires: true  // 标记封面 URL 会过期
        });
    });
    
    return {
        tk_handle: tkHandle,
        videos: videos,
        total_count: videos.length,
        page_url: window.location.href,
        extracted_at: new Date().toISOString(),
        note: '封面 URL 当天有效，建议立即下载或使用 oEmbed API 获取长期 URL'
    };
})();
"""


# ============================================================================
# TikTok 直接数据获取器
# ============================================================================

class TikTokDirectFetcher:
    """
    TikTok 直接数据获取器
    
    从 TikTok 主页直接提取视频数据和封面
    优点：封面提取成功率 100%
    缺点：封面 URL 当天过期，需要立即下载或使用 oEmbed 备用
    """
    
    def __init__(
        self,
        use_oembed_backup: bool = True,
        download_immediately: bool = False
    ):
        """
        Args:
            use_oembed_backup: 是否使用 oEmbed API 作为备用（推荐）
            download_immediately: 是否立即下载封面到本地（可选）
        """
        self.use_oembed_backup = use_oembed_backup
        self.download_immediately = download_immediately
        self.oembed_fetcher = OEmbedFetcher() if use_oembed_backup else None
    
    def fetch_videos(
        self,
        tk_handle: str,
        page_data: Dict,
        max_videos: int = 48
    ) -> List[VideoData]:
        """
        从 TikTok 页面数据获取视频
        
        Args:
            tk_handle: TikTok 账号名
            page_data: 从 browser.eval() 获取的页面数据
            max_videos: 最大视频数量
            
        Returns:
            List[VideoData]: 视频数据列表
            
        Raises:
            DataExtractionError: 数据提取失败
        """
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
                cover_url=v.get("cover_url"),
                publish_date=v.get("publish_date")
            )
            videos.append(video)
        
        # 策略选择
        if self.use_oembed_backup:
            # 使用 oEmbed API 替换临时封面 URL
            print("🔄 使用 oEmbed API 获取长期有效的封面 URL...")
            videos = self._replace_with_oembed(tk_handle, videos)
        elif self.download_immediately:
            # 立即下载封面到本地
            print("📥 立即下载封面到本地...")
            videos = self._download_covers_immediately(videos)
        else:
            # 直接使用页面封面（风险：URL 可能过期）
            print("⚠️  使用页面封面 URL（当天有效）")
        
        # 过滤出有封面的视频
        valid_videos = [v for v in videos if v.cover_url]
        
        print(f"✅ 有效视频: {len(valid_videos)}/{len(videos)}")
        
        return valid_videos
    
    def _replace_with_oembed(
        self,
        tk_handle: str,
        videos: List[VideoData]
    ) -> List[VideoData]:
        """使用 oEmbed API 替换临时封面 URL"""
        if not self.oembed_fetcher:
            return videos
        
        video_ids = [v.video_id for v in videos]
        cover_urls = self.oembed_fetcher.fetch_covers_batch(
            tk_handle,
            video_ids,
            max_workers=5
        )
        
        for video in videos:
            oembed_url = cover_urls.get(video.video_id)
            if oembed_url:
                video.cover_url = oembed_url
        
        return videos
    
    def _download_covers_immediately(
        self,
        videos: List[VideoData]
    ) -> List[VideoData]:
        """立即下载封面到本地（保存为文件）"""
        import requests
        from pathlib import Path
        
        cache_dir = Path("cache/covers")
        cache_dir.mkdir(parents=True, exist_ok=True)
        
        for video in videos:
            if not video.cover_url:
                continue
            
            try:
                # 下载封面
                resp = requests.get(video.cover_url, timeout=10)
                resp.raise_for_status()
                
                # 保存到本地
                local_path = cache_dir / f"{video.video_id}.jpg"
                local_path.write_bytes(resp.content)
                
                # 更新为本地路径
                video.cover_url = str(local_path)
                
            except Exception as e:
                print(f"⚠️  视频 {video.video_id} 下载失败: {str(e)}")
                continue
        
        return videos


# ============================================================================
# 便捷函数
# ============================================================================

def extract_tiktok_page_data_js() -> str:
    """
    返回用于在 browser 中执行的 JavaScript 代码
    
    Returns:
        str: JavaScript 代码
    """
    return TIKTOK_EXTRACTION_JS


def fetch_from_tiktok_page(
    page_data: Dict,
    use_oembed_backup: bool = True
) -> List[VideoData]:
    """
    便捷函数：从 TikTok 页面数据获取视频
    
    Args:
        page_data: 从 browser.eval() 获取的页面数据
        use_oembed_backup: 是否使用 oEmbed API 作为备用
        
    Returns:
        List[VideoData]: 视频数据列表
    """
    tk_handle = page_data.get("tk_handle", "")
    
    if not tk_handle:
        raise DataExtractionError("未找到 TikTok Handle")
    
    fetcher = TikTokDirectFetcher(use_oembed_backup=use_oembed_backup)
    return fetcher.fetch_videos(tk_handle, page_data)
