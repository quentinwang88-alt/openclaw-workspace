"""
达人风格分析 - 统一入口

整合所有模块，提供简洁的 API：
1. 支持多种数据源（Kalodata API / 页面数据）
2. 自动缓存 oEmbed 封面
3. 性能监控和日志
4. 完整的错误处理
"""

import os
import sys
from typing import Optional, Dict, List

# 添加父目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.data_schema import CreatorVibeAnalysis
from config.exceptions import (
    CreatorCRMError,
    KalodataAPIError,
    DataExtractionError,
    InsufficientDataError,
    VisionAPIError,
    ImageProcessingError
)
from core.data_fetchers import (
    VideoData,
    KalodataAPIFetcher,
    KalodataPageFetcher,
    OEmbedFetcher
)
from core.image_processor import generate_grids_from_videos
from core.vision_analyzer import analyze_creator_vision
from utils.performance import (
    PerformanceTimer,
    StatsCollector,
    setup_logger,
    log_info,
    log_success,
    log_failure
)


# ============================================================================
# 统一入口函数
# ============================================================================

def analyze_creator_vibe(
    tk_handle: str,
    vision_api_key: str,
    kalodata_api_url: Optional[str] = None,
    kalodata_api_key: Optional[str] = None,
    page_data: Optional[Dict] = None,
    vision_api_url: str = "https://api.anthropic.com/v1/messages",
    vision_model: str = "claude-3-5-sonnet-20241022",
    max_videos: int = 48,
    max_canvases: int = 4,
    enable_cache: bool = True,
    log_file: Optional[str] = "logs/creator_crm.log"
) -> CreatorVibeAnalysis:
    """
    达人风格分析 - 统一入口
    
    Args:
        tk_handle: TikTok 账号名（不带 @）
        vision_api_key: Vision API Key
        kalodata_api_url: Kalodata API 端点（可选）
        kalodata_api_key: Kalodata API Key（可选）
        page_data: Kalodata 页面数据（可选，包含 videos 列表）
        vision_api_url: Vision API 端点
        vision_model: Vision 模型名称
        max_videos: 最大视频数量
        max_canvases: 最大画板数量
        enable_cache: 是否启用 oEmbed 缓存
        log_file: 日志文件路径（None 则只输出到控制台）
        
    Returns:
        CreatorVibeAnalysis: 达人风格分析结果
        
    Raises:
        CreatorCRMError: 各种错误的基类
        
    Example:
        # 方式 1：使用 Kalodata API
        result = analyze_creator_vibe(
            tk_handle="pimrypie",
            vision_api_key="sk-xxx",
            kalodata_api_url="https://api.kalodata.com/...",
            kalodata_api_key="kalo-xxx"
        )
        
        # 方式 2：使用页面数据
        page_data = {
            "videos": [
                {"video_id": "123", "description": "...", "revenue": 1000},
                ...
            ]
        }
        result = analyze_creator_vibe(
            tk_handle="pimrypie",
            vision_api_key="sk-xxx",
            page_data=page_data
        )
    """
    # 初始化日志和性能监控
    logger = setup_logger(log_file=log_file)
    timer = PerformanceTimer(logger)
    stats = StatsCollector(logger)
    
    log_info("=" * 80)
    log_info(f"🎯 达人风格分析 - @{tk_handle}")
    log_info("=" * 80)
    
    try:
        # ========================================================================
        # 步骤 1: 获取视频数据
        # ========================================================================
        timer.start("数据获取")
        
        oembed_fetcher = OEmbedFetcher(cache_enabled=enable_cache)
        
        if kalodata_api_url and kalodata_api_key:
            # 使用 Kalodata API
            log_info("📡 数据源: Kalodata API")
            fetcher = KalodataAPIFetcher(
                api_url=kalodata_api_url,
                api_key=kalodata_api_key,
                oembed_fetcher=oembed_fetcher
            )
            videos = fetcher.fetch_videos(tk_handle, max_videos)
            
        elif page_data:
            # 使用页面数据
            log_info("📡 数据源: Kalodata 页面数据")
            fetcher = KalodataPageFetcher(oembed_fetcher=oembed_fetcher)
            videos = fetcher.fetch_videos(tk_handle, max_videos, page_data)
            
        else:
            raise ValueError(
                "必须提供 kalodata_api_url + kalodata_api_key 或 page_data"
            )
        
        timer.stop("数据获取")
        
        stats.record("视频总数", len(videos))
        stats.record("有效视频数", len([v for v in videos if v.cover_url]))
        log_success(f"获取 {len(videos)} 个视频（含封面）")
        
        # ========================================================================
        # 步骤 2: 生成画板
        # ========================================================================
        timer.start("画板生成")
        
        canvases_base64 = generate_grids_from_videos(
            videos,
            max_canvases=max_canvases
        )
        
        timer.stop("画板生成")
        
        stats.record("画板数量", len(canvases_base64))
        log_success(f"生成 {len(canvases_base64)} 张画板")
        
        # ========================================================================
        # 步骤 3: Vision 分析
        # ========================================================================
        timer.start("Vision 分析")
        
        result = analyze_creator_vision(
            tk_handle=tk_handle,
            videos=videos,
            canvases_base64=canvases_base64,
            vision_api_key=vision_api_key,
            vision_api_url=vision_api_url,
            vision_model=vision_model
        )
        
        timer.stop("Vision 分析")
        
        log_success("Vision 分析完成")
        log_info(f"   女装风格: {result.ai_apparel_style.value}")
        log_info(f"   配饰风格: {result.ai_accessory_style.value}")
        log_info(f"   品类偏好: {result.preferred_category.value}")
        
        # ========================================================================
        # 输出汇总
        # ========================================================================
        log_info("")
        timer.log_summary()
        log_info("")
        stats.log_summary()
        log_info("=" * 80)
        
        return result
        
    except KalodataAPIError as e:
        log_failure(f"Kalodata API 错误: {str(e)}")
        raise
    except VisionAPIError as e:
        log_failure(f"Vision API 错误: {str(e)}")
        raise
    except InsufficientDataError as e:
        log_failure(f"数据不足: {str(e)}")
        raise
    except ImageProcessingError as e:
        log_failure(f"图片处理错误: {str(e)}")
        raise
    except DataExtractionError as e:
        log_failure(f"数据提取错误: {str(e)}")
        raise
    except Exception as e:
        log_failure(f"未知错误: {str(e)}")
        raise CreatorCRMError(f"分析失败: {str(e)}")


# ============================================================================
# 便捷函数（向后兼容）
# ============================================================================

def analyze_creator_from_api(
    tk_handle: str,
    kalodata_api_url: str,
    kalodata_api_key: str,
    vision_api_key: str,
    **kwargs
) -> CreatorVibeAnalysis:
    """
    便捷函数：使用 Kalodata API 分析达人
    
    Args:
        tk_handle: TikTok 账号名
        kalodata_api_url: Kalodata API 端点
        kalodata_api_key: Kalodata API Key
        vision_api_key: Vision API Key
        **kwargs: 其他参数（传递给 analyze_creator_vibe）
        
    Returns:
        CreatorVibeAnalysis: 分析结果
    """
    return analyze_creator_vibe(
        tk_handle=tk_handle,
        vision_api_key=vision_api_key,
        kalodata_api_url=kalodata_api_url,
        kalodata_api_key=kalodata_api_key,
        **kwargs
    )


def analyze_creator_from_page(
    tk_handle: str,
    page_data: Dict,
    vision_api_key: str,
    **kwargs
) -> CreatorVibeAnalysis:
    """
    便捷函数：使用页面数据分析达人
    
    Args:
        tk_handle: TikTok 账号名
        page_data: Kalodata 页面数据
        vision_api_key: Vision API Key
        **kwargs: 其他参数（传递给 analyze_creator_vibe）
        
    Returns:
        CreatorVibeAnalysis: 分析结果
    """
    return analyze_creator_vibe(
        tk_handle=tk_handle,
        vision_api_key=vision_api_key,
        page_data=page_data,
        **kwargs
    )


# ============================================================================
# 主函数（用于测试）
# ============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="达人风格分析")
    parser.add_argument("tk_handle", help="TikTok 账号名（不带 @）")
    parser.add_argument("--api-url", help="Kalodata API 端点")
    parser.add_argument("--api-key", help="Kalodata API Key")
    parser.add_argument("--vision-key", help="Vision API Key", required=True)
    
    args = parser.parse_args()
    
    # 从环境变量读取 API Key（如果未提供）
    kalodata_api_url = args.api_url or os.getenv("KALODATA_API_URL")
    kalodata_api_key = args.api_key or os.getenv("KALODATA_API_KEY")
    vision_api_key = args.vision_key or os.getenv("ANTHROPIC_API_KEY")
    
    if not kalodata_api_url or not kalodata_api_key:
        print("错误：必须提供 Kalodata API URL 和 Key")
        print("可以通过命令行参数或环境变量提供：")
        print("  --api-url / KALODATA_API_URL")
        print("  --api-key / KALODATA_API_KEY")
        exit(1)
    
    # 执行分析
    result = analyze_creator_from_api(
        tk_handle=args.tk_handle,
        kalodata_api_url=kalodata_api_url,
        kalodata_api_key=kalodata_api_key,
        vision_api_key=vision_api_key
    )
    
    print("\n" + "=" * 80)
    print("📊 分析结果")
    print("=" * 80)
    print(f"TikTok 账号: @{result.tk_handle}")
    print(f"女装风格: {result.ai_apparel_style.value}")
    print(f"配饰风格: {result.ai_accessory_style.value}")
    print(f"品类偏好: {result.preferred_category.value}")
    print(f"\n分析理由:")
    print(result.analysis_reason)
    print("=" * 80)
