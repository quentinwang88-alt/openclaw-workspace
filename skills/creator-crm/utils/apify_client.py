"""
Apify 客户端封装

提供 TikTok Profile Scraper 的调用接口
"""

from typing import Dict, List, Optional, Any
import os


class ApifyClient:
    """
    Apify API 客户端封装
    
    用于调用 TikTok Profile Scraper 抓取达人主页数据
    """
    
    def __init__(self, api_token: Optional[str] = None):
        """
        初始化 Apify 客户端
        
        Args:
            api_token: Apify API Token，如果不提供则从环境变量读取
        """
        self.api_token = api_token or os.getenv("APIFY_API_TOKEN")
        
        if not self.api_token:
            raise ValueError(
                "Apify API Token 未设置。请通过参数传入或设置环境变量 APIFY_API_TOKEN"
            )
    
    def scrape_tiktok_profile(
        self,
        tk_handle: str,
        max_videos: int = 6
    ) -> Dict[str, Any]:
        """
        抓取 TikTok 达人主页数据
        
        Args:
            tk_handle: TikTok 账号名（带或不带 @ 均可）
            max_videos: 最多抓取的视频数量
            
        Returns:
            包含达人信息和视频列表的字典:
            {
                "bio": str,              # 达人简介
                "followers_count": int,  # 粉丝数
                "video_list": [          # 视频列表
                    {
                        "cover_url": str,  # 封面图 URL
                        "video_id": str    # 视频 ID
                    }
                ]
            }
            
        Raises:
            RuntimeError: 当 Apify 调用失败时
        """
        # 移除 @ 符号（如果有）
        handle = tk_handle.lstrip('@')
        
        try:
            # 注意：这里需要实际的 Apify SDK
            # 由于当前环境可能没有安装，这里提供接口定义
            # 实际使用时需要：pip install apify-client
            
            from apify_client import ApifyClient as ApifySDK
            
            client = ApifySDK(self.api_token)
            
            # 调用 TikTok Profile Scraper
            # Actor ID: GdWCkxBtKWOsKjdch
            run_input = {
                "profiles": [f"@{handle}"],
                "resultsPerPage": max_videos
            }
            
            run = client.actor("GdWCkxBtKWOsKjdch").call(
                run_input=run_input
            )
            
            # 获取结果
            items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
            
            if not items:
                raise RuntimeError(f"未找到 TikTok 账号: @{handle}")
            
            # Apify 返回的是视频列表，每个视频包含 authorMeta
            # 从第一个视频中提取达人信息
            first_video = items[0]
            author_meta = first_video.get("authorMeta", {})
            
            # 提取视频封面 URL
            video_list = []
            for video in items[:max_videos]:
                video_meta = video.get("videoMeta", {})
                video_list.append({
                    "cover_url": video_meta.get("coverUrl", ""),
                    "video_id": video.get("id", "")
                })
            
            return {
                "bio": author_meta.get("signature", ""),
                "followers_count": author_meta.get("fans", 0),
                "video_list": video_list
            }
            
        except ImportError:
            # Apify SDK 未安装，返回模拟数据用于开发测试
            print("⚠️ Apify SDK 未安装，返回模拟数据")
            return self._get_mock_data(handle)
        
        except Exception as e:
            raise RuntimeError(f"Apify 调用失败: {str(e)}")
    
    def _get_mock_data(self, handle: str) -> Dict[str, Any]:
        """
        返回模拟数据（用于开发测试）
        
        Args:
            handle: TikTok 账号名
            
        Returns:
            模拟的达人数据
        """
        return {
            "bio": f"Fashion & Style 💕 | {handle} | Thailand 🇹🇭",
            "followers_count": 50000,
            "video_list": [
                {
                    "cover_url": "https://picsum.photos/200/300?random=1",
                    "video_id": "mock_video_1"
                },
                {
                    "cover_url": "https://picsum.photos/200/300?random=2",
                    "video_id": "mock_video_2"
                },
                {
                    "cover_url": "https://picsum.photos/200/300?random=3",
                    "video_id": "mock_video_3"
                }
            ]
        }


if __name__ == "__main__":
    # 测试 Apify 客户端
    print("🧪 测试 Apify 客户端")
    print()
    
    # 使用模拟数据测试
    try:
        client = ApifyClient(api_token="mock_token_for_testing")
        result = client.scrape_tiktok_profile("fashionista_th")
        
        print(f"✅ 成功获取达人数据")
        print(f"Bio: {result['bio']}")
        print(f"粉丝数: {result['followers_count']:,}")
        print(f"视频数量: {len(result['video_list'])}")
        print()
        print("视频封面 URL:")
        for i, video in enumerate(result['video_list'], 1):
            print(f"  {i}. {video['cover_url']}")
    
    except Exception as e:
        print(f"❌ 测试失败: {str(e)}")
