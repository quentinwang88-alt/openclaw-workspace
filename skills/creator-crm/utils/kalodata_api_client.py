"""
Kalodata API 客户端 - 直接调用 API 获取达人视频数据

基于浏览器分析的结果实现
"""

import requests
import json
from typing import Dict, List, Optional

class KalodataAPIClient:
    """Kalodata API 客户端"""
    
    def __init__(self, session_cookie: str, device_id: str):
        """
        初始化客户端
        
        Args:
            session_cookie: SESSION cookie 值
            device_id: 设备 ID
        """
        self.base_url = "https://www.kalodata.com"
        self.session = requests.Session()
        
        # 设置必要的 headers
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Content-Type': 'application/json',
            'Origin': 'https://www.kalodata.com',
            'Referer': 'https://www.kalodata.com/creator/detail',
        })
        
        # 设置 cookies
        self.session.cookies.set('SESSION', session_cookie, domain='.kalodata.com')
        self.session.cookies.set('deviceId', device_id, domain='.kalodata.com')
        self.session.cookies.set('appVersion', '2.0', domain='.kalodata.com')
        self.session.cookies.set('deviceType', 'pc', domain='.kalodata.com')
    
    def get_creator_videos(
        self,
        creator_id: str,
        date_range: List[str],
        region: str = "TH",
        page_no: int = 1,
        page_size: int = 50
    ) -> Optional[Dict]:
        """
        获取达人视频列表
        
        Args:
            creator_id: 达人 ID
            date_range: 日期范围 ["2026-02-05", "2026-03-06"]
            region: 区域代码 (TH, VN, etc.)
            page_no: 页码
            page_size: 每页数量
            
        Returns:
            视频数据字典，包含视频列表和封面 URL
        """
        url = f"{self.base_url}/creator/detail/video/list"
        
        payload = {
            "creatorId": creator_id,
            "dateRange": date_range,
            "region": region,
            "pageNo": page_no,
            "pageSize": page_size,
            "sorter": {
                "field": "revenue",
                "order": "descend"
            }
        }
        
        try:
            response = self.session.post(url, json=payload, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('code') == 200:
                return data.get('data')
            else:
                print(f"API 返回错误: {data.get('message')}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"请求失败: {e}")
            return None
    
    def get_video_count(
        self,
        creator_id: str,
        date_range: List[str],
        region: str = "TH"
    ) -> Optional[int]:
        """
        获取达人视频数量
        
        Args:
            creator_id: 达人 ID
            date_range: 日期范围
            region: 区域代码
            
        Returns:
            视频数量
        """
        url = f"{self.base_url}/creator/detail/video/count"
        
        payload = {
            "creatorId": creator_id,
            "dateRange": date_range,
            "region": region
        }
        
        try:
            response = self.session.post(url, json=payload, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('code') == 200:
                return data.get('data', {}).get('count', 0)
            else:
                print(f"API 返回错误: {data.get('message')}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"请求失败: {e}")
            return None


# 使用示例
if __name__ == "__main__":
    # 从浏览器获取的认证信息
    SESSION_COOKIE = "MDlkNzdkZTktMTFiMC00ZGIyLWExMjEtMmNlMjBmY2I3Mzc2"
    DEVICE_ID = "b6f5a36e39f4d82e7ae1c1a777ab6179"
    
    # 创建客户端
    client = KalodataAPIClient(SESSION_COOKIE, DEVICE_ID)
    
    # 测试获取视频数量
    creator_id = "7207725887499846682"
    date_range = ["2026-02-05", "2026-03-06"]
    
    print("=" * 80)
    print("测试 Kalodata API 客户端")
    print("=" * 80)
    
    # 1. 获取视频数量
    print(f"\n1. 获取视频数量...")
    count = client.get_video_count(creator_id, date_range, region="TH")
    if count is not None:
        print(f"   ✅ 视频数量: {count}")
    else:
        print(f"   ❌ 获取失败")
    
    # 2. 获取视频列表
    print(f"\n2. 获取视频列表...")
    videos = client.get_creator_videos(creator_id, date_range, region="TH", page_size=12)
    
    if videos:
        video_list = videos.get('list', [])
        print(f"   ✅ 获取到 {len(video_list)} 个视频")
        
        # 显示前 3 个视频的信息
        for i, video in enumerate(video_list[:3], 1):
            print(f"\n   视频 {i}:")
            print(f"     ID: {video.get('videoId')}")
            print(f"     标题: {video.get('title', '')[:50]}...")
            print(f"     封面: {video.get('coverUrl', 'N/A')[:80]}...")
            print(f"     GMV: {video.get('revenue', 0)}")
    else:
        print(f"   ❌ 获取失败")
    
    print("\n" + "=" * 80)
