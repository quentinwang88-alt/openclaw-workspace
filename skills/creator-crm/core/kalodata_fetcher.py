#!/usr/bin/env python3
"""
Kalodata 数据抓取模块

负责从 Kalodata 页面获取达人视频数据，包括：
- 视频封面 URL
- 观看次数
- 成交金额（GMV）
"""

import sys
import os
import json
import time
import random
import requests
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta

# 添加父目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@dataclass
class KalodataVideo:
    """Kalodata 视频数据"""
    video_id: str
    cover_url: str
    views: int  # 观看次数
    gmv: float  # 成交金额
    description: str = ""
    publish_time: Optional[str] = None
    
    @staticmethod
    def build_cover_url(video_id: str) -> str:
        """
        构建封面 URL
        
        Kalodata 将视频封面存储在 CSS 背景图中，格式为：
        https://img.kalowave.cn/tiktok.video/{VIDEO_ID}/cover.png
        
        Args:
            video_id: 视频 ID
        
        Returns:
            str: 封面 URL
        """
        return f"https://img.kalowave.cn/tiktok.video/{video_id}/cover.png"


class KalodataFetcher:
    """Kalodata 数据抓取器"""
    
    def __init__(self, cookie: str = None, timeout: int = 30):
        """
        初始化抓取器
        
        Args:
            cookie: Kalodata Cookie（用于认证）
            timeout: 请求超时时间（秒）
        """
        self.cookie = cookie or self._load_cookie_from_config()
        self.timeout = timeout
        self.session = requests.Session()
        
        # 设置请求头（根据实际的 API 请求）
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Content-Type': 'application/json',
            'Origin': 'https://www.kalodata.com',
            'Referer': 'https://www.kalodata.com/creator/detail',
            'country': 'TH',
            'currency': 'CNY',
            'language': 'zh-CN',
            'sec-ch-ua': '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin'
        })
        
        if self.cookie:
            self.session.headers['Cookie'] = self.cookie
    
    def check_cookie_valid(self) -> bool:
        """
        检查 Cookie 是否有效
        
        Returns:
            bool: True=有效，False=已过期或无效
        """
        if not self.cookie:
            print("⚠️ 未配置 Cookie")
            return False
        
        # 用一个简单的 API 请求测试 Cookie
        test_url = "https://www.kalodata.com/creator/detail/video/queryList"
        test_payload = {
            "id": "6820204073527886850",
            "startDate": "2026-02-01",
            "endDate": "2026-03-01",
            "cateIds": [],
            "pageNo": 1,
            "pageSize": 1,
            "sellerId": "",
            "sort": [{"field": "revenue", "type": "DESC"}],
            "authority": True,
            "videoType": "",
            "video.filter.ad.daily_cost": "",
            "video.filter.ad.daily_roas": "",
            "video.filter.ad.revenue_ratio": "",
            "video.filter.ad.view_ratio": ""
        }
        
        try:
            response = self.session.post(test_url, json=test_payload, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get('success'):
                    # 有数据说明 Cookie 有效
                    if data.get('data'):
                        return True
                    # data 为空可能是该达人没有数据，但 Cookie 仍然有效
                    # 通过检查是否有 success=true 来判断
                    return True
                elif response.status_code == 401 or 'login' in str(data).lower():
                    print("⚠️ Cookie 已过期，请重新登录 Kalodata 并更新 Cookie")
                    return False
            elif response.status_code == 401:
                print("⚠️ Cookie 已过期（401 Unauthorized）")
                return False
        except Exception as e:
            print(f"⚠️ Cookie 检查失败: {e}")
        
        return True  # 默认认为有效，避免误判
    
    @staticmethod
    def update_cookie_in_config(new_cookie: str):
        """
        更新配置文件中的 Cookie
        
        Args:
            new_cookie: 新的 Cookie 字符串
        """
        config_file = Path(__file__).parent.parent / "config" / "api_config.json"
        
        if config_file.exists():
            try:
                with open(config_file, 'r') as f:
                    config = json.load(f)
                
                config.setdefault('kalodata', {})['cookie'] = new_cookie
                
                with open(config_file, 'w') as f:
                    json.dump(config, f, indent=2, ensure_ascii=False)
                
                print(f"✅ Cookie 已更新到配置文件: {config_file}")
            except Exception as e:
                print(f"❌ 更新 Cookie 失败: {e}")
        else:
            print(f"⚠️ 配置文件不存在: {config_file}")
    
    def _load_cookie_from_config(self) -> Optional[str]:
        """从配置文件加载 Cookie"""
        config_file = Path(__file__).parent.parent / "config" / "api_config.json"
        
        if config_file.exists():
            try:
                with open(config_file, 'r') as f:
                    config = json.load(f)
                    return config.get('kalodata', {}).get('cookie')
            except Exception as e:
                print(f"⚠️ 加载配置文件失败: {e}")
        
        return None
    
    def fetch_videos_from_url(
        self,
        kalodata_url: str,
        max_videos: int = 48,
        date_range_days: int = 180
    ) -> List[KalodataVideo]:
        """
        从 Kalodata URL 获取视频数据
        
        Args:
            kalodata_url: Kalodata 达人详情页 URL
            max_videos: 最大视频数量
            date_range_days: 日期范围（天数），默认 180 天以获取足够视频
        
        Returns:
            List[KalodataVideo]: 视频列表
        """
        print(f"📡 从 Kalodata 获取视频数据...")
        print(f"   URL: {kalodata_url}")
        print(f"   目标数量: {max_videos}")
        print(f"   日期范围: {date_range_days} 天")
        
        # 解析 URL 获取达人 ID 和日期范围
        creator_id = self._extract_creator_id(kalodata_url)
        if not creator_id:
            raise ValueError("无法从 URL 中提取达人 ID")
        
        # 尝试从 URL 中提取日期范围
        start_date_str, end_date_str = self._extract_date_range(kalodata_url)
        
        if start_date_str and end_date_str:
            print(f"   使用 URL 中的日期范围: {start_date_str} ~ {end_date_str}")
            videos = self._fetch_videos_by_date_range(
                creator_id, date_range_days,
                start_date_str=start_date_str, end_date_str=end_date_str
            )
        else:
            # 使用默认日期范围
            videos = self._fetch_videos_by_date_range(creator_id, date_range_days)
        
        # 如果不足 12 个，尝试更大范围
        if len(videos) < 12 and date_range_days < 365:
            print(f"⚠️ 数据不足（{len(videos)}/12），尝试365天...")
            videos = self._fetch_videos_by_date_range(creator_id, 365)
        
        # 限制数量
        videos = videos[:max_videos]
        
        print(f"✅ 成功获取 {len(videos)} 个视频")
        return videos
    
    def _extract_date_range(self, url: str):
        """从 URL 中提取日期范围"""
        import re
        import json as json_module
        from urllib.parse import urlparse, parse_qs, unquote
        
        try:
            parsed = urlparse(url)
            params = parse_qs(parsed.query)
            
            if 'dateRange' in params:
                date_range_raw = unquote(params['dateRange'][0])
                date_range = json_module.loads(date_range_raw)
                if len(date_range) == 2:
                    return date_range[0], date_range[1]
        except Exception:
            pass
        
        return None, None
    
    def _extract_creator_id(self, url: str) -> Optional[str]:
        """从 URL 中提取达人 ID"""
        import re
        from urllib.parse import urlparse, parse_qs
        
        # 方法1: 从查询参数中提取
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        
        if 'id' in params:
            return params['id'][0]
        
        # 方法2: 从路径中提取
        match = re.search(r'/creator/detail/(\d+)', url)
        if match:
            return match.group(1)
        
        # 方法3: 从 URL 中查找数字 ID
        match = re.search(r'id=(\d+)', url)
        if match:
            return match.group(1)
        
        return None
    
    def _fetch_videos_by_date_range(
        self,
        creator_id: str,
        days: int,
        start_date_str: str = None,
        end_date_str: str = None
    ) -> List[KalodataVideo]:
        """
        根据日期范围获取视频
        
        Args:
            creator_id: 达人 ID
            days: 日期范围（天数），当 start_date_str/end_date_str 未指定时使用
            start_date_str: 开始日期字符串（可选，格式 YYYY-MM-DD）
            end_date_str: 结束日期字符串（可选，格式 YYYY-MM-DD）
        
        Returns:
            List[KalodataVideo]: 视频列表
        """
        # 计算日期范围
        if start_date_str and end_date_str:
            start_date = start_date_str
            end_date = end_date_str
        else:
            end_dt = datetime.now()
            start_dt = end_dt - timedelta(days=days)
            start_date = start_dt.strftime("%Y-%m-%d")
            end_date = end_dt.strftime("%Y-%m-%d")
        
        # Kalodata API 地址
        api_url = "https://www.kalodata.com/creator/detail/video/queryList"
        
        # 构建请求体 - 完全按照浏览器实际请求格式
        # 注意：cateIds 使用空数组，否则会过滤掉大量视频
        payload = {
            "id": creator_id,
            "startDate": start_date,
            "endDate": end_date,
            "cateIds": [],
            "pageNo": 1,
            "pageSize": 50,
            "sellerId": "",
            "sort": [{"field": "revenue", "type": "DESC"}],
            "authority": True,
            "videoType": "",
            "video.filter.ad.daily_cost": "",
            "video.filter.ad.daily_roas": "",
            "video.filter.ad.revenue_ratio": "",
            "video.filter.ad.view_ratio": ""
        }
        
        all_videos = []
        max_retries = 3  # 最大重试次数
        retry_count = 0
        
        try:
            # 可能需要多页获取
            while len(all_videos) < 48 and payload["pageNo"] <= 5:  # 最多5页
                print(f"   正在获取第 {payload['pageNo']} 页...")
                
                try:
                    response = self.session.post(api_url, json=payload, timeout=self.timeout)
                    
                    # 处理 429 限流错误
                    if response.status_code == 429:
                        retry_count += 1
                        if retry_count > max_retries:
                            print(f"⚠️ 达到最大重试次数 ({max_retries})，跳过此达人")
                            break
                        
                        wait_time = 60 + random.uniform(10, 30)  # 60-90秒随机等待
                        print(f"⚠️ 触发限流 (429)，等待 {wait_time:.1f} 秒后重试...")
                        time.sleep(wait_time)
                        continue
                    
                    # 处理 403 禁止访问错误
                    if response.status_code == 403:
                        print(f"⚠️ 访问被拒绝 (403)，可能触发风控")
                        print(f"   建议：暂停处理，检查 Cookie 是否有效")
                        # 等待更长时间
                        wait_time = 120 + random.uniform(30, 60)
                        print(f"   等待 {wait_time:.1f} 秒后继续...")
                        time.sleep(wait_time)
                        raise Exception("Kalodata 风控触发 (403)，请稍后重试")
                    
                    # 处理其他错误
                    if response.status_code != 200:
                        print(f"⚠️ API 返回错误: HTTP {response.status_code}")
                        response.raise_for_status()
                    
                    # 重置重试计数
                    retry_count = 0
                    
                    data = response.json()
                    
                    # 解析响应数据
                    videos = self._parse_video_data(data)
                    
                    if not videos:
                        break  # 没有更多数据
                    
                    all_videos.extend(videos)
                    
                    # 如果已经足够，停止
                    if len(all_videos) >= 48:
                        break
                    
                    # 下一页
                    payload["pageNo"] += 1
                    
                    # 随机延迟 1-3 秒（避免风控）
                    delay = random.uniform(1.0, 3.0)
                    time.sleep(delay)
                    
                except requests.exceptions.RequestException as e:
                    retry_count += 1
                    if retry_count > max_retries:
                        print(f"⚠️ 达到最大重试次数，跳过: {e}")
                        break
                    
                    wait_time = 30 + random.uniform(5, 15)
                    print(f"⚠️ 请求失败: {e}，等待 {wait_time:.1f} 秒后重试...")
                    time.sleep(wait_time)
                    continue
            
            return all_videos
            
        except Exception as e:
            print(f"❌ API 请求失败: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def _build_api_url(self, creator_id: str, date_range: str) -> str:
        """
        构建 API URL
        
        TODO: 需要根据实际的 API 接口调整
        """
        # 占位符 URL，等待用户提供实际的 API 地址
        base_url = "https://www.kalodata.com/api/creator/videos"
        
        params = {
            'id': creator_id,
            'dateRange': date_range,
            'language': 'zh-CN',
            'currency': 'CNY',
            'region': 'TH'
        }
        
        # 构建查询字符串
        from urllib.parse import urlencode
        query_string = urlencode(params)
        
        return f"{base_url}?{query_string}"
    
    def _parse_video_data(self, data: Dict[str, Any]) -> List[KalodataVideo]:
        """
        解析 API 响应数据
        
        实际响应格式（已验证）：
        {
          "success": true,
          "data": [
            {
              "id": "7573568674951384340",   ← 注意是 id 不是 video_id
              "description": "...",
              "views": "6.09万",             ← 注意是字符串格式
              "revenue": "¥3.99万",
              "sale": "42",
              "create_time": "2025/11/17 12:54:39",
              "gpm": 654.465,
              "ad": 0,
              ...
            }
          ]
        }
        """
        videos = []
        
        # 检查响应是否成功
        if not data.get('success'):
            print(f"⚠️ API 返回失败: {data}")
            return videos
        
        video_list = data.get('data', [])
        
        for item in video_list:
            try:
                # 提取视频 ID（实际字段名是 'id' 而不是 'video_id'）
                video_id = str(item.get('id', '') or item.get('video_id', ''))
                
                if not video_id:
                    continue
                
                # 使用 video_id 构建封面 URL
                cover_url = KalodataVideo.build_cover_url(video_id)
                
                # 提取观看次数（views 是字符串格式如 "6.09万"，需要解析）
                views_raw = item.get('views', item.get('view', 0))
                views = self._parse_views(views_raw)
                
                # 解析成交金额（revenue 格式：¥2.68万）
                revenue_str = item.get('revenue', '¥0')
                gmv = self._parse_revenue(revenue_str)
                
                # 提取描述
                description = item.get('description', '')
                
                # 提取发布时间
                publish_time = item.get('create_time', '')
                
                video = KalodataVideo(
                    video_id=video_id,
                    cover_url=cover_url,
                    views=views,
                    gmv=gmv,
                    description=description,
                    publish_time=publish_time
                )
                
                videos.append(video)
                    
            except Exception as e:
                print(f"⚠️ 解析视频数据失败: {e}, item={item}")
                import traceback
                traceback.print_exc()
                continue
        
        return videos
    
    def _parse_revenue(self, revenue_str: str) -> float:
        """
        解析成交金额字符串
        
        格式示例：
        - ¥2.68万 → 26800
        - ¥1.5千 → 1500
        - ¥100 → 100
        
        Args:
            revenue_str: 成交金额字符串
        
        Returns:
            float: 成交金额（数值）
        """
        import re
        
        if not revenue_str:
            return 0.0
        
        # 移除货币符号
        revenue_str = str(revenue_str).replace('¥', '').replace('￥', '').strip()
        
        # 解析数字和单位
        match = re.match(r'([\d.]+)(万|千|百)?', revenue_str)
        
        if not match:
            return 0.0
        
        number = float(match.group(1))
        unit = match.group(2)
        
        # 转换单位
        if unit == '万':
            return number * 10000
        elif unit == '千':
            return number * 1000
        elif unit == '百':
            return number * 100
        else:
            return number
    
    def _parse_views(self, views_raw) -> int:
        """
        解析观看次数（支持字符串格式如 "6.09万"）
        
        格式示例：
        - "6.09万" → 60900
        - "1.5千" → 1500
        - 123456 → 123456
        - "123456" → 123456
        
        Args:
            views_raw: 观看次数（字符串或整数）
        
        Returns:
            int: 观看次数
        """
        import re
        
        if views_raw is None:
            return 0
        
        if isinstance(views_raw, (int, float)):
            return int(views_raw)
        
        views_str = str(views_raw).strip()
        
        # 解析数字和单位
        match = re.match(r'([\d.]+)(万|千|百)?', views_str)
        
        if not match:
            return 0
        
        number = float(match.group(1))
        unit = match.group(2)
        
        if unit == '万':
            return int(number * 10000)
        elif unit == '千':
            return int(number * 1000)
        elif unit == '百':
            return int(number * 100)
        else:
            return int(number)
    
    def fetch_videos_with_browser(
        self,
        kalodata_url: str,
        max_videos: int = 48
    ) -> List[KalodataVideo]:
        """
        使用浏览器工具获取视频数据（备用方案）
        
        这个方法需要在 OpenClaw 环境中使用 browser 工具
        
        Args:
            kalodata_url: Kalodata 达人详情页 URL
            max_videos: 最大视频数量
        
        Returns:
            List[KalodataVideo]: 视频列表
        """
        print("⚠️ 此方法需要在 OpenClaw 环境中使用 browser 工具")
        print("请参考 analyze_kalodata_auto.py 中的示例")
        
        raise NotImplementedError(
            "此方法需要在 OpenClaw 环境中使用 browser 工具。\n"
            "请使用 fetch_videos_from_url() 或参考 analyze_kalodata_auto.py"
        )


def test_fetcher():
    """测试抓取器"""
    print("=" * 80)
    print("🧪 Kalodata Fetcher 测试")
    print("=" * 80)
    
    # 示例 URL
    test_url = "https://www.kalodata.com/creator/detail?id=7207725887499846682&language=zh-CN&currency=CNY&region=TH"
    
    fetcher = KalodataFetcher()
    
    try:
        videos = fetcher.fetch_videos_from_url(test_url, max_videos=48)
        
        print(f"\n✅ 获取到 {len(videos)} 个视频")
        
        if videos:
            print("\n前3个视频示例：")
            for i, video in enumerate(videos[:3], 1):
                print(f"\n视频 {i}:")
                print(f"  ID: {video.video_id}")
                print(f"  封面: {video.cover_url[:50]}...")
                print(f"  观看: {video.views:,}")
                print(f"  GMV: ¥{video.gmv:,.2f}")
        
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        print("\n💡 提示:")
        print("1. 请确保已配置 Cookie")
        print("2. 请确保 API URL 和响应格式正确")
        print("3. 运行 API 信息提取脚本获取实际的 API 信息")


if __name__ == "__main__":
    test_fetcher()
