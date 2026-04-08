#!/usr/bin/env python3
"""
飞书多维表格数据读取器

负责从飞书多维表格读取达人信息，包括 Kalodata 链接
"""

import sys
import os
import json
import requests
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

# 添加父目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@dataclass
class CreatorRecord:
    """达人记录"""
    record_id: str
    tk_handle: str
    tk_url: str
    kalodata_url: Optional[str] = None
    video_screenshots: Optional[List[Dict[str, Any]]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'record_id': self.record_id,
            'tk_handle': self.tk_handle,
            'tk_url': self.tk_url,
            'kalodata_url': self.kalodata_url,
            'video_screenshots': self.video_screenshots
        }


class FeishuBitableReader:
    """飞书多维表格读取器"""
    
    def __init__(self, app_token: str = None, table_id: str = None):
        """
        初始化读取器
        
        Args:
            app_token: 飞书多维表格 APP Token
            table_id: 表格 ID
        """
        self.app_token = app_token or self._load_from_config('app_token')
        self.table_id = table_id or self._load_from_config('table_id')
        self.access_token: Optional[str] = None
        self.token_expires_at: float = 0
    
    def _load_from_config(self, key: str) -> Optional[str]:
        """从配置文件加载"""
        try:
            config_file = Path.home() / ".openclaw/openclaw.json"
            if config_file.exists():
                with open(config_file, 'r') as f:
                    config = json.load(f)
                    return config.get('feishu', {}).get(key)
        except Exception:
            pass
        
        return None
    
    def _get_access_token(self) -> str:
        """获取飞书 access_token"""
        import time
        
        # 检查 token 是否过期
        if self.access_token and time.time() < self.token_expires_at:
            return self.access_token
        
        # 获取新 token
        config_file = Path.home() / ".openclaw/openclaw.json"
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        app_id = config['channels']['feishu']['appId']
        app_secret = config['channels']['feishu']['appSecret']
        
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        response = requests.post(url, json={'app_id': app_id, 'app_secret': app_secret})
        result = response.json()
        
        if result.get('code') != 0:
            raise Exception(f"获取 access_token 失败: {result.get('msg')}")
        
        self.access_token = result['tenant_access_token']
        self.token_expires_at = time.time() + result.get('expire', 7200) - 300
        
        return self.access_token
    
    def read_records(
        self,
        filter_formula: str = None,
        page_size: int = 100
    ) -> List[CreatorRecord]:
        """
        读取表格记录
        
        Args:
            filter_formula: 过滤公式（可选）
            page_size: 每页记录数
        
        Returns:
            List[CreatorRecord]: 达人记录列表
        """
        access_token = self._get_access_token()
        
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records"
        
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        params = {
            'page_size': page_size
        }
        
        if filter_formula:
            params['filter'] = filter_formula
        
        records = []
        has_more = True
        page_token = None
        
        while has_more:
            if page_token:
                params['page_token'] = page_token
            
            response = requests.get(url, headers=headers, params=params)
            result = response.json()
            
            if result.get('code') != 0:
                raise Exception(f"读取记录失败: {result.get('msg')}")
            
            data = result.get('data', {})
            items = data.get('items', [])
            
            for item in items:
                try:
                    record = self._parse_record(item)
                    if record:
                        records.append(record)
                except Exception as e:
                    print(f"⚠️ 解析记录失败: {e}")
                    continue
            
            has_more = data.get('has_more', False)
            page_token = data.get('page_token')
        
        return records
    
    def _parse_record(self, item: Dict[str, Any]) -> Optional[CreatorRecord]:
        """
        解析记录
        
        Args:
            item: 飞书记录数据
        
        Returns:
            Optional[CreatorRecord]: 达人记录
        """
        record_id = item.get('record_id')
        fields = item.get('fields', {})
        
        # 提取字段（支持多种字段名格式）
        # 实际飞书表格字段名：达人handle（达人账号）
        tk_handle = (
            fields.get('达人handle（达人账号）') or
            fields.get('tk_handle') or
            fields.get('TikTok账号') or
            fields.get('账号') or
            fields.get('达人账号')
        )
        
        # 达人名称（当没有 handle 时使用）
        creator_name = fields.get('达人名称', '')
        
        # TikTok URL 字段
        tk_url = (
            fields.get('tk_url') or
            fields.get('TikTok链接') or
            fields.get('主页链接') or
            fields.get('TikTok主页')
        )
        
        # Kalodata URL 字段（可能是对象格式 {"link": "...", "text": "..."}）
        kalodata_raw = (
            fields.get('Kalodata_URL') or
            fields.get('kalodata_url') or
            fields.get('Kalodata链接') or
            fields.get('kalodata链接')
        )
        
        # 处理 Kalodata URL 的不同格式
        if isinstance(kalodata_raw, dict):
            kalodata_url = kalodata_raw.get('link') or kalodata_raw.get('text') or kalodata_raw.get('url')
        elif isinstance(kalodata_raw, str):
            kalodata_url = kalodata_raw
        else:
            kalodata_url = None
        
        # 获取状态字段
        status = (
            fields.get('视频宫图是否已生成') or
            fields.get('状态') or
            fields.get('处理状态')
        )
        
        # 获取视频截图（宫格图）字段
        video_screenshots = fields.get('视频截图')
        
        # 如果没有 handle，使用达人名称作为标识符
        # 注意：某些记录只有达人名称，没有 TikTok handle
        if not tk_handle:
            if creator_name:
                # 使用达人名称作为 handle（用于标识，不用于构建 URL）
                tk_handle = creator_name
            elif kalodata_url:
                # 从 Kalodata URL 中提取 creator ID 作为标识符
                import re
                from urllib.parse import urlparse, parse_qs
                try:
                    parsed = urlparse(kalodata_url)
                    params = parse_qs(parsed.query)
                    creator_id = params.get('id', ['unknown'])[0]
                    tk_handle = f"creator_{creator_id}"
                except Exception:
                    tk_handle = f"record_{record_id}"
            else:
                return None  # 无法识别的记录，跳过
        
        # 如果没有 tk_url，尝试构建（仅当 handle 是真实的 TikTok handle 时）
        if not tk_url and tk_handle and tk_handle.startswith('@'):
            tk_url = f"https://www.tiktok.com/{tk_handle}"
        elif not tk_url and tk_handle and not tk_handle.startswith('creator_') and not tk_handle.startswith('record_'):
            # 检查是否是简单的英文 handle（可能是真实的 TikTok handle）
            import re
            if re.match(r'^[a-zA-Z0-9._]+$', tk_handle):
                tk_url = f"https://www.tiktok.com/@{tk_handle}"
        
        record = CreatorRecord(
            record_id=record_id,
            tk_handle=tk_handle,
            tk_url=tk_url or '',
            kalodata_url=kalodata_url,
            video_screenshots=video_screenshots
        )
        
        # 附加状态信息（用于过滤）
        record._status = status
        
        return record
    
    def read_record_by_id(self, record_id: str) -> Optional[CreatorRecord]:
        """
        读取单条记录
        
        Args:
            record_id: 记录 ID
        
        Returns:
            Optional[CreatorRecord]: 达人记录
        """
        access_token = self._get_access_token()
        
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records/{record_id}"
        
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        response = requests.get(url, headers=headers)
        result = response.json()
        
        if result.get('code') != 0:
            raise Exception(f"读取记录失败: {result.get('msg')}")
        
        item = result.get('data', {}).get('record', {})
        return self._parse_record(item)
    
    def read_creators_with_kalodata(self) -> List[CreatorRecord]:
        """
        读取所有有 Kalodata 链接的达人
        
        Returns:
            List[CreatorRecord]: 达人记录列表
        """
        # 读取所有记录
        all_records = self.read_records()
        
        # 过滤出有 Kalodata 链接的记录
        records_with_kalodata = [
            record for record in all_records
            if record.kalodata_url
        ]
        
        print(f"✅ 找到 {len(records_with_kalodata)} 个有 Kalodata 链接的达人")
        
        return records_with_kalodata


def test_reader():
    """测试读取器"""
    print("=" * 80)
    print("🧪 飞书多维表格读取器测试")
    print("=" * 80)
    
    # 使用默认配置
    reader = FeishuBitableReader(
        app_token=os.environ.get("FEISHU_APP_TOKEN", "ES8dbWo9FaXmaVs6jA7cgMURnQe"),
        table_id="tblk1IHpVAvv2nWc"
    )
    
    try:
        # 读取所有记录
        print("\n📖 读取所有记录...")
        records = reader.read_records(page_size=10)
        
        print(f"✅ 读取到 {len(records)} 条记录")
        
        if records:
            print("\n前3条记录示例：")
            for i, record in enumerate(records[:3], 1):
                print(f"\n记录 {i}:")
                print(f"  Record ID: {record.record_id}")
                print(f"  TikTok账号: @{record.tk_handle}")
                print(f"  TikTok链接: {record.tk_url}")
                print(f"  Kalodata链接: {record.kalodata_url or '(无)'}")
        
        # 读取有 Kalodata 链接的记录
        print("\n" + "=" * 80)
        print("📖 读取有 Kalodata 链接的达人...")
        kalodata_records = reader.read_creators_with_kalodata()
        
        if kalodata_records:
            print(f"\n找到 {len(kalodata_records)} 个有 Kalodata 链接的达人")
            print("\n示例：")
            for i, record in enumerate(kalodata_records[:3], 1):
                print(f"\n{i}. @{record.tk_handle}")
                print(f"   Kalodata: {record.kalodata_url}")
        
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_reader()
