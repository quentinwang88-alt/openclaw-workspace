#!/usr/bin/env python3
"""
飞书开放平台 API 封装
支持多维表格（Bitable）操作
"""

import requests
import time
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta


class FeishuBitableAPI:
    """飞书多维表格 API 封装"""
    
    def __init__(self, app_id: str, app_secret: str):
        """
        初始化飞书 API 客户端
        
        Args:
            app_id: 飞书应用 ID
            app_secret: 飞书应用密钥
        """
        self.app_id = app_id
        self.app_secret = app_secret
        self.base_url = "https://open.feishu.cn/open-apis"
        self._access_token = None
        self._token_expire_time = None
    
    def get_access_token(self) -> str:
        """
        获取 tenant_access_token
        会自动缓存 token 并在过期前刷新
        
        Returns:
            access_token 字符串
        """
        # 如果 token 存在且未过期，直接返回
        if self._access_token and self._token_expire_time:
            if datetime.now() < self._token_expire_time:
                return self._access_token
        
        # 获取新 token
        url = f"{self.base_url}/auth/v3/tenant_access_token/internal"
        payload = {
            "app_id": self.app_id,
            "app_secret": self.app_secret
        }
        
        response = requests.post(url, json=payload, timeout=10)
        data = response.json()
        
        if data.get("code") == 0:
            self._access_token = data["tenant_access_token"]
            # token 有效期通常是 2 小时，提前 5 分钟刷新
            expire_seconds = data.get("expire", 7200) - 300
            self._token_expire_time = datetime.now() + timedelta(seconds=expire_seconds)
            return self._access_token
        else:
            raise Exception(f"获取 access_token 失败: {data}")
    
    def _make_request(self, method: str, url: str, **kwargs) -> Dict:
        """
        发起 HTTP 请求的通用方法
        
        Args:
            method: HTTP 方法 (GET, POST, DELETE 等)
            url: 请求 URL
            **kwargs: 传递给 requests 的其他参数
        
        Returns:
            响应的 JSON 数据
        """
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {self.get_access_token()}"
        
        if method.upper() in ["POST", "PUT", "PATCH"]:
            headers["Content-Type"] = "application/json"
        
        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            timeout=30,
            **kwargs
        )
        
        return response.json()
    
    def create_record(self, app_token: str, table_id: str, fields: Dict[str, Any]) -> Dict:
        """
        创建多维表格记录
        
        Args:
            app_token: 多维表格的 app_token
            table_id: 数据表 ID
            fields: 字段数据，格式为 {"字段名": 值}
        
        Returns:
            创建结果，包含 record_id 等信息
        """
        url = f"{self.base_url}/bitable/v1/apps/{app_token}/tables/{table_id}/records"
        payload = {"fields": fields}
        
        result = self._make_request("POST", url, json=payload)
        
        if result.get("code") != 0:
            raise Exception(f"创建记录失败: {result}")
        
        return result.get("data", {})
    
    def batch_create_records(self, app_token: str, table_id: str, 
                            records: List[Dict[str, Any]]) -> Dict:
        """
        批量创建多维表格记录
        
        Args:
            app_token: 多维表格的 app_token
            table_id: 数据表 ID
            records: 记录列表，每个记录格式为 {"fields": {"字段名": 值}}
        
        Returns:
            创建结果
        """
        url = f"{self.base_url}/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"
        payload = {"records": records}
        
        result = self._make_request("POST", url, json=payload)
        
        if result.get("code") != 0:
            raise Exception(f"批量创建记录失败: {result}")
        
        return result.get("data", {})
    
    def list_records(self, app_token: str, table_id: str, 
                     page_size: int = 100, page_token: Optional[str] = None,
                     filter_formula: Optional[str] = None) -> Dict:
        """
        列出多维表格记录
        
        Args:
            app_token: 多维表格的 app_token
            table_id: 数据表 ID
            page_size: 每页记录数，最大 500
            page_token: 分页标记
            filter_formula: 筛选公式
        
        Returns:
            记录列表和分页信息
        """
        url = f"{self.base_url}/bitable/v1/apps/{app_token}/tables/{table_id}/records"
        params = {"page_size": min(page_size, 500)}
        
        if page_token:
            params["page_token"] = page_token
        if filter_formula:
            params["filter"] = filter_formula
        
        result = self._make_request("GET", url, params=params)
        
        if result.get("code") != 0:
            raise Exception(f"列出记录失败: {result}")
        
        return result.get("data", {})
    
    def list_all_records(self, app_token: str, table_id: str,
                        filter_formula: Optional[str] = None) -> List[Dict]:
        """
        列出所有记录（自动处理分页）
        
        Args:
            app_token: 多维表格的 app_token
            table_id: 数据表 ID
            filter_formula: 筛选公式
        
        Returns:
            所有记录的列表
        """
        all_records = []
        page_token = None
        
        while True:
            data = self.list_records(
                app_token=app_token,
                table_id=table_id,
                page_size=500,
                page_token=page_token,
                filter_formula=filter_formula
            )
            
            items = data.get("items", [])
            all_records.extend(items)
            
            # 检查是否还有下一页
            has_more = data.get("has_more", False)
            if not has_more:
                break
            
            page_token = data.get("page_token")
            if not page_token:
                break
            
            # 避免请求过快
            time.sleep(0.1)
        
        return all_records
    
    def get_record(self, app_token: str, table_id: str, record_id: str) -> Dict:
        """
        获取单条记录
        
        Args:
            app_token: 多维表格的 app_token
            table_id: 数据表 ID
            record_id: 记录 ID
        
        Returns:
            记录数据
        """
        url = f"{self.base_url}/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"
        
        result = self._make_request("GET", url)
        
        if result.get("code") != 0:
            raise Exception(f"获取记录失败: {result}")
        
        return result.get("data", {}).get("record", {})
    
    def update_record(self, app_token: str, table_id: str, record_id: str,
                     fields: Dict[str, Any]) -> Dict:
        """
        更新记录
        
        Args:
            app_token: 多维表格的 app_token
            table_id: 数据表 ID
            record_id: 记录 ID
            fields: 要更新的字段数据
        
        Returns:
            更新结果
        """
        url = f"{self.base_url}/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"
        payload = {"fields": fields}
        
        result = self._make_request("PUT", url, json=payload)
        
        if result.get("code") != 0:
            raise Exception(f"更新记录失败: {result}")
        
        return result.get("data", {})
    
    def delete_record(self, app_token: str, table_id: str, record_id: str) -> Dict:
        """
        删除记录
        
        Args:
            app_token: 多维表格的 app_token
            table_id: 数据表 ID
            record_id: 记录 ID
        
        Returns:
            删除结果
        """
        url = f"{self.base_url}/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"
        
        result = self._make_request("DELETE", url)
        
        if result.get("code") != 0:
            raise Exception(f"删除记录失败: {result}")
        
        return result.get("data", {})
    
    def batch_delete_records(self, app_token: str, table_id: str,
                           record_ids: List[str]) -> Dict:
        """
        批量删除记录
        
        Args:
            app_token: 多维表格的 app_token
            table_id: 数据表 ID
            record_ids: 记录 ID 列表
        
        Returns:
            删除结果
        """
        url = f"{self.base_url}/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_delete"
        payload = {"records": record_ids}
        
        result = self._make_request("POST", url, json=payload)
        
        if result.get("code") != 0:
            raise Exception(f"批量删除记录失败: {result}")
        
        return result.get("data", {})


if __name__ == "__main__":
    # 使用示例
    import json
    from pathlib import Path
    
    # 加载配置
    config_path = Path(__file__).parent / "config" / "alert_config.json"
    with open(config_path) as f:
        config = json.load(f)
    
    feishu_config = config.get("feishu", {})
    app_id = feishu_config.get("app_id")
    app_secret = feishu_config.get("app_secret")
    
    if not app_id or not app_secret:
        print("错误: 请在配置文件中设置 app_id 和 app_secret")
        exit(1)
    
    # 创建 API 客户端
    api = FeishuBitableAPI(app_id, app_secret)
    
    # 测试获取 token
    try:
        token = api.get_access_token()
        print(f"✓ 成功获取 access_token: {token[:20]}...")
    except Exception as e:
        print(f"✗ 获取 access_token 失败: {e}")
