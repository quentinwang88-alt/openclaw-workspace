#!/usr/bin/env python3
"""飞书多维表格客户端。"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from app.config import get_settings


class FeishuBitableClient:
    """飞书多维表格 API 封装。"""

    def __init__(
        self,
        app_id: Optional[str] = None,
        app_secret: Optional[str] = None,
        app_token: Optional[str] = None,
        table_id: Optional[str] = None,
    ):
        settings = get_settings()
        self.settings = settings
        self.app_id = app_id or settings.feishu_app_id
        self.app_secret = app_secret or settings.feishu_app_secret
        self.app_token = app_token or settings.feishu_app_token
        self.table_id = table_id or settings.feishu_table_id
        self.base_url = "https://open.feishu.cn/open-apis"
        self._access_token: Optional[str] = None
        self._expires_at: float = 0

        if (not self.app_id or not self.app_secret) and Path.home().joinpath(".openclaw/openclaw.json").exists():
            config = json.loads(Path.home().joinpath(".openclaw/openclaw.json").read_text(encoding="utf-8"))
            self.app_id = self.app_id or config.get("channels", {}).get("feishu", {}).get("appId", "")
            self.app_secret = self.app_secret or config.get("channels", {}).get("feishu", {}).get("appSecret", "")

    def _get_access_token(self) -> str:
        if self._access_token and time.time() < self._expires_at:
            return self._access_token

        response = requests.post(
            f"{self.base_url}/auth/v3/tenant_access_token/internal",
            json={"app_id": self.app_id, "app_secret": self.app_secret},
            timeout=30,
        )
        result = response.json()
        if result.get("code") != 0:
            raise Exception(f"获取飞书 token 失败: {result.get('msg')}")

        self._access_token = result["tenant_access_token"]
        self._expires_at = time.time() + result.get("expire", 7200) - 300
        return self._access_token

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Content-Type": "application/json",
        }

    def _request(self, method: str, url: str, **kwargs) -> Dict[str, Any]:
        last_error: Optional[Exception] = None
        for attempt in range(3):
            try:
                response = requests.request(method, url, headers=self._headers(), timeout=30, **kwargs)
                result = response.json()
                if result.get("code") != 0:
                    raise Exception(result)
                return result.get("data", {})
            except Exception as exc:
                last_error = exc
                if attempt < 2:
                    wait_time = 2 ** attempt
                    time.sleep(wait_time)
        raise Exception(f"飞书请求失败: {last_error}")

    def list_fields(self) -> List[Dict[str, Any]]:
        data = self._request(
            "GET",
            f"{self.base_url}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/fields",
            params={"page_size": 500},
        )
        return data.get("items", [])

    def list_all_records(self) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        page_token = None
        page_size = min(max(self.settings.feishu_read_page_size, 1), 500)
        while True:
            params = {"page_size": page_size}
            if page_token:
                params["page_token"] = page_token
            data = self._request(
                "GET",
                f"{self.base_url}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records",
                params=params,
            )
            records.extend(data.get("items", []))
            if not data.get("has_more"):
                break
            page_token = data.get("page_token")
            if not page_token:
                break
        return records

    def batch_create_records(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        return self._request(
            "POST",
            f"{self.base_url}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records/batch_create",
            json={"records": records},
        )

    def batch_update_records(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        return self._request(
            "POST",
            f"{self.base_url}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records/batch_update",
            json={"records": records},
        )

    def update_record(self, record_id: str, fields: Dict[str, Any]) -> Dict[str, Any]:
        return self._request(
            "PUT",
            f"{self.base_url}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records/{record_id}",
            json={"fields": fields},
        )

    def delete_record(self, record_id: str) -> Dict[str, Any]:
        return self._request(
            "DELETE",
            f"{self.base_url}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records/{record_id}",
        )
