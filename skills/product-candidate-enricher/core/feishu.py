#!/usr/bin/env python3
"""Feishu bitable helpers for the product candidate enricher skill."""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

import requests


class FeishuAPIError(Exception):
    """Raised when a Feishu API request fails."""


@dataclass
class FeishuBitableInfo:
    app_token: str
    table_id: str
    view_id: Optional[str] = None
    original_url: str = ""
    is_wiki: bool = False


@dataclass
class TableField:
    field_id: str
    field_name: str
    field_type: int
    ui_type: Optional[str]
    property: Optional[Dict[str, Any]]
    description: Optional[Dict[str, Any]] = None


@dataclass
class TableRecord:
    record_id: str
    fields: Dict[str, Any]


def parse_feishu_bitable_url(url: str) -> Optional[FeishuBitableInfo]:
    if not url:
        return None

    cleaned = url.strip()
    params = parse_qs(urlparse(cleaned).query)
    table_id = params.get("table", [None])[0]
    view_id = params.get("view", [None])[0]

    match = re.search(r"/base/([a-zA-Z0-9]+)", cleaned)
    if match and table_id:
        return FeishuBitableInfo(
            app_token=match.group(1),
            table_id=table_id,
            view_id=view_id,
            original_url=cleaned,
            is_wiki=False,
        )

    match = re.search(r"/wiki/([a-zA-Z0-9]+)", cleaned)
    if match and table_id:
        return FeishuBitableInfo(
            app_token=match.group(1),
            table_id=table_id,
            view_id=view_id,
            original_url=cleaned,
            is_wiki=True,
        )

    match = re.search(r"/apps/([a-zA-Z0-9]+)/tables/([a-zA-Z0-9]+)", cleaned)
    if match:
        return FeishuBitableInfo(
            app_token=match.group(1),
            table_id=match.group(2),
            original_url=cleaned,
            is_wiki=False,
        )

    return None


def _load_openclaw_config() -> Dict[str, Any]:
    config_file = Path.home() / ".openclaw" / "openclaw.json"
    with open(config_file, "r", encoding="utf-8") as handle:
        return json.load(handle)


def get_tenant_access_token() -> str:
    config = _load_openclaw_config()
    app_id = config["channels"]["feishu"]["appId"]
    app_secret = config["channels"]["feishu"]["appSecret"]

    last_error: Optional[Exception] = None
    for attempt in range(4):
        try:
            response = requests.post(
                "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
                json={"app_id": app_id, "app_secret": app_secret},
                timeout=30,
            )
            result = response.json()
            if result.get("code") != 0:
                raise FeishuAPIError(f"获取飞书 access_token 失败: {result.get('msg')}")
            return result["tenant_access_token"]
        except (requests.exceptions.RequestException, ValueError, FeishuAPIError) as exc:
            last_error = exc
            if attempt < 3:
                time.sleep(2 ** attempt)
    raise FeishuAPIError(f"获取飞书 access_token 最终失败: {last_error}")


def resolve_wiki_bitable_app_token(wiki_token: str) -> str:
    last_error: Optional[Exception] = None
    for attempt in range(4):
        try:
            response = requests.get(
                "https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node",
                headers={"Authorization": f"Bearer {get_tenant_access_token()}"},
                params={"token": wiki_token},
                timeout=30,
            )
            result = response.json()
            if result.get("code") != 0:
                raise FeishuAPIError(f"解析 wiki token 失败: {result.get('msg')}")
            node = result.get("data", {}).get("node", {})
            if node.get("obj_type") != "bitable":
                raise FeishuAPIError(f"当前 wiki 节点不是 bitable: {node.get('obj_type')}")
            obj_token = node.get("obj_token")
            if not obj_token:
                raise FeishuAPIError("wiki 节点未返回底层 bitable obj_token")
            return obj_token
        except (requests.exceptions.RequestException, ValueError, FeishuAPIError) as exc:
            last_error = exc
            if attempt < 3:
                time.sleep(2 ** attempt)
    raise FeishuAPIError(f"解析 wiki token 最终失败: {last_error}")


class FeishuBitableClient:
    """Minimal Feishu bitable client with list/update helpers."""

    def __init__(self, app_token: str, table_id: str):
        self.app_token = app_token
        self.table_id = table_id
        self.access_token: Optional[str] = None
        self.token_expires_at: float = 0

    def _get_access_token(self) -> str:
        if self.access_token and time.time() < self.token_expires_at:
            return self.access_token
        config = _load_openclaw_config()
        self.access_token = get_tenant_access_token()
        self.token_expires_at = time.time() + config.get("feishu", {}).get("tokenExpire", 7200) - 300
        return self.access_token

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Content-Type": "application/json",
        }

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        last_error: Optional[Exception] = None
        for attempt in range(3):
            try:
                return requests.request(method, url, timeout=30, **kwargs)
            except requests.exceptions.RequestException as exc:
                last_error = exc
                if attempt < 2:
                    time.sleep(2 ** attempt)
        raise FeishuAPIError(f"飞书 API 请求失败: {last_error}")

    def list_fields(self) -> List[TableField]:
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/fields"
        response = self._request("GET", url, headers=self._headers(), params={"page_size": 500})
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError(f"获取字段定义失败: {result.get('msg')}")

        fields: List[TableField] = []
        for item in result.get("data", {}).get("items", []):
            fields.append(
                TableField(
                    field_id=item["field_id"],
                    field_name=item["field_name"],
                    field_type=item["type"],
                    ui_type=item.get("ui_type"),
                    property=item.get("property"),
                    description=item.get("description"),
                )
            )
        return fields

    def list_records(self, page_size: int = 100, limit: Optional[int] = None) -> List[TableRecord]:
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records"
        page_token = None
        has_more = True
        records: List[TableRecord] = []

        while has_more:
            params: Dict[str, Any] = {"page_size": min(max(page_size, 1), 500)}
            if page_token:
                params["page_token"] = page_token

            response = self._request("GET", url, headers=self._headers(), params=params)
            result = response.json()
            if result.get("code") != 0:
                raise FeishuAPIError(f"读取记录失败: {result.get('msg')}")

            data = result.get("data", {})
            for item in data.get("items", []):
                records.append(TableRecord(record_id=item["record_id"], fields=item.get("fields", {})))
                if limit is not None and len(records) >= limit:
                    return records

            has_more = data.get("has_more", False)
            page_token = data.get("page_token")

        return records

    def batch_update_records(self, records: List[Dict[str, Any]]) -> None:
        if not records:
            return
        url = (
            f"https://open.feishu.cn/open-apis/bitable/v1/apps/"
            f"{self.app_token}/tables/{self.table_id}/records/batch_update"
        )
        response = self._request("POST", url, headers=self._headers(), json={"records": records})
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError(f"批量更新记录失败: {result.get('msg')}")

    def update_record_fields(self, record_id: str, fields: Dict[str, Any]) -> None:
        if not fields:
            return
        url = (
            f"https://open.feishu.cn/open-apis/bitable/v1/apps/"
            f"{self.app_token}/tables/{self.table_id}/records/{record_id}"
        )
        response = self._request("PUT", url, headers=self._headers(), json={"fields": fields})
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError(f"更新记录失败: {result.get('msg')}")

    def update_field(
        self,
        field_id: str,
        field_name: str,
        field_type: int,
        property: Optional[Dict[str, Any]] = None,
        description: Optional[Dict[str, Any]] = None,
        ui_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        url = (
            f"https://open.feishu.cn/open-apis/bitable/v1/apps/"
            f"{self.app_token}/tables/{self.table_id}/fields/{field_id}"
        )
        payload: Dict[str, Any] = {
            "field_name": field_name,
            "type": field_type,
        }
        if property is not None:
            payload["property"] = property
        if description is not None:
            payload["description"] = description
        if ui_type is not None:
            payload["ui_type"] = ui_type

        response = self._request("PUT", url, headers=self._headers(), json=payload)
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError(f"更新字段失败: {result.get('msg')}")
        return result.get("data", {})
