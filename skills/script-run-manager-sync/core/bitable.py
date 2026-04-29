#!/usr/bin/env python3
"""飞书多维表格读写封装。"""

from __future__ import annotations

import json
import mimetypes
import time
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests


class FeishuAPIError(Exception):
    """飞书 API 异常。"""


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
        return [
            TableField(
                field_id=item["field_id"],
                field_name=item["field_name"],
                field_type=item["type"],
                ui_type=item.get("ui_type"),
                property=item.get("property"),
                description=item.get("description"),
            )
            for item in result.get("data", {}).get("items", [])
            if item.get("field_name")
        ]

    def list_field_names(self) -> List[str]:
        return [item.field_name for item in self.list_fields()]

    def create_field(
        self,
        field_name: str,
        field_type: int = 1,
        ui_type: str = "Text",
        property: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/fields"
        payload: Dict[str, Any] = {"field_name": field_name, "type": field_type, "ui_type": ui_type}
        if property is not None:
            payload["property"] = property
        response = self._request("POST", url, headers=self._headers(), json=payload)
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError(f"创建字段失败: {result.get('msg')}")
        return result.get("data", {})

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

    def batch_create_records(self, records: List[Dict[str, Any]]) -> None:
        if not records:
            return
        url = (
            f"https://open.feishu.cn/open-apis/bitable/v1/apps/"
            f"{self.app_token}/tables/{self.table_id}/records/batch_create"
        )
        response = self._request("POST", url, headers=self._headers(), json={"records": records})
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError(f"批量创建记录失败: {result.get('msg')}")

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

    def get_tmp_download_url(self, file_token: str) -> str:
        url = "https://open.feishu.cn/open-apis/drive/v1/medias/batch_get_tmp_download_url"
        response = self._request(
            "GET",
            url,
            headers=self._headers(),
            params={"file_tokens": file_token},
        )
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError(f"获取临时下载链接失败: {result.get('msg')}")
        urls = result.get("data", {}).get("tmp_download_urls", [])
        if not urls:
            raise FeishuAPIError("飞书未返回临时下载链接")
        return urls[0]["tmp_download_url"]

    def download_attachment_bytes(self, attachment: Dict[str, Any]) -> Tuple[bytes, str, str, int]:
        file_token = str(attachment.get("file_token", "")).strip()
        if not file_token:
            raise FeishuAPIError("附件缺少 file_token")

        tmp_url = self.get_tmp_download_url(file_token)
        response = requests.get(tmp_url, timeout=60)
        response.raise_for_status()

        content = response.content
        file_name = str(attachment.get("name") or f"{file_token}.bin")
        content_type = str(attachment.get("type") or mimetypes.guess_type(file_name)[0] or "application/octet-stream")
        size = int(attachment.get("size") or len(content))
        return content, file_name, content_type, size

    def upload_attachment(self, content: bytes, file_name: str, content_type: str, size: Optional[int] = None) -> Dict[str, Any]:
        upload_url = "https://open.feishu.cn/open-apis/drive/v1/medias/upload_all"
        payload = {
            "file_name": file_name,
            "parent_type": "bitable_image",
            "parent_node": self.app_token,
            "size": str(size or len(content)),
        }
        files = {
            "file": (file_name, BytesIO(content), content_type),
        }
        headers = {
            "Authorization": f"Bearer {self._get_access_token()}",
        }
        response = self._request("POST", upload_url, headers=headers, data=payload, files=files)
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError(f"上传附件失败: {result.get('msg')}")

        file_token = result.get("data", {}).get("file_token")
        if not file_token:
            raise FeishuAPIError("飞书未返回上传后的 file_token")
        return {
            "file_token": file_token,
            "name": file_name,
            "size": int(size or len(content)),
            "type": content_type,
        }
