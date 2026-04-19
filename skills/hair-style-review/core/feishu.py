#!/usr/bin/env python3
"""Feishu helpers for standalone hair style review."""

from __future__ import annotations

import json
import mimetypes
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse
import re

import requests


class FeishuAPIError(Exception):
    """Raised when Feishu API requests fail."""


@dataclass
class FeishuBitableInfo:
    app_token: str
    table_id: str
    view_id: Optional[str] = None
    sheet_id: Optional[str] = None
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
    sheet_id = params.get("sheet", [None])[0]

    match = re.search(r"/base/([a-zA-Z0-9]+)", cleaned)
    if match and table_id:
        return FeishuBitableInfo(
            app_token=match.group(1),
            table_id=table_id,
            view_id=view_id,
            sheet_id=sheet_id,
            original_url=cleaned,
            is_wiki=False,
        )

    match = re.search(r"/wiki/([a-zA-Z0-9]+)", cleaned)
    if match and table_id:
        return FeishuBitableInfo(
            app_token=match.group(1),
            table_id=table_id,
            view_id=view_id,
            sheet_id=sheet_id,
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


def resolve_bitable_app_token(info: FeishuBitableInfo) -> str:
    if not info.is_wiki:
        return info.app_token

    last_error: Optional[Exception] = None
    for attempt in range(4):
        try:
            response = requests.get(
                "https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node",
                headers={"Authorization": f"Bearer {get_tenant_access_token()}"},
                params={"token": info.app_token},
                timeout=30,
            )
            result = response.json()
            if result.get("code") != 0:
                raise FeishuAPIError(f"解析 wiki token 失败: {result.get('msg')}")

            node = result.get("data", {}).get("node", {})
            obj_type = node.get("obj_type")
            obj_token = node.get("obj_token")

            if obj_type == "bitable":
                if not obj_token:
                    raise FeishuAPIError("wiki bitable 节点未返回 obj_token")
                return obj_token

            if obj_type == "sheet" and info.table_id:
                if not obj_token:
                    raise FeishuAPIError("wiki sheet 节点未返回 spreadsheet token")
                return resolve_sheet_bitable_app_token(
                    spreadsheet_token=obj_token,
                    table_id=info.table_id,
                    sheet_id=info.sheet_id,
                )

            raise FeishuAPIError(f"当前 wiki 节点不支持解析为 bitable: {obj_type}")
        except (requests.exceptions.RequestException, ValueError, FeishuAPIError) as exc:
            last_error = exc
            if attempt < 3:
                time.sleep(2 ** attempt)
    raise FeishuAPIError(f"解析 wiki bitable app token 最终失败: {last_error}")


def resolve_sheet_bitable_app_token(
    spreadsheet_token: str,
    table_id: str,
    sheet_id: Optional[str] = None,
) -> str:
    url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/metainfo"
    response = requests.get(
        url,
        headers={"Authorization": f"Bearer {get_tenant_access_token()}"},
        timeout=30,
    )
    result = response.json()
    if result.get("code") != 0:
        raise FeishuAPIError(f"读取 sheets metainfo 失败: {result.get('msg')}")

    sheets = result.get("data", {}).get("sheets", [])
    target_sheet: Optional[Dict[str, Any]] = None

    if sheet_id:
        for item in sheets:
            if item.get("sheetId") == sheet_id:
                target_sheet = item
                break

    if target_sheet is None:
        for item in sheets:
            block_info = item.get("blockInfo") or {}
            block_token = block_info.get("blockToken") or ""
            if block_info.get("blockType") == "BITABLE_BLOCK" and block_token.endswith(f"_{table_id}"):
                target_sheet = item
                break

    if target_sheet is None:
        raise FeishuAPIError(f"未在 spreadsheet 中找到 table_id={table_id} 对应的 BITABLE_BLOCK")

    block_info = target_sheet.get("blockInfo") or {}
    block_token = block_info.get("blockToken") or ""
    suffix = f"_{table_id}"
    if not block_token.endswith(suffix):
        raise FeishuAPIError(f"BITABLE_BLOCK token 与 table_id 不匹配: {block_token}")

    app_token = block_token[: -len(suffix)]
    if not app_token:
        raise FeishuAPIError(f"无法从 BITABLE_BLOCK token 解析 app token: {block_token}")
    return app_token


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
        items = result.get("data", {}).get("items", [])
        return [
            TableField(
                field_id=item["field_id"],
                field_name=item["field_name"],
                field_type=item["type"],
                ui_type=item.get("ui_type"),
                property=item.get("property"),
                description=item.get("description"),
            )
            for item in items
        ]

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

    def resolve_image_url(self, raw_value: Any) -> Optional[str]:
        if raw_value is None:
            return None
        if isinstance(raw_value, str):
            value = raw_value.strip()
            if value.startswith(("http://", "https://")):
                return value
            return None
        if isinstance(raw_value, dict):
            file_token = raw_value.get("file_token")
            if file_token:
                return self.get_tmp_download_url(str(file_token))
            for key in ("url", "link", "text"):
                value = raw_value.get(key)
                if isinstance(value, str) and value.startswith(("http://", "https://")):
                    return value
            return None
        if isinstance(raw_value, list):
            for item in raw_value:
                resolved = self.resolve_image_url(item)
                if resolved:
                    return resolved
        return None

    def download_url_to_file(self, url: str, destination: Path) -> Tuple[Path, str]:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        content_type = response.headers.get("Content-Type") or mimetypes.guess_type(url)[0] or "image/jpeg"
        suffix = mimetypes.guess_extension(content_type.split(";")[0].strip()) or ".jpg"
        output_path = destination.with_suffix(suffix)
        output_path.write_bytes(response.content)
        return output_path, content_type
