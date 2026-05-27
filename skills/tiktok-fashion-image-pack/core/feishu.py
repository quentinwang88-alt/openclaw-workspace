#!/usr/bin/env python3
"""Feishu bitable helpers for the likeU product image-pack pipeline."""

from __future__ import annotations

import json
import mimetypes
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import requests


DEFAULT_TIMEOUT = 30


class FeishuAPIError(Exception):
    """Raised when a Feishu API request fails."""


@dataclass(frozen=True)
class FeishuBitableInfo:
    app_token: str
    table_id: str
    view_id: Optional[str] = None
    original_url: str = ""
    is_wiki: bool = False


@dataclass(frozen=True)
class TableField:
    field_id: str
    field_name: str
    field_type: int
    ui_type: Optional[str] = None
    property: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
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
        return FeishuBitableInfo(match.group(1), table_id, view_id, cleaned, False)

    match = re.search(r"/wiki/([a-zA-Z0-9]+)", cleaned)
    if match and table_id:
        return FeishuBitableInfo(match.group(1), table_id, view_id, cleaned, True)

    match = re.search(r"/apps/([a-zA-Z0-9]+)/tables/([a-zA-Z0-9]+)", cleaned)
    if match:
        return FeishuBitableInfo(match.group(1), match.group(2), None, cleaned, False)

    match = re.match(r"^([a-zA-Z0-9]+)[/,]([a-zA-Z0-9]+)$", cleaned)
    if match:
        return FeishuBitableInfo(match.group(1), match.group(2), None, cleaned, False)

    return None


def _load_openclaw_config() -> Dict[str, Any]:
    config_file = Path.home() / ".openclaw" / "openclaw.json"
    with config_file.open("r", encoding="utf-8") as handle:
        return json.load(handle)


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
        app_id = config["channels"]["feishu"]["appId"]
        app_secret = config["channels"]["feishu"]["appSecret"]

        last_error: Optional[Exception] = None
        for attempt in range(4):
            try:
                response = requests.post(
                    "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
                    json={"app_id": app_id, "app_secret": app_secret},
                    timeout=DEFAULT_TIMEOUT,
                )
                result = response.json()
                if result.get("code") != 0:
                    raise FeishuAPIError(f"获取飞书 access_token 失败: {result.get('msg')}")
                self.access_token = result["tenant_access_token"]
                self.token_expires_at = time.time() + int(result.get("expire", 7200)) - 300
                return self.access_token
            except (requests.exceptions.RequestException, ValueError, KeyError, FeishuAPIError) as exc:
                last_error = exc
                if attempt < 3:
                    time.sleep(2 ** attempt)
        raise FeishuAPIError(f"获取飞书 access_token 最终失败: {last_error}")

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Content-Type": "application/json",
        }

    def _request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        last_error: Optional[Exception] = None
        for attempt in range(4):
            try:
                response = requests.request(method, url, timeout=DEFAULT_TIMEOUT, **kwargs)
                if response.status_code >= 500:
                    raise FeishuAPIError(f"飞书服务异常: {response.status_code} {response.text[:300]}")
                try:
                    result = response.json()
                except ValueError:
                    result = {}
                if result.get("code") in {99991663, 99991664, 99991668, 99991671} or "Invalid access token" in str(result.get("msg", "")):
                    self.access_token = None
                    self.token_expires_at = 0
                    headers = dict(kwargs.get("headers") or {})
                    if "Authorization" in headers:
                        headers["Authorization"] = f"Bearer {self._get_access_token()}"
                        kwargs["headers"] = headers
                    raise FeishuAPIError(f"飞书 access_token 已失效，刷新后重试: {result.get('msg')}")
                return response
            except (requests.exceptions.RequestException, FeishuAPIError) as exc:
                last_error = exc
                if attempt < 3:
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
            )
            for item in result.get("data", {}).get("items", [])
            if item.get("field_name")
        ]

    def list_records(
        self,
        *,
        page_size: int = 100,
        limit: Optional[int] = None,
        view_id: Optional[str] = None,
    ) -> List[TableRecord]:
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records"
        page_token = None
        records: List[TableRecord] = []

        while True:
            params: Dict[str, Any] = {"page_size": min(max(page_size, 1), 500)}
            if page_token:
                params["page_token"] = page_token
            if view_id:
                params["view_id"] = view_id
            response = self._request("GET", url, headers=self._headers(), params=params)
            result = response.json()
            if result.get("code") != 0:
                raise FeishuAPIError(f"读取记录失败: {result.get('msg')}")
            data = result.get("data", {})
            for item in data.get("items", []):
                records.append(TableRecord(record_id=item["record_id"], fields=item.get("fields", {})))
                if limit is not None and len(records) >= limit:
                    return records
            if not data.get("has_more"):
                return records
            page_token = data.get("page_token")

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

    def download_attachment(self, attachment: Dict[str, Any], output_dir: Path) -> Path:
        file_token = attachment.get("file_token")
        if not file_token:
            raise FeishuAPIError("附件缺少 file_token")
        output_dir.mkdir(parents=True, exist_ok=True)
        file_name = sanitize_filename(attachment.get("name") or f"{file_token}.jpg")
        target = unique_download_path(output_dir / file_name)
        response = self._request("GET", self.get_tmp_download_url(file_token))
        if response.status_code >= 400:
            raise FeishuAPIError(f"附件下载失败: {response.status_code}")
        target.write_bytes(response.content)
        return target

    def upload_attachment(self, path: Path) -> Dict[str, Any]:
        file_size = path.stat().st_size
        content_type = mimetypes.guess_type(path.name)[0] or "image/png"
        upload_url = "https://open.feishu.cn/open-apis/drive/v1/medias/upload_all"
        payload = {
            "file_name": path.name,
            "parent_type": "bitable_image",
            "parent_node": self.app_token,
            "size": str(file_size),
        }
        headers = {"Authorization": f"Bearer {self._get_access_token()}"}
        with path.open("rb") as handle:
            files = {"file": (path.name, handle, content_type)}
            response = self._request("POST", upload_url, headers=headers, data=payload, files=files)
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError(f"上传附件失败: {result.get('msg')}")
        file_token = result.get("data", {}).get("file_token")
        if not file_token:
            raise FeishuAPIError("飞书未返回上传后的 file_token")
        return {
            "file_token": file_token,
            "name": path.name,
            "size": file_size,
            "type": content_type,
        }


def resolve_client_from_url(feishu_url: str) -> Tuple[FeishuBitableClient, Optional[str]]:
    info = parse_feishu_bitable_url(feishu_url)
    if not info:
        raise ValueError(f"无法解析飞书 URL: {feishu_url}")
    app_token = info.app_token
    if info.is_wiki:
        bootstrap = FeishuBitableClient(app_token="bootstrap", table_id=info.table_id)
        app_token = resolve_wiki_bitable_app_token(bootstrap, info.app_token)
    return FeishuBitableClient(app_token=app_token, table_id=info.table_id), info.view_id


def resolve_wiki_bitable_app_token(client: FeishuBitableClient, wiki_token: str) -> str:
    response = client._request(
        "GET",
        "https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node",
        headers={"Authorization": f"Bearer {client._get_access_token()}"},
        params={"token": wiki_token},
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


def extract_attachments(raw_value: Any) -> List[Dict[str, Any]]:
    if isinstance(raw_value, list):
        return [item for item in raw_value if isinstance(item, dict) and item.get("file_token")]
    if isinstance(raw_value, dict) and raw_value.get("file_token"):
        return [raw_value]
    return []


def normalize_cell_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, dict):
        for key in ("text", "name", "link", "url"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        return " / ".join(item for item in (normalize_cell_value(v) for v in value) if item)
    return str(value)


def sanitize_filename(name: str) -> str:
    cleaned = re.sub(r"[\\/:*?\"<>|]", "_", str(name or "")).strip()
    cleaned = cleaned[:120].strip("._")
    return cleaned or "image"


def unique_download_path(path: Path) -> Path:
    """Avoid overwriting same-named Feishu attachments in one output directory."""
    if not path.exists():
        return path
    stem = path.stem or "image"
    suffix = path.suffix
    for index in range(2, 1000):
        candidate = path.with_name(f"{stem}_{index}{suffix}")
        if not candidate.exists():
            return candidate
    raise FeishuAPIError(f"无法生成唯一附件文件名: {path}")
