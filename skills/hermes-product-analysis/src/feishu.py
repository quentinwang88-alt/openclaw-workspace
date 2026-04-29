#!/usr/bin/env python3
"""飞书多维表格轻量封装。"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

import requests


class FeishuAPIError(Exception):
    """飞书 API 异常。"""


@dataclass
class TableRecord:
    record_id: str
    fields: Dict[str, Any]


@dataclass
class FeishuBitableInfo:
    app_token: str
    table_id: str
    view_id: Optional[str] = None
    original_url: str = ""


def _load_openclaw_config() -> Dict[str, Any]:
    config_file = Path.home() / ".openclaw" / "openclaw.json"
    with config_file.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _resolve_app_credentials() -> tuple[str, str]:
    config = _load_openclaw_config()
    channel = config.get("channels", {}).get("feishu", {})
    app_id = str(channel.get("appId", "") or "").strip()
    app_secret = str(channel.get("appSecret", "") or "").strip()
    if not app_id or not app_secret:
        raise FeishuAPIError("缺少飞书 appId/appSecret 配置")
    return app_id, app_secret


def get_tenant_access_token() -> str:
    app_id, app_secret = _resolve_app_credentials()
    last_error = None
    for attempt in range(4):
        try:
            response = requests.post(
                "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
                json={"app_id": app_id, "app_secret": app_secret},
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()
            if payload.get("code") != 0:
                raise FeishuAPIError("获取飞书 access_token 失败: {msg}".format(msg=payload.get("msg")))
            return str(payload["tenant_access_token"])
        except (requests.RequestException, ValueError, FeishuAPIError) as exc:
            last_error = exc
            if attempt < 3:
                time.sleep(2 ** attempt)
    raise FeishuAPIError("获取飞书 access_token 最终失败: {err}".format(err=last_error))


def parse_feishu_bitable_url(url: str) -> Optional[FeishuBitableInfo]:
    if not url:
        return None
    cleaned = url.strip()

    import re

    match = re.search(r"/base/([a-zA-Z0-9]+)", cleaned)
    if match:
        params = parse_qs(urlparse(cleaned).query)
        table_id = params.get("table", [None])[0]
        view_id = params.get("view", [None])[0]
        if table_id:
            return FeishuBitableInfo(match.group(1), table_id, view_id=view_id, original_url=cleaned)

    match = re.search(r"/wiki/([a-zA-Z0-9]+)", cleaned)
    if match:
        params = parse_qs(urlparse(cleaned).query)
        table_id = params.get("table", [None])[0]
        view_id = params.get("view", [None])[0]
        if table_id:
            return FeishuBitableInfo(match.group(1), table_id, view_id=view_id, original_url=cleaned)

    match = re.search(r"/apps/([a-zA-Z0-9]+)/tables/([a-zA-Z0-9]+)", cleaned)
    if match:
        return FeishuBitableInfo(match.group(1), match.group(2), original_url=cleaned)
    return None


def resolve_wiki_bitable_app_token(wiki_token: str) -> str:
    last_error = None
    for attempt in range(4):
        try:
            response = requests.get(
                "https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node",
                headers={"Authorization": "Bearer {token}".format(token=get_tenant_access_token())},
                params={"token": wiki_token},
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()
            if payload.get("code") != 0:
                raise FeishuAPIError("解析 wiki token 失败: {msg}".format(msg=payload.get("msg")))
            node = payload.get("data", {}).get("node", {})
            if node.get("obj_type") != "bitable":
                raise FeishuAPIError("当前 wiki 节点不是 bitable: {obj_type}".format(obj_type=node.get("obj_type")))
            obj_token = str(node.get("obj_token", "") or "").strip()
            if not obj_token:
                raise FeishuAPIError("wiki 节点未返回底层 bitable obj_token")
            return obj_token
        except (requests.RequestException, ValueError, FeishuAPIError) as exc:
            last_error = exc
            if attempt < 3:
                time.sleep(2 ** attempt)
    raise FeishuAPIError("解析 wiki token 最终失败: {err}".format(err=last_error))


class FeishuBitableClient(object):
    def __init__(self, app_token: str, table_id: str):
        self.app_token = app_token
        self.table_id = table_id
        self._access_token = None  # type: Optional[str]
        self._expires_at = 0.0

    def _get_access_token(self) -> str:
        if self._access_token and time.time() < self._expires_at:
            return self._access_token
        self._access_token = get_tenant_access_token()
        self._expires_at = time.time() + 6900
        return self._access_token

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": "Bearer {token}".format(token=self._get_access_token()),
            "Content-Type": "application/json",
        }

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        last_error = None
        for attempt in range(3):
            try:
                response = requests.request(method, url, timeout=30, **kwargs)
                response.raise_for_status()
                return response
            except requests.RequestException as exc:
                last_error = exc
                if attempt < 2:
                    time.sleep(2 ** attempt)
        raise FeishuAPIError("飞书请求失败: {err}".format(err=last_error))

    def list_records(self, page_size: int = 100, limit: Optional[int] = None) -> List[TableRecord]:
        url = "https://open.feishu.cn/open-apis/bitable/v1/apps/{app}/tables/{table}/records".format(
            app=self.app_token,
            table=self.table_id,
        )
        page_token = None
        has_more = True
        records = []  # type: List[TableRecord]
        while has_more:
            params = {"page_size": min(max(page_size, 1), 500)}
            if page_token:
                params["page_token"] = page_token
            response = self._request("GET", url, headers=self._headers(), params=params)
            payload = response.json()
            if payload.get("code") != 0:
                raise FeishuAPIError("读取飞书记录失败: {msg}".format(msg=payload.get("msg")))
            data = payload.get("data", {})
            for item in data.get("items", []):
                records.append(TableRecord(record_id=item["record_id"], fields=item.get("fields", {})))
                if limit is not None and len(records) >= limit:
                    return records
            has_more = bool(data.get("has_more"))
            page_token = data.get("page_token")
        return records

    def list_field_names(self) -> List[str]:
        url = "https://open.feishu.cn/open-apis/bitable/v1/apps/{app}/tables/{table}/fields".format(
            app=self.app_token,
            table=self.table_id,
        )
        response = self._request("GET", url, headers=self._headers(), params={"page_size": 500})
        payload = response.json()
        if payload.get("code") != 0:
            raise FeishuAPIError("读取飞书字段失败: {msg}".format(msg=payload.get("msg")))
        return [str(item.get("field_name", "") or "").strip() for item in payload.get("data", {}).get("items", [])]

    def update_record_fields(self, record_id: str, fields: Dict[str, Any]) -> None:
        if not fields:
            return
        url = "https://open.feishu.cn/open-apis/bitable/v1/apps/{app}/tables/{table}/records/{record}".format(
            app=self.app_token,
            table=self.table_id,
            record=record_id,
        )
        response = self._request("PUT", url, headers=self._headers(), json={"fields": fields})
        payload = response.json()
        if payload.get("code") != 0:
            raise FeishuAPIError("更新飞书记录失败: {msg}".format(msg=payload.get("msg")))

    def get_tmp_download_url(self, file_token: str) -> str:
        url = "https://open.feishu.cn/open-apis/drive/v1/medias/batch_get_tmp_download_url"
        response = self._request(
            "GET",
            url,
            headers=self._headers(),
            params={"file_tokens": file_token},
        )
        payload = response.json()
        if payload.get("code") != 0:
            raise FeishuAPIError("获取图片临时链接失败: {msg}".format(msg=payload.get("msg")))
        urls = payload.get("data", {}).get("tmp_download_urls", [])
        if not urls:
            raise FeishuAPIError("飞书未返回临时下载链接")
        return str(urls[0]["tmp_download_url"])


class FeishuOpenClient(object):
    def __init__(self):
        self._access_token = None  # type: Optional[str]
        self._expires_at = 0.0

    def _get_access_token(self) -> str:
        if self._access_token and time.time() < self._expires_at:
            return self._access_token
        self._access_token = get_tenant_access_token()
        self._expires_at = time.time() + 6900
        return self._access_token

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": "Bearer {token}".format(token=self._get_access_token()),
            "Content-Type": "application/json",
        }

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        last_error = None
        for attempt in range(3):
            try:
                response = requests.request(method, url, timeout=60, **kwargs)
                response.raise_for_status()
                return response
            except requests.RequestException as exc:
                last_error = exc
                if attempt < 2:
                    time.sleep(2 ** attempt)
        raise FeishuAPIError("飞书请求失败: {err}".format(err=last_error))

    def get_tmp_download_url(self, file_token: str) -> str:
        normalized = str(file_token or "").strip()
        if not normalized:
            raise FeishuAPIError("file_token 不能为空")
        url = "https://open.feishu.cn/open-apis/drive/v1/medias/batch_get_tmp_download_url"
        last_error = None
        for attempt in range(4):
            try:
                response = self._request(
                    "GET",
                    url,
                    headers=self._headers(),
                    params={"file_tokens": normalized},
                )
                payload = response.json()
                if payload.get("code") != 0:
                    raise FeishuAPIError("获取图片临时链接失败: {msg}".format(msg=payload.get("msg")))
                urls = payload.get("data", {}).get("tmp_download_urls", [])
                if not urls:
                    raise FeishuAPIError("飞书未返回临时下载链接")
                return str(urls[0]["tmp_download_url"])
            except (FeishuAPIError, ValueError) as exc:
                last_error = exc
                if attempt < 3:
                    time.sleep(2 ** attempt)
        raise FeishuAPIError("获取图片临时链接最终失败: {err}".format(err=last_error))


class FeishuDocClient(FeishuOpenClient):
    def create_document(self, title: str, folder_token: str = "") -> Dict[str, str]:
        payload = {"title": title}
        if str(folder_token or "").strip():
            payload["folder_token"] = str(folder_token).strip()
        response = self._request(
            "POST",
            "https://open.feishu.cn/open-apis/docx/v1/documents",
            headers=self._headers(),
            json=payload,
        )
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError("创建飞书文档失败: {msg}".format(msg=result.get("msg")))
        document = result.get("data", {}).get("document", {})
        document_id = str(document.get("document_id", "") or "").strip()
        if not document_id:
            raise FeishuAPIError("飞书创建文档成功但未返回 document_id")
        return {
            "document_id": document_id,
            "doc_token": str(document.get("token", "") or document_id),
            "title": str(document.get("title", "") or title),
        }

    def append_markdown(self, document_id: str, content: str, batch_size: int = 40) -> Dict[str, int]:
        children = self._markdown_children(content)
        if not children:
            return {"line_count": 0, "batch_count": 0}
        url = "https://open.feishu.cn/open-apis/docx/v1/documents/{doc}/blocks/{doc}/children".format(doc=document_id)
        batch_count = 0
        for index in range(0, len(children), max(1, batch_size)):
            batch = children[index : index + max(1, batch_size)]
            response = self._request(
                "POST",
                url,
                headers=self._headers(),
                json={"children": batch},
            )
            result = response.json()
            if result.get("code") != 0:
                raise FeishuAPIError("写入飞书文档失败: {msg}".format(msg=result.get("msg")))
            batch_count += 1
        return {"line_count": len(children), "batch_count": batch_count}

    def send_text_message(self, receive_id_type: str, receive_id: str, text: str) -> Dict[str, str]:
        normalized_type = str(receive_id_type or "").strip()
        normalized_id = str(receive_id or "").strip()
        if not normalized_type or not normalized_id:
            raise FeishuAPIError("发送飞书消息缺少 receive_id_type 或 receive_id")
        url = "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type={type}".format(type=normalized_type)
        response = self._request(
            "POST",
            url,
            headers=self._headers(),
            json={
                "receive_id": normalized_id,
                "msg_type": "text",
                "content": json.dumps({"text": text}, ensure_ascii=False),
            },
        )
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError("发送飞书消息失败: {msg}".format(msg=result.get("msg")))
        return {
            "message_id": str(result.get("data", {}).get("message_id", "") or ""),
        }

    def send_webhook_text(self, webhook_url: str, text: str, secret: str = "") -> None:
        normalized = str(webhook_url or "").strip()
        if not normalized:
            raise FeishuAPIError("webhook_url 不能为空")
        payload = {"msg_type": "text", "content": {"text": text}}
        normalized_secret = str(secret or "").strip()
        if normalized_secret:
            timestamp = str(int(time.time()))
            string_to_sign = "{timestamp}\n{secret}".format(
                timestamp=timestamp,
                secret=normalized_secret,
            )
            sign = base64.b64encode(
                hmac.new(
                    string_to_sign.encode("utf-8"),
                    digestmod=hashlib.sha256,
                ).digest()
            ).decode("utf-8")
            payload["timestamp"] = timestamp
            payload["sign"] = sign
        response = requests.post(
            normalized,
            json=payload,
            timeout=30,
        )
        response.raise_for_status()
        response_payload = response.json()
        status_code = int(response_payload.get("StatusCode", response_payload.get("code", 0)) or 0)
        if status_code not in {0, 200}:
            raise FeishuAPIError("Webhook 推送失败: {payload}".format(payload=response_payload))

    def _markdown_lines(self, content: str) -> List[str]:
        normalized = str(content or "").replace("\r\n", "\n").replace("\r", "\n")
        lines = normalized.split("\n")
        if not lines:
            return []
        return lines

    def _markdown_children(self, content: str) -> List[Dict[str, Any]]:
        lines = self._markdown_lines(content)
        children: List[Dict[str, Any]] = []
        index = 0
        while index < len(lines):
            line = str(lines[index] or "")
            stripped = line.strip()
            if not stripped:
                index += 1
                continue

            if stripped.startswith("|"):
                table_lines: List[str] = []
                while index < len(lines) and str(lines[index] or "").strip().startswith("|"):
                    table_lines.append(str(lines[index] or ""))
                    index += 1
                children.extend(self._build_table_as_text_blocks(table_lines))
                continue

            if stripped in {"---", "***", "___"}:
                children.append({"block_type": 22, "divider": {}})
                index += 1
                continue

            if stripped.startswith("> "):
                children.append(self._build_rich_text_block(stripped[2:].strip(), block_type=15, key="quote"))
                index += 1
                continue

            heading_level = self._heading_level(stripped)
            if heading_level:
                heading_text = stripped[heading_level + 1 :].strip()
                children.append(
                    self._build_rich_text_block(
                        heading_text,
                        block_type=2 + heading_level,
                        key="heading{level}".format(level=heading_level),
                    )
                )
                index += 1
                continue

            ordered_parts = self._ordered_parts(stripped)
            if ordered_parts:
                children.append(self._build_rich_text_block(ordered_parts, block_type=13, key="ordered"))
                index += 1
                continue

            bullet_text = self._bullet_text(stripped)
            if bullet_text is not None:
                children.append(self._build_rich_text_block(bullet_text, block_type=12, key="bullet"))
                index += 1
                continue

            children.append(self._build_text_block(stripped))
            index += 1
        return children

    def _build_table_as_text_blocks(self, table_lines: List[str]) -> List[Dict[str, Any]]:
        rows: List[List[str]] = []
        for raw_line in table_lines:
            stripped = str(raw_line or "").strip()
            if not stripped.startswith("|"):
                continue
            cells = [cell.strip() for cell in stripped.strip("|").split("|")]
            if cells and all(set(cell) <= {"-", ":"} for cell in cells):
                continue
            rows.append(cells)
        if not rows:
            return []
        widths = [0] * max(len(row) for row in rows)
        for row in rows:
            for idx, cell in enumerate(row):
                widths[idx] = max(widths[idx], len(cell))
        blocks: List[Dict[str, Any]] = []
        for row_index, row in enumerate(rows):
            padded = []
            for idx, cell in enumerate(row):
                padded.append(cell.ljust(widths[idx]))
            line = " | ".join(padded)
            if row_index == 0:
                blocks.append(self._build_rich_text_block(line, block_type=5, key="heading3"))
            else:
                blocks.append(self._build_text_block(line))
        return blocks

    def _heading_level(self, stripped: str) -> int:
        level = 0
        while level < len(stripped) and stripped[level] == "#":
            level += 1
        if 1 <= level <= 6 and len(stripped) > level and stripped[level] == " ":
            return level
        return 0

    def _ordered_parts(self, stripped: str) -> str:
        marker, _, rest = stripped.partition(". ")
        if marker.isdigit() and rest:
            return rest.strip()
        return ""

    def _bullet_text(self, stripped: str) -> Optional[str]:
        for prefix in ("- ", "* "):
            if stripped.startswith(prefix):
                return stripped[len(prefix) :].strip()
        return None

    def _build_rich_text_block(self, text: str, block_type: int, key: str) -> Dict[str, Any]:
        return {
            "block_type": block_type,
            key: {
                "elements": [{"text_run": {"content": str(text or "")}}],
                "style": {},
            },
        }

    def _build_text_block(self, line: str) -> Dict[str, Any]:
        return self._build_rich_text_block(str(line or ""), block_type=2, key="text")


def build_bitable_client(feishu_url: str = "", app_token: str = "", bitable_table_id: str = "") -> FeishuBitableClient:
    resolved_app_token = str(app_token or "").strip()
    resolved_table_id = str(bitable_table_id or "").strip()

    if feishu_url:
        info = parse_feishu_bitable_url(feishu_url)
        if not info:
            raise FeishuAPIError("无法解析飞书 URL: {url}".format(url=feishu_url))
        resolved_app_token = info.app_token
        resolved_table_id = info.table_id
        if "/wiki/" in info.original_url:
            resolved_app_token = resolve_wiki_bitable_app_token(info.app_token)

    if not resolved_app_token or not resolved_table_id:
        raise FeishuAPIError("缺少可用的飞书 app_token/table_id")
    return FeishuBitableClient(app_token=resolved_app_token, table_id=resolved_table_id)
