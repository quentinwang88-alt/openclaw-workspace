#!/usr/bin/env python3
"""飞书 Bitable 与 IM 接口。"""

from __future__ import annotations

import json
import mimetypes
import time
import uuid
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
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


def _resolve_app_credentials(
    app_id: Optional[str] = None,
    app_secret: Optional[str] = None,
) -> Tuple[str, str]:
    if app_id and app_secret:
        return app_id, app_secret
    config = _load_openclaw_config()
    channel = config.get("channels", {}).get("feishu", {})
    resolved_app_id = app_id or channel.get("appId", "")
    resolved_app_secret = app_secret or channel.get("appSecret", "")
    if not resolved_app_id or not resolved_app_secret:
        raise FeishuAPIError("缺少飞书 app_id/app_secret 配置")
    return resolved_app_id, resolved_app_secret


def get_tenant_access_token(app_id: Optional[str] = None, app_secret: Optional[str] = None) -> str:
    resolved_app_id, resolved_app_secret = _resolve_app_credentials(app_id=app_id, app_secret=app_secret)
    last_error = None
    for attempt in range(4):
        try:
            response = requests.post(
                "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
                json={"app_id": resolved_app_id, "app_secret": resolved_app_secret},
                timeout=30,
            )
            response.raise_for_status()
            result = response.json()
            if result.get("code") != 0:
                raise FeishuAPIError("获取飞书 access_token 失败: {msg}".format(msg=result.get("msg")))
            return str(result["tenant_access_token"])
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
            return FeishuBitableInfo(
                app_token=match.group(1),
                table_id=table_id,
                view_id=view_id,
                original_url=cleaned,
            )

    match = re.search(r"/wiki/([a-zA-Z0-9]+)", cleaned)
    if match:
        params = parse_qs(urlparse(cleaned).query)
        table_id = params.get("table", [None])[0]
        view_id = params.get("view", [None])[0]
        if table_id:
            return FeishuBitableInfo(
                app_token=match.group(1),
                table_id=table_id,
                view_id=view_id,
                original_url=cleaned,
            )

    match = re.search(r"/apps/([a-zA-Z0-9]+)/tables/([a-zA-Z0-9]+)", cleaned)
    if match:
        return FeishuBitableInfo(
            app_token=match.group(1),
            table_id=match.group(2),
            original_url=cleaned,
        )
    return None


def resolve_wiki_bitable_app_token(
    wiki_token: str,
    app_id: Optional[str] = None,
    app_secret: Optional[str] = None,
) -> str:
    last_error = None
    for attempt in range(4):
        try:
            response = requests.get(
                "https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node",
                headers={"Authorization": "Bearer {token}".format(token=get_tenant_access_token(app_id, app_secret))},
                params={"token": wiki_token},
                timeout=30,
            )
            response.raise_for_status()
            result = response.json()
            if result.get("code") != 0:
                raise FeishuAPIError("解析 wiki token 失败: {msg}".format(msg=result.get("msg")))
            node = result.get("data", {}).get("node", {})
            if node.get("obj_type") != "bitable":
                raise FeishuAPIError("当前 wiki 节点不是 bitable: {typ}".format(typ=node.get("obj_type")))
            obj_token = node.get("obj_token")
            if not obj_token:
                raise FeishuAPIError("wiki 节点未返回底层 bitable obj_token")
            return str(obj_token)
        except (requests.RequestException, ValueError, FeishuAPIError) as exc:
            last_error = exc
            if attempt < 3:
                time.sleep(2 ** attempt)
    raise FeishuAPIError("解析 wiki token 最终失败: {err}".format(err=last_error))


class _FeishuAuthMixin(object):
    def __init__(self, app_id: Optional[str] = None, app_secret: Optional[str] = None):
        self._app_id = app_id
        self._app_secret = app_secret
        self._access_token = None  # type: Optional[str]
        self._expires_at = 0.0

    def _get_access_token(self) -> str:
        if self._access_token and time.time() < self._expires_at:
            return self._access_token
        self._access_token = get_tenant_access_token(self._app_id, self._app_secret)
        self._expires_at = time.time() + 6900
        return self._access_token

    def _headers(self, include_json: bool = True) -> Dict[str, str]:
        headers = {"Authorization": "Bearer {token}".format(token=self._get_access_token())}
        if include_json:
            headers["Content-Type"] = "application/json"
        return headers

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


class FeishuBitableClient(_FeishuAuthMixin):
    def __init__(
        self,
        app_token: str,
        table_id: str,
        app_id: Optional[str] = None,
        app_secret: Optional[str] = None,
    ):
        _FeishuAuthMixin.__init__(self, app_id=app_id, app_secret=app_secret)
        self.app_token = app_token
        self.table_id = table_id

    def list_records(self, page_size: int = 100, limit: Optional[int] = None) -> List[TableRecord]:
        url = "https://open.feishu.cn/open-apis/bitable/v1/apps/{app}/tables/{table}/records".format(
            app=self.app_token,
            table=self.table_id,
        )
        records = []  # type: List[TableRecord]
        page_token = None
        has_more = True
        while has_more:
            params = {"page_size": min(max(page_size, 1), 500)}
            if page_token:
                params["page_token"] = page_token
            response = self._request("GET", url, headers=self._headers(), params=params)
            result = response.json()
            if result.get("code") != 0:
                raise FeishuAPIError("读取飞书记录失败: {msg}".format(msg=result.get("msg")))
            data = result.get("data", {})
            for item in data.get("items", []):
                records.append(TableRecord(record_id=item["record_id"], fields=item.get("fields", {})))
                if limit is not None and len(records) >= limit:
                    return records
            has_more = bool(data.get("has_more"))
            page_token = data.get("page_token")
        return records

    def batch_create_records(self, records: List[Dict[str, Any]]) -> None:
        if not records:
            return
        url = "https://open.feishu.cn/open-apis/bitable/v1/apps/{app}/tables/{table}/records/batch_create".format(
            app=self.app_token,
            table=self.table_id,
        )
        response = self._request("POST", url, headers=self._headers(), json={"records": records})
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError("批量创建飞书记录失败: {msg}".format(msg=result.get("msg")))

    def batch_update_records(self, records: List[Dict[str, Any]]) -> None:
        if not records:
            return
        url = "https://open.feishu.cn/open-apis/bitable/v1/apps/{app}/tables/{table}/records/batch_update".format(
            app=self.app_token,
            table=self.table_id,
        )
        response = self._request("POST", url, headers=self._headers(), json={"records": records})
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError("批量更新飞书记录失败: {msg}".format(msg=result.get("msg")))

    def update_record_fields(self, record_id: str, fields: Dict[str, Any]) -> None:
        if not fields:
            return
        url = "https://open.feishu.cn/open-apis/bitable/v1/apps/{app}/tables/{table}/records/{record}".format(
            app=self.app_token,
            table=self.table_id,
            record=record_id,
        )
        response = self._request("PUT", url, headers=self._headers(), json={"fields": fields})
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError("更新飞书记录失败: {msg}".format(msg=result.get("msg")))

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
            raise FeishuAPIError("获取附件临时链接失败: {msg}".format(msg=result.get("msg")))
        items = result.get("data", {}).get("tmp_download_urls", [])
        if not items:
            raise FeishuAPIError("飞书未返回附件临时链接")
        return str(items[0]["tmp_download_url"])

    def download_attachment_bytes(self, attachment: Dict[str, Any]) -> Tuple[bytes, str, str, int]:
        file_token = str(attachment.get("file_token", "")).strip()
        if not file_token:
            raise FeishuAPIError("附件缺少 file_token")
        tmp_url = self.get_tmp_download_url(file_token)
        response = requests.get(tmp_url, timeout=60)
        response.raise_for_status()
        content = response.content
        file_name = str(attachment.get("name") or "{token}.bin".format(token=file_token))
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
        response = self._request(
            "POST",
            upload_url,
            headers=self._headers(include_json=False),
            data=payload,
            files=files,
        )
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError("上传飞书附件失败: {msg}".format(msg=result.get("msg")))
        file_token = result.get("data", {}).get("file_token")
        if not file_token:
            raise FeishuAPIError("飞书上传附件未返回 file_token")
        return {
            "file_token": str(file_token),
            "name": file_name,
            "size": int(size or len(content)),
            "type": content_type,
        }


class FeishuIMClient(_FeishuAuthMixin):
    BASE_URL = "https://open.feishu.cn/open-apis"

    def __init__(self, app_id: Optional[str] = None, app_secret: Optional[str] = None):
        _FeishuAuthMixin.__init__(self, app_id=app_id, app_secret=app_secret)

    @staticmethod
    def _resolve_file_type(file_name: str) -> str:
        suffix = Path(file_name).suffix.lower()
        if suffix in {".pdf"}:
            return "pdf"
        if suffix in {".doc", ".docx"}:
            return "doc"
        if suffix in {".xls", ".xlsx"}:
            return "xls"
        if suffix in {".ppt", ".pptx"}:
            return "ppt"
        if suffix in {".mp4", ".mov", ".avi", ".m4v"}:
            return "video"
        return "stream"

    def send_chat_message(self, chat_id: str, msg_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = "{base}/im/v1/messages".format(base=self.BASE_URL)
        response = self._request(
            "POST",
            url,
            headers=self._headers(),
            params={"receive_id_type": "chat_id"},
            json={
                "receive_id": chat_id,
                "msg_type": msg_type,
                "content": json.dumps(payload, ensure_ascii=False),
                "uuid": str(uuid.uuid4()),
            },
        )
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError("发送飞书群消息失败: {msg}".format(msg=result.get("msg")))
        return result.get("data", {})

    def send_text(self, chat_id: str, text: str) -> Dict[str, Any]:
        return self.send_chat_message(chat_id, "text", {"text": text})

    def send_text_with_mention(
        self,
        chat_id: str,
        user_open_id: str,
        display_name: str,
        text: str,
    ) -> Dict[str, Any]:
        mention = '<at user_id="{user_id}">{name}</at>'.format(
            user_id=user_open_id,
            name=display_name,
        )
        payload = mention if not text else "{mention}\n{text}".format(mention=mention, text=text)
        return self.send_chat_message(chat_id, "text", {"text": payload})

    def upload_file(self, file_path: Path) -> str:
        with file_path.open("rb") as handle:
            data = {
                "file_type": self._resolve_file_type(file_path.name),
                "file_name": file_path.name,
            }
            files = {
                "file": (file_path.name, BytesIO(handle.read()), mimetypes.guess_type(file_path.name)[0] or "application/octet-stream")
            }
        response = self._request(
            "POST",
            "{base}/im/v1/files".format(base=self.BASE_URL),
            headers=self._headers(include_json=False),
            data=data,
            files=files,
        )
        result = response.json()
        if result.get("code") != 0:
            raise FeishuAPIError("上传飞书群文件失败: {msg}".format(msg=result.get("msg")))
        file_key = result.get("data", {}).get("file_key")
        if not file_key:
            raise FeishuAPIError("飞书上传文件未返回 file_key")
        return str(file_key)

    def send_file(self, chat_id: str, file_path: Path) -> Dict[str, Any]:
        file_key = self.upload_file(file_path)
        return self.send_chat_message(chat_id, "file", {"file_key": file_key})

    def list_chat_messages(
        self,
        chat_id: str,
        page_size: int = 50,
        since_timestamp: Optional[float] = None,
        max_pages: int = 5,
    ) -> List[Dict[str, Any]]:
        url = "{base}/im/v1/messages".format(base=self.BASE_URL)
        items = []  # type: List[Dict[str, Any]]
        page_token = None
        local_since_ms = int(since_timestamp * 1000) if since_timestamp is not None else None
        use_remote_start_time = since_timestamp is not None
        for _ in range(max_pages):
            params = {
                "container_id_type": "chat",
                "container_id": chat_id,
                "sort_type": "ByCreateTimeDesc",
                "page_size": min(max(page_size, 1), 200),
            }
            if use_remote_start_time and local_since_ms is not None:
                params["start_time"] = local_since_ms
            if page_token:
                params["page_token"] = page_token
            try:
                response = self._request("GET", url, headers=self._headers(), params=params)
            except FeishuAPIError:
                if not use_remote_start_time:
                    raise
                use_remote_start_time = False
                params.pop("start_time", None)
                response = self._request("GET", url, headers=self._headers(), params=params)
            result = response.json()
            if result.get("code") != 0:
                raise FeishuAPIError("读取飞书群消息失败: {msg}".format(msg=result.get("msg")))
            data = result.get("data", {})
            current_items = data.get("items", [])
            if local_since_ms is not None:
                filtered = []
                for item in current_items:
                    create_time = str(item.get("create_time") or "").strip()
                    try:
                        create_time_ms = int(create_time)
                    except ValueError:
                        create_time_ms = 0
                    if create_time_ms >= local_since_ms:
                        filtered.append(item)
                current_items = filtered
            items.extend(current_items)
            if not data.get("has_more"):
                break
            page_token = data.get("page_token")
            if not page_token:
                break
        return items
