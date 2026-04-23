#!/usr/bin/env python3
"""批量把飞书多维表格中的图片 URL / 页面链接回填到附件字段。"""

from __future__ import annotations

import argparse
import json
import mimetypes
import re
import time
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, unquote, urljoin, urlparse

import requests


DEFAULT_SOURCE_FIELDS = [
    "商品图片",
    "TikTok商品落地页地址",
    "FastMoss商品详情页地址",
]
DEFAULT_TARGET_FIELD = "图片"
DEFAULT_BATCH_SIZE = 10
DEFAULT_TIMEOUT = 30
DEFAULT_SLEEP = 0.2
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


class FeishuAPIError(Exception):
    """飞书 API 异常。"""


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


@dataclass
class TableRecord:
    record_id: str
    fields: Dict[str, Any]


class FeishuBitableClient:
    def __init__(self, app_token: str, table_id: str):
        self.app_token = app_token
        self.table_id = table_id
        self.access_token: Optional[str] = None
        self.token_expires_at: float = 0

    def _load_openclaw_config(self) -> Dict[str, Any]:
        config_file = Path.home() / ".openclaw" / "openclaw.json"
        with open(config_file, "r", encoding="utf-8") as handle:
            return json.load(handle)

    def _get_access_token(self) -> str:
        if self.access_token and time.time() < self.token_expires_at:
            return self.access_token

        config = self._load_openclaw_config()
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
                self.token_expires_at = time.time() + config.get("feishu", {}).get("tokenExpire", 7200) - 300
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

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        last_error: Optional[Exception] = None
        for attempt in range(3):
            try:
                response = requests.request(method, url, timeout=DEFAULT_TIMEOUT, **kwargs)
                return response
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
            field_name = item.get("field_name")
            if not field_name:
                continue
            fields.append(
                TableField(
                    field_id=item["field_id"],
                    field_name=field_name,
                    field_type=item["type"],
                )
            )
        return fields

    def list_records(
        self,
        page_size: int = 100,
        limit: Optional[int] = None,
        view_id: Optional[str] = None,
    ) -> List[TableRecord]:
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records"
        page_token = None
        has_more = True
        records: List[TableRecord] = []

        while has_more:
            params: Dict[str, Any] = {"page_size": min(max(page_size, 1), 500)}
            if view_id:
                params["view_id"] = view_id
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


def extract_urls(value: Any) -> List[str]:
    urls: List[str] = []

    def visit(cell: Any) -> None:
        if cell is None:
            return
        if isinstance(cell, str):
            raw = cell.strip()
            if raw.startswith(("http://", "https://")):
                urls.append(raw)
            return
        if isinstance(cell, dict):
            for key in ("link", "url", "text"):
                raw = cell.get(key)
                if isinstance(raw, str) and raw.strip().startswith(("http://", "https://")):
                    urls.append(raw.strip())
            return
        if isinstance(cell, list):
            for item in cell:
                visit(item)

    visit(value)

    deduped: List[str] = []
    seen = set()
    for url in urls:
        if url not in seen:
            deduped.append(url)
            seen.add(url)
    return deduped


def has_attachment_value(value: Any) -> bool:
    if isinstance(value, list):
        return any(isinstance(item, dict) and item.get("file_token") for item in value)
    if isinstance(value, dict):
        return bool(value.get("file_token"))
    return False


def is_probable_image_url(url: str) -> bool:
    lowered = url.lower()
    return any(token in lowered for token in (".jpg", ".jpeg", ".png", ".webp", ".gif", "image", "img", "tt_product"))


def sanitize_filename(name: str) -> str:
    cleaned = re.sub(r"[\\/:*?\"<>|]", "_", name).strip()
    cleaned = cleaned[:120].strip("._")
    return cleaned or "image"


def content_type_to_extension(content_type: str) -> str:
    if not content_type:
        return ""
    main = content_type.split(";", 1)[0].strip().lower()
    return {
        "image/jpeg": ".jpg",
        "image/jpg": ".jpg",
        "image/png": ".png",
        "image/webp": ".webp",
        "image/gif": ".gif",
        "image/bmp": ".bmp",
        "image/heic": ".heic",
    }.get(main, mimetypes.guess_extension(main) or "")


def guess_filename(url: str, content_type: str, record_id: str) -> str:
    parsed = urlparse(url)
    name = Path(unquote(parsed.path)).name
    if not name or "." not in name:
        name = f"{record_id}{content_type_to_extension(content_type) or '.jpg'}"
    suffix = Path(name).suffix
    if not suffix:
        name = f"{name}{content_type_to_extension(content_type) or '.jpg'}"
    return sanitize_filename(name)


def extract_image_url_from_html(html: str, base_url: str) -> Optional[str]:
    patterns = [
        r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']',
        r'<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+name=["\']twitter:image["\']',
        r'"cover"\s*:\s*"(https?:\\/\\/[^\"]+)"',
        r'"image"\s*:\s*"(https?:\\/\\/[^\"]+)"',
    ]
    for pattern in patterns:
        match = re.search(pattern, html, flags=re.IGNORECASE)
        if match:
            candidate = match.group(1).replace("\\/", "/")
            return urljoin(base_url, candidate)
    return None


def fetch_binary(session: requests.Session, url: str) -> Tuple[bytes, str, str]:
    response = session.get(url, timeout=DEFAULT_TIMEOUT, allow_redirects=True, stream=True)
    response.raise_for_status()
    content_type = (response.headers.get("Content-Type") or "").split(";", 1)[0].strip().lower()

    if content_type.startswith("image/"):
        content = response.content
        if not content:
            raise ValueError("图片响应为空")
        return content, content_type, response.url

    text = response.text
    image_url = extract_image_url_from_html(text, response.url)
    if not image_url:
        raise ValueError(f"未从页面提取到图片地址: {url}")

    image_response = session.get(image_url, timeout=DEFAULT_TIMEOUT, allow_redirects=True)
    image_response.raise_for_status()
    image_content_type = (image_response.headers.get("Content-Type") or "").split(";", 1)[0].strip().lower()
    if not image_content_type.startswith("image/"):
        raise ValueError(f"提取到的地址不是图片: {image_url}")
    content = image_response.content
    if not content:
        raise ValueError("提取到的图片内容为空")
    return content, image_content_type, image_response.url


def iter_candidate_sources(record: TableRecord, source_fields: Sequence[str]) -> Iterable[Tuple[str, str]]:
    for field_name in source_fields:
        urls = extract_urls(record.fields.get(field_name))
        for url in urls:
            yield field_name, url


def flush_updates(client: FeishuBitableClient, pending_updates: List[Dict[str, Any]], dry_run: bool) -> int:
    if not pending_updates:
        return 0
    count = len(pending_updates)
    if not dry_run:
        client.batch_update_records(pending_updates)
    pending_updates.clear()
    return count


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", help="飞书多维表格链接（支持 wiki/base）")
    parser.add_argument("--app-token", help="飞书多维表格 app_token")
    parser.add_argument("--table-id", help="飞书多维表格 table_id")
    parser.add_argument("--view-id", help="只处理指定视图；默认优先使用链接中的 view 参数")
    parser.add_argument(
        "--source-field",
        action="append",
        dest="source_fields",
        help="源字段名，可重复传入。默认依次尝试 商品图片 / TikTok商品落地页地址 / FastMoss商品详情页地址",
    )
    parser.add_argument("--target-field", default=DEFAULT_TARGET_FIELD, help=f"目标附件字段，默认 {DEFAULT_TARGET_FIELD}")
    parser.add_argument("--limit", type=int, help="最多处理多少条记录")
    parser.add_argument("--record-id", action="append", help="只处理指定 record_id，可重复传入")
    parser.add_argument("--overwrite", action="store_true", help="覆盖已有附件字段。默认只补空白")
    parser.add_argument("--dry-run", action="store_true", help="只输出计划，不写飞书")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help=f"批量回写条数，默认 {DEFAULT_BATCH_SIZE}")
    parser.add_argument("--sleep", type=float, default=DEFAULT_SLEEP, help=f"每条记录之间的等待秒数，默认 {DEFAULT_SLEEP}")
    return parser


def resolve_table_args(args: argparse.Namespace) -> Tuple[str, str, Optional[str]]:
    if args.app_token and args.table_id:
        return args.app_token, args.table_id, args.view_id

    if not args.url:
        raise ValueError("请提供 --url，或同时提供 --app-token 与 --table-id")

    info = parse_feishu_bitable_url(args.url)
    if not info:
        raise ValueError(f"无法解析飞书表格链接: {args.url}")

    if not info.is_wiki:
        return info.app_token, info.table_id, args.view_id or info.view_id

    bootstrap_client = FeishuBitableClient(app_token="bootstrap", table_id=info.table_id)
    resolved_app_token = resolve_wiki_bitable_app_token(bootstrap_client, info.app_token)
    return resolved_app_token, info.table_id, args.view_id or info.view_id


def main() -> int:
    args = build_arg_parser().parse_args()

    source_fields = args.source_fields or list(DEFAULT_SOURCE_FIELDS)
    batch_size = max(1, min(args.batch_size, 100))
    app_token, table_id, view_id = resolve_table_args(args)
    client = FeishuBitableClient(app_token=app_token, table_id=table_id)

    fields = {field.field_name: field for field in client.list_fields()}
    missing_source_fields = [name for name in source_fields if name not in fields]
    if missing_source_fields:
        raise ValueError(f"源字段不存在: {', '.join(missing_source_fields)}")
    if args.target_field not in fields:
        raise ValueError(f"目标字段不存在: {args.target_field}")
    if fields[args.target_field].field_type != 17:
        raise ValueError(f"目标字段 {args.target_field} 不是附件字段(type=17)")

    records = client.list_records(limit=args.limit, view_id=view_id)
    record_filter = set(args.record_id or [])
    if record_filter:
        records = [record for record in records if record.record_id in record_filter]

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    summary = {
        "total": len(records),
        "queued": 0,
        "updated": 0,
        "skipped_target_filled": 0,
        "skipped_no_source": 0,
        "failed": 0,
    }
    pending_updates: List[Dict[str, Any]] = []

    print(f"📋 表格: app_token={app_token} table_id={table_id}")
    if view_id:
        print(f"👁️  视图: {view_id}")
    print(f"📥 源字段优先级: {' > '.join(source_fields)}")
    print(f"📤 目标字段: {args.target_field}")
    print(f"🧪 模式: {'dry-run' if args.dry_run else 'write'}")
    print(f"🔢 待扫描记录数: {len(records)}")

    for index, record in enumerate(records, start=1):
        target_value = record.fields.get(args.target_field)
        if has_attachment_value(target_value) and not args.overwrite:
            summary["skipped_target_filled"] += 1
            continue

        matched = False
        last_error: Optional[str] = None

        for source_field, source_url in iter_candidate_sources(record, source_fields):
            if not source_url:
                continue
            try:
                print(f"[{index}/{len(records)}] {record.record_id} <- {source_field}: {source_url}")
                if not is_probable_image_url(source_url):
                    print("    ℹ️  不是直链图片，尝试从页面提取 og:image")

                content, content_type, final_url = fetch_binary(session, source_url)
                file_name = guess_filename(final_url, content_type, record.record_id)
                summary["queued"] += 1

                if args.dry_run:
                    print(f"    ✅ dry-run 命中，准备上传文件名: {file_name}")
                else:
                    attachment = client.upload_attachment(content, file_name, content_type, len(content))
                    pending_updates.append(
                        {
                            "record_id": record.record_id,
                            "fields": {
                                args.target_field: [attachment],
                            },
                        }
                    )
                    print(f"    ✅ 已上传，待回写: {attachment['name']}")
                    if len(pending_updates) >= batch_size:
                        flushed = flush_updates(client, pending_updates, dry_run=False)
                        summary["updated"] += flushed
                        print(f"    🚚 已批量回写 {flushed} 条")
                matched = True
                break
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
                print(f"    ⚠️  失败，尝试下一个来源: {last_error}")

        if not matched:
            if last_error:
                summary["failed"] += 1
            else:
                summary["skipped_no_source"] += 1
                print(f"[{index}/{len(records)}] {record.record_id} 没有可用来源")

        if args.sleep > 0:
            time.sleep(args.sleep)

    flushed = flush_updates(client, pending_updates, dry_run=args.dry_run)
    summary["updated"] += flushed

    print("\n===== SUMMARY =====")
    for key, value in summary.items():
        print(f"{key}: {value}")

    if args.dry_run:
        print("\nℹ️ dry-run 未写回飞书")

    return 0 if summary["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
