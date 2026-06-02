#!/usr/bin/env python3
"""Clear Prompt Package Feishu attachments after human review.

The generated media should live in OSS. Feishu keeps attachments only while a
record is waiting for review; once a reviewer marks it usable or rejected, this
script clears the Feishu attachment field and leaves the OSS preview URL.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List


WORKSPACE = Path("/Users/likeu3/.openclaw/workspace")
sys.path.insert(0, str(WORKSPACE / "skills" / "script-run-manager-sync"))

from core.bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # type: ignore  # noqa: E402
from core.feishu_url_parser import parse_feishu_bitable_url  # type: ignore  # noqa: E402


ATTACHMENT_FIELD = "生成视频回流"
PREVIEW_FIELD = "预览地址"
REVIEW_FIELD = "人工审核结论"
PACKAGE_STATUS_FIELD = "包状态"
REVIEWED_VALUES = {"可使用", "废弃"}


def resolve_client(feishu_url: str) -> FeishuBitableClient:
    info = parse_feishu_bitable_url(feishu_url)
    if not info:
        raise RuntimeError(f"无法解析飞书 URL: {feishu_url}")
    app_token = info.app_token
    if "/wiki/" in info.original_url:
        app_token = resolve_wiki_bitable_app_token(info.app_token)
    return FeishuBitableClient(app_token=app_token, table_id=info.table_id)


def cleanup_reviewed_attachments(feishu_url: str, dry_run: bool = False, limit: int | None = None) -> Dict[str, Any]:
    client = resolve_client(feishu_url)
    records = client.list_records(page_size=100, limit=limit)
    cleaned: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    failed: List[Dict[str, Any]] = []

    for record in records:
        fields = record.fields or {}
        review_value = _text(fields.get(REVIEW_FIELD))
        if review_value not in REVIEWED_VALUES:
            skipped.append({"record_id": record.record_id, "reason": "not_reviewed", "review": review_value})
            continue

        files = _attachments(fields.get(ATTACHMENT_FIELD))
        if not files:
            skipped.append({"record_id": record.record_id, "reason": "no_attachment", "review": review_value})
            continue

        preview_url = _url(fields.get(PREVIEW_FIELD))
        if not preview_url:
            skipped.append({"record_id": record.record_id, "reason": "missing_preview_url", "review": review_value, "files": len(files)})
            continue

        if dry_run:
            cleaned.append({"record_id": record.record_id, "review": review_value, "files": len(files), "preview_url": preview_url, "dry_run": True})
            continue

        try:
            client.update_record_fields(record.record_id, {ATTACHMENT_FIELD: []})
            status_update = _status_after_review(review_value, _text(fields.get(PACKAGE_STATUS_FIELD)))
            if status_update:
                client.update_record_fields(record.record_id, {PACKAGE_STATUS_FIELD: status_update})
            cleaned.append({"record_id": record.record_id, "review": review_value, "files": len(files), "preview_url": preview_url})
        except Exception as exc:
            failed.append({"record_id": record.record_id, "review": review_value, "error": str(exc)})

    return {
        "checked": len(records),
        "cleaned": cleaned,
        "skipped": skipped,
        "failed": failed,
    }


def _status_after_review(review_value: str, current_status: str) -> str:
    if current_status in {"质检通过", "质检废弃"}:
        return ""
    if review_value == "可使用":
        return "质检通过"
    if review_value == "废弃":
        return "质检废弃"
    return ""


def _attachments(value: Any) -> List[Dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict) and item.get("file_token")]
    if isinstance(value, dict) and value.get("file_token"):
        return [value]
    if isinstance(value, str) and value.strip().startswith(("[", "{")):
        try:
            return _attachments(json.loads(value))
        except json.JSONDecodeError:
            return []
    return []


def _url(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ("link", "url", "text", "value"):
            item = value.get(key)
            if isinstance(item, str) and item.strip().startswith(("http://", "https://", "file://")):
                return item.strip()
        return ""
    if isinstance(value, list):
        for item in value:
            parsed = _url(item)
            if parsed:
                return parsed
    return ""


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ("text", "name", "value"):
            item = value.get(key)
            if isinstance(item, str) and item.strip():
                return item.strip()
        return ""
    if isinstance(value, list):
        return ",".join(item for item in (_text(item) for item in value) if item).strip()
    return str(value).strip()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--table-url", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    result = cleanup_reviewed_attachments(args.table_url, dry_run=args.dry_run, limit=args.limit)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 1 if result["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
