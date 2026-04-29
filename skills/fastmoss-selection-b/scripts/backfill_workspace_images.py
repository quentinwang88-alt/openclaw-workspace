#!/usr/bin/env python3
"""Backfill Feishu workspace image attachments from product image URLs."""

from __future__ import annotations

import argparse
import mimetypes
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit

import requests


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.config import get_settings  # noqa: E402
from app.db import Database  # noqa: E402
from app.feishu import FeishuBitableClient, parse_feishu_bitable_url, resolve_wiki_bitable_app_token  # noqa: E402
from app.pipeline import WORKSPACE_FIELDS  # noqa: E402
from app.utils import coerce_attachment_list, safe_text  # noqa: E402


def _build_bitable_client(feishu_url: str) -> FeishuBitableClient:
    info = parse_feishu_bitable_url(feishu_url)
    if not info:
        raise ValueError("无法解析飞书 URL: {url}".format(url=feishu_url))
    app_token = info.app_token
    if "/wiki/" in info.original_url:
        app_token = resolve_wiki_bitable_app_token(info.app_token)
    return FeishuBitableClient(app_token=app_token, table_id=info.table_id)


def _download_image_bytes(image_url: str, product_id: str) -> Tuple[bytes, str, str, int]:
    response = requests.get(
        image_url,
        timeout=30,
        headers={"User-Agent": "Mozilla/5.0"},
    )
    response.raise_for_status()
    content = response.content
    content_type = str(response.headers.get("content-type") or "application/octet-stream").split(";")[0].strip()
    suffix = Path(urlsplit(image_url).path).suffix
    if not suffix:
        suffix = mimetypes.guess_extension(content_type) or ".bin"
    file_name = "{product_id}{suffix}".format(product_id=product_id or "product_image", suffix=suffix)
    return content, file_name, content_type, len(content)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill workspace image attachments")
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--force", action="store_true", help="即使已有附件也重新上传")
    args = parser.parse_args()

    settings = get_settings()
    if not settings.workspace_table_url:
        raise RuntimeError("缺少 FASTMOSS_B_WORKSPACE_TABLE_URL 配置")

    db = Database(settings.database_url)
    workspace_client = _build_bitable_client(settings.workspace_table_url)
    selection_rows = db.list_selection_records(args.batch_id)
    selection_map = {safe_text(row.get("work_id")): row for row in selection_rows}
    workspace_rows = workspace_client.list_records(page_size=500)

    processed = 0
    skipped = 0
    failed = 0
    upload_cache: Dict[str, Dict[str, Any]] = {}

    for record in workspace_rows:
        fields = record.fields or {}
        work_id = safe_text(fields.get(WORKSPACE_FIELDS["work_id"]))
        if not work_id:
            continue
        if safe_text(fields.get(WORKSPACE_FIELDS["batch_id"])) != args.batch_id:
            continue
        if args.limit is not None and processed >= args.limit:
            break
        existing_attachments = coerce_attachment_list(fields.get(WORKSPACE_FIELDS["product_image_attachment"]))
        if existing_attachments and not args.force:
            skipped += 1
            continue
        selection_row = selection_map.get(work_id)
        if not selection_row:
            failed += 1
            print("fail missing_selection_row {work_id}".format(work_id=work_id), flush=True)
            continue
        image_url = safe_text(selection_row.get("product_image"))
        if not image_url:
            skipped += 1
            print("skip no_image_url {work_id}".format(work_id=work_id), flush=True)
            continue
        try:
            cached = upload_cache.get(image_url)
            if cached:
                uploaded = dict(cached)
            else:
                content, file_name, content_type, size = _download_image_bytes(
                    image_url,
                    safe_text(selection_row.get("product_id")),
                )
                uploaded = workspace_client.upload_attachment(
                    content=content,
                    file_name=file_name,
                    content_type=content_type,
                    size=size,
                )
                upload_cache[image_url] = dict(uploaded)
            workspace_client.update_record_fields(
                record.record_id,
                {
                    WORKSPACE_FIELDS["product_image_attachment"]: [uploaded],
                },
            )
            processed += 1
            print("ok {work_id}".format(work_id=work_id), flush=True)
        except Exception as exc:
            failed += 1
            print("fail {work_id} {error}".format(work_id=work_id, error=exc), flush=True)

    print(
        "summary processed={processed} skipped={skipped} failed={failed}".format(
            processed=processed,
            skipped=skipped,
            failed=failed,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
