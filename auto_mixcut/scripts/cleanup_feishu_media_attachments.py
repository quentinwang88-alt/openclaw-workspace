#!/usr/bin/env python3
"""Clear Feishu media attachments once the file already has an OSS home."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = Path("/Users/likeu3/.openclaw/workspace")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(WORKSPACE / "skills" / "script-run-manager-sync"))

from auto_mixcut.adapters.feishu import AutoMixcutFeishuClient  # noqa: E402
from core.bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # type: ignore  # noqa: E402
from core.feishu_url_parser import parse_feishu_bitable_url  # type: ignore  # noqa: E402


PROMPT_PACKAGE_URL = "https://gcngopvfvo0q.feishu.cn/wiki/PufTwQtBUizcPXk4fpycNwoOnKb?table=tblQb6SsNgYSYY8Q&view=vewIYG2wPN"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scope", choices=["asset_upload", "prompt_package", "all"], default="all")
    parser.add_argument("--product-id", default="")
    parser.add_argument("--prompt-package-url", default=PROMPT_PACKAGE_URL)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    result: Dict[str, Any] = {}
    if args.scope in {"asset_upload", "all"}:
        result["asset_upload"] = cleanup_asset_uploads(product_id=args.product_id, dry_run=args.dry_run, limit=args.limit or None)
    if args.scope in {"prompt_package", "all"}:
        result["prompt_package"] = cleanup_prompt_packages(args.prompt_package_url, product_id=args.product_id, dry_run=args.dry_run, limit=args.limit or None)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 1 if any(section.get("failed") for section in result.values() if isinstance(section, dict)) else 0


def cleanup_asset_uploads(product_id: str = "", dry_run: bool = False, limit: int | None = None) -> Dict[str, Any]:
    client = AutoMixcutFeishuClient("商品素材上传表")
    records = client.list_records(limit=limit)
    cleaned: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    failed: List[Dict[str, Any]] = []
    for record in records:
        fields = record.fields or {}
        record_product_id = _text(fields.get("商品ID"))
        if product_id and record_product_id != product_id:
            continue
        files = _attachments(fields.get("素材文件"))
        if not files:
            skipped.append({"record_id": record.record_id, "reason": "no_attachment"})
            continue
        if not _text(fields.get("OSS路径")) or not _text(fields.get("素材ID")):
            skipped.append({"record_id": record.record_id, "reason": "oss_not_ready", "files": len(files)})
            continue
        payload = {"素材文件": [], "是否已清理飞书附件": "是"}
        if dry_run:
            cleaned.append({"record_id": record.record_id, "product_id": record_product_id, "files": len(files), "dry_run": True})
            continue
        try:
            client.update_record(record.record_id, payload)
            cleaned.append({"record_id": record.record_id, "product_id": record_product_id, "files": len(files)})
        except Exception as exc:
            failed.append({"record_id": record.record_id, "error": str(exc)})
    return {"checked": len(records), "cleaned": cleaned, "skipped": skipped, "failed": failed}


def cleanup_prompt_packages(feishu_url: str, product_id: str = "", dry_run: bool = False, limit: int | None = None) -> Dict[str, Any]:
    client = _prompt_package_client(feishu_url)
    records = client.list_records(page_size=100, limit=limit)
    cleaned: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    failed: List[Dict[str, Any]] = []
    for record in records:
        fields = record.fields or {}
        record_product_id = _text(fields.get("商品ID"))
        if product_id and record_product_id != product_id:
            continue
        files = _attachments(fields.get("生成视频回流"))
        if not files:
            skipped.append({"record_id": record.record_id, "reason": "no_attachment"})
            continue
        if not _url(fields.get("预览地址")):
            skipped.append({"record_id": record.record_id, "reason": "missing_preview_url", "files": len(files)})
            continue
        if dry_run:
            cleaned.append({"record_id": record.record_id, "product_id": record_product_id, "files": len(files), "dry_run": True})
            continue
        try:
            client.update_record_fields(record.record_id, {"生成视频回流": []})
            cleaned.append({"record_id": record.record_id, "product_id": record_product_id, "files": len(files)})
        except Exception as exc:
            failed.append({"record_id": record.record_id, "error": str(exc)})
    return {"checked": len(records), "cleaned": cleaned, "skipped": skipped, "failed": failed}


def _prompt_package_client(feishu_url: str) -> FeishuBitableClient:
    info = parse_feishu_bitable_url(feishu_url)
    if not info:
        raise RuntimeError(f"无法解析飞书 URL: {feishu_url}")
    app_token = resolve_wiki_bitable_app_token(info.app_token) if "/wiki/" in info.original_url else info.app_token
    return FeishuBitableClient(app_token=app_token, table_id=info.table_id)


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
            if isinstance(item, str) and item.strip().startswith(("http://", "https://")):
                return item.strip()
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
        for key in ("text", "name", "value", "link", "url"):
            item = value.get(key)
            if isinstance(item, str) and item.strip():
                return item.strip()
        return ""
    if isinstance(value, list):
        return " / ".join(item for item in (_text(item) for item in value) if item)
    return str(value).strip()


if __name__ == "__main__":
    raise SystemExit(main())
