#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from auto_mixcut.adapters.feishu import AutoMixcutFeishuClient  # noqa: E402
from auto_mixcut.core.bootstrap import build_context  # noqa: E402
from auto_mixcut.skills.oss_storage_skill import OSSStorageSkill  # noqa: E402


SOURCE_TYPE_MAP = {
    "供应商素材": "supplier",
    "自有拍摄": "self_shot",
    "授权达人素材": "creator_authorized",
    "AI生成素材": "ai_generated",
    "竞品素材": "competitor",
    "抖音/搬运素材": "douyin_repost",
    "程序自动抓取素材": "auto_crawled",
    "通用场景素材": "generic_scene",
    "其他": "other",
}
PRODUCT_BINDING_MAP = {
    "当前商品同款": "exact_sku",
    "同款/高度相似款": "same_style",
    "同类目参考": "category_reference",
    "通用场景": "generic_scene",
    "不确定": "unknown",
}
SOURCE_TRUST = {
    "supplier": "high",
    "self_shot": "high",
    "creator_authorized": "high",
    "ai_generated": "medium",
    "generic_scene": "medium",
    "competitor": "low",
    "douyin_repost": "low",
    "auto_crawled": "low",
    "other": "low",
}


def text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ("text", "name", "link", "url"):
            item = value.get(key)
            if isinstance(item, str) and item.strip():
                return item.strip()
        return ""
    if isinstance(value, list):
        return " / ".join(item for item in (text(v) for v in value) if item)
    return str(value).strip()


def attachments(value: Any) -> List[Dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict) and item.get("file_token")]
    if isinstance(value, dict) and value.get("file_token"):
        return [value]
    return []


def process_record(ctx, client: AutoMixcutFeishuClient, record: Any, dry_run: bool = False) -> Dict[str, Any]:
    fields = record.fields or {}
    product_id = text(fields.get("商品ID"))
    source_cn = text(fields.get("素材来源 source_type"))
    binding_cn = text(fields.get("商品绑定类型 product_binding_type"))
    files = attachments(fields.get("素材文件"))
    if not product_id or not source_cn or not binding_cn or not files:
        return {"record_id": record.record_id, "status": "skipped", "reason": "missing required fields"}
    source_type = SOURCE_TYPE_MAP.get(source_cn, source_cn)
    binding_type = PRODUCT_BINDING_MAP.get(binding_cn, binding_cn)
    trust = SOURCE_TRUST.get(source_type, "low")
    product = ctx.repo.get("products", "product_id", product_id)
    if not product or product.get("anchor_status") != "confirmed":
        if not dry_run:
            client.update_record(record.record_id, {"上传状态": "pending_upload", "处理状态": "anchor_missing", "处理失败原因": "商品锚点卡未确认", "最近处理时间": now_ms()})
        return {"record_id": record.record_id, "product_id": product_id, "status": "anchor_missing"}
    attachment = files[0]
    if dry_run:
        return {"record_id": record.record_id, "product_id": product_id, "status": "would_upload", "file": attachment.get("name")}
    client.update_record(record.record_id, {"上传状态": "file_received", "处理状态": "pending", "最近处理时间": now_ms()})
    content, file_name, content_type, size = client.download_attachment_bytes(attachment)
    temp_dir = ctx.settings.temp_root / "feishu_uploads" / product_id
    temp_dir.mkdir(parents=True, exist_ok=True)
    local_path = temp_dir / safe_name(file_name)
    local_path.write_bytes(content)
    client.update_record(record.record_id, {"上传状态": "uploading_to_oss", "最近处理时间": now_ms()})
    uploaded = OSSStorageSkill(ctx).upload_asset(
        product_id,
        str(local_path),
        source_type=source_type,
        source_trust_level=trust,
        product_binding_type=binding_type,
    )
    if not uploaded.success:
        err = uploaded.error.message if uploaded.error else "unknown error"
        client.update_record(record.record_id, {"上传状态": "upload_failed", "处理状态": "failed", "处理失败原因": err, "最近处理时间": now_ms()})
        return {"record_id": record.record_id, "product_id": product_id, "status": "failed", "error": err}
    asset_id = uploaded.data["asset_id"]
    oss_obj = uploaded.data["oss_object"]
    media_type = "image" if str(file_name).lower().endswith((".jpg", ".jpeg", ".png", ".webp")) else "video"
    client.update_record(
        record.record_id,
        {
            "上传状态": "oss_uploaded",
            "处理状态": "pending",
            "素材ID": asset_id,
            "OSS路径": oss_obj["object_key"],
            "文件类型": media_type,
            "文件大小": int(oss_obj.get("file_size") or size),
            "是否有水印": "pending",
            "最近处理时间": now_ms(),
        },
    )
    return {"record_id": record.record_id, "product_id": product_id, "status": "oss_uploaded", "asset_id": asset_id}


def safe_name(value: str) -> str:
    cleaned = "".join("_" if ch in '\\/:*?"<>|' else ch for ch in str(value or "asset.bin")).strip()
    return cleaned or "asset.bin"


def now_ms() -> int:
    return int(datetime.utcnow().timestamp() * 1000)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--product-id")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    ctx = build_context()
    client = AutoMixcutFeishuClient("商品素材上传表")
    records = client.list_records(limit=None)
    results = []
    for record in records:
        if args.product_id and text((record.fields or {}).get("商品ID")) != args.product_id:
            continue
        results.append(process_record(ctx, client, record, dry_run=args.dry_run))
    print(json.dumps({"results": results}, ensure_ascii=False, indent=2))
    return 1 if any(item.get("status") == "failed" for item in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
