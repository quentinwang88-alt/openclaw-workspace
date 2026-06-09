#!/usr/bin/env python3
"""Import returned Prompt Package videos into OSS/RDS.

This consumes the Prompt Package workbench, not the generic material upload
table. It is intentionally narrow: only records with returned video attachments
and matching RDS prompt packages are imported.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = Path("/Users/likeu3/.openclaw/workspace")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(WORKSPACE / "skills" / "script-run-manager-sync"))

from auto_mixcut.core.bootstrap import build_context  # noqa: E402
from auto_mixcut.core.ids import new_id  # noqa: E402
from auto_mixcut.adapters.oss import AliyunOSS  # noqa: E402
from auto_mixcut.skills.segment_prompt_factory_skill import SegmentPromptFactorySkill  # noqa: E402
from core.bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # type: ignore  # noqa: E402
from core.feishu_url_parser import parse_feishu_bitable_url  # type: ignore  # noqa: E402


DEFAULT_URL = "https://gcngopvfvo0q.feishu.cn/wiki/PufTwQtBUizcPXk4fpycNwoOnKb?table=tblQb6SsNgYSYY8Q&view=vewIYG2wPN"

FIELD_PROMPT_ID = "提示词包ID"
FIELD_PRODUCT_ID = "商品ID"
FIELD_STATUS = "包状态"
FIELD_RESULT_SYNC = "结果回传状态"
FIELD_ATTACHMENT = "生成视频回流"
FIELD_VIDEO_FILE_NAME = "生成视频文件名"
FIELD_PREVIEW_URL = "预览地址"
FIELD_RESULT = "提单结果"

IMPORTED_STATUSES = {"已回流", "已导入", "质检中", "质检通过", "质检废弃"}
RESULT_SYNC_READY = {"uploaded", "downloaded", "已回流", "已上传"}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--product-id")
    parser.add_argument("--segment-prompt-id")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    ctx = build_context()
    client = resolve_client(args.url)
    records = client.list_records(page_size=500)
    results: List[Dict[str, Any]] = []

    for record in records:
        fields = record.fields or {}
        prompt_id = text(fields.get(FIELD_PROMPT_ID))
        product_id = text(fields.get(FIELD_PRODUCT_ID))
        if args.product_id and product_id != args.product_id:
            continue
        if args.segment_prompt_id and prompt_id != args.segment_prompt_id:
            continue
        if not prompt_id or not product_id:
            continue

        status = text(fields.get(FIELD_STATUS))
        result_sync = text(fields.get(FIELD_RESULT_SYNC))
        files = attachments(fields.get(FIELD_ATTACHMENT))
        if not files:
            continue
        if status not in IMPORTED_STATUSES and result_sync not in RESULT_SYNC_READY:
            continue

        result = import_record(ctx, client, record.record_id, fields, files[0], dry_run=args.dry_run, force=args.force)
        results.append(result)
        if args.limit and len(results) >= args.limit:
            break

    print(json.dumps({"count": len(results), "results": results}, ensure_ascii=False, indent=2, default=str))
    return 1 if any(item.get("status") == "failed" for item in results) else 0


def resolve_client(feishu_url: str) -> FeishuBitableClient:
    info = parse_feishu_bitable_url(feishu_url)
    if not info:
        raise RuntimeError(f"无法解析飞书 URL: {feishu_url}")
    app_token = resolve_wiki_bitable_app_token(info.app_token) if "/wiki/" in info.original_url else info.app_token
    return FeishuBitableClient(app_token=app_token, table_id=info.table_id)


def import_record(
    ctx: Any,
    client: FeishuBitableClient,
    record_id: str,
    fields: Dict[str, Any],
    attachment: Dict[str, Any],
    dry_run: bool = False,
    force: bool = False,
) -> Dict[str, Any]:
    prompt_id = text(fields.get(FIELD_PROMPT_ID))
    product_id = text(fields.get(FIELD_PRODUCT_ID))
    package = ctx.repo.get("segment_prompt_packages", "segment_prompt_id", prompt_id)
    if not package:
        return {"record_id": record_id, "segment_prompt_id": prompt_id, "status": "failed", "reason": "prompt_package_missing"}
    if package.get("generated_asset_id") and package.get("generated_segment_id") and not force:
        maybe_backfill_feishu(ctx, client, record_id, package)
        return {
            "record_id": record_id,
            "segment_prompt_id": prompt_id,
            "status": "skipped",
            "reason": "already_imported",
            "asset_id": package.get("generated_asset_id"),
            "segment_id": package.get("generated_segment_id"),
        }

    product = ctx.repo.get("products", "product_id", product_id) or {}
    if not product or product.get("anchor_status") != "confirmed":
        return {"record_id": record_id, "segment_prompt_id": prompt_id, "product_id": product_id, "status": "failed", "reason": "anchor_not_confirmed"}

    file_name = safe_name(text(attachment.get("name")) or text(fields.get(FIELD_VIDEO_FILE_NAME)) or f"{prompt_id}.mp4")
    if dry_run:
        return {"record_id": record_id, "segment_prompt_id": prompt_id, "product_id": product_id, "status": "would_import", "file_name": file_name}

    content, downloaded_name, content_type, size = client.download_attachment_bytes(attachment)
    if downloaded_name:
        file_name = safe_name(downloaded_name)
    local_path = ctx.settings.temp_root / "prompt_package_returns" / product_id / prompt_id / file_name
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_bytes(content)

    asset_id = new_id("ASSET")
    segment_id = new_id("SEG")
    market = str(product.get("market") or package.get("market") or "NA")
    category = str(product.get("category") or package.get("category") or "uncategorized")
    object_key = f"auto_mixcut/ai_generated/{market}/{category}/{product_id}/{prompt_id}/{asset_id}.mp4"
    uploaded = ctx.oss.upload(local_path, object_key)
    if not uploaded.success:
        return {"record_id": record_id, "segment_prompt_id": prompt_id, "product_id": product_id, "status": "failed", "reason": uploaded.to_dict()}

    oss_row = dict(uploaded.data, object_type="ai_generated_asset", mime_type=content_type or "video/mp4")
    oss_write = ctx.repo.upsert("oss_objects", "object_id", oss_row)
    if not oss_write.success:
        return {"record_id": record_id, "segment_prompt_id": prompt_id, "status": "failed", "reason": oss_write.to_dict()}

    prompt_json = package.get("prompt_package_json") or {}
    prompt = prompt_json.get("prompt") if isinstance(prompt_json, dict) else {}
    prompt_text = json.dumps(prompt, ensure_ascii=False) if isinstance(prompt, dict) else str(prompt or "")
    duration_ms = int(float(package.get("duration_sec") or 4) * 1000)
    now = datetime.utcnow().isoformat(timespec="seconds")

    asset_row = {
        "asset_id": asset_id,
        "product_id": product_id,
        "source_type": "ai_generated",
        "source_trust_level": "medium",
        "product_binding_type": "exact_sku",
        "media_type": "video",
        "original_oss_object_id": oss_row["object_id"],
        "file_status": "uploaded",
        "probe_status": "pending",
        "duration_ms": duration_ms,
        "has_watermark": "no",
        "risk_level": "medium",
        "asset_status": "active",
        "human_review_status": "pending",
        "source_identity": prompt_id,
        "scene_tag": str(package.get("segment_type") or ""),
        "prompt_package_id": prompt_id,
        "slot_role": str(package.get("slot_role") or ""),
        "ai_gen_grade": str(package.get("ai_gen_grade") or ""),
        "hook_intent": str(package.get("hook_intent") or ""),
        "generation_type": "image_to_video",
        "generation_model": "jimeng",
        "generation_prompt": prompt_text,
    }
    asset_write = ctx.repo.upsert("assets", "asset_id", asset_row)
    if not asset_write.success:
        return {"record_id": record_id, "segment_prompt_id": prompt_id, "status": "failed", "reason": asset_write.to_dict()}

    segment_row = {
        "segment_id": segment_id,
        "asset_id": asset_id,
        "product_id": product_id,
        "segment_oss_object_id": oss_row["object_id"],
        "start_ms": 0,
        "end_ms": duration_ms,
        "duration_ms": duration_ms,
        "width": 1080,
        "height": 1920,
        "fps": 30,
        "segment_status": "created",
        "source_type": "ai_generated",
        "source_trust_level": "medium",
        "product_binding_type": "exact_sku",
        "product_match_status": "anchor_pending",
        "product_match_confidence": "medium",
        "product_match_review_required": 1,
        "is_image_generated": 0,
        "segment_type": str(package.get("segment_type") or ""),
        "prompt_package_id": prompt_id,
        "slot_role": str(package.get("slot_role") or ""),
        "ai_gen_grade": str(package.get("ai_gen_grade") or ""),
        "hook_intent": str(package.get("hook_intent") or ""),
    }
    segment_write = ctx.repo.upsert("segments", "segment_id", segment_row)
    if not segment_write.success:
        return {"record_id": record_id, "segment_prompt_id": prompt_id, "status": "failed", "reason": segment_write.to_dict()}

    SegmentPromptFactorySkill(ctx).mark_imported(prompt_id, generated_asset_id=asset_id, generated_segment_id=segment_id)
    preview_url = preview_url_for_oss_object(ctx, oss_row, expires_seconds=30 * 86400)
    client.update_record_fields(
        record_id,
        {
            FIELD_PREVIEW_URL: url_cell(preview_url, "OSS预览"),
            FIELD_ATTACHMENT: [],
            FIELD_RESULT: f"已导入素材池 asset={asset_id} segment={segment_id}",
            FIELD_STATUS: "质检中",
        },
    )

    cleanup_local(local_path)
    return {
        "record_id": record_id,
        "segment_prompt_id": prompt_id,
        "product_id": product_id,
        "status": "imported",
        "asset_id": asset_id,
        "segment_id": segment_id,
        "oss_object_id": oss_row["object_id"],
        "object_key": object_key,
        "file_size": int(oss_row.get("file_size") or size or 0),
        "imported_at": now,
    }


def maybe_backfill_feishu(ctx: Any, client: FeishuBitableClient, record_id: str, package: Dict[str, Any]) -> None:
    asset_id = str(package.get("generated_asset_id") or "")
    segment_id = str(package.get("generated_segment_id") or "")
    if not asset_id or not segment_id:
        return
    asset = ctx.repo.get("assets", "asset_id", asset_id) or {}
    oss_object = ctx.repo.get("oss_objects", "object_id", asset.get("original_oss_object_id")) if asset.get("original_oss_object_id") else None
    preview_url = preview_url_for_oss_object(ctx, oss_object or {})
    fields: Dict[str, Any] = {
        FIELD_RESULT: f"已导入素材池 asset={asset_id} segment={segment_id}",
        FIELD_STATUS: "质检中",
    }
    if preview_url:
        fields[FIELD_PREVIEW_URL] = url_cell(preview_url, "OSS预览")
        fields[FIELD_ATTACHMENT] = []
    client.update_record_fields(record_id, fields)


def preview_url_for_oss_object(ctx: Any, oss_object: Dict[str, Any], expires_seconds: int = 30 * 86400) -> str:
    object_key = str((oss_object or {}).get("object_key") or "")
    if not object_key:
        return ""
    bucket_name = str((oss_object or {}).get("bucket") or "")
    current_bucket = getattr(ctx.oss, "bucket_name", ctx.settings.bucket)
    try:
        if bucket_name and bucket_name != current_bucket and ctx.settings.oss_provider == "aliyun":
            return AliyunOSS(
                bucket=bucket_name,
                endpoint=endpoint_for_bucket(bucket_name, ctx.settings.aliyun_oss_endpoint),
                access_key_id=ctx.settings.aliyun_access_key_id,
                access_key_secret=ctx.settings.aliyun_access_key_secret,
                security_token=ctx.settings.aliyun_security_token,
            ).signed_url(object_key, expires_seconds=expires_seconds)
        return ctx.oss.signed_url(object_key, expires_seconds=expires_seconds)
    except Exception:
        return ""


def endpoint_for_bucket(bucket: str, fallback: str) -> str:
    for marker in ("cn-shanghai", "cn-hangzhou", "cn-beijing", "cn-shenzhen", "cn-heyuan", "cn-guangzhou", "cn-hongkong", "ap-southeast-1"):
        if marker in str(bucket or ""):
            return f"https://oss-{marker}.aliyuncs.com"
    return fallback


def url_cell(url: str, text_value: str) -> Dict[str, str] | str:
    if not url:
        return ""
    return {"link": url, "text": text_value, "type": "url"}


def cleanup_local(local_path: Path) -> None:
    try:
        local_path.unlink(missing_ok=True)
        current = local_path.parent
        temp_root = local_path.parents[3]
        while current != temp_root:
            current.rmdir()
            current = current.parent
    except OSError:
        return


def attachments(value: Any) -> List[Dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict) and item.get("file_token")]
    if isinstance(value, dict) and value.get("file_token"):
        return [value]
    return []


def text(value: Any) -> str:
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
        return " / ".join(item for item in (text(v) for v in value) if item)
    return str(value).strip()


def safe_name(value: str) -> str:
    cleaned = "".join("_" if ch in '\\/:*?"<>|' else ch for ch in str(value or "asset.mp4")).strip()
    return cleaned or "asset.mp4"


if __name__ == "__main__":
    raise SystemExit(main())
