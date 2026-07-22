#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
import time
from pathlib import Path
from typing import Any

from light_tryon.feishu_client import TableEndpoint, make_client


def _client(app_token: str, table_id: str, role: str):
    return make_client(TableEndpoint(role, "", app_token, table_id, ""))


def _download_with_retry(client: Any, attachment: dict[str, Any], attempts: int = 5):
    error: Exception | None = None
    for attempt in range(attempts):
        try:
            return client.download_attachment_bytes(attachment)
        except Exception as exc:  # Feishu may briefly report “Data not ready”.
            error = exc
            if attempt + 1 < attempts:
                time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"附件下载失败: {attachment.get('name') or attachment.get('file_token')}: {error}")


def _number(value: Any) -> int | float | None:
    if value in (None, ""):
        return None
    result = float(value)
    return int(result) if result.is_integer() else result


def _normalize(value: Any, field_type: int) -> Any:
    if value in (None, "", [], {}):
        return None
    if field_type == 2:
        return _number(value)
    if field_type == 5:
        return int(value)
    if field_type == 7:
        return bool(value)
    if field_type == 15:
        if isinstance(value, dict):
            return value
        text = str(value).strip()
        return {"link": text, "text": text} if text else None
    return value


def migrate_table(
    source: Any,
    target: Any,
    *,
    primary_field: str,
    attachment_parent_type: str,
) -> dict[str, Any]:
    source_rows = source.list_records(page_size=500)
    target_rows = target.list_records(page_size=500)
    existing = {
        str(row.fields.get(primary_field) or "").strip()
        for row in target_rows
        if str(row.fields.get(primary_field) or "").strip()
    }
    target_types = {
        field.field_name: int(field.field_type)
        for field in target.list_fields()
        if int(field.field_type) not in {1001, 1002}
    }
    uploaded_by_source_token: dict[str, dict[str, Any]] = {}
    prepared: list[dict[str, Any]] = []
    attachment_count = 0
    attachment_bytes = 0

    for row in source_rows:
        identity = str(row.fields.get(primary_field) or "").strip()
        if not identity or identity in existing:
            continue
        fields: dict[str, Any] = {}
        for name, value in row.fields.items():
            field_type = target_types.get(name)
            if field_type is None:
                continue
            if field_type == 17:
                attachments = []
                for attachment in value if isinstance(value, list) else []:
                    if not isinstance(attachment, dict):
                        continue
                    source_token = str(attachment.get("file_token") or attachment.get("token") or "").strip()
                    if not source_token:
                        continue
                    uploaded = uploaded_by_source_token.get(source_token)
                    if uploaded is None:
                        content, file_name, content_type, size = _download_with_retry(source, attachment)
                        uploaded = target.upload_attachment(
                            content,
                            file_name,
                            content_type or "application/octet-stream",
                            size,
                            parent_type=attachment_parent_type,
                        )
                        uploaded_by_source_token[source_token] = uploaded
                        attachment_count += 1
                        attachment_bytes += int(size or len(content))
                    attachments.append({"file_token": uploaded["file_token"]})
                if attachments:
                    fields[name] = attachments
                continue
            normalized = _normalize(value, field_type)
            if normalized is not None:
                fields[name] = normalized
        prepared.append({"fields": fields})

    created = 0
    for start in range(0, len(prepared), 20):
        created += len(target.batch_create_records(prepared[start:start + 20]))
    return {
        "source_records": len(source_rows),
        "existing_records": len(target_rows),
        "created_records": created,
        "uploaded_attachments": attachment_count,
        "uploaded_bytes": attachment_bytes,
    }


def rebind_database(db_path: str, target_review: Any, target_visual: Any) -> dict[str, int]:
    review_rows = target_review.list_records(page_size=500)
    visual_rows = target_visual.list_records(page_size=500)
    conn = sqlite3.connect(Path(db_path).expanduser().resolve())
    review_updated = 0
    visual_updated = 0
    try:
        for row in review_rows:
            job_id = str(row.fields.get("视频任务ID") or "").strip()
            if not job_id:
                continue
            raw = row.fields.get("初始成片") if isinstance(row.fields.get("初始成片"), list) else []
            final = row.fields.get("最终视频") if isinstance(row.fields.get("最终视频"), list) else []
            cursor = conn.execute(
                "UPDATE video_jobs SET feishu_review_record_id=?, raw_video_attachments=?, final_video_attachments=? WHERE job_id=?",
                (row.record_id, json.dumps(raw, ensure_ascii=False), json.dumps(final, ensure_ascii=False), job_id),
            )
            review_updated += int(cursor.rowcount == 1)
        for row in visual_rows:
            plan_id = str(row.fields.get("视觉方案ID") or "").strip()
            if not plan_id:
                continue
            attachments = row.fields.get("产品穿搭图") if isinstance(row.fields.get("产品穿搭图"), list) else []
            cursor = conn.execute(
                "UPDATE product_visual_plans SET feishu_record_id=?, outfit_image_attachments=? WHERE visual_plan_id=?",
                (row.record_id, json.dumps(attachments, ensure_ascii=False), plan_id),
            )
            visual_updated += int(cursor.rowcount == 1)
        conn.commit()
    finally:
        conn.close()
    return {"review_jobs": review_updated, "visual_plans": visual_updated}


def main() -> int:
    parser = argparse.ArgumentParser(description="将当前有效轻视频复核数据迁移到干净的飞书多维表格")
    parser.add_argument("--source-app", required=True)
    parser.add_argument("--source-review-table", required=True)
    parser.add_argument("--source-visual-table", required=True)
    parser.add_argument("--target-app", required=True)
    parser.add_argument("--target-review-table", required=True)
    parser.add_argument("--target-visual-table", required=True)
    parser.add_argument("--db", default="")
    parser.add_argument("--manifest", default="")
    args = parser.parse_args()

    target_review = _client(args.target_app, args.target_review_table, "target_review")
    target_visual = _client(args.target_app, args.target_visual_table, "target_visual")
    result = {
        "review": migrate_table(
            _client(args.source_app, args.source_review_table, "source_review"),
            target_review,
            primary_field="视频任务ID",
            attachment_parent_type="bitable_file",
        ),
        "visual_plan": migrate_table(
            _client(args.source_app, args.source_visual_table, "source_visual"),
            target_visual,
            primary_field="视觉方案ID",
            attachment_parent_type="bitable_image",
        ),
    }
    if args.db:
        result["database_rebind"] = rebind_database(args.db, target_review, target_visual)
    if args.manifest:
        Path(args.manifest).expanduser().resolve().write_text(
            json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
