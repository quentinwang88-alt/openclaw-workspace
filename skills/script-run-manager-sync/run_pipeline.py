#!/usr/bin/env python3
"""把原创视频脚本同步到短视频自动脚本运行管理表。"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple


SKILL_DIR = Path(__file__).parent.absolute()
sys.path.insert(0, str(SKILL_DIR))

from core.bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # noqa: E402
from core.feishu_url_parser import parse_feishu_bitable_url  # noqa: E402
from core.sync import (  # noqa: E402
    SOURCE_FIELD_ALIASES,
    TARGET_FIELD_ALIASES,
    batch_records,
    build_source_failure_fields,
    build_source_success_fields,
    build_sync_tasks,
    build_target_fields,
    normalize_checkbox,
    now_text,
    resolve_field_mapping,
    summarize_sync_scope,
    validate_required_fields,
    validate_script_fields,
)


DEFAULT_SOURCE_FEISHU_URL = (
    "https://gcngopvfvo0q.feishu.cn/wiki/"
    "ZezEwZ7cKiUyeakdlI3cUuU1nRf?table=tblHRLMr9b3fvxBw&view=vewPpvR2oT"
)
DEFAULT_TARGET_FEISHU_URL = (
    "https://gcngopvfvo0q.feishu.cn/base/"
    "UvErb5HRWaGESXsBs18cvB3FnEe?table=tbl4eKSVgHw8IyDh&view=vewo6WdFGb"
)
DEFAULT_METADATA_DB_PATH = os.environ.get(
    "SHORT_VIDEO_AUTO_PUBLISH_DB_PATH",
    str(Path.home() / ".openclaw" / "shared" / "data" / "short_video_auto_publish.sqlite3"),
)


def resolve_feishu_config(feishu_url: str) -> Tuple[str, str]:
    info = parse_feishu_bitable_url(feishu_url)
    if not info:
        raise ValueError(f"无法解析飞书 URL: {feishu_url}")
    app_token = info.app_token
    if "/wiki/" in info.original_url:
        app_token = resolve_wiki_bitable_app_token(info.app_token)
        print(f"🔄 检测到 wiki 链接，已解析底层 bitable app_token: {app_token}")
    return app_token, info.table_id


def print_field_mapping(title: str, mapping: dict) -> None:
    print(f"\n📋 {title}:")
    for key, value in mapping.items():
        print(f"   {key}: {value or '未找到'}")


def ensure_reference_free_field(client: FeishuBitableClient, field_names: List[str]) -> List[str]:
    if "免参考图" in field_names:
        return field_names
    print("🧩 目标运行表缺少字段【免参考图】，正在创建...")
    try:
        client.create_field(
            "免参考图",
            field_type=3,
            ui_type="SingleSelect",
            property={"options": [{"name": "是"}, {"name": "否"}]},
        )
    except Exception as exc:
        print(f"⚠️ 创建单选字段【免参考图】失败，降级创建文本字段: {exc}")
        client.create_field("免参考图", field_type=1, ui_type="Text")
    return client.list_field_names()


def load_metadata_lookup(db_path: str) -> Dict[tuple, Dict[str, str]]:
    path = Path(db_path)
    if not path.exists():
        return {}
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT source_record_id, script_slot, script_id, store_id, product_id, parent_slot,
                   direction_label, variant_strength, short_video_title
            FROM script_metadata
            """
        ).fetchall()
    except sqlite3.DatabaseError:
        return {}
    finally:
        conn.close()
    return {
        (str(row["source_record_id"]), str(row["script_slot"])): {
            "script_id": str(row["script_id"] or ""),
            "store_id": str(row["store_id"] or ""),
            "product_id": str(row["product_id"] or ""),
            "parent_slot": str(row["parent_slot"] or ""),
            "direction_label": str(row["direction_label"] or ""),
            "variant_strength": str(row["variant_strength"] or ""),
            "short_video_title": str(row["short_video_title"] or ""),
        }
        for row in rows
    }


def target_records_by_script_id(records: List, mapping: Dict[str, object]) -> Dict[str, object]:
    script_field = mapping.get("script_id")
    if not script_field:
        return {}
    result: Dict[str, object] = {}
    for record in records:
        script_id = str(record.fields.get(script_field) or "").strip()
        if script_id and script_id not in result:
            result[script_id] = record
    return result


def can_patch_reference_free(record: object, mapping: Dict[str, object]) -> bool:
    status_field = mapping.get("task_status")
    if not status_field:
        return True
    status = str(record.fields.get(status_field) or "").strip()
    return status in {"", "待开始", "未开始"}


def transfer_reference_images(
    source_client: FeishuBitableClient,
    target_client: FeishuBitableClient,
    attachments: List[dict],
    cache: Dict[str, dict],
) -> List[dict]:
    transferred: List[dict] = []
    for attachment in attachments:
        source_file_token = str(attachment.get("file_token", "")).strip()
        if not source_file_token:
            continue
        cached = cache.get(source_file_token)
        if cached:
            transferred.append(dict(cached))
            continue

        content, file_name, content_type, size = source_client.download_attachment_bytes(attachment)
        uploaded = target_client.upload_attachment(
            content=content,
            file_name=file_name,
            content_type=content_type,
            size=size,
        )
        cache[source_file_token] = uploaded
        transferred.append(dict(uploaded))
    return transferred


def main() -> None:
    parser = argparse.ArgumentParser(description="原创视频脚本 -> 运行管理表 同步任务")
    parser.add_argument("--mode", choices=["manual", "scheduled"], default="manual", help="触发模式")
    parser.add_argument("--source-feishu-url", default=DEFAULT_SOURCE_FEISHU_URL, help="源表飞书 URL")
    parser.add_argument("--target-feishu-url", default=DEFAULT_TARGET_FEISHU_URL, help="目标表飞书 URL")
    parser.add_argument("--limit", type=int, help="限制同步脚本条数")
    parser.add_argument("--product-code", help="只处理指定产品编码")
    parser.add_argument("--record-id", help="只处理指定源表 record_id")
    parser.add_argument("--batch-size", type=int, default=100, help="写入批大小，默认 100")
    parser.add_argument("--dry-run", action="store_true", help="只预览，不落表")
    parser.add_argument("--metadata-db-path", default=DEFAULT_METADATA_DB_PATH, help="脚本主数据 SQLite 路径")
    parser.add_argument(
        "--include-publish-metadata",
        action="store_true",
        help="额外把短视频标题/店铺ID/产品ID/所属母版/母版方向/变体强度回写到运行表；默认只写最小字段",
    )
    args = parser.parse_args()

    print(f"🚀 开始执行同步任务 | mode={args.mode}")

    source_app_token, source_table_id = resolve_feishu_config(args.source_feishu_url)
    target_app_token, target_table_id = resolve_feishu_config(args.target_feishu_url)

    source_client = FeishuBitableClient(app_token=source_app_token, table_id=source_table_id)
    target_client = FeishuBitableClient(app_token=target_app_token, table_id=target_table_id)

    source_field_names = source_client.list_field_names()
    target_field_names = ensure_reference_free_field(target_client, target_client.list_field_names())
    source_mapping = resolve_field_mapping(source_field_names, SOURCE_FIELD_ALIASES)
    target_mapping = resolve_field_mapping(target_field_names, TARGET_FIELD_ALIASES)

    validate_required_fields(
        source_mapping,
        ["product_code", "product_images"],
    )
    validate_script_fields(source_mapping)
    validate_required_fields(target_mapping, ["task_name", "prompt", "reference_images", "script_id"])
    metadata_lookup = load_metadata_lookup(args.metadata_db_path)

    print_field_mapping("源表字段映射", source_mapping)
    print_field_mapping("目标表字段映射", target_mapping)
    print(f"\n🗂️ 脚本主数据命中数: {len(metadata_lookup)} | metadata_db_path={args.metadata_db_path}")
    print("   说明: 运行表里的脚本ID优先来自脚本主数据库；数据库未命中时才按源表规则即时推导")

    source_records = source_client.list_records(page_size=100)
    target_records = target_client.list_records(page_size=100)
    target_by_script_id = target_records_by_script_id(target_records, target_mapping)

    sync_tasks = build_sync_tasks(
        source_records,
        source_mapping,
        product_code=args.product_code,
        record_id=args.record_id,
        limit=args.limit,
        metadata_lookup=metadata_lookup,
    )
    tasks_by_source: Dict[str, List] = defaultdict(list)
    for task in sync_tasks:
        tasks_by_source[task.source_record_id].append(task)

    print("\n📊 预检查结果:")
    print(f"   源表记录数: {len(source_records)}")
    print(f"   待同步源记录数: {len(tasks_by_source)}")
    print(f"   待新增脚本数: {len(sync_tasks)}")

    if sync_tasks:
        print("\n🧩 任务预览:")
        for task in sync_tasks[: min(len(sync_tasks), 10)]:
            print(f"   - {task.task_name} | source_record_id={task.source_record_id} | action=create")

    if args.dry_run:
        print("\n🔍 dry-run 模式，不执行写入。")
        return

    image_cache: Dict[str, dict] = {}
    created = 0
    failed_records = 0

    for source_record_id, source_tasks in tasks_by_source.items():
        try:
            source_record = next((record for record in source_records if record.record_id == source_record_id), None)
            source_fields = source_record.fields if source_record else {}
            prepared_creates = []
            for task in source_tasks:
                fields = build_target_fields(
                    task,
                    target_mapping,
                    include_publish_metadata=args.include_publish_metadata,
                )
                if target_mapping.get("reference_images"):
                    fields[target_mapping["reference_images"]] = transfer_reference_images(
                        source_client,
                        target_client,
                        task.reference_images,
                        image_cache,
                    )
                existing_target = target_by_script_id.get(task.script_id)
                if existing_target is not None:
                    existing_updates = {}
                    prompt_field = target_mapping.get("prompt")
                    if prompt_field and "【脚本ID】" not in str(existing_target.fields.get(prompt_field) or ""):
                        existing_updates[prompt_field] = fields[prompt_field]
                    if fields.get(target_mapping.get("reference_free")) == "是" and can_patch_reference_free(existing_target, target_mapping):
                        existing_updates[target_mapping["reference_free"]] = "是"
                    if existing_updates:
                        target_client.update_record_fields(existing_target.record_id, existing_updates)
                        patched_names = "、".join(existing_updates.keys())
                        print(f"   🔁 script_id={task.script_id} 已存在，已补写{patched_names}")
                    else:
                        print(f"   🔁 script_id={task.script_id} 已存在，跳过重复创建")
                    continue
                prepared_creates.append({"fields": fields})

            for batch in batch_records(prepared_creates, batch_size=args.batch_size):
                if not batch:
                    continue
                target_client.batch_create_records(batch)
                created += len(batch)
                print(f"   ✅ source_record_id={source_record_id} 已创建 {len(batch)} 条")

            synced_at = now_text()
            legacy_enabled = normalize_checkbox(source_fields.get(source_mapping["sync_enabled"])) if source_mapping.get("sync_enabled") else False
            master_enabled = normalize_checkbox(source_fields.get(source_mapping["sync_master_enabled"])) if source_mapping.get("sync_master_enabled") else False
            variant_enabled = normalize_checkbox(source_fields.get(source_mapping["sync_variant_enabled"])) if source_mapping.get("sync_variant_enabled") else False
            source_client.update_record_fields(
                source_record_id,
                build_source_success_fields(
                    source_mapping,
                    synced_count=len(source_tasks),
                    synced_at=synced_at,
                    sync_scope=summarize_sync_scope(source_tasks),
                    cleared_legacy=legacy_enabled,
                    cleared_master=master_enabled,
                    cleared_variant=variant_enabled,
                ),
            )
            print(f"   ✅ source_record_id={source_record_id} 已回写同步状态")
        except Exception as exc:
            failed_records += 1
            synced_at = now_text()
            source_client.update_record_fields(
                source_record_id,
                build_source_failure_fields(
                    source_mapping,
                    error_message=str(exc),
                    synced_at=synced_at,
                    sync_scope=summarize_sync_scope(source_tasks),
                ),
            )
            print(f"   ❌ source_record_id={source_record_id} 同步失败: {exc}")

    print("\n🎉 同步完成")
    print(f"   创建: {created}")
    print(f"   失败源记录数: {failed_records}")


if __name__ == "__main__":
    main()
