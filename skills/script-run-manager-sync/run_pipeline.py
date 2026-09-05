#!/usr/bin/env python3
"""把原创视频脚本同步到短视频自动脚本运行管理表。"""

from __future__ import annotations

import argparse
import hashlib
import fcntl
import os
import sqlite3
import sys
import time
from contextlib import contextmanager
from collections import defaultdict
from pathlib import Path
from types import SimpleNamespace
from typing import Dict, List, Optional, Tuple


SKILL_DIR = Path(__file__).parent.absolute()
sys.path.insert(0, str(SKILL_DIR))

from core.bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # noqa: E402
from core.feishu_url_parser import parse_feishu_bitable_url  # noqa: E402
from core.manual_source import (  # noqa: E402
    build_manual_sync_tasks,
    resolve_manual_field_mapping,
    upsert_manual_metadata,
)
from core.original_batch_source import (  # noqa: E402
    build_original_batch_sync_tasks,
    resolve_original_batch_field_mapping,
)
from core.first_frame_handoff import (  # noqa: E402
    apply_composite_reference_handoff,
    complete_wsr_reference_handoff,
    first_frame_preview_action,
    first_frame_source_supported,
    generate_first_frame_from_snapshot,
)
from core.reference_manifest_handoff import bind_transferred_wsr_references, load_frozen_wsr_context
from core.seeding_batch_source import (  # noqa: E402
    build_seeding_sync_tasks,
    resolve_seeding_batch_field_mapping,
)
from core.sync import (  # noqa: E402
    RUN_MANAGER_SCRIPT_TYPE_OPTIONS,
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
    "Bbi4bD4Hxa9cWms2GO2cDZ9wnBc?table=tbljUInlUld4MnOw&view=vewo6WdFGb"
)
DEFAULT_MANUAL_SOURCE_FEISHU_URL = (
    "https://gcngopvfvo0q.feishu.cn/wiki/"
    "LUfYwCLiTidK26kwTFBcrIignuI?table=tblyaHzECcsu4hyo&view=vewayNJu3z"
)
DEFAULT_ORIGINAL_BATCH_SOURCE_FEISHU_URL = (
    "https://gcngopvfvo0q.feishu.cn/wiki/"
    "KsX7w8Y8ZiJfnsk2Mtvc7xLun1f?table=tblIvHJ0nsn9WCwi&view=vewKfXc8lj"
)
DEFAULT_METADATA_DB_PATH = os.environ.get(
    "SHORT_VIDEO_AUTO_PUBLISH_DB_PATH",
    str(Path.home() / ".openclaw" / "shared" / "data" / "short_video_auto_publish.sqlite3"),
)
DEFAULT_LOCK_FILE = os.environ.get("SCRIPT_RUN_MANAGER_SYNC_LOCK_FILE", "/tmp/script_run_manager_sync.pid")
PATCHABLE_TARGET_STATUSES = {"", "待处理", "待开始", "未开始", "失败", "阻塞"}


class ReferencePreparationPending(RuntimeError):
    """The operator selected a first frame, but its asset is not ready yet."""


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


def ensure_target_default_fields(client: FeishuBitableClient, field_names: List[str]) -> List[str]:
    changed = False
    for field_name in (
        "产品ID", "全球产品ID", "任务来源", "人物模板ID", "人物模板合同",
    ):
        if field_name in field_names:
            continue
        print(f"🧩 目标运行表缺少字段【{field_name}】，正在创建...")
        client.create_field(field_name, field_type=1, ui_type="Text")
        changed = True
    if "脚本类型" not in field_names:
        print("🧩 目标运行表缺少字段【脚本类型】，正在创建...")
        client.create_field(
            "脚本类型",
            field_type=3,
            ui_type="SingleSelect",
            property={"options": [{"name": item} for item in RUN_MANAGER_SCRIPT_TYPE_OPTIONS]},
        )
        changed = True
    else:
        script_type_field = next(
            (field for field in client.list_fields() if field.field_name == "脚本类型"),
            None,
        )
        if script_type_field and int(script_type_field.field_type or 0) == 3:
            current_options = list((script_type_field.property or {}).get("options") or [])
            current_names = {str(item.get("name") or "") for item in current_options}
            missing_options = [
                value for value in RUN_MANAGER_SCRIPT_TYPE_OPTIONS if value not in current_names
            ]
            if missing_options:
                client.update_field(
                    script_type_field.field_id,
                    field_name="脚本类型",
                    field_type=3,
                    property={"options": current_options + [{"name": value} for value in missing_options]},
                )
                changed = True
    if "店铺ID" not in field_names:
        print("🧩 目标运行表缺少字段【店铺ID】，正在创建...")
        client.create_field("店铺ID", field_type=1, ui_type="Text")
        changed = True
    if "免参考图" not in field_names:
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
        changed = True
    if "视频时长" not in field_names:
        print("🧩 目标运行表缺少字段【视频时长】，正在创建...")
        client.create_field("视频时长", field_type=2, ui_type="Number")
        changed = True
    for field_name in ("口播表达合同", "口播执行计划"):
        if field_name in field_names:
            continue
        print(f"🧩 目标运行表缺少字段【{field_name}】，正在创建...")
        client.create_field(field_name, field_type=1, ui_type="Text")
        changed = True
    if "视觉参考模式" not in field_names:
        print("🧩 目标运行表缺少字段【视觉参考模式】，正在创建...")
        client.create_field("视觉参考模式", field_type=1, ui_type="Text")
        changed = True
    for field_name in ("发布用途", "是否挂车", "内容分支"):
        if field_name in field_names:
            continue
        print(f"🧩 目标运行表缺少字段【{field_name}】，正在创建...")
        client.create_field(field_name, field_type=1, ui_type="Text")
        changed = True
    return client.list_field_names() if changed else field_names


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
                   direction_label, variant_strength, short_video_title, canonical_script_key
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
            "canonical_script_key": str(row["canonical_script_key"] or ""),
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


@contextmanager
def process_lock(lock_file: str):
    path = Path(lock_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(f"同步任务已在运行中，已跳过本次启动: lock={lock_file}") from exc
        handle.seek(0)
        handle.truncate()
        handle.write(f"{os.getpid()} {int(time.time())}\n")
        handle.flush()
        try:
            yield
        finally:
            handle.seek(0)
            handle.truncate()
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _record_field_text(record: object, field_name: Optional[str]) -> str:
    if not field_name:
        return ""
    return str(record.fields.get(field_name) or "").strip()


def build_target_record_indexes(records: List, mapping: Dict[str, object]) -> Dict[str, Dict[str, object]]:
    indexes: Dict[str, Dict[str, object]] = {
        "script_id": {},
        "task_name": {},
        "internal_script_key": {},
    }
    for record in records:
        for index_name in indexes:
            field_name = mapping.get(index_name)
            value = _record_field_text(record, field_name)
            if value and value not in indexes[index_name]:
                indexes[index_name][value] = record
    return indexes


def find_existing_target(task, indexes: Dict[str, Dict[str, object]]) -> Tuple[Optional[object], str]:
    if task.script_id and task.script_id in indexes["script_id"]:
        return indexes["script_id"][task.script_id], "脚本ID"
    internal_script_key = getattr(task, "internal_script_key", "") or task.task_name
    if internal_script_key and internal_script_key in indexes["internal_script_key"]:
        return indexes["internal_script_key"][internal_script_key], "内部脚本键"
    if task.task_name and task.task_name in indexes["task_name"]:
        return indexes["task_name"][task.task_name], "任务名"
    return None, ""


def can_update_existing_target(record: object, mapping: Dict[str, object]) -> bool:
    status_field = mapping.get("task_status")
    if not status_field:
        return False
    status = _record_field_text(record, status_field)
    return status in PATCHABLE_TARGET_STATUSES


def build_existing_target_updates(
    existing_target: object,
    fields: Dict[str, object],
    mapping: Dict[str, object],
    *,
    allow_full_patch: bool,
) -> Dict[str, object]:
    # Once execution has started, even missing metadata is part of the frozen
    # snapshot. Do not silently change a scheduled/submitted production task.
    if not can_update_existing_target(existing_target, mapping):
        return {}
    updates: Dict[str, object] = {}
    prompt_field = mapping.get("prompt")
    if prompt_field and (
        allow_full_patch
        or "【脚本ID】" not in str(existing_target.fields.get(prompt_field) or "")
    ):
        updates[prompt_field] = fields[prompt_field]

    for logical_name in (
        "script_id", "store_id", "internal_script_key", "task_name", "script_type",
        "short_video_title", "parent_slot", "direction_label", "variant_strength",
        "script_source", "publish_purpose", "cart_enabled", "content_branch",
        "product_id", "canonical_product_id", "target_language", "persona_id", "persona_contract", "first_frame_strategy",
        "voiceover_expression_contract", "voiceover_execution_plan", "voiceover_requested", "voiceover_status",
    ):
        field_name = mapping.get(logical_name)
        if field_name and field_name in fields and (allow_full_patch or (fields[field_name] and not existing_target.fields.get(field_name))):
            updates[field_name] = fields[field_name]

    reference_free_field = mapping.get("reference_free")
    if allow_full_patch and reference_free_field in fields:
        updates[reference_free_field] = fields[reference_free_field]
    if fields.get(mapping.get("reference_free")) == "是" and can_patch_reference_free(existing_target, mapping):
        updates[mapping["reference_free"]] = "是"

    duration_field = mapping.get("video_duration")
    if duration_field and fields.get(duration_field) and (allow_full_patch or not existing_target.fields.get(duration_field)):
        updates[duration_field] = fields[duration_field]
    return updates


def frozen_pool_target_conflict(task, record: object, fields: Dict[str, object], mapping: Dict[str, object]) -> str:
    if not task.script_pool_entry or can_update_existing_target(record, mapping):
        return ""
    for logical in ("prompt", "script_source", "publish_purpose", "cart_enabled", "content_branch", "product_id", "canonical_product_id", "voiceover_execution_plan", "persona_contract"):
        # A blank new-source platform ID means "resolve from the binding",
        # not a requested clear, unless the operator explicitly disabled cart.
        if logical == "product_id" and task.cart_enabled == "是" and not task.product_id:
            continue
        field_name = mapping.get(logical)
        if field_name in fields and (record.fields.get(field_name) or "") != (fields[field_name] or ""):
            return f"SCRIPT_POOL_EXECUTION_FROZEN:任务已开始或排期，{field_name}修改未应用；需另建生产任务"
    return ""


def register_pool_task_metadata(task, db_path: str) -> Dict[str, object]:
    """Freeze the operator's selected settings in the existing publish DB.

    Import lazily: previews and unrelated legacy sources never open this DB.
    """
    publisher_dir = SKILL_DIR.parent / "short-video-auto-publisher"
    if str(publisher_dir) not in sys.path:
        sys.path.insert(0, str(publisher_dir))
    from app.db import AutoPublishDB
    from app.script_pool import register_script_pool_metadata

    return register_script_pool_metadata(
        AutoPublishDB(Path(db_path)), script_id=task.script_id,
        source_record_id=task.source_record_id, prompt=task.prompt_text,
        product_id=task.product_code, platform_product_id=task.product_id or None,
        store_id=task.store_id, publish_purpose=task.publish_purpose,
        cart_enabled=task.cart_enabled, target_country=task.target_country,
        target_language=task.target_language, short_video_title=task.short_video_title,
        parent_slot=task.parent_slot, direction_label=task.direction_label,
        variant_strength=task.variant_strength, product_type=task.product_type,
        task_name=task.task_name, script_source=task.script_source,
        content_branch=task.content_branch,
    )


def validate_pool_target_mapping(task, mapping: Dict[str, object]) -> None:
    if not task.script_pool_entry:
        return
    required = ["script_source", "script_type", "publish_purpose", "cart_enabled", "content_branch", "product_id", "canonical_product_id", "reference_free"]
    if task.persona_contract:
        required.append("persona_contract")
    if task.source_voiceover_managed:
        required.extend(["voiceover_requested", "voiceover_status", "voiceover_execution_plan", "voiceover_expression_contract"])
    missing = [key for key in required if not mapping.get(key)]
    if missing:
        raise ValueError("SCRIPT_POOL_TARGET_FIELDS_MISSING:" + ",".join(missing))


def remember_target_fields(indexes: Dict[str, Dict[str, object]], record: object, fields: Dict[str, object], mapping: Dict[str, object]) -> None:
    remembered_record = record or SimpleNamespace(record_id="", fields=dict(fields))
    for index_name in indexes:
        field_name = mapping.get(index_name)
        value = str(fields.get(field_name) or "").strip() if field_name else ""
        if value and value not in indexes[index_name]:
            indexes[index_name][value] = remembered_record


def resolve_task_action(task, indexes: Dict[str, Dict[str, object]], mapping: Dict[str, object]) -> str:
    waiting_action = first_frame_preview_action(task)
    if waiting_action:
        return waiting_action
    existing_target, reason = find_existing_target(task, indexes)
    if existing_target is None:
        return "create"
    if reason == "脚本ID":
        return "skip(script_id exists)"
    if can_update_existing_target(existing_target, mapping):
        return f"update({reason})"
    return f"skip({reason} exists)"


def existing_target_conflict_reason(task, existing_target: object, existing_reason: str, mapping: Dict[str, object]) -> str:
    if existing_reason == "脚本ID":
        return ""
    script_field = mapping.get("script_id")
    target_script_id = _record_field_text(existing_target, script_field)
    if target_script_id and target_script_id != task.script_id:
        return f"{existing_reason}命中旧记录，但脚本ID不一致: target={target_script_id}, expected={task.script_id}"

    prompt_field = mapping.get("prompt")
    target_prompt = _record_field_text(existing_target, prompt_field)
    if "【脚本ID】" in target_prompt and task.script_id and task.script_id not in target_prompt:
        return f"{existing_reason}命中旧记录，但提示词脚本ID不一致: expected={task.script_id}"
    return ""


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
            transferred.append({key: value for key, value in cached.items() if not key.startswith("_")})
            continue

        content, file_name, content_type, size = source_client.download_attachment_bytes(attachment)
        uploaded = target_client.upload_attachment(
            content=content,
            file_name=file_name,
            content_type=content_type,
            size=size,
        )
        cache[source_file_token] = {**uploaded, "_transfer_sha256": hashlib.sha256(content).hexdigest()}
        transferred.append(dict(uploaded))
    return transferred


def prepare_selected_first_frame(task, record, source_mapping, source_url, target_indexes, target_mapping):
    """Generate exactly this already-selected short row, then rebuild in memory.

    Called only under the production process lock, after schema/policy checks
    and target indexing. A check/dry-run never reaches this function.
    """
    verify_wsr_ready = task.script_source == "成功脚本复刻" and normalize_checkbox(
        record.fields.get(source_mapping.get("first_frame_requested")))
    if not task.reference_preparation_error and not verify_wsr_ready:
        return task
    if not first_frame_source_supported(task):
        raise RuntimeError(f"FIRST_FRAME_SOURCE_UNSUPPORTED:暂不支持此来源生成首帧：{task.script_source}")
    existing, _ = find_existing_target(task, target_indexes)
    if existing is not None and not can_update_existing_target(existing, target_mapping):
        raise RuntimeError("FIRST_FRAME_EXECUTION_FROZEN:运行任务已提交或排期，不重新生成首帧；已保留进入生产勾选")
    record.execution_target_snapshot = {"record_id": existing.record_id, "fields": dict(existing.fields)} if existing is not None else None
    print(f"   🖼️ 同轮准备首帧 | source_record_id={record.record_id} | script_id={task.script_id}", flush=True)
    fields = generate_first_frame_from_snapshot(record, source_url)
    record.fields.update(fields)
    errors = {}
    rebuilt = build_original_batch_sync_tasks([record], source_mapping, record_id=record.record_id, limit=1, errors=errors)
    if errors or len(rebuilt) != 1 or rebuilt[0].reference_preparation_error:
        raise RuntimeError(f"FIRST_FRAME_HANDOFF_INVALID:首帧处理后记录仍不可交接：{errors or '参考附件未就绪'}")
    prepared = complete_wsr_reference_handoff(rebuilt[0], record, source_mapping)
    validate_pool_target_mapping(prepared, target_mapping)
    return prepared


def main() -> None:
    parser = argparse.ArgumentParser(description="原创视频脚本 -> 运行管理表 同步任务")
    parser.add_argument("--mode", choices=["manual", "scheduled"], default="manual", help="触发模式")
    parser.add_argument(
        "--source-kind",
        choices=["production", "manual", "original-batch", "seeding-batch"],
        default="production",
        help="源表类型；original-batch/seeding-batch 均为一行一条的独立生产脚本表",
    )
    parser.add_argument("--source-feishu-url", help="源表飞书 URL；未传时按源表类型使用默认表")
    parser.add_argument("--target-feishu-url", default=DEFAULT_TARGET_FEISHU_URL, help="目标表飞书 URL")
    parser.add_argument("--limit", type=int, help="限制同步脚本条数")
    parser.add_argument("--product-code", help="只处理指定产品编码")
    parser.add_argument("--record-id", help="只处理指定源表 record_id")
    parser.add_argument("--batch-size", type=int, default=100, help="写入批大小，默认 100")
    parser.add_argument("--dry-run", action="store_true", help="只预览，不落表")
    parser.add_argument("--metadata-db-path", default=DEFAULT_METADATA_DB_PATH, help="脚本主数据 SQLite 路径")
    parser.add_argument("--lock-file", default=DEFAULT_LOCK_FILE, help="进程锁文件，避免定时/手动同步并发执行")
    parser.add_argument(
        "--include-publish-metadata",
        action="store_true",
        help="额外把短视频标题/店铺ID/产品ID/所属母版/母版方向/变体强度回写到运行表；默认只写最小字段",
    )
    args = parser.parse_args()

    with process_lock(args.lock_file):
        _main_with_lock(args)


def _main_with_lock(args: argparse.Namespace) -> None:

    print(f"🚀 开始执行同步任务 | mode={args.mode} | source_kind={args.source_kind}")
    include_publish_metadata = args.include_publish_metadata or args.source_kind == "seeding-batch"

    default_source_urls = {
        "manual": DEFAULT_MANUAL_SOURCE_FEISHU_URL,
        "original-batch": DEFAULT_ORIGINAL_BATCH_SOURCE_FEISHU_URL,
        "production": DEFAULT_SOURCE_FEISHU_URL,
    }
    if args.source_kind == "seeding-batch" and not args.source_feishu_url:
        raise ValueError("seeding-batch 必须显式传 --source-feishu-url，禁止误读原创脚本表")
    source_feishu_url = args.source_feishu_url or default_source_urls[args.source_kind]
    source_app_token, source_table_id = resolve_feishu_config(source_feishu_url)
    target_app_token, target_table_id = resolve_feishu_config(args.target_feishu_url)

    source_client = FeishuBitableClient(app_token=source_app_token, table_id=source_table_id)
    target_client = FeishuBitableClient(app_token=target_app_token, table_id=target_table_id)

    source_field_names = source_client.list_field_names()
    target_field_names = target_client.list_field_names()
    if not args.dry_run:
        target_field_names = ensure_target_default_fields(target_client, target_field_names)
    if args.source_kind == "manual":
        source_mapping = resolve_manual_field_mapping(source_field_names)
    elif args.source_kind == "original-batch":
        source_mapping = resolve_original_batch_field_mapping(source_field_names)
    elif args.source_kind == "seeding-batch":
        source_mapping = resolve_seeding_batch_field_mapping(source_field_names)
    else:
        source_mapping = resolve_field_mapping(source_field_names, SOURCE_FIELD_ALIASES)
    target_mapping = resolve_field_mapping(target_field_names, TARGET_FIELD_ALIASES)

    if args.source_kind == "manual":
        validate_required_fields(source_mapping, ["script_id", "script", "purpose", "store_id", "sync_enabled"])
    elif args.source_kind == "original-batch":
        validate_required_fields(
            source_mapping,
            ["script_id", "video_prompt", "sync_enabled"],
        )
    elif args.source_kind == "seeding-batch":
        validate_required_fields(
            source_mapping,
            [
                "script_id", "product_code", "product_images", "video_prompt",
                "sync_enabled", "publish_policy",
            ],
        )
    else:
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

    # A record-scoped run must stay record-scoped.  The old implementation
    # fetched every source row even when --record-id was supplied.
    source_records = (
        [source_client.get_record(args.record_id)]
        if args.record_id
        else source_client.list_records(page_size=500)
    )

    manual_script_ids: Dict[str, str] = {}
    preflight_errors: Dict[str, str] = {}
    if args.source_kind == "manual":
        manual_result = build_manual_sync_tasks(
            source_records,
            source_mapping,
            record_id=args.record_id,
            limit=args.limit,
        )
        sync_tasks = manual_result.tasks
        manual_script_ids = manual_result.script_ids
        preflight_errors = manual_result.errors
    elif args.source_kind == "original-batch":
        sync_tasks = build_original_batch_sync_tasks(
            source_records,
            source_mapping,
            product_code=args.product_code,
            record_id=args.record_id,
            limit=args.limit,
            errors=preflight_errors,
        )
    elif args.source_kind == "seeding-batch":
        sync_tasks = build_seeding_sync_tasks(
            source_records,
            source_mapping,
            product_code=args.product_code,
            record_id=args.record_id,
            limit=args.limit,
        )
    else:
        sync_tasks = build_sync_tasks(
            source_records,
            source_mapping,
            product_code=args.product_code,
            record_id=args.record_id,
            limit=args.limit,
            metadata_lookup=metadata_lookup,
        )
    tasks_by_source: Dict[str, List] = defaultdict(list)
    valid_tasks = []
    source_by_id = {record.record_id: record for record in source_records}
    for task in sync_tasks:
        if args.source_kind == "original-batch":
            task = complete_wsr_reference_handoff(task, source_by_id[task.source_record_id], source_mapping)
        try:
            validate_pool_target_mapping(task, target_mapping)
        except ValueError as exc:
            preflight_errors[task.source_record_id] = str(exc)
            continue
        valid_tasks.append(task)
        tasks_by_source[task.source_record_id].append(task)
    sync_tasks = valid_tasks

    # The target table is the large table in this flow.  Do not read it during
    # an idle poll or when the selected source record fails preflight checks.
    target_records = target_client.list_records(page_size=500) if sync_tasks else []
    target_indexes = build_target_record_indexes(target_records, target_mapping)

    print("\n📊 预检查结果:")
    print(f"   源表记录数: {len(source_records)}")
    print(f"   待同步源记录数: {len(tasks_by_source)}")
    print(f"   待新增脚本数: {len(sync_tasks)}")
    if preflight_errors:
        print(f"   输入校验失败源记录数: {len(preflight_errors)}")
        for failed_record_id, message in list(preflight_errors.items())[:10]:
            print(f"   - {failed_record_id}: {message}")

    if sync_tasks:
        print("\n🧩 任务预览:")
        for task in sync_tasks[: min(len(sync_tasks), 10)]:
            action = resolve_task_action(task, target_indexes, target_mapping)
            print(f"   - {task.task_name} | source_record_id={task.source_record_id} | action={action}")

    if args.dry_run:
        print("\n🔍 dry-run 模式，不执行写入。")
        print(
            f"📡 飞书记录接口请求: source={source_client.request_count} "
            f"target={target_client.request_count} total={source_client.request_count + target_client.request_count}"
        )
        return

    failed_records = 0
    if preflight_errors:
        for failed_record_id, message in preflight_errors.items():
            try:
                failure_fields = build_source_failure_fields(
                    source_mapping,
                    error_message=message,
                    synced_at=now_text(),
                    sync_scope="人工脚本" if args.source_kind == "manual" else "视频脚本总库",
                )
                if args.source_kind == "original-batch":
                    if source_mapping.get("sync_time"):
                        failure_fields[source_mapping["sync_time"]] = int(time.time() * 1000)
                    if source_mapping.get("processing_status"):
                        failure_fields[source_mapping["processing_status"]] = "同步失败"
                script_id_field = source_mapping.get("script_id")
                if script_id_field and manual_script_ids.get(failed_record_id):
                    failure_fields[script_id_field] = manual_script_ids[failed_record_id]
                source_client.update_record_fields(failed_record_id, failure_fields)
                print(f"   ❌ source_record_id={failed_record_id} 输入校验失败: {message}")
            except Exception as exc:
                print(f"   ⚠️ source_record_id={failed_record_id} 回写校验失败状态失败: {exc}")
            failed_records += 1

    if args.source_kind == "manual" and sync_tasks:
        metadata_tasks = []
        for task in sync_tasks:
            existing_target, _ = find_existing_target(task, target_indexes)
            if existing_target is None or can_update_existing_target(existing_target, target_mapping):
                metadata_tasks.append(task)
        registered = upsert_manual_metadata(metadata_tasks, args.metadata_db_path)
        print(f"   ✅ 已登记人工脚本主数据: {registered} 条")

    image_cache: Dict[str, dict] = {}
    wsr_context_cache: Dict[str, dict] = {}
    created = 0

    for source_record_id, source_tasks in tasks_by_source.items():
        try:
            source_record = next((record for record in source_records if record.record_id == source_record_id), None)
            source_fields = source_record.fields if source_record else {}
            prepared_creates = []
            patched_for_source = 0
            existing_for_source = 0
            unresolved_tasks = []
            for task in source_tasks:
                verify_wsr_ready = task.script_source == "成功脚本复刻" and normalize_checkbox(
                    source_fields.get(source_mapping.get("first_frame_requested")))
                if args.source_kind == "original-batch" and (task.reference_preparation_error or verify_wsr_ready):
                    existing_frame_target, _ = find_existing_target(task, target_indexes)
                    if task.script_source == "成功脚本复刻" and (existing_frame_target is None or can_update_existing_target(existing_frame_target, target_mapping)):
                        if task.script_id not in wsr_context_cache:
                            wsr_context_cache[task.script_id] = load_frozen_wsr_context(task.script_id)
                        source_record.fields["_wsr_reference_manifest"] = wsr_context_cache[task.script_id].get("reference_manifest")
                    task = prepare_selected_first_frame(
                        task, source_record, source_mapping, source_feishu_url,
                        target_indexes, target_mapping,
                    )
                if task.reference_preparation_error:
                    unresolved_tasks.append(
                        f"{task.script_id}: {task.reference_preparation_error}"
                    )
                    print(
                        f"   ⏸️ 视觉参考尚未就绪 | task={task.task_name} | "
                        f"{task.reference_preparation_error}"
                    )
                    continue
                fields = build_target_fields(
                    task,
                    target_mapping,
                    include_publish_metadata=include_publish_metadata,
                )
                fields = apply_composite_reference_handoff(task, fields, target_mapping)
                existing_target, existing_reason = find_existing_target(task, target_indexes)
                if task.script_pool_entry:
                    frozen_reason = frozen_pool_target_conflict(task, existing_target, fields, target_mapping) if existing_target is not None else ""
                    if frozen_reason:
                        unresolved_tasks.append(f"{task.script_id}: {frozen_reason}")
                        continue
                    if existing_target is None or can_update_existing_target(existing_target, target_mapping):
                        metadata_result = register_pool_task_metadata(task, args.metadata_db_path)
                        if metadata_result.get("status") == "frozen":
                            unresolved_tasks.append(f"{task.script_id}: SCRIPT_POOL_EXECUTION_FROZEN:发布数据已排期或提交，修改未应用")
                            continue
                        if metadata_result.get("status") not in {"created", "updated", "unchanged"}:
                            raise RuntimeError(f"SCRIPT_POOL_METADATA_REGISTRATION_FAILED:{metadata_result.get('status')}")
                        if "platform_product_id" in metadata_result and target_mapping.get("product_id"):
                            fields[target_mapping["product_id"]] = metadata_result["platform_product_id"] or None
                        if metadata_result.get("canonical_script_key") and target_mapping.get("internal_script_key"):
                            fields[target_mapping["internal_script_key"]] = metadata_result["canonical_script_key"]
                if existing_target is not None:
                    if not getattr(existing_target, "record_id", ""):
                        print(f"   🔁 本轮内已准备创建，跳过重复创建 | task={task.task_name} | script_id={task.script_id}")
                        existing_for_source += 1
                        continue
                    conflict_reason = existing_target_conflict_reason(task, existing_target, existing_reason, target_mapping)
                    if conflict_reason:
                        unresolved_tasks.append(f"{task.script_id}: {conflict_reason}")
                        print(f"   ⚠️ {conflict_reason} | task={task.task_name}")
                        continue
                    allow_full_patch = (
                    existing_reason != "脚本ID"
                    or args.source_kind in {"manual", "original-batch", "seeding-batch"}
                    ) and can_update_existing_target(existing_target, target_mapping)
                    existing_updates = build_existing_target_updates(
                        existing_target,
                        fields,
                        target_mapping,
                        allow_full_patch=allow_full_patch,
                    )
                    if (
                        args.source_kind in {"manual", "original-batch", "seeding-batch"}
                        and allow_full_patch
                        and target_mapping.get("reference_images")
                    ):
                        existing_updates[target_mapping["reference_images"]] = transfer_reference_images(
                            source_client,
                            target_client,
                            task.reference_images,
                            image_cache,
                        )
                        if task.script_source == "成功脚本复刻":
                            if task.script_id not in wsr_context_cache:
                                wsr_context_cache[task.script_id] = load_frozen_wsr_context(task.script_id)
                            existing_updates = bind_transferred_wsr_references(task, existing_updates, target_mapping, image_cache, wsr_context_cache[task.script_id])
                    if existing_updates:
                        target_client.update_record_fields(existing_target.record_id, existing_updates)
                        patched_names = "、".join(existing_updates.keys())
                        remember_target_fields(target_indexes, existing_target, {**fields, **existing_updates}, target_mapping)
                        print(f"   🔁 {existing_reason} 已存在，已补写{patched_names} | task={task.task_name} | script_id={task.script_id}")
                        patched_for_source += 1
                    else:
                        print(f"   🔁 {existing_reason} 已存在，跳过重复创建 | task={task.task_name} | script_id={task.script_id}")
                        existing_for_source += 1
                    continue
                if target_mapping.get("reference_images"):
                    fields[target_mapping["reference_images"]] = transfer_reference_images(
                        source_client,
                        target_client,
                        task.reference_images,
                        image_cache,
                    )
                    if task.script_source == "成功脚本复刻":
                        if task.script_id not in wsr_context_cache:
                            wsr_context_cache[task.script_id] = load_frozen_wsr_context(task.script_id)
                        fields = bind_transferred_wsr_references(task, fields, target_mapping, image_cache, wsr_context_cache[task.script_id])
                prepared_creates.append({"fields": fields})
                remember_target_fields(target_indexes, None, fields, target_mapping)

            if unresolved_tasks:
                message = "；".join(unresolved_tasks[:5])
                if all("WAITING_USER_SELECTED_FIRST_FRAME" in item for item in unresolved_tasks):
                    raise ReferencePreparationPending(message)
                raise RuntimeError(message)

            created_target_ids = []
            for batch in batch_records(prepared_creates, batch_size=args.batch_size):
                if not batch:
                    continue
                created_target_ids.extend(target_client.batch_create_records(batch))
                created += len(batch)
                print(f"   ✅ source_record_id={source_record_id} 已创建 {len(batch)} 条")

            synced_at = now_text()
            legacy_enabled = normalize_checkbox(source_fields.get(source_mapping["sync_enabled"])) if source_mapping.get("sync_enabled") else False
            master_enabled = normalize_checkbox(source_fields.get(source_mapping["sync_master_enabled"])) if source_mapping.get("sync_master_enabled") else False
            variant_enabled = normalize_checkbox(source_fields.get(source_mapping["sync_variant_enabled"])) if source_mapping.get("sync_variant_enabled") else False
            success_fields = build_source_success_fields(
                source_mapping,
                synced_count=len(prepared_creates),
                synced_at=synced_at,
                sync_scope="人工脚本" if args.source_kind == "manual" else summarize_sync_scope(source_tasks),
                patched_count=patched_for_source,
                existing_count=existing_for_source,
                cleared_legacy=legacy_enabled,
                cleared_master=master_enabled,
                cleared_variant=variant_enabled,
            )
            if args.source_kind in {"original-batch", "seeding-batch"}:
                if source_mapping.get("processing_status"):
                    success_fields[source_mapping["processing_status"]] = "已送生产"
                if source_mapping.get("sync_time"):
                    success_fields[source_mapping["sync_time"]] = int(time.time() * 1000)
                if source_mapping.get("run_task_id") and created_target_ids:
                    success_fields[source_mapping["run_task_id"]] = created_target_ids[0]
            script_id_field = source_mapping.get("script_id")
            if args.source_kind == "manual" and script_id_field and manual_script_ids.get(source_record_id):
                success_fields[script_id_field] = manual_script_ids[source_record_id]
            source_client.update_record_fields(source_record_id, success_fields)
            print(f"   ✅ source_record_id={source_record_id} 已回写同步状态")
        except ReferencePreparationPending as exc:
            # This is an expected wait state, not a production failure.  Keep
            # `进入生产` checked so the next patrol can continue after the
            # first-frame runner writes an asset.
            waiting_fields = {}
            if source_mapping.get("sync_status"):
                waiting_fields[source_mapping["sync_status"]] = f"等待首帧：{exc}"
            if source_mapping.get("sync_time"):
                waiting_fields[source_mapping["sync_time"]] = int(time.time() * 1000)
            if waiting_fields:
                source_client.update_record_fields(source_record_id, waiting_fields)
            print(f"   ⏸️ source_record_id={source_record_id} 等待用户已选择的首帧就绪")
        except Exception as exc:
            failed_records += 1
            synced_at = now_text()
            failure_fields = build_source_failure_fields(
                source_mapping,
                error_message=str(exc),
                synced_at=synced_at,
                sync_scope="人工脚本" if args.source_kind == "manual" else summarize_sync_scope(source_tasks),
            )
            if args.source_kind in {"original-batch", "seeding-batch"} and source_mapping.get("processing_status"):
                failure_fields[source_mapping["processing_status"]] = "同步失败"
            if args.source_kind in {"original-batch", "seeding-batch"} and source_mapping.get("sync_time"):
                failure_fields[source_mapping["sync_time"]] = int(time.time() * 1000)
            script_id_field = source_mapping.get("script_id")
            if args.source_kind == "manual" and script_id_field and manual_script_ids.get(source_record_id):
                failure_fields[script_id_field] = manual_script_ids[source_record_id]
            source_client.update_record_fields(source_record_id, failure_fields)
            print(f"   ❌ source_record_id={source_record_id} 同步失败: {exc}")

    print("\n🎉 同步完成")
    print(f"   创建: {created}")
    print(f"   失败源记录数: {failed_records}")
    print(
        f"📡 飞书记录接口请求: source={source_client.request_count} "
        f"target={target_client.request_count} total={source_client.request_count + target_client.request_count}"
    )


if __name__ == "__main__":
    main()
