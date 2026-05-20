#!/usr/bin/env python3
"""短视频自动发布系统入口。"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple


SKILL_DIR = Path(__file__).parent.absolute()
sys.path.insert(0, str(SKILL_DIR))

from app.db import AutoPublishDB, default_db_path, default_video_dir  # noqa: E402
from app.metadata import (  # noqa: E402
    FallbackTitleGenerator,
    HeuristicTitleGenerator,
    LLMTitleGenerator,
    LocalizedLLMTitleGenerator,
    SOURCE_FIELD_ALIASES as SCRIPT_SOURCE_FIELD_ALIASES,
    build_script_metadata_records,
    is_title_compatible_with_country,
    resolve_field_mapping as resolve_script_mapping,
    sanitize_title,
)
from app.models import ScriptMetadata  # noqa: E402
from app.notifications import (  # noqa: E402
    default_queue_date,
    default_summary_date,
    format_daily_publish_summary,
    format_manual_publish_queue,
    send_feishu_webhook_text,
    send_openclaw_feishu_text,
)
from app.publishers import DryRunPublishAdapter, GeeLarkPublishAdapter, HttpPublishAdapter  # noqa: E402
from app.reporting import (  # noqa: E402
    apply_manual_publish_statuses_from_table,
    sync_manual_publish_queue_table,
    sync_product_publish_report_table,
    sync_publish_report_table,
)
from app.scheduler import (  # noqa: E402
    ACCOUNT_FIELD_ALIASES,
    RUN_MANAGER_FIELD_ALIASES,
    resolve_field_mapping as resolve_table_mapping,
    schedule_slots,
    sync_accounts,
    sync_publish_results,
    sync_videos,
)


SYNC_SKILL_DIR = SKILL_DIR.parent / "script-run-manager-sync"
sys.path.insert(0, str(SYNC_SKILL_DIR))
from core.bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # type: ignore  # noqa: E402
from core.feishu_url_parser import parse_feishu_bitable_url  # type: ignore  # noqa: E402


DEFAULT_SCRIPT_FEISHU_URL = (
    "https://gcngopvfvo0q.feishu.cn/wiki/"
    "ZezEwZ7cKiUyeakdlI3cUuU1nRf?table=tblHRLMr9b3fvxBw&view=vewPpvR2oT"
)
DEFAULT_RUN_MANAGER_FEISHU_URL = (
    "https://gcngopvfvo0q.feishu.cn/base/"
    "UvErb5HRWaGESXsBs18cvB3FnEe?table=tbl4eKSVgHw8IyDh&view=vewo6WdFGb"
)
DEFAULT_ACCOUNT_FEISHU_URL = (
    "https://gcngopvfvo0q.feishu.cn/wiki/"
    "Ucp3wCAPiiUYY5kbByecQSienFc?table=tblD0bF1eiQ0Q6rr&view=vewVD91euG"
)
DEFAULT_REPORT_FEISHU_URL = (
    "https://gcngopvfvo0q.feishu.cn/wiki/"
    "QjAWwBMPSiwajkkNXROc3SiVnrg?table=tblflJ8MuATv1Duk&view=vewo3uicWW"
)
DEFAULT_MANUAL_QUEUE_FEISHU_URL = (
    "https://gcngopvfvo0q.feishu.cn/wiki/"
    "NKKFwOIyqiD46PkZ27yc4tNJn3z?table=tblP6lL3ABNxOInH&view=vewJI1HObF"
)
DEFAULT_PRODUCT_PUBLISH_REPORT_FEISHU_URL = (
    "https://gcngopvfvo0q.feishu.cn/wiki/"
    "NaIowjl1tiGwW7kQHWhcdqrwnKv?table=tblGrXCTRiLA1zgm&view=vew9CZwG3X"
)
DEFAULT_CONFIG_PATH = Path("/Users/likeu3/.openclaw/shared/data/short_video_auto_publisher_config.json")
DEFAULT_OPENCLAW_CONFIG_PATH = Path("/Users/likeu3/.openclaw/openclaw.json")


@contextmanager
def exclusive_run_lock(name: str):
    lock_dir = Path.home() / ".openclaw" / "shared" / "locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / f"short-video-auto-publisher-{name}.lock"
    with open(lock_path, "w", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(f"已有 {name} 流程正在运行，为避免重复创建 GeeLark 任务，本次退出") from exc
        yield


def ensure_video_storage_ready(video_dir: str | Path, *, sample_paths: Iterable[str] | None = None) -> Path:
    requested = Path(video_dir).expanduser()
    resolved = requested.resolve(strict=False)

    if requested.is_symlink() and not requested.exists():
        raise RuntimeError(
            f"本地视频目录软链已断开: {requested} -> {resolved}。请先挂载移动存储空间1，再执行发布任务。"
        )

    if not requested.exists():
        raise RuntimeError(f"本地视频目录不存在: {requested}。请先检查视频存储目录或挂载移动存储空间1。")

    if not requested.is_dir():
        raise RuntimeError(f"本地视频目录不是文件夹: {requested}")

    test_file = requested / ".openclaw_storage_probe"
    try:
        test_file.write_text("ok", encoding="utf-8")
        test_file.unlink()
    except OSError as exc:
        raise RuntimeError(f"本地视频目录不可写: {requested}，请检查移动盘权限或挂载状态。原始错误: {exc}") from exc

    if sample_paths:
        for raw_path in sample_paths:
            candidate = Path(str(raw_path or "").strip())
            if not candidate:
                continue
            if str(candidate).strip() and not candidate.exists():
                raise RuntimeError(
                    f"检测到数据库中的视频文件不可访问: {candidate}。通常表示移动存储空间1未挂载或路径失效。"
                )

    return requested


def load_local_config(config_path: str | Path = DEFAULT_CONFIG_PATH) -> Dict[str, Any]:
    path = Path(config_path)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def resolve_disabled_store_ids(args: argparse.Namespace) -> set[str]:
    """读取本地临时停发店铺配置。

    配置示例：
    {"disabled_store_ids": ["THPS01"]}
    """
    config = load_local_config(getattr(args, "config_path", DEFAULT_CONFIG_PATH))
    raw_values = []
    for key in ("disabled_store_ids", "paused_store_ids"):
        value = config.get(key)
        if isinstance(value, list):
            raw_values.extend(value)
        elif isinstance(value, str):
            raw_values.extend(value.split(","))
    return {str(item or "").strip().upper() for item in raw_values if str(item or "").strip()}


def apply_disabled_store_overrides(db: AutoPublishDB, args: argparse.Namespace) -> Dict[str, int]:
    """把临时停发店铺强制从本地排期池中移除，避免下次 run-all 又排上。"""
    disabled_store_ids = resolve_disabled_store_ids(args)
    if not disabled_store_ids:
        return {"disabled_stores": 0, "paused_accounts": 0, "deleted_future_pending_slots": 0}

    now_text = db._now_text()  # noqa: SLF001 - 当前项目 DB 封装未暴露批量暂停接口
    cutoff = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    paused_accounts = 0
    deleted_slots = 0
    with db._connect() as conn:  # noqa: SLF001
        for store_id in sorted(disabled_store_ids):
            paused_accounts += conn.execute(
                """
                UPDATE account_configs
                SET account_status = '暂停', updated_at = ?
                WHERE UPPER(COALESCE(store_id, '')) = ?
                  AND account_status <> '暂停'
                """,
                (now_text, store_id),
            ).rowcount
            deleted_slots += conn.execute(
                """
                DELETE FROM publish_slots
                WHERE UPPER(COALESCE(store_id, '')) = ?
                  AND scheduled_for >= ?
                  AND schedule_status = '待排期'
                  AND publish_task_id IS NULL
                """,
                (store_id, cutoff),
            ).rowcount
    return {
        "disabled_stores": len(disabled_store_ids),
        "paused_accounts": paused_accounts,
        "deleted_future_pending_slots": deleted_slots,
    }


def sync_recent_manual_publish_statuses(db: AutoPublishDB, args: argparse.Namespace) -> Dict[str, int]:
    lookback_days = max(0, int(getattr(args, "manual_status_lookback_days", 3) or 0))
    if lookback_days <= 0:
        return {
            "manual_rows_checked": 0,
            "manual_rows_marked_published": 0,
            "manual_rows_skipped": 0,
            "manual_rows_failed": 0,
            "lookback_days": 0,
        }
    today = datetime.now().date()
    days = {(today - timedelta(days=offset)).isoformat() for offset in range(lookback_days + 1)}
    app_token, table_id = resolve_feishu_config(resolve_manual_queue_feishu_url(args))
    client = FeishuBitableClient(app_token=app_token, table_id=table_id)
    stats = apply_manual_publish_statuses_from_table(db, client, days=days)
    stats["lookback_days"] = lookback_days
    return stats


def resolve_publish_api_token(args: argparse.Namespace) -> str:
    explicit = str(getattr(args, "publish_api_token", "") or "").strip()
    if explicit:
        return explicit

    env_candidates = [
        "SHORT_VIDEO_AUTO_PUBLISH_API_TOKEN",
        "GEELARK_BEARER_TOKEN",
        "GEELARK_API_TOKEN",
        "GEE_LARK_BEARER_TOKEN",
        "GEE_LARK_API_TOKEN",
    ]
    for key in env_candidates:
        value = str(os.environ.get(key, "") or "").strip()
        if value:
            return value

    config = load_local_config(getattr(args, "config_path", DEFAULT_CONFIG_PATH))
    for key in ("publish_api_token", "geelark_token", "bearer_token", "token"):
        value = str(config.get(key, "") or "").strip()
        if value:
            return value
    return ""


def resolve_feishu_webhook_url(args: argparse.Namespace) -> str:
    explicit = str(getattr(args, "feishu_webhook_url", "") or "").strip()
    if explicit:
        return explicit

    for key in (
        "SHORT_VIDEO_AUTO_PUBLISH_FEISHU_WEBHOOK_URL",
        "FEISHU_WEBHOOK_URL",
        "LARK_WEBHOOK_URL",
    ):
        value = str(os.environ.get(key, "") or "").strip()
        if value:
            return value

    config = load_local_config(getattr(args, "config_path", DEFAULT_CONFIG_PATH))
    for key in (
        "feishu_webhook_url",
        "daily_summary_webhook_url",
        "publish_summary_webhook_url",
        "group_webhook_url",
        "webhook_url",
    ):
        value = str(config.get(key, "") or "").strip()
        if value:
            return value
    return ""


def resolve_feishu_chat_id(args: argparse.Namespace) -> str:
    explicit = str(getattr(args, "feishu_chat_id", "") or "").strip()
    if explicit:
        return explicit

    for key in (
        "SHORT_VIDEO_AUTO_PUBLISH_FEISHU_CHAT_ID",
        "FEISHU_CHAT_ID",
        "LARK_CHAT_ID",
    ):
        value = str(os.environ.get(key, "") or "").strip()
        if value:
            return value

    config = load_local_config(getattr(args, "config_path", DEFAULT_CONFIG_PATH))
    for key in (
        "feishu_chat_id",
        "daily_summary_chat_id",
        "publish_summary_chat_id",
        "openclaw_feishu_chat_id",
    ):
        value = str(config.get(key, "") or "").strip()
        if value:
            return value
    return ""


def resolve_manual_queue_feishu_url(args: argparse.Namespace) -> str:
    explicit = str(getattr(args, "manual_queue_feishu_url", "") or "").strip()
    if explicit:
        return explicit
    config = load_local_config(getattr(args, "config_path", DEFAULT_CONFIG_PATH))
    for key in ("manual_queue_feishu_url", "manual_publish_queue_feishu_url"):
        value = str(config.get(key, "") or "").strip()
        if value:
            return value
    return DEFAULT_MANUAL_QUEUE_FEISHU_URL


def resolve_feishu_config(feishu_url: str) -> Tuple[str, str]:
    info = parse_feishu_bitable_url(feishu_url)
    if not info:
        raise ValueError(f"无法解析飞书 URL: {feishu_url}")
    app_token = info.app_token
    if "/wiki/" in info.original_url:
        app_token = resolve_wiki_bitable_app_token(info.app_token)
    return app_token, info.table_id


def ensure_account_nurture_fields(client: FeishuBitableClient, field_names: list[str]) -> list[str]:
    existing = set(field_names)
    if "是否开启养号" not in existing:
        try:
            client.create_field(
                "是否开启养号",
                field_type=3,
                ui_type="SingleSelect",
                property={"options": [{"name": "是"}, {"name": "否"}]},
            )
        except Exception:
            client.create_field("是否开启养号", field_type=1, ui_type="Text")
    if "每日养号条数" not in existing:
        try:
            client.create_field("每日养号条数", field_type=2, ui_type="Number")
        except Exception:
            client.create_field("每日养号条数", field_type=1, ui_type="Text")
    if "是否仅养号" not in existing:
        try:
            client.create_field("是否仅养号", field_type=7, ui_type="Checkbox")
        except Exception:
            try:
                client.create_field(
                    "是否仅养号",
                    field_type=3,
                    ui_type="SingleSelect",
                    property={"options": [{"name": "是"}, {"name": "否"}]},
                )
            except Exception:
                client.create_field("是否仅养号", field_type=1, ui_type="Text")
    return client.list_field_names()


def build_title_generator(mode: str, llm_route: str):
    if mode == "heuristic":
        return HeuristicTitleGenerator()
    if mode == "llm":
        return LocalizedLLMTitleGenerator(preferred_route=llm_route)
    return FallbackTitleGenerator(
        primary=LocalizedLLMTitleGenerator(preferred_route=llm_route),
        secondary=HeuristicTitleGenerator(),
    )


def build_publish_adapter(args: argparse.Namespace):
    mode = args.publish_mode
    resolved_token = resolve_publish_api_token(args)
    if mode == "http":
        return HttpPublishAdapter(args.publish_api_base_url, token=resolved_token)
    if mode == "geelark":
        return GeeLarkPublishAdapter(
            token=resolved_token,
            endpoint=args.geelark_task_add_endpoint,
            upload_endpoint=args.geelark_upload_get_url_endpoint,
            auth_header=args.geelark_auth_header,
            auth_scheme=args.geelark_auth_scheme,
            plan_name_field=args.geelark_plan_name_field,
            remark_field=args.geelark_remark_field,
            task_type_field=args.geelark_task_type_field,
            list_field=args.geelark_list_field,
            task_type_value=args.geelark_task_type_value,
            env_id_field=args.geelark_env_id_field,
            video_field=args.geelark_video_field,
            schedule_at_field=args.geelark_schedule_at_field,
            video_desc_field=args.geelark_video_desc_field,
            product_id_field=args.geelark_product_id_field,
            product_title_field=args.geelark_product_title_field,
            ref_video_id_field=args.geelark_ref_video_id_field,
            upload_file_type_field=args.geelark_upload_file_type_field,
            status_endpoint=args.geelark_status_endpoint,
            status_method=args.geelark_status_method,
            status_task_id_field=args.geelark_status_task_id_field,
            task_id_paths=args.geelark_task_id_paths,
            upload_url_paths=args.geelark_upload_url_paths,
            resource_url_paths=args.geelark_resource_url_paths,
            status_value_paths=args.geelark_status_value_paths,
            success_values=args.geelark_success_values,
            failure_values=args.geelark_failure_values,
            published_at_paths=args.geelark_published_at_paths,
            error_message_paths=args.geelark_error_message_paths,
            extra_body_json=args.geelark_extra_body_json,
        )
    return DryRunPublishAdapter()


def command_sync_script_db(args: argparse.Namespace) -> None:
    db = AutoPublishDB(Path(args.db_path))
    title_generator = build_title_generator(args.title_mode, args.llm_route)
    existing_lookup = db.build_metadata_lookup()

    app_token, table_id = resolve_feishu_config(args.script_feishu_url)
    client = FeishuBitableClient(app_token=app_token, table_id=table_id)
    field_names = client.list_field_names()
    mapping = resolve_script_mapping(field_names, SCRIPT_SOURCE_FIELD_ALIASES)
    records = client.list_records(page_size=100, limit=args.limit)

    def emit_progress(payload: Dict[str, int]) -> None:
        print({"step": "sync_script_db_progress", **payload}, flush=True)

    metadata = build_script_metadata_records(
        records,
        mapping,
        title_generator=title_generator,
        existing_lookup=existing_lookup,
        progress_callback=emit_progress,
        record_id=args.record_id,
        product_id=args.product_id,
        limit=args.limit,
    )
    written = db.upsert_script_metadata(metadata)
    print({"written": written, "db_path": str(db.db_path)})


def command_sync_videos(args: argparse.Namespace) -> None:
    ensure_video_storage_ready(args.video_dir)
    db = AutoPublishDB(Path(args.db_path))
    app_token, table_id = resolve_feishu_config(args.run_manager_feishu_url)
    client = FeishuBitableClient(app_token=app_token, table_id=table_id)
    field_names = client.list_field_names()
    mapping = resolve_table_mapping(field_names, RUN_MANAGER_FIELD_ALIASES)
    records = client.list_records(page_size=100, limit=args.limit)
    stats = sync_videos(
        records,
        mapping,
        db,
        download_dir=Path(args.video_dir),
        client=client,
    )
    print(stats)


def command_sync_accounts(args: argparse.Namespace) -> None:
    db = AutoPublishDB(Path(args.db_path))
    app_token, table_id = resolve_feishu_config(args.account_feishu_url)
    client = FeishuBitableClient(app_token=app_token, table_id=table_id)
    field_names = ensure_account_nurture_fields(client, client.list_field_names())
    mapping = resolve_table_mapping(field_names, ACCOUNT_FIELD_ALIASES)
    records = client.list_records(page_size=100, limit=args.limit)
    count = sync_accounts(records, mapping, db)
    override_stats = apply_disabled_store_overrides(db, args)
    print({"accounts_upserted": count, "disabled_store_overrides": override_stats})


def command_schedule(args: argparse.Namespace) -> None:
    with exclusive_run_lock("schedule"):
        video_dir = ensure_video_storage_ready(args.video_dir)
        db = AutoPublishDB(Path(args.db_path))
        override_stats = apply_disabled_store_overrides(db, args)
        if override_stats["disabled_stores"]:
            print({"disabled_store_overrides": override_stats}, flush=True)
        sample_paths = [str(row["local_file_path"] or "") for row in db.list_scheduled_tasks()[:10]]
        ensure_video_storage_ready(video_dir, sample_paths=sample_paths)
        publisher = build_publish_adapter(args)
        stats = schedule_slots(db, publisher)
        print(stats.__dict__)


def command_sync_results(args: argparse.Namespace) -> None:
    ensure_video_storage_ready(args.video_dir)
    db = AutoPublishDB(Path(args.db_path))
    publisher = build_publish_adapter(args)
    stats = sync_publish_results(db, publisher)
    print(stats)


def command_refresh_titles(args: argparse.Namespace) -> None:
    db = AutoPublishDB(Path(args.db_path))
    title_generator = build_title_generator(args.title_mode, args.llm_route)
    all_items = db.list_script_metadata(limit=args.limit)

    candidates: list[ScriptMetadata] = []
    skipped = 0
    for item in all_items:
        sanitized_existing = sanitize_title(item.short_video_title)
        needs_refresh = args.force
        if not needs_refresh:
            needs_refresh = (
                not sanitized_existing
                or sanitized_existing != item.short_video_title
                or not is_title_compatible_with_country(sanitized_existing, item.target_country)
            )
        if needs_refresh:
            candidates.append(item)
        else:
            skipped += 1

    written = 0
    batch: list[ScriptMetadata] = []
    for index, item in enumerate(candidates, start=1):
        title = sanitize_title(title_generator.generate(item))
        batch.append(
            ScriptMetadata(
                **{
                    **item.__dict__,
                    "short_video_title": title,
                    "title_source": getattr(title_generator, "source", item.title_source or "unknown"),
                }
            )
        )
        if len(batch) >= args.batch_size:
            written += db.upsert_script_metadata(batch)
            batch.clear()
        if index % args.batch_size == 0 or index == len(candidates):
            print(
                {
                    "step": "refresh_titles_progress",
                    "processed": index,
                    "total": len(candidates),
                    "written": written,
                },
                flush=True,
            )
    if batch:
        written += db.upsert_script_metadata(batch)
    print(
        {
            "scanned": len(all_items),
            "refresh_candidates": len(candidates),
            "refreshed": written,
            "skipped": skipped,
        }
    )


def command_sync_report_table(args: argparse.Namespace) -> None:
    db = AutoPublishDB(Path(args.db_path))
    app_token, table_id = resolve_feishu_config(args.report_feishu_url)
    client = FeishuBitableClient(app_token=app_token, table_id=table_id)
    stats = sync_publish_report_table(db, client)
    print(stats)


def command_sync_product_publish_report_table(args: argparse.Namespace) -> None:
    db = AutoPublishDB(Path(args.db_path))
    target_app_token, target_table_id = resolve_feishu_config(args.product_report_feishu_url)
    target_client = FeishuBitableClient(app_token=target_app_token, table_id=target_table_id)
    source_client = None
    if not args.no_upload_images:
        source_app_token, source_table_id = resolve_feishu_config(args.script_feishu_url)
        source_client = FeishuBitableClient(app_token=source_app_token, table_id=source_table_id)
    stats = sync_product_publish_report_table(
        db,
        target_client,
        source_client=source_client,
        reference_date=args.date,
        source_image_field=args.source_image_field,
        upload_images=not args.no_upload_images,
        force_upload_images=args.force_upload_images,
    )
    print(stats)


def command_sync_manual_queue_table(args: argparse.Namespace) -> None:
    db = AutoPublishDB(Path(args.db_path))
    app_token, table_id = resolve_feishu_config(resolve_manual_queue_feishu_url(args))
    client = FeishuBitableClient(app_token=app_token, table_id=table_id)
    queue_date = args.date or default_queue_date()
    stats = sync_manual_publish_queue_table(
        db,
        client,
        day=queue_date,
        upload_videos=not args.no_upload_videos,
        force_upload_videos=args.force_upload_videos,
        include_published=args.include_published,
    )
    print(stats)


def command_cleanup_published_videos(args: argparse.Namespace) -> None:
    ensure_video_storage_ready(args.video_dir)
    db = AutoPublishDB(Path(args.db_path))
    stats = db.cleanup_published_videos(
        older_than_days=args.retention_days,
        base_dir=Path(args.video_dir),
    )
    print(stats)


def command_disable_product(args: argparse.Namespace) -> None:
    db = AutoPublishDB(Path(args.db_path))
    stats = db.disable_product(args.product_id, reason=args.reason)
    print(stats)


def command_disable_account(args: argparse.Namespace) -> None:
    db = AutoPublishDB(Path(args.db_path))
    stats = db.disable_account(args.account_id, reason=args.reason)
    print(stats)


def command_enforce_retry_limit(args: argparse.Namespace) -> None:
    db = AutoPublishDB(Path(args.db_path))
    stats = db.enforce_retry_limit(max_auto_retries=args.max_auto_retries, reason=args.reason)
    print(json.dumps(stats, ensure_ascii=False, indent=2))


def send_notification_message(
    *,
    db: AutoPublishDB,
    args: argparse.Namespace,
    notification_key: str,
    message: str,
) -> Dict[str, Any]:
    if args.dry_run:
        print(message)
        return {"dry_run": True, "notification_key": notification_key}

    if db.has_sent_notification(notification_key) and not args.force:
        return {"skipped": True, "reason": "already_sent", "notification_key": notification_key}

    if args.delivery == "webhook":
        webhook_url = resolve_feishu_webhook_url(args)
        result = send_feishu_webhook_text(webhook_url, message)
        channel = "feishu_webhook"
    else:
        chat_id = resolve_feishu_chat_id(args)
        if not chat_id:
            raise ValueError("缺少飞书群 chat_id，请传 --feishu-chat-id 或写入配置 feishu_chat_id")
        result = send_openclaw_feishu_text(
            openclaw_config_path=args.openclaw_config_path,
            chat_id=chat_id,
            text=message,
            account=args.openclaw_feishu_account,
        )
        channel = f"openclaw_feishu:{args.openclaw_feishu_account or 'main'}:{chat_id}"
    db.mark_notification_sent(notification_key, channel, message)
    return {"sent": True, "notification_key": notification_key, "result": result}


def command_notify_daily_summary(args: argparse.Namespace) -> None:
    db = AutoPublishDB(Path(args.db_path))
    summary_date = args.date or default_summary_date()
    summary = db.build_daily_publish_summary(summary_date)
    message = format_daily_publish_summary(summary, max_failure_lines=args.max_failure_lines)
    notification_key = f"daily-publish-summary:{args.delivery}:{summary_date}"
    result = send_notification_message(db=db, args=args, notification_key=notification_key, message=message)
    print(result)


def command_notify_manual_queue(args: argparse.Namespace) -> None:
    db = AutoPublishDB(Path(args.db_path))
    queue_date = args.date or default_queue_date()
    rows = db.list_manual_publish_queue(queue_date, include_published=args.include_published)
    message = format_manual_publish_queue(rows, queue_date=queue_date, max_items=args.max_items)
    notification_key = f"manual-publish-queue:{args.delivery}:{queue_date}"
    result = send_notification_message(db=db, args=args, notification_key=notification_key, message=message)
    print(result)


def command_notify_ops_daily(args: argparse.Namespace) -> None:
    db = AutoPublishDB(Path(args.db_path))
    summary_date = args.summary_date or default_summary_date()
    queue_date = args.queue_date or default_queue_date()
    result_payload: Dict[str, Any] = {}

    if args.sync_manual_queue_table:
        app_token, table_id = resolve_feishu_config(resolve_manual_queue_feishu_url(args))
        manual_client = FeishuBitableClient(app_token=app_token, table_id=table_id)
        sync_dates = []
        for date_value in (summary_date, queue_date):
            if date_value and date_value not in sync_dates:
                sync_dates.append(date_value)
        result_payload["sync_manual_queue_table"] = {
            date_value: sync_manual_publish_queue_table(
                db,
                manual_client,
                day=date_value,
                upload_videos=not args.no_upload_videos,
                force_upload_videos=args.force_upload_videos,
                include_published=True,
            )
            for date_value in sync_dates
        }

    summary = db.build_daily_publish_summary(summary_date)
    summary_message = format_daily_publish_summary(summary, max_failure_lines=args.max_failure_lines)
    summary_key = f"daily-publish-summary:{args.delivery}:{summary_date}"
    summary_result = send_notification_message(
        db=db,
        args=args,
        notification_key=summary_key,
        message=summary_message,
    )

    queue_rows = db.list_manual_publish_queue(queue_date, include_published=args.include_published)
    queue_message = format_manual_publish_queue(queue_rows, queue_date=queue_date, max_items=args.max_items)
    queue_key = f"manual-publish-queue:{args.delivery}:{queue_date}"
    queue_result = send_notification_message(
        db=db,
        args=args,
        notification_key=queue_key,
        message=queue_message,
    )
    result_payload.update({"summary": summary_result, "manual_queue": queue_result})
    print(result_payload)


def command_run_all(args: argparse.Namespace) -> None:
    with exclusive_run_lock("schedule"):
        _command_run_all_locked(args)


def _command_run_all_locked(args: argparse.Namespace) -> None:
    summary = {}
    video_dir = ensure_video_storage_ready(args.video_dir)

    db = AutoPublishDB(Path(args.db_path))
    title_generator = build_title_generator(args.title_mode, args.llm_route)
    existing_lookup = db.build_metadata_lookup()

    print("[run-all] sync_script_db start", flush=True)
    script_app_token, script_table_id = resolve_feishu_config(args.script_feishu_url)
    script_client = FeishuBitableClient(app_token=script_app_token, table_id=script_table_id)
    script_field_names = script_client.list_field_names()
    script_mapping = resolve_script_mapping(script_field_names, SCRIPT_SOURCE_FIELD_ALIASES)
    script_records = script_client.list_records(page_size=100, limit=args.limit)
    print(f"[run-all] sync_script_db fetched {len(script_records)} source records", flush=True)

    def emit_progress(payload: Dict[str, int]) -> None:
        print(f"[run-all] sync_script_db progress {payload}", flush=True)

    metadata = build_script_metadata_records(
        script_records,
        script_mapping,
        title_generator=title_generator,
        existing_lookup=existing_lookup,
        progress_callback=emit_progress,
        record_id=args.record_id,
        product_id=args.product_id,
        limit=args.limit,
    )
    summary["sync_script_db"] = {"written": db.upsert_script_metadata(metadata)}
    print(f"[run-all] sync_script_db done {summary['sync_script_db']}", flush=True)

    print("[run-all] sync_accounts start", flush=True)
    account_app_token, account_table_id = resolve_feishu_config(args.account_feishu_url)
    account_client = FeishuBitableClient(app_token=account_app_token, table_id=account_table_id)
    account_field_names = ensure_account_nurture_fields(account_client, account_client.list_field_names())
    account_mapping = resolve_table_mapping(account_field_names, ACCOUNT_FIELD_ALIASES)
    account_records = account_client.list_records(page_size=100, limit=args.limit)
    summary["sync_accounts"] = {"accounts_upserted": sync_accounts(account_records, account_mapping, db)}
    summary["disabled_store_overrides"] = apply_disabled_store_overrides(db, args)
    print(f"[run-all] sync_accounts done {summary['sync_accounts']}", flush=True)
    if summary["disabled_store_overrides"]["disabled_stores"]:
        print(f"[run-all] disabled_store_overrides {summary['disabled_store_overrides']}", flush=True)

    print("[run-all] sync_videos start", flush=True)
    run_manager_app_token, run_manager_table_id = resolve_feishu_config(args.run_manager_feishu_url)
    run_manager_client = FeishuBitableClient(app_token=run_manager_app_token, table_id=run_manager_table_id)
    run_manager_field_names = run_manager_client.list_field_names()
    run_manager_mapping = resolve_table_mapping(run_manager_field_names, RUN_MANAGER_FIELD_ALIASES)
    run_manager_records = run_manager_client.list_records(page_size=100, limit=args.limit)
    summary["sync_videos"] = sync_videos(
        run_manager_records,
        run_manager_mapping,
        db,
        download_dir=video_dir,
        client=run_manager_client,
    )
    print(f"[run-all] sync_videos done {summary['sync_videos']}", flush=True)

    print("[run-all] schedule start", flush=True)
    sample_paths = [str(row["local_file_path"] or "") for row in db.list_scheduled_tasks()[:10]]
    ensure_video_storage_ready(video_dir, sample_paths=sample_paths)
    publisher = build_publish_adapter(args)
    print("[run-all] sync_recent_manual_statuses start", flush=True)
    summary["sync_recent_manual_statuses"] = sync_recent_manual_publish_statuses(db, args)
    print(f"[run-all] sync_recent_manual_statuses done {summary['sync_recent_manual_statuses']}", flush=True)
    summary["enforce_retry_limit_before_schedule"] = db.enforce_retry_limit(
        max_auto_retries=args.max_auto_retries,
    )
    if summary["enforce_retry_limit_before_schedule"]["candidates"]:
        print(
            f"[run-all] enforce_retry_limit_before_schedule {summary['enforce_retry_limit_before_schedule']}",
            flush=True,
        )
    summary["schedule"] = schedule_slots(db, publisher).__dict__
    print(f"[run-all] schedule done {summary['schedule']}", flush=True)

    print("[run-all] sync_results start", flush=True)
    summary["sync_results"] = sync_publish_results(db, publisher)
    print(f"[run-all] sync_results done {summary['sync_results']}", flush=True)
    summary["enforce_retry_limit_after_results"] = db.enforce_retry_limit(
        max_auto_retries=args.max_auto_retries,
    )
    if summary["enforce_retry_limit_after_results"]["candidates"]:
        print(
            f"[run-all] enforce_retry_limit_after_results {summary['enforce_retry_limit_after_results']}",
            flush=True,
        )

    print("[run-all] cleanup_videos start", flush=True)
    summary["cleanup_videos"] = db.cleanup_published_videos(
        older_than_days=args.cleanup_published_days,
        base_dir=video_dir,
    )
    print(f"[run-all] cleanup_videos done {summary['cleanup_videos']}", flush=True)

    print("[run-all] sync_report_table start", flush=True)
    report_app_token, report_table_id = resolve_feishu_config(args.report_feishu_url)
    report_client = FeishuBitableClient(app_token=report_app_token, table_id=report_table_id)
    summary["sync_report_table"] = sync_publish_report_table(db, report_client)
    print(f"[run-all] sync_report_table done {summary['sync_report_table']}", flush=True)
    print(summary)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="短视频自动发布系统")
    parser.set_defaults(func=None)
    parser.add_argument("--db-path", default=str(default_db_path()), help="SQLite 路径")
    parser.add_argument("--config-path", default=str(DEFAULT_CONFIG_PATH), help="本地配置文件路径")
    parser.add_argument("--video-dir", default=str(default_video_dir()), help="本地视频目录")

    subparsers = parser.add_subparsers(dest="command")

    sync_script = subparsers.add_parser("sync-script-db", help="从生产脚本宽表拆分脚本主数据并落库")
    sync_script.add_argument("--script-feishu-url", default=DEFAULT_SCRIPT_FEISHU_URL, help="生产脚本表飞书 URL")
    sync_script.add_argument("--product-id", help="只处理指定产品 ID")
    sync_script.add_argument("--record-id", help="只处理指定 record_id")
    sync_script.add_argument("--limit", type=int, help="限制处理数量")
    sync_script.add_argument("--title-mode", choices=["heuristic", "llm", "fallback"], default="fallback")
    sync_script.add_argument("--llm-route", default="auto", help="标题生成 LLM 线路")
    sync_script.set_defaults(func=command_sync_script_db)

    sync_video = subparsers.add_parser("sync-videos", help="从运行管理表同步成功视频并下载落地")
    sync_video.add_argument("--run-manager-feishu-url", default=DEFAULT_RUN_MANAGER_FEISHU_URL, help="运行管理表飞书 URL")
    sync_video.add_argument("--limit", type=int, help="限制处理数量")
    sync_video.set_defaults(func=command_sync_videos)

    sync_account = subparsers.add_parser("sync-accounts", help="同步账号配置表到数据库")
    sync_account.add_argument("--account-feishu-url", default=DEFAULT_ACCOUNT_FEISHU_URL, help="账号配置飞书 URL")
    sync_account.add_argument("--limit", type=int, help="限制处理数量")
    sync_account.set_defaults(func=command_sync_accounts)

    sync_report = subparsers.add_parser("sync-report-table", help="回写发布追踪表")
    sync_report.add_argument("--report-feishu-url", default=DEFAULT_REPORT_FEISHU_URL, help="发布追踪表飞书 URL")
    sync_report.set_defaults(func=command_sync_report_table)

    sync_product_report = subparsers.add_parser("sync-product-publish-report-table", help="同步店铺产品发布汇总表")
    sync_product_report.add_argument("--product-report-feishu-url", default=DEFAULT_PRODUCT_PUBLISH_REPORT_FEISHU_URL, help="店铺产品发布汇总表飞书 URL")
    sync_product_report.add_argument("--script-feishu-url", default=DEFAULT_SCRIPT_FEISHU_URL, help="生产脚本表飞书 URL，用于复制产品主图")
    sync_product_report.add_argument("--date", default="", help="参考日期 YYYY-MM-DD，默认今天；上周按该日期的上一自然周计算")
    sync_product_report.add_argument("--source-image-field", default="", help="源脚本表产品主图字段名，默认自动识别 产品主图/产品图片")
    sync_product_report.add_argument("--no-upload-images", action="store_true", help="不复制产品主图，只同步统计数据")
    sync_product_report.add_argument("--force-upload-images", action="store_true", help="即使目标表已有主图也重新上传")
    sync_product_report.set_defaults(func=command_sync_product_publish_report_table)

    sync_manual_queue = subparsers.add_parser("sync-manual-queue-table", help="同步人工发布清单表")
    sync_manual_queue.add_argument("--manual-queue-feishu-url", default=DEFAULT_MANUAL_QUEUE_FEISHU_URL, help="人工发布清单表飞书 URL")
    sync_manual_queue.add_argument("--date", default="", help="清单日期 YYYY-MM-DD，默认今天")
    sync_manual_queue.add_argument("--include-published", action="store_true", help="包含已发布记录")
    sync_manual_queue.add_argument("--no-upload-videos", action="store_true", help="不上传视频附件，只写本地路径")
    sync_manual_queue.add_argument("--force-upload-videos", action="store_true", help="即使已有附件也强制重新上传")
    sync_manual_queue.set_defaults(func=command_sync_manual_queue_table)

    cleanup_videos = subparsers.add_parser("cleanup-published-videos", help="清理已发布超过保留期的本地视频")
    cleanup_videos.add_argument("--retention-days", type=int, default=60, help="保留天数，默认 60")
    cleanup_videos.set_defaults(func=command_cleanup_published_videos)

    disable_product = subparsers.add_parser("disable-product", help="禁排指定产品并取消本地待发布/已排期任务")
    disable_product.add_argument("--product-id", required=True, help="要禁排的产品ID/商品ID")
    disable_product.add_argument("--reason", default="产品已下架，停止自动发布", help="写入本地状态的原因")
    disable_product.set_defaults(func=command_disable_product)

    disable_account = subparsers.add_parser("disable-account", help="暂停指定账号并取消未来本地排期")
    disable_account.add_argument("--account-id", required=True, help="要暂停的账号ID/envId")
    disable_account.add_argument("--reason", default="账号连续发布失败，暂停自动发布", help="写入本地状态的原因")
    disable_account.set_defaults(func=command_disable_account)

    enforce_retry = subparsers.add_parser("enforce-retry-limit", help="清理超过自动重试上限的历史活跃排期")
    enforce_retry.add_argument("--max-auto-retries", type=int, default=2, help="失败后最多自动重试次数，默认 2")
    enforce_retry.add_argument("--reason", default="", help="写入本地状态的原因")
    enforce_retry.set_defaults(func=command_enforce_retry_limit)

    def add_notification_args(command: argparse.ArgumentParser) -> None:
        command.add_argument("--delivery", choices=["openclaw-bot", "webhook"], default="openclaw-bot", help="发送方式，默认使用 OpenClaw 主飞书机器人")
        command.add_argument("--feishu-webhook-url", default="", help="飞书群机器人 webhook URL；也可写入配置或环境变量")
        command.add_argument("--feishu-chat-id", default="", help="OpenClaw 飞书机器人发送目标群 chat_id")
        command.add_argument("--openclaw-config-path", default=str(DEFAULT_OPENCLAW_CONFIG_PATH), help="OpenClaw 配置文件路径")
        command.add_argument("--openclaw-feishu-account", default="", help="OpenClaw 多机器人账号 key；留空表示主 agent 飞书 app")
        command.add_argument("--dry-run", action="store_true", help="只打印消息，不发送")
        command.add_argument("--force", action="store_true", help="忽略已发送记录，强制再次发送")

    notify_daily = subparsers.add_parser("notify-daily-summary", help="发送前一天发布结果飞书群日报")
    notify_daily.add_argument("--date", default="", help="统计日期 YYYY-MM-DD，默认昨天")
    notify_daily.add_argument("--max-failure-lines", type=int, default=20, help="失败明细最多展开条数，默认 20")
    add_notification_args(notify_daily)
    notify_daily.set_defaults(func=command_notify_daily_summary)

    notify_queue = subparsers.add_parser("notify-manual-queue", help="发送当天人工发布清单")
    notify_queue.add_argument("--date", default="", help="清单日期 YYYY-MM-DD，默认今天")
    notify_queue.add_argument("--max-items", type=int, default=80, help="最多展开条数，默认 80")
    notify_queue.add_argument("--include-published", action="store_true", help="包含已发布记录，默认只看已排期/发布失败")
    add_notification_args(notify_queue)
    notify_queue.set_defaults(func=command_notify_manual_queue)

    notify_ops = subparsers.add_parser("notify-ops-daily", help="发送昨日发布结果 + 今日人工发布清单")
    notify_ops.add_argument("--summary-date", default="", help="结果统计日期 YYYY-MM-DD，默认昨天")
    notify_ops.add_argument("--queue-date", default="", help="人工清单日期 YYYY-MM-DD，默认今天")
    notify_ops.add_argument("--max-failure-lines", type=int, default=20, help="失败明细最多展开条数，默认 20")
    notify_ops.add_argument("--max-items", type=int, default=80, help="人工清单最多展开条数，默认 80")
    notify_ops.add_argument("--include-published", action="store_true", help="人工清单包含已发布记录")
    notify_ops.add_argument("--sync-manual-queue-table", action="store_true", help="发送通知前同步人工发布清单表")
    notify_ops.add_argument("--manual-queue-feishu-url", default="", help="人工发布清单表飞书 URL")
    notify_ops.add_argument("--no-upload-videos", action="store_true", help="同步人工表时不上传视频附件")
    notify_ops.add_argument("--force-upload-videos", action="store_true", help="同步人工表时强制重新上传视频附件")
    add_notification_args(notify_ops)
    notify_ops.set_defaults(func=command_notify_ops_daily)

    schedule = subparsers.add_parser("schedule", help="按 24 小时窗口增量补排")
    schedule.add_argument("--publish-mode", choices=["dry-run", "http", "geelark"], default="dry-run")
    schedule.add_argument("--publish-api-base-url", default="", help="自动发布 API Base URL")
    schedule.add_argument("--publish-api-token", default="", help="自动发布 API Token")
    schedule.add_argument("--geelark-task-add-endpoint", default="https://openapi.geelark.cn/open/v1/task/add", help="GeeLark task/add 接口")
    schedule.add_argument("--geelark-upload-get-url-endpoint", default="https://openapi.geelark.cn/open/v1/upload/getUrl", help="GeeLark 获取上传地址接口")
    schedule.add_argument("--geelark-auth-header", default="Authorization", help="GeeLark 鉴权 header")
    schedule.add_argument("--geelark-auth-scheme", default="Bearer", help="GeeLark 鉴权前缀，例如 Bearer")
    schedule.add_argument("--geelark-plan-name-field", default="planName", help="GeeLark 顶层计划名字段")
    schedule.add_argument("--geelark-remark-field", default="remark", help="GeeLark 顶层备注字段")
    schedule.add_argument("--geelark-task-type-field", default="taskType", help="GeeLark 顶层任务类型字段")
    schedule.add_argument("--geelark-list-field", default="list", help="GeeLark 顶层任务数组字段")
    schedule.add_argument("--geelark-task-type-value", type=int, default=1, help="GeeLark 任务类型值，发布视频=1")
    schedule.add_argument("--geelark-env-id-field", default="envId", help="GeeLark 云手机 ID 字段")
    schedule.add_argument("--geelark-video-field", default="video", help="GeeLark 视频 URL 字段")
    schedule.add_argument("--geelark-schedule-at-field", default="scheduleAt", help="GeeLark 秒级时间戳字段")
    schedule.add_argument("--geelark-video-desc-field", default="videoDesc", help="GeeLark 视频文案字段")
    schedule.add_argument("--geelark-product-id-field", default="productId", help="GeeLark 商品ID字段")
    schedule.add_argument("--geelark-product-title-field", default="productTitle", help="GeeLark 商品标题字段")
    schedule.add_argument("--geelark-ref-video-id-field", default="refVideoId", help="GeeLark 同款视频ID字段")
    schedule.add_argument("--geelark-upload-file-type-field", default="fileType", help="GeeLark 上传接口文件类型字段")
    schedule.add_argument("--geelark-extra-body-json", default="", help='GeeLark 额外请求体 JSON，对象结构支持 {"top_level": {...}, "item": {...}}')
    schedule.add_argument("--geelark-status-endpoint", default="https://openapi.geelark.cn/open/v1/task/query", help="GeeLark 任务状态查询接口")
    schedule.add_argument("--geelark-status-method", choices=["GET", "POST"], default="POST", help="GeeLark 状态查询方法")
    schedule.add_argument("--geelark-status-task-id-field", default="ids", help="GeeLark 状态查询任务ID字段")
    schedule.add_argument("--geelark-task-id-paths", default="data.taskIds.0,taskIds.0,task_id,id,data.task_id,data.id", help="GeeLark 返回体任务ID候选路径，逗号分隔")
    schedule.add_argument("--geelark-upload-url-paths", default="data.uploadUrl,uploadUrl", help="GeeLark 上传地址候选路径，逗号分隔")
    schedule.add_argument("--geelark-resource-url-paths", default="data.resourceUrl,resourceUrl", help="GeeLark 资源地址候选路径，逗号分隔")
    schedule.add_argument("--geelark-status-value-paths", default="data.items.0.status,items.0.status,data.status,status", help="GeeLark 返回体状态候选路径，逗号分隔")
    schedule.add_argument("--geelark-success-values", default="success,published,done,3", help="GeeLark 成功状态值，逗号分隔")
    schedule.add_argument("--geelark-failure-values", default="failed,error,-1,4,5,7", help="GeeLark 失败状态值，逗号分隔")
    schedule.add_argument("--geelark-published-at-paths", default="", help="GeeLark 返回体发布时间候选路径，逗号分隔")
    schedule.add_argument("--geelark-error-message-paths", default="data.items.0.failDesc,items.0.failDesc,data.failDesc,failDesc,message,error_message,data.message,data.error_message", help="GeeLark 返回体错误信息候选路径，逗号分隔")
    schedule.set_defaults(func=command_schedule)

    sync_results = subparsers.add_parser("sync-results", help="同步定时发布结果")
    sync_results.add_argument("--publish-mode", choices=["dry-run", "http", "geelark"], default="dry-run")
    sync_results.add_argument("--publish-api-base-url", default="", help="自动发布 API Base URL")
    sync_results.add_argument("--publish-api-token", default="", help="自动发布 API Token")
    sync_results.add_argument("--geelark-task-add-endpoint", default="https://openapi.geelark.cn/open/v1/task/add", help="GeeLark task/add 接口")
    sync_results.add_argument("--geelark-upload-get-url-endpoint", default="https://openapi.geelark.cn/open/v1/upload/getUrl", help="GeeLark 获取上传地址接口")
    sync_results.add_argument("--geelark-auth-header", default="Authorization", help="GeeLark 鉴权 header")
    sync_results.add_argument("--geelark-auth-scheme", default="Bearer", help="GeeLark 鉴权前缀，例如 Bearer")
    sync_results.add_argument("--geelark-plan-name-field", default="planName", help="GeeLark 顶层计划名字段")
    sync_results.add_argument("--geelark-remark-field", default="remark", help="GeeLark 顶层备注字段")
    sync_results.add_argument("--geelark-task-type-field", default="taskType", help="GeeLark 顶层任务类型字段")
    sync_results.add_argument("--geelark-list-field", default="list", help="GeeLark 顶层任务数组字段")
    sync_results.add_argument("--geelark-task-type-value", type=int, default=1, help="GeeLark 任务类型值，发布视频=1")
    sync_results.add_argument("--geelark-env-id-field", default="envId", help="GeeLark 云手机 ID 字段")
    sync_results.add_argument("--geelark-video-field", default="video", help="GeeLark 视频 URL 字段")
    sync_results.add_argument("--geelark-schedule-at-field", default="scheduleAt", help="GeeLark 秒级时间戳字段")
    sync_results.add_argument("--geelark-video-desc-field", default="videoDesc", help="GeeLark 视频文案字段")
    sync_results.add_argument("--geelark-product-id-field", default="productId", help="GeeLark 商品ID字段")
    sync_results.add_argument("--geelark-product-title-field", default="productTitle", help="GeeLark 商品标题字段")
    sync_results.add_argument("--geelark-ref-video-id-field", default="refVideoId", help="GeeLark 同款视频ID字段")
    sync_results.add_argument("--geelark-upload-file-type-field", default="fileType", help="GeeLark 上传接口文件类型字段")
    sync_results.add_argument("--geelark-extra-body-json", default="", help='GeeLark 额外请求体 JSON，对象结构支持 {"top_level": {...}, "item": {...}}')
    sync_results.add_argument("--geelark-status-endpoint", default="https://openapi.geelark.cn/open/v1/task/query", help="GeeLark 任务状态查询接口")
    sync_results.add_argument("--geelark-status-method", choices=["GET", "POST"], default="POST", help="GeeLark 状态查询方法")
    sync_results.add_argument("--geelark-status-task-id-field", default="ids", help="GeeLark 状态查询任务ID字段")
    sync_results.add_argument("--geelark-task-id-paths", default="data.taskIds.0,taskIds.0,task_id,id,data.task_id,data.id", help="GeeLark 返回体任务ID候选路径，逗号分隔")
    sync_results.add_argument("--geelark-upload-url-paths", default="data.uploadUrl,uploadUrl", help="GeeLark 上传地址候选路径，逗号分隔")
    sync_results.add_argument("--geelark-resource-url-paths", default="data.resourceUrl,resourceUrl", help="GeeLark 资源地址候选路径，逗号分隔")
    sync_results.add_argument("--geelark-status-value-paths", default="data.items.0.status,items.0.status,data.status,status", help="GeeLark 返回体状态候选路径，逗号分隔")
    sync_results.add_argument("--geelark-success-values", default="success,published,done,3", help="GeeLark 成功状态值，逗号分隔")
    sync_results.add_argument("--geelark-failure-values", default="failed,error,-1,4,5,7", help="GeeLark 失败状态值，逗号分隔")
    sync_results.add_argument("--geelark-published-at-paths", default="", help="GeeLark 返回体发布时间候选路径，逗号分隔")
    sync_results.add_argument("--geelark-error-message-paths", default="data.items.0.failDesc,items.0.failDesc,data.failDesc,failDesc,message,error_message,data.message,data.error_message", help="GeeLark 返回体错误信息候选路径，逗号分隔")
    sync_results.set_defaults(func=command_sync_results)

    refresh_titles = subparsers.add_parser("refresh-titles", help="只刷新数据库中已有脚本主数据的短视频标题")
    refresh_titles.add_argument("--limit", type=int, help="限制处理数量")
    refresh_titles.add_argument("--batch-size", type=int, default=20, help="分批写库数量，默认 20")
    refresh_titles.add_argument("--title-mode", choices=["heuristic", "llm", "fallback"], default="fallback")
    refresh_titles.add_argument("--llm-route", default="auto", help="标题生成 LLM 线路")
    refresh_titles.add_argument("--force", action="store_true", help="忽略现有标题状态，强制全部重算")
    refresh_titles.set_defaults(func=command_refresh_titles)

    run_all = subparsers.add_parser("run-all", help="一键执行主数据同步、账号同步、视频同步、排期与结果回写")
    run_all.add_argument("--script-feishu-url", default=DEFAULT_SCRIPT_FEISHU_URL, help="生产脚本表飞书 URL")
    run_all.add_argument("--run-manager-feishu-url", default=DEFAULT_RUN_MANAGER_FEISHU_URL, help="运行管理表飞书 URL")
    run_all.add_argument("--account-feishu-url", default=DEFAULT_ACCOUNT_FEISHU_URL, help="账号配置飞书 URL")
    run_all.add_argument("--report-feishu-url", default=DEFAULT_REPORT_FEISHU_URL, help="发布追踪表飞书 URL")
    run_all.add_argument("--manual-queue-feishu-url", default=DEFAULT_MANUAL_QUEUE_FEISHU_URL, help="人工发布清单表飞书 URL")
    run_all.add_argument("--video-dir", default=str(default_video_dir()), help="本地视频目录")
    run_all.add_argument("--product-id", help="只处理指定产品 ID")
    run_all.add_argument("--record-id", help="只处理指定 record_id")
    run_all.add_argument("--limit", type=int, help="限制处理数量")
    run_all.add_argument("--title-mode", choices=["heuristic", "llm", "fallback"], default="fallback")
    run_all.add_argument("--llm-route", default="auto", help="标题生成 LLM 线路")
    run_all.add_argument("--cleanup-published-days", type=int, default=60, help="已发布本地视频保留天数，默认 60；传 0 可关闭")
    run_all.add_argument("--max-auto-retries", type=int, default=2, help="失败后最多自动重试次数，默认 2")
    run_all.add_argument("--manual-status-lookback-days", type=int, default=3, help="排期前回读最近 N 天人工清单状态，默认 3；传 0 可关闭")
    run_all.add_argument("--publish-mode", choices=["dry-run", "http", "geelark"], default="dry-run")
    run_all.add_argument("--publish-api-base-url", default="", help="自动发布 API Base URL")
    run_all.add_argument("--publish-api-token", default="", help="自动发布 API Token")
    run_all.add_argument("--geelark-task-add-endpoint", default="https://openapi.geelark.cn/open/v1/task/add", help="GeeLark task/add 接口")
    run_all.add_argument("--geelark-upload-get-url-endpoint", default="https://openapi.geelark.cn/open/v1/upload/getUrl", help="GeeLark 获取上传地址接口")
    run_all.add_argument("--geelark-auth-header", default="Authorization", help="GeeLark 鉴权 header")
    run_all.add_argument("--geelark-auth-scheme", default="Bearer", help="GeeLark 鉴权前缀，例如 Bearer")
    run_all.add_argument("--geelark-plan-name-field", default="planName", help="GeeLark 顶层计划名字段")
    run_all.add_argument("--geelark-remark-field", default="remark", help="GeeLark 顶层备注字段")
    run_all.add_argument("--geelark-task-type-field", default="taskType", help="GeeLark 顶层任务类型字段")
    run_all.add_argument("--geelark-list-field", default="list", help="GeeLark 顶层任务数组字段")
    run_all.add_argument("--geelark-task-type-value", type=int, default=1, help="GeeLark 任务类型值，发布视频=1")
    run_all.add_argument("--geelark-env-id-field", default="envId", help="GeeLark 云手机 ID 字段")
    run_all.add_argument("--geelark-video-field", default="video", help="GeeLark 视频 URL 字段")
    run_all.add_argument("--geelark-schedule-at-field", default="scheduleAt", help="GeeLark 秒级时间戳字段")
    run_all.add_argument("--geelark-video-desc-field", default="videoDesc", help="GeeLark 视频文案字段")
    run_all.add_argument("--geelark-product-id-field", default="productId", help="GeeLark 商品ID字段")
    run_all.add_argument("--geelark-product-title-field", default="productTitle", help="GeeLark 商品标题字段")
    run_all.add_argument("--geelark-ref-video-id-field", default="refVideoId", help="GeeLark 同款视频ID字段")
    run_all.add_argument("--geelark-upload-file-type-field", default="fileType", help="GeeLark 上传接口文件类型字段")
    run_all.add_argument("--geelark-extra-body-json", default="", help='GeeLark 额外请求体 JSON，对象结构支持 {"top_level": {...}, "item": {...}}')
    run_all.add_argument("--geelark-status-endpoint", default="https://openapi.geelark.cn/open/v1/task/query", help="GeeLark 任务状态查询接口")
    run_all.add_argument("--geelark-status-method", choices=["GET", "POST"], default="POST", help="GeeLark 状态查询方法")
    run_all.add_argument("--geelark-status-task-id-field", default="ids", help="GeeLark 状态查询任务ID字段")
    run_all.add_argument("--geelark-task-id-paths", default="data.taskIds.0,taskIds.0,task_id,id,data.task_id,data.id", help="GeeLark 返回体任务ID候选路径，逗号分隔")
    run_all.add_argument("--geelark-upload-url-paths", default="data.uploadUrl,uploadUrl", help="GeeLark 上传地址候选路径，逗号分隔")
    run_all.add_argument("--geelark-resource-url-paths", default="data.resourceUrl,resourceUrl", help="GeeLark 资源地址候选路径，逗号分隔")
    run_all.add_argument("--geelark-status-value-paths", default="data.items.0.status,items.0.status,data.status,status", help="GeeLark 返回体状态候选路径，逗号分隔")
    run_all.add_argument("--geelark-success-values", default="success,published,done,3", help="GeeLark 成功状态值，逗号分隔")
    run_all.add_argument("--geelark-failure-values", default="failed,error,-1,4,5,7", help="GeeLark 失败状态值，逗号分隔")
    run_all.add_argument("--geelark-published-at-paths", default="", help="GeeLark 返回体发布时间候选路径，逗号分隔")
    run_all.add_argument("--geelark-error-message-paths", default="data.items.0.failDesc,items.0.failDesc,data.failDesc,failDesc,message,error_message,data.message,data.error_message", help="GeeLark 返回体错误信息候选路径，逗号分隔")
    run_all.set_defaults(func=command_run_all)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.func is None:
        parser.print_help()
        return
    args.func(args)


if __name__ == "__main__":
    main()
