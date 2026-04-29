#!/usr/bin/env python3
"""短视频自动发布系统入口。"""

from __future__ import annotations

import argparse
import json
import os
import sys
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
from app.publishers import DryRunPublishAdapter, GeeLarkPublishAdapter, HttpPublishAdapter  # noqa: E402
from app.reporting import sync_publish_report_table  # noqa: E402
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
DEFAULT_CONFIG_PATH = Path("/Users/likeu3/.openclaw/shared/data/short_video_auto_publisher_config.json")


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
    print({"accounts_upserted": count})


def command_schedule(args: argparse.Namespace) -> None:
    video_dir = ensure_video_storage_ready(args.video_dir)
    db = AutoPublishDB(Path(args.db_path))
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


def command_cleanup_published_videos(args: argparse.Namespace) -> None:
    ensure_video_storage_ready(args.video_dir)
    db = AutoPublishDB(Path(args.db_path))
    stats = db.cleanup_published_videos(
        older_than_days=args.retention_days,
        base_dir=Path(args.video_dir),
    )
    print(stats)


def command_run_all(args: argparse.Namespace) -> None:
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
    print(f"[run-all] sync_accounts done {summary['sync_accounts']}", flush=True)

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
    summary["schedule"] = schedule_slots(db, publisher).__dict__
    print(f"[run-all] schedule done {summary['schedule']}", flush=True)

    print("[run-all] sync_results start", flush=True)
    summary["sync_results"] = sync_publish_results(db, publisher)
    print(f"[run-all] sync_results done {summary['sync_results']}", flush=True)

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

    cleanup_videos = subparsers.add_parser("cleanup-published-videos", help="清理已发布超过保留期的本地视频")
    cleanup_videos.add_argument("--retention-days", type=int, default=60, help="保留天数，默认 60")
    cleanup_videos.set_defaults(func=command_cleanup_published_videos)

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
    run_all.add_argument("--video-dir", default=str(default_video_dir()), help="本地视频目录")
    run_all.add_argument("--product-id", help="只处理指定产品 ID")
    run_all.add_argument("--record-id", help="只处理指定 record_id")
    run_all.add_argument("--limit", type=int, help="限制处理数量")
    run_all.add_argument("--title-mode", choices=["heuristic", "llm", "fallback"], default="fallback")
    run_all.add_argument("--llm-route", default="auto", help="标题生成 LLM 线路")
    run_all.add_argument("--cleanup-published-days", type=int, default=60, help="已发布本地视频保留天数，默认 60；传 0 可关闭")
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
