#!/usr/bin/env python3
"""Run row-based original batches from the Feishu operation task table."""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import traceback
from pathlib import Path

SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from core.bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # noqa: E402
from core.feishu_url_parser import parse_feishu_bitable_url  # noqa: E402
from core.original_batch_executor import run_plan_only, run_script_only  # noqa: E402
from core.original_batch_models import BatchRequest  # noqa: E402
from core.original_batch_storage import BatchStorage  # noqa: E402
from core.production_script_feishu import (  # noqa: E402
    OPERATION_TASK_FIELD_RENAMES,
    OPERATION_TASK_FIELD_NAMES,
    OPERATION_TASK_FIELDS,
    OPERATION_TASK_STATUS_OPTIONS,
    PRODUCT_TYPE_OPTIONS,
    PRODUCTION_SCRIPT_FIELDS,
    TEST_PHASE_OPTIONS,
    TOP_CATEGORY_OPTIONS,
    ensure_fields,
    ensure_single_select_options,
    export_ready_batch,
    now_millis,
    operation_record_values,
    rename_known_fields,
    transfer_attachments,
)

DEFAULT_OPERATION_URL = (
    "https://gcngopvfvo0q.feishu.cn/wiki/"
    "RJxHw0uAkiJPkSkXvMvcq3hXn5B?table=tblr8C7uvGIPBQar&view=vewWEsmd4q"
)
DEFAULT_SCRIPT_URL = (
    "https://gcngopvfvo0q.feishu.cn/wiki/"
    "KsX7w8Y8ZiJfnsk2Mtvc7xLun1f?table=tblIvHJ0nsn9WCwi&view=vewKfXc8lj"
)


def _client(url: str) -> FeishuBitableClient:
    info = parse_feishu_bitable_url(url)
    if not info:
        raise ValueError(f"无法解析飞书链接: {url}")
    token = info.app_token
    if "/wiki/" in url:
        token = resolve_wiki_bitable_app_token(token)
    return FeishuBitableClient(token, info.table_id)


def _request_id(record_id: str, task: dict) -> str:
    material = "|".join(
        [
            record_id,
            str(task.get("task_id") or ""),
            str(task.get("product_code") or ""),
            str(task.get("random_seed") or 0),
            str(task.get("test_phase") or "INITIAL"),
            "simplified_v1",
        ]
    )
    return "OP_FEISHU_" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:20].upper()


def _update(client: FeishuBitableClient, record_id: str, **values) -> None:
    names = OPERATION_TASK_FIELD_NAMES
    payload = {
        names[key]: value
        for key, value in values.items()
        if key in names and value is not None
    }
    client.update_record_fields(record_id, payload)


def _validate_task(task: dict) -> None:
    if not str(task.get("product_code") or "").strip():
        raise ValueError("缺少产品编码")
    count = int(task.get("requested_count") or 0)
    if count < 1 or count > 20:
        raise ValueError("生成数量必须在1到20之间")
    duration = float(task.get("duration_seconds") or 0)
    if duration <= 0:
        raise ValueError("视频时长必须大于0")
    if task.get("top_category") not in TOP_CATEGORY_OPTIONS:
        raise ValueError("一级类目必须从飞书枚举中选择：女装 / 配饰")
    if task.get("product_type") not in PRODUCT_TYPE_OPTIONS:
        raise ValueError("产品类型未命中系统枚举，请从飞书下拉框选择")


def _sync_confirmed_selling_points(
    *,
    product_code: str,
    voiceover_root: str,
) -> dict:
    """Run the central voiceover engine's official idempotent Feishu sync."""
    root = Path(voiceover_root).expanduser().resolve()
    script = root / "scripts" / "sync_feishu_product_claims.py"
    db_path = root / "var" / "voiceover.sqlite"
    if not script.exists():
        raise RuntimeError(f"中央卖点同步程序不存在: {script}")
    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "--apply",
            "--product-code",
            product_code,
            "--db-path",
            str(db_path),
        ],
        cwd=str(root),
        text=True,
        capture_output=True,
        timeout=240,
        check=False,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "unknown error").strip()
        raise RuntimeError(f"中央卖点前置同步失败: {detail[:1600]}")
    try:
        report = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"中央卖点同步返回不可解析: {completed.stdout[:800]}"
        ) from exc
    sync = report.get("sync") if isinstance(report.get("sync"), dict) else {}
    print(
        "中央卖点同步: "
        f"eligible_rows={sync.get('eligible_rows', 0)}, "
        f"confirmed_segments={sync.get('confirmed_segments', 0)}, "
        f"mapped_segments={sync.get('mapped_segments', 0)}, "
        f"unmapped_segments={sync.get('unmapped_segments', 0)}, "
        f"verified_claims={sync.get('verified_claims', 0)}, "
        f"unresolved_claims={sync.get('unresolved_claims', 0)}"
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="短视频运营任务表 -> 原创视频生产脚本")
    parser.add_argument("--operation-url", default=DEFAULT_OPERATION_URL)
    parser.add_argument("--script-url", default=DEFAULT_SCRIPT_URL)
    parser.add_argument("--record-id")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--plan-only", action="store_true")
    parser.add_argument(
        "--resume-failed",
        action="store_true",
        help="仅配合 --record-id 显式续跑失败或部分完成任务中的失败条目",
    )
    parser.add_argument(
        "--replan",
        action="store_true",
        help="仅配合 --record-id，重新同步卖点并生成新策略版本批次",
    )
    parser.add_argument("--delay-between-items", type=int, default=2)
    parser.add_argument("--voiceover-root", default="/Users/likeu3/voiceover_copy_engine")
    parser.add_argument(
        "--voiceover-model-command",
        default="python3 /Users/likeu3/voiceover_copy_engine/scripts/codex_model_command.py",
    )
    parser.add_argument("--blueprint-model", default="gpt-5.6-sol")
    parser.add_argument("--blueprint-reasoning", default="high")
    args = parser.parse_args()
    if args.resume_failed and not args.record_id:
        parser.error("--resume-failed 必须与 --record-id 一起使用")
    if args.replan and not args.record_id:
        parser.error("--replan 必须与 --record-id 一起使用")

    operation_client = _client(args.operation_url)
    script_client = _client(args.script_url)
    rename_known_fields(operation_client, OPERATION_TASK_FIELD_RENAMES)
    ensure_single_select_options(
        operation_client,
        {
            "一级类目（需填写）": TOP_CATEGORY_OPTIONS,
            "产品类型（需填写）": PRODUCT_TYPE_OPTIONS,
            "测试阶段（可选，默认初测）": TEST_PHASE_OPTIONS,
            "任务状态（需填写，仅选择待执行）": OPERATION_TASK_STATUS_OPTIONS,
        },
    )
    ensure_fields(operation_client, primary_field_name="任务ID", specs=OPERATION_TASK_FIELDS)
    ensure_fields(script_client, primary_field_name="脚本ID", specs=PRODUCTION_SCRIPT_FIELDS)

    candidates = []
    for record in operation_client.list_records(page_size=100):
        if args.record_id and record.record_id != args.record_id:
            continue
        task = operation_record_values(record)
        can_resume_failed = (
            args.resume_failed
            and bool(args.record_id)
            and task["status"] in {"失败", "部分完成"}
        )
        can_replan = (
            args.replan
            and bool(args.record_id)
            and task["status"] in {"失败", "部分完成", "已完成"}
        )
        if task["status"] != "待执行" and not can_resume_failed and not can_replan:
            continue
        candidates.append((record, task))
        if args.limit and len(candidates) >= args.limit:
            break

    print(f"待执行运营任务: {len(candidates)}")
    had_failures = False
    for record, task in candidates:
        print(
            f"- {record.record_id} | {task['product_code']} × {task['requested_count']} "
            f"| {task['test_phase']} | {task['duration_seconds']:g}s"
        )
    if args.dry_run:
        return 0

    runtime_root = Path.home() / ".openclaw" / "shared" / "data" / "original_production_runs"
    runtime_root.mkdir(parents=True, exist_ok=True)
    storage = BatchStorage()
    storage.ensure_schema()

    for record, task in candidates:
        try:
            _validate_task(task)
            task_id = task["task_id"] or record.record_id
            output_dir = runtime_root / task_id.replace("/", "_")
            output_dir.mkdir(parents=True, exist_ok=True)
            voiceover_db = str(output_dir / "voiceover.sqlite3")

            batch = (
                storage.get_batch(task["batch_id"])
                if task["batch_id"] and not args.replan
                else None
            )
            if batch:
                items = storage.get_items(batch.batch_id)
            else:
                _update(
                    operation_client,
                    record.record_id,
                    status="执行中-规划",
                    error="",
                    last_run_at=now_millis(),
                )
                _sync_confirmed_selling_points(
                    product_code=task["product_code"],
                    voiceover_root=args.voiceover_root,
                )
                request = BatchRequest(
                    request_id=_request_id(record.record_id, task),
                    product_code=task["product_code"],
                    requested_count=task["requested_count"],
                    test_phase=task["test_phase"],
                    duration_seconds=task["duration_seconds"],
                    execution_mode="PLAN_ONLY",
                    random_seed=task["random_seed"],
                    target_country=task["target_country"],
                    target_language=task["target_language"],
                    top_category=task["top_category"],
                    product_type=task["product_type"],
                    source_record_id=record.record_id,
                    script_mode="simplified_v1",
                )
                batch, items, _ = run_plan_only(
                    request,
                    output_dir=str(output_dir),
                    voiceover_root=args.voiceover_root,
                    voiceover_db_path=voiceover_db,
                )
                _update(
                    operation_client,
                    record.record_id,
                    batch_id=batch.batch_id,
                    planned_count=batch.planned_count,
                    status="执行中-规划" if args.plan_only else "执行中-脚本生成",
                    last_run_at=now_millis(),
                )

            if args.plan_only:
                print(f"规划完成: {batch.batch_id} | planned={batch.planned_count}")
                continue

            _update(
                operation_client,
                record.record_id,
                status="执行中-脚本生成",
                last_run_at=now_millis(),
            )
            batch, items = run_script_only(
                batch.batch_id,
                resume=True,
                script_mode="simplified_v1",
                delay_between_items=args.delay_between_items,
                voiceover_root=args.voiceover_root,
                voiceover_db_path=voiceover_db,
                voiceover_model_command=args.voiceover_model_command,
                blueprint_model=args.blueprint_model,
                blueprint_reasoning=args.blueprint_reasoning,
            )

            transferred_images = transfer_attachments(
                operation_client,
                script_client,
                task["product_images"],
            ) if task["product_images"] else []
            export_summary = export_ready_batch(
                batch=batch,
                items=items,
                target_client=script_client,
                product_images=transferred_images,
                store_id=task["store_id"],
            )
            final_status = "已完成"
            if batch.ready_count < batch.planned_count or batch.planned_count < batch.requested_count:
                final_status = "部分完成"
            try:
                input_snapshot = json.loads(batch.input_snapshot_json or "{}")
            except (TypeError, json.JSONDecodeError):
                input_snapshot = {}
            try:
                allocation_summary = json.loads(batch.allocation_summary_json or "{}")
            except (TypeError, json.JSONDecodeError):
                allocation_summary = {}
            selling_sources = input_snapshot.get("selling_point_catalog_sources") or {}
            selling_distribution = allocation_summary.get("selling_argument_distribution") or {}
            summary = (
                f"请求{batch.requested_count}条；计划{batch.planned_count}条；"
                f"完成{batch.ready_count}条；失败{batch.failed_count}条；"
                f"人工确认卖点{selling_sources.get('central_confirmed_count', 0)}个；"
                f"可用{selling_sources.get('central_available_count', 0)}个；"
                f"已映射{selling_sources.get('central_mapped_count', 0)}个；"
                f"未映射{selling_sources.get('central_unmapped_count', 0)}个；"
                f"本批次使用{len(selling_distribution)}个；"
                f"脚本表新增{export_summary['created']}条、更新{export_summary['updated']}条"
            )
            _update(
                operation_client,
                record.record_id,
                status=final_status,
                batch_id=batch.batch_id,
                planned_count=batch.planned_count,
                ready_count=batch.ready_count,
                failed_count=batch.failed_count,
                summary=summary,
                error="",
                last_run_at=now_millis(),
            )
            print(f"完成: {task_id} | {summary}")
        except Exception as exc:
            had_failures = True
            _update(
                operation_client,
                record.record_id,
                status="失败",
                error=str(exc)[:1800],
                last_run_at=now_millis(),
            )
            print(f"失败: {record.record_id}: {exc}", file=sys.stderr)
            traceback.print_exc()
    return 1 if had_failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
