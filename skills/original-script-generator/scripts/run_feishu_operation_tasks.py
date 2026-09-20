#!/usr/bin/env python3
"""Run row-based original batches from the Feishu operation task table."""
from __future__ import annotations

import argparse
import atexit
import fcntl
import hashlib
import json
import os
import signal
import subprocess
import sys
import traceback
from pathlib import Path

SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from core.bitable import FeishuBitableClient, resolve_wiki_bitable_app_token  # noqa: E402
from core.accessory_mixed_templates import (  # noqa: E402
    ACCESSORY_MIXED_TEMPLATE_ENV,
)
from core.selling_fact_evidence import SELLING_FACT_EVIDENCE_ENV  # noqa: E402
from core.mixed_mainline_contract import MAINLINE_CONTRACT_ENV  # noqa: E402
from core.feishu_url_parser import parse_feishu_bitable_url  # noqa: E402
from core.original_batch_executor import (  # noqa: E402
    load_product_context,
    run_plan_only,
    run_script_only,
)
from core.original_batch_models import BatchRequest  # noqa: E402
from core.original_batch_storage import BatchStorage  # noqa: E402
from core.operation_product_bootstrap import build_operation_product_context  # noqa: E402
from core.longform.feishu_workbench import (  # noqa: E402
    build_longform_text_batch,
    export_longform_projections,
    longform_batch_id,
    load_longform_source_snapshot,
    SOURCE_POLICY_VERSION,
    task_input_snapshot,
)
from core.longform.storage import LongformStorage  # noqa: E402
from core.longform.model import DEFAULT_BLUEPRINT_MODEL as LONGFORM_BLUEPRINT_MODEL  # noqa: E402
from core.longform.source_adapter import source_from_product_plan  # noqa: E402
from core.production_script_feishu import (  # noqa: E402
    OPERATION_TASK_FIELD_RENAMES,
    OPERATION_TASK_FIELD_NAMES,
    OPERATION_TASK_FIELDS,
    OPERATION_TASK_STATUS_OPTIONS,
    PRODUCT_TYPE_OPTIONS,
    PRODUCTION_SCRIPT_FIELDS,
    LONGFORM_SCENE_MODE_OPTIONS,
    TEST_PHASE_OPTIONS,
    TOP_CATEGORY_OPTIONS,
    VIDEO_SPEC_OPTIONS,
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
DEFAULT_RUN_LOCK_PATH = (
    Path.home() / ".openclaw" / "shared" / "locks" / "original-script-production.lock"
)


class RunInterrupted(RuntimeError):
    """Raised when the scheduler asks the active task to stop gracefully."""


def _acquire_process_lock(lock_path: Path = DEFAULT_RUN_LOCK_PATH):
    """Acquire the production-wide lock without waiting or triggering retries."""
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        return None
    handle.seek(0)
    handle.truncate()
    handle.write(f"pid={os.getpid()}\n")
    handle.flush()
    return handle


def _install_interrupt_handler() -> None:
    """Turn scheduler shutdown signals into a recoverable task interruption.

    The caller catches :class:`RunInterrupted` around the active Feishu record
    and returns it to ``待执行``.  This keeps a gateway restart from leaving a
    row permanently labelled as executing when no worker remains alive.
    """

    def _handle_interrupt(signum: int, _frame: object) -> None:
        try:
            signal_name = signal.Signals(signum).name
        except ValueError:
            signal_name = f"signal {signum}"
        raise RunInterrupted(f"收到 {signal_name}")

    for signal_number in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(signal_number, _handle_interrupt)


def _interruption_error(exc: BaseException) -> str:
    detail = str(exc).strip() or "调度器提前终止"
    return f"运行被中断，可重试：{detail}"[:1800]


def _enable_production_category_extensions() -> None:
    """Enable registered accessory adapters on the official workbench path.

    Unsupported products, including apparel, still compile an empty extension;
    this only prevents a recognised scarf/headscarf task from silently losing
    its physical execution contract because a shell-level feature flag was not
    exported.

    The second switch turns on the authored accessory mixed-display templates
    (AMX_A/B/C) for *new* plans.  It is scoped inside the code, not here: only a
    15-second short-video original for an accessory physical subtype compiles a
    mixed contract, and every other branch -- apparel, long-form, remake,
    resumed legacy plans -- is refused by ``mixed_scope_decision`` and keeps its
    exact previous behaviour.  Enabling it where the other adapter switch is
    enabled is what makes it part of the official workbench path instead of a
    flag someone has to remember to export.
    """

    os.environ.setdefault("ORIGINAL_SCRIPT_ACCESSORY_PROFILE_ENABLED", "1")
    os.environ.setdefault(ACCESSORY_MIXED_TEMPLATE_ENV, "1")
    # The per-argument fact-evidence record (plan §4 / C1) is produced either
    # way; this switch is what lets a *blocking* verdict keep a candidate out of
    # planning.  Only two verdicts block -- "the theme depends on a part the
    # confirmed structure says is absent" and "the candidate itself declares a
    # required part that is still unconfirmed".  Nothing on the current catalog
    # declares a required part, so enabling it cannot silently shrink a plan;
    # it only refuses an argument that contradicts a confirmed structure.
    os.environ.setdefault(SELLING_FACT_EVIDENCE_ENV, "1")
    # C2: freeze one mainline (观众问题／核心价值／事实依据／可见回答／表达边界) per film
    # and let it carry the observation tasks.  It is a *planning-time* switch: a
    # frozen package that has no mainline keeps its previous behaviour, so turning
    # this on never rewrites an existing plan.
    os.environ.setdefault(MAINLINE_CONTRACT_ENV, "1")


def _client(url: str) -> FeishuBitableClient:
    info = parse_feishu_bitable_url(url)
    if not info:
        raise ValueError(f"无法解析飞书链接: {url}")
    token = info.app_token
    if "/wiki/" in url:
        token = resolve_wiki_bitable_app_token(token)
    return FeishuBitableClient(token, info.table_id)


def _request_id(record_id: str, task: dict, *, replan: bool = False) -> str:
    parts = [
        record_id,
        str(task.get("task_id") or ""),
        str(task.get("product_code") or ""),
        str(task.get("random_seed") or 0),
        str(task.get("test_phase") or "INITIAL"),
        str(task.get("video_spec") or "15秒原创"),
        str(task.get("longform_scene_mode") or "auto"),
        "simplified_v1",
    ]
    if replan:
        parts.extend(
            [
                "replan-v2-selling-snapshot",
                str(task.get("batch_id") or "NO_PREVIOUS_BATCH"),
            ]
        )
    material = "|".join(parts)
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
    if task.get("video_spec") not in VIDEO_SPEC_OPTIONS:
        raise ValueError("视频规格必须从飞书枚举中选择")
    if task.get("video_format") == "LONGFORM":
        if int(duration) not in {20, 25, 30, 35, 40, 45}:
            raise ValueError("长视频首版仅支持20/25/30/35/40/45秒")
        if count > 3:
            raise ValueError("长视频单个运营任务首版最多生成3条")
    if task.get("top_category") not in TOP_CATEGORY_OPTIONS:
        raise ValueError("一级类目必须从飞书枚举中选择：女装 / 配饰")
    if task.get("product_type") not in PRODUCT_TYPE_OPTIONS:
        raise ValueError("产品类型未命中系统枚举，请从飞书下拉框选择")


def _batch_summary_text(batch, export_summary: dict) -> str:
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
    return (
        f"请求{batch.requested_count}条；计划{batch.planned_count}条；"
        f"完成{batch.ready_count}条；失败{batch.failed_count}条；"
        f"人工确认卖点{selling_sources.get('central_confirmed_count', 0)}个；"
        f"可用{selling_sources.get('central_available_count', 0)}个；"
        f"已映射{selling_sources.get('central_mapped_count', 0)}个；"
        f"未映射{selling_sources.get('central_unmapped_count', 0)}个；"
        f"本批次使用{len(selling_distribution)}个；"
        f"脚本表新增{export_summary.get('created', 0)}条、"
        f"更新{export_summary.get('updated', 0)}条、"
        f"跳过{export_summary.get('skipped', 0)}条"
    )


def _operation_status_for_batch(batch) -> str:
    if int(batch.ready_count or 0) >= int(batch.planned_count or 0) and int(batch.planned_count or 0) > 0:
        return "已完成"
    if int(batch.ready_count or 0) > 0:
        return "部分完成"
    return "失败"


def _export_ready_and_update_operation(
    *,
    operation_client: FeishuBitableClient,
    script_client: FeishuBitableClient,
    record_id: str,
    task: dict,
    batch,
    items,
    storage=None,
) -> dict:
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
        storage=storage,
    )
    final_status = _operation_status_for_batch(batch)
    summary = _batch_summary_text(batch, export_summary)
    error = "" if final_status == "已完成" else (
        f"部分完成：ready={batch.ready_count}, failed={batch.failed_count}, "
        f"planned={batch.planned_count}"
    )
    _update(
        operation_client,
        record_id,
        status=final_status,
        batch_id=batch.batch_id,
        planned_count=batch.planned_count,
        ready_count=batch.ready_count,
        failed_count=batch.failed_count,
        summary=summary,
        error=error,
        last_run_at=now_millis(),
    )
    return export_summary


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
            "--storage-mode",
            "rds",
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


def _load_or_bootstrap_operation_context(
    *,
    operation_client: FeishuBitableClient,
    task: dict,
    record_id: str,
    output_dir: Path,
    voiceover_root: str,
) -> dict:
    """Resolve the same product authority for short and long-form planning."""

    try:
        return load_product_context(
            task["product_code"],
            target_country=task["target_country"],
            target_language=task["target_language"],
            top_category=task["top_category"],
            product_type=task["product_type"],
            voiceover_root=voiceover_root,
            allow_missing_structure_route=True,
        )
    except RuntimeError as exc:
        missing_context_markers = (
            "找不到产品",
            "只有 stage0 测试记录",
            "存在正式生产 run，但缺少 anchor_card",
        )
        if not any(marker in str(exc) for marker in missing_context_markers):
            raise
        print("新 SKU 无历史锚点，使用运营任务产品图建立 P1 锚点卡")
        return build_operation_product_context(
            operation_client=operation_client,
            task=task,
            record_id=record_id,
            output_dir=output_dir,
            voiceover_root=voiceover_root,
        )


def _build_direct_longform_sources(
    *,
    operation_client: FeishuBitableClient,
    task: dict,
    record_id: str,
    output_dir: Path,
    voiceover_root: str,
    voiceover_db_path: str,
    replan: bool,
    longform_batch_id_value: str = "",
) -> list[dict]:
    """Use the stable planner without generating a hidden 15-second script."""

    if task.get("product_images"):
        product_context = build_operation_product_context(
            operation_client=operation_client, task=task, record_id=record_id,
            output_dir=output_dir, voiceover_root=voiceover_root,
            require_current_references=True,
        )
    else:
        product_context = _load_or_bootstrap_operation_context(
            operation_client=operation_client, task=task, record_id=record_id,
            output_dir=output_dir, voiceover_root=voiceover_root,
        )
    product_context["longform_outfit_color_matching"] = True
    product_context["longform_prefer_persona_pack"] = True
    material = "|".join((
        longform_batch_id_value or _request_id(record_id, task, replan=replan),
        json.dumps(task_input_snapshot(task), ensure_ascii=False, sort_keys=True),
        str(product_context.get("input_hash") or ""),
        SOURCE_POLICY_VERSION,
    ))
    source_request = BatchRequest(
        request_id="OP_LF_SOURCE_" + hashlib.sha256(
            material.encode("utf-8")
        ).hexdigest()[:20].upper(),
        product_code=task["product_code"],
        requested_count=task["requested_count"],
        test_phase=task["test_phase"],
        # The shared planner selects product/structure/creative assets.  The
        # long-form compiler owns the real duration and segment count.
        duration_seconds=15.0,
        execution_mode="PLAN_ONLY",
        random_seed=task["random_seed"],
        target_country=task["target_country"],
        target_language=task["target_language"],
        top_category=task["top_category"],
        product_type=task["product_type"],
        source_record_id=record_id,
        script_mode="simplified_v1",
    )
    source_batch, source_items, _ = run_plan_only(
        source_request,
        output_dir=str(output_dir),
        voiceover_root=voiceover_root,
        voiceover_db_path=voiceover_db_path,
        product_context_override=product_context,
    )
    if not source_items:
        raise RuntimeError("DIRECT_PRODUCT_PLAN 没有形成任何可用冻结方向")
    sources = []
    for item in source_items[: int(task["requested_count"])]:
        frozen = json.loads(item.frozen_direction_package_json or "{}")
        persona_assets = _cache_longform_persona_references(
            operation_client, frozen, output_dir / "persona_references",
        )
        sources.append({
            "source_reference_id": item.batch_item_id,
            "source_plan_item_id": item.batch_item_id,
            "source_plan_batch_id": source_batch.batch_id,
            "product_context": product_context,
            "frozen_package": frozen,
            "persona_reference_assets": persona_assets,
            "source": source_from_product_plan(
                product_context,
                frozen,
                duration_seconds=int(task["duration_seconds"]),
                product_code=task["product_code"],
                source_plan_item_id=item.batch_item_id,
            ),
        })
    return sources


def _cache_longform_persona_references(client: FeishuBitableClient, frozen: dict,
                                      root: Path) -> list[dict]:
    """Reuse the existing reference downloader; never borrow a product model's face."""
    from scripts.run_first_frame_tasks import _download_references
    diversity = (frozen.get("simplified_creative_seed") or {}).get("diversity_context") or {}
    creative = frozen.get("creative_diversity_contract") or {}
    persona = (diversity.get("persona_selection_contract")
               or frozen.get("persona_selection_contract")
               or creative.get("persona_selection_contract") or {})
    references = persona.get("reference_images") or []
    if not references:
        return []
    reference_key = hashlib.sha256(json.dumps(
        {"version": "persona-cache-v2-roles", "references": references}, sort_keys=True, default=str,
    ).encode()).hexdigest()[:20]
    target = root / reference_key
    target.mkdir(parents=True, exist_ok=True)
    manifest = target / "references.json"
    if manifest.exists():
        assets = json.loads(manifest.read_text(encoding="utf-8"))
        if assets and all(Path(item["local_path"]).is_file() and hashlib.sha256(
            Path(item["local_path"]).read_bytes()).hexdigest() == item["sha256"] for item in assets):
            return assets
    assets = []
    # Resolve individually: a skipped attachment must not shift the role of
    # every following image. Pack view/primary metadata survives freezing.
    for reference in references:
        if not isinstance(reference, dict) or reference.get("approved") is False:
            continue
        paths = _download_references(client, [reference], target, cache_dir=target / "cache")
        for path in paths:
            digest = hashlib.sha256(Path(path).read_bytes()).hexdigest()
            if reference.get("sha256") and digest != reference["sha256"]:
                raise ValueError("PERSONA_REFERENCE_CONTENT_CHANGED: 人物参考图指纹改变，请刷新人物配置")
            assets.append({"role": "PERSONA_REFERENCE", "local_path": str(path),
                           "sha256": digest, "persona_id": persona.get("persona_id", ""),
                           "reference_view": reference.get("role", ""),
                           "is_primary": bool(reference.get("is_primary")),
                           "source_asset_id": reference.get("file_token") or reference.get("asset_id", "")})
    manifest.write_text(json.dumps(assets, ensure_ascii=False), encoding="utf-8")
    return assets


def main() -> int:
    _enable_production_category_extensions()
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
    parser.add_argument(
        "--export-ready-only",
        action="store_true",
        help="仅配合 --record-id，不生成不规划，只导出当前批次中已SCRIPT_READY的脚本",
    )
    parser.add_argument("--delay-between-items", type=int, default=2)
    parser.add_argument(
        "--item-timeout-seconds",
        type=int,
        default=420,
        help="单条脚本最大等待秒数；0 表示关闭 item 级超时",
    )
    parser.add_argument("--voiceover-root", default="/Users/likeu3/voiceover_copy_engine")
    parser.add_argument(
        "--voiceover-model-command",
        default="python3 /Users/likeu3/voiceover_copy_engine/scripts/codex_model_command.py",
    )
    parser.add_argument("--blueprint-model", default=None,
                        help="显式覆盖；默认长视频 Astra/high，15秒原创 Sol/high")
    parser.add_argument("--blueprint-reasoning", default="high")
    args = parser.parse_args()
    if args.resume_failed and not args.record_id:
        parser.error("--resume-failed 必须与 --record-id 一起使用")
    if args.replan and not args.record_id:
        parser.error("--replan 必须与 --record-id 一起使用")
    if args.export_ready_only and not args.record_id:
        parser.error("--export-ready-only 必须与 --record-id 一起使用")

    run_lock = None
    if not args.dry_run:
        run_lock = _acquire_process_lock()
        if run_lock is None:
            print("SKIPPED_LOCKED: 已有原创脚本生产任务运行，本次不重复启动")
            return 0
        atexit.register(run_lock.close)

    _install_interrupt_handler()

    operation_client = _client(args.operation_url)
    script_client = _client(args.script_url)
    rename_known_fields(operation_client, OPERATION_TASK_FIELD_RENAMES)
    ensure_single_select_options(
        operation_client,
        {
            "一级类目（需填写）": TOP_CATEGORY_OPTIONS,
            "产品类型（需填写）": PRODUCT_TYPE_OPTIONS,
            "测试阶段（可选，默认初测）": TEST_PHASE_OPTIONS,
            "视频规格（需填写）": VIDEO_SPEC_OPTIONS,
            "长视频场景模式（可选）": LONGFORM_SCENE_MODE_OPTIONS,
            "任务状态（需填写，仅选择待执行）": OPERATION_TASK_STATUS_OPTIONS,
        },
    )
    ensure_fields(operation_client, primary_field_name="任务ID", specs=OPERATION_TASK_FIELDS)
    ensure_fields(script_client, primary_field_name="脚本ID", specs=PRODUCTION_SCRIPT_FIELDS)

    candidates = []
    for record in operation_client.list_records(page_size=500):
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
        can_export_ready = args.export_ready_only and bool(args.record_id) and bool(task["batch_id"])
        if (
            task["status"] != "待执行"
            and not can_resume_failed
            and not can_replan
            and not can_export_ready
        ):
            continue
        candidates.append((record, task))
        if args.limit and len(candidates) >= args.limit:
            break

    print(f"待执行运营任务: {len(candidates)}")
    had_failures = False
    interrupted = False
    for record, task in candidates:
        print(
            f"- {record.record_id} | {task['product_code']} × {task['requested_count']} "
            f"| {task['test_phase']} | {task['video_spec']} "
            f"| scene={task['longform_scene_mode']}"
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

            if task["video_format"] == "LONGFORM":
                if args.export_ready_only:
                    raise RuntimeError("长视频任务暂不使用export-ready-only；请直接续跑原任务")
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
                previous_batch_id = str(task.get("batch_id") or "").strip()
                lf_batch_id = (
                    longform_batch_id(record.record_id, task, replan=True)
                    if args.replan
                    else (
                        previous_batch_id
                        if previous_batch_id.startswith("LFB_")
                        else longform_batch_id(record.record_id, task)
                    )
                )
                # Resume exact sources before reading current template/anchor
                # state. New/replan always uses the current task, even when
                # the SKU already has many successful short scripts.
                direct_sources = load_longform_source_snapshot(lf_batch_id, task)
                lf_storage = LongformStorage()
                lf_storage.ensure_schema()
                frozen_jobs = lf_storage.list_batch_jobs(lf_batch_id)
                if not direct_sources and not frozen_jobs:
                    print("从当前任务商品图与最新共享资产规划长视频，不继承旧脚本穿搭")
                    direct_sources = _build_direct_longform_sources(
                        operation_client=operation_client,
                        task=task,
                        record_id=record.record_id,
                        output_dir=output_dir,
                        voiceover_root=args.voiceover_root,
                        voiceover_db_path=voiceover_db,
                        replan=bool(args.replan),
                        longform_batch_id_value=lf_batch_id,
                    )
                result = build_longform_text_batch(
                    record_id=record.record_id,
                    task=task,
                    batch_id=lf_batch_id,
                    blueprint_model=args.blueprint_model or LONGFORM_BLUEPRINT_MODEL,
                    blueprint_reasoning=args.blueprint_reasoning,
                    voiceover_model_command=args.voiceover_model_command,
                    include_voiceover=not args.plan_only,
                    direct_sources=direct_sources,
                )
                _update(
                    operation_client,
                    record.record_id,
                    batch_id=lf_batch_id,
                    planned_count=result["planned_count"],
                    ready_count=result["ready_count"],
                    failed_count=result["failed_count"],
                    status="执行中-规划" if args.plan_only else "执行中-脚本生成",
                    last_run_at=now_millis(),
                )
                if args.plan_only:
                    print(
                        f"长视频规划完成: {lf_batch_id} | planned={result['planned_count']}"
                    )
                    continue
                transferred_images = transfer_attachments(
                    operation_client, script_client, task["product_images"]
                ) if task["product_images"] else []
                export_summary = export_longform_projections(
                    target_client=script_client,
                    projections=result["projections"],
                    product_images=transferred_images,
                    store_id=task["store_id"],
                )
                if result["ready_count"] >= result["requested_count"]:
                    final_status = "已完成"
                elif result["ready_count"] > 0:
                    final_status = "部分完成"
                else:
                    final_status = "失败"
                errors = "；".join(
                    f"#{item['item_index']} {item['error']}"
                    for item in result["failures"][:3]
                )
                summary = (
                    f"长视频{task['video_spec']}；请求{result['requested_count']}条；"
                    f"完成{result['ready_count']}条；失败{result['failed_count']}条；"
                    f"脚本表新增{export_summary['created']}条、更新{export_summary['updated']}条"
                )
                _update(
                    operation_client,
                    record.record_id,
                    status=final_status,
                    batch_id=lf_batch_id,
                    planned_count=result["planned_count"],
                    ready_count=result["ready_count"],
                    failed_count=result["failed_count"],
                    summary=summary,
                    error=errors,
                    last_run_at=now_millis(),
                )
                print(f"长视频文本任务完成: {lf_batch_id} | {summary}")
                if final_status != "已完成":
                    had_failures = True
                continue

            batch = (
                storage.get_batch(task["batch_id"])
                if task["batch_id"] and not args.replan
                else None
            )
            if batch:
                items = storage.get_items(batch.batch_id)
                if args.export_ready_only:
                    export_summary = _export_ready_and_update_operation(
                        operation_client=operation_client,
                        script_client=script_client,
                        record_id=record.record_id,
                        task=task,
                        batch=batch,
                        items=items,
                        storage=storage,
                    )
                    print(
                        f"导出已完成脚本: {task_id} | batch={batch.batch_id} | "
                        f"created={export_summary.get('created', 0)} | "
                        f"updated={export_summary.get('updated', 0)} | "
                        f"skipped={export_summary.get('skipped', 0)}"
                    )
                    continue
            elif args.export_ready_only:
                raise RuntimeError(
                    f"当前任务没有可导出的批次或本地批次不存在: {task.get('batch_id')}"
                )
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
                    request_id=_request_id(
                        record.record_id,
                        task,
                        replan=bool(args.replan),
                    ),
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
                product_context_override = _load_or_bootstrap_operation_context(
                    operation_client=operation_client,
                    task=task,
                    record_id=record.record_id,
                    output_dir=output_dir,
                    voiceover_root=args.voiceover_root,
                )
                batch, items, _ = run_plan_only(
                    request,
                    output_dir=str(output_dir),
                    voiceover_root=args.voiceover_root,
                    voiceover_db_path=voiceover_db,
                    product_context_override=product_context_override,
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
                blueprint_model=args.blueprint_model or "gpt-5.6-sol",
                blueprint_reasoning=args.blueprint_reasoning,
                item_timeout_seconds=args.item_timeout_seconds,
            )

            export_summary = _export_ready_and_update_operation(
                operation_client=operation_client,
                script_client=script_client,
                record_id=record.record_id,
                task=task,
                batch=batch,
                items=items,
                storage=storage,
            )
            print(
                f"完成: {task_id} | "
                f"created={export_summary.get('created', 0)} | "
                f"updated={export_summary.get('updated', 0)} | "
                f"skipped={export_summary.get('skipped', 0)}"
            )
        except RunInterrupted as exc:
            interrupted = True
            # A completed plan is safely resumable from its saved batch.  Do
            # not classify a scheduler restart as a business failure or leave
            # the row stranded in an executing status.
            try:
                _update(
                    operation_client,
                    record.record_id,
                    status="待执行",
                    error=_interruption_error(exc),
                    last_run_at=now_millis(),
                )
            except Exception as update_exc:  # noqa: BLE001 - preserve signal exit.
                print(
                    f"WARN: 中断后回写待执行失败: {record.record_id}: {update_exc}",
                    file=sys.stderr,
                )
            print(f"中断: {record.record_id}: {exc}", file=sys.stderr)
            break
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
    return 130 if interrupted else (1 if had_failures else 0)


if __name__ == "__main__":
    raise SystemExit(main())
