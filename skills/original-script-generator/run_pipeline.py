#!/usr/bin/env python3
"""
原创短视频脚本自动生成流水线入口。
"""

import argparse
import fcntl
import os
import signal
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple


SKILL_DIR = Path(__file__).parent.absolute()
sys.path.insert(0, str(SKILL_DIR))

from core.bitable import FeishuBitableClient, resolve_field_mapping, resolve_wiki_bitable_app_token  # noqa: E402
from core.feishu_url_parser import parse_feishu_bitable_url  # noqa: E402
from core.pipeline import (  # noqa: E402
    OriginalScriptPipeline,
    load_pending_records,
    load_selected_records,
    STATUS_PENDING_RERUN_ALL,
    STATUS_PENDING_RERUN_SCRIPT,
    STATUS_PENDING_VARIANTS,
    validate_required_fields,
)
from core.runtime_config import resolve_llm_route  # noqa: E402


FEISHU_APP_TOKEN = os.environ.get("FEISHU_APP_TOKEN", "")
FEISHU_TABLE_ID = os.environ.get("FEISHU_TABLE_ID", "")
DEFAULT_MAX_WORKERS = 1
DEFAULT_POLL_INTERVAL_SECONDS = 3600


def resolve_feishu_config(feishu_url: Optional[str]) -> Tuple[str, str]:
    if feishu_url:
        info = parse_feishu_bitable_url(feishu_url)
        if info:
            app_token = info.app_token
            if "/wiki/" in info.original_url:
                resolved = resolve_wiki_bitable_app_token(info.app_token)
                print(f"🔄 检测到 wiki 链接，已解析底层 bitable app_token: {resolved}")
                app_token = resolved
            print("🔗 从 URL 解析飞书配置:")
            print(f"   app_token: {app_token}")
            print(f"   table_id: {info.table_id}")
            return app_token, info.table_id
        print("⚠️ 无法解析飞书 URL，回退到环境变量配置")

    if not FEISHU_APP_TOKEN or not FEISHU_TABLE_ID:
        raise ValueError("请通过 --feishu-url 提供表格地址，或设置 FEISHU_APP_TOKEN / FEISHU_TABLE_ID")
    return FEISHU_APP_TOKEN, FEISHU_TABLE_ID


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="原创短视频脚本自动生成流水线")
    parser.add_argument("--feishu-url", "-u", help="飞书多维表格链接")
    parser.add_argument("--limit", "-n", type=int, help="限制处理数量")
    parser.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help=f"记录级最大并发数，默认 {DEFAULT_MAX_WORKERS}，实际自动封顶到 3",
    )
    parser.add_argument("--watch", action="store_true", help="持续轮询待执行任务")
    parser.add_argument(
        "--poll-interval-seconds",
        type=int,
        default=DEFAULT_POLL_INTERVAL_SECONDS,
        help="轮询间隔秒数，默认 3600（每 1 小时检查一次）",
    )
    parser.add_argument("--max-cycles", type=int, help="最多轮询多少轮，便于测试定时机制")
    parser.add_argument("--product-code", help="按产品编码单独执行，忽略当前状态")
    parser.add_argument("--record-id", help="按飞书 record_id 单独执行，忽略当前状态")
    parser.add_argument("--task-no", action="append", help="按任务编号执行，可重复传入多个任务编号，忽略当前状态")
    parser.add_argument("--force-variants", action="store_true", help="对指定记录强制执行脚本变体分支")
    parser.add_argument("--force-rerun-script", action="store_true", help="对指定记录强制执行母版脚本重跑分支；如表格已勾选生成变体，则在通过后继续生成变体")
    parser.add_argument("--force-rerun-all", action="store_true", help="对指定记录强制执行全流程重跑")
    parser.add_argument(
        "--llm-route",
        help="选择 LLM 线路。当前只支持 primary=走 OpenClaw 当前主 agent 的 openai-codex/gpt-5.4；不传则使用已保存的默认线路",
    )
    parser.add_argument(
        "--llm-route-order",
        help="已废弃，仅为兼容保留；当前不会生效",
    )
    parser.add_argument(
        "--script-index",
        type=int,
        action="append",
        choices=[1, 2, 3, 4],
        help="只重跑指定脚本位，可重复传入；用于母版脚本重跑或只跑指定脚本位的变体",
    )
    parser.add_argument(
        "--variant-script-index",
        type=int,
        action="append",
        choices=[1, 2, 3, 4],
        help="只执行指定脚本索引的变体，可重复传入；默认扩 1/2/3/4，显式传参时仅执行指定脚本",
    )
    parser.add_argument("--dry-run", action="store_true", help="只查看待处理任务")
    parser.add_argument(
        "--run-timeout",
        type=int,
        default=0,
        help="单次运行超时秒数，0 表示不限（默认取环境变量 ORIGINAL_SCRIPT_RUN_TIMEOUT，未设置则 7200 秒=2 小时）",
    )
    return parser


def validate_args(args: argparse.Namespace) -> None:
    if args.force_rerun_script and args.force_variants:
        raise ValueError("--force-rerun-script 与 --force-variants 不能同时使用")
    if args.force_rerun_all and (args.force_rerun_script or args.force_variants):
        raise ValueError("--force-rerun-all 不能与 --force-rerun-script / --force-variants 同时使用")
    if args.script_index and args.variant_script_index:
        raise ValueError("--script-index 与 --variant-script-index 不能同时混用，请只保留一种")
    if args.max_workers < 1:
        raise ValueError("--max-workers 必须大于等于 1")
    if args.poll_interval_seconds < 1:
        raise ValueError("--poll-interval-seconds 必须大于等于 1")
    if args.max_cycles is not None and args.max_cycles < 1:
        raise ValueError("--max-cycles 必须大于等于 1")
    if not args.watch:
        return
    if args.product_code or args.record_id or args.task_no:
        raise ValueError("--watch 仅支持轮询待执行队列，不要与 --product-code / --record-id / --task-no 混用")
    if args.force_rerun_all or args.force_rerun_script or args.force_variants:
        raise ValueError("--watch 仅支持轮询待执行队列，不要与强制重跑参数混用")
    if args.script_index or args.variant_script_index:
        raise ValueError("--watch 仅支持轮询待执行队列，不要与 --script-index / --variant-script-index 混用")


def print_runtime_config(
    mapping: Dict[str, Optional[str]],
    llm_route: str,
    llm_route_source: str,
    args: argparse.Namespace,
) -> None:
    print("📋 字段映射:")
    for key, value in mapping.items():
        if value:
            print(f"   {key}: {value}")
    print(f"🤖 当前 LLM 线路: {llm_route} (source={llm_route_source})")
    print(f"🚦 默认记录级并发上限: {args.max_workers}")
    if args.watch:
        next_run = datetime.now() + timedelta(seconds=args.poll_interval_seconds)
        print(
            "⏰ 已启用定时轮询: "
            f"每 {args.poll_interval_seconds} 秒检查一次待执行任务 | "
            f"预计下一次检查时间: {next_run:%Y-%m-%d %H:%M:%S}"
        )


def load_records_for_run(
    args: argparse.Namespace,
    client: FeishuBitableClient,
    mapping: Dict[str, Optional[str]],
) -> Tuple[list, str]:
    if args.product_code or args.record_id or args.task_no:
        records = load_selected_records(
            client,
            mapping,
            product_code=args.product_code,
            record_id=args.record_id,
            task_nos=args.task_no,
            force_status=(
                STATUS_PENDING_RERUN_ALL
                if args.force_rerun_all
                else STATUS_PENDING_RERUN_SCRIPT
                if args.force_rerun_script
                else STATUS_PENDING_VARIANTS if args.force_variants else None
            ),
        )
        if args.limit:
            records = records[: args.limit]
        selector_label = args.record_id or args.product_code or ",".join(args.task_no or [])
        return records, f"📌 找到指定任务 {len(records)} 条 | selector={selector_label}"

    records = load_pending_records(client, mapping, limit=args.limit)
    return records, f"📌 找到待处理任务 {len(records)} 条"


def print_summary(stats: Dict[str, int], title: str = "执行完成") -> None:
    print(f"\n{'=' * 72}")
    print(f"📊 {title}")
    print(f"{'=' * 72}")
    print(f"总任务数: {stats['total']}")
    print(f"成功: {stats['success']}")
    print(f"失败: {stats['failed']}")


def run_once(
    args: argparse.Namespace,
    client: FeishuBitableClient,
    mapping: Dict[str, Optional[str]],
    llm_route: str,
    llm_route_order: Optional[Tuple[str, ...]],
) -> Dict[str, int]:
    selected_script_indexes = args.script_index or args.variant_script_index
    records, message = load_records_for_run(args, client, mapping)
    print(f"\n{message}")

    if not records:
        print("📭 当前没有待处理任务")
        return {"total": 0, "success": 0, "failed": 0}

    pipeline = OriginalScriptPipeline(
        client,
        mapping,
        variant_script_indexes=selected_script_indexes,
        script_rerun_indexes=args.script_index,
        llm_route=llm_route,
        llm_route_order=llm_route_order,
    )
    return pipeline.process_records(records, dry_run=args.dry_run, max_workers=args.max_workers)


def run_watch_loop(
    args: argparse.Namespace,
    client: FeishuBitableClient,
    mapping: Dict[str, Optional[str]],
    llm_route: str,
    llm_route_order: Optional[Tuple[str, ...]],
) -> None:
    cycle = 0
    aggregated_stats = {"total": 0, "success": 0, "failed": 0}
    run_timeout = args.run_timeout if args.run_timeout > 0 else DEFAULT_RUN_TIMEOUT_SECONDS

    try:
        while True:
            cycle += 1
            print(f"\n{'#' * 72}")
            print(f"🔁 轮询第 {cycle} 次 | {datetime.now():%Y-%m-%d %H:%M:%S}")
            print(f"{'#' * 72}")
            # Reset timeout alarm per cycle in watch mode
            if run_timeout > 0:
                signal.alarm(run_timeout)
            try:
                stats = run_once(args, client, mapping, llm_route, llm_route_order)
            except TimeoutError:
                print(f"\n⏰ 第 {cycle} 轮运行超时 ({run_timeout} 秒)，跳到下一轮。")
                continue
            signal.alarm(0)  # cancel alarm after successful cycle
            for key in aggregated_stats:
                aggregated_stats[key] += stats.get(key, 0)
            print_summary(stats, title=f"第 {cycle} 轮执行完成")

            if args.max_cycles and cycle >= args.max_cycles:
                print("🛑 已达到最大轮询次数，停止定时轮询")
                break

            next_run = datetime.now() + timedelta(seconds=args.poll_interval_seconds)
            print(
                "⏳ 等待下一轮检查: "
                f"{args.poll_interval_seconds} 秒后继续 | "
                f"下次检查时间: {next_run:%Y-%m-%d %H:%M:%S}"
            )
            time.sleep(args.poll_interval_seconds)
    except KeyboardInterrupt:
        print("\n🛑 已收到中断信号，停止定时轮询")
    finally:
        print_summary(aggregated_stats, title="轮询累计结果")


PID_FILE = Path(os.environ.get("ORIGINAL_SCRIPT_PID_FILE", "/tmp/original_script_pipeline.pid"))
DEFAULT_RUN_TIMEOUT_SECONDS = int(os.environ.get("ORIGINAL_SCRIPT_RUN_TIMEOUT", "7200"))  # 2 hours


def acquire_pid_lock() -> Optional[int]:
    """Try to acquire an exclusive PID-file lock using fcntl.flock.
    Returns the locked file descriptor on success, None if another instance is running."""
    try:
        PID_FILE.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(PID_FILE), os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, IOError):
            # Another instance is running
            pid_str = os.read(fd, 32).decode().strip()
            os.close(fd)
            return None, pid_str
        # Write our PID
        os.ftruncate(fd, 0)
        os.write(fd, f"{os.getpid()}\n".encode())
        return fd, None
    except Exception as exc:
        print(f"⚠️ PID 锁异常（继续执行）: {exc}")
        return -1, None  # -1 means no-lock-mode; proceed anyway


def release_pid_lock(fd: int) -> None:
    if fd < 0:
        return
    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
        PID_FILE.unlink(missing_ok=True)
    except Exception:
        pass


def _timeout_handler(signum, frame):
    raise TimeoutError("Pipeline run exceeded the configured timeout")


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    validate_args(args)
    llm_route, llm_route_source = resolve_llm_route(args.llm_route)
    llm_route_order = None
    if args.llm_route_order:
        print("⚠️ --llm-route-order 已废弃，当前会忽略该参数并固定走 primary")

    # --- Process lock (PID file + flock) ---
    lock_fd, existing_pid = acquire_pid_lock()
    if lock_fd is None:
        print(f"🚫 另一个 pipeline 实例正在运行 (PID={existing_pid})，退出。")
        print(f"   如确认无实例在跑，删除 {PID_FILE} 后重试。")
        sys.exit(1)
    if lock_fd == -1:
        print("⚠️ PID 锁获取异常，以无锁模式继续执行")
    else:
        print(f"🔒 进程锁已获取 (PID={os.getpid()}, lock={PID_FILE})")

    # --- Run timeout ---
    run_timeout = args.run_timeout if args.run_timeout > 0 else DEFAULT_RUN_TIMEOUT_SECONDS
    if run_timeout > 0:
        signal.signal(signal.SIGALRM, _timeout_handler)
        signal.alarm(run_timeout)
        print(f"⏱️ 单次运行超时: {run_timeout} 秒 ({run_timeout // 60} 分钟)")

    try:
        app_token, table_id = resolve_feishu_config(args.feishu_url)
        client = FeishuBitableClient(app_token=app_token, table_id=table_id)

        field_names = client.list_field_names()
        mapping = resolve_field_mapping(field_names)
        validate_required_fields(mapping)

        print_runtime_config(mapping, llm_route, llm_route_source, args)

        if args.watch:
            run_watch_loop(args, client, mapping, llm_route, llm_route_order)
            return

        stats = run_once(args, client, mapping, llm_route, llm_route_order)
        if stats["total"] == 0:
            return
        print_summary(stats)
    except TimeoutError:
        print(f"\n⏰ 运行超时 ({run_timeout} 秒)，当前轮次终止。")
        print("   已处理的记录不受影响；未完成的记录在下次巡检时会重新拾起。")
    except KeyboardInterrupt:
        print("\n🛑 收到中断信号，退出。")
    finally:
        # Cancel any remaining alarm
        signal.alarm(0)
        # Release PID lock
        release_pid_lock(lock_fd)


if __name__ == "__main__":
    main()
