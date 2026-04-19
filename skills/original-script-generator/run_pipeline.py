#!/usr/bin/env python3
"""
原创短视频脚本自动生成流水线入口。
"""

import argparse
import os
import sys
from pathlib import Path
from typing import Optional, Tuple


SKILL_DIR = Path(__file__).parent.absolute()
sys.path.insert(0, str(SKILL_DIR))

from core.bitable import FeishuBitableClient, resolve_field_mapping, resolve_wiki_bitable_app_token  # noqa: E402
from core.feishu_url_parser import parse_feishu_bitable_url  # noqa: E402
from core.llm_client import AUTO_ROUTE, BACKUP_ROUTE, GEMINI_ROUTE, PRIMARY_ROUTE, normalize_route_order  # noqa: E402
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


def main() -> None:
    parser = argparse.ArgumentParser(description="原创短视频脚本自动生成流水线")
    parser.add_argument("--feishu-url", "-u", help="飞书多维表格链接")
    parser.add_argument("--limit", "-n", type=int, help="限制处理数量")
    parser.add_argument("--max-workers", type=int, default=3, help="记录级最大并发数，默认 3，实际自动封顶到 3")
    parser.add_argument("--product-code", help="按产品编码单独执行，忽略当前状态")
    parser.add_argument("--record-id", help="按飞书 record_id 单独执行，忽略当前状态")
    parser.add_argument("--task-no", action="append", help="按任务编号执行，可重复传入多个任务编号，忽略当前状态")
    parser.add_argument("--force-variants", action="store_true", help="对指定记录强制执行脚本变体分支")
    parser.add_argument("--force-rerun-script", action="store_true", help="对指定记录强制执行母版脚本重跑分支，并在通过后继续生成变体")
    parser.add_argument("--force-rerun-all", action="store_true", help="对指定记录强制执行全流程重跑")
    parser.add_argument(
        "--llm-route",
        choices=[AUTO_ROUTE, PRIMARY_ROUTE, BACKUP_ROUTE, GEMINI_ROUTE],
        help="选择 LLM 线路：primary=走 OpenClaw 当前主 agent 的 openai-codex/gpt-5.4；backup=只走 Yunwu GPT-5.4；auto=默认优先 primary，失败自动切 backup；gemini=只走 Gemini 3.1 Pro（仅手动调试保留）；不传则使用已保存的默认线路",
    )
    parser.add_argument(
        "--llm-route-order",
        help="自定义 LLM 自动切换优先顺序，逗号分隔，例如 primary,backup；仅填具体线路，不要包含 auto",
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
    args = parser.parse_args()
    llm_route, llm_route_source = resolve_llm_route(args.llm_route)
    llm_route_order = normalize_route_order(args.llm_route_order) if args.llm_route_order else None

    if args.force_rerun_script and args.force_variants:
        raise ValueError("--force-rerun-script 与 --force-variants 不能同时使用")
    if args.force_rerun_all and (args.force_rerun_script or args.force_variants):
        raise ValueError("--force-rerun-all 不能与 --force-rerun-script / --force-variants 同时使用")
    if args.script_index and args.variant_script_index:
        raise ValueError("--script-index 与 --variant-script-index 不能同时混用，请只保留一种")

    app_token, table_id = resolve_feishu_config(args.feishu_url)
    client = FeishuBitableClient(app_token=app_token, table_id=table_id)

    field_names = client.list_field_names()
    mapping = resolve_field_mapping(field_names)
    validate_required_fields(mapping)

    print("📋 字段映射:")
    for key, value in mapping.items():
        if value:
            print(f"   {key}: {value}")
    print(f"🤖 当前 LLM 线路: {llm_route} (source={llm_route_source})")

    selected_script_indexes = args.script_index or args.variant_script_index

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
        print(f"\n📌 找到指定任务 {len(records)} 条 | selector={selector_label}")
    else:
        records = load_pending_records(client, mapping, limit=args.limit)
        print(f"\n📌 找到待处理任务 {len(records)} 条")

    if not records:
        return

    pipeline = OriginalScriptPipeline(
        client,
        mapping,
        variant_script_indexes=selected_script_indexes,
        script_rerun_indexes=args.script_index,
        llm_route=llm_route,
        llm_route_order=llm_route_order,
    )
    stats = pipeline.process_records(records, dry_run=args.dry_run, max_workers=args.max_workers)

    print(f"\n{'=' * 72}")
    print("📊 执行完成")
    print(f"{'=' * 72}")
    print(f"总任务数: {stats['total']}")
    print(f"成功: {stats['success']}")
    print(f"失败: {stats['failed']}")


if __name__ == "__main__":
    main()
