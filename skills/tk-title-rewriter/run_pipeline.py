#!/usr/bin/env python3
"""Rewrite Chinese product titles into localized TikTok Shop titles and write them back to Feishu."""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence


SKILL_DIR = Path(__file__).resolve().parent
REPO_ROOT = SKILL_DIR.parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))
if str(SKILL_DIR) in sys.path:
    sys.path.remove(str(SKILL_DIR))
sys.path.insert(0, str(SKILL_DIR))

from workspace_support import load_repo_env  # noqa: E402

load_repo_env()

from config import (  # noqa: E402
    DEFAULT_CATEGORY_FIELD_CANDIDATES,
    DEFAULT_LLM_API_KEY,
    DEFAULT_LLM_BASE_URL,
    DEFAULT_LLM_BATCH_SIZE,
    DEFAULT_LLM_MODEL,
    DEFAULT_LLM_TIMEOUT_SECONDS,
    DEFAULT_OUTPUT_FIELD,
    DEFAULT_TITLE_FIELD_CANDIDATES,
    DEFAULT_WRITE_BATCH_SIZE,
    MAX_LLM_BATCH_SIZE,
    MAX_WRITE_BATCH_SIZE,
)
from core.feishu import (  # noqa: E402
    FeishuAPIError,
    FeishuBitableClient,
    FeishuBitableInfo,
    TableRecord,
    parse_feishu_bitable_url,
    resolve_bitable_app_token,
)
from core.llm import (  # noqa: E402
    LLMError,
    RewriteFailure,
    RewriteInput,
    TKTitleRewriterLLMClient,
    align_rewrites,
)
from core.mapping import resolve_field_name  # noqa: E402
from core.prompt_loader import (  # noqa: E402
    PromptTemplate,
    load_prompt_templates,
    match_prompt_template,
)


@dataclass(frozen=True)
class SelectedRecord:
    record_id: str
    category: str
    original_title: str


def main() -> None:
    args = parse_args()
    summary = run(args)
    print_summary(summary)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rewrite localized TikTok Shop titles for a Feishu bitable.")
    parser.add_argument("--feishu-url", help="飞书多维表格 URL")
    parser.add_argument("--base-token", help="飞书 base / app token")
    parser.add_argument("--table-id", help="飞书 table_id")
    parser.add_argument("--title-field", help="原始标题字段名")
    parser.add_argument("--category-field", help="产品类目字段名")
    parser.add_argument("--output-field", default=DEFAULT_OUTPUT_FIELD, help="输出字段名，默认 TK标题")
    parser.add_argument(
        "--cn-summary-field",
        help="可选：同步写入中文摘要字段；传空字符串则不写",
    )
    parser.add_argument("--limit", type=int, help="限制处理记录数")
    parser.add_argument("--record-id", action="append", default=[], help="只处理指定 record_id，可多次传入")
    parser.add_argument("--overwrite", action="store_true", help="即使输出字段已有值也覆盖")
    parser.add_argument("--dry-run", action="store_true", help="只预览，不回写飞书")
    parser.add_argument(
        "--llm-batch-size",
        type=int,
        default=DEFAULT_LLM_BATCH_SIZE,
        help=f"单次 LLM 批大小，最大 {MAX_LLM_BATCH_SIZE}",
    )
    parser.add_argument(
        "--write-batch-size",
        type=int,
        default=DEFAULT_WRITE_BATCH_SIZE,
        help=f"飞书批量写回大小，最大 {MAX_WRITE_BATCH_SIZE}",
    )
    parser.add_argument("--llm-base-url", default=DEFAULT_LLM_BASE_URL, help="LLM base URL")
    parser.add_argument("--llm-api-key", default=DEFAULT_LLM_API_KEY, help="LLM API Key")
    parser.add_argument("--llm-model", default=DEFAULT_LLM_MODEL, help="LLM model")
    parser.add_argument("--timeout-seconds", type=int, default=DEFAULT_LLM_TIMEOUT_SECONDS, help="LLM 请求超时秒数")
    parser.add_argument(
        "--prompts-dir",
        default=str(SKILL_DIR / "prompts"),
        help="Prompt 模板目录",
    )
    return parser.parse_args()


def run(args: argparse.Namespace) -> Dict[str, Any]:
    llm_batch_size = min(max(int(args.llm_batch_size or DEFAULT_LLM_BATCH_SIZE), 1), MAX_LLM_BATCH_SIZE)
    write_batch_size = min(max(int(args.write_batch_size or DEFAULT_WRITE_BATCH_SIZE), 1), MAX_WRITE_BATCH_SIZE)

    info = resolve_feishu_info(args)
    app_token = resolve_bitable_app_token(info) if info.original_url else str(args.base_token or "").strip()
    if not app_token:
        raise ValueError("无法解析飞书 app_token，请检查 --feishu-url 或 --base-token")

    prompts_dir = Path(args.prompts_dir).expanduser().resolve()
    templates = load_prompt_templates(prompts_dir)
    if not templates:
        raise ValueError(f"Prompt 目录为空: {prompts_dir}")

    client = FeishuBitableClient(app_token=app_token, table_id=info.table_id)
    fields = client.list_fields()
    field_names = [field.field_name for field in fields]

    title_field = resolve_field_name(field_names, args.title_field, DEFAULT_TITLE_FIELD_CANDIDATES)
    if not title_field:
        raise ValueError(f"未找到原始标题字段，可选字段包括: {', '.join(field_names)}")

    category_field = resolve_field_name(field_names, args.category_field, DEFAULT_CATEGORY_FIELD_CANDIDATES)
    if not category_field:
        raise ValueError(f"未找到产品类目字段，可选字段包括: {', '.join(field_names)}")

    output_field = resolve_field_name(field_names, args.output_field, [args.output_field]) if args.output_field else None
    if not output_field and args.output_field:
        client.create_text_field(args.output_field)
        output_field = args.output_field
        field_names.append(output_field)

    cn_summary_field = None
    requested_cn_summary_field = None
    if args.cn_summary_field is not None:
        requested_cn_summary_field = (args.cn_summary_field or "").strip()
        if requested_cn_summary_field:
            cn_summary_field = resolve_field_name(field_names, requested_cn_summary_field, [requested_cn_summary_field])
            if not cn_summary_field:
                client.create_text_field(requested_cn_summary_field)
                cn_summary_field = requested_cn_summary_field
                field_names.append(cn_summary_field)

    records = client.list_records(limit=None)
    if args.record_id:
        record_ids = {item.strip() for item in args.record_id if item.strip()}
        records = [record for record in records if record.record_id in record_ids]
    if args.limit:
        records = records[: args.limit]

    selected_records: List[SelectedRecord] = []
    skipped_empty_title: List[str] = []
    skipped_existing_output: List[str] = []
    unmatched_categories: Dict[str, int] = {}
    templates_by_path: Dict[Path, PromptTemplate] = {template.path: template for template in templates}
    grouped_inputs: Dict[Path, List[RewriteInput]] = defaultdict(list)
    match_modes: Dict[str, int] = defaultdict(int)

    for record in records:
        original_title = normalize_cell_value(record.fields.get(title_field))
        if not original_title:
            skipped_empty_title.append(record.record_id)
            continue

        existing_output = normalize_cell_value(record.fields.get(output_field)) if output_field else ""
        if existing_output and not args.overwrite:
            skipped_existing_output.append(record.record_id)
            continue

        category = normalize_cell_value(record.fields.get(category_field))
        template, match_mode = match_prompt_template(category, templates)
        if not template:
            unmatched_categories[category or "(空类目)"] = unmatched_categories.get(category or "(空类目)", 0) + 1
            continue

        selected = SelectedRecord(
            record_id=record.record_id,
            category=category,
            original_title=original_title,
        )
        selected_records.append(selected)
        grouped_inputs[template.path].append(
            RewriteInput(record_id=selected.record_id, category=selected.category, original_title=selected.original_title)
        )
        if match_mode:
            match_modes[match_mode] += 1

    llm_client: Optional[TKTitleRewriterLLMClient] = None
    if grouped_inputs:
        llm_client = TKTitleRewriterLLMClient(
            base_url=args.llm_base_url,
            api_key=args.llm_api_key,
            model=args.llm_model,
            timeout=args.timeout_seconds,
        )

    pending_updates: List[Dict[str, Any]] = []
    successes: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []

    for template_path, items in grouped_inputs.items():
        template = templates_by_path[template_path]
        for batch in chunked(items, llm_batch_size):
            try:
                if llm_client is None:
                    raise LLMError("LLM 客户端未初始化")
                raw_text, parsed_items = llm_client.rewrite_batch(
                    system_prompt=template.system_prompt,
                    items=batch,
                    include_cn_summary=bool(cn_summary_field),
                )
                aligned, batch_failures = align_rewrites(batch, parsed_items)

                for failure in batch_failures:
                    failures.append(failure_to_dict(failure, template))

                for requested_item, parsed_item in aligned:
                    update_fields = {output_field: parsed_item.tk_title}
                    if cn_summary_field:
                        update_fields[cn_summary_field] = parsed_item.chinese_summary or ""
                    pending_updates.append({"record_id": requested_item.record_id, "fields": update_fields})
                    successes.append(
                        {
                            "record_id": requested_item.record_id,
                            "category": requested_item.category,
                            "original_title": requested_item.original_title,
                            "tk_title": parsed_item.tk_title,
                            "chinese_summary": parsed_item.chinese_summary,
                            "template": template.category_name,
                            "template_match_mode": "exact"
                            if template.category_name == requested_item.category
                            else "alias",
                            "character_count": parsed_item.character_count,
                            "attributes": parsed_item.extracted_attributes,
                        }
                    )
                    if parsed_item.warning:
                        warnings.append(
                            {
                                "record_id": requested_item.record_id,
                                "category": requested_item.category,
                                "warning": parsed_item.warning,
                            }
                        )
            except (LLMError, FeishuAPIError, ValueError) as exc:
                for item in batch:
                    failures.append(
                        {
                            "record_id": item.record_id,
                            "category": item.category,
                            "original_title": item.original_title,
                            "reason": str(exc),
                            "template": template.category_name,
                        }
                    )
                raw_text = ""

    if not args.dry_run:
        for batch in chunked(pending_updates, write_batch_size):
            client.batch_update_records(batch)

    return {
        "feishu": {
            "app_token": app_token,
            "table_id": info.table_id,
            "source": info.original_url or "manual",
        },
        "mapping": {
            "title_field": title_field,
            "category_field": category_field,
            "output_field": output_field,
            "cn_summary_field": cn_summary_field or "",
        },
        "stats": {
            "total_records": len(records),
            "selected_records": len(selected_records),
            "success_count": len(successes),
            "skip_existing_count": len(skipped_existing_output),
            "skip_empty_title_count": len(skipped_empty_title),
            "unmatched_category_count": sum(unmatched_categories.values()),
            "failure_count": len(failures),
            "llm_batch_size": llm_batch_size,
            "write_batch_size": write_batch_size,
            "dry_run": bool(args.dry_run),
            "exact_match_count": match_modes.get("exact", 0),
            "alias_match_count": match_modes.get("alias", 0),
        },
        "skipped": {
            "empty_title_record_ids": skipped_empty_title,
            "existing_output_record_ids": skipped_existing_output,
        },
        "unmatched_categories": unmatched_categories,
        "warnings": warnings,
        "successes": successes,
        "failures": failures,
        "pending_updates_preview": pending_updates[:20],
    }


def resolve_feishu_info(args: argparse.Namespace) -> FeishuBitableInfo:
    if args.feishu_url:
        info = parse_feishu_bitable_url(args.feishu_url)
        if not info:
            raise ValueError(f"无法解析飞书 URL: {args.feishu_url}")
        return info

    if args.base_token and args.table_id:
        return FeishuBitableInfo(
            app_token=str(args.base_token).strip(),
            table_id=str(args.table_id).strip(),
            original_url="",
            is_wiki=False,
        )

    raise ValueError("请提供 --feishu-url，或同时提供 --base-token 与 --table-id")


def normalize_cell_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        return str(value).strip()
    if isinstance(value, dict):
        for key in ("text", "name", "title"):
            cell = value.get(key)
            if isinstance(cell, str) and cell.strip():
                return cell.strip()
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        parts = [normalize_cell_value(item) for item in value]
        return " / ".join(part for part in parts if part)
    return str(value).strip()


def chunked(items: Sequence[Any], size: int) -> List[List[Any]]:
    return [list(items[index : index + size]) for index in range(0, len(items), size)]


def failure_to_dict(failure: RewriteFailure, template: PromptTemplate) -> Dict[str, Any]:
    return {
        "record_id": failure.record_id,
        "original_title": failure.original_title,
        "reason": failure.reason,
        "template": template.category_name,
    }


def print_summary(summary: Dict[str, Any]) -> None:
    stats = summary["stats"]
    mapping = summary["mapping"]
    print("已识别字段映射：")
    print(f"- 原始标题 → {mapping['title_field']}")
    print(f"- 产品类目 → {mapping['category_field']}")
    print(f"- 输出字段 → {mapping['output_field']}")
    if mapping.get("cn_summary_field"):
        print(f"- 中文摘要字段 → {mapping['cn_summary_field']}")

    print("")
    print("改写完成：")
    print(f"- 总记录数：{stats['total_records']} 条")
    print(f"- 命中待处理记录：{stats['selected_records']} 条")
    print(f"- 成功改写：{stats['success_count']} 条")
    print(f"- 跳过（已有TK标题）：{stats['skip_existing_count']} 条")
    print(f"- 跳过（原始标题为空）：{stats['skip_empty_title_count']} 条")
    print(f"- 失败（无匹配品类模板）：{stats['unmatched_category_count']} 条")
    print(f"- 失败（模型/解析/回写）：{stats['failure_count']} 条")
    print(f"- 模板精确匹配：{stats['exact_match_count']} 条")
    print(f"- 模板别名匹配：{stats['alias_match_count']} 条")
    print(f"- 回写状态：{'dry-run，未写回飞书' if stats['dry_run'] else '已写回飞书表格'}")

    successes = summary.get("successes") or []
    if successes:
        print("")
        print("结果预览：")
        for item in successes[:5]:
            print(f"- {item['record_id']} | 类目={item['category']}")
            print(f"  原标题：{item['original_title'][:80]}")
            print(f"  优化后：{item['tk_title'][:120]}")
            if item.get("chinese_summary"):
                print(f"  中文摘要：{item['chinese_summary'][:120]}")

    unmatched_categories = summary.get("unmatched_categories") or {}
    if unmatched_categories:
        categories = ", ".join(
            f"{category} x{count}" for category, count in sorted(unmatched_categories.items(), key=lambda item: item[0])
        )
        print(f"- 未匹配品类：{categories}")

    warnings = summary.get("warnings") or []
    if warnings:
        print("")
        print("警告预览：")
        for item in warnings[:10]:
            print(f"- {item['record_id']} | {item['category']} | {item['warning']}")

    failures = summary.get("failures") or []
    if failures:
        print("")
        print("失败预览：")
        for item in failures[:10]:
            reason = item.get("reason") or "未知原因"
            original_title = item.get("original_title") or ""
            print(f"- {item.get('record_id', '')} | {original_title[:36]} | {reason}")


if __name__ == "__main__":
    main()
