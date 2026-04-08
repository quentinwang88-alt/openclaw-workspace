#!/usr/bin/env python3
"""
轻量级养号视频复刻流水线。

读取飞书多维表格中状态为“待开始”的记录，直接把视频传给模型分析，
按顺序生成：
1. 脚本拆解
2. 复刻卡
3. 复刻后的脚本
4. 最终复刻视频提示词
"""

import argparse
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional


SKILL_DIR = Path(__file__).parent.absolute()
REPO_ROOT = SKILL_DIR.parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(SKILL_DIR))

from workspace_support import load_repo_env

load_repo_env()

from core.bitable import (  # noqa: E402
    FeishuBitableClient,
    RemakeRecord,
    normalize_cell_value,
    resolve_field_mapping,
    resolve_video_url,
    resolve_wiki_bitable_app_token,
)
from core.feishu_url_parser import parse_feishu_bitable_url  # noqa: E402
from core.llm_client import VideoRemakeLLMClient  # noqa: E402
from core.prompts import (  # noqa: E402
    CONTENT_BRANCH_NON_PRODUCT,
    CONTENT_BRANCH_PRODUCT,
    build_final_video_prompt,
    build_remade_script_prompt,
    build_remake_card_prompt,
    build_script_breakdown_prompt,
)


FEISHU_APP_TOKEN = os.environ.get("FEISHU_APP_TOKEN", "")
FEISHU_TABLE_ID = os.environ.get("FEISHU_TABLE_ID", "")

STATUS_PENDING = "待开始"
STATUS_PROCESSING = "处理中"
STATUS_DONE = "已完成"
STATUS_FAILED = "失败"


def resolve_feishu_config(feishu_url: Optional[str]) -> (str, str):
    """支持通过飞书链接动态切表。"""
    if feishu_url:
        info = parse_feishu_bitable_url(feishu_url)
        if info:
            app_token = info.app_token
            if "/wiki/" in info.original_url:
                resolved_token = resolve_wiki_bitable_app_token(info.app_token)
                print(f"🔄 检测到 wiki 链接，已解析底层 bitable app_token: {resolved_token}")
                app_token = resolved_token
            print("🔗 从 URL 解析飞书配置:")
            print(f"   app_token: {app_token}")
            print(f"   table_id: {info.table_id}")
            return app_token, info.table_id
        print("⚠️ 无法解析飞书 URL，回退到默认配置")

    if not FEISHU_APP_TOKEN or not FEISHU_TABLE_ID:
        raise RuntimeError(
            "缺少飞书配置。请先填写仓库根目录 .env 中的 FEISHU_APP_TOKEN / FEISHU_TABLE_ID，"
            "或运行时使用 --feishu-url。"
        )
    return FEISHU_APP_TOKEN, FEISHU_TABLE_ID


def validate_required_fields(mapping: Dict[str, Optional[str]]) -> None:
    """校验运行所需字段。"""
    required = {
        "status": "状态字段",
        "video": "视频字段",
        "content_branch": "内容分支字段",
        "script_breakdown": "脚本拆解字段",
        "remake_card": "复刻卡字段",
        "remade_script": "复刻后的脚本字段",
        "final_prompt": "最终复刻视频提示词字段",
    }
    missing = [label for key, label in required.items() if not mapping.get(key)]
    if missing:
        raise Exception(f"表格缺少必需字段: {', '.join(missing)}")


def build_context(record: RemakeRecord, mapping: Dict[str, Optional[str]]) -> Dict[str, str]:
    """抽取单条任务上下文。"""
    fields = record.fields
    replicate_mode = (
        normalize_cell_value(fields.get(mapping.get("remake_mode")))
        if mapping.get("remake_mode") else ""
    ) or "轻本地化复刻"
    raw_branch = normalize_cell_value(fields.get(mapping.get("content_branch"))) if mapping.get("content_branch") else ""
    if raw_branch == "商品展示型":
        content_branch = CONTENT_BRANCH_PRODUCT
    elif raw_branch == "非商品展示型":
        content_branch = CONTENT_BRANCH_NON_PRODUCT
    else:
        raise ValueError("内容分支字段必须填写为“商品展示型”或“非商品展示型”")
    return {
        "content_branch": content_branch,
        "content_branch_label": raw_branch,
        "target_country": normalize_cell_value(fields.get(mapping.get("target_country"))) if mapping.get("target_country") else "",
        "target_language": normalize_cell_value(fields.get(mapping.get("target_language"))) if mapping.get("target_language") else "",
        "product_type": normalize_cell_value(fields.get(mapping.get("product_type"))) if mapping.get("product_type") else "",
        "remake_mode": replicate_mode,
        "replicate_mode": replicate_mode,
    }


def load_pending_records(
    client: FeishuBitableClient,
    mapping: Dict[str, Optional[str]],
    limit: Optional[int] = None,
) -> List[RemakeRecord]:
    """读取待开始任务。"""
    records = client.list_records(page_size=100)
    status_field = mapping["status"]
    pending = [
        record for record in records
        if normalize_cell_value(record.fields.get(status_field)) == STATUS_PENDING
    ]
    if limit:
        pending = pending[:limit]
    return pending


def has_text_value(value: object) -> bool:
    """判断字段是否已有可用文本。"""
    return isinstance(value, str) and bool(value.strip())


class VideoRemakePipeline:
    """四阶段轻量复刻流水线。"""

    def __init__(self, client: FeishuBitableClient, mapping: Dict[str, Optional[str]]):
        self.client = client
        self.mapping = mapping
        self.llm_client = VideoRemakeLLMClient()
        self.stats = {
            "total": 0,
            "success": 0,
            "failed": 0,
        }

    def process_records(self, records: List[RemakeRecord], dry_run: bool = False) -> Dict[str, int]:
        self.stats["total"] = len(records)

        if dry_run:
            print("🔍 Dry run 仅展示待处理记录：")
            for index, record in enumerate(records, 1):
                context = build_context(record, self.mapping)
                print(
                    f"  {index}. record_id={record.record_id} | "
                    f"分支={context['content_branch_label'] or '未提供'} | "
                    f"国家={context['target_country'] or '未提供'} | "
                    f"语言={context['target_language'] or '未提供'} | "
                    f"商品={context['product_type'] or '未提供'}"
                )
            return self.stats

        for index, record in enumerate(records, 1):
            print(f"\n{'=' * 70}")
            print(f"🎬 处理任务 {index}/{len(records)}: {record.record_id}")
            print(f"{'=' * 70}")
            try:
                self._process_single_record(record)
                self.stats["success"] += 1
            except Exception as exc:
                self.stats["failed"] += 1
                print(f"❌ 当前记录失败: {exc}")
                self._mark_failed(record.record_id, str(exc))

        return self.stats

    def _process_single_record(self, record: RemakeRecord) -> None:
        fields = record.fields
        context = build_context(record, self.mapping)
        video_field = self.mapping["video"]
        status_field = self.mapping["status"]
        error_field = self.mapping.get("error_message")

        video_url = resolve_video_url(self.client, fields.get(video_field))
        print(f"  🎞️ 视频地址已解析: {video_url[:120]}{'...' if len(video_url) > 120 else ''}")
        print(
            f"  🧭 分支={context['content_branch_label'] or '未提供'} | "
            f"国家={context['target_country'] or '未提供'} | "
            f"语言={context['target_language'] or '未提供'} | "
            f"商品={context['product_type'] or '未提供'} | "
            f"模式={context['remake_mode'] or '轻本地化复刻'}"
        )

        processing_fields = {status_field: STATUS_PROCESSING}
        if error_field:
            processing_fields[error_field] = ""
        self.client.update_record_fields(record.record_id, processing_fields)

        script_breakdown = fields.get(self.mapping["script_breakdown"])
        if has_text_value(script_breakdown):
            print("  1/4 跳过脚本拆解，复用已写入结果...")
        else:
            print("  1/4 生成脚本拆解...")
            script_breakdown = self.llm_client.chat_with_video(
                video_url=video_url,
                prompt=build_script_breakdown_prompt(context),
                max_tokens=2500,
            )
            self.client.update_record_fields(
                record.record_id,
                {self.mapping["script_breakdown"]: script_breakdown},
            )

        remake_card = fields.get(self.mapping["remake_card"])
        if has_text_value(remake_card):
            print("  2/4 跳过复刻卡，复用已写入结果...")
        else:
            print("  2/4 生成复刻卡...")
            remake_card = self.llm_client.chat_text(
                prompt=build_remake_card_prompt(context, script_breakdown),
                max_tokens=2500,
            )
            self.client.update_record_fields(
                record.record_id,
                {self.mapping["remake_card"]: remake_card},
            )

        remade_script = fields.get(self.mapping["remade_script"])
        if has_text_value(remade_script):
            print("  3/4 跳过复刻后的脚本，复用已写入结果...")
        else:
            print("  3/4 生成复刻后的脚本...")
            remade_script = self.llm_client.chat_text(
                prompt=build_remade_script_prompt(context, remake_card),
                max_tokens=2500,
            )
            self.client.update_record_fields(
                record.record_id,
                {self.mapping["remade_script"]: remade_script},
            )

        final_prompt = fields.get(self.mapping["final_prompt"])
        if has_text_value(final_prompt):
            print("  4/4 跳过最终复刻视频提示词，复用已写入结果...")
        else:
            print("  4/4 生成最终复刻视频提示词...")
            final_prompt = self.llm_client.chat_text(
                prompt=build_final_video_prompt(context, remade_script),
                max_tokens=2500,
            )
        done_fields = {
            status_field: STATUS_DONE,
        }
        if not has_text_value(fields.get(self.mapping["final_prompt"])):
            done_fields[self.mapping["final_prompt"]] = final_prompt
        if error_field:
            done_fields[error_field] = ""
        self.client.update_record_fields(record.record_id, done_fields)
        print("  ✅ 当前记录完成")

    def _mark_failed(self, record_id: str, error_message: str) -> None:
        status_field = self.mapping["status"]
        error_field = self.mapping.get("error_message")
        fields = {status_field: STATUS_FAILED}
        if error_field:
            fields[error_field] = error_message[:1000]
        try:
            self.client.update_record_fields(record_id, fields)
        except Exception as exc:
            print(f"⚠️ 回写失败状态也失败了: {exc}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="轻量级养号视频复刻流水线",
    )
    parser.add_argument("--feishu-url", "-u", help="飞书多维表格链接")
    parser.add_argument("--limit", "-n", type=int, help="限制处理数量")
    parser.add_argument("--dry-run", action="store_true", help="只查看待处理任务")
    args = parser.parse_args()

    app_token, table_id = resolve_feishu_config(args.feishu_url)
    client = FeishuBitableClient(app_token=app_token, table_id=table_id)

    field_names = client.list_field_names()
    mapping = resolve_field_mapping(field_names)
    validate_required_fields(mapping)

    print("📋 字段映射:")
    for key, value in mapping.items():
        if value:
            print(f"   {key}: {value}")

    records = load_pending_records(client, mapping, limit=args.limit)
    print(f"\n📌 找到待处理任务 {len(records)} 条")
    if not records:
        return

    pipeline = VideoRemakePipeline(client, mapping)
    stats = pipeline.process_records(records, dry_run=args.dry_run)

    print(f"\n{'=' * 70}")
    print("📊 执行完成")
    print(f"{'=' * 70}")
    print(f"总任务数: {stats['total']}")
    print(f"成功: {stats['success']}")
    print(f"失败: {stats['failed']}")


if __name__ == "__main__":
    main()
