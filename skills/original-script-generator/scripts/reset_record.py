#!/usr/bin/env python3
"""
清空原创脚本生成任务的输出字段，并可选重置状态。
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, List


SKILL_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SKILL_DIR))

from core.bitable import FeishuBitableClient, build_update_payload, resolve_field_mapping, resolve_wiki_bitable_app_token  # noqa: E402
from core.feishu_url_parser import parse_feishu_bitable_url  # noqa: E402


RESET_OUTPUT_FIELDS: List[str] = [
    "input_hash",
    "last_run_at",
    "error_message",
    "execution_log",
    "stage_durations",
    "anchor_card_json",
    "opening_strategy_json",
    "styling_plan_json",
    "three_strategies_json",
    "final_s1_json",
    "final_s2_json",
    "final_s3_json",
    "final_s4_json",
    "exp_s1_json",
    "exp_s2_json",
    "exp_s3_json",
    "exp_s4_json",
    "script_s1_json",
    "script_s2_json",
    "script_s3_json",
    "script_s4_json",
    "review_s1_json",
    "review_s2_json",
    "review_s3_json",
    "review_s4_json",
    "script_s1",
    "script_s2",
    "script_s3",
    "script_s4",
    "video_prompt_s1_json",
    "video_prompt_s2_json",
    "video_prompt_s3_json",
    "video_prompt_s4_json",
    "video_prompt_s1",
    "video_prompt_s2",
    "video_prompt_s3",
    "video_prompt_s4",
    "variant_s1_json",
    "variant_s2_json",
    "variant_s3_json",
    "variant_s4_json",
    "script_1_variant_1",
    "script_1_variant_2",
    "script_1_variant_3",
    "script_1_variant_4",
    "script_1_variant_5",
    "script_2_variant_1",
    "script_2_variant_2",
    "script_2_variant_3",
    "script_2_variant_4",
    "script_2_variant_5",
    "script_3_variant_1",
    "script_3_variant_2",
    "script_3_variant_3",
    "script_3_variant_4",
    "script_3_variant_5",
    "script_4_variant_1",
    "script_4_variant_2",
    "script_4_variant_3",
    "script_4_variant_4",
    "script_4_variant_5",
    "output_summary",
]


def resolve_feishu_config(feishu_url: str) -> tuple[str, str]:
    info = parse_feishu_bitable_url(feishu_url)
    if not info:
        raise ValueError(f"无法解析飞书链接: {feishu_url}")
    app_token = info.app_token
    if "/wiki/" in info.original_url:
        app_token = resolve_wiki_bitable_app_token(info.app_token)
    return app_token, info.table_id


def main() -> None:
    parser = argparse.ArgumentParser(description="清空原创脚本任务的输出字段")
    parser.add_argument("--feishu-url", required=True, help="飞书多维表格链接")
    parser.add_argument("--record-id", required=True, help="飞书 record_id")
    parser.add_argument("--status", default="待执行-全流程", help="重置后的任务状态")
    args = parser.parse_args()

    app_token, table_id = resolve_feishu_config(args.feishu_url)
    client = FeishuBitableClient(app_token=app_token, table_id=table_id)
    field_names = client.list_field_names()
    mapping = resolve_field_mapping(field_names)

    updates: Dict[str, object] = {logical_name: "" for logical_name in RESET_OUTPUT_FIELDS}
    updates["status"] = args.status

    payload = build_update_payload(mapping, updates)
    client.update_record_fields(args.record_id, payload)
    print(f"reset ok | record_id={args.record_id} | status={args.status} | updated_fields={len(payload)}")


if __name__ == "__main__":
    main()
