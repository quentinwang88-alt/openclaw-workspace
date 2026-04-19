#!/usr/bin/env python3
"""
为飞书表中已生成的脚本/变体补回写统一脚本 ID，并将 ID 单独落到本地数据库。
"""

import argparse
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple


SKILL_DIR = Path(__file__).parent.absolute()
sys.path.insert(0, str(SKILL_DIR))

from core.bitable import (  # noqa: E402
    FeishuBitableClient,
    build_update_payload,
    normalize_cell_value,
    resolve_field_mapping,
    resolve_wiki_bitable_app_token,
)
from core.feishu_url_parser import parse_feishu_bitable_url  # noqa: E402
from core.script_ids import (  # noqa: E402
    build_script_id_from_fields,
    extract_unified_id_from_text,
    is_valid_unified_id,
    parse_slot_from_logical_name,
    prepend_unified_id,
)
from core.script_renderer import render_script, render_video_prompt  # noqa: E402
from core.storage import PipelineStorage  # noqa: E402


TEXT_SLOTS: List[Tuple[str, str]] = [
    ("script_s1", "script_s1"),
    ("script_s2", "script_s2"),
    ("script_s3", "script_s3"),
    ("script_s4", "script_s4"),
    ("video_prompt_s1", "video_prompt_s1"),
    ("video_prompt_s2", "video_prompt_s2"),
    ("video_prompt_s3", "video_prompt_s3"),
    ("video_prompt_s4", "video_prompt_s4"),
]
TEXT_SLOTS.extend(
    (f"script_{script_index}_variant_{variant_index}", f"script_{script_index}_v{variant_index}")
    for script_index in range(1, 5)
    for variant_index in range(1, 6)
)

SCRIPT_STAGE_MAP = {
    "script_s1": "script_s1",
    "script_s2": "script_s2",
    "script_s3": "script_s3",
    "script_s4": "script_s4",
}

VIDEO_PROMPT_STAGE_MAP = {
    "video_prompt_s1": "video_prompt_s1",
    "video_prompt_s2": "video_prompt_s2",
    "video_prompt_s3": "video_prompt_s3",
    "video_prompt_s4": "video_prompt_s4",
}


def resolve_feishu_config(feishu_url: str) -> Tuple[str, str]:
    info = parse_feishu_bitable_url(feishu_url)
    if not info:
        raise ValueError("无法解析飞书表格链接")
    app_token = info.app_token
    if "/wiki/" in info.original_url:
        app_token = resolve_wiki_bitable_app_token(info.app_token)
    return app_token, info.table_id

def _prepend_content_id(text: str, content_id: str) -> str:
    return prepend_unified_id(text, content_id)


def _valid_content_id(value: object) -> str:
    text = str(value or "").strip()
    return text if is_valid_unified_id(text) else ""


def main() -> None:
    parser = argparse.ArgumentParser(description="为已生成脚本补回写统一脚本 ID")
    parser.add_argument("--feishu-url", "-u", required=True, help="飞书多维表格链接")
    parser.add_argument("--record-id", help="只处理指定 record_id")
    parser.add_argument("--product-code", help="只处理指定产品编码")
    parser.add_argument("--limit", type=int, help="限制处理条数")
    args = parser.parse_args()

    app_token, table_id = resolve_feishu_config(args.feishu_url)
    client = FeishuBitableClient(app_token=app_token, table_id=table_id)
    storage = PipelineStorage()

    field_names = client.list_field_names()
    mapping = resolve_field_mapping(field_names)
    records = client.list_records(page_size=100)

    updated_records = 0
    updated_slots = 0
    db_updated_runs = 0

    for record in records:
        if args.record_id and record.record_id != args.record_id:
            continue
        product_code_field = mapping.get("product_code")
        product_code = normalize_cell_value(record.fields.get(product_code_field)) if product_code_field else ""
        if args.product_code and product_code != args.product_code:
            continue

        runs = storage.query_runs_by_record_id(record.record_id, limit=1)
        latest_run = runs[0] if runs else None
        run_id = int(latest_run["run_id"]) if latest_run else None
        content_ids = storage.get_run_content_ids(run_id) if run_id is not None else {}
        update_values: Dict[str, str] = {}
        db_touched = False

        for logical_name, content_key in TEXT_SLOTS:
            field_name = mapping.get(logical_name)
            if not field_name:
                continue
            script_index, variant_no = parse_slot_from_logical_name(logical_name)
            if script_index is None:
                continue
            content_id = build_script_id_from_fields(
                record.fields,
                mapping,
                script_index=script_index,
                variant_no=variant_no,
                record_id=record.record_id,
            )

            stage_name = SCRIPT_STAGE_MAP.get(logical_name) or VIDEO_PROMPT_STAGE_MAP.get(logical_name)
            current_text = normalize_cell_value(record.fields.get(field_name))
            existing_id = extract_unified_id_from_text(current_text) if current_text else ""
            if content_ids.get(content_key) != content_id:
                content_ids[content_key] = content_id
                db_touched = True

            if stage_name:
                stage_output = storage.get_latest_stage_output_json(
                    record_id=record.record_id,
                    stage_name=stage_name,
                    product_code=product_code,
                )
                if isinstance(stage_output, dict):
                    stage_output["content_id"] = content_id
                    rendered = (
                        render_script(stage_output)
                        if logical_name.startswith("script_")
                        else render_video_prompt(stage_output)
                    )
                    if rendered and rendered != current_text:
                        update_values[logical_name] = rendered
                        updated_slots += 1
                    continue

            if current_text and existing_id != content_id:
                update_values[logical_name] = _prepend_content_id(current_text, content_id)
                updated_slots += 1

        if update_values:
            payload = build_update_payload(mapping, update_values)
            if payload:
                client.update_record_fields(record.record_id, payload)
                updated_records += 1

        if run_id is not None and content_ids and (db_touched or update_values):
            storage.update_run_artifacts(run_id, content_ids=content_ids)
            db_updated_runs += 1

        if args.limit and updated_records >= args.limit:
            break

    print(
        {
            "updated_records": updated_records,
            "updated_slots": updated_slots,
            "db_updated_runs": db_updated_runs,
        }
    )


if __name__ == "__main__":
    main()
