#!/usr/bin/env python3
"""
按最新渲染规则，重写指定任务在飞书中的母版脚本 / 视频提示词 / 变体脚本文本。

用途：
- 不重新生成脚本
- 仅根据数据库里已成功的结构化结果重新渲染并回写
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


SKILL_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SKILL_DIR))

from core.bitable import (  # noqa: E402
    FeishuBitableClient,
    build_update_payload,
    normalize_cell_value,
    resolve_field_mapping,
    resolve_wiki_bitable_app_token,
)
from core.feishu_url_parser import parse_feishu_bitable_url  # noqa: E402
from core.script_ids import build_script_id_from_fields  # noqa: E402
from core.script_renderer import render_script, render_variant_script, render_video_prompt  # noqa: E402
from core.storage import PipelineStorage  # noqa: E402


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

SCRIPT_TEXT_SLOTS: List[Tuple[str, str]] = [
    ("script_s1", "script_s1"),
    ("script_s2", "script_s2"),
    ("script_s3", "script_s3"),
    ("script_s4", "script_s4"),
    ("video_prompt_s1", "video_prompt_s1"),
    ("video_prompt_s2", "video_prompt_s2"),
    ("video_prompt_s3", "video_prompt_s3"),
    ("video_prompt_s4", "video_prompt_s4"),
]

VARIANT_TEXT_SLOTS: List[Tuple[str, str, int, int]] = [
    (f"script_{script_index}_variant_{variant_index}", f"script_{script_index}_v{variant_index}", script_index, variant_index)
    for script_index in range(1, 5)
    for variant_index in range(1, 6)
]


def resolve_feishu_config(feishu_url: str) -> Tuple[str, str]:
    info = parse_feishu_bitable_url(feishu_url)
    if not info:
        raise ValueError("无法解析飞书表格链接")
    app_token = info.app_token
    if "/wiki/" in info.original_url:
        app_token = resolve_wiki_bitable_app_token(info.app_token)
    return app_token, info.table_id


def _variant_from_batch_output(batch_output: Dict[str, Any], variant_no: int) -> Optional[Dict[str, Any]]:
    variants = batch_output.get("variants", []) or []
    target_variant_id = f"V{variant_no}"
    for item in variants:
        if not isinstance(item, dict):
            continue
        if int(item.get("variant_no", 0) or 0) == variant_no:
            return item
        if str(item.get("variant_id", "") or "").strip().upper() == target_variant_id:
            return item
    return None


def _build_content_id(
    fields: Dict[str, Any],
    mapping: Dict[str, Optional[str]],
    script_index: int,
    variant_no: Optional[int],
    record_id: str,
) -> str:
    return build_script_id_from_fields(
        fields=fields,
        mapping=mapping,
        script_index=script_index,
        variant_no=variant_no,
        record_id=record_id,
    )


def _first_non_empty_content_ids(storage: PipelineStorage, record_id: str) -> Dict[str, Any]:
    for run in storage.query_runs_by_record_id(record_id, limit=20):
        content_ids = storage.get_run_content_ids(int(run["run_id"]))
        if content_ids:
            return content_ids
    return {}


def _task_no_for_record(fields: Dict[str, Any], mapping: Dict[str, Optional[str]]) -> str:
    field_name = mapping.get("task_no")
    return normalize_cell_value(fields.get(field_name)) if field_name else ""


def rewrite_selected_tasks(feishu_url: str, task_nos: Iterable[str]) -> Dict[str, int]:
    requested = {str(item).strip() for item in task_nos if str(item).strip()}
    app_token, table_id = resolve_feishu_config(feishu_url)
    client = FeishuBitableClient(app_token=app_token, table_id=table_id)
    storage = PipelineStorage()

    field_names = client.list_field_names()
    mapping = resolve_field_mapping(field_names)
    records = client.list_records(page_size=100)

    updated_records = 0
    updated_slots = 0

    for record in records:
        task_no = _task_no_for_record(record.fields, mapping)
        if task_no not in requested:
            continue

        product_code_field = mapping.get("product_code")
        product_code = normalize_cell_value(record.fields.get(product_code_field)) if product_code_field else ""
        content_ids = _first_non_empty_content_ids(storage, record.record_id)
        update_values: Dict[str, str] = {}

        for logical_name, content_key in SCRIPT_TEXT_SLOTS:
            field_name = mapping.get(logical_name)
            if not field_name:
                continue
            stage_name = SCRIPT_STAGE_MAP.get(logical_name) or VIDEO_PROMPT_STAGE_MAP.get(logical_name)
            if not stage_name:
                continue
            stage_output = storage.get_latest_stage_output_json(
                record_id=record.record_id,
                stage_name=stage_name,
                product_code=product_code,
            )
            if not isinstance(stage_output, dict):
                continue

            script_index = int(logical_name[-1])
            content_id = str(content_ids.get(content_key) or "").strip() or _build_content_id(
                fields=record.fields,
                mapping=mapping,
                script_index=script_index,
                variant_no=None,
                record_id=record.record_id,
            )
            stage_output["content_id"] = content_id
            rendered = (
                render_script(stage_output)
                if logical_name.startswith("script_")
                else render_video_prompt(stage_output)
            )
            current_text = normalize_cell_value(record.fields.get(field_name))
            if rendered and rendered != current_text:
                update_values[logical_name] = rendered
                updated_slots += 1

        for logical_name, content_key, script_index, variant_no in VARIANT_TEXT_SLOTS:
            field_name = mapping.get(logical_name)
            if not field_name:
                continue
            stage_name = f"variant_s{script_index}_batch_{variant_no}"
            stage_output = storage.get_latest_stage_output_json(
                record_id=record.record_id,
                stage_name=stage_name,
                product_code=product_code,
            )
            if not isinstance(stage_output, dict):
                continue
            variant = _variant_from_batch_output(stage_output, variant_no)
            if not isinstance(variant, dict):
                continue

            content_id = str(content_ids.get(content_key) or "").strip() or _build_content_id(
                fields=record.fields,
                mapping=mapping,
                script_index=script_index,
                variant_no=variant_no,
                record_id=record.record_id,
            )
            variant["content_id"] = content_id
            if isinstance(variant.get("final_video_script_prompt"), dict):
                variant["final_video_script_prompt"]["content_id"] = content_id
            rendered = render_variant_script(variant)
            current_text = normalize_cell_value(record.fields.get(field_name))
            if rendered and rendered != current_text:
                update_values[logical_name] = rendered
                updated_slots += 1

        if update_values:
            payload = build_update_payload(mapping, update_values)
            if payload:
                client.update_record_fields(record.record_id, payload)
                updated_records += 1

    return {"updated_records": updated_records, "updated_slots": updated_slots}


def main() -> None:
    parser = argparse.ArgumentParser(description="按最新渲染规则重写指定任务的脚本输出")
    parser.add_argument("--feishu-url", "-u", required=True, help="飞书多维表格链接")
    parser.add_argument("--task-no", action="append", required=True, help="指定任务编号，可重复传入")
    args = parser.parse_args()

    result = rewrite_selected_tasks(args.feishu_url, args.task_no)
    print(result)


if __name__ == "__main__":
    main()
