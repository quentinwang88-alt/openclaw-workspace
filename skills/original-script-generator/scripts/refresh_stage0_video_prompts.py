#!/usr/bin/env python3
"""Deterministically refresh Feishu video prompts from stage-0 result files.

The command never calls a model and never mutates the source result.  Apply
mode updates only ``视频生成提示词`` on already exported script rows.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Sequence

SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from core.production_script_feishu import PRODUCTION_SCRIPT_FIELD_NAMES  # noqa: E402
from core.production_script_renderer import (  # noqa: E402
    STAGE0_VIDEO_PROMPT_PROFILE,
)
from scripts.export_stage0_result_to_feishu import (  # noqa: E402
    _build_projection,
    _list_records_retry,
    _text,
)
from scripts.run_feishu_operation_tasks import DEFAULT_SCRIPT_URL, _client  # noqa: E402


def _prompt_clip_count(text: Any) -> int:
    return str(text or "").count("【拍摄片段")


def _load_projections(result_paths: Sequence[str]) -> List[Dict[str, Any]]:
    projections: List[Dict[str, Any]] = []
    for raw_path in result_paths:
        result_path = Path(raw_path).expanduser().resolve()
        payload = json.loads(result_path.read_text(encoding="utf-8"))
        batch_id = "STAGE0_" + hashlib.sha256(
            str(result_path).encode()
        ).hexdigest()[:16].upper()
        item_index = 0
        for product in payload.get("products") or []:
            if not isinstance(product, dict):
                continue
            product_context = {
                "product_code": _text(product.get("product_code")),
                "target_country": _text(product.get("target_country")) or "泰国",
                "target_language": _text(product.get("target_language")) or "泰语",
                "top_category": _text(product.get("top_category")) or "女装",
                "product_type": _text(product.get("product_type")) or "外套",
            }
            for direction in product.get("directions") or []:
                if not isinstance(direction, dict) or not isinstance(
                    direction.get("script"), dict
                ):
                    continue
                item_index += 1
                projection = _build_projection(
                    product=product_context,
                    direction=direction,
                    script=direction["script"],
                    batch_id=batch_id,
                    item_index=item_index,
                )
                projection["source_result_path"] = str(result_path)
                projection["slot"] = _text(direction.get("output_slot"))
                projections.append(projection)
    return projections


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="从阶段0结果无模型刷新飞书视频生成提示词"
    )
    parser.add_argument("--stage0-result", action="append", required=True)
    parser.add_argument("--script-id", action="append", default=[])
    parser.add_argument("--script-url", default=DEFAULT_SCRIPT_URL)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--write-feishu",
        action="store_true",
        help="与 --apply 同时使用时，只回写视频生成提示词字段",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.apply != args.write_feishu:
        parser.error("--apply 与 --write-feishu 必须同时使用")

    selected_ids = {_text(value) for value in args.script_id if _text(value)}
    projections = _load_projections(args.stage0_result)
    if selected_ids:
        projections = [
            item for item in projections if _text(item.get("script_id")) in selected_ids
        ]
        missing_from_source = sorted(
            selected_ids - {_text(item.get("script_id")) for item in projections}
        )
        if missing_from_source:
            raise RuntimeError(
                "指定脚本ID不在输入阶段0结果中: " + ", ".join(missing_from_source)
            )
    if not projections:
        raise RuntimeError("输入结果中没有可刷新的完整脚本")

    client = _client(args.script_url)
    records = {
        _text(record.fields.get(PRODUCTION_SCRIPT_FIELD_NAMES["script_id"])): record
        for record in _list_records_retry(client)
        if _text(record.fields.get(PRODUCTION_SCRIPT_FIELD_NAMES["script_id"]))
    }
    prompt_field = PRODUCTION_SCRIPT_FIELD_NAMES["video_prompt"]
    report: List[Dict[str, Any]] = []
    missing_records: List[str] = []
    for projection in projections:
        script_id = _text(projection.get("script_id"))
        record = records.get(script_id)
        new_prompt = _text(projection.get("video_prompt"))
        if record is None:
            missing_records.append(script_id)
            old_prompt = ""
        else:
            old_prompt = _text(record.fields.get(prompt_field))
        report.append(
            {
                "script_id": script_id,
                "slot": projection.get("slot"),
                "record_id": getattr(record, "record_id", None),
                "source_result_path": projection.get("source_result_path"),
                "old_chars": len(old_prompt),
                "new_chars": len(new_prompt),
                "old_clip_count": _prompt_clip_count(old_prompt),
                "new_clip_count": _prompt_clip_count(new_prompt),
                "changed": old_prompt != new_prompt,
                "status": "MISSING_FEISHU_RECORD" if record is None else "READY",
            }
        )

    print(
        json.dumps(
            {
                "mode": "APPLY" if args.apply else "DRY_RUN",
                "render_profile": STAGE0_VIDEO_PROMPT_PROFILE,
                "model_calls": 0,
                "items": report,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    if missing_records:
        raise RuntimeError("飞书中缺少已导出脚本行: " + ", ".join(missing_records))
    if not args.apply:
        return 0

    updated = 0
    for projection in projections:
        script_id = _text(projection.get("script_id"))
        record = records[script_id]
        new_prompt = _text(projection.get("video_prompt"))
        if _text(record.fields.get(prompt_field)) == new_prompt:
            continue
        client.update_record_fields(record.record_id, {prompt_field: new_prompt})
        updated += 1

    verified_records = {
        _text(record.fields.get(PRODUCTION_SCRIPT_FIELD_NAMES["script_id"])): record
        for record in _list_records_retry(client)
        if _text(record.fields.get(PRODUCTION_SCRIPT_FIELD_NAMES["script_id"]))
    }
    mismatched = []
    for projection in projections:
        script_id = _text(projection.get("script_id"))
        record = verified_records.get(script_id)
        actual = _text(record.fields.get(prompt_field)) if record is not None else ""
        if actual != _text(projection.get("video_prompt")):
            mismatched.append(script_id)
    if mismatched:
        raise RuntimeError("飞书提示词回读校验失败: " + ", ".join(mismatched))
    print(
        json.dumps(
            {"updated": updated, "verified_count": len(projections)},
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
