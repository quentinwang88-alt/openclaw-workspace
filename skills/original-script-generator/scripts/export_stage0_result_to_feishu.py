#!/usr/bin/env python3
"""Export reviewed stage-0 scripts to the Feishu original-script table.

This path is intentionally separate from the production batch exporter:
stage-0 results are text experiments and do not live in BatchStorage.  It
creates or updates only rows in ``原创视频生产脚本`` and never checks
``进入生产`` or writes the automatic run-manager table.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Sequence

SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from core.production_script_feishu import (  # noqa: E402
    PRODUCTION_SCRIPT_FIELD_NAMES,
    projection_to_feishu_fields,
    transfer_attachments,
)
from core.production_script_renderer import (  # noqa: E402
    render_stage0_video_generation_prompt,
)
from scripts.run_feishu_operation_tasks import (  # noqa: E402
    DEFAULT_OPERATION_URL,
    DEFAULT_SCRIPT_URL,
    _client,
)
from scripts.run_reality_reference_stage0 import _render_storyboard  # noqa: E402


def _text(value: Any) -> str:
    return str(value or "").strip()


def _dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _stable_id(prefix: str, value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    return prefix + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24].upper()


def _source_record(operation_client: Any, product_code: str) -> Any:
    code = _text(product_code)
    for record in _list_records_retry(operation_client):
        fields = record.fields or {}
        for field_name in ("产品编码（需填写）", "产品编码"):
            if _text(fields.get(field_name)) == code:
                return record
    return None


def _list_records_retry(client: Any, attempts: int = 5) -> List[Any]:
    """Handle Feishu's short post-write ``Data not ready`` window."""

    last_error: Exception | None = None
    for attempt in range(max(1, attempts)):
        try:
            return client.list_records(page_size=100)
        except Exception as exc:  # FeishuAPIError is intentionally provider-agnostic here.
            last_error = exc
            if attempt + 1 >= max(1, attempts):
                raise
            time.sleep(2 ** attempt)
    raise RuntimeError(f"读取飞书记录失败: {last_error}")


def _source_value(fields: Dict[str, Any], *names: str) -> str:
    for name in names:
        value = _text(fields.get(name))
        if value:
            return value
    return ""


def _source_images(record: Any) -> List[Dict[str, Any]]:
    if record is None:
        return []
    fields = record.fields or {}
    value = fields.get("产品图片（需填写）") or fields.get("产品图片") or []
    return value if isinstance(value, list) else []


def _render_stage0_text(script: Dict[str, Any]) -> tuple[str, str]:
    complete = _render_storyboard(script)
    video_prompt = render_stage0_video_generation_prompt(
        script=script,
        duration_seconds=15,
    )
    return complete, video_prompt


def _build_projection(
    *,
    product: Dict[str, Any],
    direction: Dict[str, Any],
    script: Dict[str, Any],
    batch_id: str,
    item_index: int,
) -> Dict[str, Any]:
    product_code = _text(product.get("product_code"))
    slot = _text(direction.get("output_slot")) or f"S{item_index}"
    provenance = _dict(script.get("reality_reference_provenance"))
    production = _dict(script.get("production_design"))
    brief = _dict(script.get("video_generation_brief"))
    voice = _dict(script.get("continuous_voiceover"))
    voice_plan = _dict(script.get("voiceover_execution_plan"))
    blueprint = _dict(script.get("creative_blueprint"))
    plan = _dict(script.get("structure_execution_plan"))
    scene = _dict(production.get("scene_setting"))
    bundle = _dict(direction.get("content_bundle_brief")) or _dict(
        script.get("script_positioning")
    )
    complete_script, video_prompt = _render_stage0_text(script)
    script_id = _stable_id(
        "SCSCRIPT_",
        {"batch_id": batch_id, "slot": slot, "blueprint_id": provenance.get("creative_blueprint_id")},
    )
    item_id = f"{batch_id}_{slot}"
    claim_atoms = [item for item in bundle.get("claim_atoms", []) if isinstance(item, dict)]
    core_point = _text(bundle.get("content_mainline")) or _text(
        claim_atoms[0].get("fact_text") if claim_atoms else ""
    )
    target_language = _text(product.get("target_language")) or "泰语"
    return {
        "script_id": script_id,
        "batch_id": batch_id,
        "batch_item_id": item_id,
        "item_index": item_index,
        "product_code": product_code,
        "target_country": _text(product.get("target_country")) or "泰国",
        "target_language": target_language,
        "top_category": _text(product.get("top_category")) or "女装",
        "product_type": _text(product.get("product_type")) or "外套",
        "duration_seconds": 15,
        "script_title": "｜".join(
            part for part in (_text(scene.get("location")), core_point) if part
        ) or f"{product_code}｜{slot}",
        "core_selling_point": core_point,
        "hook_type": _text(voice_plan.get("hook_id") or voice.get("hook_id")),
        "macro_family": _text(plan.get("macro_family_key")),
        "carrier_mode": _text(production.get("presentation_mode")),
        "creative_signature": "|".join(
            part for part in (_text(provenance.get("creative_blueprint_id")), slot) if part
        ),
        "scene_summary": _text(scene.get("location")),
        "target_voiceover": _text(voice.get("target_language")),
        "chinese_voiceover": _text(voice.get("chinese_translation")),
        "complete_script": complete_script,
        "video_prompt": video_prompt,
        "script_type": "原创脚本_阶段0达人分享",
        "processing_status": "待审核",
        "production_enabled": False,
        "first_frame_requested": False,
        "first_frame_status": "用户未选择",
        "cluster_id": provenance.get("cluster_id"),
        "cluster_version": _text(provenance.get("cluster_version")),
        "selection_run_id": _text(provenance.get("selection_run_id")),
        "direction_assignment_id": _text(provenance.get("direction_assignment_id")),
        "content_bundle_id": _text(provenance.get("content_bundle_id")),
        "creative_contract_id": _text(provenance.get("creative_diversity_contract_id")),
        "reference_strategy": _text(
            _dict(brief.get("creator_recording_profile")).get("recording_mode")
        ) or "CREATOR_DIRECT_SHARE",
        "model_version": "stage0:gpt-5.6-sol/high",
        "input_snapshot_hash": _stable_id("", {"script": script})[0:24],
        "_script_id": script_id,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="将阶段0结果幂等写入原创视频生产脚本表")
    parser.add_argument("--stage0-result", required=True)
    parser.add_argument("--script-url", default=DEFAULT_SCRIPT_URL)
    parser.add_argument("--operation-url", default=DEFAULT_OPERATION_URL)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--allow-partial",
        action="store_true",
        help="允许从PARTIAL阶段结果中仅导出已有完整脚本，忽略失败槽位",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    result_path = Path(args.stage0_result).expanduser().resolve()
    payload = json.loads(result_path.read_text(encoding="utf-8"))
    batch_status = _text(payload.get("batch_status"))
    if batch_status != "COMPLETED" and not (
        args.allow_partial and batch_status == "PARTIAL"
    ):
        raise RuntimeError(
            "阶段0结果不是 COMPLETED；如需仅导出其中已完成脚本，请显式使用 --allow-partial"
        )
    products = payload.get("products") or []
    directions = [
        (product, direction)
        for product in products
        if isinstance(product, dict)
        for direction in product.get("directions", []) or []
        if isinstance(direction, dict) and isinstance(direction.get("script"), dict)
    ]
    if not directions:
        raise RuntimeError("阶段0结果没有可写入的完整脚本")

    batch_id = "STAGE0_" + hashlib.sha256(str(result_path).encode()).hexdigest()[:16].upper()
    operation_client = _client(args.operation_url)
    source_records = {}
    projections: List[Dict[str, Any]] = []
    for index, (product, direction) in enumerate(directions, 1):
        code = _text(product.get("product_code"))
        source = source_records.setdefault(code, _source_record(operation_client, code))
        fields = source.fields if source is not None else {}
        product_context = {
            "product_code": code,
            "target_country": _source_value(fields, "目标国家") or "泰国",
            "target_language": _source_value(fields, "目标语言") or "泰语",
            "top_category": _source_value(fields, "一级类目（需填写）", "一级类目"),
            "product_type": _source_value(fields, "产品类型（需填写）", "产品类型"),
        }
        projections.append(
            _build_projection(
                product=product_context,
                direction=direction,
                script=direction["script"],
                batch_id=batch_id,
                item_index=index,
            )
        )

    script_client = _client(args.script_url)
    existing = {
        _text(record.fields.get(PRODUCTION_SCRIPT_FIELD_NAMES["script_id"])): record
        for record in _list_records_retry(script_client)
        if _text(record.fields.get(PRODUCTION_SCRIPT_FIELD_NAMES["script_id"]))
    }
    preflight = []
    for projection in projections:
        preflight.append(
            {
                "product_code": projection["product_code"],
                "slot": projection["batch_item_id"].rsplit("_", 1)[-1],
                "script_id": projection["script_id"],
                "action": "update" if projection["script_id"] in existing else "create",
            }
        )
    print(json.dumps({"mode": "APPLY" if args.apply else "DRY_RUN", "batch_id": batch_id, "items": preflight}, ensure_ascii=False, indent=2))
    if not args.apply:
        return 0

    transferred_images: Dict[str, List[Dict[str, Any]]] = {}
    for code, source in source_records.items():
        if source is not None:
            transferred_images[code] = transfer_attachments(
                operation_client,
                script_client,
                _source_images(source),
            )

    workflow_fields = {
        PRODUCTION_SCRIPT_FIELD_NAMES[name]
        for name in (
            "processing_status", "production_enabled", "first_frame_requested",
            "composite_first_frame", "first_frame_status", "review_note",
            "sync_result", "sync_time", "run_task_id",
        )
    }
    created = 0
    updated = 0
    for projection in projections:
        fields = projection_to_feishu_fields(
            projection,
            product_images=transferred_images.get(projection["product_code"], []),
            include_workflow_defaults=True,
        )
        record = existing.get(projection["script_id"])
        if record is not None:
            script_client.update_record_fields(
                record.record_id,
                {key: value for key, value in fields.items() if key not in workflow_fields},
            )
            updated += 1
        else:
            script_client.batch_create_records([{"fields": fields}])
            created += 1

    written = {
        _text(record.fields.get(PRODUCTION_SCRIPT_FIELD_NAMES["script_id"]))
        for record in _list_records_retry(script_client)
    }
    expected = {projection["script_id"] for projection in projections}
    missing = sorted(expected - written)
    if missing:
        raise RuntimeError(f"飞书写入校验失败，缺少脚本ID: {missing}")
    print(json.dumps({"created": created, "updated": updated, "verified_count": len(expected)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
