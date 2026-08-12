#!/usr/bin/env python3
"""Backfill deterministic scarf execution handoff for already-ready batches.

This migration does not call a model or rewrite script content.  It only adds
the registered category execution contract to batches generated before the
official Feishu workbench enabled the accessory extension by default, then
optionally re-exports the deterministic video prompt to existing Feishu rows.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Tuple

SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from core.category_execution import (  # noqa: E402
    build_category_video_brief,
    compile_category_execution_extension,
    resolve_category_argument_execution,
    resolve_category_carrier_execution,
)
from core.original_batch_storage import BatchStorage  # noqa: E402
from core.production_script_feishu import export_ready_batch  # noqa: E402
from scripts.run_feishu_operation_tasks import (  # noqa: E402
    DEFAULT_SCRIPT_URL,
    _client,
)


def _dict(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def patch_item_payloads(
    *,
    frozen: Dict[str, Any],
    result: Dict[str, Any],
    anchor_card: Dict[str, Any],
    product_type: str,
    top_category: str,
    selling_argument: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    base = compile_category_execution_extension(
        product_type=product_type,
        top_category=top_category,
        anchor_card=anchor_card,
        enabled=True,
    )
    if not base:
        return frozen, result
    extension = resolve_category_argument_execution(
        base,
        selling_argument=selling_argument,
    )
    patched_frozen = dict(frozen)
    patched_frozen["category_execution_extension"] = extension
    seed = _dict(patched_frozen.get("simplified_creative_seed"))
    seed["category_execution_extension"] = extension

    patched_result = dict(result)
    script = _dict(patched_result.get("script"))
    production = _dict(script.get("production_design"))
    presentation = str(production.get("presentation_mode") or "")
    carrier = resolve_category_carrier_execution(
        extension,
        presentation_mode=presentation,
    )
    seed["carrier_specific_execution"] = carrier
    patched_frozen["simplified_creative_seed"] = seed
    production["accessory_execution"] = carrier
    script["production_design"] = production
    script["category_execution_extension"] = extension
    brief = _dict(script.get("video_generation_brief"))
    brief["production_design"] = production
    brief["category_execution_extension"] = extension
    brief["accessory_execution_brief"] = build_category_video_brief(
        extension,
        carrier_execution=carrier,
    )
    script["video_generation_brief"] = brief
    patched_result["script"] = script
    return patched_frozen, patched_result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", action="append", required=True)
    parser.add_argument("--write-feishu", action="store_true")
    args = parser.parse_args()
    storage = BatchStorage()
    summaries = []
    for batch_id in args.batch_id:
        batch = storage.get_batch(batch_id)
        if not batch:
            raise RuntimeError(f"批次不存在: {batch_id}")
        anchor_path = (
            Path.home()
            / ".openclaw"
            / "shared"
            / "data"
            / "original_production_runs"
            / str(batch.source_record_id)
            / "operation_anchor_card.json"
        )
        cached = json.loads(anchor_path.read_text(encoding="utf-8"))
        anchor_card = _dict(cached.get("anchor_card"))
        patched_count = 0
        for item in storage.get_items(batch_id):
            if item.status != "SCRIPT_READY":
                continue
            frozen = json.loads(item.frozen_direction_package_json or "{}")
            result = json.loads(item.result_json or "{}")
            bundle = json.loads(item.content_bundle_json or "{}")
            patched_frozen, patched_result = patch_item_payloads(
                frozen=frozen,
                result=result,
                anchor_card=anchor_card,
                product_type=batch.product_type,
                top_category=batch.top_category,
                selling_argument=_dict(bundle.get("selling_argument")),
            )
            with storage._connect() as conn:
                conn.execute(
                    "UPDATE original_content_item SET "
                    "frozen_direction_package_json=?, result_json=?, updated_at=CURRENT_TIMESTAMP "
                    "WHERE batch_item_id=?",
                    (
                        json.dumps(patched_frozen, ensure_ascii=False, sort_keys=True),
                        json.dumps(patched_result, ensure_ascii=False, sort_keys=True),
                        item.batch_item_id,
                    ),
                )
            patched_count += 1
        export_summary = {}
        if args.write_feishu:
            export_summary = export_ready_batch(
                batch=batch,
                items=storage.get_items(batch_id),
                target_client=_client(DEFAULT_SCRIPT_URL),
                product_images=(),
            )
        summaries.append(
            {
                "batch_id": batch_id,
                "patched": patched_count,
                "feishu": export_summary,
            }
        )
    print(json.dumps(summaries, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
