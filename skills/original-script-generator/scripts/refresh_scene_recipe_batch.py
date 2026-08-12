#!/usr/bin/env python3
"""Refresh frozen scene recipes for ready batches without model calls.

The migration preserves structure, selling argument, hook, voiceover, character,
outfit and storyboard.  It only refreshes the read-only scene reference,
visual-execution contract and deterministic final video prompt projection.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence, Tuple


SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from core.original_batch_storage import BatchStorage  # noqa: E402
from core.production_script_feishu import export_ready_batch  # noqa: E402
from core.scene_reference_adapter import (  # noqa: E402
    load_scene_reference_contexts,
    scene_family_for_motif,
    scene_reference_contract_for_family,
)
from core.simplified_complete_script import build_simplified_creative_seed  # noqa: E402
from core.visual_execution_contract import build_visual_execution_diagnostics  # noqa: E402
from scripts.run_feishu_operation_tasks import DEFAULT_SCRIPT_URL, _client  # noqa: E402


REFRESH_POLICY_VERSION = "scene-recipe-local-refresh-v1"
DEFAULT_BACKUP_ROOT = (
    Path.home() / ".openclaw" / "shared" / "data" / "scene_recipe_refresh"
)


def _dict(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _stable_hash(value: Any) -> str:
    payload = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _anchor_card_for_batch(batch: Any) -> Dict[str, Any]:
    path = (
        Path.home()
        / ".openclaw"
        / "shared"
        / "data"
        / "original_production_runs"
        / str(batch.source_record_id)
        / "operation_anchor_card.json"
    )
    cached = json.loads(path.read_text(encoding="utf-8"))
    anchor = _dict(cached.get("anchor_card"))
    if not anchor:
        raise RuntimeError(f"批次缺少锚点卡: {batch.batch_id}")
    return anchor


def _direction_for_item(item: Any, frozen: Mapping[str, Any]) -> Dict[str, Any]:
    contract = _dict(frozen.get("structure_contract"))
    identity = _dict(contract.get("direction_identity"))
    return {
        "direction_assignment_id": _text(
            frozen.get("direction_assignment_id") or item.direction_assignment_id
        ),
        "cluster_id": frozen.get("cluster_id", item.cluster_id),
        "macro_family_key": _text(identity.get("macro_family_key")),
        "structure_contract": contract,
    }


def refresh_item_payloads(
    *,
    item: Any,
    batch: Any,
    frozen: Dict[str, Any],
    result: Dict[str, Any],
    anchor_card: Dict[str, Any],
    scene_context: Mapping[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any], str, Dict[str, Any]]:
    creative = _dict(frozen.get("creative_diversity_contract"))
    old_scene = _dict(frozen.get("scene_reference_contract"))
    scene_motif = _text(creative.get("scene_motif"))
    scene_family = _text(old_scene.get("scene_family_key")) or scene_family_for_motif(
        scene_motif
    )
    script = _dict(result.get("script"))
    production = _dict(script.get("production_design"))
    presentation = _text(production.get("presentation_mode")) or _text(
        item.carrier_mode
    )
    new_scene = scene_reference_contract_for_family(
        scene_context,
        scene_family,
        scene_motif=scene_motif,
        presentation=presentation,
        country=batch.target_country,
        category=batch.top_category,
        product_type=batch.product_type,
        scene_request=_dict(creative.get("scene_request_contract")),
    )
    creative["scene_reference_contract"] = new_scene

    old_seed = _dict(frozen.get("simplified_creative_seed"))
    relationship_device = _text(
        _dict(old_seed.get("voiceover_surface_contract")).get(
            "relationship_device"
        )
    )
    rebuilt_seed = build_simplified_creative_seed(
        anchor_card=anchor_card,
        structure_contract=_dict(frozen.get("structure_contract")),
        content_bundle=_dict(frozen.get("content_bundle_brief")),
        creative_contract=creative,
        execution_reference=_dict(frozen.get("execution_reference")),
        requested_hook_id=item.requested_hook_id,
        content_angle_key=item.content_angle_key,
        relationship_device=relationship_device,
        product_type=batch.product_type,
        top_category=batch.top_category,
        category_execution_extension=_dict(
            frozen.get("category_execution_extension")
        ),
    )
    # Preserve all non-scene frozen inputs exactly.  Only the two scene-facing
    # projections are copied from the freshly compiled seed.
    patched_seed = dict(old_seed)
    diversity = _dict(patched_seed.get("diversity_context"))
    rebuilt_diversity = _dict(rebuilt_seed.get("diversity_context"))
    diversity["scene_reference"] = _dict(
        rebuilt_diversity.get("scene_reference")
    )
    patched_seed["diversity_context"] = diversity
    if isinstance(rebuilt_seed.get("visual_execution_contract"), Mapping):
        patched_seed["visual_execution_contract"] = _dict(
            rebuilt_seed.get("visual_execution_contract")
        )
    patched_seed["creative_seed_id"] = "SCS_" + _stable_hash(
        {
            "previous": old_seed.get("creative_seed_id"),
            "scene_reference": diversity.get("scene_reference"),
            "refresh_policy": REFRESH_POLICY_VERSION,
        }
    )[:24]

    patched_frozen = dict(frozen)
    patched_frozen["scene_reference_contract"] = new_scene
    patched_frozen["creative_diversity_contract"] = creative
    patched_frozen["simplified_creative_seed"] = patched_seed

    patched_result = dict(result)
    patched_script = dict(script)
    visual = _dict(patched_seed.get("visual_execution_contract"))
    if visual:
        patched_script["visual_execution_contract"] = visual
        diagnostics = build_visual_execution_diagnostics(
            contract=visual,
            production_design=production,
        )
        patched_script["visual_execution_diagnostics"] = diagnostics
        brief = _dict(patched_script.get("video_generation_brief"))
        brief["visual_execution_contract"] = visual
        brief["visual_execution_diagnostics"] = diagnostics
        patched_script["video_generation_brief"] = brief
    patched_result["script"] = patched_script
    patched_result["scene_recipe_refresh"] = {
        "policy_version": REFRESH_POLICY_VERSION,
        "refreshed_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "preserved": [
            "structure",
            "selling_argument",
            "hook",
            "voiceover",
            "character",
            "outfit",
            "storyboard",
            "script_id",
        ],
    }
    new_snapshot_hash = _stable_hash(
        {
            "previous_item_snapshot_hash": item.item_snapshot_hash,
            "scene_reference_contract": new_scene,
            "refresh_policy": REFRESH_POLICY_VERSION,
        }
    )
    card = _dict(new_scene.get("scene_execution_card"))
    summary = {
        "batch_item_id": item.batch_item_id,
        "script_id": _text(result.get("script_id")),
        "scene_family_key": scene_family,
        "source_quality": _text(card.get("source_quality")),
        "situation_tags": list(card.get("situation_tags") or []),
        "aesthetic_anchors": list(card.get("aesthetic_anchors") or []),
        "visual_scene_recipe": _dict(card.get("visual_scene_recipe")),
    }
    return patched_frozen, patched_result, new_snapshot_hash, summary


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="局部刷新已就绪原创批次的场景配方与最终提示词"
    )
    parser.add_argument("--batch-id", action="append", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--write-feishu", action="store_true")
    parser.add_argument("--script-url", default=DEFAULT_SCRIPT_URL)
    parser.add_argument("--backup-root", default=str(DEFAULT_BACKUP_ROOT))
    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.write_feishu and not args.apply:
        parser.error("--write-feishu 必须与 --apply 同时使用")

    os.environ["ORIGINAL_SCRIPT_SCENE_REFERENCE_ENABLED"] = "1"
    storage = BatchStorage()
    batches = []
    backups: List[Dict[str, Any]] = []
    all_summaries: List[Dict[str, Any]] = []
    for batch_id in args.batch_id:
        batch = storage.get_batch(batch_id)
        if not batch:
            raise RuntimeError(f"批次不存在: {batch_id}")
        items = storage.get_items(batch_id)
        ready = [item for item in items if item.status == "SCRIPT_READY"]
        if not ready:
            raise RuntimeError(f"批次没有就绪脚本: {batch_id}")
        frozen_rows = [json.loads(item.frozen_direction_package_json or "{}") for item in ready]
        directions = [
            _direction_for_item(item, frozen)
            for item, frozen in zip(ready, frozen_rows)
        ]
        contexts = load_scene_reference_contexts(directions)
        anchor_card = _anchor_card_for_batch(batch)
        patched_rows = []
        for item, frozen, direction in zip(ready, frozen_rows, directions):
            result = json.loads(item.result_json or "{}")
            direction_id = _text(direction.get("direction_assignment_id"))
            patched = refresh_item_payloads(
                item=item,
                batch=batch,
                frozen=frozen,
                result=result,
                anchor_card=anchor_card,
                scene_context=_dict(contexts.get(direction_id)),
            )
            patched_rows.append((item, *patched[:3]))
            summary = dict(patched[3])
            summary["batch_id"] = batch_id
            all_summaries.append(summary)
            backups.append(
                {
                    "batch_id": batch_id,
                    "batch_item_id": item.batch_item_id,
                    "item_snapshot_hash": item.item_snapshot_hash,
                    "frozen_direction_package_json": item.frozen_direction_package_json,
                    "result_json": item.result_json,
                }
            )
        batches.append((batch, patched_rows))

    output = {
        "mode": "APPLY" if args.apply else "DRY_RUN",
        "policy_version": REFRESH_POLICY_VERSION,
        "items": all_summaries,
    }
    if not args.apply:
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0

    stamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    backup_dir = Path(args.backup_root).expanduser().resolve() / stamp
    backup_dir.mkdir(parents=True, exist_ok=False)
    (backup_dir / "local_items_before_refresh.json").write_text(
        json.dumps(backups, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    with storage._connect() as conn:
        for _batch, rows in batches:
            for item, frozen, result, snapshot_hash in rows:
                conn.execute(
                    "UPDATE original_content_item SET "
                    "frozen_direction_package_json=?, result_json=?, "
                    "item_snapshot_hash=?, updated_at=CURRENT_TIMESTAMP "
                    "WHERE batch_item_id=?",
                    (
                        json.dumps(frozen, ensure_ascii=False, sort_keys=True),
                        json.dumps(result, ensure_ascii=False, sort_keys=True),
                        snapshot_hash,
                        item.batch_item_id,
                    ),
                )

    feishu = []
    if args.write_feishu:
        client = _client(args.script_url)
        for batch, _rows in batches:
            feishu.append(
                {
                    "batch_id": batch.batch_id,
                    **export_ready_batch(
                        batch=batch,
                        items=storage.get_items(batch.batch_id),
                        target_client=client,
                        product_images=(),
                    ),
                }
            )
    output["backup_dir"] = str(backup_dir)
    output["feishu"] = feishu
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
