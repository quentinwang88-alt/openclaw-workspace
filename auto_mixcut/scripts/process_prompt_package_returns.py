#!/usr/bin/env python3
"""End-to-end patrol for returned Prompt Package videos.

The script is safe to run repeatedly. Expensive post-processing steps skip
records that already have outputs unless --force-postprocess is set.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
import re
from typing import Any, Dict, List, Set


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from auto_mixcut.core.bootstrap import build_context  # noqa: E402
from auto_mixcut.skills.ai_anchor_check_skill import AIAnchorCheckSkill  # noqa: E402
from auto_mixcut.skills.ai_generated_consistency_skill import AIGeneratedConsistencySkill  # noqa: E402
from auto_mixcut.skills.ai_generation_qc_skill import AIGenerationQCSkill  # noqa: E402
from auto_mixcut.skills.ai_tagging_skill import AITaggingSkill  # noqa: E402
from auto_mixcut.skills.effective_role_skill import EffectiveRoleSkill  # noqa: E402
from auto_mixcut.skills.media_probe_skill import MediaProbeSkill  # noqa: E402
from auto_mixcut.skills.readiness_check_skill import ReadinessCheckSkill  # noqa: E402


IMPORTER_PATH = ROOT / "scripts" / "import_prompt_package_returns.py"
importer_spec = importlib.util.spec_from_file_location("prompt_package_return_importer", IMPORTER_PATH)
if importer_spec is None or importer_spec.loader is None:
    raise RuntimeError(f"无法加载导入脚本: {IMPORTER_PATH}")
importer = importlib.util.module_from_spec(importer_spec)
importer_spec.loader.exec_module(importer)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=importer.DEFAULT_URL)
    parser.add_argument("--product-id")
    parser.add_argument("--segment-prompt-id")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force-import", action="store_true")
    parser.add_argument("--force-frames", action="store_true")
    parser.add_argument("--force-postprocess", action="store_true")
    parser.add_argument("--no-import", action="store_true")
    parser.add_argument("--no-postprocess", action="store_true")
    args = parser.parse_args()

    ctx = build_context()
    imported: List[Dict[str, Any]] = []
    product_ids: Set[str] = set()

    if not args.no_import:
        client = importer.resolve_client(args.url)
        records = client.list_records(page_size=500)
        for record in records:
            fields = record.fields or {}
            prompt_id = importer.text(fields.get(importer.FIELD_PROMPT_ID))
            product_id = importer.text(fields.get(importer.FIELD_PRODUCT_ID))
            if args.product_id and product_id != args.product_id:
                continue
            if args.segment_prompt_id and prompt_id != args.segment_prompt_id:
                continue
            if not prompt_id or not product_id:
                continue
            files = importer.attachments(fields.get(importer.FIELD_ATTACHMENT))
            if not files:
                continue
            status = importer.text(fields.get(importer.FIELD_STATUS))
            result_sync = importer.text(fields.get(importer.FIELD_RESULT_SYNC))
            if status not in importer.IMPORTED_STATUSES and result_sync not in importer.RESULT_SYNC_READY:
                continue

            res = importer.import_record(ctx, client, record.record_id, fields, files[0], dry_run=args.dry_run, force=args.force_import)
            imported.append(res)
            product_ids.add(product_id)
            if args.limit and len(imported) >= args.limit:
                break

    if args.product_id:
        product_ids.add(args.product_id)
    for item in imported:
        if item.get("product_id"):
            product_ids.add(str(item["product_id"]))

    postprocess: List[Dict[str, Any]] = []
    if not args.no_postprocess and not args.dry_run:
        for product_id in sorted(product_ids):
            postprocess.append(process_product(ctx, product_id, force=args.force_postprocess, force_frames=args.force_frames))

    result = {
        "import": {"count": len(imported), "results": imported},
        "postprocess": postprocess,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    failed = any(item.get("status") == "failed" for item in imported)
    failed = failed or any(not step.get("success", True) for item in postprocess for step in item.get("steps", []))
    return 1 if failed else 0


def process_product(ctx: Any, product_id: str, force: bool = False, force_frames: bool = False) -> Dict[str, Any]:
    steps: List[Dict[str, Any]] = []

    def run_step(name: str, fn):
        res = fn()
        item = {"step": name, **res.to_dict()}
        steps.append(item)
        return res

    run_step("probe", lambda: MediaProbeSkill(ctx).probe_product(product_id, source_types=["ai_generated"]))
    metadata = sync_prompt_package_metadata(ctx, product_id)
    steps.append({"step": "sync_prompt_package_metadata", "success": metadata["success"], "error": None, "data": metadata})
    sync = sync_returned_segments_from_probe(ctx, product_id)
    steps.append({"step": "sync_segment_probe", "success": sync["success"], "error": None, "data": sync})
    force_frames = force_frames or force or bool(sync.get("updated")) or bool(metadata.get("updated_segments"))

    for name, fn in [
        ("frame_sample", lambda: sample_ai_frames(ctx, product_id, force=force_frames)),
        ("ai_tag_submit", lambda: maybe_submit_tags(ctx, product_id, force=force)),
        ("ai_tag_poll", lambda: AITaggingSkill(ctx).poll_results(product_id, force=force, source_types=["ai_generated"])),
        ("consistency", lambda: AIGeneratedConsistencySkill(ctx).check_product(product_id, force=force)),
        ("qc", lambda: AIGenerationQCSkill(ctx).check_product(product_id)),
        ("anchor_check", lambda: AIAnchorCheckSkill(ctx).check_product(product_id, force=force)),
        ("effective_roles", lambda: EffectiveRoleSkill(ctx).compute_product(product_id, source_types=["ai_generated"])),
        ("readiness", lambda: ReadinessCheckSkill(ctx).check_product(product_id)),
    ]:
        res = run_step(name, fn)
        if not res.success:
            break

    return {"product_id": product_id, "steps": steps}


def maybe_submit_tags(ctx: Any, product_id: str, force: bool = False):
    from auto_mixcut.core.result import Result

    segments = ctx.repo.list_where("segments", "product_id=? AND source_type='ai_generated'", (product_id,))
    missing = []
    for segment in segments:
        tags = ctx.repo.list_where("segment_tags", "segment_id=? ORDER BY id DESC LIMIT 1", (segment["segment_id"],))
        if not tags:
            missing.append(segment["segment_id"])
    if not missing and not force:
        return Result.ok({"skipped": True, "reason": "tags_exist", "missing_segments": []})
    return AITaggingSkill(ctx).submit_batch(product_id, source_types=["ai_generated"])


def sample_ai_frames(ctx: Any, product_id: str, force: bool = False):
    from auto_mixcut.core.result import Result
    from auto_mixcut.skills.frame_sample_skill import FrameSampleSkill

    segments = ctx.repo.list_where("segments", "product_id=? AND source_type='ai_generated' AND segment_status IN ('created','qc_passed','qc_failed')", (product_id,))
    skill = FrameSampleSkill(ctx)
    results = []
    for segment in segments:
        frame_count = len(ctx.repo.list_where("segment_frames", "segment_id=?", (segment["segment_id"],)))
        if not force and frame_count >= 6:
            results.append({"success": True, "error": None, "data": {"segment_id": segment["segment_id"], "skipped": True, "reason": "sufficient_frames", "frame_count": frame_count}})
            continue
        results.append(skill.sample_segment(segment["segment_id"], force=force).to_dict())
    ctx.repo.update("content_tasks", "product_id", product_id, {"task_status": "FRAMES_SAMPLED"})
    return Result.ok({"count": len(results), "results": results, "source_types": ["ai_generated"]})


def sync_prompt_package_metadata(ctx: Any, product_id: str) -> Dict[str, Any]:
    updated_assets = []
    updated_segments = []
    packages = ctx.repo.list_where("segment_prompt_packages", "product_id=?", (product_id,))
    package_by_id = {str(row.get("segment_prompt_id") or ""): row for row in packages}
    assets = ctx.repo.list_where("assets", "product_id=? AND source_type='ai_generated'", (product_id,))
    for asset in assets:
        prompt_id = str(asset.get("prompt_package_id") or asset.get("source_identity") or "")
        if not prompt_id:
            prompt_id = infer_prompt_package_id_from_asset(ctx, product_id, asset)
        package = package_by_id.get(prompt_id)
        if not package:
            continue
        asset_patch = _package_asset_patch(prompt_id, package)
        if _needs_patch(asset, asset_patch):
            ctx.repo.update("assets", "asset_id", asset["asset_id"], asset_patch)
            updated_assets.append({"asset_id": asset["asset_id"], "prompt_package_id": prompt_id, "segment_type": asset_patch.get("scene_tag")})
        segments = ctx.repo.list_where("segments", "asset_id=?", (asset["asset_id"],))
        for segment in segments:
            segment_patch = _package_segment_patch(prompt_id, package)
            if _needs_patch(segment, segment_patch):
                ctx.repo.update("segments", "segment_id", segment["segment_id"], segment_patch)
                updated_segments.append({"segment_id": segment["segment_id"], "prompt_package_id": prompt_id, "segment_type": segment_patch.get("segment_type")})
    return {"success": True, "updated_assets": updated_assets, "updated_segments": updated_segments}


def infer_prompt_package_id_from_asset(ctx: Any, product_id: str, asset: Dict[str, Any]) -> str:
    text_parts = [str(asset.get("generation_prompt") or "")]
    obj = ctx.repo.get("oss_objects", "object_id", asset.get("original_oss_object_id")) if asset.get("original_oss_object_id") else {}
    if obj:
        text_parts.extend([str(obj.get("file_name") or ""), str(obj.get("object_key") or "")])
    text_value = " ".join(text_parts)
    package_ids = [str(row.get("segment_prompt_id") or "") for row in ctx.repo.list_where("segment_prompt_packages", "product_id=?", (product_id,))]
    for package_id in package_ids:
        if package_id and package_id in text_value:
            return package_id
    for match in re.findall(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4,12}", text_value):
        for package_id in package_ids:
            if package_id.startswith(match) or match.startswith(package_id[: len(match)]):
                return package_id
    for package_id in package_ids:
        if package_id and package_id[:8] in text_value:
            return package_id
    return ""


def _package_asset_patch(prompt_id: str, package: Dict[str, Any]) -> Dict[str, Any]:
    patch = {
        "prompt_package_id": prompt_id,
        "source_identity": prompt_id,
        "scene_tag": str(package.get("segment_type") or ""),
        "slot_role": str(package.get("slot_role") or ""),
        "ai_gen_grade": str(package.get("ai_gen_grade") or ""),
        "hook_intent": str(package.get("hook_intent") or ""),
    }
    prompt_json = package.get("prompt_package_json") or {}
    prompt = prompt_json.get("prompt") if isinstance(prompt_json, dict) else {}
    if prompt:
        patch.setdefault("generation_type", "image_to_video")
        patch.setdefault("generation_model", "jimeng")
        patch["generation_prompt"] = json.dumps(prompt, ensure_ascii=False) if isinstance(prompt, dict) else str(prompt)
    return patch


def _package_segment_patch(prompt_id: str, package: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "prompt_package_id": prompt_id,
        "segment_type": str(package.get("segment_type") or ""),
        "slot_role": str(package.get("slot_role") or ""),
        "ai_gen_grade": str(package.get("ai_gen_grade") or ""),
        "hook_intent": str(package.get("hook_intent") or ""),
    }


def _needs_patch(row: Dict[str, Any], patch: Dict[str, Any]) -> bool:
    for key, value in patch.items():
        if value and str(row.get(key) or "") != str(value):
            return True
    return False


def sync_returned_segments_from_probe(ctx: Any, product_id: str) -> Dict[str, Any]:
    updated = []
    skipped = []
    segments = ctx.repo.list_where("segments", "product_id=? AND source_type='ai_generated'", (product_id,))
    for segment in segments:
        asset = ctx.repo.get("assets", "asset_id", segment.get("asset_id")) or {}
        if not asset or asset.get("probe_status") != "done":
            skipped.append({"segment_id": segment.get("segment_id"), "reason": "asset_not_probed"})
            continue
        if asset.get("original_oss_object_id") != segment.get("segment_oss_object_id"):
            skipped.append({"segment_id": segment.get("segment_id"), "reason": "not_full_asset_segment"})
            continue
        duration_ms = int(asset.get("duration_ms") or segment.get("duration_ms") or 0)
        values = {
            "start_ms": 0,
            "end_ms": duration_ms,
            "duration_ms": duration_ms,
            "width": int(asset.get("width") or segment.get("width") or 0),
            "height": int(asset.get("height") or segment.get("height") or 0),
            "fps": float(asset.get("fps") or segment.get("fps") or 0),
        }
        if all(_same_value(segment.get(key), value) for key, value in values.items()):
            skipped.append({"segment_id": segment.get("segment_id"), "reason": "segment_probe_already_synced"})
            continue
        ctx.repo.update("segments", "segment_id", segment["segment_id"], values)
        updated.append({"segment_id": segment["segment_id"], **values})
    return {"success": True, "updated": updated, "skipped": skipped}


def _same_value(left: Any, right: Any) -> bool:
    try:
        if isinstance(right, float):
            return abs(float(left or 0) - right) < 0.001
        if isinstance(right, int):
            return int(float(left or 0)) == right
    except (TypeError, ValueError):
        return False
    return left == right


if __name__ == "__main__":
    raise SystemExit(main())
