from __future__ import annotations

import os
from pathlib import Path

from auto_mixcut.core.ids import new_id
from auto_mixcut.core.result import Result
from auto_mixcut.core.storage_paths import resolve_oss_object_path

from .context import SkillContext


class SegmentSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def segment_product(self, product_id: str, source_types: list[str] | None = None) -> Result:
        source_types = [str(item) for item in (source_types or []) if str(item or "").strip()]
        if source_types:
            placeholders = ",".join("?" for _ in source_types)
            assets = self.ctx.repo.list_where(
                "assets",
                f"product_id=? AND probe_status='done' AND has_watermark='no' AND source_type IN ({placeholders})",
                (product_id, *source_types),
            )
        else:
            assets = self.ctx.repo.list_where(
                "assets",
                "product_id=? AND probe_status='done' AND has_watermark='no'",
                (product_id,),
            )
        candidate_assets = [a for a in assets if not self.ctx.repo.list_where("segments", "asset_id=? LIMIT 1", (a["asset_id"],))]
        already_segmented = len(assets) - len(candidate_assets)
        limit = _segment_asset_limit()
        selected = candidate_assets[:limit] if limit > 0 else candidate_assets
        results = [self.segment_asset(a["asset_id"]).to_dict() for a in selected]
        remaining = max(0, len(candidate_assets) - len(selected))
        self.ctx.repo.update("content_tasks", "product_id", product_id, {"task_status": "SEGMENTED"})
        return Result.ok({
            "count": len(results),
            "asset_count": len(assets),
            "already_segmented_count": already_segmented,
            "candidate_asset_count": len(candidate_assets),
            "remaining_candidate_count": remaining,
            "limit": limit,
            "results": results,
        })

    def segment_asset(self, asset_id: str) -> Result:
        asset = self.ctx.repo.get("assets", "asset_id", asset_id)
        if not asset:
            return Result.fail("ASSET_NOT_FOUND", "asset not found", {"asset_id": asset_id})
        if asset.get("has_watermark") == "yes":
            return Result.fail("ASSET_REJECTED_WATERMARK", "watermarked asset cannot be segmented", {"asset_id": asset_id})
        duration = int(asset.get("duration_ms") or 3000)
        windows = [(0, min(duration, 3000))] if duration <= 5000 or asset["media_type"] == "image" else _windows(duration)
        product = self.ctx.repo.get("products", "product_id", asset["product_id"]) or {}
        existing = self.ctx.repo.list_where("segments", "asset_id=?", (asset_id,))
        existing_windows = {
            (int(seg.get("start_ms") or 0), int(seg.get("end_ms") or 0)): seg.get("segment_id")
            for seg in existing
        }
        created = []
        skipped = []
        for idx, (start, end) in enumerate(windows, start=1):
            existing_segment_id = existing_windows.get((start, end))
            if existing_segment_id:
                skipped.append(existing_segment_id)
                continue
            segment_id = new_id("SEG")
            local = self.ctx.settings.temp_root / "segments" / asset["product_id"] / f"{segment_id}.mp4"
            local.parent.mkdir(parents=True, exist_ok=True)
            if self.ctx.ffmpeg.mock:
                local.write_bytes(f"mock segment {segment_id}".encode("utf-8"))
            else:
                # Real rendering uses FFmpeg only. Scale/pad to 1080x1920 at 30fps.
                resolved = resolve_oss_object_path(self.ctx, asset["original_oss_object_id"], "segments_source")
                if not resolved.success:
                    return resolved
                source = Path(resolved.data["path"])
                args = ["-y", "-ss", str(start / 1000), "-to", str(end / 1000), "-i", str(source), "-vf", "scale=1080:1920:force_original_aspect_ratio=decrease,pad=1080:1920:(ow-iw)/2:(oh-ih)/2,fps=30", "-c:v", "libx264", "-pix_fmt", "yuv420p", str(local)]
                rendered = self.ctx.ffmpeg.run(args, "SEGMENT_FAILED")
                if not rendered.success:
                    return rendered
            object_key = f"auto_mixcut/segments/{product.get('market','NA')}/{product.get('category','uncategorized')}/{asset['product_id']}/{segment_id}.mp4"
            upload = self.ctx.oss.upload(local, object_key)
            if not upload.success:
                return upload
            oss_row = dict(upload.data, object_type="segment", mime_type="video/mp4")
            self.ctx.repo.upsert("oss_objects", "object_id", oss_row)
            seg_row = {
                "segment_id": segment_id,
                "asset_id": asset_id,
                "product_id": asset["product_id"],
                "segment_oss_object_id": oss_row["object_id"],
                "start_ms": start,
                "end_ms": end,
                "duration_ms": end - start,
                "width": 1080,
                "height": 1920,
                "fps": 30,
                "segment_status": "created",
                "source_type": asset["source_type"],
                "source_trust_level": asset["source_trust_level"],
                "product_binding_type": asset["product_binding_type"],
                "product_match_status": _default_match(asset),
                "product_match_confidence": "high" if asset["source_trust_level"] == "high" else "medium",
                "is_image_generated": int(asset["media_type"] == "image"),
                "segment_type": asset.get("scene_tag") or "",
                "prompt_package_id": asset.get("prompt_package_id") or "",
                "slot_role": asset.get("slot_role") or "",
                "ai_gen_grade": asset.get("ai_gen_grade") or "",
                "hook_intent": asset.get("hook_intent") or "",
            }
            write = self.ctx.repo.upsert("segments", "segment_id", seg_row)
            if not write.success:
                return write
            created.append(segment_id)
        return Result.ok({"asset_id": asset_id, "segments": created, "skipped_segments": skipped})


def _windows(duration_ms: int):
    limit = min(duration_ms - 300, 30000)
    start = 300
    windows = []
    while start < limit:
        end = min(start + 3000, limit)
        if end - start >= 1500:
            windows.append((start, end))
        start += 3000
    return windows or [(0, min(duration_ms, 3000))]


def _segment_asset_limit() -> int:
    try:
        return max(0, int(os.environ.get("AUTO_MIXCUT_SEGMENT_ASSET_LIMIT", "0") or "0"))
    except ValueError:
        return 0


def _default_match(asset):
    if asset["source_trust_level"] == "high" and asset["product_binding_type"] == "exact_sku":
        return "trusted_by_source"
    if asset["source_trust_level"] == "low":
        return "uncertain"
    return "anchor_pass"
