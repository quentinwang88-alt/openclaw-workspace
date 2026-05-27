from __future__ import annotations

from pathlib import Path

from auto_mixcut.core.ids import new_id
from auto_mixcut.core.result import Result

from .context import SkillContext


class SegmentSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def segment_product(self, product_id: str) -> Result:
        assets = self.ctx.repo.list_where(
            "assets",
            "product_id=? AND probe_status='done' AND COALESCE(has_watermark,'no')!='yes'",
            (product_id,),
        )
        results = [self.segment_asset(a["asset_id"]).to_dict() for a in assets]
        self.ctx.repo.update("content_tasks", "product_id", product_id, {"task_status": "SEGMENTED"})
        return Result.ok({"count": len(results), "results": results})

    def segment_asset(self, asset_id: str) -> Result:
        asset = self.ctx.repo.get("assets", "asset_id", asset_id)
        if not asset:
            return Result.fail("ASSET_NOT_FOUND", "asset not found", {"asset_id": asset_id})
        if asset.get("has_watermark") == "yes":
            return Result.fail("ASSET_REJECTED_WATERMARK", "watermarked asset cannot be segmented", {"asset_id": asset_id})
        duration = int(asset.get("duration_ms") or 3000)
        windows = [(0, min(duration, 3000))] if duration <= 5000 or asset["media_type"] == "image" else _windows(duration)
        product = self.ctx.repo.get("products", "product_id", asset["product_id"]) or {}
        created = []
        for idx, (start, end) in enumerate(windows, start=1):
            segment_id = new_id("SEG")
            local = self.ctx.settings.temp_root / "segments" / asset["product_id"] / f"{segment_id}.mp4"
            local.parent.mkdir(parents=True, exist_ok=True)
            if self.ctx.ffmpeg.mock:
                local.write_bytes(f"mock segment {segment_id}".encode("utf-8"))
            else:
                # Real rendering uses FFmpeg only. Scale/pad to 1080x1920 at 30fps.
                obj = self.ctx.repo.get("oss_objects", "object_id", asset["original_oss_object_id"]) or {}
                source = self.ctx.settings.oss_root / obj["object_key"]
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
            }
            write = self.ctx.repo.upsert("segments", "segment_id", seg_row)
            if not write.success:
                return write
            created.append(segment_id)
        return Result.ok({"asset_id": asset_id, "segments": created})


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


def _default_match(asset):
    if asset["source_trust_level"] == "high" and asset["product_binding_type"] == "exact_sku":
        return "trusted_by_source"
    if asset["source_trust_level"] == "low":
        return "uncertain"
    return "anchor_pass"
