from __future__ import annotations

from pathlib import Path

from auto_mixcut.core.result import Result
from auto_mixcut.core.storage_paths import require_oss_object_path

from .context import SkillContext


class WatermarkProcessSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def process_product(self, product_id: str, source_types: list[str] | None = None) -> Result:
        source_types = [str(item) for item in (source_types or []) if str(item or "").strip()]
        if source_types:
            placeholders = ",".join("?" for _ in source_types)
            assets = self.ctx.repo.list_where(
                "assets",
                f"product_id=? AND probe_status='done' AND has_watermark='yes' AND source_type IN ({placeholders})",
                (product_id, *source_types),
            )
        else:
            assets = self.ctx.repo.list_where(
                "assets",
                "product_id=? AND probe_status='done' AND has_watermark='yes'",
                (product_id,),
            )
        results = [self.process_asset(asset["asset_id"]).to_dict() for asset in assets]
        self.ctx.repo.update("content_tasks", "product_id", product_id, {"task_status": "WATERMARK_PROCESSED"})
        return Result.ok({"count": len(results), "results": results})

    def process_asset(self, asset_id: str) -> Result:
        asset = self.ctx.repo.get("assets", "asset_id", asset_id)
        if not asset:
            return Result.fail("ASSET_NOT_FOUND", "asset not found", {"asset_id": asset_id})
        if asset.get("has_watermark") != "yes":
            return Result.ok({"asset_id": asset_id, "processed": False, "reason": "no watermark"})
        if asset.get("normalized_oss_object_id"):
            return Result.ok({"asset_id": asset_id, "processed": True, "existing": True, "normalized_oss_object_id": asset["normalized_oss_object_id"]})

        source = require_oss_object_path(self.ctx, asset.get("original_oss_object_id"), "watermark_assets")
        if not source or not source.exists():
            return Result.fail("RAW_FILE_NOT_FOUND", "raw asset file missing from OSS/cache", {"asset_id": asset_id, "object_id": asset.get("original_oss_object_id")})

        target = self.ctx.settings.temp_root / "watermark_processed" / asset["product_id"] / f"{asset_id}.mp4"
        target.parent.mkdir(parents=True, exist_ok=True)
        rendered = self._render_processed(source, target, asset)
        if not rendered.success:
            self.ctx.repo.update("assets", "asset_id", asset_id, {"asset_status": "watermark_process_failed"})
            return rendered

        product = self.ctx.repo.get("products", "product_id", asset["product_id"]) or {}
        key = (
            f"auto_mixcut/normalized/{product.get('market','NA')}/{product.get('category','uncategorized')}/"
            f"{asset['product_id']}/{asset_id}/watermark_processed.mp4"
        )
        upload = self.ctx.oss.upload(target, key)
        if not upload.success:
            return upload
        oss_row = dict(upload.data, object_type="normalized_asset", mime_type="video/mp4")
        write = self.ctx.repo.upsert("oss_objects", "object_id", oss_row)
        if not write.success:
            return write
        values = {
            "normalized_oss_object_id": oss_row["object_id"],
            "has_watermark": "processed",
            "asset_status": "watermark_processed",
            "watermark_reason": _append_reason(asset.get("watermark_reason"), "auto processed by center-crop normalization"),
        }
        updated = self.ctx.repo.update("assets", "asset_id", asset_id, values)
        if not updated.success:
            return updated
        return Result.ok({"asset_id": asset_id, "processed": True, "normalized_oss_object_id": oss_row["object_id"], "object_key": key})

    def _render_processed(self, source: Path, target: Path, asset: dict) -> Result:
        if self.ctx.ffmpeg.mock:
            target.write_bytes(b"mock watermark processed video")
            return Result.ok({"mock": True, "path": str(target)})

        video_filter = (
            "crop=iw*0.90:ih*0.90:iw*0.05:ih*0.05,"
            "scale=1080:1920:force_original_aspect_ratio=increase,"
            "crop=1080:1920,fps=30"
        )
        if asset.get("media_type") == "image":
            args = [
                "-y",
                "-loop",
                "1",
                "-t",
                str(max(1, int(asset.get("duration_ms") or 3000)) / 1000),
                "-i",
                str(source),
                "-vf",
                video_filter,
                "-c:v",
                "libx264",
                "-pix_fmt",
                "yuv420p",
                "-an",
                str(target),
            ]
        else:
            args = [
                "-y",
                "-i",
                str(source),
                "-vf",
                video_filter,
                "-c:v",
                "libx264",
                "-pix_fmt",
                "yuv420p",
                "-an",
                str(target),
            ]
        return self.ctx.ffmpeg.run(args, "WATERMARK_PROCESS_FAILED")


def _append_reason(reason: str | None, suffix: str) -> str:
    if reason:
        return f"{reason}; {suffix}"
    return suffix
