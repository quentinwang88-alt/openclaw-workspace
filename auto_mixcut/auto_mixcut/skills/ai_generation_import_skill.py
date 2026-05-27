from __future__ import annotations

from pathlib import Path
from typing import List

from auto_mixcut.core.ids import new_id
from auto_mixcut.core.result import Result

from .context import SkillContext


class AIGenerationImportSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def import_generated_assets(self, job_id: str) -> Result:
        job = self.ctx.repo.get("ai_generation_jobs", "job_id", job_id)
        if not job:
            return Result.fail("JOB_NOT_FOUND", "ai_generation_jobs not found", {"job_id": job_id})
        if job.get("status") != "GENERATED":
            return Result.fail("JOB_NOT_GENERATED", "generation not completed", {"job_id": job_id, "status": job.get("status")})

        self.ctx.repo.update("ai_generation_jobs", "job_id", job_id, {"status": "IMPORTING"})

        product_id = str(job.get("product_id") or "")
        product = self.ctx.repo.get("products", "product_id", product_id) or {}
        segment_type = str(job.get("segment_type") or "")
        prompt_text = str(job.get("prompt_text") or "")
        generation_model = str(job.get("model_name") or "")
        generation_type = str(job.get("generation_type") or "text_to_video")
        market = str(product.get("market") or "NA")
        category = str(product.get("category") or "uncategorized")

        temp_dir = self.ctx.settings.temp_root / "ai_generated" / product_id / job_id
        files = sorted(temp_dir.glob("*.mp4")) if temp_dir.exists() else []
        if not files:
            return Result.fail("NO_GENERATED_FILES", "no generated files found", {"job_id": job_id, "temp_dir": str(temp_dir)})

        imported_segments: List[str] = []
        imported_assets: List[str] = []
        for f in files:
            asset_id = new_id("ASSET")
            segment_id = new_id("SEG")

            object_key = f"auto_mixcut/ai_generated/{market}/{category}/{product_id}/{asset_id}.mp4"
            upload = self.ctx.oss.upload(f, object_key)
            if not upload.success:
                continue

            oss_row = dict(upload.data, object_type="ai_generated_asset", mime_type="video/mp4")
            self.ctx.repo.upsert("oss_objects", "object_id", oss_row)

            asset_row = {
                "asset_id": asset_id,
                "product_id": product_id,
                "source_type": "ai_generated",
                "source_trust_level": "medium",
                "product_binding_type": "same_style",
                "media_type": "video",
                "original_oss_object_id": oss_row["object_id"],
                "file_status": "uploaded",
                "probe_status": "pending",
                "has_watermark": "no",
                "risk_level": "medium",
                "asset_status": "active",
                "generation_job_id": job_id,
                "generation_type": generation_type,
                "generation_model": generation_model,
                "generation_prompt": prompt_text,
            }
            write_asset = self.ctx.repo.upsert("assets", "asset_id", asset_row)
            if not write_asset.success:
                continue
            imported_assets.append(asset_id)

            seg_row = {
                "segment_id": segment_id,
                "asset_id": asset_id,
                "product_id": product_id,
                "segment_oss_object_id": oss_row["object_id"],
                "start_ms": 0,
                "end_ms": 5000,
                "duration_ms": 5000,
                "width": 1080,
                "height": 1920,
                "fps": 30,
                "segment_status": "created",
                "source_type": "ai_generated",
                "source_trust_level": "medium",
                "product_binding_type": "same_style",
                "product_match_status": "uncertain",
                "product_match_confidence": "medium",
                "is_image_generated": 1,
                "segment_type": segment_type,
            }
            write_seg = self.ctx.repo.upsert("segments", "segment_id", seg_row)
            if not write_seg.success:
                continue
            imported_segments.append(segment_id)

        self.ctx.repo.update("ai_generation_jobs", "job_id", job_id, {
            "status": "IMPORTED",
            "imported_segment_count": len(imported_segments),
            "generated_count": len(imported_assets),
        })

        return Result.ok({
            "job_id": job_id,
            "imported_assets": len(imported_assets),
            "imported_segments": len(imported_segments),
            "asset_ids": imported_assets,
            "segment_ids": imported_segments,
        })
