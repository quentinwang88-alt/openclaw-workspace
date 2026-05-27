from __future__ import annotations

from pathlib import Path
from typing import Optional

from auto_mixcut.core.ids import new_id
from auto_mixcut.core.result import Result

from .context import SkillContext


class OSSStorageSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def upload_asset(
        self,
        product_id: str,
        file_path: str,
        source_type: str = "self_shot",
        source_trust_level: str = "high",
        product_binding_type: str = "exact_sku",
    ) -> Result:
        gate = _anchor_gate(self.ctx, product_id)
        if not gate.success:
            return gate
        product = self.ctx.repo.get("products", "product_id", product_id) or {}
        source = Path(file_path)
        if not source.exists():
            return Result.fail("LOCAL_FILE_NOT_FOUND", "asset file does not exist", {"file_path": file_path})
        asset_id = new_id("ASSET")
        media_type = "image" if source.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"} else "video"
        object_key = f"auto_mixcut/raw/{product.get('market','NA')}/{product.get('category','uncategorized')}/{product_id}/{asset_id}/{source.name}"
        uploaded = self.ctx.oss.upload(source, object_key)
        if not uploaded.success:
            return uploaded
        oss_row = dict(uploaded.data)
        oss_row.update({"object_type": "raw", "mime_type": _mime(source)})
        oss_write = self.ctx.repo.upsert("oss_objects", "object_id", oss_row)
        if not oss_write.success:
            return oss_write
        asset_write = self.ctx.repo.upsert(
            "assets",
            "asset_id",
            {
                "asset_id": asset_id,
                "product_id": product_id,
                "source_type": source_type,
                "source_trust_level": source_trust_level,
                "product_binding_type": product_binding_type,
                "media_type": media_type,
                "original_oss_object_id": oss_row["object_id"],
                "file_status": "uploaded",
                "asset_status": "uploaded",
                "probe_status": "pending",
                "has_watermark": "pending",
            },
        )
        return asset_write if not asset_write.success else Result.ok({"asset_id": asset_id, "oss_object": oss_row})


def _anchor_gate(ctx: SkillContext, product_id: str) -> Result:
    product = ctx.repo.get("products", "product_id", product_id)
    if product and product.get("anchor_status") == "confirmed":
        return Result.ok()
    return Result.fail("ANCHOR_REQUIRED", "confirmed product anchor is required before upload", {"product_id": product_id})


def _mime(path: Path) -> str:
    return "image/jpeg" if path.suffix.lower() in {".jpg", ".jpeg"} else "video/mp4"
