from __future__ import annotations

from pathlib import Path

from auto_mixcut.core.result import Result

from .context import SkillContext


class MediaProbeSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def probe_product(self, product_id: str) -> Result:
        assets = self.ctx.repo.list_where("assets", "product_id=? AND probe_status!='done'", (product_id,))
        results = []
        for asset in assets:
            res = self.probe_asset(asset["asset_id"])
            results.append(res.to_dict())
        self.ctx.repo.update("content_tasks", "product_id", product_id, {"task_status": "PROBED"})
        return Result.ok({"count": len(results), "results": results})

    def probe_asset(self, asset_id: str) -> Result:
        asset = self.ctx.repo.get("assets", "asset_id", asset_id)
        if not asset:
            return Result.fail("ASSET_NOT_FOUND", "asset not found", {"asset_id": asset_id})
        if asset["media_type"] == "image":
            data = {"duration_ms": 3000, "width": 1080, "height": 1920, "fps": 30.0, "codec": "image", "has_audio": False, "orientation": "vertical", "raw": {"image": True}}
        else:
            obj = self.ctx.repo.get("oss_objects", "object_id", asset["original_oss_object_id"])
            if not obj:
                return Result.fail("OSS_OBJECT_NOT_FOUND", "raw object missing", {"asset_id": asset_id})
            path = self.ctx.settings.oss_root / obj["object_key"]
            probed = self.ctx.ffmpeg.probe(path)
            if not probed.success:
                self.ctx.repo.update("assets", "asset_id", asset_id, {"probe_status": "failed"})
                return probed
            data = probed.data
        self.ctx.repo.update(
            "assets",
            "asset_id",
            asset_id,
            {
                "probe_status": "done",
                "duration_ms": data["duration_ms"],
                "width": data["width"],
                "height": data["height"],
                "fps": data["fps"],
                "codec": data["codec"],
                "orientation": data["orientation"],
                "has_audio": int(data["has_audio"]),
                "probe_json": data["raw"],
            },
        )
        return Result.ok({"asset_id": asset_id, **data})
