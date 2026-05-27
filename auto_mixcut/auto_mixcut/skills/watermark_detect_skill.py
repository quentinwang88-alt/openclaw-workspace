from __future__ import annotations

from datetime import datetime

from auto_mixcut.core.result import Result

from .context import SkillContext


LOW_TRUST_SOURCES = {"douyin_repost", "competitor", "auto_crawled"}


class WatermarkDetectSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def check_product(self, product_id: str, unknown_policy: str = "review") -> Result:
        assets = self.ctx.repo.list_where("assets", "product_id=? AND probe_status='done'", (product_id,))
        results = [self.check_asset(a["asset_id"], unknown_policy).to_dict() for a in assets]
        self.ctx.repo.update("content_tasks", "product_id", product_id, {"task_status": "WATERMARK_CHECKED"})
        return Result.ok({"count": len(results), "results": results})

    def check_asset(self, asset_id: str, unknown_policy: str = "review") -> Result:
        asset = self.ctx.repo.get("assets", "asset_id", asset_id)
        if not asset:
            return Result.fail("ASSET_NOT_FOUND", "asset not found", {"asset_id": asset_id})
        must_check = asset["source_type"] in LOW_TRUST_SOURCES or asset["source_trust_level"] == "low"
        if not must_check:
            values = {"has_watermark": "no", "asset_status": "watermark_skipped", "watermark_checked_at": _now()}
        else:
            object_row = self.ctx.repo.get("oss_objects", "object_id", asset["original_oss_object_id"]) or {}
            haystack = f"{object_row.get('file_name','')} {object_row.get('object_key','')}".lower()
            hit = any(token in haystack for token in ["watermark", "tiktok", "douyin", "logo", "userid"])
            if hit:
                values = {
                    "has_watermark": "yes",
                    "watermark_type": "platform_or_user_id",
                    "watermark_position": "unknown",
                    "watermark_confidence": "high",
                    "watermark_reason": "mock Tier 2 vision detected platform watermark marker",
                    "risk_level": "high",
                    "asset_status": "rejected_watermark",
                    "watermark_checked_at": _now(),
                }
            else:
                values = {
                    "has_watermark": "no",
                    "watermark_confidence": "medium",
                    "watermark_reason": "mock Tier 2 vision found no watermark marker",
                    "asset_status": "watermark_passed",
                    "watermark_checked_at": _now(),
                }
        res = self.ctx.repo.update("assets", "asset_id", asset_id, values)
        return res if not res.success else Result.ok({"asset_id": asset_id, **values})


def _now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")
