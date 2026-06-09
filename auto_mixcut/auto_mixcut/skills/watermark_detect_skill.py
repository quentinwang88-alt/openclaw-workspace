from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path

from auto_mixcut.core.result import Result
from auto_mixcut.core.storage_paths import resolve_oss_object_path

from .context import SkillContext

LOW_TRUST_SOURCES = {"douyin_repost", "competitor", "auto_crawled"}


class WatermarkDetectSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def check_product(self, product_id: str, unknown_policy: str = "review", source_types: list[str] | None = None) -> Result:
        source_types = [str(item) for item in (source_types or []) if str(item or "").strip()]
        if source_types:
            placeholders = ",".join("?" for _ in source_types)
            assets = self.ctx.repo.list_where(
                "assets",
                f"product_id=? AND probe_status='done' AND source_type IN ({placeholders})",
                (product_id, *source_types),
            )
        else:
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
        elif not self.ctx.settings.mock_llm:
            return self._check_raw_via_router(asset, unknown_policy)
        else:
            values = self._check_mock(asset)

        res = self.ctx.repo.update("assets", "asset_id", asset_id, values)
        return res if not res.success else Result.ok({"asset_id": asset_id, **values})

    def _check_raw_via_router(self, asset: dict, unknown_policy: str = "review") -> Result:
        from .llm_router_skill import LLMRouterSkill

        sampled = _sample_raw_frames(self.ctx, asset)
        if not sampled.success:
            values = _unknown_values(f"raw frame sampling failed: {sampled.error.message if sampled.error else 'unknown'}", unknown_policy)
            res = self.ctx.repo.update("assets", "asset_id", asset["asset_id"], values)
            return res if not res.success else Result.ok({"asset_id": asset["asset_id"], **values})

        router = LLMRouterSkill(self.ctx)
        image_paths = sampled.data.get("image_paths", [])
        try:
            call = router.call(
                "watermark_detection",
                {"prompt_version": "v1.0", "image_count": len(image_paths), "image_paths": image_paths},
                product_id=asset.get("product_id", ""),
                asset_id=asset["asset_id"],
            )
        finally:
            _cleanup_sampled_frames(sampled.data.get("sample_dir"))

        if not call.success:
            values = _unknown_values(f"router check failed: {call.error.message if call.error else 'unknown'}", unknown_policy)
        else:
            data = call.data.get("response", {})
            has_wm = str(data.get("has_watermark", "unknown"))
            confidence = str(data.get("confidence", "medium"))
            has_watermark = _to_enum(has_wm, ["yes", "no", "unknown"])
            values = {
                "has_watermark": has_watermark,
                "watermark_type": str(data.get("watermark_type", "")),
                "watermark_position": str(data.get("watermark_position", "")),
                "watermark_confidence": _to_enum(confidence, ["high", "medium", "low"]),
                "watermark_reason": str(data.get("reason", "")),
                "watermark_checked_at": _now(),
                "asset_status": _watermark_asset_status(has_watermark),
            }
            if has_watermark == "yes":
                values["risk_level"] = "high"

        res = self.ctx.repo.update("assets", "asset_id", asset["asset_id"], values)
        return res if not res.success else Result.ok({"asset_id": asset["asset_id"], **values})

    def _check_mock(self, asset: dict) -> dict:
        object_row = self.ctx.repo.get("oss_objects", "object_id", asset["original_oss_object_id"]) or {}
        haystack = f"{object_row.get('file_name','')} {object_row.get('object_key','')}".lower()
        hit = any(token in haystack for token in ["watermark", "tiktok", "douyin", "logo", "userid"])
        if hit:
            return {
                "has_watermark": "yes",
                "watermark_type": "platform_or_user_id",
                "watermark_position": "unknown",
                "watermark_confidence": "high",
                "watermark_reason": "mock Tier 2 vision detected platform watermark marker",
                "risk_level": "high",
                "asset_status": "rejected_watermark",
                "watermark_checked_at": _now(),
            }
        return {
            "has_watermark": "no",
            "watermark_confidence": "medium",
            "watermark_reason": "mock Tier 2 vision found no watermark marker",
            "asset_status": "watermark_passed",
            "watermark_checked_at": _now(),
        }

    def _fallback_mock(self, asset: dict) -> Result:
        values = _unknown_values("no frames available for vision check")
        res = self.ctx.repo.update("assets", "asset_id", asset["asset_id"], values)
        return res if not res.success else Result.ok({"asset_id": asset["asset_id"], **values})


def _now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def _to_enum(value: str, allowed: list[str]) -> str:
    text = str(value or "").strip().lower()
    return text if text in allowed else allowed[-1] if allowed else "unknown"


def _watermark_asset_status(has_watermark: str) -> str:
    if has_watermark == "yes":
        return "rejected_watermark"
    if has_watermark == "no":
        return "watermark_passed"
    return "watermark_needs_review"


def _unknown_values(reason: str, unknown_policy: str = "review") -> dict:
    status = "rejected_watermark" if unknown_policy == "reject" else "watermark_needs_review"
    values = {
        "has_watermark": "unknown",
        "watermark_confidence": "low",
        "watermark_reason": reason,
        "asset_status": status,
        "watermark_checked_at": _now(),
    }
    if unknown_policy == "reject":
        values["risk_level"] = "high"
    return values


def _sample_raw_frames(ctx: SkillContext, asset: dict) -> Result:
    resolved = resolve_oss_object_path(ctx, asset.get("original_oss_object_id"), "watermark_source")
    if not resolved.success:
        return resolved
    source = Path(resolved.data["path"])

    sample_dir = ctx.settings.temp_root / "watermark" / str(asset.get("product_id") or "unknown") / str(asset.get("asset_id") or "asset")
    sample_dir.mkdir(parents=True, exist_ok=True)

    if asset.get("media_type") == "image":
        suffix = source.suffix.lower() if source.suffix else ".jpg"
        frame_path = sample_dir / f"raw_frame_001{suffix}"
        shutil.copy2(source, frame_path)
        return Result.ok({"image_paths": [str(frame_path)], "sample_dir": str(sample_dir)})

    duration_ms = int(asset.get("duration_ms") or 0)
    timestamps = _sample_timestamps(duration_ms)
    image_paths = []
    for idx, timestamp_ms in enumerate(timestamps, start=1):
        frame_path = sample_dir / f"raw_frame_{idx:03d}.jpg"
        if ctx.ffmpeg.mock:
            frame_path.write_bytes(b"\xff\xd8\xff\xe0mock-watermark-frame\xff\xd9")
        else:
            sampled = ctx.ffmpeg.run(
                ["-y", "-ss", f"{timestamp_ms / 1000:.3f}", "-i", str(source), "-frames:v", "1", "-q:v", "2", str(frame_path)],
                "WATERMARK_FRAME_SAMPLE_FAILED",
            )
            if not sampled.success:
                return sampled
        if frame_path.exists() and frame_path.stat().st_size > 0:
            image_paths.append(str(frame_path))
    if not image_paths:
        return Result.fail("WATERMARK_FRAME_SAMPLE_FAILED", "no raw frames sampled", {"asset_id": asset.get("asset_id")})
    return Result.ok({"image_paths": image_paths, "sample_dir": str(sample_dir)})


def _sample_timestamps(duration_ms: int) -> list[int]:
    if duration_ms <= 0:
        return [500, 1500, 2500]
    safe_end = max(0, duration_ms - 500)
    candidates = [500, duration_ms // 2, safe_end]
    timestamps = []
    for value in candidates:
        ts = max(0, min(safe_end, int(value)))
        if ts not in timestamps:
            timestamps.append(ts)
    return timestamps or [0]


def _cleanup_sampled_frames(sample_dir: str | None) -> None:
    if not sample_dir:
        return
    try:
        shutil.rmtree(Path(sample_dir))
    except OSError:
        pass
