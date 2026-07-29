from __future__ import annotations

import hashlib
from io import BytesIO
from pathlib import Path

from auto_mixcut.core.ids import new_id
from auto_mixcut.core.result import Result
from auto_mixcut.core.storage_paths import resolve_oss_object_path

from .context import SkillContext


class SegmentFingerprintSkill:
    """Computes lightweight visual fingerprints from sampled segment frames."""

    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def fingerprint_product(self, product_id: str, only_ai_generated: bool = True) -> Result:
        where = "product_id=?"
        params: tuple = (product_id,)
        if only_ai_generated:
            where += " AND source_type='ai_generated'"
        segments = self.ctx.repo.list_where("segments", where, params)
        results = []
        for segment in segments:
            if segment.get("visual_phash"):
                results.append({"segment_id": segment["segment_id"], "phash": segment["visual_phash"], "frame_count": 0, "skipped": True, "reason": "fingerprint_exists"})
                continue
            res = self.fingerprint_segment(segment["segment_id"])
            if not res.success:
                return res
            results.append(res.data)
        return Result.ok({"product_id": product_id, "fingerprinted_segments": len(results), "results": results})

    def fingerprint_segment(self, segment_id: str) -> Result:
        segment = self.ctx.repo.get("segments", "segment_id", segment_id)
        if not segment:
            return Result.fail("SEGMENT_NOT_FOUND", "segment not found", {"segment_id": segment_id})
        if segment.get("visual_phash"):
            return Result.ok({"segment_id": segment_id, "phash": segment["visual_phash"], "frame_count": 0, "skipped": True, "reason": "fingerprint_exists"})
        frame_rows = self.ctx.repo.list_where("segment_frames", "segment_id=? ORDER BY frame_index", (segment_id,))
        frame_bytes = []
        for row in frame_rows:
            local_sample = _local_sampled_frame_path(self.ctx, segment, row)
            if local_sample.exists():
                frame_bytes.append(local_sample.read_bytes())
                continue
            resolved = resolve_oss_object_path(self.ctx, row.get("oss_object_id"), "fingerprint_frame")
            if not resolved.success:
                continue
            path = Path(resolved.data["path"])
            if path.exists():
                frame_bytes.append(path.read_bytes())
        if not frame_bytes:
            return Result.fail("SEGMENT_FRAMES_MISSING", "sampled frames are required before fingerprinting", {"segment_id": segment_id})
        phash, hash_method = _perceptual_hash64(frame_bytes)
        row = {
            "fingerprint_id": new_id("FP"),
            "product_id": segment.get("product_id"),
            "segment_id": segment_id,
            "source_type": segment.get("source_type"),
            "phash": phash,
            "hash_method": hash_method,
            "frame_count": len(frame_bytes),
        }
        table = _ensure_table(self.ctx)
        if not table.success:
            return table
        write = self.ctx.repo.upsert("segment_visual_fingerprints", "fingerprint_id", row)
        if not write.success:
            return write
        self.ctx.repo.update("segments", "segment_id", segment_id, {"visual_phash": phash})
        return Result.ok({"segment_id": segment_id, "phash": phash, "frame_count": len(frame_bytes)})


def _local_sampled_frame_path(ctx: SkillContext, segment: dict, frame: dict) -> Path:
    """Reuse frames produced earlier in the same pipeline run before downloading from OSS."""
    return (
        ctx.settings.temp_root
        / "frames"
        / str(segment.get("product_id") or "")
        / str(segment.get("segment_id") or "")
        / f"frame_{int(frame.get('frame_index') or 0):03d}.jpg"
    )


def _simhash64(frames: list[bytes]) -> str:
    weights = [0] * 64
    for payload in frames:
        digest = hashlib.sha256(payload).digest()
        value = int.from_bytes(digest[:8], "big")
        for bit in range(64):
            weights[bit] += 1 if value & (1 << bit) else -1
    out = 0
    for bit, weight in enumerate(weights):
        if weight >= 0:
            out |= 1 << bit
    return f"{out:016x}"


def _perceptual_hash64(frames: list[bytes]) -> tuple[str, str]:
    """Aggregate 64-bit dHash values; invalid legacy/mock frames use the safe fallback."""
    try:
        from PIL import Image
    except ImportError:
        return _simhash64(frames), "frame_sha256_simhash64_fallback"
    values = []
    for payload in frames:
        try:
            with Image.open(BytesIO(payload)) as image:
                image = image.convert("L").resize((9, 8), Image.Resampling.LANCZOS)
                pixels = list(image.getdata())
            value = 0
            for row in range(8):
                for col in range(8):
                    value = (value << 1) | int(pixels[row * 9 + col] > pixels[row * 9 + col + 1])
            values.append(value)
        except Exception:
            continue
    if not values:
        return _simhash64(frames), "frame_sha256_simhash64_fallback"
    weights = [0] * 64
    for value in values:
        for bit in range(64):
            weights[bit] += 1 if value & (1 << bit) else -1
    aggregate = 0
    for bit, weight in enumerate(weights):
        if weight >= 0:
            aggregate |= 1 << bit
    return f"{aggregate:016x}", "frame_dhash64_majority"


def _ensure_table(ctx: SkillContext) -> Result:
    if getattr(ctx.repo, "dialect", "sqlite") == "mysql":
        try:
            with ctx.repo.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS segment_visual_fingerprints (
                          id BIGINT PRIMARY KEY AUTO_INCREMENT,
                          fingerprint_id VARCHAR(128) NOT NULL UNIQUE,
                          product_id VARCHAR(128),
                          segment_id VARCHAR(128),
                          source_type VARCHAR(64),
                          phash VARCHAR(64),
                          hash_method VARCHAR(64),
                          frame_count INT,
                          created_at DATETIME,
                          updated_at DATETIME
                        )
                        """
                    )
            return Result.ok()
        except Exception as exc:
            return Result.fail("FINGERPRINT_TABLE_FAILED", str(exc))
    try:
        with ctx.repo.connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS segment_visual_fingerprints (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  fingerprint_id TEXT NOT NULL UNIQUE,
                  product_id TEXT,
                  segment_id TEXT,
                  source_type TEXT,
                  phash TEXT,
                  hash_method TEXT,
                  frame_count INTEGER,
                  created_at TEXT,
                  updated_at TEXT
                )
                """
            )
        return Result.ok()
    except Exception as exc:
        return Result.fail("FINGERPRINT_TABLE_FAILED", str(exc))
