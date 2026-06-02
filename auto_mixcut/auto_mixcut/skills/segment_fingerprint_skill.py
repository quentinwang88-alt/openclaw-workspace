from __future__ import annotations

import hashlib

from auto_mixcut.core.ids import new_id
from auto_mixcut.core.result import Result

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
            res = self.fingerprint_segment(segment["segment_id"])
            if not res.success:
                return res
            results.append(res.data)
        return Result.ok({"product_id": product_id, "fingerprinted_segments": len(results), "results": results})

    def fingerprint_segment(self, segment_id: str) -> Result:
        segment = self.ctx.repo.get("segments", "segment_id", segment_id)
        if not segment:
            return Result.fail("SEGMENT_NOT_FOUND", "segment not found", {"segment_id": segment_id})
        frame_rows = self.ctx.repo.list_where("segment_frames", "segment_id=? ORDER BY frame_index", (segment_id,))
        frame_bytes = []
        for row in frame_rows:
            obj = self.ctx.repo.get("oss_objects", "object_id", row.get("oss_object_id"))
            if not obj:
                continue
            path = self.ctx.settings.oss_root / obj["object_key"]
            if path.exists():
                frame_bytes.append(path.read_bytes())
        if not frame_bytes:
            return Result.fail("SEGMENT_FRAMES_MISSING", "sampled frames are required before fingerprinting", {"segment_id": segment_id})
        phash = _simhash64(frame_bytes)
        row = {
            "fingerprint_id": new_id("FP"),
            "product_id": segment.get("product_id"),
            "segment_id": segment_id,
            "source_type": segment.get("source_type"),
            "phash": phash,
            "hash_method": "frame_sha256_simhash64",
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
