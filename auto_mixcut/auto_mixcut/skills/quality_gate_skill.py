from __future__ import annotations

import re
import subprocess

from auto_mixcut.core.result import Result

from .context import SkillContext


class QualityGateSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def check_batch(self, batch_id: str) -> Result:
        outputs = self.ctx.repo.list_where("outputs", "batch_id=?", (batch_id,))
        results = [self.check_output(o["output_id"]).to_dict() for o in outputs]
        return Result.ok({"batch_id": batch_id, "results": results})

    def check_output(self, output_id: str) -> Result:
        output = self.ctx.repo.get("outputs", "output_id", output_id)
        if not output:
            return Result.fail("OUTPUT_NOT_FOUND", "output not found", {"output_id": output_id})
        slots = self.ctx.repo.list_where("output_segments", "output_id=? ORDER BY slot_index", (output_id,))
        reasons = []
        first = _segment_bundle(self.ctx, slots[0]["segment_id"]) if slots else None
        if output.get("duration_ms") < 14500 or output.get("duration_ms") > 15500:
            reasons.append("duration out of range")
        if output.get("width") != 1080 or output.get("height") != 1920:
            reasons.append("resolution is not 1080x1920")
        volume = _audio_volume(self.ctx, output)
        if volume is None:
            reasons.append("audio volume could not be measured")
        elif volume < -34.0:
            reasons.append(f"audio mean volume too low ({volume:.1f} dB)")
        if first:
            tag = first["tag"]
            segment = first["segment"]
            roles = segment.get("effective_roles_json") or []
            if tag.get("product_visibility") != "high":
                reasons.append("first segment product visibility is not high")
            if tag.get("risk_level") != "low":
                reasons.append("first segment risk is not low")
            if tag.get("hook_strength") not in {"strong", "medium"}:
                reasons.append("first segment hook is weak")
            if segment.get("product_match_status") not in {"trusted_by_source", "anchor_pass"}:
                reasons.append("first segment product match is not trusted")
            if not set(roles).intersection({"hero", "result", "detail"}):
                reasons.append("first segment lacks core effective role")
        used_roles = {s["role_used"] for s in slots}
        if "result" not in used_roles:
            reasons.append("missing result segment")
        if "detail" not in used_roles:
            reasons.append("missing detail segment")
        assets = {s["asset_id"] for s in slots if s.get("asset_id")}
        if len(assets) < 3:
            reasons.append("unique_source_assets < 3")
        status = "passed" if not reasons else "failed"
        self.ctx.repo.update("outputs", "output_id", output_id, {"machine_quality_status": status})
        return Result.ok({"output_id": output_id, "machine_quality_status": status, "score": 100 if status == "passed" else 60, "reasons": reasons})


def _segment_bundle(ctx: SkillContext, segment_id: str):
    segment = ctx.repo.get("segments", "segment_id", segment_id) or {}
    tags = ctx.repo.list_where("segment_tags", "segment_id=? ORDER BY id DESC", (segment_id,))
    return {"segment": segment, "tag": tags[0] if tags else {}}


def _audio_volume(ctx: SkillContext, output: dict) -> float | None:
    obj = ctx.repo.get("oss_objects", "object_id", output.get("output_oss_object_id"))
    if not obj:
        return None
    path = ctx.settings.oss_root / obj["object_key"]
    if not path.exists():
        return None
    proc = subprocess.run(
        ["ffmpeg", "-hide_banner", "-i", str(path), "-af", "volumedetect", "-vn", "-sn", "-dn", "-f", "null", "-"],
        capture_output=True,
        text=True,
    )
    match = re.search(r"mean_volume:\s*(-?\d+(?:\.\d+)?) dB", proc.stderr)
    return float(match.group(1)) if match else None
