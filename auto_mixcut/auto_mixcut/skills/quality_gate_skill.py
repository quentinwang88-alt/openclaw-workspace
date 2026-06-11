from __future__ import annotations

import re
import subprocess

from auto_mixcut.core.result import Result
from auto_mixcut.core.storage_paths import require_oss_object_path

from .context import SkillContext
from .usage_counter_skill import refresh_output_segment_usage


AUDIO_MEAN_MIN_DB = -16.0
AUDIO_TAIL_MIN_DB = -16.0


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
        expected_duration = _expected_duration_ms(slots)
        actual_duration = int(output.get("duration_ms") or 0)
        if expected_duration and abs(actual_duration - expected_duration) > 500:
            reasons.append("duration does not match render plan")
        elif actual_duration < 12000 or actual_duration > 30000:
            reasons.append("duration out of supported range")
        if output.get("width") != 1080 or output.get("height") != 1920:
            reasons.append("resolution is not 1080x1920")
        volume = _audio_volume(self.ctx, output)
        if volume is None:
            reasons.append("audio volume could not be measured")
        elif volume < AUDIO_MEAN_MIN_DB:
            reasons.append(f"audio mean volume too low ({volume:.1f} dB)")
        reasons.extend(_audio_tail_window_reasons(self.ctx, output, actual_duration))
        if first:
            tag = first["tag"]
            segment = first["segment"]
            roles = segment.get("effective_roles_json") or []
            if tag.get("product_visibility") != "high":
                reasons.append("first segment product visibility is not high")
            ai_anchor_trusted = segment.get("source_type") == "ai_generated" and segment.get("anchor_match_level") == "strict_pass"
            trusted_real_first = _trusted_real_first_segment(first)
            if tag.get("risk_level") != "low" and not ai_anchor_trusted and not trusted_real_first:
                reasons.append("first segment risk is not low")
            if tag.get("hook_strength") not in {"strong", "medium"}:
                reasons.append("first segment hook is weak")
            if (
                segment.get("product_match_status") not in {"trusted_by_source", "anchor_pass"}
                and not ai_anchor_trusted
                and not _low_trust_first_slot_review_candidate(first)
            ):
                reasons.append("first segment product match is not trusted")
            if not set(roles).intersection({"hero", "result", "detail"}):
                reasons.append("first segment lacks core effective role")
        used_roles = {s["role_used"] for s in slots}
        required_roles = _planned_roles(self.ctx, output, slots)
        if "result" in required_roles and "result" not in used_roles:
            reasons.append("missing result segment")
        if "detail" in required_roles and "detail" not in used_roles:
            reasons.append("missing detail segment")
        assets = {s["asset_id"] for s in slots if s.get("asset_id")}
        if len(assets) < 3:
            reasons.append("unique_source_assets < 3")
        status = "passed" if not reasons else "failed"
        self.ctx.repo.update("outputs", "output_id", output_id, {"machine_quality_status": status})
        _sync_render_plan_quality_status(self.ctx, output, status)
        refresh_output_segment_usage(self.ctx, output_id)
        return Result.ok({"output_id": output_id, "machine_quality_status": status, "score": 100 if status == "passed" else 60, "reasons": reasons})


def _segment_bundle(ctx: SkillContext, segment_id: str):
    segment = ctx.repo.get("segments", "segment_id", segment_id) or {}
    asset = ctx.repo.get("assets", "asset_id", segment.get("asset_id")) if segment.get("asset_id") else {}
    tags = ctx.repo.list_where("segment_tags", "segment_id=? ORDER BY id DESC", (segment_id,))
    return {"segment": segment, "asset": asset or {}, "tag": tags[0] if tags else {}}


def _sync_render_plan_quality_status(ctx: SkillContext, output: dict, status: str) -> None:
    output_id = str(output.get("output_id") or "")
    plans = ctx.repo.list_where("render_plans", "output_id=?", (output_id,)) if output_id else []
    if not plans:
        plans = ctx.repo.list_where(
            "render_plans",
            "batch_id=? AND variant_no=?",
            (output.get("batch_id"), output.get("variant_no")),
        )
    for plan in plans:
        ctx.repo.update("render_plans", "render_plan_id", plan["render_plan_id"], {"quality_gate_status": status})


def _trusted_real_first_segment(bundle: dict) -> bool:
    segment = bundle.get("segment") or {}
    asset = bundle.get("asset") or {}
    tag = bundle.get("tag") or {}
    source_type = str(segment.get("source_type") or asset.get("source_type") or "")
    trust = str(segment.get("source_trust_level") or asset.get("source_trust_level") or "")
    binding = str(segment.get("product_binding_type") or asset.get("product_binding_type") or "")
    match = str(segment.get("product_match_status") or "")
    if source_type not in {"authorized_creator", "self_shot", "original_script", "creator_original"}:
        return False
    if trust not in {"high", "medium"} or binding != "exact_sku" or match not in {"trusted_by_source", "anchor_pass"}:
        return False
    if str(tag.get("product_visibility") or "") != "high":
        return False
    if str(tag.get("confidence") or "") not in {"high", "medium"}:
        return False
    if str(tag.get("risk_level") or "") != "medium":
        return False
    reason = str(tag.get("reason") or "")
    soft_tokens = ["锚点未知", "锚点不确定", "锚点缺失", "商品锚点", "商品信息缺失", "需核对", "需复核", "需确认", "人工确认", "人工核实"]
    hard_tokens = ["水印", "平台", "账号", "logo", "Logo", "错款", "错品类", "竞品", "SKU一致性", "漂移", "无关元素", "品牌包", "遮挡严重"]
    return any(token in reason for token in soft_tokens) and not any(token in reason for token in hard_tokens)


def _low_trust_first_slot_review_candidate(bundle: dict) -> bool:
    segment = bundle.get("segment") or {}
    asset = bundle.get("asset") or {}
    tag = bundle.get("tag") or {}
    source_type = str(segment.get("source_type") or asset.get("source_type") or "")
    trust = str(segment.get("source_trust_level") or asset.get("source_trust_level") or "")
    binding = str(segment.get("product_binding_type") or asset.get("product_binding_type") or "")
    match = str(segment.get("product_match_status") or "")
    if source_type not in {"douyin_repost", "competitor"}:
        return False
    if trust != "low" or binding not in {"exact_sku", "same_style"}:
        return False
    if match not in {"", "uncertain", "trusted_by_source", "anchor_pass"}:
        return False
    if "hero" not in (segment.get("effective_roles_json") or []):
        return False
    return (
        str(tag.get("primary_shot_role") or "") == "hero"
        and str(tag.get("product_visibility") or "") == "high"
        and str(tag.get("confidence") or "") == "high"
        and str(tag.get("risk_level") or "") == "low"
        and str(tag.get("mixcut_usability") or "") == "yes"
        and str(tag.get("text_overlay_risk") or "none") in {"", "none", "low", "minor"}
        and str(asset.get("has_watermark") or segment.get("has_watermark") or "no") != "yes"
    )


def _audio_volume(ctx: SkillContext, output: dict, start_sec: float | None = None, duration_sec: float | None = None) -> float | None:
    path = require_oss_object_path(ctx, output.get("output_oss_object_id"), "quality_outputs")
    if not path or not path.exists():
        return None
    window_args = []
    if start_sec is not None:
        window_args.extend(["-ss", f"{max(start_sec, 0.0):.2f}"])
    if duration_sec is not None:
        window_args.extend(["-t", f"{max(duration_sec, 0.1):.2f}"])
    proc = subprocess.run(
        ["ffmpeg", "-hide_banner", "-i", str(path), *window_args, "-af", "volumedetect", "-vn", "-sn", "-dn", "-f", "null", "-"],
        capture_output=True,
        text=True,
    )
    match = re.search(r"mean_volume:\s*(-?\d+(?:\.\d+)?) dB", proc.stderr)
    return float(match.group(1)) if match else None


def _audio_tail_window_reasons(ctx: SkillContext, output: dict, actual_duration_ms: int) -> list[str]:
    duration_sec = max(actual_duration_ms, 0) / 1000
    if duration_sec <= 0:
        return ["audio duration could not be measured"]
    reasons = []
    for offset in [3, 2, 1]:
        start = max(0.0, duration_sec - offset)
        volume = _audio_volume(ctx, output, start_sec=start, duration_sec=1.0)
        if volume is None:
            reasons.append(f"audio tail window t-{offset}s could not be measured")
        elif volume < AUDIO_TAIL_MIN_DB:
            reasons.append(f"audio tail window t-{offset}s too low ({volume:.1f} dB)")
    return reasons


def _expected_duration_ms(slots: list[dict]) -> int:
    if not slots:
        return 0
    return max(int(slot.get("end_ms_in_output") or 0) for slot in slots)


def _planned_roles(ctx: SkillContext, output: dict, slots: list[dict]) -> set[str]:
    plans = ctx.repo.list_where(
        "render_plans",
        "batch_id=? AND variant_no=? ORDER BY id DESC LIMIT 1",
        (output.get("batch_id"), output.get("variant_no")),
    )
    if plans:
        roles = {
            str(slot.get("role"))
            for slot in ((plans[0].get("plan_json") or {}).get("segments") or [])
            if slot.get("role")
        }
        if roles:
            return roles
    return {str(slot.get("role_used")) for slot in slots if slot.get("role_used")}
