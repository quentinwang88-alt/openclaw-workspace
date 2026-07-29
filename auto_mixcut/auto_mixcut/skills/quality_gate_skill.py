from __future__ import annotations

import re
import subprocess
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from auto_mixcut.adapters.oss import file_sha256
from auto_mixcut.config.factory_config import factory_config
from auto_mixcut.core.result import Result
from auto_mixcut.core.storage_paths import require_oss_object_path

from .context import SkillContext
from .material_policy_skill import MaterialPolicySkill, evaluate_material_policy
from .material_usage_ledger_skill import MaterialUsageLedgerSkill
from .output_similarity_skill import OutputSimilaritySkill
from .usage_counter_skill import refresh_output_segment_usage


AUDIO_MEAN_MIN_DB = -16.0
AUDIO_TAIL_MIN_DB = -16.0
VOICEOVER_AUDIO_MEAN_MIN_DB = -20.0


class QualityGateSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def check_batch(self, batch_id: str) -> Result:
        outputs = self.ctx.repo.list_where("outputs", "batch_id=?", (batch_id,))
        max_workers = _quality_concurrency()
        if max_workers <= 1 or len(outputs) <= 1:
            results = [self.check_output(o["output_id"]).to_dict() for o in outputs]
        else:
            by_id = {}
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = {pool.submit(self.check_output, output["output_id"]): output["output_id"] for output in outputs}
                for future in as_completed(futures):
                    output_id = futures[future]
                    try:
                        by_id[output_id] = future.result().to_dict()
                    except Exception as exc:
                        by_id[output_id] = Result.fail("QUALITY_GATE_EXCEPTION", str(exc), {"output_id": output_id}).to_dict()
            results = [by_id[output["output_id"]] for output in outputs]
        return Result.ok({"batch_id": batch_id, "results": results})

    def check_output(self, output_id: str) -> Result:
        output = self.ctx.repo.get("outputs", "output_id", output_id)
        if not output:
            return Result.fail("OUTPUT_NOT_FOUND", "output not found", {"output_id": output_id})
        slots = self.ctx.repo.list_where("output_segments", "output_id=? ORDER BY slot_index", (output_id,))
        reasons = []
        warnings = []
        first = _segment_bundle(self.ctx, slots[0]["segment_id"]) if slots else None
        expected_duration = _expected_duration_ms(slots)
        actual_duration = int(output.get("duration_ms") or 0)
        if expected_duration and abs(actual_duration - expected_duration) > 500:
            reasons.append("duration does not match render plan")
        elif not expected_duration and (actual_duration < 12000 or actual_duration > 30000):
            # 只在没有预期时长（无 output_segments）时才检查 12-30s 范围。
            # 投流片 6-10s 有预期时长且匹配，不应被此规则拦截。
            reasons.append("duration out of supported range")
        if output.get("width") != 1080 or output.get("height") != 1920:
            reasons.append("resolution is not 1080x1920")
        content_mode = str(output.get("content_mode") or "bgm").lower()
        volume = _audio_volume(self.ctx, output)
        if volume is None:
            reasons.append("audio volume could not be measured")
        elif volume < (VOICEOVER_AUDIO_MEAN_MIN_DB if content_mode == "voiceover" else AUDIO_MEAN_MIN_DB):
            reasons.append(f"audio mean volume too low ({volume:.1f} dB)")
        if content_mode == "voiceover":
            reasons.extend(_voiceover_reasons(self.ctx, output, slots, actual_duration))
        else:
            reasons.extend(_audio_tail_window_reasons(self.ctx, output, actual_duration))
        if first:
            tag = first["tag"]
            segment = first["segment"]
            roles = segment.get("effective_roles_json") or []
            material_policy = MaterialPolicySkill(self.ctx).evaluate_segment(segment, asset=first.get("asset") or {}, tag=tag, usecase="ads_mixcut" if _is_ads_mode() else "mixcut", slot_index=1, role="hero")
            if not material_policy.get("first_slot_allowed"):
                reasons.append("first segment blocked by material policy: " + ",".join(material_policy.get("block_reasons") or ["material_policy_blocked"]))
            if tag.get("product_visibility") != "high":
                reasons.append("first segment product visibility is not high")
            ai_anchor_trusted = segment.get("source_type") == "ai_generated" and segment.get("anchor_match_level") == "strict_pass"
            trusted_real_first = bool(material_policy.get("trusted_real_first"))
            if tag.get("risk_level") != "low" and not ai_anchor_trusted and not trusted_real_first:
                reasons.append("first segment risk is not low")
            if tag.get("hook_strength") not in {"strong", "medium"}:
                reasons.append("first segment hook is weak")
            if (
                segment.get("product_match_status") not in {"trusted_by_source", "anchor_pass"}
                and not ai_anchor_trusted
                and not material_policy.get("low_trust_first_slot_candidate")
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
        required_asset_count = 2 if actual_duration <= 10000 else 3
        if len(assets) < required_asset_count:
            message = f"unique_source_assets < {required_asset_count}"
            if content_mode == "voiceover":
                warnings.append(message)
            else:
                reasons.append(message)
        similarity = None
        status = "passed" if not reasons else "failed"
        if status == "passed" and _is_ads_mode():
            similarity_result = OutputSimilaritySkill(self.ctx).check_output(output_id)
            if similarity_result.success:
                similarity = similarity_result.data or {}
                decision = str(similarity.get("decision") or "pass")
                if decision in {"duplicate_blocked", "similarity_review"}:
                    status = decision
                    reasons.append(f"output similarity gate: {decision}")
            else:
                similarity = similarity_result.to_dict()
        patch = {"machine_quality_status": status}
        if content_mode == "voiceover":
            patch["voiceover_qc_status"] = status
        self.ctx.repo.update("outputs", "output_id", output_id, patch)
        _sync_render_plan_quality_status(self.ctx, output, status)
        refresh_output_segment_usage(self.ctx, output_id)
        product_id = str(output.get("product_id") or "")
        if product_id:
            MaterialUsageLedgerSkill(self.ctx).refresh_product(product_id)
        return Result.ok({"output_id": output_id, "machine_quality_status": status, "score": 100 if status == "passed" else 60, "reasons": reasons, "warnings": warnings, "similarity": similarity})


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
    return evaluate_material_policy(None, segment, asset=asset, tag=tag, usecase="ads_mixcut" if _is_ads_mode() else "mixcut", slot_index=1, role="hero").trusted_real_first


def _low_trust_first_slot_review_candidate(bundle: dict) -> bool:
    segment = bundle.get("segment") or {}
    asset = bundle.get("asset") or {}
    tag = bundle.get("tag") or {}
    return evaluate_material_policy(None, segment, asset=asset, tag=tag, usecase="ads_mixcut" if _is_ads_mode() else "mixcut", slot_index=1, role="hero").low_trust_first_slot_candidate


def _is_ads_mode() -> bool:
    return factory_config().ads_fast_mode


def _audio_volume(ctx: SkillContext, output: dict, start_sec: float | None = None, duration_sec: float | None = None) -> float | None:
    path = _quality_media_path(ctx, output)
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


def _quality_media_path(ctx: SkillContext, output: dict) -> Path | None:
    cached = output.get("_quality_media_path")
    if cached:
        path = Path(cached)
        if path.exists():
            return path
    local = ctx.settings.temp_root / "render" / str(output.get("product_id") or "") / f"{output.get('output_id')}.mp4"
    if _local_render_matches_oss_object(ctx, local, output.get("output_oss_object_id")):
        output["_quality_media_path"] = str(local)
        return local
    path = require_oss_object_path(ctx, output.get("output_oss_object_id"), "quality_outputs")
    if path:
        output["_quality_media_path"] = str(path)
    return path


def _local_render_matches_oss_object(ctx: SkillContext, path: Path, object_id: str | None) -> bool:
    if not path.exists() or not object_id:
        return False
    obj = ctx.repo.get("oss_objects", "object_id", object_id) or {}
    try:
        expected_size = int(obj.get("file_size") or 0)
    except (TypeError, ValueError):
        expected_size = 0
    if expected_size and path.stat().st_size != expected_size:
        return False
    expected_hash = str(obj.get("file_hash") or "")
    if expected_hash and file_sha256(path) != expected_hash:
        return False
    return True


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


def _voiceover_reasons(ctx: SkillContext, output: dict, slots: list[dict], actual_duration_ms: int) -> list[str]:
    reasons = []
    if not output.get("voiceover_oss_object_id"):
        reasons.append("voiceover audio object is missing")
    plans = ctx.repo.list_where("render_plans", "output_id=? ORDER BY id DESC LIMIT 1", (output.get("output_id"),))
    plan = plans[0] if plans else {}
    if plan.get("evidence_gap_json"):
        reasons.append("voiceover evidence gaps remain")
    timeline = plan.get("tts_timeline_json") or []
    if isinstance(timeline, dict):
        timeline = timeline.get("placements") or timeline.get("lines") or []
    ends = []
    for row in timeline if isinstance(timeline, list) else []:
        try:
            ends.append(int(row.get("end_ms") or round(float(row.get("end_seconds") or 0) * 1000)))
        except (TypeError, ValueError):
            continue
    if ends and max(ends) > actual_duration_ms + 500:
        reasons.append("tts timeline exceeds rendered duration")
    plan_segments = ((plan.get("plan_json") or {}).get("segments") or []) if plan else []
    if any(row.get("critical_evidence") and row.get("required_shot_roles") and not row.get("evidence_match") for row in plan_segments):
        reasons.append("required voiceover evidence is not matched")
    if not slots:
        reasons.append("voiceover output has no material slots")
    return reasons


def _quality_concurrency() -> int:
    try:
        return max(1, min(3, int(os.environ.get("AUTO_MIXCUT_QUALITY_CONCURRENCY", "3") or "3")))
    except ValueError:
        return 3


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
