from __future__ import annotations

import json
import shlex
import subprocess
from pathlib import Path

from auto_mixcut.core.ids import new_id
from auto_mixcut.core.result import Result
from auto_mixcut.core.storage_paths import require_oss_object_path

from .context import SkillContext
from .bgm_usage_skill import refresh_bgm_track_usage
from .feishu_review_skill import sync_product_task_best_effort
from .hard_subtitle_policy import is_repairable_bottom_caption
from .usage_counter_skill import is_good_rendered_output, refresh_segment_usage

MIN_BGM_VOLUME = 1.0
BGM_LOUDNORM_TARGET_I = -10
BGM_LOUDNORM_TRUE_PEAK = -0.5
BGM_LOUDNORM_LRA = 7
BGM_BATCH_ID_CAP = 1
BGM_BATCH_TRACK_NAME_CAP = 1


class RenderSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def render_batch(self, batch_id: str) -> Result:
        plans = self.ctx.repo.list_where("render_plans", "batch_id=? AND render_status='planned'", (batch_id,))
        outputs = []
        failures = []
        for plan in plans:
            res = self.render_plan(plan["render_plan_id"])
            if res.success:
                outputs.append(res.data["output_id"])
            else:
                failures.append({"render_plan_id": plan["render_plan_id"], "error": res.error.to_dict() if res.error else {}})
        batch = self.ctx.repo.get("mixcut_batches", "batch_id", batch_id)
        total_outputs = len(self.ctx.repo.list_where("outputs", "batch_id=?", (batch_id,)))
        if failures:
            if batch:
                self.ctx.repo.update("mixcut_batches", "batch_id", batch_id, {"rendered_count": total_outputs, "batch_status": "render_failed"})
            return Result.fail("BATCH_RENDER_FAILED", "one or more render plans failed", {"batch_id": batch_id, "output_ids": outputs, "failures": failures})
        if batch:
            self.ctx.repo.update("mixcut_batches", "batch_id", batch_id, {"rendered_count": total_outputs, "batch_status": "rendered"})
            actual_count = _actual_generated_count(self.ctx, batch.get("product_id"))
            task = self.ctx.repo.get("content_tasks", "task_id", batch.get("task_id")) or {}
            requested_count = int(task.get("requested_variant_count") or task.get("allowed_variant_count") or 0)
            current_blocked_reason = str(task.get("blocked_reason") or "")
            keep_progress_reason = ("AI补素材" in current_blocked_reason or "补差额" in current_blocked_reason) and actual_count < requested_count
            self.ctx.repo.update(
                "content_tasks",
                "task_id",
                batch.get("task_id"),
                {
                    "actual_variant_count": actual_count,
                    "task_status": "RENDERED",
                    "blocked_reason": current_blocked_reason if keep_progress_reason else "",
                    "failure_reason": "",
                },
            )
        task_sync = sync_product_task_best_effort(self.ctx, batch.get("product_id") if batch else "")
        return Result.ok({"batch_id": batch_id, "output_ids": outputs, "task_sync": task_sync})

    def render_plan(self, render_plan_id: str) -> Result:
        plan = self.ctx.repo.get("render_plans", "render_plan_id", render_plan_id)
        if not plan:
            return Result.fail("RENDER_PLAN_NOT_FOUND", "render plan not found", {"render_plan_id": render_plan_id})
        output_id = new_id("OUT")
        local = self.ctx.settings.temp_root / "render" / plan["product_id"] / f"{output_id}.mp4"
        cover = self.ctx.settings.temp_root / "render" / plan["product_id"] / f"{output_id}.jpg"
        manifest = self.ctx.settings.temp_root / "manifests" / plan["product_id"] / f"{output_id}.json"
        local.parent.mkdir(parents=True, exist_ok=True)
        cover.parent.mkdir(parents=True, exist_ok=True)
        manifest.parent.mkdir(parents=True, exist_ok=True)
        segments = plan["plan_json"]["segments"]
        if self.ctx.ffmpeg.mock:
            local.write_bytes(f"mock rendered video {output_id}".encode("utf-8"))
            cover.write_bytes(b"\xff\xd8\xff\xe0mock-cover\xff\xd9")
        else:
            subtitles = _subtitle_plan(self.ctx, plan)
            rendered = self._render_real(plan, segments, local, cover, subtitles)
            if not rendered.success:
                return rendered
            bgm_object = rendered.data.get("bgm_object")
            bgm_plan = rendered.data.get("bgm") or {}
        if self.ctx.ffmpeg.mock:
            subtitles = _subtitle_plan(self.ctx, plan)
            bgm_object = None
            bgm_plan = _bgm_manifest(_default_bgm_plan())
        product = self.ctx.repo.get("products", "product_id", plan["product_id"]) or {}
        out_key = f"auto_mixcut/outputs/{product.get('market','NA')}/{product.get('category','uncategorized')}/{plan['product_id']}/{plan['batch_id']}/variant_{plan['variant_no']:03d}.mp4"
        cover_key = f"auto_mixcut/covers/{product.get('market','NA')}/{product.get('category','uncategorized')}/{plan['product_id']}/{plan['batch_id']}/variant_{plan['variant_no']:03d}.jpg"
        out_upload = self.ctx.oss.upload(local, out_key)
        cover_upload = self.ctx.oss.upload(cover, cover_key)
        if not out_upload.success:
            return out_upload
        if not cover_upload.success:
            return cover_upload
        out_obj = dict(out_upload.data, object_type="output", mime_type="video/mp4", lifecycle_policy="output_temp_until_publish_or_reject")
        cover_obj = dict(cover_upload.data, object_type="cover", mime_type="image/jpeg")
        self.ctx.repo.upsert("oss_objects", "object_id", out_obj)
        self.ctx.repo.upsert("oss_objects", "object_id", cover_obj)
        duration_ms = _planned_duration_ms(plan, segments)
        output_row = {"output_id": output_id, "batch_id": plan["batch_id"], "product_id": plan["product_id"], "variant_no": plan["variant_no"], "template_id": plan["template_id"], "output_oss_object_id": out_obj["object_id"], "cover_oss_object_id": cover_obj["object_id"], "duration_ms": duration_ms, "width": 1080, "height": 1920, "render_status": "rendered", "machine_quality_status": "pending", "human_quality_status": "pending", "bgm_plan_json": {**bgm_plan, "oss_object_id": bgm_object, "loudness_normalized": True}}
        self.ctx.repo.upsert("outputs", "output_id", output_row)
        for slot in segments:
            self.ctx.repo.insert("output_segments", {"output_id": output_id, "segment_id": slot["segment_id"], "asset_id": slot["asset_id"], "slot_index": slot["slot"], "role_used": slot["role"], "start_ms_in_output": slot["start_ms_in_output"], "end_ms_in_output": slot["end_ms_in_output"]})
            refresh_segment_usage(self.ctx, slot["segment_id"])
        _record_bgm_usage(self.ctx, output_row, bgm_plan)
        manifest_data = {"output_id": output_id, "batch_id": plan["batch_id"], "product_id": plan["product_id"], "template_id": plan["template_id"], "duration_ms": duration_ms, "output_oss_object_id": out_obj["object_id"], "cover_oss_object_id": cover_obj["object_id"], "segments": segments, "subtitles": subtitles, "bgm": {**bgm_plan, "oss_object_id": bgm_object, "loudness_normalized": True}, "machine_quality_status": "pending", "experiment_group": plan["template_id"], "experiment_batch": plan["batch_id"]}
        manifest.write_text(json.dumps(manifest_data, ensure_ascii=False, indent=2), encoding="utf-8")
        man_key = f"auto_mixcut/manifests/{product.get('market','NA')}/{product.get('category','uncategorized')}/{plan['product_id']}/{plan['batch_id']}/variant_{plan['variant_no']:03d}.json"
        man_upload = self.ctx.oss.upload(manifest, man_key)
        if man_upload.success:
            self.ctx.repo.upsert("oss_objects", "object_id", dict(man_upload.data, object_type="manifest", mime_type="application/json"))
        self.ctx.repo.update("render_plans", "render_plan_id", render_plan_id, {"render_status": "rendered", "output_id": output_id})
        return Result.ok({"output_id": output_id, "manifest": manifest_data})

    def _render_real(self, plan: dict, slots: list[dict], output_path: Path, cover_path: Path, subtitles: list[dict]) -> Result:
        tool_check = self.ctx.ffmpeg.require_tools()
        if not tool_check.success:
            return tool_check
        work_dir = output_path.parent / f"{output_path.stem}_parts"
        work_dir.mkdir(parents=True, exist_ok=True)
        bgm = _ensure_bgm(self.ctx, plan)
        if not bgm.success:
            return bgm
        parts = []
        for slot in slots:
            source = _segment_path(self.ctx, slot["segment_id"])
            if not source:
                return Result.fail("RENDER_FAILED", "segment source object not found", {"segment_id": slot["segment_id"]})
            duration = max((int(slot["end_ms_in_output"]) - int(slot["start_ms_in_output"])) / 1000, 0.5)
            part = work_dir / f"slot_{int(slot['slot']):03d}.mp4"
            args = [
                "-y",
                "-i",
                str(source),
                "-t",
                f"{duration:.3f}",
                "-vf",
                _slot_video_filter(self.ctx, slot),
                "-c:v",
                "libx264",
                "-preset",
                "veryfast",
                "-an",
                str(part),
            ]
            res = self.ctx.ffmpeg.run(args, "RENDER_FAILED")
            if not res.success:
                return res
            parts.append(part)
        concat_file = work_dir / "concat.txt"
        concat_file.write_text("".join(f"file '{part.as_posix()}'\n" for part in parts), encoding="utf-8")
        video_only = work_dir / "video_only.mp4"
        duration_arg = _duration_arg(_planned_duration_ms(plan, slots))
        concat_args = [
            "-y",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(concat_file),
            "-t",
            duration_arg,
            "-c",
            "copy",
            str(video_only),
        ]
        concat = self.ctx.ffmpeg.run(concat_args, "RENDER_FAILED")
        if not concat.success:
            return concat
        drawtext = _drawtext_filter(subtitles)
        bgm_bed = _render_bgm_bed(self.ctx, bgm.data, _planned_duration_ms(plan, slots), work_dir)
        if not bgm_bed.success:
            return bgm_bed
        final_args = [
            "-y",
            "-i",
            str(video_only),
            "-i",
            str(bgm_bed.data["path"]),
            "-filter_complex",
            f"[0:v]{drawtext}[v];[1:a]anull[a]",
            "-map",
            "[v]",
            "-map",
            "[a]",
            "-t",
            duration_arg,
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
            "-ar",
            "44100",
            "-ac",
            "2",
            str(output_path),
        ]
        final = self.ctx.ffmpeg.run(final_args, "RENDER_FAILED")
        if not final.success:
            return final
        cover = self.ctx.ffmpeg.run(["-y", "-ss", "0.2", "-i", str(output_path), "-frames:v", "1", "-q:v", "2", str(cover_path)], "COVER_FAILED")
        if not cover.success:
            return cover
        probed = self.ctx.ffmpeg.probe(output_path)
        if not probed.success:
            return probed
        data = probed.data
        if data.get("width") != 1080 or data.get("height") != 1920 or not data.get("has_audio"):
            return Result.fail("RENDER_FAILED", "rendered output failed technical probe", {"probe": data})
        return Result.ok({"path": str(output_path), "probe": data, "bgm_object": bgm.data.get("object_id"), "bgm": _bgm_manifest(bgm.data)})


def _planned_duration_ms(plan: dict, segments: list[dict]) -> int:
    try:
        planned = int(plan.get("planned_duration_ms") or 0)
    except (TypeError, ValueError):
        planned = 0
    if planned > 0:
        return planned
    try:
        return max(int(slot.get("end_ms_in_output") or 0) for slot in segments)
    except (TypeError, ValueError, StopIteration):
        return 15000


def _actual_generated_count(ctx: SkillContext, product_id: str | None) -> int:
    product_id = str(product_id or "").strip()
    if not product_id:
        return 0
    outputs = ctx.repo.list_where("outputs", "product_id=?", (product_id,))
    return sum(1 for output in outputs if is_good_rendered_output(output))


def _duration_arg(duration_ms: int) -> str:
    return f"{max(duration_ms, 500) / 1000:.3f}"


def _segment_path(ctx: SkillContext, segment_id: str) -> Path | None:
    segment = ctx.repo.get("segments", "segment_id", segment_id)
    if not segment:
        return None
    return require_oss_object_path(ctx, segment.get("segment_oss_object_id"), "render_segments")


def _record_bgm_usage(ctx: SkillContext, output: dict, bgm_plan: dict) -> None:
    bgm_id = str((bgm_plan or {}).get("bgm_id") or "")
    if not bgm_id:
        return
    try:
        write = ctx.repo.insert(
            "bgm_usage_events",
            {
                "event_id": new_id("BGMUSE"),
                "bgm_id": bgm_id,
                "output_id": output.get("output_id"),
                "batch_id": output.get("batch_id"),
                "product_id": output.get("product_id"),
                "template_id": output.get("template_id"),
                "usage_status": "rendered",
                "quality_status": output.get("machine_quality_status") or "pending",
                "reason": "render_success",
            },
        )
        if not write.success:
            return
        refresh_bgm_track_usage(ctx, bgm_id, last_feedback_status=output.get("machine_quality_status") or "pending")
    except Exception:
        return


def _slot_video_filter(ctx: SkillContext, slot: dict) -> str:
    cleanup = slot.get("subtitle_cleanup") or {}
    if cleanup.get("action") == "bottom_crop" or _segment_needs_bottom_subtitle_crop(ctx, str(slot.get("segment_id") or "")):
        return "scale=1210:2152:force_original_aspect_ratio=increase,crop=1080:1920:(iw-1080)/2:0,fps=30,format=yuv420p"
    return "scale=1080:1920:force_original_aspect_ratio=decrease,pad=1080:1920:(ow-iw)/2:(oh-ih)/2,fps=30,format=yuv420p"


def _segment_needs_bottom_subtitle_crop(ctx: SkillContext, segment_id: str) -> bool:
    rows = ctx.repo.list_where("segment_tags", "segment_id=? ORDER BY id DESC LIMIT 1", (segment_id,))
    segment = ctx.repo.get("segments", "segment_id", segment_id) or {}
    tag = rows[0] if rows else {}
    reason = " ".join(
        str(value or "")
        for value in [
            tag.get("text_overlay_reason"),
            tag.get("reason"),
            segment.get("effective_roles_reason"),
            segment.get("product_match_reason"),
        ]
    )
    return is_repairable_bottom_caption({**tag, "reason": reason})


def _ensure_bgm(ctx: SkillContext, plan: dict | None = None) -> Result:
    try:
        from auto_mixcut.skills.bgm_library_skill import BgmLibrarySkill
        product_id = (plan or {}).get("product_id", "")
        template_id = (plan or {}).get("template_id", "")
        plan_template = ((plan or {}).get("plan_json") or {}).get("template") or {}
        bgm_profile = plan_template.get("bgm_profile") or {}
        moods = bgm_profile.get("moods") or plan_template.get("default_moods") or []
        mood = str(moods[0]) if moods else ""
        category = ""
        if product_id:
            product = ctx.repo.get("products", "product_id", product_id) or {}
            category = product.get("category", "")
        rec = BgmLibrarySkill(ctx).get_recommendation(product_id=product_id, category=category, mood=mood, template_id=template_id)
        recs = rec.data.get("recommendations", []) if rec.success else []
        if recs:
            best = _choose_bgm_candidate_for_batch(ctx, recs, plan or {})
            full_track = ctx.repo.get("bgm_tracks", "bgm_id", best.get("bgm_id")) or {}
            best = {**best, **full_track}
            if best.get("bgm_id") and not full_track.get("audio_analysis_json"):
                try:
                    from auto_mixcut.skills.bgm_audio_analysis_skill import BgmAudioAnalysisSkill
                    analyzed = BgmAudioAnalysisSkill(ctx).analyze_track(str(best["bgm_id"]), apply_tags=True)
                    if analyzed.success:
                        full_track = ctx.repo.get("bgm_tracks", "bgm_id", best.get("bgm_id")) or full_track
                        best = {**best, **full_track}
                        analysis = analyzed.data.get("analysis") or {}
                        suggestions = analysis.get("mix_suggestions") or {}
                        best.update({k: v for k, v in suggestions.items() if k in {"recommended_start_sec", "default_volume", "fade_in_ms", "fade_out_ms"}})
                except Exception:
                    pass
            best = _apply_audio_mix_suggestions(best)
            path = _bgm_track_path(ctx, best)
            if path and path.exists():
                best = _normalize_bgm_mix(
                    ctx,
                    best,
                    path,
                    _planned_duration_ms(plan or {}, ((plan or {}).get("plan_json") or {}).get("segments") or []),
                )
                return Result.ok({
                    **_default_bgm_plan(),
                    **best,
                    "path": str(path),
                    "object_id": best.get("oss_object_id", ""),
                    "matched_template_id": template_id,
                    "matched_mood": mood,
                })
    except Exception:
        pass

    bgm_dir = ctx.settings.root_dir / "assets" / "bgm"
    bgm_dir.mkdir(parents=True, exist_ok=True)
    paths = [path for path in sorted(bgm_dir.rglob("*")) if path.suffix.lower() in {".mp3", ".wav", ".m4a", ".aac"} and not path.name.startswith("test_")]
    if paths:
        seed = int((plan or {}).get("variant_no") or 1) - 1
        return _register_bgm(ctx, paths[seed % len(paths)])
    generated = bgm_dir / "test_soft_bgm_15s.m4a"
    if not generated.exists():
        res = ctx.ffmpeg.run(
            [
                "-y",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=220:duration=15",
                "-filter:a",
                "volume=0.08,afade=t=in:st=0:d=0.8,afade=t=out:st=14:d=1",
                "-c:a",
                "aac",
                str(generated),
            ],
            "BGM_GENERATE_FAILED",
        )
        if not res.success:
            return res
    return _register_bgm(ctx, generated)


def _choose_bgm_candidate_for_batch(ctx: SkillContext, recs: list[dict], plan: dict) -> dict:
    if not recs:
        return {}
    id_counts, name_counts = _batch_bgm_usage(ctx, str(plan.get("batch_id") or ""))
    variant_no = max(1, int(plan.get("variant_no") or 1))
    annotated = []
    for index, rec in enumerate(recs):
        bgm_id = str(rec.get("bgm_id") or "")
        track_name_key = _bgm_track_name_key(rec)
        id_count = int(id_counts.get(bgm_id, 0)) if bgm_id else 0
        name_count = int(name_counts.get(track_name_key, 0)) if track_name_key else 0
        over_cap = id_count >= BGM_BATCH_ID_CAP or name_count >= BGM_BATCH_TRACK_NAME_CAP
        score = _safe_float(rec.get("score"), 0)
        annotated.append((over_cap, name_count, id_count, -score, (index + variant_no - 1) % max(len(recs), 1), rec))
    annotated.sort(key=lambda item: item[:5])
    return annotated[0][5]


def _batch_bgm_usage(ctx: SkillContext, batch_id: str) -> tuple[dict[str, int], dict[str, int]]:
    if not batch_id:
        return {}, {}
    id_counts: dict[str, int] = {}
    name_counts: dict[str, int] = {}
    for output in ctx.repo.list_where("outputs", "batch_id=? AND render_status='rendered'", (batch_id,)):
        plan = output.get("bgm_plan_json") or {}
        if isinstance(plan, str):
            try:
                plan = json.loads(plan)
            except json.JSONDecodeError:
                plan = {}
        if not isinstance(plan, dict):
            continue
        bgm_id = str(plan.get("bgm_id") or "")
        if bgm_id:
            id_counts[bgm_id] = int(id_counts.get(bgm_id, 0)) + 1
        track_name_key = _bgm_track_name_key(plan)
        if track_name_key:
            name_counts[track_name_key] = int(name_counts.get(track_name_key, 0)) + 1
    return id_counts, name_counts


def _bgm_track_name_key(track: dict) -> str:
    return " ".join(str(track.get("track_name") or "").strip().lower().split())


def _register_bgm(ctx: SkillContext, path: Path) -> Result:
    object_key = f"auto_mixcut/bgm/{path.name}"
    existing = ctx.repo.list_where("oss_objects", "object_key=? AND object_type='bgm' ORDER BY id DESC", (object_key,))
    if existing:
        return Result.ok({"path": str(path), "object_id": existing[0]["object_id"], **_default_bgm_plan(), "fallback_source": "local_bgm"})
    uploaded = ctx.oss.upload(path, object_key)
    if not uploaded.success:
        return uploaded
    row = dict(uploaded.data, object_type="bgm", mime_type="audio/mp4")
    saved = ctx.repo.upsert("oss_objects", "object_id", row)
    return saved if not saved.success else Result.ok({"path": str(path), "object_id": row["object_id"], **_default_bgm_plan(), "fallback_source": "local_bgm"})


def _default_bgm_plan() -> dict:
    return {
        "bgm_id": "",
        "track_name": "",
        "recommended_start_sec": 0,
        "default_volume": MIN_BGM_VOLUME,
        "fade_in_ms": 60,
        "fade_out_ms": 0,
        "matched_template_id": "",
        "matched_mood": "",
    }


def _normalize_bgm_mix(ctx: SkillContext, track: dict, path: Path, planned_duration_ms: int) -> dict:
    updated = dict(track)
    planned_sec = max(planned_duration_ms, 500) / 1000
    duration_sec = _audio_duration_sec(path)
    start_sec = max(_safe_float(updated.get("recommended_start_sec"), 0), 0)
    if duration_sec and duration_sec > planned_sec + 1:
        max_start = max(0.0, duration_sec - planned_sec - 0.5)
        start_sec = min(start_sec, max_start)
    elif duration_sec and start_sec >= max(duration_sec - 1.0, 0):
        start_sec = 0.0
    updated["recommended_start_sec"] = round(start_sec, 3)
    updated["default_volume"] = max(_safe_float(updated.get("default_volume"), MIN_BGM_VOLUME), MIN_BGM_VOLUME)
    updated["fade_in_ms"] = min(int(_safe_float(updated.get("fade_in_ms"), 60)), 80)
    updated["fade_out_ms"] = 0
    if duration_sec:
        updated["source_duration_sec"] = round(duration_sec, 3)
    return updated


def _audio_duration_sec(path: Path) -> float | None:
    proc = subprocess.run(
        ["ffprobe", "-v", "error", "-print_format", "json", "-show_format", str(path)],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return None
    try:
        raw = json.loads(proc.stdout)
        duration = float((raw.get("format") or {}).get("duration") or 0)
        return duration if duration > 0 else None
    except (TypeError, ValueError, json.JSONDecodeError):
        return None


def _bgm_track_path(ctx: SkillContext, track: dict) -> Path | None:
    local_path = str(track.get("local_file_path") or "")
    local = Path(local_path) if local_path else None
    if local and local.exists() and local.is_file():
        return local
    return require_oss_object_path(ctx, track.get("oss_object_id", ""), "render_bgm")


def _apply_audio_mix_suggestions(track: dict) -> dict:
    analysis = track.get("audio_analysis_json") or {}
    if not isinstance(analysis, dict):
        return track
    suggestions = analysis.get("mix_suggestions") or {}
    if not isinstance(suggestions, dict):
        return track
    updated = dict(track)
    for key in ["recommended_start_sec", "default_volume", "fade_in_ms", "fade_out_ms"]:
        value = suggestions.get(key)
        if value is not None:
            updated[key] = value
    return updated


def _bgm_manifest(data: dict) -> dict:
    volume = _safe_float(data.get("default_volume"), MIN_BGM_VOLUME)
    return {
        "bgm_id": data.get("bgm_id") or "",
        "track_name": data.get("track_name") or "",
        "default_volume": volume,
        "volume": volume,
        "recommended_start_sec": _safe_float(data.get("recommended_start_sec"), 0),
        "fade_in_ms": _safe_int(data.get("fade_in_ms"), 500),
        "fade_out_ms": _safe_int(data.get("fade_out_ms"), 0),
        "loudnorm_target_i": BGM_LOUDNORM_TARGET_I,
        "matched_template_id": data.get("matched_template_id") or "",
        "matched_mood": data.get("matched_mood") or "",
        "fallback_source": data.get("fallback_source") or "",
    }


def _bgm_audio_filter(data: dict, duration_ms: int) -> str:
    duration_sec = max(duration_ms, 500) / 1000
    volume = min(max(_safe_float(data.get("default_volume"), MIN_BGM_VOLUME), MIN_BGM_VOLUME), 1.0)
    fade_in = max(_safe_int(data.get("fade_in_ms"), 500), 0) / 1000
    fade_out = max(_safe_int(data.get("fade_out_ms"), 0), 0) / 1000
    parts = [
        f"loudnorm=I={BGM_LOUDNORM_TARGET_I}:TP={BGM_LOUDNORM_TRUE_PEAK}:LRA={BGM_LOUDNORM_LRA}",
        f"volume={volume:.3f}",
        f"atrim=0:{duration_sec:.3f}",
    ]
    if fade_in > 0:
        parts.append(f"afade=t=in:st=0:d={fade_in:.3f}")
    if fade_out > 0:
        parts.append(f"afade=t=out:st={max(duration_sec - fade_out, 0):.3f}:d={fade_out:.3f}")
    return ",".join(parts)


def _render_bgm_bed(ctx: SkillContext, data: dict, duration_ms: int, work_dir: Path) -> Result:
    bed_path = work_dir / "bgm_bed.m4a"
    start_sec = max(_safe_float(data.get("recommended_start_sec"), 0), 0)
    args = [
        "-y",
        "-stream_loop",
        "-1",
        "-ss",
        f"{start_sec:.3f}",
        "-i",
        str(data["path"]),
        "-map",
        "0:a:0",
        "-vn",
        "-t",
        _duration_arg(duration_ms),
        "-af",
        _bgm_audio_filter(data, duration_ms),
        "-c:a",
        "aac",
        "-ar",
        "44100",
        "-ac",
        "2",
        str(bed_path),
    ]
    res = ctx.ffmpeg.run(args, "BGM_BED_RENDER_FAILED")
    if not res.success:
        return res
    expected_sec = max(duration_ms, 500) / 1000
    actual_sec = _audio_duration_sec(bed_path)
    if not actual_sec or actual_sec < expected_sec - 0.25:
        return Result.fail(
            "BGM_BED_RENDER_FAILED",
            "bgm bed audio is missing or shorter than expected",
            {"path": str(bed_path), "expected_sec": expected_sec, "actual_sec": actual_sec},
        )
    return Result.ok({"path": str(bed_path)})


def _safe_float(value, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _subtitle_plan(ctx: SkillContext, plan: dict) -> list[dict]:
    plan_json = plan.get("plan_json") or {}
    subtitle_cfg = plan_json.get("subtitles") or {}
    if not isinstance(subtitle_cfg, dict) or not subtitle_cfg.get("enabled"):
        return []

    items = subtitle_cfg.get("items") or []
    subtitles: list[dict] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        text = str(item.get("text") or "").strip()
        if not text:
            continue
        start_ms = _safe_int(item.get("start_ms"), 0)
        end_ms = _safe_int(item.get("end_ms"), 0)
        if end_ms <= start_ms:
            continue
        subtitles.append({"start_ms": start_ms, "end_ms": end_ms, "text": text})
    return subtitles


def _drawtext_filter(subtitles: list[dict]) -> str:
    font = _font_path()
    chain = "format=yuv420p"
    for item in subtitles:
        start = int(item["start_ms"]) / 1000
        end = int(item["end_ms"]) / 1000
        text = _escape_drawtext(str(item["text"]))
        opts = [
            f"text='{text}'",
            "x=(w-text_w)/2",
            "y=h-320",
            "fontsize=58",
            "fontcolor=white",
            "borderw=4",
            "bordercolor=black@0.65",
            "box=1",
            "boxcolor=black@0.28",
            "boxborderw=24",
            f"enable='between(t,{start:.3f},{end:.3f})'",
        ]
        if font:
            opts.insert(0, f"fontfile={shlex.quote(font)}")
        chain += ",drawtext=" + ":".join(opts)
    return chain


def _font_path() -> str | None:
    for path in [
        "/System/Library/Fonts/PingFang.ttc",
        "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
        "/Library/Fonts/Arial Unicode.ttf",
    ]:
        if Path(path).exists():
            return path
    return None


def _escape_drawtext(text: str) -> str:
    return text.replace("\\", "\\\\").replace("'", "\\'").replace(":", "\\:").replace("%", "\\%")
