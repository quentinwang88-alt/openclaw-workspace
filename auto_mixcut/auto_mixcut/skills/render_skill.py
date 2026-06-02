from __future__ import annotations

import json
import shlex
from pathlib import Path

from auto_mixcut.core.ids import new_id
from auto_mixcut.core.result import Result
from auto_mixcut.core.storage_paths import require_oss_object_path

from .context import SkillContext


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
        if failures:
            if batch:
                self.ctx.repo.update("mixcut_batches", "batch_id", batch_id, {"rendered_count": len(outputs), "batch_status": "render_failed"})
            return Result.fail("BATCH_RENDER_FAILED", "one or more render plans failed", {"batch_id": batch_id, "output_ids": outputs, "failures": failures})
        if batch:
            self.ctx.repo.update("mixcut_batches", "batch_id", batch_id, {"rendered_count": len(outputs), "batch_status": "rendered"})
            self.ctx.repo.update("content_tasks", "task_id", batch.get("task_id"), {"actual_variant_count": len(outputs), "task_status": "RENDERED"})
        return Result.ok({"batch_id": batch_id, "output_ids": outputs})

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
        out_obj = dict(out_upload.data, object_type="output", mime_type="video/mp4")
        cover_obj = dict(cover_upload.data, object_type="cover", mime_type="image/jpeg")
        self.ctx.repo.upsert("oss_objects", "object_id", out_obj)
        self.ctx.repo.upsert("oss_objects", "object_id", cover_obj)
        duration_ms = _planned_duration_ms(plan, segments)
        output_row = {"output_id": output_id, "batch_id": plan["batch_id"], "product_id": plan["product_id"], "variant_no": plan["variant_no"], "template_id": plan["template_id"], "output_oss_object_id": out_obj["object_id"], "cover_oss_object_id": cover_obj["object_id"], "duration_ms": duration_ms, "width": 1080, "height": 1920, "render_status": "rendered", "machine_quality_status": "pending", "human_quality_status": "pending"}
        self.ctx.repo.upsert("outputs", "output_id", output_row)
        for slot in segments:
            self.ctx.repo.insert("output_segments", {"output_id": output_id, "segment_id": slot["segment_id"], "asset_id": slot["asset_id"], "slot_index": slot["slot"], "role_used": slot["role"], "start_ms_in_output": slot["start_ms_in_output"], "end_ms_in_output": slot["end_ms_in_output"]})
        manifest_data = {"output_id": output_id, "batch_id": plan["batch_id"], "product_id": plan["product_id"], "template_id": plan["template_id"], "duration_ms": duration_ms, "output_oss_object_id": out_obj["object_id"], "cover_oss_object_id": cover_obj["object_id"], "segments": segments, "subtitles": subtitles, "bgm": {**bgm_plan, "oss_object_id": bgm_object, "loudness_normalized": True}, "machine_quality_status": "pending", "experiment_group": plan["template_id"], "experiment_batch": plan["batch_id"]}
        manifest.write_text(json.dumps(manifest_data, ensure_ascii=False, indent=2), encoding="utf-8")
        man_key = f"auto_mixcut/manifests/{product.get('market','NA')}/{product.get('category','uncategorized')}/{plan['product_id']}/{plan['batch_id']}/variant_{plan['variant_no']:03d}.json"
        man_upload = self.ctx.oss.upload(manifest, man_key)
        if man_upload.success:
            self.ctx.repo.upsert("oss_objects", "object_id", dict(man_upload.data, object_type="manifest", mime_type="application/json"))
        self.ctx.repo.update("render_plans", "render_plan_id", render_plan_id, {"render_status": "rendered"})
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
                "-stream_loop",
                "-1",
                "-i",
                str(source),
                "-t",
                f"{duration:.3f}",
                "-vf",
                "scale=1080:1920:force_original_aspect_ratio=decrease,pad=1080:1920:(ow-iw)/2:(oh-ih)/2,fps=30,format=yuv420p",
                "-c:v",
                "libx264",
                "-preset",
                "veryfast",
                "-an",
                "-shortest",
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
        final_args = [
            "-y",
            "-i",
            str(video_only),
            "-stream_loop",
            "-1",
            "-ss",
            f"{_safe_float(bgm.data.get('recommended_start_sec'), 0):.3f}",
            "-i",
            str(bgm.data["path"]),
            "-filter_complex",
            f"[0:v]{drawtext}[v];[1:a]{_bgm_audio_filter(bgm.data, _planned_duration_ms(plan, slots))}[a]",
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
            "-shortest",
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


def _duration_arg(duration_ms: int) -> str:
    return f"{max(duration_ms, 500) / 1000:.3f}"


def _segment_path(ctx: SkillContext, segment_id: str) -> Path | None:
    segment = ctx.repo.get("segments", "segment_id", segment_id)
    if not segment:
        return None
    return require_oss_object_path(ctx, segment.get("segment_oss_object_id"), "render_segments")


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
            best = recs[0]
            path = require_oss_object_path(ctx, best.get("oss_object_id", ""), "render_bgm")
            if path and path.exists():
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
        "default_volume": 0.2,
        "fade_in_ms": 500,
        "fade_out_ms": 800,
        "matched_template_id": "",
        "matched_mood": "",
    }


def _bgm_manifest(data: dict) -> dict:
    return {
        "bgm_id": data.get("bgm_id") or "",
        "track_name": data.get("track_name") or "",
        "volume": _safe_float(data.get("default_volume"), 0.2),
        "recommended_start_sec": _safe_float(data.get("recommended_start_sec"), 0),
        "fade_in_ms": _safe_int(data.get("fade_in_ms"), 500),
        "fade_out_ms": _safe_int(data.get("fade_out_ms"), 800),
        "matched_template_id": data.get("matched_template_id") or "",
        "matched_mood": data.get("matched_mood") or "",
        "fallback_source": data.get("fallback_source") or "",
    }


def _bgm_audio_filter(data: dict, duration_ms: int) -> str:
    duration_sec = max(duration_ms, 500) / 1000
    volume = min(max(_safe_float(data.get("default_volume"), 0.2), 0.02), 1.0)
    fade_in = max(_safe_int(data.get("fade_in_ms"), 500), 0) / 1000
    fade_out = max(_safe_int(data.get("fade_out_ms"), 800), 0) / 1000
    parts = ["loudnorm=I=-18:TP=-1.5:LRA=11", f"volume={volume:.3f}"]
    if fade_in > 0:
        parts.append(f"afade=t=in:st=0:d={fade_in:.3f}")
    if fade_out > 0:
        parts.append(f"afade=t=out:st={max(duration_sec - fade_out, 0):.3f}:d={fade_out:.3f}")
    return ",".join(parts)


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
    product = ctx.repo.get("products", "product_id", plan["product_id"]) or {}
    category = product.get("category") or ""
    if category == "hair_accessories":
        lines = ["Giu toc gon gang", "Thanh lich cho moi ngay", "Nhe nhang, de phoi do"]
    else:
        lines = ["Chi tiet dep mat", "De dung moi ngay", "Phong cach tu nhien"]
    return [
        {"start_ms": 300, "end_ms": 3300, "text": lines[0]},
        {"start_ms": 5200, "end_ms": 8500, "text": lines[1]},
        {"start_ms": 10800, "end_ms": 14200, "text": lines[2]},
    ]


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
