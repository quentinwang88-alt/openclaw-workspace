from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from auto_mixcut.core.ids import new_id
from auto_mixcut.core.result import Result
from auto_mixcut.core.storage_paths import require_oss_object_path

from .context import SkillContext
from .llm_prompts import BGM_ALLOWED_LABELS, _default_bgm_tag


class BgmTaggingSkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx

    def tag_track(self, bgm_id: str, force: bool = False) -> Result:
        track = self.ctx.repo.get("bgm_tracks", "bgm_id", bgm_id)
        if not track:
            return Result.fail("BGM_NOT_FOUND", "bgm track not found", {"bgm_id": bgm_id})
        return self.tag_track_row(track, force)

    def tag_track_row(self, track: dict, force: bool = False) -> Result:
        bgm_id = track.get("bgm_id", "")
        current_version = "bgm_tagging_v1.0"

        if not force and self._should_skip(track, current_version):
            return Result.ok({
                "bgm_id": bgm_id,
                "skipped": True,
                "reason": "already tagged with current prompt version",
            })

        self.ctx.repo.update("bgm_tracks", "bgm_id", bgm_id, {
            "bgm_tag_status": "tagging",
        })

        payload = self._build_payload(track)

        audio_path = self._resolve_audio_path(track)
        if audio_path:
            payload["audio_path"] = audio_path

        from .llm_router_skill import LLMRouterSkill
        router = LLMRouterSkill(self.ctx)
        call = router.call(
            "bgm_metadata_tagging",
            {
                **payload,
                "prompt_version": current_version,
            },
            product_id="",
        )

        if not call.success:
            return self._apply_fallback(track, call)

        response = call.data.get("response", {})
        result = self._apply_tags(track, response, current_version)
        return result

    def calibrate_all(self, only_low_confidence: bool = False, force: bool = False, max_concurrency: int = 2) -> Result:
        tracks = self.ctx.repo.list_where("bgm_tracks", "1=1")
        tagged = 0
        skipped = 0
        errors: List[Dict[str, str]] = []
        lock = __import__("threading").Lock()

        def _tag_one(track: dict) -> Optional[Dict[str, str]]:
            bgm_id = track.get("bgm_id", "")
            if only_low_confidence and track.get("tag_confidence") not in {"low", "", "unknown"}:
                with lock:
                    nonlocal skipped
                    skipped += 1
                return None
            try:
                result = self.tag_track_row(track, force)
                if not result.success:
                    return {"bgm_id": bgm_id, "error": result.error.message if result.error else "unknown"}
                with lock:
                    nonlocal tagged
                    tagged += 1
                return None
            except Exception as exc:
                return {"bgm_id": bgm_id, "error": str(exc)}

        with ThreadPoolExecutor(max_workers=max_concurrency) as pool:
            futures = {pool.submit(_tag_one, t): t for t in tracks}
            for future in as_completed(futures):
                err = future.result()
                if err:
                    with lock:
                        errors.append(err)

        return Result.ok({
            "tagged": tagged,
            "skipped": skipped,
            "errors": errors,
        })

    def _should_skip(self, track: dict, current_version: str) -> bool:
        if track.get("bgm_tag_status") != "tagged":
            return False
        if track.get("bgm_tag_prompt_version") != current_version:
            return False
        return True

    def _resolve_audio_path(self, track: dict) -> str:
        local = track.get("local_file_path") or ""
        if local and Path(local).exists():
            return local
        oss_id = track.get("oss_object_id") or ""
        if oss_id:
            path = require_oss_object_path(self.ctx, oss_id, "bgm_tagging")
            if path and path.exists():
                return str(path)
        return ""

    def _build_payload(self, track: dict) -> Dict[str, Any]:
        official_tags = _parse_json_safe(track.get("official_tags_json"), [])
        existing_human = _parse_json_safe(track.get("existing_human_tags_json"), {})
        if not existing_human and track.get("mood_tags_json"):
            existing_human = {
                "mood_tags": _parse_json_safe(track.get("mood_tags_json"), []),
                "energy_level": track.get("energy_level", ""),
                "vocal_type": track.get("vocal_type", ""),
                "category_tags": _parse_json_safe(track.get("category_tags_json"), []),
                "template_tags": _parse_json_safe(track.get("template_tags_json"), []),
            }

        return {
            "bgm_id": track.get("bgm_id", ""),
            "track_name": track.get("track_name", ""),
            "artist_name": track.get("artist_name", ""),
            "source_platform": track.get("source_platform", ""),
            "source_url": track.get("source_url", ""),
            "file_name": track.get("file_name", ""),
            "download_version": track.get("download_version", ""),
            "duration_ms": track.get("duration_ms", 0),
            "official_tags": official_tags if isinstance(official_tags, list) else [],
            "license_note": track.get("license_note", ""),
            "existing_human_tags": existing_human if isinstance(existing_human, dict) else {},
            "allowed_labels": BGM_ALLOWED_LABELS,
        }

    def _apply_tags(self, track: dict, response: dict, version: str) -> Result:
        bgm_id = track.get("bgm_id", "")

        ai_tags = response.get("ai_suggested_tags", {})
        mix = response.get("mix_suggestions", {})
        confidence = response.get("tag_confidence", "low")
        review_required = response.get("tag_review_required", False)
        tag_diff = response.get("tag_diff_json", {}) or {}
        reason = response.get("reason", "")

        has_human_mood = bool(_parse_json_safe(track.get("mood_tags_json"), []))
        has_human_energy = bool(track.get("energy_level"))
        has_human_vocal = bool(track.get("vocal_type"))
        has_human_cat = bool(_parse_json_safe(track.get("category_tags_json"), []))
        has_human = has_human_mood or has_human_energy or has_human_vocal or has_human_cat

        values: Dict[str, Any] = {
            "ai_suggested_tags_json": json.dumps(ai_tags, ensure_ascii=False),
            "tag_diff_json": json.dumps(tag_diff, ensure_ascii=False),
            "tag_confidence": confidence,
            "tag_review_required": int(review_required or (has_human and any(
                self._conflict(a, track) for a in (ai_tags.get("mood_tags") or [])
            ))),
            "bgm_tag_reason": reason,
            "bgm_tag_status": "tagged",
            "bgm_tagged_at": datetime.utcnow().isoformat(timespec="seconds"),
            "bgm_tag_prompt_version": version,
        }

        if not has_human:
            values.update({
                "mood_tags_json": json.dumps(ai_tags.get("mood_tags", []) or [], ensure_ascii=False),
                "energy_level": ai_tags.get("energy_level", "medium"),
                "vocal_type": ai_tags.get("vocal_type", "unknown"),
                "category_tags_json": json.dumps(ai_tags.get("category_tags", []) or [], ensure_ascii=False),
                "template_tags_json": json.dumps(ai_tags.get("template_tags", []) or [], ensure_ascii=False),
                "recommended_start_sec": float(mix.get("recommended_start_sec", 0)),
                "default_volume": float(mix.get("default_volume", 0.2)),
                "fade_in_ms": int(mix.get("fade_in_ms", 500)),
                "fade_out_ms": int(mix.get("fade_out_ms", 800)),
                "suitable_for_intro": int(mix.get("suitable_for_intro", True)),
                "loop_friendly": int(mix.get("loop_friendly", False)),
                "voiceover_friendly": int(mix.get("voiceover_friendly", True)),
            })

        res = self.ctx.repo.update("bgm_tracks", "bgm_id", bgm_id, values)
        if not res.success:
            return res

        return Result.ok({
            "bgm_id": bgm_id,
            "tag_confidence": confidence,
            "tag_review_required": values["tag_review_required"],
            "mood_tags": ai_tags.get("mood_tags", []),
        })

    def _conflict(self, mood: str, track: dict) -> bool:
        human_moods = _parse_json_safe(track.get("mood_tags_json"), [])
        return isinstance(human_moods, list) and mood not in human_moods

    def _apply_fallback(self, track: dict, call_result: Result) -> Result:
        bgm_id = track.get("bgm_id", "")
        fallback = _default_bgm_tag()

        tags = fallback["ai_suggested_tags"]
        mix = fallback["mix_suggestions"]

        values = {
            "ai_suggested_tags_json": json.dumps(tags, ensure_ascii=False),
            "tag_diff_json": json.dumps({}, ensure_ascii=False),
            "tag_confidence": "low",
            "tag_review_required": 1,
            "bgm_tag_reason": f"LLM fallback: {call_result.error.message if call_result.error else 'unknown'}",
            "bgm_tag_status": "fallback",
            "bgm_tagged_at": datetime.utcnow().isoformat(timespec="seconds"),
            "bgm_tag_prompt_version": "bgm_tagging_v1.0",
        }

        has_human = any([
            _parse_json_safe(track.get("mood_tags_json"), []),
            track.get("energy_level"),
            track.get("vocal_type"),
            _parse_json_safe(track.get("category_tags_json"), []),
        ])

        if not has_human:
            values.update({
                "mood_tags_json": json.dumps(tags.get("mood_tags", []) or [], ensure_ascii=False),
                "energy_level": tags.get("energy_level", "medium"),
                "vocal_type": tags.get("vocal_type", "unknown"),
                "category_tags_json": json.dumps(tags.get("category_tags", []) or [], ensure_ascii=False),
                "template_tags_json": json.dumps(tags.get("template_tags", []) or [], ensure_ascii=False),
                "recommended_start_sec": float(mix.get("recommended_start_sec", 0)),
                "default_volume": float(mix.get("default_volume", 0.2)),
                "fade_in_ms": int(mix.get("fade_in_ms", 500)),
                "fade_out_ms": int(mix.get("fade_out_ms", 800)),
                "suitable_for_intro": int(mix.get("suitable_for_intro", True)),
                "loop_friendly": int(mix.get("loop_friendly", False)),
                "voiceover_friendly": int(mix.get("voiceover_friendly", True)),
            })

        self.ctx.repo.update("bgm_tracks", "bgm_id", bgm_id, values)
        return Result.ok({
            "bgm_id": bgm_id,
            "fallback": True,
            "reason": "LLM tagging failed, fallback defaults applied",
        })


def _parse_json_safe(value, default=None):
    if value is None:
        return default
    if isinstance(value, (list, dict)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return default
    return default
