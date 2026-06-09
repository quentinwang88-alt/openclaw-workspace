from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from auto_mixcut.core.ids import new_id
from auto_mixcut.core.result import Result

from .context import SkillContext
from .llm_prompts import BGM_ALLOWED_LABELS


_KEYWORD_MAP = {
    "mood": {
        "happy": "daily_clean",
        "upbeat": "energetic",
        "chill": "calm_lifestyle",
        "lofi": "calm_lifestyle",
        "dream": "minimal_clean",
        "rainbow": "soft_feminine",
        "city": "fashion_chic",
        "dance": "energetic",
        "piano": "calm_lifestyle",
        "guitar": "daily_clean",
        "ambient": "minimal_clean",
        "cyberpunk": "fashion_chic",
        "magic": "soft_feminine",
        "space": "minimal_clean",
        "clean": "daily_clean",
        "fresh": "fresh_summer",
        "cute": "cute_light",
        "warm": "warm_cozy",
        "winter": "winter_soft",
        "fashion": "fashion_chic",
        "premium": "premium_clean",
        "minimal": "minimal_clean",
        "soft": "soft_feminine",
        "calm": "calm_lifestyle",
    },
    "energy": {
        "bpm125": "high",
        "bpm120": "high",
        "dance": "high",
        "upbeat": "high",
        "happy": "medium",
        "lofi": "low",
        "chill": "low",
        "dream": "low",
        "ambient": "low",
        "piano": "low",
    },
}


class BgmLibrarySkill:
    def __init__(self, ctx: SkillContext):
        self.ctx = ctx
        self._bgm_root: Optional[Path] = None

    def sync_local_library(self) -> Result:
        bgm_dir = self.ctx.settings.root_dir / "assets" / "bgm"
        if not bgm_dir.exists():
            return Result.ok({"synced": 0, "message": "bgm directory not found"})

        synced = 0
        for path in sorted(bgm_dir.rglob("*")):
            if not path.is_file():
                continue
            if path.name.startswith("test_") or path.suffix.lower() not in {".mp3", ".wav", ".m4a", ".aac"}:
                continue
            if path.suffix == ".invalid":
                continue

            try:
                result = self._sync_one_file(path, bgm_dir)
                if result.success:
                    synced += 1
            except Exception:
                continue

        return Result.ok({"synced": synced})

    def _sync_one_file(self, path: Path, bgm_dir: Path) -> Result:
        file_name = path.name
        bgm_id = self._file_to_bgm_id(file_name, path)

        existing = self.ctx.repo.get("bgm_tracks", "bgm_id", bgm_id)
        if existing:
            self._apply_metadata_fallback_if_missing(existing)
            return Result.ok({"bgm_id": bgm_id, "status": "existing"})

        metadata = self._parse_filename(file_name)
        fallback_tags = self.check_metadata_tags({
            "file_name": file_name,
            "official_tags_json": json.dumps(metadata.get("official_tags", []) or [], ensure_ascii=False),
        })

        oss_res = self.ctx.oss.upload(path, f"auto_mixcut/bgm_library/{path.relative_to(bgm_dir).as_posix()}")
        oss_object_id = oss_res.data.get("object_id", "") if oss_res.success else ""

        row = {
            "bgm_id": bgm_id,
            "track_name": metadata.get("track_name", file_name.rsplit(".", 1)[0]),
            "artist_name": metadata.get("artist_name", ""),
            "source_platform": metadata.get("source_platform", "local"),
            "file_name": file_name,
            "download_version": "Full Mix",
            "duration_ms": self._probe_duration(path),
            "file_size": path.stat().st_size,
            "audio_format": path.suffix.lstrip("."),
            "oss_object_id": oss_object_id,
            "local_file_path": str(path),
            "official_tags_json": json.dumps(metadata.get("official_tags", []) or [], ensure_ascii=False),
            "license_note": metadata.get("license_note", ""),
            "mood_tags_json": json.dumps(fallback_tags.get("mood_tags", []) or [], ensure_ascii=False),
            "energy_level": fallback_tags.get("energy_level", "medium"),
            "vocal_type": fallback_tags.get("vocal_type", "unknown"),
            "category_tags_json": json.dumps(fallback_tags.get("category_tags", []) or [], ensure_ascii=False),
            "template_tags_json": json.dumps(fallback_tags.get("template_tags", []) or [], ensure_ascii=False),
            "recommended_start_sec": fallback_tags.get("recommended_start_sec", 0),
            "default_volume": fallback_tags.get("default_volume", 0.2),
            "fade_in_ms": fallback_tags.get("fade_in_ms", 500),
            "fade_out_ms": fallback_tags.get("fade_out_ms", 800),
            "suitable_for_intro": int(bool(fallback_tags.get("suitable_for_intro", True))),
            "loop_friendly": int(bool(fallback_tags.get("loop_friendly", False))),
            "voiceover_friendly": int(bool(fallback_tags.get("voiceover_friendly", True))),
            "bgm_tag_status": "fallback",
            "tag_confidence": "low",
            "tag_review_required": 0,
            "bgm_tag_reason": "filename metadata fallback",
        }

        return self.ctx.repo.upsert("bgm_tracks", "bgm_id", row)

    def _apply_metadata_fallback_if_missing(self, track: dict) -> None:
        if track.get("bgm_tag_status") not in {"", None, "untagged"}:
            return
        if _parse_json_safe(track.get("mood_tags_json"), []):
            return
        fallback_tags = self.check_metadata_tags(track)
        self.ctx.repo.update(
            "bgm_tracks",
            "bgm_id",
            track["bgm_id"],
            {
                "mood_tags_json": json.dumps(fallback_tags.get("mood_tags", []) or [], ensure_ascii=False),
                "energy_level": fallback_tags.get("energy_level", "medium"),
                "vocal_type": fallback_tags.get("vocal_type", "unknown"),
                "category_tags_json": json.dumps(fallback_tags.get("category_tags", []) or [], ensure_ascii=False),
                "template_tags_json": json.dumps(fallback_tags.get("template_tags", []) or [], ensure_ascii=False),
                "recommended_start_sec": fallback_tags.get("recommended_start_sec", 0),
                "default_volume": fallback_tags.get("default_volume", 0.2),
                "fade_in_ms": fallback_tags.get("fade_in_ms", 500),
                "fade_out_ms": fallback_tags.get("fade_out_ms", 800),
                "suitable_for_intro": int(bool(fallback_tags.get("suitable_for_intro", True))),
                "loop_friendly": int(bool(fallback_tags.get("loop_friendly", False))),
                "voiceover_friendly": int(bool(fallback_tags.get("voiceover_friendly", True))),
                "bgm_tag_status": "fallback",
                "tag_confidence": "low",
                "tag_review_required": 0,
                "bgm_tag_reason": "filename metadata fallback",
            },
        )

    def get_recommendation(self, product_id: str = "", category: str = "", mood: str = "", template_id: str = "") -> Result:
        tracks = self.ctx.repo.list_where("bgm_tracks", "1=1")
        usable = [t for t in tracks if t.get("bgm_tag_status") in {"tagged", "fallback"}]
        if not usable:
            usable = tracks

        scored = []
        for track in usable:
            score = self._score_track(track, category, mood, template_id)
            scored.append((score, track))

        scored.sort(key=lambda x: x[0], reverse=True)

        top = []
        for score, track in scored[:12]:
            mix_suggestions = _audio_mix_suggestions(track)
            top.append({
                "bgm_id": track.get("bgm_id"),
                "track_name": track.get("track_name"),
                "mood_tags": _parse_json_safe(track.get("mood_tags_json"), []),
                "energy_level": track.get("energy_level", "medium"),
                "score": score,
                "usage_count": int(track.get("usage_count") or 0),
                "degrade_mode": "observe",
                "recommended_start_sec": mix_suggestions.get("recommended_start_sec", track.get("recommended_start_sec", 0)),
                "default_volume": mix_suggestions.get("default_volume", track.get("default_volume", 0.2)),
                "fade_in_ms": mix_suggestions.get("fade_in_ms", track.get("fade_in_ms", 500)),
                "fade_out_ms": mix_suggestions.get("fade_out_ms", track.get("fade_out_ms", 800)),
                "oss_object_id": track.get("oss_object_id"),
                "local_file_path": track.get("local_file_path"),
            })

        return Result.ok({"recommendations": top})

    def check_metadata_tags(self, track: dict) -> Dict[str, Any]:
        file_name = track.get("file_name", "").lower()
        name_parts = file_name.replace(".mp3", "").replace(".wav", "").split("__")

        mood_tags = []
        for part in name_parts:
            for keyword, mood in _KEYWORD_MAP["mood"].items():
                if keyword in part and mood not in mood_tags:
                    mood_tags.append(mood)

        energy = "medium"
        for part in name_parts:
            for keyword, level in _KEYWORD_MAP["energy"].items():
                if keyword in part:
                    energy = level
                    break

        official_tags = _parse_json_safe(track.get("official_tags_json"), [])
        official_text = " ".join(str(t).lower() for t in official_tags)
        for keyword, mood in _KEYWORD_MAP["mood"].items():
            if keyword in official_text and mood not in mood_tags:
                mood_tags.append(mood)

        if not mood_tags:
            mood_tags.append("daily_clean")

        return {
            "mood_tags": mood_tags[:3],
            "energy_level": energy,
            "vocal_type": "unknown",
            "category_tags": ["generic_fashion"],
            "template_tags": [],
            "recommended_start_sec": 12,
            "default_volume": 0.2,
            "fade_in_ms": 180,
            "fade_out_ms": 800,
            "suitable_for_intro": True,
            "loop_friendly": False,
            "voiceover_friendly": True,
        }

    def _score_track(self, track: dict, category: str, mood: str, template_id: str) -> float:
        score = 1.0
        cat_tags = _parse_json_safe(track.get("category_tags_json"), [])
        if category and isinstance(cat_tags, list):
            if category in cat_tags:
                score += 0.45
            elif "generic_fashion" in cat_tags:
                score += 0.15
        else:
            score += 0.1

        mood_tags = _parse_json_safe(track.get("mood_tags_json"), [])
        if mood and isinstance(mood_tags, list):
            if mood in mood_tags:
                score += 0.5

        template_tags = _parse_json_safe(track.get("template_tags_json"), [])
        if template_id and isinstance(template_tags, list):
            if template_id in template_tags:
                score += 0.7
            elif any(str(tag).startswith("AI_") and template_id.startswith("AI_") for tag in template_tags):
                score += 0.2

        confidence = track.get("tag_confidence", "medium")
        if confidence == "low":
            score *= 0.5

        energy_level = str(track.get("energy_level") or "")
        if energy_level == "high":
            score += 0.25
        elif energy_level == "medium":
            score += 0.1
        elif energy_level == "low":
            score -= 0.15

        analysis = _parse_json_safe(track.get("audio_analysis_json"), {})
        features = analysis.get("features") if isinstance(analysis, dict) else {}
        suggestions = analysis.get("mix_suggestions") if isinstance(analysis, dict) else {}
        if isinstance(features, dict):
            energy_score = _safe_float(features.get("energy_score"), 0)
            beat_confidence = _safe_float(features.get("beat_confidence"), 0)
            brightness = _safe_float(features.get("brightness_score"), 0)
            bpm = _safe_float(features.get("estimated_bpm"), 0)
            score += min(max(energy_score, 0), 1) * 0.45
            score += min(max(beat_confidence, 0), 1) * 0.25
            score += min(max(brightness, 0), 1) * 0.18
            if 105 <= bpm <= 145:
                score += 0.25
            elif 95 <= bpm < 105:
                score += 0.1
            elif 0 < bpm < 90:
                score -= 0.2
        if isinstance(suggestions, dict):
            if suggestions.get("suitable_for_intro") is True:
                score += 0.2
            start = _safe_float(suggestions.get("recommended_start_sec"), -1)
            if 4 <= start <= 30:
                score += 0.12
            elif start == 0:
                score -= 0.08

        rejected = int(track.get("rejected_usage_count") or 0)
        usage = max(int(track.get("usage_count") or 0), rejected)
        if rejected >= 2 and usage:
            rejection_ratio = rejected / max(usage, 1)
            score -= min(0.45, 0.12 + rejection_ratio * 0.35)

        return score

    def _file_to_bgm_id(self, file_name: str, path: Path) -> str:
        import hashlib
        body = str(path.relative_to(self.ctx.settings.root_dir)) if path.is_relative_to(self.ctx.settings.root_dir) else file_name
        return "BGM_" + hashlib.sha256(body.encode()).hexdigest()[:12].upper()

    def _parse_filename(self, file_name: str) -> Dict[str, Any]:
        parts = Path(file_name).stem.split("__")
        metadata: Dict[str, Any] = {"track_name": parts[0].replace("_", " ").title(), "artist_name": "", "source_platform": "local", "official_tags": [], "license_note": ""}

        if len(parts) >= 2:
            metadata["artist_name"] = parts[1].replace("_", " ").title()
        if len(parts) >= 3:
            license_part = parts[2]
            if license_part.upper() == "CC0":
                metadata["license_note"] = "CC0 Public Domain"
                metadata["source_platform"] = "opengameart"

        return metadata

    def _probe_duration(self, path: Path) -> int:
        try:
            probe = self.ctx.ffmpeg.probe(path)
            if probe.success and probe.data:
                return int(probe.data.get("duration_ms", 0) or 0)
        except Exception:
            pass
        return 0


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


def _audio_mix_suggestions(track: dict) -> dict:
    analysis = _parse_json_safe(track.get("audio_analysis_json"), {})
    if not isinstance(analysis, dict):
        return {}
    suggestions = analysis.get("mix_suggestions") or {}
    return suggestions if isinstance(suggestions, dict) else {}


def _safe_float(value, default=0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)
