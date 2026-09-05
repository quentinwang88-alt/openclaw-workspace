#!/usr/bin/env python3
"""Select country-local NeoBund music for OPV organic videos at commit time."""

from __future__ import annotations

from datetime import datetime, timedelta
import json
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Tuple


OPV_PACKAGE_ROOT = Path("/Users/likeu3/.openclaw/workspace/packages/organic_photo_video")
if str(OPV_PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(OPV_PACKAGE_ROOT))

from services import neobund_music  # noqa: E402
from services import bgm_audio  # noqa: E402
from services.neobund_publisher import (  # noqa: E402
    DEFAULT_MUSIC_VOLUME,
    DEFAULT_VIDEO_ORIGINAL_SOUND_VOLUME,
    LOCALE_BY_COUNTRY,
    NeoBundTrendingMusicSource,
)
from app.script_pool import resolve_cart  # noqa: E402


_BGM_AUDIO_MODES = {"silent_source_platform_bgm", "generated_nonvoice", "clean_voice"}


class BgmSelectionError(RuntimeError):
    pass


def requires_platform_bgm(candidate: Any) -> bool:
    """Whether an organic candidate participates in the shared BGM policy.

    This is intentionally independent from where a script was generated.  A
    product-bound (cart) video is left untouched until the corresponding
    NeoBund shoppable music contract is captured.
    """
    try:
        if resolve_cart(candidate) != "否":
            return False
    except (TypeError, ValueError):
        return False
    source = str(getattr(candidate, "script_source", "") or "").strip()
    branch = str(getattr(candidate, "content_branch", "") or "").strip()
    markers = " ".join((source, branch)).lower()
    if "混剪" in markers or "mixcut" in markers:
        return False
    context = parse_context(candidate)
    return _audio_mode(candidate, context) in _BGM_AUDIO_MODES


def parse_context(candidate: Any) -> Dict[str, Any]:
    raw = str(getattr(candidate, "script_text", "") or "").strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _strings(values: Any) -> List[str]:
    if isinstance(values, str):
        return [item.strip() for item in values.split(",") if item.strip()]
    if isinstance(values, list):
        return [str(item).strip() for item in values if str(item).strip()]
    return []


def _audio_mode(candidate: Any, context: Dict[str, Any]) -> str:
    """Return only an explicitly declared source-audio state.

    Prompt text is content, not an audio contract: a script saying “口播” does
    not prove that its rendered video contains narration.  Unknown sources are
    deliberately excluded so platform music can never accidentally stack on
    top of speech or a generated soundtrack.
    """
    value = context.get("audio_mode") or getattr(candidate, "audio_mode", "")
    mode = str(value or "").strip().lower()
    aliases = {
        "silent": "silent_source_platform_bgm",
        "no_audio": "silent_source_platform_bgm",
        "voice": "clean_voice",
        "voiceover": "clean_voice",
        "generated": "generated_nonvoice",
    }
    return aliases.get(mode, mode if mode in _BGM_AUDIO_MODES else "unknown")


def derive_bgm_profile(candidate: Any, context: Dict[str, Any]) -> Dict[str, Any]:
    """Build deterministic content requirements without an LLM call."""
    markers = " ".join(
        str(getattr(candidate, attr, "") or "")
        for attr in ("script_source", "publish_purpose", "content_branch", "product_type", "short_video_title")
    )
    content_template = str(context.get("content_template") or context.get("video_template") or "")
    markers = markers.lower()
    explicit_hints = _strings(context.get("bgm_mood_hints"))
    audio_mode = _audio_mode(candidate, context)
    voice = audio_mode == "clean_voice"
    profile = neobund_music.derive_content_profile(
        markers,
        explicit_hints,
        content_template=content_template,
        rhythm_preference=str(context.get("rhythm_preference") or ""),
    )
    return {
        **profile,
        "audio_mode": audio_mode,
        "voice_present": voice,
        "music_sound_volume": 45 if voice else DEFAULT_MUSIC_VOLUME,
        "video_original_sound_volume": 60 if voice else DEFAULT_VIDEO_ORIGINAL_SOUND_VOLUME,
    }


def _video_duration_ms(candidate: Any, context: Dict[str, Any]) -> int:
    value = context.get("video_duration_ms")
    try:
        if value and int(value) > 0:
            return int(value)
    except (TypeError, ValueError):
        pass
    path = Path(str(getattr(candidate, "local_file_path", "") or ""))
    if path.exists():
        try:
            return bgm_audio.video_duration_ms(path)
        except Exception:
            pass
    raise BgmSelectionError("无法确认成片时长，暂不选择 BGM")


def _timing_plan(candidate: Any, profile: Dict[str, Any], video_ms: int) -> Dict[str, Any]:
    analysis = (getattr(candidate, "raw", {}) or {}).get("audio_analysis") or {}
    beats = list(analysis.get("beat_times_ms") or []) if isinstance(analysis, dict) else []
    mode = profile["sync_mode"]
    if mode == "slideshow" and beats:
        return {"mode": "slideshow", "status": "planned", "cut_candidates_ms": [beat for beat in beats if 500 <= beat < video_ms]}
    if mode == "reveal" and beats:
        return {"mode": "reveal", "status": "platform_offset_unverified", "beat_candidates_ms": [beat for beat in beats if beat < video_ms]}
    return {"mode": mode, "status": "not_applicable" if mode == "none" else "no_reliable_beat"}


def _neobund_adapter(publisher: Any, account_id: str) -> Any:
    resolver = getattr(publisher, "_adapter_for_account", None)
    adapter = resolver(account_id) if callable(resolver) else publisher
    if not getattr(adapter, "client", None):
        raise BgmSelectionError("当前发布适配器不支持 NeoBund BGM 搜索")
    return adapter


def select_platform_bgm(
    db: Any,
    publisher: Any,
    *,
    account_id: str,
    candidate: Any,
    now: datetime,
    source_factory=NeoBundTrendingMusicSource,
    audio_enricher=bgm_audio.enrich_candidates,
) -> Tuple[Dict[str, Any], str]:
    """Return the NeoBund commit contract and an auditable compact snapshot."""
    country = str(getattr(candidate, "target_country", "") or "").strip().upper()
    country = {
        "墨西哥": "MX", "MEXICO": "MX", "MÉXICO": "MX", "ES-MX": "MX",
        "西班牙语（墨西哥）": "MX", "泰国": "TH", "THAILAND": "TH", "TH-TH": "TH",
        "越南": "VN", "VIETNAM": "VN", "VIỆT NAM": "VN", "VI-VN": "VN",
    }.get(country, country)
    if country not in LOCALE_BY_COUNTRY:
        raise BgmSelectionError(f"NeoBund BGM 暂不支持国家: {country or '未设置'}")

    context = parse_context(candidate)
    profile = derive_bgm_profile(candidate, context)
    mood_hints = profile["mood_hints"]
    video_ms = _video_duration_ms(candidate, context)

    adapter = _neobund_adapter(publisher, account_id)
    candidates = source_factory(adapter.client).fetch(
        account_id,
        country,
        language=LOCALE_BY_COUNTRY[country],
        mood_hints=mood_hints,
    )
    use_counts = db.recent_bgm_use_counts(
        account_id,
        since=now - timedelta(days=neobund_music.DEDUP_WINDOW_DAYS),
    )
    eligible = neobund_music.eligible_candidates(
        candidates, video_duration_ms=video_ms, use_counts=use_counts,
    )
    if not eligible:
        raise BgmSelectionError("没有时长足够且未超出去重上限的 NeoBund BGM")

    strong = profile["rhythm_preference"] == "strong"
    if strong:
        # Only analyze candidates that already pass hard rules.  If the first
        # batch is unavailable, expand once instead of silently falling back
        # to a title-tagged “Dance” song.
        ranked = []
        analyzed: List[Any] = []
        for start in range(0, min(len(eligible), 16), bgm_audio.MAX_ANALYZED_CANDIDATES):
            batch = eligible[start : start + bgm_audio.MAX_ANALYZED_CANDIDATES]
            audio_enricher(batch)
            analyzed.extend(batch)
            ranked = neobund_music.select_top(
                analyzed, mood_hints=mood_hints, video_duration_ms=video_ms,
                use_counts=use_counts, rhythm_preference=profile["rhythm_preference"],
                require_audio_analysis=True, require_usable_duration=True,
            )
            if ranked:
                break
    else:
        ranked = neobund_music.select_top(
            eligible, mood_hints=mood_hints, video_duration_ms=video_ms,
            use_counts=use_counts, rhythm_preference=profile["rhythm_preference"],
            require_usable_duration=True,
        )
    if not ranked:
        raise BgmSelectionError("未找到符合近期去重规则的 NeoBund BGM")

    selected, score = ranked[0]
    music_url = str(selected.raw.get("play_url") or "").strip()
    author = str(selected.raw.get("author") or "").strip()
    if not music_url or not author:
        raise BgmSelectionError("NeoBund BGM 结果缺少提交所需的作者或音源 URL")

    selection = {
        "music_id": selected.music_id,
        "music_title": selected.title,
        "music_author": author,
        "music_url": music_url,
        "music_cover_url": str(selected.raw.get("cover_url") or "").strip(),
        "music_sound_volume": profile["music_sound_volume"],
        "video_original_sound_volume": profile["video_original_sound_volume"],
    }
    audit = {
        "policy_version": neobund_music.POLICY_VERSION,
        "country": country,
        "music_id": selected.music_id,
        "music_title": selected.title,
        "score": score,
        "profile": profile,
        "video_duration_ms": video_ms,
        "audio_analysis": selected.raw.get("audio_analysis") or {},
        "timing_plan": _timing_plan(selected, profile, video_ms),
        "selected_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "top_candidates": [
            {"music_id": item.music_id, "title": item.title, "score": item_score}
            for item, item_score in ranked
        ],
    }
    return selection, json.dumps(audit, ensure_ascii=False, sort_keys=True)
