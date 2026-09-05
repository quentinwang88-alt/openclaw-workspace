"""NeoBund hot-BGM selection policy (opv-bgm-v1, Stage F).

Scoring weights (MODEL_HANDOFF 3.4 / 6.2):

    内容情绪匹配 35%  |  当地热度 30%  |  节奏/时长适配 20%  |  账号近期去重 15%

Hard rules enforced here:

- The same song is used at most ``DEDUP_MAX_USES`` times per account within
  ``DEDUP_WINDOW_DAYS``; songs at the limit are excluded entirely.
- Top-N candidates are produced, Top-1 is the default pick.
- The NeoBund trending-music HTTP fields are NOT yet captured, so this module
  is pure policy: callers feed candidates from a music source. Never feed
  invented field names into the NeoBund API (MODEL_HANDOFF section 14).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from domain import contracts

SCORE_WEIGHTS = {"mood": 0.25, "rhythm": 0.35, "hot": 0.20, "fit": 0.10, "fresh": 0.10}
DEDUP_WINDOW_DAYS = 3
DEDUP_MAX_USES = 2
DEFAULT_TOP_N = 3
POLICY_VERSION = "opv-bgm-v4"  # v4: explicit source/audio + duration safety

MOOD_ALIASES = {
    "bright": {"bright", "playful", "upbeat_pop", "pop", "happy", "fun"},
    "high_energy": {"high_energy", "upbeat_pop", "transformation_beat", "party"},
    "dance": {"dance", "transformation_beat", "mid_tempo_pop", "remix", "beat"},
    "soft": {"soft", "soft_acoustic", "romantic", "lofi", "calm", "chill"},
    "calm": {"calm", "soft_acoustic", "romantic", "lofi", "chill"},
}


class BgmSelectionError(ValueError):
    pass


@dataclass
class BgmCandidate:
    music_id: str
    title: str
    rank: Optional[int] = None
    mood_tags: Tuple[str, ...] = ()
    duration_ms: Optional[int] = None
    hot_score: Optional[float] = None  # normalized 0..1; derived from rank if absent
    raw: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.music_id or not self.title:
            raise BgmSelectionError("candidate requires music_id and title")
        if self.hot_score is None:
            if self.rank is not None and self.rank > 0:
                self.hot_score = max(0.0, 1.0 - (self.rank - 1) / 50.0)
            else:
                self.hot_score = 0.5


def canonical_moods(values: Iterable[str]) -> set:
    raw = {str(value).strip().lower() for value in values if str(value).strip()}
    canonical = set(raw)
    for target, aliases in MOOD_ALIASES.items():
        if raw & aliases:
            canonical.add(target)
    return canonical


def derive_content_profile(
    markers: str,
    mood_hints: Iterable[str] = (),
    *,
    content_template: str = "",
    rhythm_preference: str = "",
) -> Dict[str, Any]:
    """Deterministically map content cues to shared BGM requirements.

    This is the common policy for image-video, remake and original-video
    publishers.  It is intentionally keyword/template based so it costs no
    model calls and gives the same cue the same BGM treatment everywhere.
    """
    text = (str(markers or "") + " " + str(content_template or "")).lower()
    hints = [str(item).strip() for item in mood_hints if str(item).strip()]
    transformation = any(token in text for token in (
        "变装", "前后对比", "揭晓", "transformation", "reveal",
    ))
    outfit = any(token in text for token in ("穿搭", "outfit", "造型", "fashion"))
    calm = any(token in text for token in ("咖啡", "cafe", "生活", "lofi", "氛围", "calm"))
    explicit_rhythm = str(rhythm_preference or "").strip().lower()
    if explicit_rhythm in {"strong", "soft", "balanced"}:
        rhythm = explicit_rhythm
    elif transformation or outfit:
        rhythm = "strong"
        hints = hints or ["dance", "high_energy"]
    elif calm:
        rhythm = "soft"
        hints = hints or ["soft", "calm"]
    else:
        rhythm = "balanced"
        hints = hints or ["bright"]
    return {
        "rhythm_preference": rhythm,
        "mood_hints": hints,
        "sync_mode": "reveal" if transformation else "slideshow" if "图文" in text else "none",
    }


def task_content_profile_inputs(task: object) -> Dict[str, str]:
    """Extract explicit BGM semantics from a frozen OPV content contract.

    Titles are not used as a proxy for tempo. Recipes and goals survive
    localization; an operator can still set the enum ``bgm_rhythm_preference``
    in the plan to override the inferred policy.
    """
    plan = getattr(task, "plan_json", None) or {}
    if not isinstance(plan, dict):
        plan = {}
    bgm = plan.get("bgm") if isinstance(plan.get("bgm"), dict) else {}
    explicit = str(
        bgm.get("rhythm_preference")
        or plan.get("bgm_rhythm_preference")
        or ""
    ).strip().lower()
    # Free-form advice (for example "strong-beat preferred") is not silently
    # elevated to an operator override.
    if explicit not in {"strong", "soft", "balanced"}:
        explicit = ""
    parts = [
        str(bgm.get("content_template") or "").strip(),
        str(plan.get("bgm_content_template") or "").strip(),
        str(getattr(task, "recipe_id", "") or "").strip(),
        str(getattr(task, "content_goal", "") or "").strip(),
        str(plan.get("content_goal") or "").strip(),
    ]
    recipe = plan.get("recipe") if isinstance(plan.get("recipe"), dict) else {}
    parts.extend((
        str(recipe.get("id") or "").strip(),
        str(recipe.get("content_goal") or "").strip(),
    ))
    outfit = getattr(task, "outfit_plan_json", None) or plan.get("outfit_plan")
    if outfit or "OUTFIT_BREAKDOWN" in " ".join(parts).upper():
        # These are canonical policy cues, never displayed copy.
        parts.append("outfit breakdown fashion 穿搭 图文")
    return {
        "content_template": " ".join(part for part in parts if part),
        "rhythm_preference": explicit,
    }


def mood_score(candidate: BgmCandidate, mood_hints: Iterable[str]) -> float:
    hints = canonical_moods(mood_hints)
    if not hints:
        return 0.5  # no theme hints: neutral
    tags = canonical_moods(candidate.mood_tags)
    if not tags:
        return 0.0
    # Only canonical dimensions are comparable. Raw labels remain available
    # for observability but do not dilute a valid alias match.
    dimensions = set(MOOD_ALIASES)
    comparable_hints = hints & dimensions
    comparable_tags = tags & dimensions
    if not comparable_hints:
        return 0.0
    return len(comparable_hints & comparable_tags) / len(comparable_hints)


def duration_fit_score(candidate: BgmCandidate, video_duration_ms: int) -> float:
    if not candidate.duration_ms or candidate.duration_ms <= 0:
        return 1.0  # unknown length: platform trims loops, assume usable
    if candidate.duration_ms >= video_duration_ms:
        return 1.0
    return candidate.duration_ms / max(video_duration_ms, 1)


def has_usable_duration(candidate: BgmCandidate, video_duration_ms: int) -> bool:
    """A platform track must cover the whole final video without looping.

    NeoBund can trim a track, but we do not silently loop an unknown/short
    preview: it makes the advertised beat plan untrustworthy.
    """
    return bool(candidate.duration_ms and candidate.duration_ms >= max(int(video_duration_ms or 0), 1))


def eligible_candidates(
    candidates: Iterable[BgmCandidate],
    *,
    video_duration_ms: int,
    use_counts: Mapping[str, int],
) -> List[BgmCandidate]:
    """Apply hard rules before costly local preview analysis."""
    seen: set[str] = set()
    result: List[BgmCandidate] = []
    for candidate in candidates:
        if candidate.music_id in seen:
            continue
        seen.add(candidate.music_id)
        if int(use_counts.get(candidate.music_id, 0)) >= DEDUP_MAX_USES:
            continue
        if not has_usable_duration(candidate, video_duration_ms):
            continue
        result.append(candidate)
    return result


def rhythm_score(candidate: BgmCandidate, *, preference: str = "balanced") -> float:
    """Return an audio-signal rhythm score, not a title-keyword score.

    Legacy or unavailable previews remain neutral for balanced content.  A
    strong-rhythm request is filtered separately by ``audio_ready`` so that a
    song called "Dance" cannot pass as a verified beat track.
    """
    analysis = candidate.raw.get("audio_analysis") or {}
    features = analysis.get("features") if isinstance(analysis, dict) else None
    if not isinstance(features, dict):
        return 0.5
    value = float(features.get("rhythm_strength") or 0.0)
    if preference == "strong":
        return min(1.0, value * 1.20)
    if preference == "soft":
        # For quiet scenes, extreme percussion is not automatically better.
        return 1.0 - abs(value - 0.42)
    return value


def audio_ready(candidate: BgmCandidate, *, strong: bool = False) -> bool:
    analysis = candidate.raw.get("audio_analysis") or {}
    if not isinstance(analysis, dict) or analysis.get("status") != "ready":
        return False
    return bool(analysis.get("strong_rhythm")) if strong else True


def freshness_score(use_count: int) -> float:
    if use_count <= 0:
        return 1.0
    if use_count >= DEDUP_MAX_USES:
        return 0.0
    return 1.0 - use_count / DEDUP_MAX_USES


def score_candidates(
    candidates: Iterable[BgmCandidate],
    *,
    mood_hints: Iterable[str],
    video_duration_ms: int,
    use_counts: Mapping[str, int],
    rhythm_preference: str = "balanced",
    require_audio_analysis: bool = False,
    require_usable_duration: bool = False,
) -> List[Tuple[BgmCandidate, float]]:
    """Score and rank candidates; songs at the dedup limit are excluded."""
    scored: List[Tuple[BgmCandidate, float]] = []
    for candidate in candidates:
        uses = int(use_counts.get(candidate.music_id, 0))
        if uses >= DEDUP_MAX_USES:
            continue  # hard dedup limit
        if require_usable_duration and not has_usable_duration(candidate, video_duration_ms):
            continue
        if require_audio_analysis and not audio_ready(
            candidate, strong=rhythm_preference == "strong"
        ):
            continue
        total = (
            SCORE_WEIGHTS["mood"] * mood_score(candidate, mood_hints)
            + SCORE_WEIGHTS["rhythm"] * rhythm_score(candidate, preference=rhythm_preference)
            + SCORE_WEIGHTS["hot"] * float(candidate.hot_score or 0.0)
            + SCORE_WEIGHTS["fit"] * duration_fit_score(candidate, video_duration_ms)
            + SCORE_WEIGHTS["fresh"] * freshness_score(uses)
        )
        scored.append((candidate, round(total, 4)))
    scored.sort(key=lambda pair: pair[1], reverse=True)
    return scored


def select_top(
    candidates: Iterable[BgmCandidate],
    *,
    mood_hints: Iterable[str],
    video_duration_ms: int,
    use_counts: Mapping[str, int],
    top_n: int = DEFAULT_TOP_N,
    rhythm_preference: str = "balanced",
    require_audio_analysis: bool = False,
    require_usable_duration: bool = False,
) -> List[Tuple[BgmCandidate, float]]:
    scored = score_candidates(
        candidates,
        mood_hints=mood_hints,
        video_duration_ms=video_duration_ms,
        use_counts=use_counts,
        rhythm_preference=rhythm_preference,
        require_audio_analysis=require_audio_analysis,
        require_usable_duration=require_usable_duration,
    )
    return scored[: max(int(top_n), 0)]


def build_bgm_payload(
    ranked: List[Tuple[BgmCandidate, float]],
    *,
    country: str,
    auth_id: str,
    strategy: str = "platform_hot_bgm",
    chosen_index: int = 0,
    selected_at: str = "",
) -> Dict[str, Any]:
    """Build the opv-bgm-v1 persistence payload (docs/MODEL_HANDOFF 8.3).

    Field names here are OUR persistence contract only; they must not be used
    as NeoBund API payload until the real request fields are captured.
    """
    if not ranked:
        raise BgmSelectionError("no ranked candidates to persist")
    if not 0 <= chosen_index < len(ranked):
        raise BgmSelectionError(f"chosen_index {chosen_index} out of range")
    candidate, score = ranked[chosen_index]
    # Persist the concrete track instance (signed urls expire ~daily, well
    # beyond the 30-120 min selection->submit window; submit reuses them).
    payload: Dict[str, Any] = {
        "schema_version": contracts.BGM_SCHEMA_VERSION,
        "audio_strategy": strategy,
        "selection_policy_version": POLICY_VERSION,
        "country": country,
        "auth_id": auth_id,
        "selected": {
            "music_id": candidate.music_id,
            "title": candidate.title,
            "author": str(candidate.raw.get("author") or ""),
            "music_url": str(candidate.raw.get("play_url") or ""),
            "music_cover_url": str(candidate.raw.get("cover_url") or ""),
            "rank": candidate.rank,
            "selected_at": selected_at,
            "score": score,
            "top_candidates": [
                {"music_id": c.music_id, "title": c.title, "score": s}
                for c, s in ranked
            ],
        },
        "actual": {"music_id": "", "title": "", "confirmed_at": ""},
        "fallback_reason": None,
    }
    contracts.ensure_valid(
        contracts.validate_bgm_selection_payload(payload), "bgm selection"
    )
    return payload


def pending_capture_payload(*, country: str, auth_id: str) -> Dict[str, Any]:
    """Payload recorded when the NeoBund music fields are not captured yet."""
    payload = {
        "schema_version": contracts.BGM_SCHEMA_VERSION,
        "audio_strategy": "platform_hot_bgm",
        "selection_policy_version": POLICY_VERSION,
        "country": country,
        "auth_id": auth_id,
        "status": "pending_field_capture",
        "selected": {
            "music_id": "",
            "title": "",
            "rank": None,
            "selected_at": "",
            "score": None,
        },
        "actual": {"music_id": "", "title": "", "confirmed_at": ""},
        "fallback_reason": None,
    }
    contracts.ensure_valid(
        contracts.validate_bgm_selection_payload(payload), "bgm pending payload"
    )
    return payload


def downgrade(
    reason: str, current: str = "platform_hot_bgm"
) -> Tuple[str, str]:
    """Return (new_strategy, fallback_reason) per the degradation chain.

    platform_hot_bgm -> embedded_bgm or no_bgm. Never the reverse, and a
    locally burned BGM plus a platform BGM must never coexist.
    """
    chain = {"platform_hot_bgm": ("embedded_bgm", reason)}
    if current != "platform_hot_bgm":
        raise BgmSelectionError(f"cannot downgrade from {current!r}")
    new_strategy, fallback_reason = chain[current]
    if reason in ("embedded_bgm_unavailable", "operator_choice_silent"):
        new_strategy = "no_bgm"
    return new_strategy, fallback_reason
