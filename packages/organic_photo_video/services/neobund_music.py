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

SCORE_WEIGHTS = {"mood": 0.35, "hot": 0.30, "fit": 0.20, "fresh": 0.15}
DEDUP_WINDOW_DAYS = 3
DEDUP_MAX_USES = 2
DEFAULT_TOP_N = 3
POLICY_VERSION = "opv-bgm-v2"  # v2: mood via rhythm lexicon tags + dance pool


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


def mood_score(candidate: BgmCandidate, mood_hints: Iterable[str]) -> float:
    hints = {h.strip().lower() for h in mood_hints if h and h.strip()}
    if not hints:
        return 0.5  # no theme hints: neutral
    tags = {t.strip().lower() for t in candidate.mood_tags}
    if not tags:
        return 0.0
    return len(hints & tags) / len(hints)


def duration_fit_score(candidate: BgmCandidate, video_duration_ms: int) -> float:
    if not candidate.duration_ms or candidate.duration_ms <= 0:
        return 1.0  # unknown length: platform trims loops, assume usable
    if candidate.duration_ms >= video_duration_ms:
        return 1.0
    return candidate.duration_ms / max(video_duration_ms, 1)


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
) -> List[Tuple[BgmCandidate, float]]:
    """Score and rank candidates; songs at the dedup limit are excluded."""
    scored: List[Tuple[BgmCandidate, float]] = []
    for candidate in candidates:
        uses = int(use_counts.get(candidate.music_id, 0))
        if uses >= DEDUP_MAX_USES:
            continue  # hard dedup limit
        total = (
            SCORE_WEIGHTS["mood"] * mood_score(candidate, mood_hints)
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
) -> List[Tuple[BgmCandidate, float]]:
    scored = score_candidates(
        candidates,
        mood_hints=mood_hints,
        video_duration_ms=video_duration_ms,
        use_counts=use_counts,
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
