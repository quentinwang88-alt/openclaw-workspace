#!/usr/bin/env python3
"""BGM selection policy tests (pure, no NeoBund)."""

from __future__ import annotations

from pathlib import Path
import sys
import unittest

TESTS_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = TESTS_DIR.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from domain.contracts import validate_bgm_selection_payload
from services import neobund_music
from services.neobund_music import BgmCandidate, BgmSelectionError


def candidate(mid, title="song", rank=None, tags=(), duration_ms=None, hot=None):
    return BgmCandidate(
        music_id=mid, title=title, rank=rank, mood_tags=tags,
        duration_ms=duration_ms, hot_score=hot,
    )


class ScoringTest(unittest.TestCase):
    def test_mood_match_beats_hotter_but_offmood_song(self) -> None:
        onmood = candidate("m1", rank=10, tags=("bright", "playful"))
        offmood = candidate("m2", rank=1, tags=("sad", "slow"))
        ranked = neobund_music.score_candidates(
            [offmood, onmood],
            mood_hints=["bright", "playful"],
            video_duration_ms=12500,
            use_counts={},
        )
        self.assertEqual(ranked[0][0].music_id, "m1")

    def test_weights_sum_to_one(self) -> None:
        self.assertAlmostEqual(sum(neobund_music.SCORE_WEIGHTS.values()), 1.0)

    def test_hot_score_defaults_from_rank(self) -> None:
        self.assertGreater(candidate("a", rank=1).hot_score, candidate("b", rank=30).hot_score)
        self.assertAlmostEqual(candidate("c").hot_score, 0.5)

    def test_duration_fit_prefers_longer_songs(self) -> None:
        long_song = candidate("m1", duration_ms=30000, hot=0.5)
        short_song = candidate("m2", duration_ms=6000, hot=0.5)
        ranked = neobund_music.score_candidates(
            [short_song, long_song],
            mood_hints=[],
            video_duration_ms=12500,
            use_counts={},
        )
        self.assertEqual(ranked[0][0].music_id, "m1")

    def test_dedup_hard_excludes_song_at_limit(self) -> None:
        burned = candidate("m1", rank=1)
        ranked = neobund_music.score_candidates(
            [burned],
            mood_hints=[],
            video_duration_ms=12500,
            use_counts={"m1": neobund_music.DEDUP_MAX_USES},
        )
        self.assertEqual(ranked, [])

    def test_freshness_penalizes_previous_use(self) -> None:
        fresh = neobund_music.freshness_score(0)
        once = neobund_music.freshness_score(1)
        self.assertGreater(fresh, once)
        self.assertEqual(neobund_music.freshness_score(2), 0.0)

    def test_select_top_returns_requested_count(self) -> None:
        pool = [candidate(f"m{i}", rank=i) for i in range(1, 6)]
        top = neobund_music.select_top(
            pool, mood_hints=[], video_duration_ms=12500, use_counts={}, top_n=3
        )
        self.assertEqual(len(top), 3)
        self.assertEqual(top[0][0].music_id, "m1")
        scores = [s for _, s in top]
        self.assertEqual(scores, sorted(scores, reverse=True))


class BgmPayloadTest(unittest.TestCase):
    def test_build_payload_passes_contract(self) -> None:
        ranked = [(candidate("m1", "Hot Song", rank=1), 0.87)]
        payload = neobund_music.build_bgm_payload(
            ranked,
            country="TH",
            auth_id="OPV_TH_TEST_001",
            selected_at="2026-08-30T20:00:00",
        )
        self.assertEqual(validate_bgm_selection_payload(payload), [])
        self.assertEqual(payload["selected"]["music_id"], "m1")
        self.assertEqual(payload["actual"]["music_id"], "")
        self.assertIsNone(payload["fallback_reason"])

    def test_build_payload_requires_candidates_and_valid_index(self) -> None:
        with self.assertRaises(BgmSelectionError):
            neobund_music.build_bgm_payload([], country="TH", auth_id="a")
        ranked = [(candidate("m1"), 0.5)]
        with self.assertRaises(BgmSelectionError):
            neobund_music.build_bgm_payload(ranked, country="TH", auth_id="a", chosen_index=5)

    def test_pending_capture_payload_passes_contract(self) -> None:
        payload = neobund_music.pending_capture_payload(country="TH", auth_id="a")
        self.assertEqual(validate_bgm_selection_payload(payload), [])
        self.assertEqual(payload["status"], "pending_field_capture")

    def test_downgrade_chain(self) -> None:
        strategy, reason = neobund_music.downgrade("neobund_music_unavailable")
        self.assertEqual(strategy, "embedded_bgm")
        strategy, reason = neobund_music.downgrade("embedded_bgm_unavailable")
        self.assertEqual(strategy, "no_bgm")
        with self.assertRaises(BgmSelectionError):
            neobund_music.downgrade("x", current="no_bgm")


if __name__ == "__main__":
    unittest.main()
