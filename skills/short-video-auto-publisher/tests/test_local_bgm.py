#!/usr/bin/env python3

from __future__ import annotations

import csv
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch


SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from app import local_bgm  # noqa: E402


class LocalBgmLibraryTest(unittest.TestCase):
    def test_loader_accepts_only_manifest_approved_commercial_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            good = root / "good_song__artist__cc0.mp3"
            internal = root / "test_song.m4a"
            missing = root / "missing.mp3"
            good.write_bytes(b"good")
            internal.write_bytes(b"test")
            rows = [
                [good.name, "Library", "https://example/good", "CC0", "yes", "no", "2026-01-01", ""],
                [internal.name, "local", "", "internal_test_only", "no", "no", "2026-01-01", ""],
                [missing.name, "Library", "https://example/missing", "CC0", "yes", "no", "2026-01-01", ""],
            ]
            with (root / "LICENSES.csv").open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["file_name", "source", "track_url", "license_type", "commercial_use", "attribution_required", "downloaded_at", "notes"])
                writer.writerows(rows)
            with patch("app.local_bgm._probe_duration_ms", return_value=30_000):
                tracks = local_bgm.load_licensed_tracks(bgm_root=root)
        self.assertEqual(len(tracks), 1)
        self.assertEqual(tracks[0]["license_type"], "CC0")
        self.assertEqual(tracks[0]["license_source"], "Library")
        self.assertTrue(tracks[0]["music_id"].startswith("LOCAL_"))

    def test_selection_uses_shared_hard_dedupe_rule(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            first = root / "first.mp3"
            second = root / "second.mp3"
            first.write_bytes(b"a")
            second.write_bytes(b"b")
            tracks = [
                {"music_id": "USED", "music_title": "Used", "path": str(first), "duration_ms": 30_000, "mood_tags": ("dance",)},
                {"music_id": "FRESH", "music_title": "Fresh", "path": str(second), "duration_ms": 30_000, "mood_tags": ("dance",)},
            ]
            analysis = {"status": "ready", "strong_rhythm": True, "features": {"rhythm_strength": 0.8}, "beat_times_ms": [500, 1000, 1500]}
            with patch("app.local_bgm._analysis_for", return_value=analysis):
                selected, _score, _top = local_bgm.select_track(
                    tracks=tracks, mood_hints=["dance"], rhythm_preference="strong",
                    video_duration_ms=6000, use_counts={"USED": 2},
                )
        self.assertEqual(selected["music_id"], "FRESH")


if __name__ == "__main__":
    unittest.main()
