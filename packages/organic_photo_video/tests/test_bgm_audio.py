from __future__ import annotations

from pathlib import Path
import sys
import unittest

import numpy as np


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from services import bgm_audio


class BgmAudioAnalysisTest(unittest.TestCase):
    def test_detects_repeated_percussive_signal_without_model(self) -> None:
        sample_rate = 22_050
        duration_seconds = 8
        samples = np.zeros(sample_rate * duration_seconds, dtype=np.float32)
        # 120 BPM impulses, lightly windowed so the signal resembles a beat.
        for start in range(0, samples.size, sample_rate // 2):
            width = min(500, samples.size - start)
            samples[start : start + width] = np.hanning(width).astype(np.float32)
        analysis = bgm_audio.analyze_audio_array(samples, sample_rate)
        self.assertEqual(analysis["status"], "ready")
        self.assertGreaterEqual(len(analysis["beat_times_ms"]), 8)
        self.assertGreater(analysis["features"]["beat_confidence"], 0.10)
        self.assertIn("rhythm_strength", analysis["features"])

    def test_unavailable_payload_is_explicit(self) -> None:
        payload = bgm_audio._unavailable("cdn failed")
        self.assertEqual(payload["status"], "unavailable")
        self.assertEqual(payload["analysis_version"], bgm_audio.ANALYSIS_VERSION)


if __name__ == "__main__":
    unittest.main()
