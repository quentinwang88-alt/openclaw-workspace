from __future__ import annotations

import math
import unittest

import numpy as np

from auto_mixcut.skills.bgm_audio_analysis_skill import analyze_audio_array


class BgmAudioAnalysisTest(unittest.TestCase):
    def test_audio_analysis_uses_signal_without_metadata(self):
        sample_rate = 22050
        seconds = 12
        t = np.arange(sample_rate * seconds, dtype=np.float32) / sample_rate
        beat = ((np.sin(2 * math.pi * 2.0 * t) > 0.88).astype(np.float32) * 0.8)
        tone = 0.2 * np.sin(2 * math.pi * 440 * t).astype(np.float32)
        samples = np.clip(tone + beat, -1.0, 1.0)

        result = analyze_audio_array(samples, sample_rate)

        self.assertEqual(result["source"], "audio_only_signal_analysis")
        self.assertIn(result["audio_suggested_tags"]["energy_level"], {"medium", "high"})
        self.assertTrue(result["features"]["estimated_bpm"])
        self.assertGreater(result["features"]["beat_confidence"], 0)
        self.assertIn("曲名", result["reason"])


if __name__ == "__main__":
    unittest.main()
