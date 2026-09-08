from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from PIL import Image, ImageDraw

from services.photo_style_reference_analyzer import PhotoStyleReferenceAnalyzer


class PhotoStyleReferenceAnalyzerTest(unittest.TestCase):
    def test_warm_garment_flat_lay_becomes_structured_profile(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "warm-flat-lay.jpg"
            image = Image.new("RGB", (900, 1600), "#B7926D")
            draw = ImageDraw.Draw(image)
            draw.rectangle((180, 280, 680, 800), fill="#513727")
            draw.rectangle((250, 820, 650, 1380), fill="#E7D5B7")
            image.save(path)
            profile = PhotoStyleReferenceAnalyzer().analyze([str(path)])
        self.assertEqual(profile["presentation_type"], "FLAT_LAY")
        self.assertEqual(profile["temperature"], "warm")
        self.assertEqual(profile["season"], "autumn")
        self.assertIn("warm_brown", profile["palette"])
        self.assertEqual(len(profile["source_hashes"]), 1)


if __name__ == "__main__":
    unittest.main()
