from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


SCRIPT = (
    Path("/Users/likeu3/.openclaw/workspace")
    / "skills"
    / "xiaohongshu-video-downloader"
    / "scripts"
    / "download_xhs_video.py"
)
SPEC = importlib.util.spec_from_file_location("download_xhs_video", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class TikTokDownloaderMusicTest(unittest.TestCase):
    def test_extracts_music_asset_for_matching_video(self):
        payload = {
            "itemInfo": {
                "itemStruct": {
                    "id": "1234567890123456789",
                    "music": {
                        "id": "9988",
                        "title": "Example Song",
                        "authorName": "Example Artist",
                        "original": False,
                        "playUrl": "https://example.com/music.mp3",
                    },
                }
            }
        }
        music = MODULE.extract_tiktok_music([payload], "1234567890123456789")
        self.assertEqual(music["id"], "9988")
        self.assertEqual(music["title"], "Example Song")
        self.assertEqual(music["play_url"], "https://example.com/music.mp3")
        self.assertFalse(music["original"])

    def test_extracts_rendered_photo_post_audio(self):
        html = (
            '<a href="https://www.tiktok.com/music/original-sound-7627692912205187861"></a>'
            '<audio src="https://v77.tiktokcdn.com/audio/?mime_type=audio_mpeg&amp;bt=125"></audio>'
        )
        music = MODULE.extract_tiktok_music_from_html(html)
        self.assertEqual(music["id"], "7627692912205187861")
        self.assertEqual(music["title"], "original sound")
        self.assertIn("mime_type=audio_mpeg&bt=125", music["play_url"])
        self.assertTrue(music["original"])


if __name__ == "__main__":
    unittest.main()
