from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

from auto_mixcut.core.bootstrap import build_context
from auto_mixcut.skills.bgm_library_skill import BgmLibrarySkill
from auto_mixcut.skills.bgm_tag_fusion_skill import BgmTagFusionSkill
from auto_mixcut.skills.bgm_tagging_skill import BgmTaggingSkill
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill


class BgmSystemTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        os.environ["AUTO_MIXCUT_ROOT"] = str(Path(__file__).resolve().parents[1])
        os.environ["AUTO_MIXCUT_DB"] = str(root / "db.sqlite")
        os.environ["AUTO_MIXCUT_OSS_ROOT"] = str(root / "oss")
        os.environ["AUTO_MIXCUT_OSS_PROVIDER"] = "local"
        os.environ["AUTO_MIXCUT_TEMP_ROOT"] = str(root / "tmp")
        os.environ["AUTO_MIXCUT_MOCK_FFMPEG"] = "1"
        os.environ["AUTO_MIXCUT_MOCK_LLM"] = "1"
        self.ctx = build_context()
        init = RDSRepositorySkill(self.ctx).init_db()
        self.assertTrue(init.success, init.to_dict())

    def tearDown(self):
        self.tmp.cleanup()

    def test_local_library_tagging_and_audio_fusion_are_consumable(self):
        sync = BgmLibrarySkill(self.ctx).sync_local_library()
        self.assertTrue(sync.success, sync.to_dict())
        self.assertGreater(sync.data["synced"], 0)

        track = self.ctx.repo.list_where("bgm_tracks", "1=1 ORDER BY bgm_id")[0]
        tag = BgmTaggingSkill(self.ctx).tag_track(track["bgm_id"], force=True)
        self.assertTrue(tag.success, tag.to_dict())

        self.ctx.repo.update(
            "bgm_tracks",
            "bgm_id",
            track["bgm_id"],
            {
                "audio_analysis_json": {
                    "audio_suggested_tags": {
                        "mood_tags": ["fashion_chic"],
                        "energy_level": "high",
                        "vocal_type": "instrumental",
                    },
                    "mix_suggestions": {
                        "recommended_start_sec": 4,
                        "default_volume": 0.18,
                        "fade_in_ms": 300,
                        "fade_out_ms": 700,
                        "voiceover_friendly": True,
                    },
                    "tag_confidence": "high",
                },
                "audio_tag_confidence": "high",
            },
        )
        fuse = BgmTagFusionSkill(self.ctx).fuse_track(track["bgm_id"])
        self.assertTrue(fuse.success, fuse.to_dict())

        row = self.ctx.repo.get("bgm_tracks", "bgm_id", track["bgm_id"])
        self.assertEqual(row["energy_level"], "high")
        self.assertEqual(row["vocal_type"], "instrumental")
        self.assertIn("fashion_chic", row["mood_tags_json"])
        self.assertEqual(row["recommended_start_sec"], 4)
        self.assertIn("tag_fusion", row["mix_constraints_json"])

        rec = BgmLibrarySkill(self.ctx).get_recommendation(category="generic_fashion", mood="fashion_chic")
        self.assertTrue(rec.success, rec.to_dict())
        self.assertTrue(rec.data["recommendations"])


if __name__ == "__main__":
    unittest.main()
