from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from auto_mixcut.core.bootstrap import build_context
from auto_mixcut.skills.product_reference_image_skill import ProductReferenceImageSkill
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill


class ProductReferenceImageSkillTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        os.environ["AUTO_MIXCUT_ROOT"] = str(Path(__file__).resolve().parents[1])
        os.environ["AUTO_MIXCUT_DB_PROVIDER"] = "sqlite"
        os.environ["AUTO_MIXCUT_DB"] = str(root / "db.sqlite")
        os.environ["AUTO_MIXCUT_OSS_ROOT"] = str(root / "oss")
        os.environ["AUTO_MIXCUT_OSS_PROVIDER"] = "local"
        os.environ["AUTO_MIXCUT_TEMP_ROOT"] = str(root / "tmp")
        os.environ["AUTO_MIXCUT_MOCK_FFMPEG"] = "1"
        os.environ["AUTO_MIXCUT_MOCK_LLM"] = "1"
        self.root = root
        self.ctx = build_context()
        ready = RDSRepositorySkill(self.ctx).init_db()
        self.assertTrue(ready.success, ready.to_dict())

    def tearDown(self):
        self.tmp.cleanup()

    def test_ensure_pack_uploads_once_and_reuses_active_pack(self):
        image = self.root / "main.jpg"
        image.write_bytes(b"fake-image-v1")
        skill = ProductReferenceImageSkill(self.ctx)

        first = skill.ensure_pack(
            "1734482585843304442",
            market="TH",
            sku_id="cream",
            sku_label="Cream",
            source_images=[{"path": str(image), "image_role": "main"}],
        )
        self.assertTrue(first.success, first.to_dict())
        pack = first.data["pack"]
        self.assertEqual(pack["reference_image_pack_id"], "REFPACK_TH_1734482585843304442_CREAM_V1")
        self.assertEqual(pack["sku_id"], "CREAM")
        self.assertEqual(pack["version"], 1)
        self.assertEqual(first.data["images"][0]["reference_image_id"].split("_V1_")[0], "REFIMG_TH_1734482585843304442_CREAM")
        self.assertTrue((self.root / "oss" / first.data["images"][0]["object_key"]).exists())

        second = skill.ensure_pack(
            "1734482585843304442",
            market="TH",
            sku_id="cream",
            source_images=[{"path": str(image), "image_role": "main"}],
        )
        self.assertTrue(second.success, second.to_dict())
        self.assertEqual(second.data["pack"]["reference_image_pack_id"], pack["reference_image_pack_id"])

    def test_refresh_pack_versions_without_overwriting_history(self):
        image = self.root / "main.jpg"
        image.write_bytes(b"fake-image-v1")
        skill = ProductReferenceImageSkill(self.ctx)
        first = skill.ensure_pack("P1", market="VN", source_images=[{"path": str(image)}])
        self.assertTrue(first.success, first.to_dict())

        image.write_bytes(b"fake-image-v2")
        refreshed = skill.refresh_pack("P1", market="VN", source_images=[{"path": str(image)}])
        self.assertTrue(refreshed.success, refreshed.to_dict())
        self.assertEqual(refreshed.data["pack"]["reference_image_pack_id"], "REFPACK_VN_P1_DEFAULT_V2")
        self.assertNotEqual(first.data["images"][0]["reference_image_id"], refreshed.data["images"][0]["reference_image_id"])

        active = skill.get_active_pack("P1", market="VN")
        self.assertTrue(active.success, active.to_dict())
        self.assertEqual(active.data["pack"]["version"], 2)
        old_pack = self.ctx.repo.get("product_reference_image_packs", "reference_image_pack_id", "REFPACK_VN_P1_DEFAULT_V1")
        self.assertEqual(old_pack["status"], "archived")


if __name__ == "__main__":
    unittest.main()
