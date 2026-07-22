from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from auto_mixcut.agent.orchestrator import AutoMixcutOrchestratorAgent
from auto_mixcut.core.bootstrap import build_context
from auto_mixcut.skills.oss_storage_skill import OSSStorageSkill
from auto_mixcut.skills.product_anchor_skill import ProductAnchorSkill
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill


class AutoMixcutMockE2ETest(unittest.TestCase):
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
        # 测试必须禁飞书，防止 run_product 的 sync_task 把测试商品写进飞书任务表
        os.environ["AUTO_MIXCUT_FEISHU_ENABLED"] = "0"
        self.ctx = build_context()
        init = RDSRepositorySkill(self.ctx).init_db()
        self.assertTrue(init.success, init.to_dict())

    def tearDown(self):
        self.tmp.cleanup()

    def test_product_pipeline_generates_outputs_and_lineage(self):
        create = RDSRepositorySkill(self.ctx).create_product_task("VN_HAIR_001", "Pearl bow hair clip", "VN", "hair_accessories", 2)
        self.assertTrue(create.success, create.to_dict())
        self.assertTrue(ProductAnchorSkill(self.ctx).draft_anchor("VN_HAIR_001").success)
        self.assertTrue(ProductAnchorSkill(self.ctx).confirm_anchor("VN_HAIR_001").success)
        assets = self._create_mock_assets(7)
        for path in assets:
            res = OSSStorageSkill(self.ctx).upload_asset("VN_HAIR_001", str(path), source_type="self_shot", source_trust_level="high", product_binding_type="exact_sku")
            self.assertTrue(res.success, res.to_dict())
        run = AutoMixcutOrchestratorAgent(self.ctx).run_product("VN_HAIR_001", requested_count=2, auto_confirm_anchor=True)
        self.assertTrue(run.success, run.to_dict())
        outputs = self.ctx.repo.list_where("outputs", "product_id=?", ("VN_HAIR_001",))
        self.assertGreaterEqual(len(outputs), 1)
        # 钩子加权（改动1）会改变 variant 间的选片顺序，不再假定 outputs[0] 恒为 5 段模板。
        # 这里验证"至少一条成片有完整 5 段 lineage"，不依赖 variant 顺序。
        lineage_counts = []
        for output in outputs:
            lineage = self.ctx.repo.list_where("output_segments", "output_id=?", (output["output_id"],))
            lineage_counts.append(len(lineage))
        self.assertIn(5, lineage_counts, f"expected at least one output with 5-segment lineage, got {lineage_counts}")
        self.assertTrue(self.ctx.repo.list_where("llm_calls", "product_id=?", ("VN_HAIR_001",)))

    def test_watermarked_low_trust_asset_is_excluded_even_after_processing(self):
        create = RDSRepositorySkill(self.ctx).create_product_task("VN_HAIR_002", "Scarf", "VN", "scarves", 1)
        self.assertTrue(create.success, create.to_dict())
        ProductAnchorSkill(self.ctx).draft_anchor("VN_HAIR_002")
        ProductAnchorSkill(self.ctx).confirm_anchor("VN_HAIR_002")
        file_path = Path(self.tmp.name) / "douyin_watermark_clip.mp4"
        file_path.write_bytes(b"mock")
        upload = OSSStorageSkill(self.ctx).upload_asset("VN_HAIR_002", str(file_path), source_type="douyin_repost", source_trust_level="low", product_binding_type="category_reference")
        self.assertTrue(upload.success, upload.to_dict())
        run = AutoMixcutOrchestratorAgent(self.ctx).run_product("VN_HAIR_002", requested_count=1, auto_confirm_anchor=True)
        self.assertFalse(run.success)
        asset = self.ctx.repo.get("assets", "asset_id", upload.data["asset_id"])
        self.assertEqual(asset["has_watermark"], "processed")
        self.assertEqual(self.ctx.repo.list_where("segments", "product_id=?", ("VN_HAIR_002",)), [])

    def test_source_reference_mode_uploads_without_confirming_anchor(self):
        create = RDSRepositorySkill(self.ctx).create_product_task(
            "TH_REF_001", "Source reference jacket", "TH", "outerwear", 1,
        )
        self.assertTrue(create.success, create.to_dict())
        source = Path(self.tmp.name) / "source_reference.mp4"
        source.write_bytes(b"mock source reference video")
        uploaded = OSSStorageSkill(self.ctx).upload_asset(
            "TH_REF_001",
            str(source),
            source_type="ai_generated",
            source_trust_level="medium",
            product_binding_type="exact_sku",
            allow_source_reference=True,
            source_reference_count=2,
        )
        self.assertTrue(uploaded.success, uploaded.to_dict())
        product = self.ctx.repo.get("products", "product_id", "TH_REF_001")
        self.assertEqual(product["anchor_status"], "pending")

    def _create_mock_assets(self, count: int):
        root = Path(self.tmp.name) / "assets"
        root.mkdir(parents=True, exist_ok=True)
        paths = []
        for idx in range(count):
            path = root / f"asset_{idx:02d}.mp4"
            path.write_bytes(f"mock video {idx}".encode("utf-8"))
            paths.append(path)
        return paths


if __name__ == "__main__":
    unittest.main()
