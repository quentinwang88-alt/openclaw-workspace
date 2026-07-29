from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from auto_mixcut.core.bootstrap import build_context
from auto_mixcut.skills.product_identity_resolver_skill import ProductIdentityResolverSkill
from auto_mixcut.skills.material_pool_query import list_material_segments
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill
from auto_mixcut.skills.run_manager_material_import_skill import RunManagerMaterialImportSkill


class _FakeFeishuClient:
    def download_attachment_bytes(self, attachment):
        content = b"generated video bytes"
        return content, attachment.get("name") or "generated.mp4", "video/mp4", len(content)


class RunManagerMaterialImportTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        os.environ["AUTO_MIXCUT_ROOT"] = str(Path(__file__).resolve().parents[1])
        os.environ["AUTO_MIXCUT_DB"] = str(root / "db.sqlite")
        os.environ["AUTO_MIXCUT_OSS_ROOT"] = str(root / "oss")
        os.environ["AUTO_MIXCUT_OSS_PROVIDER"] = "local"
        os.environ["AUTO_MIXCUT_TEMP_ROOT"] = str(root / "tmp")
        os.environ["AUTO_MIXCUT_MOCK_FFMPEG"] = "1"
        self.ctx = build_context()
        migrated = RDSRepositorySkill(self.ctx).init_db()
        self.assertTrue(migrated.success, migrated.to_dict())
        self.ctx.repo.upsert(
            "products",
            "product_id",
            {
                "product_id": "TH_LOCAL_1",
                "canonical_product_id": "GLOBAL_1",
                "product_name": "Product",
                "market": "TH",
                "shop_id": "TH01",
                "anchor_status": "confirmed",
            },
        )
        self.importer = RunManagerMaterialImportSkill(self.ctx, app_token="app", table_id="table")

    def tearDown(self):
        self.tmp.cleanup()

    def test_import_is_idempotent_and_uses_canonical_product(self):
        fields = {
            "结果回传状态": "uploaded",
            "生成视频": [{"file_token": "file1", "name": "result.mp4", "size": 21}],
            "脚本ID": "SCRIPT_1",
            "产品ID": "TH_LOCAL_1",
            "任务来源": "轻量试穿视频",
            "渠道": "iMini",
            "模型": "Seedance 2.0",
            "最新追踪ID": "TRACE_1",
            "完成时间": "2026-07-22T10:00:00",
        }
        candidates = self.importer.inspect_record("rec1", fields)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].canonical_product_id, "GLOBAL_1")
        first = self.importer.import_candidate(_FakeFeishuClient(), candidates[0], dry_run=False)
        self.assertTrue(first.success, first.to_dict())
        second = self.importer.import_candidate(_FakeFeishuClient(), candidates[0], dry_run=False)
        self.assertTrue(second.success, second.to_dict())
        self.assertEqual(second.data["status"], "already_imported")
        self.assertTrue(second.data["object_key"])
        assets = self.ctx.repo.list_where("assets", "canonical_product_id=?", ("GLOBAL_1",))
        self.assertEqual(len(assets), 1)
        self.assertEqual(assets[0]["product_id"], "GLOBAL_1")
        self.assertEqual(assets[0]["source_flow"], "light_video")

    def test_mixcut_output_is_excluded(self):
        fields = {
            "结果回传状态": "uploaded",
            "生成视频": [{"file_token": "file1", "name": "mixcut.mp4"}],
            "内部脚本键": "mixcut:OUTPUT_1",
            "任务来源": "混剪视频",
        }
        self.assertEqual(self.importer.inspect_record("rec_mix", fields), [])

    def test_legacy_task_name_supplies_canonical_product(self):
        fields = {
            "结果回传状态": "uploaded",
            "生成视频": [{"file_token": "file1", "name": "old.mp4"}],
            "任务名": "1736444730937804794.2478_M1_M",
        }
        candidate = self.importer.inspect_record("rec_legacy", fields)[0]
        self.assertEqual(candidate.product_id, "1736444730937804794")
        self.assertEqual(candidate.canonical_product_id, "1736444730937804794")

    def test_light_review_prefers_raw_initial_video(self):
        importer = RunManagerMaterialImportSkill(
            self.ctx,
            app_token="review_app",
            table_id="review_table",
            source_system="light_video_review",
        )
        fields = {
            "视频任务ID": "LTV_1",
            "商品ID": "1736444730937804794",
            "初始成片": [{"file_token": "initial", "name": "initial.mp4"}],
            "最终视频": [{"file_token": "final", "name": "final.mp4"}],
            "生成渠道": "Jimeng",
            "生成模型": "Seedance",
        }
        candidate = importer.inspect_light_review_record("review_rec", fields)[0]
        self.assertEqual(candidate.attachment["file_token"], "initial")
        self.assertEqual(candidate.source_stage, "raw_initial")
        self.assertEqual(candidate.legacy_flag, 0)
        self.assertEqual(candidate.source_flow, "light_video")
        self.assertEqual(candidate.canonical_product_id, "1736444730937804794")
        result = importer.import_candidate(_FakeFeishuClient(), candidate, dry_run=False)
        self.assertTrue(result.success, result.to_dict())
        registry = self.ctx.repo.get("material_source_registry", "source_key", candidate.source_key)
        self.assertEqual(registry["source_system"], "light_video_review")
        self.assertEqual(registry["source_payload_json"]["source_stage"], "raw_initial")

    def test_light_review_raw_key_does_not_collide_with_former_final_first_key(self):
        importer = RunManagerMaterialImportSkill(
            self.ctx,
            app_token="review_app",
            table_id="review_table",
            source_system="light_video_review",
        )
        fields = {
            "视频任务ID": "LTV_1",
            "商品ID": "1736444730937804794",
            "初始成片": [{"file_token": "initial", "name": "initial.mp4"}],
            "最终视频": [{"file_token": "final", "name": "final.mp4"}],
            "最新追踪ID": "TRACE_SHARED",
        }
        raw = importer.inspect_light_review_record("review_rec", fields)[0]
        former_final = importer.inspect_record(
            "review_rec",
            {
                "结果回传状态": "uploaded",
                "生成视频": fields["最终视频"],
                "商品ID": fields["商品ID"],
                "任务来源": "轻视频复核",
                "最新追踪ID": "TRACE_SHARED",
            },
        )[0]
        self.assertNotEqual(raw.source_key, former_final.source_key)

    def test_light_review_final_only_is_legacy_until_explicit_activation(self):
        importer = RunManagerMaterialImportSkill(
            self.ctx,
            app_token="review_app",
            table_id="review_table",
            source_system="light_video_review",
        )
        fields = {
            "视频任务ID": "LTV_OLD",
            "商品ID": "1736444730937804794",
            "最终视频": [{"file_token": "final", "name": "final.mp4"}],
        }
        candidate = importer.inspect_light_review_record("review_old", fields)[0]
        self.assertEqual(candidate.source_stage, "postprocessed_final_fallback")
        self.assertEqual(candidate.legacy_flag, 1)
        result = importer.import_candidate(_FakeFeishuClient(), candidate, dry_run=False)
        self.assertTrue(result.success, result.to_dict())
        self.assertEqual(result.data["status"], "legacy_only")
        self.assertEqual(self.ctx.repo.list_where("assets", "source_record_id=?", ("review_old",)), [])

    def test_old_record_is_index_only_until_activated(self):
        fields = {
            "结果回传状态": "uploaded",
            "生成视频": [{"file_token": "file1", "name": "old.mp4"}],
            "产品ID": "TH_LOCAL_1",
            "完成时间": "2026-01-01T00:00:00",
        }
        candidate = self.importer.inspect_record("rec_old", fields, cutoff_at="2026-07-01T00:00:00")[0]
        result = self.importer.import_candidate(_FakeFeishuClient(), candidate, dry_run=False)
        self.assertTrue(result.success, result.to_dict())
        self.assertEqual(result.data["status"], "legacy_only")
        self.assertEqual(self.ctx.repo.list_where("assets", "source_record_id=?", ("rec_old",)), [])

    def test_alias_binding_resolves_cross_market_identity(self):
        resolver = ProductIdentityResolverSkill(self.ctx)
        bound = resolver.bind_alias(
            "GLOBAL_1",
            product_id="US_LOCAL_2",
            local_product_id="US-LISTING-2",
            store_id="US01",
            market="US",
        )
        self.assertTrue(bound.success, bound.to_dict())
        identity = resolver.resolve(product_id="US_LOCAL_2")
        self.assertEqual(identity.canonical_product_id, "GLOBAL_1")

    def test_canonical_material_query_can_share_segments_cross_market(self):
        self.ctx.repo.upsert(
            "products",
            "product_id",
            {"product_id": "US_LOCAL_2", "canonical_product_id": "GLOBAL_1", "anchor_status": "confirmed"},
        )
        for index, product_id in enumerate(("TH_LOCAL_1", "US_LOCAL_2"), start=1):
            self.ctx.repo.upsert(
                "segments",
                "segment_id",
                {
                    "segment_id": f"SEG_{index}",
                    "asset_id": f"ASSET_{index}",
                    "product_id": product_id,
                    "canonical_product_id": "GLOBAL_1",
                    "segment_status": "created",
                },
            )
        with patch.dict(os.environ, {"MATERIAL_CANONICAL_PRODUCT_SELECTION_ENABLED": "1"}):
            segments = list_material_segments(self.ctx, "TH_LOCAL_1")
        self.assertEqual({row["product_id"] for row in segments}, {"TH_LOCAL_1", "US_LOCAL_2"})


if __name__ == "__main__":
    unittest.main()
