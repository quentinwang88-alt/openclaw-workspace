from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from auto_mixcut.agent.ai_diversity_budget import AIDiversityBudget
from auto_mixcut.core.bootstrap import build_context
from auto_mixcut.skills.quality_gate_skill import QualityGateSkill
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill
from auto_mixcut.skills.render_plan_skill import RenderPlanSkill
from auto_mixcut.skills.segment_fingerprint_skill import SegmentFingerprintSkill


class AIFactoryStrategyTest(unittest.TestCase):
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

    def test_ai_friendly_templates_do_not_disrupt_first_four_legacy_templates(self):
        product_id = "VN_AI_TEMPLATE_ORDER"
        create = RDSRepositorySkill(self.ctx).create_product_task(product_id, "Clip", "VN", "hair_accessories", 5)
        self.assertTrue(create.success, create.to_dict())
        self.ctx.repo.update("content_tasks", "product_id", product_id, {"allowed_variant_count": 5, "material_tier": "tier_3_full", "material_status": "ready"})
        for idx in range(36):
            self.ctx.repo.upsert(
                "segments",
                "segment_id",
                {
                    "segment_id": f"SEG_AI_TPL_{idx:02d}",
                    "asset_id": f"ASSET_AI_TPL_{idx:02d}",
                    "product_id": product_id,
                    "source_type": "ai_generated" if idx % 3 == 0 else "authorized_creator",
                    "source_trust_level": "high",
                    "product_binding_type": "exact_sku",
                    "product_match_status": "anchor_pass",
                    "effective_roles_json": ["hero", "detail", "result", "scene", "ending"],
                },
            )
        res = RenderPlanSkill(self.ctx).create_plans(product_id, 5)
        self.assertTrue(res.success, res.to_dict())
        plans = self.ctx.repo.list_where("render_plans", "batch_id=? ORDER BY variant_no", (res.data["batch_id"],))
        self.assertEqual([p["template_id"] for p in plans[:4]], ["GENERAL_BALANCED_15S", "RESULT_FIRST_15S", "DETAIL_HOOK_15S", "CLEAN_PRODUCT_PROOF_15S"])
        self.assertIn(plans[4]["planned_duration_ms"], {16000, 20000, 24000})

    def test_quality_gate_accepts_twenty_second_template_duration(self):
        output_id = "OUT_20S_OK"
        product_id = "VN_AI_20S"
        self.ctx.repo.upsert(
            "outputs",
            "output_id",
            {
                "output_id": output_id,
                "batch_id": "BATCH_20S",
                "product_id": product_id,
                "template_id": "AI_PRODUCT_FIRST_20S",
                "duration_ms": 20000,
                "width": 1080,
                "height": 1920,
                "machine_quality_status": "pending",
            },
        )
        for idx, role in enumerate(["hero", "detail", "result", "scene", "ending"], start=1):
            segment_id = f"SEG_20S_{idx}"
            self.ctx.repo.upsert(
                "segments",
                "segment_id",
                {
                    "segment_id": segment_id,
                    "asset_id": f"ASSET_20S_{idx}",
                    "product_id": product_id,
                    "source_type": "ai_generated",
                    "source_trust_level": "high",
                    "product_binding_type": "exact_sku",
                    "product_match_status": "anchor_pass",
                    "effective_roles_json": ["hero", "detail", "result", "scene", "ending"],
                },
            )
            self.ctx.repo.insert(
                "segment_tags",
                {
                    "segment_id": segment_id,
                    "tag_source": "test",
                    "primary_shot_role": role,
                    "secondary_roles_json": [],
                    "product_visibility": "high",
                    "hook_strength": "strong",
                    "mixcut_usability": "yes",
                    "risk_level": "low",
                    "confidence": "high",
                    "needs_human_review": 0,
                    "text_overlay_risk": "none",
                },
            )
            self.ctx.repo.insert(
                "output_segments",
                {
                    "output_id": output_id,
                    "segment_id": segment_id,
                    "asset_id": f"ASSET_20S_{idx}",
                    "slot_index": idx,
                    "role_used": role,
                    "start_ms_in_output": (idx - 1) * 4000,
                    "end_ms_in_output": idx * 4000,
                },
            )
        res = QualityGateSkill(self.ctx).check_output(output_id)
        self.assertTrue(res.success, res.to_dict())
        self.assertNotIn("duration out of supported range", res.data["reasons"])
        self.assertNotIn("duration does not match render plan", res.data["reasons"])

    def test_lightweight_ai_ratio_budget_warns_only_when_mature_and_real_shortage(self):
        product_id = "VN_AI_BUDGET"
        RDSRepositorySkill(self.ctx).create_product_task(product_id, "Budget product", "VN", "hair_accessories", 1)
        for idx in range(81):
            self.ctx.repo.upsert(
                "outputs",
                "output_id",
                {
                    "output_id": f"OUT_BUDGET_{idx:03d}",
                    "batch_id": "BATCH_BUDGET",
                    "product_id": product_id,
                    "machine_quality_status": "publish_ready",
                    "human_quality_status": "pending",
                },
            )
        res = AIDiversityBudget(self.ctx).evaluate(product_id)
        self.assertTrue(res.success, res.to_dict())
        self.assertEqual(res.data["phase"], "mature")
        self.assertEqual(res.data["ai_ratio_cap"], 0.3)
        self.assertEqual(res.data["warning"]["reason"], "diversity_exhausted_real_shortage")
        product = self.ctx.repo.get("products", "product_id", product_id)
        self.assertEqual(product["product_status"], "retire_candidate")
        alerts = self.ctx.repo.list_where("ai_diversity_alerts", "product_id=?", (product_id,))
        self.assertEqual(len(alerts), 1)

    def test_phash_cluster_signal_demotes_phase_early(self):
        product_id = "VN_AI_PHASH"
        for idx, phash in enumerate(["0000000000000000", "0000000000000001", "0000000000000003"]):
            self.ctx.repo.upsert(
                "segment_visual_fingerprints",
                "fingerprint_id",
                {
                    "fingerprint_id": f"FP_PHASH_{idx}",
                    "product_id": product_id,
                    "segment_id": f"SEG_PHASH_{idx}",
                    "source_type": "ai_generated",
                    "phash": phash,
                    "hash_method": "test",
                    "frame_count": 4,
                },
            )
        res = AIDiversityBudget(self.ctx).evaluate(product_id)
        self.assertTrue(res.success, res.to_dict())
        self.assertEqual(res.data["base_phase"], "cold")
        self.assertEqual(res.data["phase"], "ramp")
        self.assertTrue(res.data["phash_signal"]["triggered"])

    def test_completion_decay_signal_demotes_phase_early(self):
        product_id = "VN_AI_COMPLETION"
        rates = [0.90, 0.88, 0.86, 0.25, 0.24, 0.23]
        for idx, rate in enumerate(rates):
            self.ctx.repo.upsert(
                "outputs",
                "output_id",
                {
                    "output_id": f"OUT_COMP_{idx}",
                    "batch_id": "BATCH_COMP",
                    "product_id": product_id,
                    "machine_quality_status": "publish_ready",
                    "avg_completion_rate": rate,
                    "published_at": f"2026-06-0{idx + 1}T00:00:00",
                },
            )
        res = AIDiversityBudget(self.ctx).evaluate(product_id)
        self.assertTrue(res.success, res.to_dict())
        self.assertTrue(res.data["completion_signal"]["triggered"])
        self.assertEqual(res.data["phase"], "ramp")

    def test_trusted_real_anchor_count_prefers_source_identity_and_scene_tag(self):
        product_id = "VN_AI_REAL_DENSITY"
        for idx in range(3):
            asset_id = f"ASSET_REAL_DENSE_{idx}"
            self.ctx.repo.upsert(
                "assets",
                "asset_id",
                {
                    "asset_id": asset_id,
                    "product_id": product_id,
                    "source_type": "authorized_creator",
                    "source_trust_level": "high",
                    "product_binding_type": "exact_sku",
                    "has_watermark": "no",
                    "source_identity": "same_creator",
                    "scene_tag": "same_room",
                },
            )
            self.ctx.repo.upsert(
                "segments",
                "segment_id",
                {
                    "segment_id": f"SEG_REAL_DENSE_{idx}",
                    "asset_id": asset_id,
                    "product_id": product_id,
                    "source_type": "authorized_creator",
                    "source_trust_level": "high",
                    "product_binding_type": "exact_sku",
                    "product_match_status": "trusted_by_source",
                    "effective_roles_json": ["hero", "detail"],
                },
            )
        res = AIDiversityBudget(self.ctx).evaluate(product_id)
        self.assertTrue(res.success, res.to_dict())
        self.assertEqual(res.data["trusted_real_anchor_count"], 1)

    def test_segment_fingerprint_writes_visual_phash(self):
        product_id = "VN_AI_FP"
        segment_id = "SEG_FP_001"
        object_key = f"frames/{product_id}/{segment_id}/frame.jpg"
        path = self.ctx.settings.oss_root / object_key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"fake-frame-image")
        self.ctx.repo.upsert("oss_objects", "object_id", {"object_id": "OBJ_FP_FRAME", "object_key": object_key, "object_type": "frame"})
        self.ctx.repo.upsert(
            "segments",
            "segment_id",
            {
                "segment_id": segment_id,
                "asset_id": "ASSET_FP",
                "product_id": product_id,
                "source_type": "ai_generated",
            },
        )
        self.ctx.repo.upsert("segment_frames", "frame_id", {"frame_id": "FRAME_FP", "segment_id": segment_id, "frame_index": 1, "oss_object_id": "OBJ_FP_FRAME"})
        res = SegmentFingerprintSkill(self.ctx).fingerprint_segment(segment_id)
        self.assertTrue(res.success, res.to_dict())
        segment = self.ctx.repo.get("segments", "segment_id", segment_id)
        self.assertEqual(segment["visual_phash"], res.data["phash"])


if __name__ == "__main__":
    unittest.main()
