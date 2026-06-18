"""补钩子链路（改动3B）专项测试。

覆盖：
- precheck_hook_coverage：无素材→缺口；有钩子素材→ok；有素材但无钩子→缺口
- HookSupplementSkill.run：
  - 库里钩子充足 → skipped="hook_sufficient"
  - 库里缺钩子 → 委派给 AISupplementWorkbenchSkill（delegated=True）
  - 飞书未启用时 → 委派仍走 sync_for_product（内部 skip feishu，返回 skipped）
"""
from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from auto_mixcut.core.bootstrap import build_context
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill
from auto_mixcut.skills.product_anchor_skill import ProductAnchorSkill
from auto_mixcut.skills.render_plan_skill import precheck_hook_coverage
from auto_mixcut.skills.hook_supplement_skill import HookSupplementSkill


def _make_segment_with_tag(ctx, segment_id, product_id="PROD_HOOK_SUP", *, hook_strength="strong", hook_visual_type="product_reveal", roles=None, risk="low"):
    ctx.repo.upsert("segments", "segment_id", {
        "segment_id": segment_id, "asset_id": f"ASSET_{segment_id}", "product_id": product_id,
        "source_type": "self_shot", "source_trust_level": "high",
        "product_binding_type": "exact_sku", "product_match_status": "trusted_by_source",
        "effective_roles_json": roles or ["hero"],
    })
    ctx.repo.insert("segment_tags", {
        "segment_id": segment_id, "tag_source": "ai", "primary_shot_role": "hero",
        "hook_strength": hook_strength, "hook_visual_type": hook_visual_type,
        "product_visibility": "high", "mixcut_usability": "yes", "risk_level": risk,
        "confidence": "high", "needs_human_review": 0,
    })


class HookSupplementTest(unittest.TestCase):
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
        # 飞书在测试环境禁用，sync_for_product 会 skip feishu 但不报错
        os.environ["AUTO_MIXCUT_FEISHU_ENABLED"] = "0"
        self.ctx = build_context()
        RDSRepositorySkill(self.ctx).init_db()
        RDSRepositorySkill(self.ctx).create_product_task("PROD_HOOK_SUP", "Test hook", "VN", "hair_accessories", 1)
        ProductAnchorSkill(self.ctx).draft_anchor("PROD_HOOK_SUP")
        ProductAnchorSkill(self.ctx).confirm_anchor("PROD_HOOK_SUP")

    def tearDown(self):
        self.tmp.cleanup()

    # === precheck_hook_coverage 测试 ===

    def test_precheck_no_segments_returns_gap(self):
        result = precheck_hook_coverage(self.ctx, "PROD_HOOK_SUP", required_count=1)
        self.assertFalse(result["ok"])
        self.assertEqual(result["gap"]["missing_role"], "hero")
        self.assertEqual(result["gap"]["shortfall"], 1)

    def test_precheck_has_hook_segment_returns_ok(self):
        _make_segment_with_tag(self.ctx, "SEG_PRECHECK_OK", hook_strength="strong", hook_visual_type="effect_reveal")
        result = precheck_hook_coverage(self.ctx, "PROD_HOOK_SUP", required_count=1)
        self.assertTrue(result["ok"])
        self.assertEqual(result["current_candidates"], 1)

    def test_precheck_segment_without_visual_hook_returns_gap(self):
        _make_segment_with_tag(self.ctx, "SEG_NO_VISUAL", hook_strength="strong", hook_visual_type="none")
        result = precheck_hook_coverage(self.ctx, "PROD_HOOK_SUP", required_count=1)
        self.assertFalse(result["ok"])
        self.assertEqual(result["gap"]["shortfall"], 1)

    def test_precheck_weak_hook_returns_gap(self):
        _make_segment_with_tag(self.ctx, "SEG_WEAK", hook_strength="weak", hook_visual_type="action")
        result = precheck_hook_coverage(self.ctx, "PROD_HOOK_SUP", required_count=1)
        self.assertFalse(result["ok"])

    def test_precheck_medium_hook_with_visual_returns_ok(self):
        _make_segment_with_tag(self.ctx, "SEG_MED", hook_strength="medium", hook_visual_type="face_emotion")
        result = precheck_hook_coverage(self.ctx, "PROD_HOOK_SUP", required_count=1)
        self.assertTrue(result["ok"])

    # === HookSupplementSkill.run 测试 ===

    def test_hook_sufficient_returns_skipped(self):
        _make_segment_with_tag(self.ctx, "SEG_OK", hook_strength="strong", hook_visual_type="effect_reveal")
        result = HookSupplementSkill(self.ctx).run("PROD_HOOK_SUP")
        self.assertTrue(result.success)
        self.assertEqual(result.data.get("skipped"), "hook_sufficient")

    def test_hook_gap_delegates_to_supplement_workbench(self):
        # 无任何钩子素材 → 应委派给 sync_for_product
        result = HookSupplementSkill(self.ctx).run("PROD_HOOK_SUP")
        self.assertTrue(result.success, result.to_dict())
        # 飞书禁用时 sync_for_product 返回 skipped，但 HookSupplementSkill 仍标记 delegated
        self.assertTrue(result.data.get("delegated"), "should delegate to AISupplementWorkbenchSkill")
        self.assertIn("hero首镜", result.data.get("gap_text", ""))

    def test_hook_gap_writes_blocked_reason_to_task(self):
        # 缺口委派后，content_tasks.blocked_reason 应被写入 gap_text
        result = HookSupplementSkill(self.ctx).run("PROD_HOOK_SUP")
        self.assertTrue(result.success)
        task = self.ctx.repo.list_where("content_tasks", "product_id=?", ("PROD_HOOK_SUP",))
        self.assertTrue(task, "task should exist")
        blocked_reason = task[0].get("blocked_reason") or ""
        self.assertIn("AI补素材", blocked_reason)
        self.assertIn("hero首镜", blocked_reason)


if __name__ == "__main__":
    unittest.main()
