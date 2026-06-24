"""钩子强度加权（改动1）专项测试。

覆盖方案 docs/hook_capability_plan_v2.md §6.1 验收用例：
- 1-V1: 同 role 池含 1 strong + N weak，首镜 strong 片被选中
- 1-V2: strong 片 usage_count 偏高但仍在阈值内，首镜仍优先 strong
- 1-V4: config strong:0（关闭加权）退化为改动前行为
- 1-V5: 投流模板 hook_weight_scale=2.0 钩子片得分显著高于种草模板 1.0
- 首镜 multiplier 生效、非首镜不加倍
- 老素材 hook_strength=None 不报错且加 0 分
"""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from auto_mixcut.core.bootstrap import build_context
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill
from auto_mixcut.skills.render_plan_skill import (
    TemplateSpec,
    _build_template_constraints,
    _hook_score_weights,
    _segment_score,
    _select_segments,
)


def _empty_batch_state() -> dict:
    return {"segments": set(), "assets": {}, "first_assets": set()}


def _make_segment(
    segment_id: str,
    *,
    hook_strength: str | None = "strong",
    source_trust_level: str = "high",
    asset_id: str | None = None,
    usage_count: int = 0,
    used_in_outputs_count: int = 0,
) -> dict:
    """构造一个可信实拍 segment，只暴露要测的变量。"""
    return {
        "segment_id": segment_id,
        "asset_id": asset_id or f"ASSET_{segment_id}",
        "source_type": "authorized_creator",
        "source_trust_level": source_trust_level,
        "product_binding_type": "exact_sku",
        "product_match_status": "trusted_by_source",
        "effective_roles_json": ["hero"],
        "duration_ms": 3000,
        "usage_count": usage_count,
        "used_in_outputs_count": used_in_outputs_count,
        "used_in_rejected_outputs_count": 0,
        "risk_level": "low",
        "text_overlay_risk": "none",
        # hook_strength 直接放 segment 上，_latest_tag_value 会优先读 segment 内嵌值
        "hook_strength": hook_strength,
    }


class HookScoringTest(unittest.TestCase):
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
        RDSRepositorySkill(self.ctx).init_db()

    def tearDown(self):
        self.tmp.cleanup()

    # 1-V1: 同 role 池含 1 strong + N weak，首镜 strong 片得分更高
    def test_strong_beats_weak_in_first_slot(self):
        slot = {"role": "hero", "duration_ms": 3000}
        strong = _make_segment("SEG_STRONG_V1", hook_strength="strong")
        weak = _make_segment("SEG_WEAK_V1", hook_strength="weak")
        bs = _empty_batch_state()
        strong_score = _segment_score(self.ctx, strong, [], bs, slot, slot_index=1, variant_no=1)
        weak_score = _segment_score(self.ctx, weak, [], bs, slot, slot_index=1, variant_no=1)
        self.assertGreater(strong_score, weak_score, "strong hook should outscore weak in first slot")

    # 1-V2: strong 片 usage 偏高但仍优先于 weak
    def test_strong_with_usage_still_beats_weak(self):
        slot = {"role": "hero", "duration_ms": 3000}
        strong_used = _make_segment("SEG_STRONG_USED", hook_strength="strong", usage_count=3, used_in_outputs_count=2)
        weak_fresh = _make_segment("SEG_WEAK_FRESH", hook_strength="weak", usage_count=0)
        bs = _empty_batch_state()
        strong_score = _segment_score(self.ctx, strong_used, [], bs, slot, slot_index=1, variant_no=1)
        weak_score = _segment_score(self.ctx, weak_fresh, [], bs, slot, slot_index=1, variant_no=1)
        self.assertGreater(strong_score, weak_score, "strong hook with moderate usage should still beat fresh weak")

    # 1-V4: config strong:0 退化为改动前行为（strong 不再加分）
    def test_disabling_hook_weight_regresses_to_baseline(self):
        slot = {"role": "hero", "duration_ms": 3000}
        strong = _make_segment("SEG_STRONG_V4", hook_strength="strong")
        weak = _make_segment("SEG_WEAK_V4", hook_strength="weak")
        bs = _empty_batch_state()
        with patch("auto_mixcut.skills.render_plan_skill._hook_score_weights", return_value={
            "strong": 0, "medium": 0, "weak": 0, "none": 0, "first_slot_multiplier": 2.0,
        }):
            strong_off = _segment_score(self.ctx, strong, [], bs, slot, slot_index=1, variant_no=1)
            weak_off = _segment_score(self.ctx, weak, [], bs, slot, slot_index=1, variant_no=1)
        # 权重关闭后，strong 和 weak 的 hook_strength 都加 0，两者分差应仅来自 _stable_spread
        self.assertAlmostEqual(strong_off, weak_off, places=0, msg="with hook weights off, strong==weak (only stable_spread differs)")

    # 1-V5: 投流模板 hook_weight_scale=2.0 比种草 1.0 钩子片得分更高
    def test_ad_template_hook_scale_amplifies_strong(self):
        slot = {"role": "hero", "duration_ms": 3000}
        strong = _make_segment("SEG_STRONG_V5", hook_strength="strong")
        bs = _empty_batch_state()
        nurture_score = _segment_score(self.ctx, strong, [], bs, slot, slot_index=1, variant_no=1, constraints={})
        ad_score = _segment_score(self.ctx, strong, [], bs, slot, slot_index=1, variant_no=1, constraints={"hook_weight_scale": 2.0})
        delta = ad_score - nurture_score
        # strong=40, first_slot_multiplier=2.0 → 基础 80；scale 2.0 → 160；scale 1.0 → 80；差值 80
        self.assertAlmostEqual(delta, 80.0, places=1, msg="ad template (scale=2.0) should add 80 more than nurture (scale=1.0) for strong first slot")

    # 首镜 multiplier 生效、非首镜不加倍
    def test_first_slot_multiplier_only_applies_to_slot_1(self):
        slot = {"role": "hero", "duration_ms": 3000}
        strong = _make_segment("SEG_STRONG_MULT", hook_strength="strong")
        bs = _empty_batch_state()
        first_slot = _segment_score(self.ctx, strong, [], bs, slot, slot_index=1, variant_no=1)
        later_slot = _segment_score(self.ctx, strong, [], bs, slot, slot_index=3, variant_no=1)
        # 首镜 strong=40*2=80；非首镜 strong=40*1=40；钩子部分差值 40。
        # 另有首镜核心角色 +20（:736-737，effective_role 含 hero 时 slot_index==1 才加），
        # 以及 _stable_spread 的小确定性差异。因此 delta 应 ≥ 40（钩子差）且 < 80（不会超过钩子差+角色加分）。
        delta = first_slot - later_slot
        self.assertGreaterEqual(delta, 40.0, "first slot should score at least 40 more (hook multiplier delta)")
        self.assertLess(delta, 80.0, "delta should stay under 80 (hook 40 + role 20 + spread, not more)")

    # 老素材 hook_strength=None 不报错且加 0 分（等价 weak）
    def test_null_hook_strength_scores_zero(self):
        slot = {"role": "hero", "duration_ms": 3000}
        null_hook = _make_segment("SEG_NULL_HOOK", hook_strength=None)
        weak = _make_segment("SEG_WEAK_NULL", hook_strength="weak")
        bs = _empty_batch_state()
        null_score = _segment_score(self.ctx, null_hook, [], bs, slot, slot_index=1, variant_no=1)
        weak_score = _segment_score(self.ctx, weak, [], bs, slot, slot_index=1, variant_no=1)
        self.assertAlmostEqual(null_score, weak_score, places=0, msg="null hook_strength should score same as weak (both +0)")

    # _build_template_constraints：种草模板返回空 dict，投流模板返回 hook_weight_scale
    def test_build_template_constraints_nurture_vs_ad(self):
        nurture = TemplateSpec(
            template_id="NURTURE_15S", duration_ms=15000, slots=[], default_moods=[],
            suitable_categories=[], template_objective="balanced", pacing="balanced",
            required_roles=[], risk_policy={}, source_policy={}, bgm_profile={},
        )
        ad = TemplateSpec(
            template_id="AD_FAST_HOOK_8S", duration_ms=8000, slots=[], default_moods=[],
            suitable_categories=[], template_objective="ad_fast_hook", pacing="fast",
            required_roles=["hero"], risk_policy={}, source_policy={}, bgm_profile={},
            selection_policy={"hook_weight_scale": 2.0, "require_hook_visual_first_slot": True},
        )
        self.assertEqual(_build_template_constraints(nurture), {}, "nurture template must produce empty constraints")
        ad_c = _build_template_constraints(ad)
        self.assertEqual(ad_c.get("hook_weight_scale"), 2.0)
        self.assertTrue(ad_c.get("require_hook_visual_first_slot"))

    def test_ad_template_constraints_include_unique_source_floor(self):
        ad = TemplateSpec(
            template_id="AD_FAST_HOOK_8S", duration_ms=8000, slots=[], default_moods=[],
            suitable_categories=[], template_objective="ad_fast_hook", pacing="fast",
            required_roles=["hero"], risk_policy={}, source_policy={}, bgm_profile={},
            selection_policy={"hook_weight_scale": 2.0, "require_hook_visual_first_slot": True, "min_unique_source_assets": 3},
        )
        ad_c = _build_template_constraints(ad)
        self.assertEqual(ad_c.get("min_unique_source_assets"), 3)

    def test_ad_selection_forces_unique_source_assets(self):
        slots = [
            {"role": "hero", "duration_ms": 3000},
            {"role": "result", "duration_ms": 3000},
            {"role": "detail", "duration_ms": 2000},
        ]
        segments = [
            _make_segment("SEG_HOOK", hook_strength="strong", source_trust_level="medium", asset_id="ASSET_A")
            | {"source_type": "ai_generated", "product_match_status": "anchor_pending", "hook_visual_type": "product_reveal", "product_visibility": "high", "effective_roles_json": ["hero", "detail"]},
            _make_segment("SEG_RESULT_B", hook_strength="strong", source_trust_level="low", asset_id="ASSET_B")
            | {"source_type": "douyin_repost", "product_match_status": "uncertain", "hook_visual_type": "before_after", "product_visibility": "high", "effective_roles_json": ["result", "detail"]},
            _make_segment("SEG_DETAIL_B", hook_strength="medium", source_trust_level="low", asset_id="ASSET_B")
            | {"source_type": "douyin_repost", "product_match_status": "uncertain", "hook_visual_type": "product_reveal", "product_visibility": "high", "effective_roles_json": ["detail"]},
            _make_segment("SEG_DETAIL_C", hook_strength="medium", source_trust_level="low", asset_id="ASSET_C")
            | {"source_type": "douyin_repost", "product_match_status": "uncertain", "hook_visual_type": "product_reveal", "product_visibility": "high", "effective_roles_json": ["detail", "hero"]},
        ]
        state = {
            "segments": set(),
            "segment_counts": {},
            "core_segment_counts": {},
            "assets": {},
            "first_assets": set(),
            "first_asset_counts": {},
            "first_segment_counts": {},
            "_selection_segments": segments,
        }
        res = _select_segments(
            self.ctx,
            "PROD_AD_UNIQUE",
            slots,
            batch_state=state,
            variant_no=1,
            constraints={"require_hook_visual_first_slot": True, "min_unique_source_assets": 3},
        )
        self.assertTrue(res.success, res.to_dict())
        asset_ids = [item["asset_id"] for item in res.data]
        self.assertEqual(len(set(asset_ids)), 3)
        self.assertEqual(asset_ids, ["ASSET_A", "ASSET_B", "ASSET_C"])

    # config 实际加载验证（确保 render_scoring.yaml 被读到）
    def test_hook_score_weights_loaded_from_config(self):
        weights = _hook_score_weights(self.ctx)
        self.assertEqual(weights.get("strong"), 40)
        self.assertEqual(weights.get("medium"), 15)
        self.assertEqual(weights.get("first_slot_multiplier"), 2.0)


if __name__ == "__main__":
    unittest.main()
