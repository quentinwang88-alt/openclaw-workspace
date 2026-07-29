"""视觉钩子打标字段（改动2）专项测试。

覆盖方案 docs/hook_capability_plan_v2.md §6.2 验收用例：
- 2-V1: 新切片打标，segment_tags 多出 hook_visual_type，值在枚举内
- 2-V2: 模型返回非法值/缺字段，解析兜底为 none，打标不失败
- 2-V3: 存量素材 hook_visual_type=NULL，种草线零影响（不消费该字段）
- 2-V4: 明显开箱片 → unboxing（normalize 层验证）
- 2-V5: 平稳背景片 → none（normalize 层验证）
- mock 打标全链路：submit→poll→写库→enrich 能读到 hook_visual_type
- prompt 含 hook_visual_type 枚举字段
"""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from auto_mixcut.core.bootstrap import build_context
from auto_mixcut.skills.rds_repository_skill import RDSRepositorySkill
from auto_mixcut.skills.ai_tagging_skill import AITaggingSkill
from auto_mixcut.skills.render_plan_skill import _enrich_segments_for_selection, _latest_tag_value
from auto_mixcut.skills.llm_prompts import normalize_segment_tag, segment_tagging_prompt


VALID_TYPES = {"unboxing", "before_after", "effect_reveal", "detail_macro", "action", "face_emotion", "product_reveal", "none"}


class HookVisualTaggingTest(unittest.TestCase):
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

    # 2-V2: 模型返回非法值 → 兜底 none
    def test_normalize_invalid_value_falls_back_to_none(self):
        tag = normalize_segment_tag({"hook_visual_type": "totally_bogus", "hook_strength": "strong"})
        self.assertEqual(tag["hook_visual_type"], "none")

    # 2-V2: 模型缺字段 → 兜底 none
    def test_normalize_missing_field_falls_back_to_none(self):
        tag = normalize_segment_tag({"hook_strength": "medium"})
        self.assertEqual(tag["hook_visual_type"], "none")

    # 2-V4: 合法枚举值原样保留
    def test_normalize_valid_values_preserved(self):
        for vt in VALID_TYPES:
            tag = normalize_segment_tag({"hook_visual_type": vt, "hook_strength": "strong"})
            self.assertEqual(tag["hook_visual_type"], vt, f"{vt} should be preserved")

    # normalize 不破坏现有 hook_strength 语义
    def test_normalize_keeps_hook_strength_unchanged(self):
        for hs in ("strong", "medium", "weak"):
            tag = normalize_segment_tag({"hook_strength": hs, "hook_visual_type": "action"})
            self.assertEqual(tag["hook_strength"], hs)

    # prompt 含 hook_visual_type 枚举字段
    def test_prompt_contains_hook_visual_type_enum(self):
        prompt = segment_tagging_prompt({"product_id": "P"}, {"source_type": "self_shot"}, {"segment_id": "S"})
        self.assertIn("hook_visual_type", prompt)
        for vt in VALID_TYPES:
            self.assertIn(vt, prompt, f"prompt should list enum value {vt}")

    # 2-V1: mock 打标全链路——submit→poll→写库→能读到 hook_visual_type
    def test_mock_tagging_pipeline_writes_hook_visual_type(self):
        product_id = "PROD_HOOK_VIS_E2E"
        RDSRepositorySkill(self.ctx).create_product_task(product_id, "Test clip", "VN", "hair_accessories", 1)
        # 建一个 segment + frame，让打标能跑
        segment_id = "SEG_HOOK_VIS_E2E"
        self.ctx.repo.upsert("segments", "segment_id", {
            "segment_id": segment_id, "asset_id": "ASSET_HV", "product_id": product_id,
            "source_type": "self_shot", "source_trust_level": "high",
            "product_binding_type": "exact_sku", "product_match_status": "trusted_by_source",
            "start_ms": 0, "end_ms": 3000,
        })
        self.ctx.repo.insert("segment_frames", {"segment_id": segment_id, "frame_index": 0, "frame_oss_object_id": "f0"})
        self.ctx.repo.insert("segment_frames", {"segment_id": segment_id, "frame_index": 1, "frame_oss_object_id": "f1"})

        AITaggingSkill(self.ctx).submit_batch(product_id)
        res = AITaggingSkill(self.ctx).poll_results(product_id)
        self.assertTrue(res.success, res.to_dict())

        tags = self.ctx.repo.list_where("segment_tags", "segment_id=?", (segment_id,))
        self.assertEqual(len(tags), 1)
        # mock 给 hero(index 0)→product_reveal，必须在枚举内且非 none
        self.assertIn(tags[0]["hook_visual_type"], VALID_TYPES)
        self.assertEqual(tags[0]["hook_visual_type"], "product_reveal")

    def test_tagging_excludes_archived_segments(self):
        product_id = "PROD_ARCHIVED_FILTER"
        for segment_id, status in (("SEG_ACTIVE", "created"), ("SEG_ARCHIVED", "archived")):
            self.ctx.repo.upsert("segments", "segment_id", {
                "segment_id": segment_id,
                "asset_id": f"ASSET_{segment_id}",
                "product_id": product_id,
                "source_type": "light_video",
                "segment_status": status,
            })

        submitted = AITaggingSkill(self.ctx).submit_batch(product_id)

        self.assertTrue(submitted.success, submitted.to_dict())
        self.assertEqual(submitted.data["total_segments"], 1)

    # 2-V1 续：enrich 能把 hook_visual_type 加载进 segment
    def test_enrich_loads_hook_visual_type(self):
        segment_id = "SEG_ENRICH_HV"
        self.ctx.repo.upsert("segments", "segment_id", {
            "segment_id": segment_id, "asset_id": "ASSET_ENRICH", "product_id": "P_ENRICH",
            "source_type": "self_shot", "source_trust_level": "high",
        })
        self.ctx.repo.insert("segment_tags", {
            "segment_id": segment_id, "tag_source": "ai", "primary_shot_role": "hero",
            "hook_strength": "strong", "hook_visual_type": "effect_reveal",
            "product_visibility": "high", "mixcut_usability": "yes", "risk_level": "low",
            "confidence": "high", "needs_human_review": 0,
        })
        enriched = _enrich_segments_for_selection(self.ctx, [{"segment_id": segment_id, "asset_id": "ASSET_ENRICH"}])
        self.assertEqual(len(enriched), 1)
        self.assertEqual(_latest_tag_value(self.ctx, enriched[0], "hook_visual_type"), "effect_reveal")

    # 2-V3: 存量素材无 hook_visual_type → _latest_tag_value 返回 None，不报错
    def test_legacy_segment_without_hook_visual_type_returns_none(self):
        segment_id = "SEG_LEGACY_HV"
        self.ctx.repo.upsert("segments", "segment_id", {
            "segment_id": segment_id, "asset_id": "ASSET_LEGACY", "product_id": "P_LEGACY",
            "source_type": "self_shot", "source_trust_level": "high",
        })
        # 老素材只打 hook_strength，没有 hook_visual_type
        self.ctx.repo.insert("segment_tags", {
            "segment_id": segment_id, "tag_source": "ai", "primary_shot_role": "hero",
            "hook_strength": "strong",
            "product_visibility": "high", "mixcut_usability": "yes", "risk_level": "low",
            "confidence": "high", "needs_human_review": 0,
        })
        enriched = _enrich_segments_for_selection(self.ctx, [{"segment_id": segment_id, "asset_id": "ASSET_LEGACY"}])
        # 老素材读 hook_visual_type 应得 None（列存在但值为 NULL），不抛错
        hv = _latest_tag_value(self.ctx, enriched[0], "hook_visual_type")
        self.assertIsNone(hv, "legacy segment without hook_visual_type should return None, not error")


if __name__ == "__main__":
    unittest.main()
