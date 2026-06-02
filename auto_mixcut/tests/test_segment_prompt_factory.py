from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from auto_mixcut.core.bootstrap import build_context
from auto_mixcut.skills.segment_prompt_factory_skill import SegmentPromptFactorySkill


class SegmentPromptFactoryTest(unittest.TestCase):
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

    def tearDown(self):
        self.tmp.cleanup()

    def test_grade_a_controls_generation_policy_without_repeating_anchors(self):
        res = SegmentPromptFactorySkill(self.ctx).build_package(_brief(), _slot(ai_gen_grade="A", person_framing="ai_full_face"))
        self.assertTrue(res.success, res.to_dict())
        package = res.data
        positive = package["prompt"]["positive"]
        negative = package["prompt"]["negative"]

        self.assertEqual(package["duration_sec"], 4)
        self.assertEqual(package["person_framing"], "ai_local")
        self.assertEqual(package["gen_policy"]["num_variants"], 4)
        self.assertTrue(package["gen_policy"]["lock_character_ref"])
        self.assertIn("产品清晰为主体，对焦在产品上", positive)
        self.assertEqual(positive.count("珍珠蝴蝶结轮廓"), 1)
        self.assertIn("不要切镜", negative)
        self.assertIn("不要水印", negative)
        self.assertIn("不要字幕文字", negative)
        self.assertEqual(package["anchor_ref"]["hard_anchors"], ["珍珠蝴蝶结轮廓", "奶油白缎面"])

        saved = self.ctx.repo.get("segment_prompt_packages", "segment_prompt_id", package["segment_prompt_id"])
        self.assertIsNotNone(saved)
        self.assertEqual(saved["package_status"], "created")
        self.assertEqual(saved["prompt_grade"], "A")

    def test_same_segment_type_keeps_prompt_detail_independent_from_grade(self):
        grade_a = SegmentPromptFactorySkill(self.ctx).build_package(_brief(), _slot(ai_gen_grade="A"), persist=False)
        grade_b = SegmentPromptFactorySkill(self.ctx).build_package(_brief(), _slot(ai_gen_grade="B"), persist=False)
        self.assertTrue(grade_a.success, grade_a.to_dict())
        self.assertTrue(grade_b.success, grade_b.to_dict())

        a_positive = grade_a.data["prompt"]["positive"]
        b_positive = grade_b.data["prompt"]["positive"]
        self.assertIn("产品清晰为主体，对焦在产品上", a_positive)
        self.assertNotIn("产品清晰为主体，对焦在产品上", b_positive)
        self.assertIn("带有珍珠蝴蝶结轮廓、奶油白缎面的发饰特写", a_positive)
        self.assertIn("带有珍珠蝴蝶结轮廓、奶油白缎面的发饰特写", b_positive)
        self.assertEqual(a_positive.count("珍珠蝴蝶结轮廓"), 1)
        self.assertEqual(b_positive.count("珍珠蝴蝶结轮廓"), 1)

    def test_category_is_normalized_but_raw_category_is_preserved(self):
        res = SegmentPromptFactorySkill(self.ctx).build_package(_outerwear_brief(category="womens_top"), _slot(ai_gen_grade="A"))
        self.assertTrue(res.success, res.to_dict())
        self.assertEqual(res.data["raw_category"], "womens_top")
        self.assertEqual(res.data["category"], "womens_outerwear")

    def test_womens_outerwear_rejects_full_face_when_not_grade_a(self):
        res = SegmentPromptFactorySkill(self.ctx).build_package(_outerwear_brief(), _slot(ai_gen_grade="B", person_framing="ai_full_face"))
        self.assertFalse(res.success)
        self.assertEqual(res.error.code, "AI_FULL_FACE_FORBIDDEN")

    def test_outerwear_negative_l1_is_complete_and_l2_is_limited(self):
        res = SegmentPromptFactorySkill(self.ctx).build_package(_outerwear_brief(), _slot(ai_gen_grade="A", segment_type="product_display"), persist=False)
        self.assertTrue(res.success, res.to_dict())
        negative = res.data["prompt"]["negative"]

        for redline in ["不要切镜", "不要水印", "不要字幕文字", "不要竞品logo", "商品变形", "错品类", "正脸全身", "前后帧衣服不一致"]:
            self.assertIn(redline, negative)

        l2_items = [
            "错误廓形",
            "变形衣形",
            "错误袖长",
            "错误衣长",
            "塑料假面料",
            "纸片薄面料",
            "融化的缝线",
            "不对称衣领",
            "缺失胸前双翻盖口袋结构",
        ]
        self.assertLessEqual(sum(1 for item in l2_items if item in negative), 6)

    def test_batch_packages_have_unique_perturbations(self):
        res = SegmentPromptFactorySkill(self.ctx).build_packages(_brief(), _slot(ai_gen_grade="A"), count=4, persist=False)
        self.assertTrue(res.success, res.to_dict())
        seeds = [
            tuple(item["gen_policy"]["perturbation_seed_group"][key] for key in ["camera_motion", "time_light", "composition", "color_tone", "props_env", "micro_arc"])
            for item in res.data["packages"]
        ]
        self.assertEqual(len(seeds), len(set(seeds)))

    def test_missing_hard_anchors_fails(self):
        res = SegmentPromptFactorySkill(self.ctx).build_package(_brief(hard_anchors=[]), _slot())
        self.assertFalse(res.success)
        self.assertEqual(res.error.code, "HARD_ANCHORS_REQUIRED")


def _brief(category: str = "hair_accessories", hard_anchors: list[str] | None = None) -> dict:
    return {
        "material_anchor_brief": {
            "product_id": "VN_HAIR_PROMPT_001",
            "display_family": "发饰",
            "product_subtype": "珍珠蝴蝶结发夹",
            "category": category,
            "primary_visual_result": "发夹清楚固定在头发上",
            "must_show": ["发夹在头发上"],
            "must_not_show": ["不要文字贴纸"],
            "hard_anchors": ["珍珠蝴蝶结轮廓", "奶油白缎面"] if hard_anchors is None else hard_anchors,
            "display_anchors": ["温柔发型效果"],
            "key_visual_constraints": ["发饰必须稳定可见"],
            "safe_micro_actions": ["轻轻夹上头发"],
            "forbidden_actions": ["不要竞品logo", "不要字幕文字"],
            "ai_shot_risk_profile": {"consistency_risk": "medium", "deformation_risk": "medium"},
        },
        "ai_local_human_brief": {
            "enabled": True,
            "gaze_options": ["视线略微向下"],
            "micro_behavior_options": ["单手轻轻调整发夹"],
            "body_language_options": ["侧后方局部入镜"],
            "forbidden_performance": ["夸张表演"],
        },
    }


def _outerwear_brief(category: str = "womens_outerwear") -> dict:
    data = _brief(category=category, hard_anchors=["米白色短款版型", "罗纹立领"])
    brief = data["material_anchor_brief"]
    brief.update(
        {
            "product_id": "VN_OUTER_PROMPT_001",
            "display_family": "女装外套",
            "product_subtype": "米白色短款夹克外套",
            "primary_visual_result": "短款外套版型清楚成型",
            "must_show": ["衣长至腰腹位置", "罗纹立领"],
            "key_visual_constraints": ["不要完整正脸全身", "衣服前后帧保持一致"],
            "forbidden_actions": ["正脸全身(AI禁止)", "前后帧衣服不一致", "错配衣长"],
        }
    )
    return data


def _slot(ai_gen_grade: str = "A", person_framing: str = "ai_local", segment_type: str = "product_display") -> dict:
    return {
        "template_id": "RESULT_FIRST_15S",
        "slot_index": 0,
        "slot_role": "hero",
        "hook_intent": "product_clarity",
        "ai_gen_grade": ai_gen_grade,
        "segment_type": segment_type,
        "person_framing": person_framing,
        "duration_sec": 4,
    }


if __name__ == "__main__":
    unittest.main()
