"""Offline coverage for subtractive long-form changes and shared visibility."""
import copy
import unittest
from unittest.mock import patch

from core.longform.keyframes import _base_frame_contract, build_keyframe_contracts
from core.longform.model import build_master_script_prompt
from core.longform.planner import _bridge_state, _compact_world, compile_longform_plan
from core.longform.source_adapter import source_from_product_plan
from core.longform.visual_guidance import segment_visibility
from core.visual_execution_contract import (
    _visual_saliency_contract, build_subject_visibility_guidance,
)
from tests.test_longform_original import fixture


class LongformRelaxedVisibilityTests(unittest.TestCase):
    def setUp(self):
        self.master = fixture(30)
        self.master["product_truth"]["identity_anchors"] = ["奶白色短款外套"]
        self.master["production_world"].update({
            "presentation_mode": "PERSON_ON_CAMERA",
            "scene_contract": {"location": "A客厅", "lighting": "窗边自然光"},
            "outfit": "白背心白裤",
            "visual_saliency": {
                "exposure": {"guidance": "A旧窗光"},
                "opening_focus": {"guidance": "旧全身构图与开场动作"},
            },
        })

    def test_plain_cuts_accept_missing_end_state_keep_units(self):
        for unit in self.master["capture_units"]:
            unit.pop("end_state", None)
            unit.pop("observable_end_state", None)
        plan = compile_longform_plan(self.master)
        self.assertEqual(len(plan["segments"]), 2)
        self.assertTrue(all(len(s["execution_units"]) == 3 for s in plan["segments"]))
        self.assertTrue(build_keyframe_contracts(self.master, plan)["K0"]["prompt"])
        prompt = build_master_script_prompt(self.master)
        self.assertNotIn("每个拍摄单元都应有可自然停住", prompt)
        self.assertIn("连续不等于静止", prompt)

    def test_continuity_can_record_motion_without_stop(self):
        state = _bridge_state(self.master, {
            "observable_end_state": "人物仍在向前行走，右脚迈出",
            "camera": "侧面中景",
        })
        self.assertEqual(state["pose_or_action_state"], "人物仍在向前行走，右脚迈出")
        self.assertEqual(state["product_wear_state"], "商品已经穿好")

    def test_frame_and_video_share_segment_local_exposure_and_separation(self):
        before = copy.deepcopy(self.master)
        for scene in ({"location": "A客厅", "lighting": "窗边自然光"},
                      {"location": "B候车区", "lighting": "现场顶灯"}):
            expected = segment_visibility(self.master, scene)
            frame = _base_frame_contract(self.master, "外套局部", "正在行走", [],
                                         scene_block=scene, camera="领口近景")
            video = _compact_world(self.master, scene)
            self.assertEqual(frame["visual_saliency"]["exposure"], expected["exposure"])
            self.assertEqual(video["native_exposure"], expected["exposure"]["guidance"])
            self.assertEqual(video["background_separation"], expected["separation"]["background_guidance"])
            self.assertEqual(expected["separation"]["palette_class"], "LIGHT")
            self.assertNotIn("opening_focus", expected)
            self.assertNotIn("中等或较深", expected["separation"]["outfit_guidance"])
            self.assertEqual(video["frozen_outfit"], "白背心白裤")
            if scene["location"].startswith("B"):
                self.assertNotIn("窗口", video["native_exposure"])
                self.assertNotIn("A旧", str(frame))
        self.assertEqual(before, self.master)

    def test_unknown_colour_and_disabled_visibility_are_safe(self):
        self.master["product_truth"] = {}
        guide = segment_visibility(self.master, {"lighting": "现场顶灯"})
        self.assertEqual(guide["separation"]["palette_class"], "UNKNOWN")
        with patch.dict("os.environ", {"ORIGINAL_SCRIPT_VISUAL_SALIENCY_V1_ENABLED": "0"}):
            self.assertEqual(segment_visibility(self.master, {}), {})
            self.assertEqual(_compact_world(self.master, {})["native_exposure"], "")

    def test_direct_source_no_longer_has_empty_visibility(self):
        package = {"simplified_creative_seed": {"product_truth": self.master["product_truth"]},
                   "creative_diversity_contract": {"scene_motif": "候车区"}}
        with patch("core.longform.source_adapter._build_argument_bundle", return_value={"supporting_arguments": []}):
            source = source_from_product_plan({"product_code": "P1", "product_type": "外套"},
                                              package, duration_seconds=30)
        self.assertEqual(source["production_world"]["visual_saliency"]["separation"]["palette_class"], "LIGHT")
        self.assertNotIn("opening_focus", source["production_world"]["visual_saliency"])

    def test_shared_short_form_projection_retains_original_behaviour(self):
        for mode, subject in (("STATIC_PRODUCT", "PRODUCT"), ("HANDS_ONLY", "PRODUCT_AND_HANDS"),
                              ("PERSON_ON_CAMERA", "FACE_AND_PRODUCT")):
            kwargs = dict(presentation_mode=mode, scene_card={"lighting": "窗边自然光"},
                          product_truth=self.master["product_truth"])
            shared = build_subject_visibility_guidance(**kwargs)
            original = _visual_saliency_contract(**kwargs, canonical_type="outerwear",
                                                 outfit_contract={}, opening_visual_job={}, action_design={})
            self.assertEqual(original["exposure"], shared["exposure"])
            self.assertEqual(original["separation"], shared["separation"])
            self.assertEqual(original["exposure"]["subject_priority"], subject)
            self.assertIn("opening_focus", original)
            self.assertIn("中等或较深", original["separation"]["outfit_guidance"])
            self.assertFalse(original["policy"]["may_block_generation"])


if __name__ == "__main__":
    unittest.main()
