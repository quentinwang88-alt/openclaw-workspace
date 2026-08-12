import unittest
from unittest.mock import patch

from core.visual_execution_contract import (
    build_visual_execution_contract,
    build_visual_execution_diagnostics,
    build_opening_scene_projection,
)


def _scene(light="窗边白天自然光"):
    return {
        "execution_card": {
            "source_quality": "CURATED_MOTIF_FALLBACK",
            "lighting": light,
            "visual_scene_recipe": {
                "space_relationship": "咖啡厅靠窗座位",
                "material_palette": "暖木桌面与浅色墙面",
                "lighting_texture": light,
                "lived_in_detail": "桌边放着一只日常小包",
            },
            "coherence_key": "CURATED:CAFE_DINING",
        }
    }


class VisualExecutionContractTest(unittest.TestCase):
    def test_dark_scarf_gets_bright_native_and_lightness_separation(self):
        contract = build_visual_execution_contract(
            canonical_product_type="silk_scarf",
            presentation_mode="PERSON_ON_CAMERA",
            capture_mode="CREATOR_SELF_SHOT",
            outfit_contract={"demonstration_mode": "NECK_WORN"},
            scene_reference=_scene(),
            product_truth={
                "identity_anchors": ["深蓝纯色底与方形轮廓"],
                "visible_detail_anchors": ["蓝色边角"],
            },
            opening_visual_job={"job": "SHOW_RESULT"},
            action_design={"primary_action_mode": "RESULT_SHOW"},
        )

        self.assertEqual(
            contract["schema_version"], "visual-execution-contract-v4-wearable-saliency"
        )
        saliency = contract["visual_saliency"]
        self.assertEqual(saliency["exposure"]["profile"], "BRIGHT_NATIVE")
        self.assertIn("不欠曝、不蒙灰", saliency["exposure"]["guidance"])
        self.assertEqual(saliency["separation"]["palette_class"], "DARK")
        self.assertEqual(
            saliency["separation"]["contrast_axis"], "LIGHTNESS_PRIMARY"
        )
        self.assertEqual(
            saliency["opening_focus"]["framing"], "FACE_NECK_UPPER_BODY"
        )
        self.assertFalse(saliency["policy"]["may_block_generation"])
        self.assertFalse(saliency["policy"]["may_trigger_retry"])

    def test_light_headscarf_uses_head_focus_without_retying(self):
        contract = build_visual_execution_contract(
            canonical_product_type="headscarf",
            presentation_mode="PERSON_ON_CAMERA",
            capture_mode="CREATOR_SELF_SHOT",
            outfit_contract={"demonstration_mode": "HEAD_WORN"},
            scene_reference=_scene("普通室内光"),
            product_truth={
                "identity_anchors": ["浅绿纯色底与星星图案"],
                "visible_detail_anchors": ["浅绿边缘"],
            },
            opening_visual_job={"job": "SHOW_RESULT"},
            action_design={"primary_action_mode": "RESULT_SHOW"},
        )

        saliency = contract["visual_saliency"]
        self.assertEqual(
            saliency["separation"]["palette_class"],
            "PATTERNED_OR_MULTICOLOR",
        )
        self.assertEqual(
            saliency["opening_focus"]["focus_subject"], "HEAD_WORN_RESULT"
        )
        self.assertEqual(
            saliency["opening_focus"]["framing"],
            "HEAD_SHOULDER_TO_UPPER_BODY",
        )
        self.assertIn("不重新包裹或系结", saliency["opening_focus"]["natural_change"])

    def test_simple_wear_process_does_not_add_another_opening_action(self):
        contract = build_visual_execution_contract(
            canonical_product_type="winter_scarf",
            presentation_mode="PERSON_ON_CAMERA",
            capture_mode="CREATOR_SELF_SHOT",
            outfit_contract={"demonstration_mode": "NECK_WORN"},
            scene_reference=_scene(),
            product_truth={"identity_anchors": ["黑色围巾"]},
            action_design={"primary_action_mode": "SIMPLE_WEAR_PROCESS"},
        )

        self.assertIn(
            "服从本条已经冻结的简单佩戴动作",
            contract["visual_saliency"]["opening_focus"]["natural_change"],
        )

    def test_hands_only_keeps_existing_hand_action_authority(self):
        contract = build_visual_execution_contract(
            canonical_product_type="silk_scarf",
            presentation_mode="HANDS_ONLY",
            capture_mode="HANDS_PRODUCT_SHARE",
            outfit_contract={},
            scene_reference=_scene(),
            product_truth={"identity_anchors": ["深蓝色边角"]},
            action_design={"primary_action_mode": "HANDHELD_PRODUCT"},
        )
        saliency = contract["visual_saliency"]
        self.assertEqual(
            saliency["exposure"]["subject_priority"], "PRODUCT_AND_HANDS"
        )
        self.assertIn(
            "服从本条已经冻结的手部商品动作",
            saliency["opening_focus"]["natural_change"],
        )

    def test_unknown_color_stays_unknown_and_diagnostics_are_soft(self):
        contract = build_visual_execution_contract(
            canonical_product_type="scarf",
            presentation_mode="STATIC_PRODUCT",
            capture_mode="STATIC_PRODUCT_RECORD",
            outfit_contract={},
            scene_reference=_scene(""),
            product_truth={"identity_anchors": ["方形轮廓"]},
        )
        saliency = contract["visual_saliency"]
        self.assertEqual(saliency["separation"]["palette_class"], "UNKNOWN")
        self.assertEqual(
            saliency["opening_focus"]["focus_subject"], "PRODUCT_FIRST"
        )
        diagnostics = build_visual_execution_diagnostics(
            contract=contract,
            production_design={"life_event": {}},
        )
        self.assertEqual(diagnostics["mode"], "SOFT_ONLY")
        self.assertIn(
            "PRODUCT_PALETTE_EVIDENCE_UNAVAILABLE", diagnostics["warnings"]
        )

    def test_saliency_can_be_disabled_without_disabling_visual_finish(self):
        with patch.dict(
            "os.environ", {"ORIGINAL_SCRIPT_VISUAL_SALIENCY_V1_ENABLED": "0"}
        ):
            contract = build_visual_execution_contract(
                canonical_product_type="silk_scarf",
                presentation_mode="PERSON_ON_CAMERA",
                capture_mode="CREATOR_SELF_SHOT",
                outfit_contract={},
                scene_reference=_scene(),
                product_truth={"identity_anchors": ["深蓝色"]},
            )
        self.assertEqual(contract["visual_finish_profile"], "NATIVE_STYLED")
        self.assertNotIn("visual_saliency", contract)

    def test_brown_outerwear_gets_wearable_saliency_without_category_adapter(self):
        contract = build_visual_execution_contract(
            canonical_product_type="outerwear",
            presentation_mode="PERSON_ON_CAMERA",
            capture_mode="CREATOR_SELF_SHOT",
            outfit_contract={"demonstration_mode": "GARMENT_WORN"},
            scene_reference=_scene(),
            product_truth={"identity_anchors": ["棕色短款麂皮外套"]},
            opening_visual_job={"job": "SHOW_RESULT"},
        )
        self.assertEqual("WEARABLE_VISUAL_SALIENCY", contract["feature_scope"])
        saliency = contract["visual_saliency"]
        self.assertEqual("BROWN_WARM", saliency["separation"]["palette_class"])
        self.assertEqual(
            "TARGET_GARMENT_WORN_RESULT",
            saliency["opening_focus"]["focus_subject"],
        )
        self.assertFalse(saliency["policy"]["may_block_generation"])

    def test_dense_bookstore_is_compacted_only_for_opening(self):
        projection = build_opening_scene_projection(
            {
                "location": "书店暖色书架过道",
                "background": "两侧密集书架、陈列台、出口文字牌",
                "lived_in_trace": "桌上收据和待归还书籍",
            },
            presentation_mode="PERSON_ON_CAMERA",
        )
        self.assertEqual("HIGH", projection["source_density"])
        self.assertIn("只保留一个", projection["opening_background_anchor"])
        self.assertIn("侧边或远处", projection["dense_elements_placement"])
        self.assertEqual(
            "FIRST_FRAME_AND_FIRST_CAPTURE_UNIT_ONLY", projection["authority"]
        )
        self.assertTrue(projection["does_not_change_full_scene"])


if __name__ == "__main__":
    unittest.main()
