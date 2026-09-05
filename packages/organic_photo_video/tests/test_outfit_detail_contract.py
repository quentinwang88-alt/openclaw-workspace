import sys
from pathlib import Path
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from services.content_planner import ContentPlannerService


class DetailContractTest(unittest.TestCase):
    def test_detail_role_overrides_face_focused_variant_grammar(self):
        with mock.patch.object(ContentPlannerService, "_grammar_shot", return_value={
            "camera_hint": "beauty selfie", "framing": "face_closeup", "action": "touch cheek"
        }):
            slot, composition = ContentPlannerService._apply_grammar_to_shot(
                {}, {"slot_index": 4, "purpose": "商品结构证明"}, content_goal="outfit_breakdown"
            )
        self.assertEqual(composition["framing"], "garment_neck_to_upper_thigh")
        self.assertIn("face_dominant", composition["forbidden"])
        self.assertEqual(slot["purpose"], "商品结构证明")
        self.assertNotIn("beauty selfie", slot["camera_hint"])

    def test_full_body_anchor_overrides_downward_variant_camera(self):
        with mock.patch.object(ContentPlannerService, "_grammar_shot", return_value={"camera_angle": "downward"}):
            _, composition = ContentPlannerService._apply_grammar_to_shot(
                {}, {"slot_index": 2}, content_goal="outfit_breakdown"
            )
        self.assertEqual(composition["camera_angle"], "waist_height_level")


if __name__ == "__main__":
    unittest.main()
