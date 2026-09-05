"""Segment entry authority regressions; no database, model or Feishu calls."""
import copy
import unittest

from core.first_frame_contract import render_first_frame_prompt
from core.longform.keyframes import _base_frame_contract, build_keyframe_contracts


def _master():
    return {
        "product_identity_lock": {"must_preserve": ["白色外套", "按扣"]},
        "product_truth": {"canonical_product_type": "outerwear"},
        "frozen_reference_assets": {"assets": [
            {"role": "PERSONA_REFERENCE", "local_path": "/frozen/persona.jpg"},
        ]},
        "production_world": {
            "presentation_mode": "PERSON_ON_CAMERA",
            "carrier_mode": "PERSON_ON_CAMERA",
            "camera": "普通手机近全身中景",
            "scene_contract": {
                "location": "卧室行李箱旁", "background": "卧室床头",
                "execution_card": {"space": {"location": "卧室行李箱旁"}},
            },
            "opening_scene_projection": {
                "location_identity": "卧室行李箱旁",
                "opening_background_anchor": "卧室床头",
            },
            "visual_saliency": {
                "opening_focus": {"guidance": "人物完整服装与全身同时可见"},
                "exposure": {"guidance": "卧室床头窗光"},
                "separation": {"background_guidance": "卧室行李箱旁保留边界"},
            },
            "persona_contract": {
                "persona_id": "P1",
                "identity_lock": {"body_proportion_text": "自然成年人物比例"},
                "script_projection": {
                    "identity": "同一旅行分享者", "hair_makeup": "自然长直发",
                },
            },
            "outfit_contract": {"target_role": "TARGET_GARMENT"},
            "outfit_prompt_projection": {"frozen_outfit": "白色外套、黑色长裤"},
        },
    }


def _unit(camera, action="面对手机说话，同时一手托住衣领边缘"):
    return {
        "visual_content": "已穿戴的白色外套按扣清楚可见",
        "character_action": action,
        "camera": camera,
        "product_anchors_visible": ["按扣"],
    }


def _plan():
    return {
        "segments": [
            {"segment_id": "A", "scene_block": {
                "location": "卧室行李箱旁", "lighting": "白天自然光",
            }, "execution_units": [_unit("普通手机中景")]},
            {"segment_id": "B", "scene_block": {
                "location": "酒店大堂入口", "lighting": "大堂明亮室内光",
            }, "execution_units": [_unit("领口至胸前的按扣局部近景")]},
            {"segment_id": "C", "scene_block": {
                "location": "酒店大堂入口", "lighting": "大堂明亮室内光",
            }, "execution_units": [_unit("袖口商品局部近景")]},
        ],
        "bridge_contract": {"boundaries": [
            {"boundary_mode": "DISCONTINUOUS_CUT", "entry_frame_role": "SCENE_ENTRY"},
            {"boundary_mode": "DISCONTINUOUS_CUT", "entry_frame_role": "SETUP_ENTRY"},
        ]},
    }


class LongformFrameAuthorityTest(unittest.TestCase):
    def test_b_and_c_use_their_scene_without_inheriting_a(self):
        master, plan = _master(), _plan()
        before = copy.deepcopy((master, plan))
        package = build_keyframe_contracts(master, plan)
        self.assertIn("地点：卧室行李箱旁", package["K0"]["prompt"])
        for key in ("SB_ENTRY", "SC_ENTRY"):
            prompt = package[key]["prompt"]
            self.assertIn("地点：酒店大堂入口", prompt)
            self.assertIn("大堂明亮室内光", prompt)
            self.assertNotIn("卧室", prompt)
            self.assertNotIn("行李箱", prompt)
        self.assertIn("跨场景硬切", package["SB_ENTRY"]["prompt"])
        self.assertIn("同一场景中切到新手机机位", package["SC_ENTRY"]["prompt"])
        self.assertNotIn("跨场景硬切", package["SC_ENTRY"]["prompt"])
        self.assertEqual((master, plan), before)

    def test_longform_detail_framing_and_postdub_win_over_opening_defaults(self):
        package = build_keyframe_contracts(_master(), _plan())
        for key in ("K0", "SB_ENTRY", "SC_ENTRY"):
            prompt = package[key]["prompt"]
            self.assertNotIn("至少覆盖头部至膝部", prompt)
            self.assertNotIn("目标服装必须完整可辨", prompt)
            self.assertNotIn("准备开口", prompt)
            self.assertNotIn("面对手机说话", prompt)
            self.assertIn("后期画外旁白", prompt)
            self.assertIn("自然成年人物比例", prompt)
            self.assertIn("人物参考图只决定同一人物", prompt)
        self.assertIn("领口至胸前的按扣局部近景", package["SB_ENTRY"]["prompt"])
        self.assertIn("袖口商品局部近景", package["SC_ENTRY"]["prompt"])

    def test_legacy_opening_keeps_framing_and_speech_behavior(self):
        contract = _base_frame_contract(
            _master(), "人物穿好外套", "自然开口分享", ["按扣"],
            {"location": "酒店大堂入口"}, camera="商品近景",
        )
        # Shared OPENING still projects speech to the original still state.
        contract["opening_contract"]["character_action"] = "自然开口分享"
        prompt = render_first_frame_prompt(contract)
        self.assertIn("至少覆盖头部至膝部", prompt)
        self.assertIn("人物刚准备开口", prompt)
        self.assertIn("目标服装必须完整可辨", prompt)
        self.assertNotIn("后期画外旁白", prompt)

    def test_missing_scene_block_uses_world_scene_and_rebuilds_projection(self):
        master = _master()
        master["production_world"]["scene_contract"] = {"location": "机场候机区"}
        contract = _base_frame_contract(master, "商品正面可见", "自然站立", ["按扣"])
        self.assertEqual(contract["opening_scene_projection"]["location_identity"], "机场候机区")
        self.assertNotIn("卧室", str(contract["scene_contract"]))

    def test_bridge_uses_end_unit_framing_and_actual_segment_scene(self):
        plan = _plan()
        plan["bridge_contract"]["boundaries"][1] = {"boundary_mode": "CONTINUOUS"}
        plan["segments"][1]["execution_units"].append(
            _unit("侧面袖口局部近景", "手停在袖口边缘")
        )
        package = build_keyframe_contracts(_master(), plan)
        prompt = package["K2_PLANNED"]["prompt"]
        self.assertIn("地点：酒店大堂入口", prompt)
        self.assertIn("机位与景别：侧面袖口局部近景", prompt)
        self.assertNotIn("卧室", prompt)
        self.assertNotIn("至少覆盖头部至膝部", prompt)
        self.assertNotIn("准备开口", prompt)


if __name__ == "__main__":
    unittest.main()
