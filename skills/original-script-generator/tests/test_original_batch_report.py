import unittest

from scripts.run_original_batch import _render_complete_scripts_markdown


class CompleteScriptReportTest(unittest.TestCase):
    def test_complete_report_keeps_full_production_design(self):
        report = {
        "batch_id": "B1",
        "product_code": "P1",
        "status": "SCRIPT_READY",
        "planned_count": 1,
        "ready_count": 1,
        "items": [{
            "item_index": 1,
            "compatibility_slot": "S1",
            "status": "SCRIPT_READY",
            "structure": {"macro_family_key": "HOOK>PROOF", "carrier_mode": "WEARER_ACTIVE"},
            "expression": {"requested_hook_id": "GENERAL_PRODUCT_SHARE", "actual_hook_id": "GENERAL_PRODUCT_SHARE"},
            "content": {"content_angle_key": "ARG_1"},
            "script": {
                "script_concept": {
                    "one_sentence_idea": "出门前分享",
                    "viewer_need": "空调房外搭",
                    "hook_intent": "直接进入生活时刻",
                    "macro_structure": ["HOOK", "PROOF"],
                },
                "production_design": {
                    "presentation_mode": "PERSON_ON_CAMERA",
                    "character": {
                        "identity": "曼谷通勤女性",
                        "appearance": "暖调肤色，气质利落",
                        "hair_makeup": "齐肩黑发，自然妆",
                        "speaking_personality": "朋友式分享",
                    },
                    "outfit": {
                        "base_outfit": "米白内搭和深色西裤",
                        "product_role": "短款外搭",
                        "accessories": "通勤包",
                    },
                    "scene": {
                        "location": "公寓客厅",
                        "moment": "工作日早晨",
                        "lighting": "窗侧自然光",
                        "background": "浅灰墙面",
                    },
                    "emotion": {
                        "starting_state": "专注",
                        "natural_change": "逐渐放松",
                        "ending_state": "轻微笑意",
                    },
                },
                "product_usage": {
                    "identity_anchors_preserved": ["近黑色短款圆领外套"],
                    "selling_points_used": ["CLM_TEST_1"],
                },
                "continuous_voiceover": {
                    "target_text": "ข้อความภาษาไทย",
                    "chinese_translation": "中文口播",
                    "selling_argument_realization": "卖点短语",
                },
                "storyboard": [{
                    "shot_no": 1,
                    "time_range": "0-3s",
                    "narrative_role": "HOOK",
                    "visual_content": "人物拿起通勤包",
                    "character_action": "自然起身",
                    "natural_emotion": "平静",
                    "camera": "固定中景",
                    "product_anchors_visible": ["商品正面"],
                }],
            },
        }],
    }

        markdown = _render_complete_scripts_markdown(report)
        for expected in (
            "暖调肤色，气质利落",
            "齐肩黑发，自然妆",
            "米白内搭和深色西裤",
            "逐渐放松",
            "人物拿起通勤包",
            "商品正面",
            "近黑色短款圆领外套",
            "CLM_TEST_1",
            "卖点短语",
        ):
            with self.subTest(expected=expected):
                self.assertIn(expected, markdown)


if __name__ == "__main__":
    unittest.main()
