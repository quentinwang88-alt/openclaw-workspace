import os
import unittest
from unittest import mock

from scripts.run_original_batch import (
    _counts_section,
    _render_complete_scripts_markdown,
)
from core.original_batch_executor import DIRECTION_LIMIT_ENV, _resolve_direction_limit


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


class CapacityReportTest(unittest.TestCase):
    """A content-capacity shortage must never look like a model failure."""

    def _report(self, **overrides):
        report = {
            "batch_id": "B2",
            "product_code": "P2",
            "status": "PARTIAL_PLANNED",
            "requested_count": 10,
            "planned_count": 4,
            "ready_count": 0,
            "failed_count": 0,
            "allocation_summary": {
                "allocation_status": "PARTIAL_CONTENT_CAPACITY",
                "requested_count": 10,
                "planned_count": 4,
                "shortage_count": 6,
                "structure_count": 2,
                "used_selling_argument_count": 2,
                "deferred_content": [
                    {"downgrade_reason": "SELLING_ARGUMENT_UNAVAILABLE"},
                    {"downgrade_reason": "SELLING_ARGUMENT_UNAVAILABLE"},
                    {"downgrade_reason": "WEARER_VISUAL_REQUIRED"},
                ],
            },
            "items": [],
        }
        report.update(overrides)
        return report

    def test_shortage_and_reasons_are_visible(self):
        markdown = _render_complete_scripts_markdown(self._report())
        self.assertIn("内容容量与规划状态", markdown)
        self.assertIn("PARTIAL_CONTENT_CAPACITY", markdown)
        self.assertIn("未补足：6 条", markdown)
        self.assertIn("不是模型失败", markdown)
        self.assertIn("SELLING_ARGUMENT_UNAVAILABLE × 2", markdown)
        self.assertIn("WEARER_VISUAL_REQUIRED × 1", markdown)

    def test_complete_batch_omits_the_shortage_sentence(self):
        report = self._report(
            status="PLANNED",
            planned_count=10,
            allocation_summary={
                "allocation_status": "COMPLETE",
                "requested_count": 10,
                "planned_count": 10,
                "shortage_count": 0,
                "structure_count": 4,
                "used_selling_argument_count": 6,
                "deferred_content": [],
            },
        )
        markdown = _render_complete_scripts_markdown(report)
        self.assertIn("COMPLETE", markdown)
        self.assertNotIn("未补足", markdown)

    def test_missing_allocation_summary_does_not_break_render(self):
        markdown = _render_complete_scripts_markdown(
            self._report(allocation_summary=None)
        )
        self.assertIn("内容容量与规划状态", markdown)
        self.assertIn("请求 / 已规划 / 已就绪 / 失败：10 / 4 / 0 / 0", markdown)


class DirectionLimitTest(unittest.TestCase):
    """The independent-direction cap keeps its legacy default of four."""

    def test_default_matches_the_previous_hard_coded_cap(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(DIRECTION_LIMIT_ENV, None)
            self.assertEqual(_resolve_direction_limit(1), 1)
            self.assertEqual(_resolve_direction_limit(4), 4)
            for requested in (5, 6, 10, 20):
                with self.subTest(requested=requested):
                    self.assertEqual(_resolve_direction_limit(requested), 4)

    def test_env_can_raise_the_cap(self):
        with mock.patch.dict(os.environ, {DIRECTION_LIMIT_ENV: "12"}, clear=False):
            self.assertEqual(_resolve_direction_limit(20), 12)
            self.assertEqual(_resolve_direction_limit(5), 5)
            self.assertEqual(_resolve_direction_limit(1), 1)

    def test_invalid_env_falls_back_to_default(self):
        for value in ("abc", "0", "-3", ""):
            with self.subTest(value=value):
                with mock.patch.dict(
                    os.environ, {DIRECTION_LIMIT_ENV: value}, clear=False
                ):
                    self.assertEqual(_resolve_direction_limit(20), 4)

    def test_bad_requested_count_is_safe(self):
        self.assertEqual(_resolve_direction_limit(None), 1)
        self.assertEqual(_resolve_direction_limit("x"), 1)
        self.assertEqual(_resolve_direction_limit(0), 1)


class MixedHistoryProtectionReportTest(unittest.TestCase):
    """B4: 报告不得声称一次并未落盘的跨批去重保护。

    The rendered signature is what the *next* batch compares 正文 against.  When
    the write failed, the item has no such protection, and a report that shows
    only "生成成功" would read as if the ledger had been updated.
    """

    def _report(self, persistence, *, not_persisted=None):
        failed = not persistence.get("history_persisted")
        return {
            "counts_available": True,
            "requested_count": 1,
            "planned_count": 1,
            "ready_count": 1,
            "items": [
                {
                    "item_index": 1,
                    "compatibility_slot": "S1",
                    "status": "SCRIPT_READY",
                    "structure": {},
                    "expression": {},
                    "content": {},
                    "script": {},
                    "mixed_delivery": {
                        "verdict": "DISTINCT_THEME",
                        "reason": "与最近参照在最终镜头上不同",
                    },
                    "mixed_history_persistence": persistence,
                }
            ],
            "history_not_persisted_count": (
                1 if failed else 0
            ) if not_persisted is None else not_persisted,
        }

    def test_a_failed_write_is_shown_per_item(self):
        markdown = _render_complete_scripts_markdown(
            self._report(
                {
                    "history_persisted": False,
                    "reason": "LEDGER_ERROR:RuntimeError",
                    "usage_id": "CPU_1",
                }
            )
        )
        self.assertIn("跨批保护", markdown)
        self.assertIn("未写入台账", markdown)
        self.assertIn("LEDGER_ERROR:RuntimeError", markdown)

    def test_a_successful_write_says_so(self):
        markdown = _render_complete_scripts_markdown(
            self._report(
                {"history_persisted": True, "reason": "", "usage_id": "CPU_1"}
            )
        )
        self.assertIn("已写入台账", markdown)
        self.assertNotIn("未写入台账", markdown)

    def test_the_counts_section_warns_when_a_write_failed(self):
        joined = "\n".join(
            _counts_section(
                self._report(
                    {"history_persisted": False, "reason": "ROW_NOT_FOUND"}
                )
            )
        )
        self.assertIn("**没有**跨批去重保护", joined)
        self.assertIn("成稿签名未写入台账的条目：1", joined)

    def test_the_counts_section_stays_silent_when_every_write_landed(self):
        joined = "\n".join(
            _counts_section(
                self._report({"history_persisted": True, "reason": ""})
            )
        )
        self.assertNotIn("未写入台账", joined)


if __name__ == "__main__":
    unittest.main()
