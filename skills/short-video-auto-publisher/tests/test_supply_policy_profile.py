"""自动供稿策略列 → photo_content_profile 构建（Phase 2，2026-09-16）。

红线：供给列全空时输出与旧版逐字节一致（不新增 photo_supply_policy 键）。
"""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

SKILL_DIR = Path(__file__).resolve().parents[1]
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from app.scheduler import build_photo_content_profile  # noqa: E402


class SupplyPolicyProfileTest(unittest.TestCase):
    def test_legacy_output_byte_identical(self):
        # 只有定位列时，输出与 2026-09-15 版本一致（无 photo_supply_policy 键）
        value = build_photo_content_profile(
            positioning="搭配灵感", default_theme="旅行穿搭")
        payload = json.loads(value)
        self.assertNotIn("photo_supply_policy", payload)
        self.assertEqual(payload["schema_version"], "opv-publish-account-profile-v1")
        self.assertEqual(payload["positioning"], "搭配灵感")

    def test_empty_everything_still_empty(self):
        self.assertEqual(build_photo_content_profile(), "")

    def test_supply_columns_written_verbatim(self):
        value = build_photo_content_profile(
            default_theme="旅行穿搭",
            supply_strategy="参考优先",
            supply_product_mode="使用指定商品",
            supply_product_codes="P1，P2",
            supply_automation="自动生产并发布",
            supply_daily_limit="3",
            supply_preset="图文｜TH｜四选一穿搭",
            supply_material_scope="旅游冬装, 显高搭配",
        )
        payload = json.loads(value)
        policy = payload["photo_supply_policy"]
        self.assertEqual(policy["content_strategy"], "参考优先")
        self.assertEqual(policy["product_mode"], "使用指定商品")
        self.assertEqual(policy["product_codes"], ["P1", "P2"])
        self.assertEqual(policy["automation"], "自动生产并发布")
        self.assertEqual(policy["daily_limit"], 3)
        self.assertEqual(policy["preset"], "图文｜TH｜四选一穿搭")
        self.assertEqual(policy["material_scope"], ["旅游冬装", "显高搭配"])

    def test_daily_limit_accepts_float_forms(self):
        value = build_photo_content_profile(
            supply_automation="自动生产", supply_daily_limit=3.0)
        self.assertEqual(json.loads(value)["photo_supply_policy"]["daily_limit"], 3)
        value = build_photo_content_profile(
            supply_automation="自动生产", supply_daily_limit="2.0")
        self.assertEqual(json.loads(value)["photo_supply_policy"]["daily_limit"], 2)

    def test_supply_off_without_other_config_writes_no_key(self):
        value = build_photo_content_profile(
            default_theme="旅行穿搭", supply_automation="关闭")
        self.assertNotIn("photo_supply_policy", json.loads(value))

    def test_supply_on_writes_key_even_with_defaults(self):
        value = build_photo_content_profile(supply_automation="自动生产")
        policy = json.loads(value)["photo_supply_policy"]
        self.assertEqual(policy["automation"], "自动生产")
        self.assertEqual(policy["content_strategy"], "定位优先")
        self.assertEqual(policy["product_mode"], "不指定商品")


if __name__ == "__main__":
    unittest.main()
