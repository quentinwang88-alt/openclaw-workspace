import unittest

from app.services.feishu_mapper import SYSTEM_OWNED_FIELDS, build_feishu_record_fields


class FeishuMapperTest(unittest.TestCase):
    def test_build_feishu_record_fields(self):
        payload = build_feishu_record_fields(
            {
                "record_key": "tiktok:th:泰国服装1店:alice",
                "stat_week": "2026-W13",
                "creator_name": "Alice",
                "country": "th",
                "store": "泰国服装1店",
                "gmv": 1000,
                "prev_gmv": 500,
                "gmv_wow": 1.0,
                "content_action_count": 3,
                "prev_content_action_count": 2,
                "action_count_wow": 0.5,
                "refund_rate": 0.02,
                "refund_rate_wow": 0.25,
                "gmv_per_action": 333.3,
                "gmv_per_action_wow": 0.2,
                "gmv_4w": 2000,
                "primary_tag": "core_maintain",
                "risk_tags": "high_refund_risk",
                "priority_level": "high",
                "decision_reason": "stable",
                "next_action": "keep",
            }
        )
        self.assertEqual(payload["当前主标签"], "core_maintain")
        self.assertEqual(payload["达人名称"], "Alice")
        self.assertEqual(payload["店铺"], "泰国服装1店")
        self.assertEqual(payload["当前统计周"], "2026-W13")
        self.assertEqual(payload["上周GMV"], 500.0)
        self.assertEqual(payload["GMV环比"], 100.0)
        self.assertEqual(payload["本周退款率"], 2.0)
        self.assertEqual(payload["退款率变化"], 25.0)
        self.assertTrue("负责人" not in payload)
        self.assertIn("最近更新时间", payload)
        self.assertEqual(len(SYSTEM_OWNED_FIELDS), 22)


if __name__ == "__main__":
    unittest.main()
