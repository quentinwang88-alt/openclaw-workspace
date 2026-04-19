import unittest

from app.config import Settings
from app.services.tag_engine import evaluate_creator_tags


class TagEngineTest(unittest.TestCase):
    def test_potential_new_priority(self):
        current = {
            "gmv": 500,
            "refund_rate": 0.01,
            "commission_rate": 0.05,
            "gmv_per_action": 250,
            "gmv_per_sample": 500,
            "avg_weekly_gmv_4w": 300,
            "avg_gmv_per_action_4w": 200,
            "avg_refund_rate_4w": 0.01,
            "gmv_lifetime": 500,
            "weeks_active_lifetime": 2,
            "weeks_with_gmv_lifetime": 1,
            "content_action_count": 2,
            "shipped_sample_count": 1,
            "gmv_4w": 500,
        }
        history = [
            {
                "stat_week": "2026-W12",
                "gmv": 100,
                "order_count": 1,
                "refund_rate": 0.0,
                "content_action_count": 1,
                "gmv_per_action": 100,
                "gmv_per_sample": 100,
                "shipped_sample_count": 1,
            },
            {
                "stat_week": "2026-W13",
                "gmv": 500,
                "order_count": 5,
                "refund_rate": 0.01,
                "content_action_count": 2,
                "gmv_per_action": 250,
                "gmv_per_sample": 500,
                "shipped_sample_count": 1,
            },
        ]
        thresholds = {
            "gmv_50p": 200,
            "gmv_75p": 400,
            "gmv_80p": 450,
            "gmv_4w_80p": 800,
            "gmv_lifetime_80p": 1200,
            "gmv_per_action_50p": 120,
            "gmv_per_sample_50p": 150,
            "refund_rate_75p": 0.05,
            "commission_rate_75p": 0.2,
        }
        result = evaluate_creator_tags(current, history, [], thresholds, Settings())
        self.assertEqual(result["primary_tag"], "potential_new")
        self.assertEqual(result["priority_level"], "medium_high")

    def test_potential_new_does_not_pass_when_efficiency_threshold_is_zero(self):
        current = {
            "gmv": 150,
            "refund_rate": 0.0,
            "commission_rate": 0.05,
            "gmv_per_action": 150,
            "gmv_per_sample": 0,
            "avg_weekly_gmv_4w": 150,
            "avg_gmv_per_action_4w": 150,
            "avg_refund_rate_4w": 0.0,
            "gmv_lifetime": 150,
            "weeks_active_lifetime": 2,
            "weeks_with_gmv_lifetime": 1,
            "content_action_count": 1,
            "shipped_sample_count": 0,
            "gmv_4w": 150,
        }
        history = [
            {
                "stat_week": "2026-W12",
                "gmv": 100,
                "order_count": 1,
                "refund_rate": 0.0,
                "content_action_count": 1,
                "gmv_per_action": 100,
                "gmv_per_sample": 0,
                "shipped_sample_count": 0,
            },
            {
                "stat_week": "2026-W13",
                "gmv": 150,
                "order_count": 2,
                "refund_rate": 0.0,
                "content_action_count": 1,
                "gmv_per_action": 150,
                "gmv_per_sample": 0,
                "shipped_sample_count": 0,
            },
        ]
        thresholds = {
            "gmv_50p": 200,
            "gmv_75p": 400,
            "gmv_80p": 450,
            "gmv_4w_80p": 800,
            "gmv_lifetime_80p": 1200,
            "gmv_per_action_50p": 0,
            "gmv_per_sample_50p": 0,
            "refund_rate_75p": 0.05,
            "commission_rate_75p": 0.2,
        }
        result = evaluate_creator_tags(current, history, [], thresholds, Settings())
        self.assertEqual(result["primary_tag"], "new_observe")


if __name__ == "__main__":
    unittest.main()
