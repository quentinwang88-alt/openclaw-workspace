import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.direction_card_update import (  # noqa: E402
    build_direction_card_version,
    check_early_review_triggers,
    run_monthly_direction_card_review,
    split_direction_card_fields,
    update_weekly_dynamic_fields,
)


class DirectionCardUpdateTest(unittest.TestCase):
    def test_split_stable_and_dynamic_fields(self):
        card = {
            "direction_id": "MY__earrings__sweet_gift",
            "direction_name": "少女礼物感型",
            "core_user_scene": ["送礼"],
            "direction_action": "prioritize_low_cost_test",
            "direction_execution_brief": {"task_type": "low_cost_test"},
        }
        payload = split_direction_card_fields(card)
        self.assertIn("core_user_scene", payload["stable_fields"])
        self.assertEqual(payload["dynamic_fields"]["current_action"], "prioritize_low_cost_test")
        self.assertEqual(payload["dynamic_fields"]["direction_execution_brief"]["task_type"], "low_cost_test")

    def test_weekly_dynamic_update_and_version(self):
        cards = update_weekly_dynamic_fields(
            [
                {
                    "direction_id": "MY__earrings__sweet_gift",
                    "direction_execution_brief": {
                        "direction_action": "prioritize_low_cost_test",
                        "task_type": "low_cost_test",
                        "target_pool": "test_product_pool",
                        "sample_pool_requirements": ["近90天新品"],
                    },
                }
            ],
            crawl_batch_id="batch_1",
            batch_date="2026-04-26",
        )
        version = build_direction_card_version("MY", "earrings", "2026-04-26")

        self.assertEqual(cards[0]["current_action"], "prioritize_low_cost_test")
        self.assertEqual(cards[0]["weekly_sample_pool_requirements"], ["近90天新品"])
        self.assertEqual(version["dynamic_version"], "MY_earrings_2026W17")

    def test_monthly_review_and_early_triggers_are_tasks_not_mutations(self):
        review = run_monthly_direction_card_review(
            "MY",
            "earrings",
            [
                {
                    "crawl_batch_id": "b1",
                    "directions": [{"direction_id": "d1", "direction_action": "cautious_test"}],
                },
                {
                    "crawl_batch_id": "b2",
                    "directions": [{"direction_id": "d1", "direction_action": "prioritize_low_cost_test"}],
                },
            ],
        )
        triggers = check_early_review_triggers(
            [
                {"direction_id": "d1", "new_product_entry_signal": "medium"},
                {"direction_id": "d1", "new_product_entry_signal": "high"},
            ]
        )

        self.assertTrue(review["human_review_required"])
        self.assertEqual(review["stable_direction_updates"][0]["direction_id"], "d1")
        self.assertEqual(triggers[0]["trigger"], "new_product_entry_signal_2w_upgrade")


if __name__ == "__main__":
    unittest.main()
