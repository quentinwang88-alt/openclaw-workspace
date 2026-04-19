import unittest
from decimal import Decimal

from app.services.metrics_calculator import calc_wow, compute_basic_metrics


class MetricsTest(unittest.TestCase):
    def test_compute_basic_metrics(self):
        metrics = compute_basic_metrics(
            {
                "gmv": 1000,
                "refund_amount": 50,
                "estimated_commission": 100,
                "order_count": 20,
                "sold_item_count": 30,
                "content_action_count": 5,
                "shipped_sample_count": 2,
            }
        )
        self.assertEqual(metrics["refund_rate"], Decimal("0.05"))
        self.assertEqual(metrics["commission_rate"], Decimal("0.1"))
        self.assertEqual(metrics["gmv_per_action"], Decimal("200"))
        self.assertEqual(metrics["gmv_per_sample"], Decimal("500"))
        self.assertEqual(metrics["items_per_order"], Decimal("1.5"))

    def test_calc_wow(self):
        self.assertEqual(calc_wow(Decimal("120"), Decimal("100")), Decimal("0.2"))
        self.assertIsNone(calc_wow(Decimal("120"), Decimal("0")))


if __name__ == "__main__":
    unittest.main()

