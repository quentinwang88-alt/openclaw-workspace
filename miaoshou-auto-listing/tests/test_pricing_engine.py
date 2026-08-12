from decimal import Decimal
import unittest

from miaoshou_auto_listing.services.pricing_engine import PricingEngine
from miaoshou_auto_listing.handlers.price import (
    multiplier_price_matches,
    multiplier_sale_price,
)


class PricingEngineTest(unittest.TestCase):
    def test_purchase_multiplier_uses_cent_rounding(self) -> None:
        self.assertEqual(
            multiplier_sale_price(Decimal("6.55"), Decimal("3.5")),
            Decimal("22.93"),
        )
        self.assertTrue(
            multiplier_price_matches(Decimal("22.91"), Decimal("22.93"))
        )
        self.assertFalse(
            multiplier_price_matches(Decimal("22.90"), Decimal("22.93"))
        )
    def test_x9_rounding_and_minimum(self) -> None:
        engine = PricingEngine(
            {
                "RULE": {
                    "purchase_cost_multiplier": 5.5,
                    "fixed_cost": 0,
                    "min_price": 99,
                    "rounding": "X9",
                }
            }
        )
        self.assertEqual(engine.calculate(Decimal("8"), "RULE"), Decimal("99"))
        self.assertEqual(engine.calculate(Decimal("20"), "RULE"), Decimal("119"))

    def test_cent_rounding(self) -> None:
        engine = PricingEngine(
            {"RULE": {"purchase_cost_multiplier": 0.75, "rounding": "CENT"}}
        )
        self.assertEqual(engine.calculate(Decimal("13.333"), "RULE"), Decimal("10.00"))
