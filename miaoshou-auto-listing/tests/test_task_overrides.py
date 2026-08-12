from decimal import Decimal
import unittest

from miaoshou_auto_listing.models import PricingMode, ProductTask


class TaskOverrideTest(unittest.TestCase):
    def test_fixed_price_and_stock_are_validated(self) -> None:
        task = ProductTask(
            task_id="T1",
            miaoshou_product_id="975683523984",
            source_url="https://detail.1688.com/offer/975683523984.html",
            target_shop="LIKEU_SHOP",
            market="th",
            category_group="clothing",
            pricing_rule_id="TH_ACCESSORY_V1",
            fixed_sale_price="100",
            stock_per_sku=500,
        )
        self.assertEqual(task.fixed_sale_price, Decimal("100"))
        self.assertEqual(task.pricing_mode, PricingMode.FIXED)
        self.assertEqual(task.stock_per_sku, 500)
        self.assertEqual(task.market, "TH")
        self.assertEqual(task.category_group, "CLOTHING")
        self.assertFalse(task.allow_republish)

    def test_multiplier_pricing_is_validated(self) -> None:
        task = ProductTask(
            task_id="T2",
            miaoshou_product_id="975683523984",
            target_shop="LIKEU_SHOP",
            market="TH",
            category_group="ACCESSORY",
            pricing_rule_id="TH_ACCESSORY_V1",
            pricing_mode="PURCHASE_MULTIPLIER",
            purchase_price_multiplier="3.5",
        )
        self.assertEqual(task.pricing_mode, PricingMode.PURCHASE_MULTIPLIER)
        self.assertEqual(task.purchase_price_multiplier, Decimal("3.5"))
        self.assertIsNone(task.fixed_sale_price)

    def test_multiplier_cannot_also_have_fixed_price(self) -> None:
        with self.assertRaisesRegex(ValueError, "cannot include fixed"):
            ProductTask(
                task_id="T3",
                miaoshou_product_id="975683523984",
                target_shop="LIKEU_SHOP",
                market="TH",
                category_group="ACCESSORY",
                pricing_rule_id="TH_ACCESSORY_V1",
                pricing_mode="PURCHASE_MULTIPLIER",
                purchase_price_multiplier="3.5",
                fixed_sale_price="100",
            )
