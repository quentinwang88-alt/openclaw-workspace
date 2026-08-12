from pathlib import Path
import unittest

from miaoshou_auto_listing.config import load_config, validate_task_config
from miaoshou_auto_listing.models import ProductTask


CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"


class ConfigTest(unittest.TestCase):
    def test_loads_all_phase_one_rules(self) -> None:
        config = load_config(CONFIG_DIR)
        self.assertEqual(config.logistics_profiles["ACCESSORY"]["weight_g"], 20)
        self.assertEqual(config.logistics_profiles["CLOTHING"]["height_cm"], 5)
        self.assertEqual(
            config.warehouse["TH"]["name"],
            "The Chinese mainland Pickup Warehouse",
        )
        self.assertEqual(config.shops["LIKEU_SHOP"]["shop_id"], "5159032")
        self.assertEqual(
            config.browser.publish_history_path,
            "/tiktok/move_collect/history?status=success",
        )
        self.assertEqual(config.browser.publish_verify_timeout_ms, 600_000)

    def test_rejects_shop_market_mismatch(self) -> None:
        config = load_config(CONFIG_DIR)
        task = ProductTask(
            task_id="T1",
            miaoshou_product_id="M1",
            target_shop="TH_WOMEN_01",
            market="VN",
            category_group="ACCESSORY",
            pricing_rule_id="VN_ACCESSORY_V1",
        )
        with self.assertRaisesRegex(ValueError, "belongs to TH"):
            validate_task_config(task, config)
