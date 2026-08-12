from pathlib import Path
import unittest

from miaoshou_auto_listing.browser.selectors import SelectorRegistry


SELECTOR_DIR = Path(__file__).resolve().parents[1] / "config" / "selectors"


class SelectorRegistryTest(unittest.TestCase):
    def test_registry_has_no_scattered_publish_selector(self) -> None:
        registry = SelectorRegistry(SELECTOR_DIR)
        self.assertIn("publish_button", registry.keys)
        self.assertEqual(
            registry.candidates("publish_button")[0],
            {"kind": "testid", "value": "save-and-publish"},
        )

    def test_registry_renders_business_text_parameter(self) -> None:
        registry = SelectorRegistry(SELECTOR_DIR)
        candidate = registry.candidates("warehouse_option", warehouse_name="WH-A")[0]
        self.assertEqual(candidate["name"], "WH-A")
