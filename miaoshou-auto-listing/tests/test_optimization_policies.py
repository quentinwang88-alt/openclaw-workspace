import inspect
import unittest

from miaoshou_auto_listing.handlers.editor_dom import (
    atomic_row_input_values,
    contains_thai,
    grams_to_kg_text,
)
from miaoshou_auto_listing.handlers.image_translation import (
    image_language_code,
    image_url_fingerprint,
)
from miaoshou_auto_listing.handlers.preflight import (
    numeric_equal,
    shorten_option_name,
    unique_short_option_names,
)
from miaoshou_auto_listing.handlers.image_translation import (
    detail_image_delete_indices,
)
from miaoshou_auto_listing.handlers.price import PriceHandler
from miaoshou_auto_listing.handlers.publish import (
    online_product_id,
    published_product_id,
    validate_publish_api_receipt,
)
from miaoshou_auto_listing.models import Step
from miaoshou_auto_listing.handlers.stock import StockHandler
from miaoshou_auto_listing.state import approval_fingerprint
from miaoshou_auto_listing.workflows.publish_product import DEFAULT_HANDLERS


class OptimizationPolicyTest(unittest.TestCase):
    def test_detail_image_limit_preserves_the_first_thirty(self) -> None:
        self.assertEqual(detail_image_delete_indices(29), [])
        self.assertEqual(detail_image_delete_indices(30), [])
        self.assertEqual(detail_image_delete_indices(31), [30])
        self.assertEqual(detail_image_delete_indices(39), list(range(30, 39)))

    def test_fixed_price_and_stock_use_native_bulk_controls(self) -> None:
        price_source = inspect.getsource(PriceHandler._fill_all_fixed_price)
        multiplier_source = inspect.getsource(PriceHandler._fill_purchase_multiplier)
        stock_source = inspect.getsource(StockHandler._configure_real_rows)
        self.assertIn('sku_header(editor, "本地展示价")', price_source)
        self.assertIn("_bulk_set_stock", stock_source)
        self.assertNotIn("gear.click", stock_source)
        self.assertNotIn("rows.nth(index)", stock_source)
        self.assertIn('choose_radio(dialog, "formula")', multiplier_source)
        self.assertIn('name="来源原价"', multiplier_source)
        self.assertIn('choose_radio(dialog, "twoPoint")', multiplier_source)

    def test_logistics_converts_grams_to_kg(self) -> None:
        self.assertEqual(grams_to_kg_text(200), "0.2")
        self.assertEqual(grams_to_kg_text(20), "0.02")

    def test_thai_title_detection(self) -> None:
        self.assertTrue(contains_thai("เสื้อแจ็คเก็ตสำหรับผู้หญิง"))
        self.assertFalse(contains_thai("美拉德短款外套"))

    def test_numeric_preflight_ignores_decimal_format_only(self) -> None:
        self.assertTrue(numeric_equal("100.00", "100"))
        self.assertFalse(numeric_equal("99.83", "100"))
        self.assertFalse(numeric_equal("", "100"))

    def test_long_translated_option_names_are_short_and_unique(self) -> None:
        values = [
            "Kẹp tóc hình móng vuốt kim cương lấp lánh [Họa tiết da báo]",
            "Kẹp tóc hình móng vuốt kim cương lấp lánh [Họa tiết da báo]",
        ]
        shortened = unique_short_option_names(values)
        self.assertTrue(all(len(value) <= 50 for value in shortened))
        self.assertEqual(len(set(shortened)), 2)
        self.assertIn("…", shorten_option_name(values[0]))

    def test_publish_record_requires_success_and_platform_id(self) -> None:
        text = "产品ID: 1736979963960461306\n975683523984\n发布成功\nLikeU shop"
        self.assertEqual(published_product_id(text), "1736979963960461306")
        self.assertEqual(published_product_id(text.replace("发布成功", "发布失败")), "")
        self.assertEqual(published_product_id("发布成功，但还没有产品ID"), "")

    def test_online_product_requires_an_explicit_platform_id(self) -> None:
        self.assertEqual(
            online_product_id("产品ID：1736979963960461306 货源ID：963538422732"),
            "1736979963960461306",
        )
        self.assertEqual(online_product_id("货源ID：963538422732"), "")

    def test_publish_api_receipts_are_hard_success_gates(self) -> None:
        receipt = validate_publish_api_receipt(
            "saveMoveCollectTask",
            "https://erp.91miaoshou.com/api/saveMoveCollectTask",
            200,
            {"code": 0},
        )
        self.assertIn("saveMoveCollectTask", receipt)
        with self.assertRaisesRegex(ValueError, "HTTP 500"):
            validate_publish_api_receipt(
                "saveMoveCollectTask",
                "https://erp.91miaoshou.com/api/saveMoveCollectTask",
                500,
                {},
            )
        with self.assertRaisesRegex(ValueError, "code=1001"):
            validate_publish_api_receipt(
                "saveMoveCollectTask",
                "https://erp.91miaoshou.com/api/saveMoveCollectTask",
                200,
                {"code": 1001, "msg": "rejected"},
            )

    def test_default_workflow_has_safety_gates(self) -> None:
        steps = [handler.step for handler in DEFAULT_HANDLERS]
        self.assertLess(steps.index(Step.CHECK_DUPLICATE), steps.index(Step.COLLECT))
        self.assertLess(
            steps.index(Step.TRANSLATE), steps.index(Step.TRANSLATE_SIZE_CHART)
        )
        self.assertLess(
            steps.index(Step.TRANSLATE_SIZE_CHART),
            steps.index(Step.TRANSLATE_DETAIL_IMAGES),
        )
        self.assertLess(
            steps.index(Step.TRANSLATE_MAIN_IMAGES), steps.index(Step.PREFLIGHT)
        )
        self.assertLess(steps.index(Step.PREFLIGHT), steps.index(Step.PUBLISH))
        self.assertEqual(steps[-1], Step.VERIFY)

    def test_image_translation_language_mapping(self) -> None:
        self.assertEqual(image_language_code("TH"), "th")
        self.assertEqual(image_language_code("VN"), "vi")
        self.assertEqual(image_language_code("MY"), "ms")
        with self.assertRaisesRegex(ValueError, "no language mapping"):
            image_language_code("XX")

    def test_image_fingerprint_is_ordered_and_ignores_fragments(self) -> None:
        first = image_url_fingerprint(
            ["https://img.example/a.jpg#x", "https://img.example/b.jpg"]
        )
        self.assertEqual(
            first,
            image_url_fingerprint(
                ["https://img.example/a.jpg#y", "https://img.example/b.jpg"]
            ),
        )
        self.assertNotEqual(
            first,
            image_url_fingerprint(
                ["https://img.example/b.jpg", "https://img.example/a.jpg"]
            ),
        )

    def test_approval_fingerprint_ignores_live_platform_conversion(self) -> None:
        base = {
            "shop": "LikeU shop",
            "market": "TH",
            "title": "สินค้าไทย",
            "skus": [
                {
                    "label": "ท็อป T1 S CNY CNY THB： 245.87 0 / 50 KG 不设置",
                    "cny_price": "50",
                    "platform_price": "THB： 245.87",
                    "stock": "100",
                    "weight_kg": "0.2",
                }
            ],
            "package": {},
            "size_chart_count": 1,
            "image_translation": {},
        }
        changed = {**base, "skus": [dict(base["skus"][0])]}
        changed["skus"][0]["label"] = (
            "ท็อป T1 S CNY CNY THB： 245.35 0 / 50 KG 不设置"
        )
        changed["skus"][0]["platform_price"] = "THB： 245.35"
        self.assertEqual(
            approval_fingerprint(base), approval_fingerprint(changed)
        )


class VirtualSkuSnapshotTest(unittest.IsolatedAsyncioTestCase):
    async def test_atomic_input_snapshot_uses_one_browser_evaluation(self) -> None:
        class Fields:
            calls = 0

            async def evaluate_all(self, expression):
                self.calls += 1
                return ["100", "100", "100"]

        class Rows:
            fields = Fields()

            def locator(self, selector):
                self.selector = selector
                return self.fields

        rows = Rows()
        values = await atomic_row_input_values(
            rows, "input.jx-input__inner[readonly]"
        )
        self.assertEqual(values, ["100", "100", "100"])
        self.assertEqual(rows.fields.calls, 1)
        self.assertEqual(rows.selector, "input.jx-input__inner[readonly]")
