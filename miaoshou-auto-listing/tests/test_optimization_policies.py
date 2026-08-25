import inspect
import unittest
from unittest.mock import AsyncMock, patch

from miaoshou_auto_listing.handlers.editor_dom import (
    atomic_row_input_values,
    clear_description_text,
    contains_thai,
    description_counter_value,
    description_character_count,
    description_text_cutoff,
    grams_to_kg_text,
    has_visible_description_text,
    normalize_description_length,
)
from miaoshou_auto_listing.handlers.image_translation import (
    _choose_ali_translation_provider,
    _is_ali_translation_provider,
    image_dialog_state,
    image_language_code,
    image_url_fingerprint,
)
from miaoshou_auto_listing.handlers.preflight import (
    clear_description_text_record_ids,
    description_validation_error,
    numeric_equal,
    should_clear_description_text,
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

    def test_description_counter_detects_over_limit_without_form_error(self) -> None:
        self.assertEqual(
            description_counter_value("字符： 11308/10000 图片：11/30"),
            11308,
        )
        self.assertIsNone(description_counter_value("产品描述"))

    def test_description_validation_fails_closed(self) -> None:
        code, message = description_validation_error(None)
        self.assertEqual(code.value, "DESCRIPTION_COUNT_UNAVAILABLE")
        self.assertIn("禁止发布", message)
        code, message = description_validation_error(11308)
        self.assertEqual(code.value, "DESCRIPTION_LIMIT_EXCEEDED")
        self.assertIn("11308/10000", message)
        self.assertEqual(description_validation_error(10000), (None, ""))

    def test_description_cutoff_prefers_a_semantic_boundary(self) -> None:
        text = "第一段完整内容。第二段不应该被切断在单词中间 trailing"
        cutoff = description_text_cutoff(text, 18)
        self.assertLessEqual(cutoff, 18)
        self.assertEqual(text[:cutoff], "第一段完整内容。")

    def test_explicit_description_clear_allowlist_is_exact(self) -> None:
        value = "recvsUdEXiHWTV, recOther\nrecThird"
        self.assertEqual(
            clear_description_text_record_ids(value),
            {"recvsUdEXiHWTV", "recOther", "recThird"},
        )
        self.assertTrue(should_clear_description_text("recvsUdEXiHWTV", value))
        self.assertFalse(should_clear_description_text("recvsUdEXiHW", value))
        self.assertFalse(should_clear_description_text("recUnrelated", value))

    def test_description_visible_text_ignores_editor_placeholders(self) -> None:
        self.assertFalse(has_visible_description_text(" \n\u200b\ufeff"))
        self.assertTrue(has_visible_description_text("产品参数"))

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

    def test_ali_translation_provider_matches_current_and_legacy_labels(self) -> None:
        self.assertTrue(_is_ali_translation_provider("阿里翻译（剩216张）"))
        self.assertTrue(_is_ali_translation_provider("阿里AI翻译（剩216张）"))
        self.assertFalse(
            _is_ali_translation_provider("简体中文→英语(阿里/不翻品牌)")
        )
        self.assertFalse(_is_ali_translation_provider("顶秀译图（剩0张）"))

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
    async def test_image_dialog_state_reports_existing_overlays(self) -> None:
        class Dialogs:
            def __init__(self, visible, selected=""):
                self.visible = visible
                self.selected = selected

            def filter(self, *, has):
                return Dialogs(self.visible, has)

            async def count(self):
                return int(self.selected in self.visible)

        class Page:
            def __init__(self):
                self.visible = {"图片翻译", "批量翻译/处理图片"}

            def locator(self, selector):
                return Dialogs(self.visible)

            def get_by_role(self, role, *, name, exact):
                return name

        self.assertEqual(
            await image_dialog_state(Page()),
            ["图片翻译", "批量翻译/处理图片"],
        )

    async def test_explicit_description_clear_preserves_images(self) -> None:
        class Images:
            async def count(self):
                return 11

        class Editable:
            def __init__(self):
                self.text = "需要清除的产品描述"
                self.script = ""

            async def text_content(self):
                return self.text

            def locator(self, selector):
                return Images()

            async def evaluate(self, script):
                self.script = script
                self.text = ""

        class Editables:
            def __init__(self, editable):
                self.first = editable

            async def count(self):
                return 1

        class Region:
            def __init__(self, editable):
                self.editable = editable

            def locator(self, selector):
                return Editables(self.editable)

        editable = Editable()
        with patch(
            "miaoshou_auto_listing.handlers.editor_dom.description_region",
            AsyncMock(return_value=Region(editable)),
        ):
            result = await clear_description_text(object())
        self.assertEqual(result["text_characters_after"], 0)
        self.assertEqual(result["images_before"], 11)
        self.assertEqual(result["images_after"], 11)
        self.assertIn("NodeFilter.SHOW_TEXT", editable.script)
        self.assertNotIn("remove()", editable.script)

    async def test_description_count_uses_label_ancestor_fallback(self) -> None:
        class Empty:
            @property
            def first(self):
                return self

            async def count(self):
                return 0

        class Editable(Empty):
            async def count(self):
                return 1

        class Ancestor(Empty):
            def __init__(self, matches):
                self.matches = matches

            async def inner_text(self):
                if self.matches:
                    return "产品描述 字符：11308/10000 图片：11/30"
                return "产品描述"

            def locator(self, selector):
                return Editable() if self.matches else Empty()

        class Anchor(Empty):
            def locator(self, selector):
                return Ancestor(selector.count("..") == 3)

        class Anchors(Empty):
            async def count(self):
                return 1

            def nth(self, index):
                return Anchor()

        class Editor:
            def get_by_text(self, text, exact=False):
                if text == "产品描述":
                    return Anchors()
                return Empty()

            def locator(self, selector):
                return Empty()

        with patch(
            "miaoshou_auto_listing.handlers.editor_dom.form_item",
            AsyncMock(return_value=None),
        ):
            self.assertEqual(await description_character_count(Editor()), 11308)

    async def test_description_count_uses_editor_counter_without_richtext_dom(self) -> None:
        class Editor:
            async def inner_text(self):
                return "产品描述 字符： 9988/10000 图片：11/30"

        self.assertEqual(await description_character_count(Editor()), 9988)

    async def test_description_normalizer_only_edits_text_nodes(self) -> None:
        class Editable:
            def __init__(self):
                self.script = ""
                self.cutoff = None

            async def text_content(self):
                return "保留的完整句子。需要删除的尾部内容"

            async def evaluate(self, script, cutoff):
                self.script = script
                self.cutoff = cutoff

        class Editables:
            def __init__(self, editable):
                self.first = editable

            async def count(self):
                return 1

        class Region:
            def __init__(self, editable):
                self.editable = editable

            def locator(self, selector):
                return Editables(self.editable)

        editable = Editable()
        with patch(
            "miaoshou_auto_listing.handlers.editor_dom.description_region",
            AsyncMock(return_value=Region(editable)),
        ):
            changed = await normalize_description_length(
                object(), current_count=10_005
            )
        self.assertTrue(changed)
        self.assertIn("NodeFilter.SHOW_TEXT", editable.script)
        self.assertNotIn("innerHTML", editable.script)
        self.assertGreater(editable.cutoff, 0)
        self.assertLess(editable.cutoff, len(await editable.text_content()))

    async def test_current_ali_provider_option_is_clicked(self) -> None:
        class Candidate:
            def __init__(self, text):
                self.text = text
                self.clicked = False

            async def inner_text(self):
                return self.text

            async def is_visible(self):
                return True

            async def click(self, *, force=False):
                self.clicked = force

        class Candidates:
            def __init__(self, items):
                self.items = items

            async def count(self):
                return len(self.items)

            def nth(self, index):
                return self.items[index]

        class Menu:
            def __init__(self, items):
                self.items = items

            def locator(self, selector):
                return Candidates(self.items)

        recent = Candidate("简体中文→英语(阿里/不翻品牌)")
        provider = Candidate("阿里翻译（剩216张）")
        self.assertTrue(
            await _choose_ali_translation_provider(Menu([recent, provider]))
        )
        self.assertFalse(recent.clicked)
        self.assertTrue(provider.clicked)

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
