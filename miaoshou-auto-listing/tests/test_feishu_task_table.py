from decimal import Decimal
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

from miaoshou_auto_listing.config import load_config, validate_task_config
from miaoshou_auto_listing.models import (
    ErrorCode,
    ExecutionResult,
    PricingMode,
    Step,
)
from miaoshou_auto_listing.services.feishu_task_table import (
    FeishuTaskTable,
    FeishuTaskError,
    attachment_metadata,
    extract_1688_offer_id,
    normalize_1688_source_url,
    task_from_record,
)


CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"


class FeishuTaskMappingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.config = load_config(CONFIG_DIR)

    def record(self, **overrides):
        fields = {
            "采购链接": "https://detail.1688.com/offer/975683523984.html?offerId=975683523984",
            "目标店铺": "yours.beauty-2",
            "定价": 100,
            "每SKU库存": 500,
            "执行状态": "待执行",
        }
        fields.update(overrides)
        return {"record_id": "rec001", "fields": fields}

    def test_maps_alias_to_exact_shop_and_market(self) -> None:
        claimed = task_from_record(self.record(), self.config)
        self.assertEqual(claimed.task.target_shop, "YOURS_BEAUTY_2")
        self.assertEqual(claimed.task.market, "VN")
        self.assertEqual(claimed.task.miaoshou_product_id, "975683523984")
        self.assertEqual(claimed.task.fixed_sale_price, Decimal("100"))
        self.assertEqual(claimed.task.pricing_mode, PricingMode.FIXED)
        self.assertIsNone(claimed.task.purchase_price_multiplier)
        self.assertEqual(claimed.task.stock_per_sku, 500)
        self.assertEqual(claimed.task.category_group, "AUTO")
        validate_task_config(claimed.task, self.config)

    def test_explicit_fixed_price_keeps_the_legacy_path(self) -> None:
        claimed = task_from_record(
            self.record(**{"定价方式": "固定售价", "定价": 88}), self.config
        )
        self.assertEqual(claimed.task.pricing_mode, PricingMode.FIXED)
        self.assertEqual(claimed.task.fixed_sale_price, Decimal("88"))
        self.assertIsNone(claimed.task.purchase_price_multiplier)

    def test_purchase_multiplier_is_mutually_exclusive_with_fixed_price(self) -> None:
        claimed = task_from_record(
            self.record(**{"定价方式": "采购价倍数", "定价": 3.5}), self.config
        )
        self.assertEqual(
            claimed.task.pricing_mode, PricingMode.PURCHASE_MULTIPLIER
        )
        self.assertIsNone(claimed.task.fixed_sale_price)
        self.assertEqual(claimed.task.purchase_price_multiplier, Decimal("3.5"))

    def test_rejects_unknown_pricing_mode(self) -> None:
        with self.assertRaisesRegex(FeishuTaskError, "定价方式只能"):
            task_from_record(
                self.record(**{"定价方式": "随便定价"}), self.config
            )

    def test_rejects_excessive_purchase_multiplier(self) -> None:
        with self.assertRaisesRegex(FeishuTaskError, "不超过 100"):
            task_from_record(
                self.record(**{"定价方式": "采购价倍数", "定价": 101}),
                self.config,
            )

    def test_all_configured_aliases_have_valid_market_rules(self) -> None:
        aliases = [
            "LikeU shop",
            "yours.beauty-1",
            "yours.beauty-2",
            "lunara-1",
            "lunara-2",
            "TOWU123",
        ]
        for alias in aliases:
            with self.subTest(alias=alias):
                claimed = task_from_record(self.record(**{"目标店铺": alias}), self.config)
                validate_task_config(claimed.task, self.config)

    def test_rejects_unknown_alias(self) -> None:
        with self.assertRaisesRegex(FeishuTaskError, "未配置"):
            task_from_record(self.record(**{"目标店铺": "unknown-1"}), self.config)

    def test_rejects_fractional_stock(self) -> None:
        with self.assertRaisesRegex(FeishuTaskError, "非负整数"):
            task_from_record(self.record(**{"每SKU库存": 1.5}), self.config)

    def test_extracts_offer_id_from_query_fallback(self) -> None:
        self.assertEqual(
            extract_1688_offer_id("https://detail.1688.com/x?offerId=123456"),
            "123456",
        )

    @patch("miaoshou_auto_listing.services.feishu_task_table.requests.get")
    def test_normalizes_qr_short_link_with_trailing_product_code(self, request) -> None:
        response = Mock()
        response.url = "https://qr.1688.com/s/example"
        response.text = "wireless1688://ma.m.1688.com/offer?id=996541810259.html&amp;offerId=996541810259"
        response.raise_for_status.return_value = None
        request.return_value = response
        self.assertEqual(
            normalize_1688_source_url(
                "https://qr.1688.com/s/example CZ2470"
            ),
            "https://detail.1688.com/offer/996541810259.html",
        )

    def test_maps_optional_size_chart_attachment(self) -> None:
        record = self.record(
            **{
                "尺码图": [
                    {
                        "file_token": "file-token-1",
                        "name": "size.png",
                        "tmp_url": "https://example.invalid/size.png",
                    }
                ]
            }
        )
        claimed = task_from_record(record, self.config)
        self.assertEqual(
            claimed.task.size_chart_url, "https://example.invalid/size.png"
        )
        self.assertEqual(claimed.task.size_chart_file_token, "file-token-1")

    def test_attachment_metadata_accepts_a_direct_image_link(self) -> None:
        self.assertEqual(
            attachment_metadata("https://example.invalid/size.jpg"),
            {"url": "https://example.invalid/size.jpg"},
        )

    def test_attachment_download_can_fall_back_to_temporary_url(self) -> None:
        claimed = task_from_record(
            self.record(
                **{
                    "尺码图": [
                        {
                            "file_token": "file-token-1",
                            "name": "size.png",
                            "tmp_url": "https://example.invalid/size.png",
                        }
                    ]
                }
            ),
            self.config,
        )
        table = FeishuTaskTable(self.config)
        table._access_token = Mock(return_value="token")
        with patch(
            "miaoshou_auto_listing.services.feishu_task_table.requests.get",
            return_value=Mock(status_code=403),
        ):
            table._hydrate_size_chart_attachment(claimed)
        self.assertEqual(
            claimed.task.size_chart_url, "https://example.invalid/size.png"
        )
        self.assertEqual(claimed.task.size_chart_path, "")

    def test_exact_record_claim_uses_linear_running_state(self) -> None:
        source = self.record(**{"执行状态": ""})

        class RecordingTable(FeishuTaskTable):
            def _get_record(self, record_id):
                return source

            def _update_record(self, record_id, fields):
                self.updated = (record_id, fields)

        table = RecordingTable(self.config)
        claimed = table.claim_for_execute("rec001")
        self.assertEqual(claimed.record_id, "rec001")
        self.assertEqual(table.updated[1]["执行状态"], "执行中")
        self.assertEqual(table.updated[1]["执行结果"], "线性上架中")

    def test_duplicate_is_written_as_safe_success(self) -> None:
        class RecordingTable(FeishuTaskTable):
            def _update_record(self, record_id, fields):
                self.updated = (record_id, fields)

        claimed = task_from_record(self.record(), self.config)
        table = RecordingTable(self.config)
        table.complete(
            claimed,
            ExecutionResult(
                task_id=claimed.task.task_id,
                success=False,
                current_step=Step.CHECK_DUPLICATE,
                error_code=ErrorCode.ALREADY_PUBLISHED,
                error_message="duplicate",
            ),
        )
        self.assertEqual(table.updated[1]["执行状态"], "成功")
        self.assertIn("安全跳过", table.updated[1]["执行结果"])

    def test_non_linear_preparation_is_not_a_success_result(self) -> None:
        class RecordingTable(FeishuTaskTable):
            def _update_record(self, record_id, fields):
                self.updated = (record_id, fields)

        claimed = task_from_record(self.record(), self.config)
        table = RecordingTable(self.config)
        table.complete(
            claimed,
            ExecutionResult(
                task_id=claimed.task.task_id,
                success=True,
                current_step=Step.PREFLIGHT,
                published_status="WAITING_APPROVAL_NOT_PUBLISHED",
                approval_fingerprint="1234567890abcdef",
                preflight={"sku_count": 8},
            ),
        )
        self.assertEqual(table.updated[1]["执行状态"], "异常")
        self.assertEqual(table.updated[1]["TikTok产品ID"], "")

    def test_submitted_but_unverified_task_stays_publishing(self) -> None:
        class RecordingTable(FeishuTaskTable):
            def _update_record(self, record_id, fields):
                self.updated = (record_id, fields)

        claimed = task_from_record(self.record(), self.config)
        table = RecordingTable(self.config)
        table.complete(
            claimed,
            ExecutionResult(
                task_id=claimed.task.task_id,
                success=False,
                current_step=Step.VERIFY,
                published_status="SUBMITTED_PENDING_VERIFICATION",
                error_code=ErrorCode.PUBLISH_VALIDATION_FAILED,
                error_message="not visible yet",
            ),
        )
        self.assertEqual(table.updated[1]["执行状态"], "待核验")
        self.assertIn("禁止自动重发", table.updated[1]["执行结果"])
        self.assertEqual(table.updated[1]["TikTok产品ID"], "")
