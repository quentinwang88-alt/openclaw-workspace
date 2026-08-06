import unittest
from unittest.mock import patch

from core.original_batch_executor import load_product_context


class _LegacyFormalStorage:
    def query_runs_by_product_code(self, product_code, limit=200):
        return [
            {
                "run_id": 445,
                "record_id": "rec-legacy-formal",
                "input_hash": "legacy-input-hash",
            }
        ]

    def get_latest_stage_output_json(self, record_id, stage_name, product_code):
        if stage_name == "anchor_card":
            return {
                "product_type": "上装",
                "top_category": "女装",
                "target_country": "泰国",
                "target_language": "泰语",
            }
        if stage_name == "strategy_cards":
            return {"selling_point_catalog": []}
        return None


class _LegacyAccessoryStorage(_LegacyFormalStorage):
    def query_runs_by_product_code(self, product_code, limit=200):
        return [
            {
                "run_id": 585,
                "record_id": "rec-legacy-accessory",
                "input_hash": "legacy-accessory-input-hash",
                "product_type": "发夹",
                "top_category": "配饰",
                "target_country": "泰国",
                "target_language": "泰语",
            }
        ]

    def get_latest_stage_output_json(self, record_id, stage_name, product_code):
        if stage_name == "anchor_card":
            return {"product_positioning_one_liner": "深色光面抓夹"}
        if stage_name == "strategy_cards":
            return {"selling_point_catalog": []}
        return None


class ProductContextLoaderTest(unittest.TestCase):
    def test_simplified_path_reuses_formal_anchor_and_rebuilds_route(self):
        central = {"catalog": [{"selling_argument_id": "PCL_1"}]}
        with patch("core.storage.PipelineStorage", return_value=_LegacyFormalStorage()), patch(
            "core.product_selling_argument_adapter.load_verified_selling_point_catalog",
            return_value=central,
        ):
            context = load_product_context(
                "P1",
                top_category="女装",
                product_type="外套",
                allow_missing_structure_route=True,
            )

        self.assertEqual("外套", context["product_type"])
        self.assertEqual("女装", context["top_category"])
        self.assertEqual("REBUILD_REQUIRED", context["structure_route"]["status"])
        self.assertEqual(1, context["selling_point_catalog_sources"]["central_available_count"])

    def test_legacy_accessory_uses_run_row_type_before_apparel_default(self):
        central = {"catalog": []}
        with patch(
            "core.storage.PipelineStorage", return_value=_LegacyAccessoryStorage()
        ), patch(
            "core.product_selling_argument_adapter.load_verified_selling_point_catalog",
            return_value=central,
        ):
            context = load_product_context(
                "P_ACCESSORY", allow_missing_structure_route=True
            )

        self.assertEqual("发夹", context["product_type"])
        self.assertEqual("配饰", context["top_category"])

    def test_legacy_path_still_requires_structure_route(self):
        with patch("core.storage.PipelineStorage", return_value=_LegacyFormalStorage()):
            with self.assertRaisesRegex(RuntimeError, "有 anchor_card 但没有 structure_route"):
                load_product_context("P1")


if __name__ == "__main__":
    unittest.main()
