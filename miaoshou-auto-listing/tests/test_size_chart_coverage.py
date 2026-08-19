import unittest

from miaoshou_auto_listing.services.size_chart_coverage import (
    SizeChartCoverageError,
    SizeChartCoverageValidator,
    extract_expected_size_tokens,
)


class FakeVision:
    def __init__(self, sizes):
        self.sizes = sizes

    def call_json(self, prompt, image_paths, max_output_tokens):
        return {"sizes": self.sizes}


class SizeChartCoverageTest(unittest.TestCase):
    def test_extracts_and_normalizes_sku_sizes(self):
        self.assertEqual(
            extract_expected_size_tokens(["粉色 / S", "粉色 / XXL", "灰色 / 3XL"]),
            {"S", "2XL", "3XL"},
        )

    def test_skips_products_without_size_tokens(self):
        result = SizeChartCoverageValidator(FakeVision([])).validate(
            "unused.jpg", ["粉色", "黑色"]
        )
        self.assertFalse(result.expected)

    def test_accepts_equivalent_visible_sizes(self):
        result = SizeChartCoverageValidator(FakeVision(["S", "XXL"])).validate(
            "unused.jpg", ["粉色 / S", "粉色 / 2XL"]
        )
        self.assertFalse(result.missing)

    def test_rejects_missing_sku_size(self):
        with self.assertRaisesRegex(SizeChartCoverageError, "2XL"):
            SizeChartCoverageValidator(FakeVision(["S"])).validate(
                "unused.jpg", ["粉色 / S", "粉色 / XXL"]
            )


if __name__ == "__main__":
    unittest.main()
