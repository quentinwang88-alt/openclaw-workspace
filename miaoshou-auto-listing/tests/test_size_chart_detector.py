import unittest

from miaoshou_auto_listing.services.size_chart_detector import (
    SizeChartDetectionError,
    SizeChartDetector,
)
from miaoshou_auto_listing.services.source_size_chart import (
    is_source_size_spec_text,
)


class FakeClient:
    def __init__(self, result):
        self.result = result

    def call_json(self, prompt, image_paths, max_output_tokens):
        self.prompt = prompt
        self.image_paths = image_paths
        return self.result


def row(index, is_size_chart=False, confidence=0.99):
    return {
        "index": index,
        "is_size_chart": is_size_chart,
        "confidence": confidence,
        "evidence": "grid" if is_size_chart else "product photo",
    }


class SizeChartDetectorTest(unittest.TestCase):
    def test_selects_exactly_one_high_confidence_candidate(self):
        client = FakeClient(
            {"results": [row(0, True), row(1), row(2)]}
        )
        selected = SizeChartDetector(client=client).detect(["0.jpg", "1.jpg", "2.jpg"])
        self.assertEqual(selected.index, 0)
        self.assertEqual(selected.confidence, 0.99)
        self.assertIn("不要抄录", client.prompt)

    def test_rejects_no_candidate(self):
        client = FakeClient({"results": [row(0), row(1)]})
        with self.assertRaisesRegex(SizeChartDetectionError, "found 0"):
            SizeChartDetector(client=client).detect(["0.jpg", "1.jpg"])

    def test_rejects_multiple_candidates(self):
        client = FakeClient({"results": [row(0, True), row(1, True)]})
        with self.assertRaisesRegex(SizeChartDetectionError, "found 2"):
            SizeChartDetector(client=client).detect(["0.jpg", "1.jpg"])

    def test_rejects_incomplete_result(self):
        client = FakeClient({"results": [row(0, True)]})
        with self.assertRaisesRegex(SizeChartDetectionError, "cover every"):
            SizeChartDetector(client=client).detect(["0.jpg", "1.jpg"])

    def test_accepts_1688_packaging_table_with_sku_sizes(self):
        text = """
        包装信息 商品件重尺
        颜色 尺码 长(cm) 宽(cm) 高(cm) 体积(cm³) 重量(g)
        灰色【薄款】 S 38 28 2 2128 240
        灰色【薄款】 M 38 28 2 2128 240
        """
        self.assertTrue(is_source_size_spec_text(text))

    def test_accepts_one_size_recommendation_in_packaging_table(self):
        text = """
        包装信息 商品件重尺
        颜色 尺码 重量(g)
        杏色 均码（建议85-125斤） 300
        灰色 均码（建议85-125斤） 300
        """
        self.assertTrue(is_source_size_spec_text(text))

    def test_rejects_packaging_data_without_size_mapping(self):
        text = "包装信息 商品重量 300g 包裹长 38cm 宽 28cm 高 2cm"
        self.assertFalse(is_source_size_spec_text(text))
