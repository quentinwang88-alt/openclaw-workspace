import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.market_insight_analyzer import MarketInsightAnalyzer  # noqa: E402
from src.market_insight_models import ProductRankingSnapshot  # noqa: E402
from src.models import HermesOutputError  # noqa: E402


class FakeBase(object):
    def __init__(self):
        self.last_product_images = None

    def _run_prompt(self, prompt_name, payload, product_images):
        self.last_product_images = list(product_images)
        raise HermesOutputError("图片字段存在，但未能准备 Hermes 可读图片")


class FakeSuccessBase(object):
    def __init__(self):
        self.last_product_images = None

    def _run_prompt(self, prompt_name, payload, product_images):
        self.last_product_images = list(product_images)
        return {
            "is_valid_sample": True,
            "style_cluster": "基础通勤型",
            "style_tags_secondary": [],
            "product_form": "抓夹",
            "length_form": "other",
            "element_tags": ["大抓齿"],
            "value_points": ["快速整理头发"],
            "scene_tags": ["出门"],
            "reason_short": "图片和标题都指向基础通勤抓夹",
        }


class MarketInsightAnalyzerTest(unittest.TestCase):
    def test_image_preparation_error_degrades_to_invalid_sample(self):
        analyzer = MarketInsightAnalyzer(skill_dir=ROOT)
        analyzer.base = FakeBase()
        snapshot = ProductRankingSnapshot(
            batch_date="2026-04-21",
            batch_id="batch_1",
            country="VN",
            category="hair_accessory",
            product_id="p1",
            product_name="低饱和抓夹",
            shop_name="店铺A",
            price_min=10.0,
            price_max=20.0,
            price_mid=15.0,
            sales_7d=10.0,
            gmv_7d=100.0,
            creator_count=2.0,
            video_count=3.0,
            listing_days=5,
            product_images=["https://cdn.example.com/1.jpg"],
        )

        result = analyzer.tag_product(
            snapshot,
            taxonomy={
                "style_cluster": ["基础通勤型", "other"],
                "product_form": ["抓夹", "other"],
                "length_form": ["常规", "other"],
                "element_tags": ["大抓齿", "other"],
                "value_points": ["快速整理头发", "other"],
                "scene_tags": ["出门", "other"],
            },
        )

        self.assertFalse(result.is_valid_sample)
        self.assertEqual(result.style_cluster, "other")
        self.assertEqual(result.reason_short, "图片不可读，按无效样本处理")
        self.assertEqual(result.length_form, "other")

    def test_tag_product_resolves_feishu_images_lazily(self):
        analyzer = MarketInsightAnalyzer(skill_dir=ROOT)
        analyzer.base = FakeSuccessBase()
        analyzer.shared._normalize_images = lambda value, client=None: ["https://download.example.com/img_tok_1.jpg"]
        analyzer._get_lazy_image_client = lambda snapshot: object()
        snapshot = ProductRankingSnapshot(
            batch_date="2026-04-21",
            batch_id="batch_1",
            country="VN",
            category="hair_accessory",
            product_id="p1",
            product_name="低饱和抓夹",
            shop_name="店铺A",
            price_min=10.0,
            price_max=20.0,
            price_mid=15.0,
            sales_7d=10.0,
            gmv_7d=100.0,
            creator_count=2.0,
            video_count=3.0,
            listing_days=5,
            product_images=["https://open.feishu.cn/open-apis/drive/v1/medias/batch_get_tmp_download_url?file_tokens=img_tok_1"],
            raw_product_images=[{"file_token": "img_tok_1"}],
            source_feishu_url="https://example.feishu.cn/base/app?table=tbl",
        )

        result = analyzer.tag_product(
            snapshot,
            taxonomy={
                "style_cluster": ["基础通勤型", "other"],
                "product_form": ["抓夹", "other"],
                "length_form": ["other"],
                "element_tags": ["大抓齿", "other"],
                "value_points": ["快速整理头发", "other"],
                "scene_tags": ["出门", "other"],
            },
        )

        self.assertTrue(result.is_valid_sample)
        self.assertEqual(analyzer.base.last_product_images, ["https://download.example.com/img_tok_1.jpg"])


if __name__ == "__main__":
    unittest.main()
