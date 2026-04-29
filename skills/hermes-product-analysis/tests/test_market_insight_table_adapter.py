import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.feishu import TableRecord  # noqa: E402
from src.market_insight_table_adapter import MarketInsightTableAdapter  # noqa: E402
from src.market_insight_models import MarketInsightConfig  # noqa: E402
from src.models import TableSourceConfig  # noqa: E402


class FakeClient(object):
    def __init__(self, records=None, field_names=None):
        self.records = list(records or [])
        self.field_names = list(field_names or [])

    def list_records(self, page_size=100, limit=None):
        items = list(self.records)
        if limit is not None:
            return items[:limit]
        return items

    def list_field_names(self):
        return list(self.field_names)

    def get_tmp_download_url(self, file_token):
        return "https://download.example.com/{token}.jpg".format(token=file_token)


def build_config():
    return MarketInsightConfig(
        table_id="vn_fastmoss_hair_product_ranking",
        table_name="越南 FastMoss 发饰榜单",
        enabled=True,
        source=TableSourceConfig(),
        input_mode="auto",
        default_country="VN",
        default_category="hair_accessory",
        batch_date="2026-04-21",
        max_samples=50,
        price_scale_divisor=1000.0,
        price_band_edges=[0, 50, 100, 200, 500, 1000],
        field_map={
            "product_name": "商品名称",
            "product_images": ["商品图片", "图片"],
            "shop_name": "店铺名称",
            "country": "国家/地区",
            "category": "商品分类",
            "price_text": "售价",
            "sales_7d": "7天销量",
            "gmv_7d": "7天销售额",
            "creator_count": "带货达人总数",
            "video_count": "带货视频总数",
            "product_url": ["TikTok商品落地页地址", "FastMoss商品详情页地址"],
            "listing_time": "预估商品上架时间",
        },
    )


class MarketInsightTableAdapterTest(unittest.TestCase):
    def test_detect_input_mode_prefers_product_ranking(self):
        adapter = MarketInsightTableAdapter()
        client = FakeClient(field_names=["商品名称", "商品图片", "7天销量"])

        mode = adapter.detect_input_mode(build_config(), client)

        self.assertEqual(mode, "product_ranking")

    def test_read_product_snapshots_normalizes_price_images_and_links(self):
        adapter = MarketInsightTableAdapter()
        client = FakeClient(
            records=[
                TableRecord(
                    record_id="rec_1",
                    fields={
                        "商品名称": "大抓齿低饱和抓夹",
                        "商品图片": [
                            {"file_token": "img_tok_1"},
                            "https://cdn.example.com/2.jpg",
                        ],
                        "店铺名称": "店铺A",
                        "国家/地区": "越南",
                        "商品分类": "时尚配件",
                        "售价": "16,999 ₫ - 36,999 ₫",
                        "7天销量": "128",
                        "7天销售额": "2,560,000 ₫",
                        "带货达人总数": "45",
                        "带货视频总数": "72",
                        "TikTok商品落地页地址": "https://shop.example.com/product?id=987654321",
                        "预估商品上架时间": "2026-04-10",
                    },
                )
            ]
        )

        snapshots = adapter.read_product_snapshots(build_config(), client)

        self.assertEqual(len(snapshots), 1)
        snapshot = snapshots[0]
        self.assertEqual(snapshot.country, "VN")
        self.assertEqual(snapshot.category, "hair_accessory")
        self.assertEqual(snapshot.product_id, "987654321")
        self.assertEqual(snapshot.price_min, 16999.0)
        self.assertEqual(snapshot.price_max, 36999.0)
        self.assertEqual(snapshot.price_mid, 26999.0)
        self.assertEqual(
            snapshot.product_images,
            ["feishu-file-token:img_tok_1", "https://cdn.example.com/2.jpg"],
        )
        self.assertEqual(snapshot.image_url, "https://cdn.example.com/2.jpg")
        self.assertEqual(snapshot.raw_product_images, [{"file_token": "img_tok_1"}, "https://cdn.example.com/2.jpg"])
        self.assertEqual(snapshot.sales_7d, 128.0)
        self.assertEqual(snapshot.gmv_7d, 2560000.0)
        self.assertGreaterEqual(snapshot.listing_days or 0, 0)


if __name__ == "__main__":
    unittest.main()
