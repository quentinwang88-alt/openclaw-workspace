import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.feishu import TableRecord  # noqa: E402
from src.models import ReadFilterConfig, TableConfig, TableSourceConfig  # noqa: E402
from src.table_adapter import TableAdapter  # noqa: E402


def build_table_config():
    return TableConfig(
        table_id="manual_table_tops_01",
        table_name="轻上装人工选品表",
        enabled=True,
        source_type="manual",
        source=TableSourceConfig(),
        supported_manual_categories=["发饰", "轻上装"],
        batch_field="分析批次",
        read_filter=ReadFilterConfig(status_field="分析状态", pending_values=["待处理", ""]),
        field_map={
            "product_title": "产品标题",
            "product_images": "产品图片",
            "cost_price": "采购价",
            "target_price": "拟售价",
            "manual_category": "人工类目",
            "product_notes": "产品备注",
            "competitor_notes": "竞品备注",
            "competitor_links": "竞品链接",
            "target_market": "目标国家",
        },
        writeback_map={"analysis_status": "分析状态"},
    )


class FakeClient(object):
    def __init__(self, records=None, download_urls=None):
        self.records = records or []
        self.download_urls = download_urls or {}

    def list_records(self, page_size=100, limit=None):
        items = list(self.records)
        if limit is not None:
            return items[:limit]
        return items

    def get_tmp_download_url(self, file_token):
        return self.download_urls[file_token]


class TableAdapterTest(unittest.TestCase):
    def test_read_pending_records_filters_by_status(self):
        adapter = TableAdapter()
        config = build_table_config()
        client = FakeClient(
            records=[
                TableRecord(record_id="rec_1", fields={"分析状态": "待处理"}),
                TableRecord(record_id="rec_2", fields={"分析状态": ""}),
                TableRecord(record_id="rec_3", fields={"分析状态": "已完成分析"}),
                TableRecord(record_id="rec_4", fields={}),
            ]
        )

        records = adapter.read_pending_records(config, client)
        self.assertEqual([record.record_id for record in records], ["rec_1", "rec_2", "rec_4"])

    def test_read_completed_records_filters_by_status(self):
        adapter = TableAdapter()
        config = build_table_config()
        client = FakeClient(
            records=[
                TableRecord(record_id="rec_1", fields={"分析状态": "待处理"}),
                TableRecord(record_id="rec_2", fields={"分析状态": "已完成分析"}),
                TableRecord(record_id="rec_3", fields={"分析状态": "分析失败"}),
            ]
        )

        records = adapter.read_pending_records(config, client, record_scope="completed")
        self.assertEqual([record.record_id for record in records], ["rec_2"])

    def test_read_completed_missing_v2_records_filters_by_missing_core_writeback(self):
        adapter = TableAdapter()
        config = build_table_config()
        config.writeback_map["batch_priority_score"] = "批内优先级分"
        config.writeback_map["market_match_score"] = "市场匹配分"
        config.writeback_map["suggested_action"] = "建议动作"
        config.writeback_map["feature_scores_json"] = "特征打点JSON"
        client = FakeClient(
            records=[
                TableRecord(record_id="rec_1", fields={"分析状态": "已完成分析", "特征打点JSON": ""}),
                TableRecord(record_id="rec_2", fields={"分析状态": "已完成分析", "批内优先级分": 76}),
                TableRecord(record_id="rec_3", fields={"分析状态": "待处理", "特征打点JSON": ""}),
                TableRecord(record_id="rec_4", fields={"分析状态": "已完成分析", "建议动作": "优先测试"}),
                TableRecord(record_id="rec_5", fields={"分析状态": "已完成分析"}),
            ]
        )

        records = adapter.read_pending_records(config, client, record_scope="completed_missing_v2")
        self.assertEqual([record.record_id for record in records], ["rec_1", "rec_5"])

    def test_read_completed_records_can_filter_by_risk_tag(self):
        adapter = TableAdapter()
        config = build_table_config()
        config.writeback_map["risk_tag"] = "主要风险标签"
        client = FakeClient(
            records=[
                TableRecord(record_id="rec_1", fields={"分析状态": "已完成分析", "主要风险标签": "图片信息不足"}),
                TableRecord(record_id="rec_2", fields={"分析状态": "已完成分析", "主要风险标签": "同质化偏高"}),
                TableRecord(record_id="rec_3", fields={"分析状态": "待处理", "主要风险标签": "图片信息不足"}),
            ]
        )

        records = adapter.read_pending_records(
            config,
            client,
            record_scope="completed",
            only_risk_tag="图片信息不足",
        )
        self.assertEqual([record.record_id for record in records], ["rec_1"])

    def test_map_record_to_candidate_task_normalizes_v2_fields(self):
        adapter = TableAdapter()
        config = build_table_config()
        client = FakeClient(download_urls={"img_tok_1": "https://download.example.com/1.jpg"})
        record = TableRecord(
            record_id="rec_1",
            fields={
                "分析批次": "batch_20260416",
                "产品标题": "  夏季薄款冰丝开衫防晒罩衫  ",
                "产品图片": [
                    {"file_token": "img_tok_1", "name": "image-1.jpg"},
                    "https://cdn.example.com/2.jpg",
                ],
                "采购价": "¥12.50",
                "拟售价": "29.9 元",
                "人工类目": " 轻上装 ",
                "产品备注": None,
                "竞品备注": "偏轻薄外搭",
                "竞品链接": "https://a.example.com\nhttps://b.example.com",
                "目标国家": "TH",
                "未映射字段": "保留原值",
            },
        )

        task = adapter.map_record_to_candidate_task(record, config, client=client)

        self.assertEqual(task.source_record_id, "rec_1")
        self.assertEqual(task.batch_id, "batch_20260416")
        self.assertEqual(task.product_title, "夏季薄款冰丝开衫防晒罩衫")
        self.assertEqual(task.product_name, "夏季薄款冰丝开衫防晒罩衫")
        self.assertEqual(
            task.product_images,
            ["https://download.example.com/1.jpg", "https://cdn.example.com/2.jpg"],
        )
        self.assertEqual(task.cost_price, 12.5)
        self.assertEqual(task.target_price, 29.9)
        self.assertEqual(task.manual_category, "轻上装")
        self.assertEqual(task.product_notes, "")
        self.assertEqual(task.competitor_notes, "偏轻薄外搭")
        self.assertEqual(task.competitor_links, ["https://a.example.com", "https://b.example.com"])
        self.assertEqual(task.target_market, "TH")
        self.assertEqual(task.title_keyword_tags, [])
        self.assertEqual(task.extra_fields, {"未映射字段": "保留原值"})

    def test_normalize_image_item_supports_link_dict(self):
        adapter = TableAdapter()

        normalized = adapter._normalize_image_item(
            {
                "link": "https://cdn.example.com/product.jpg",
                "text": "https://cdn.example.com/product.jpg",
            }
        )

        self.assertEqual(normalized, "https://cdn.example.com/product.jpg")


if __name__ == "__main__":
    unittest.main()
