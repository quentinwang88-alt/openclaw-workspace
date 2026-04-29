import io
import json
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.config import Settings  # noqa: E402
from app.db import Database  # noqa: E402
from app.feishu import TableRecord  # noqa: E402
from app.models import HermesBatchResult  # noqa: E402
from app.pipeline import FastMossPipeline  # noqa: E402


def dataframe_to_xlsx_bytes(frame: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    frame.to_excel(buffer, index=False)
    return buffer.getvalue()


class FakeTableClient(object):
    def __init__(self, records=None, attachment_payloads=None):
        self.records = records or []
        self.attachment_payloads = attachment_payloads or {}

    def list_records(self, page_size=100, limit=None):
        items = list(self.records)
        if limit is not None:
            return items[:limit]
        return items

    def batch_create_records(self, records):
        for item in records:
            record_id = "rec_{index}".format(index=len(self.records) + 1)
            self.records.append(TableRecord(record_id=record_id, fields=dict(item["fields"])))

    def batch_update_records(self, records):
        for item in records:
            self.update_record_fields(item["record_id"], item["fields"])

    def update_record_fields(self, record_id, fields):
        for item in self.records:
            if item.record_id == record_id:
                item.fields.update(fields)
                return
        raise KeyError(record_id)

    def download_attachment_bytes(self, attachment):
        file_token = attachment["file_token"]
        payload = self.attachment_payloads[file_token]
        return payload, attachment.get("name", "fastmoss.xlsx"), attachment.get("type", "application/vnd.ms-excel"), len(payload)

    def upload_attachment(self, content, file_name, content_type, size=None):
        return {
            "file_token": "uploaded_{name}".format(name=file_name),
            "name": file_name,
            "size": int(size or len(content)),
            "type": content_type,
        }


class FakeMessenger(object):
    def __init__(self):
        self.sent_texts = []
        self.sent_mentions = []
        self.sent_files = []
        self.messages_by_chat = {}

    def send_text(self, chat_id, text):
        self.sent_texts.append((chat_id, text))
        return {"chat_id": chat_id}

    def send_text_with_mention(self, chat_id, user_open_id, display_name, text):
        self.sent_mentions.append((chat_id, user_open_id, display_name, text))
        return {"chat_id": chat_id}

    def send_file(self, chat_id, file_path):
        self.sent_files.append((chat_id, str(file_path)))
        return {"chat_id": chat_id}

    def list_chat_messages(self, chat_id, page_size=50, since_timestamp=None, max_pages=5):
        return list(self.messages_by_chat.get(chat_id, []))


def fake_hermes_runner(batch_id, selection_rows, archive_dir, command_template, timeout_seconds):
    archive_dir = Path(archive_dir)
    archive_dir.mkdir(parents=True, exist_ok=True)
    output_path = archive_dir / "hermes_output.json"
    items = {}
    for row in selection_rows:
        items[row["work_id"]] = {
            "content_potential_score": 87,
            "differentiation_score": 78,
            "fit_judgment": "适合内容测试店",
            "strategy_suggestion": "先以低客单真人试穿切",
            "recommended_action": "优先跟进",
            "recommendation_reason": "毛利安全且竞争密度可控",
            "risk_warning": "注意供货稳定性",
        }
    output_path.write_text(json.dumps({"items": [dict({"work_id": key}, **value) for key, value in items.items()]}, ensure_ascii=False), encoding="utf-8")
    return HermesBatchResult(
        status="success",
        items=items,
        input_path=str(archive_dir / "hermes_input.json"),
        output_path=str(output_path),
    )


def fake_image_downloader(image_url, row=None):
    return b"fake-image", "image.jpg", "image/jpeg", len(b"fake-image")


class PipelineTest(unittest.TestCase):
    def test_end_to_end_batch_flow(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            db_path = temp_root / "fastmoss.sqlite3"
            settings = Settings(
                database_url="sqlite:///{path}".format(path=db_path),
                archive_root=temp_root / "runs",
                download_root=temp_root / "downloads",
                accio_bot_open_id="ou_accio",
            )
            database = Database(settings.database_url)
            database.init_schema()

            sample_frame = pd.DataFrame(
                [
                    {
                        "商品名称": "Cloud Tee",
                        "TikTok商品落地页地址": "https://shop.tiktok.com/view/product/1729384756?item_id=1729384756",
                        "店铺名称": "TH Apparel",
                        "商品图片": "https://img.example.com/1.jpg",
                        "预估商品上架时间": "2026-03-20",
                        "售价": "THB 120 - 180",
                        "7天销量": 240,
                        "7天销售额": 36000,
                        "总销量": 1200,
                        "总销售额": 156000,
                        "带货达人总数": 18,
                        "达人出单率": "12.5%",
                        "带货视频总数": 4,
                        "带货直播总数": 1,
                        "佣金比例": "15%",
                    },
                    {
                        "商品名称": "Reject Item",
                        "TikTok商品落地页地址": "https://shop.tiktok.com/view/product/9988776655",
                        "预估商品上架时间": "2026-03-05",
                        "售价": "THB 100",
                        "7天销量": 100,
                        "7天销售额": 10000,
                        "总销量": 300,
                        "总销售额": 30000,
                        "带货达人总数": 20,
                        "达人出单率": "5%",
                        "带货视频总数": 10,
                        "带货直播总数": 1,
                        "佣金比例": "10%",
                    },
                ]
            )
            attachment_bytes = dataframe_to_xlsx_bytes(sample_frame)

            config_client = FakeTableClient(
                records=[
                    TableRecord(
                        record_id="cfg_rec",
                        fields={
                            "config_id": "cfg_1",
                            "国家": "TH",
                            "类目": "Apparel",
                            "是否启用": True,
                            "新品天数阈值": 90,
                            "总销量下限": 500,
                            "总销量上限": 5000,
                            "新品7天销量下限": 120,
                            "老品7天销量下限": 200,
                            "老品7天销量占比下限": 0.10,
                            "视频竞争密度上限": 5,
                            "达人竞争密度上限": 20,
                            "汇率到人民币": 4.8,
                            "Accio目标群ID": "chat_accio",
                            "是否启用Hermes": True,
                            "规则版本号": "v1",
                            "备注": "",
                        },
                    )
                ]
            )
            batch_client = FakeTableClient(
                records=[
                    TableRecord(
                        record_id="batch_rec",
                        fields={
                            "batch_id": "batch_001",
                            "国家": "TH",
                            "类目": "Apparel",
                            "快照时间": "2026-04-14 12:00:00",
                            "原始文件附件": [{"file_token": "file_tok_1", "name": "fastmoss.xlsx"}],
                            "原始文件名": "fastmoss.xlsx",
                            "原始记录数": 2,
                            "A导入状态": "已完成",
                            "整体状态": "待B下载",
                            "重试次数": 0,
                        },
                    )
                ],
                attachment_payloads={"file_tok_1": attachment_bytes},
            )
            workspace_client = FakeTableClient()
            followup_client = FakeTableClient()
            messenger = FakeMessenger()

            pipeline = FastMossPipeline(
                settings=settings,
                db=database,
                config_client=config_client,
                batch_client=batch_client,
                workspace_client=workspace_client,
                followup_client=followup_client,
                messenger=messenger,
                hermes_runner=fake_hermes_runner,
                image_downloader=fake_image_downloader,
            )

            process_result = pipeline.process_pending_batches(send_accio=True)
            self.assertEqual(process_result["processed"], 1)
            self.assertEqual(len(workspace_client.records), 1)
            self.assertEqual(len(messenger.sent_texts), 0)
            self.assertEqual(len(messenger.sent_mentions), 1)
            self.assertEqual(len(messenger.sent_files), 2)
            sent_file_names = {Path(file_path).name for _, file_path in messenger.sent_files}
            self.assertEqual(sent_file_names, {"accio_request.xlsx", "accio_images.zip"})
            image_bundle_path = next(Path(file_path) for _, file_path in messenger.sent_files if file_path.endswith("accio_images.zip"))
            with zipfile.ZipFile(image_bundle_path, "r") as archive:
                bundle_names = set(archive.namelist())
                self.assertIn("1729384756.jpg", bundle_names)
                self.assertIn("manifest.json", bundle_names)
            batch_fields = batch_client.records[0].fields
            self.assertEqual(batch_fields["Accio状态"], "已发送")
            self.assertEqual(batch_fields["整体状态"], "规则完成待Accio")
            self.assertNotIn("本地文件路径", batch_fields)
            self.assertNotIn("文件哈希", batch_fields)

            work_id = workspace_client.records[0].fields["work_id"]
            initial_workspace_fields = workspace_client.records[0].fields
            self.assertEqual(
                set(initial_workspace_fields.keys()),
                {
                    "work_id",
                    "batch_id",
                    "product_id",
                    "国家",
                    "类目",
                    "商品名称",
                    "商品图片",
                    "商品图片附件",
                    "TikTok商品落地页地址",
                    "上架天数",
                    "7天销量",
                    "最低价_rmb",
                    "最高价_rmb",
                    "总销量",
                    "7天成交均价_rmb",
                    "入池类型",
                    "竞争成熟度",
                    "规则总分",
                    "规则通过原因",
                    "推荐采购价_rmb",
                    "Accio备注",
                    "商品粗毛利率",
                    "分销后毛利率",
                    "打法建议",
                    "Hermes推荐动作",
                    "Hermes推荐理由",
                    "Hermes风险提醒",
                    "人工最终状态",
                    "负责人",
                    "人工备注",
                    "是否进入跟进",
                },
            )
            accio_payload = {
                "batch_id": "batch_001",
                "items": [
                    {
                        "work_id": work_id,
                        "source_url": "https://detail.1688.com/offer/123.html",
                        "procurement_price_rmb": 20.0,
                        "procurement_price_range": "18-22",
                        "confidence": 0.82,
                        "abnormal_low_price": False,
                        "note": "报价稳定",
                    }
                ],
            }
            messenger.messages_by_chat["chat_accio"] = [
                {
                    "message_id": "msg_1",
                    "body": {
                        "content": json.dumps(
                            {"text": "```json\n{payload}\n```".format(payload=json.dumps(accio_payload, ensure_ascii=False))},
                            ensure_ascii=False,
                        )
                    },
                }
            ]

            collect_result = pipeline.collect_accio_results(run_hermes=True)
            self.assertEqual(collect_result["updated"], 1)
            workspace_fields = workspace_client.records[0].fields
            self.assertEqual(workspace_fields["Hermes推荐动作"], "优先跟进")
            self.assertAlmostEqual(workspace_fields["推荐采购价_rmb"], 20.0)
            self.assertAlmostEqual(workspace_fields["最低价_rmb"], 25.0, places=2)
            self.assertAlmostEqual(workspace_fields["最高价_rmb"], 37.5, places=2)
            self.assertAlmostEqual(workspace_fields["商品粗毛利率"], -0.08, places=2)
            self.assertAlmostEqual(workspace_fields["分销后毛利率"], -0.23, places=2)
            self.assertIn("https://detail.1688.com/offer/123.html", workspace_fields["Accio备注"])
            self.assertIn("报价稳定", workspace_fields["Accio备注"])
            self.assertEqual(batch_client.records[0].fields["Hermes状态"], "已完成")
            self.assertEqual(batch_client.records[0].fields["整体状态"], "Hermes完成待人审")
            self.assertNotIn("Accio状态", workspace_fields)
            self.assertNotIn("Hermes状态", workspace_fields)
            self.assertNotIn("店铺名称", workspace_fields)
            self.assertNotIn("视频竞争密度", workspace_fields)
            self.assertNotIn("达人竞争密度", workspace_fields)

            workspace_record_id = workspace_client.records[0].record_id
            workspace_client.update_record_fields(
                workspace_record_id,
                {"是否进入跟进": True, "人工最终状态": "优先跟进", "人工备注": "进入测试"},
            )
            sync_result = pipeline.sync_followups()
            self.assertEqual(sync_result["created"], 1)
            self.assertEqual(len(followup_client.records), 1)
            self.assertEqual(
                set(followup_client.records[0].fields.keys()),
                {
                    "followup_id",
                    "来源work_id",
                    "商品名称",
                    "国家",
                    "类目",
                    "跟进开始时间",
                    "打法",
                    "当前状态",
                    "7天复盘",
                    "30天复盘",
                    "最终结论",
                    "是否写回经验",
                    "复盘备注",
                },
            )
            self.assertEqual(followup_client.records[0].fields["来源work_id"], work_id)
            self.assertEqual(followup_client.records[0].fields["当前状态"], "跟进中")

            sync_again = pipeline.sync_followups()
            self.assertEqual(sync_again["created"], 0)
            self.assertEqual(sync_again["updated"], 1)
            self.assertEqual(len(followup_client.records), 1)

    def test_partial_accio_response_does_not_complete_batch(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            db_path = temp_root / "fastmoss.sqlite3"
            settings = Settings(
                database_url="sqlite:///{path}".format(path=db_path),
                archive_root=temp_root / "runs",
                download_root=temp_root / "downloads",
            )
            database = Database(settings.database_url)
            database.init_schema()

            sample_frame = pd.DataFrame(
                [
                    {
                        "商品名称": "Cloud Tee",
                        "TikTok商品落地页地址": "https://shop.tiktok.com/view/product/1729384756?item_id=1729384756",
                        "店铺名称": "TH Apparel",
                        "商品图片": "https://img.example.com/1.jpg",
                        "预估商品上架时间": "2026-03-20",
                        "售价": "THB 120 - 180",
                        "7天销量": 240,
                        "7天销售额": 36000,
                        "总销量": 1200,
                        "总销售额": 156000,
                        "带货达人总数": 18,
                        "达人出单率": "12.5%",
                        "带货视频总数": 4,
                        "带货直播总数": 1,
                        "佣金比例": "15%",
                    },
                    {
                        "商品名称": "Sky Tee",
                        "TikTok商品落地页地址": "https://shop.tiktok.com/view/product/1729384757?item_id=1729384757",
                        "店铺名称": "TH Apparel",
                        "商品图片": "https://img.example.com/2.jpg",
                        "预估商品上架时间": "2026-03-19",
                        "售价": "THB 150 - 210",
                        "7天销量": 260,
                        "7天销售额": 42900,
                        "总销量": 1400,
                        "总销售额": 210000,
                        "带货达人总数": 16,
                        "达人出单率": "14%",
                        "带货视频总数": 5,
                        "带货直播总数": 1,
                        "佣金比例": "12%",
                    },
                ]
            )
            attachment_bytes = dataframe_to_xlsx_bytes(sample_frame)

            config_client = FakeTableClient(
                records=[
                    TableRecord(
                        record_id="cfg_rec",
                        fields={
                            "config_id": "cfg_1",
                            "国家": "TH",
                            "类目": "Apparel",
                            "是否启用": True,
                            "新品天数阈值": 90,
                            "总销量下限": 500,
                            "总销量上限": 5000,
                            "新品7天销量下限": 120,
                            "老品7天销量下限": 200,
                            "老品7天销量占比下限": 0.10,
                            "视频竞争密度上限": 5,
                            "达人竞争密度上限": 20,
                            "汇率到人民币": 4.8,
                            "Accio目标群ID": "chat_accio",
                            "是否启用Hermes": True,
                            "规则版本号": "v1",
                            "备注": "",
                        },
                    )
                ]
            )
            batch_client = FakeTableClient(
                records=[
                    TableRecord(
                        record_id="batch_rec",
                        fields={
                            "batch_id": "batch_partial",
                            "国家": "TH",
                            "类目": "Apparel",
                            "快照时间": "2026-04-14 12:00:00",
                            "原始文件附件": [{"file_token": "file_tok_1", "name": "fastmoss.xlsx"}],
                            "原始文件名": "fastmoss.xlsx",
                            "原始记录数": 2,
                            "A导入状态": "已完成",
                            "整体状态": "待B下载",
                            "重试次数": 0,
                        },
                    )
                ],
                attachment_payloads={"file_tok_1": attachment_bytes},
            )
            workspace_client = FakeTableClient()
            messenger = FakeMessenger()

            pipeline = FastMossPipeline(
                settings=settings,
                db=database,
                config_client=config_client,
                batch_client=batch_client,
                workspace_client=workspace_client,
                followup_client=FakeTableClient(),
                messenger=messenger,
                hermes_runner=fake_hermes_runner,
                image_downloader=fake_image_downloader,
            )

            process_result = pipeline.process_pending_batches(send_accio=True)
            self.assertEqual(process_result["processed"], 1)
            self.assertEqual(len(workspace_client.records), 2)

            work_ids = [record.fields["work_id"] for record in workspace_client.records]
            accio_payload = {
                "batch_id": "batch_partial",
                "items": [
                    {
                        "work_id": work_ids[0],
                        "source_url": "https://detail.1688.com/offer/123.html",
                        "procurement_price_rmb": 20.0,
                        "note": "先回一条",
                    }
                ],
            }
            messenger.messages_by_chat["chat_accio"] = [
                {
                    "message_id": "msg_partial",
                    "body": {
                        "content": json.dumps(
                            {"text": "```json\n{payload}\n```".format(payload=json.dumps(accio_payload, ensure_ascii=False))},
                            ensure_ascii=False,
                        )
                    },
                }
            ]

            collect_result = pipeline.collect_accio_results(run_hermes=True)
            self.assertEqual(collect_result["updated"], 1)
            batch_fields = batch_client.records[0].fields
            self.assertEqual(batch_fields["Accio状态"], "已发送")
            self.assertEqual(batch_fields["整体状态"], "规则完成待Accio")
            self.assertEqual(batch_fields["Hermes状态"], "部分完成")
            self.assertIn("Accio 部分回收 1/2", batch_fields["错误信息"])
            self.assertIn("Hermes 已处理 1/1", batch_fields["错误信息"])

            priced_rows = [record.fields for record in workspace_client.records if record.fields.get("推荐采购价_rmb") is not None]
            self.assertEqual(len(priced_rows), 1)
            self.assertIsNone(next(record.fields for record in workspace_client.records if record.fields["work_id"] == work_ids[1]).get("推荐采购价_rmb"))
            self.assertEqual(priced_rows[0].get("Hermes推荐动作"), "优先跟进")

    def test_partial_accio_only_runs_hermes_for_newly_recovered_rows(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            db_path = temp_root / "fastmoss.sqlite3"
            settings = Settings(
                database_url="sqlite:///{path}".format(path=db_path),
                archive_root=temp_root / "runs",
                download_root=temp_root / "downloads",
            )
            database = Database(settings.database_url)
            database.init_schema()

            sample_frame = pd.DataFrame(
                [
                    {
                        "商品名称": "Cloud Tee",
                        "TikTok商品落地页地址": "https://shop.tiktok.com/view/product/1729384756?item_id=1729384756",
                        "店铺名称": "TH Apparel",
                        "商品图片": "https://img.example.com/1.jpg",
                        "预估商品上架时间": "2026-03-20",
                        "售价": "THB 120 - 180",
                        "7天销量": 240,
                        "7天销售额": 36000,
                        "总销量": 1200,
                        "总销售额": 156000,
                        "带货达人总数": 18,
                        "达人出单率": "12.5%",
                        "带货视频总数": 4,
                        "带货直播总数": 1,
                        "佣金比例": "15%",
                    },
                    {
                        "商品名称": "Sky Tee",
                        "TikTok商品落地页地址": "https://shop.tiktok.com/view/product/1729384757?item_id=1729384757",
                        "店铺名称": "TH Apparel",
                        "商品图片": "https://img.example.com/2.jpg",
                        "预估商品上架时间": "2026-03-19",
                        "售价": "THB 150 - 210",
                        "7天销量": 260,
                        "7天销售额": 42900,
                        "总销量": 1400,
                        "总销售额": 210000,
                        "带货达人总数": 16,
                        "达人出单率": "14%",
                        "带货视频总数": 5,
                        "带货直播总数": 1,
                        "佣金比例": "12%",
                    },
                ]
            )
            attachment_bytes = dataframe_to_xlsx_bytes(sample_frame)

            config_client = FakeTableClient(
                records=[
                    TableRecord(
                        record_id="cfg_rec",
                        fields={
                            "config_id": "cfg_1",
                            "国家": "TH",
                            "类目": "Apparel",
                            "是否启用": True,
                            "新品天数阈值": 90,
                            "总销量下限": 500,
                            "总销量上限": 5000,
                            "新品7天销量下限": 120,
                            "老品7天销量下限": 200,
                            "老品7天销量占比下限": 0.10,
                            "视频竞争密度上限": 5,
                            "达人竞争密度上限": 20,
                            "汇率到人民币": 4.8,
                            "Accio目标群ID": "chat_accio",
                            "是否启用Hermes": True,
                            "规则版本号": "v1",
                            "备注": "",
                        },
                    )
                ]
            )
            batch_client = FakeTableClient(
                records=[
                    TableRecord(
                        record_id="batch_rec",
                        fields={
                            "batch_id": "batch_partial_2",
                            "国家": "TH",
                            "类目": "Apparel",
                            "快照时间": "2026-04-14 12:00:00",
                            "原始文件附件": [{"file_token": "file_tok_1", "name": "fastmoss.xlsx"}],
                            "原始文件名": "fastmoss.xlsx",
                            "原始记录数": 2,
                            "A导入状态": "已完成",
                            "整体状态": "待B下载",
                            "重试次数": 0,
                        },
                    )
                ],
                attachment_payloads={"file_tok_1": attachment_bytes},
            )
            workspace_client = FakeTableClient()
            messenger = FakeMessenger()
            hermes_batches = []

            def tracking_hermes_runner(batch_id, selection_rows, archive_dir, command_template, timeout_seconds):
                hermes_batches.append([row["work_id"] for row in selection_rows])
                return fake_hermes_runner(batch_id, selection_rows, archive_dir, command_template, timeout_seconds)

            pipeline = FastMossPipeline(
                settings=settings,
                db=database,
                config_client=config_client,
                batch_client=batch_client,
                workspace_client=workspace_client,
                followup_client=FakeTableClient(),
                messenger=messenger,
                hermes_runner=tracking_hermes_runner,
                image_downloader=fake_image_downloader,
            )

            pipeline.process_pending_batches(send_accio=True)
            work_ids = [record.fields["work_id"] for record in workspace_client.records]

            first_payload = {
                "batch_id": "batch_partial_2",
                "items": [
                    {
                        "work_id": work_ids[0],
                        "source_url": "https://detail.1688.com/offer/123.html",
                        "procurement_price_rmb": 20.0,
                        "note": "first",
                    }
                ],
            }
            messenger.messages_by_chat["chat_accio"] = [
                {
                    "message_id": "msg_partial_1",
                    "body": {
                        "content": json.dumps(
                            {"text": "```json\n{payload}\n```".format(payload=json.dumps(first_payload, ensure_ascii=False))},
                            ensure_ascii=False,
                        )
                    },
                }
            ]
            pipeline.collect_accio_results(run_hermes=True)

            second_payload = {
                "batch_id": "batch_partial_2",
                "items": [
                    {
                        "work_id": work_ids[0],
                        "source_url": "https://detail.1688.com/offer/123.html",
                        "procurement_price_rmb": 20.0,
                        "note": "first",
                    },
                    {
                        "work_id": work_ids[1],
                        "source_url": "https://detail.1688.com/offer/456.html",
                        "procurement_price_rmb": 25.0,
                        "note": "second",
                    },
                ],
            }
            messenger.messages_by_chat["chat_accio"] = [
                {
                    "message_id": "msg_partial_2",
                    "body": {
                        "content": json.dumps(
                            {"text": "```json\n{payload}\n```".format(payload=json.dumps(second_payload, ensure_ascii=False))},
                            ensure_ascii=False,
                        )
                    },
                }
            ]
            pipeline.collect_accio_results(run_hermes=True)

            self.assertEqual(hermes_batches, [[work_ids[0]], [work_ids[1]]])
            batch_fields = batch_client.records[0].fields
            self.assertEqual(batch_fields["Accio状态"], "已回收")
            self.assertEqual(batch_fields["Hermes状态"], "已完成")
            self.assertEqual(batch_fields["整体状态"], "Hermes完成待人审")

    def test_run_hermes_with_batch_id_ignores_timeout_batch_status_if_rows_are_ready(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            db_path = temp_root / "fastmoss.sqlite3"
            settings = Settings(
                database_url="sqlite:///{path}".format(path=db_path),
                archive_root=temp_root / "runs",
                download_root=temp_root / "downloads",
            )
            database = Database(settings.database_url)
            database.init_schema()

            sample_frame = pd.DataFrame(
                [
                    {
                        "商品名称": "Cloud Tee",
                        "TikTok商品落地页地址": "https://shop.tiktok.com/view/product/1729384756?item_id=1729384756",
                        "店铺名称": "TH Apparel",
                        "商品图片": "https://img.example.com/1.jpg",
                        "预估商品上架时间": "2026-03-20",
                        "售价": "THB 120 - 180",
                        "7天销量": 240,
                        "7天销售额": 36000,
                        "总销量": 1200,
                        "总销售额": 156000,
                        "带货达人总数": 18,
                        "达人出单率": "12.5%",
                        "带货视频总数": 4,
                        "带货直播总数": 1,
                        "佣金比例": "15%",
                    }
                ]
            )
            attachment_bytes = dataframe_to_xlsx_bytes(sample_frame)

            config_client = FakeTableClient(
                records=[
                    TableRecord(
                        record_id="cfg_rec",
                        fields={
                            "config_id": "cfg_1",
                            "国家": "TH",
                            "类目": "Apparel",
                            "是否启用": True,
                            "新品天数阈值": 90,
                            "总销量下限": 500,
                            "总销量上限": 5000,
                            "新品7天销量下限": 120,
                            "老品7天销量下限": 200,
                            "老品7天销量占比下限": 0.10,
                            "视频竞争密度上限": 5,
                            "达人竞争密度上限": 20,
                            "汇率到人民币": 4.8,
                            "Accio目标群ID": "chat_accio",
                            "是否启用Hermes": True,
                            "规则版本号": "v1",
                            "备注": "",
                        },
                    )
                ]
            )
            batch_client = FakeTableClient(
                records=[
                    TableRecord(
                        record_id="batch_rec",
                        fields={
                            "batch_id": "batch_timeout_manual",
                            "国家": "TH",
                            "类目": "Apparel",
                            "快照时间": "2026-04-14 12:00:00",
                            "原始文件附件": [{"file_token": "file_tok_1", "name": "fastmoss.xlsx"}],
                            "原始文件名": "fastmoss.xlsx",
                            "原始记录数": 1,
                            "A导入状态": "已完成",
                            "整体状态": "待B下载",
                            "重试次数": 0,
                        },
                    )
                ],
                attachment_payloads={"file_tok_1": attachment_bytes},
            )
            workspace_client = FakeTableClient()
            messenger = FakeMessenger()

            pipeline = FastMossPipeline(
                settings=settings,
                db=database,
                config_client=config_client,
                batch_client=batch_client,
                workspace_client=workspace_client,
                followup_client=FakeTableClient(),
                messenger=messenger,
                hermes_runner=fake_hermes_runner,
                image_downloader=fake_image_downloader,
            )

            pipeline.process_pending_batches(send_accio=True)
            work_id = workspace_client.records[0].fields["work_id"]
            pipeline.db.upsert_selection_records(
                [
                    {
                        "work_id": work_id,
                        "accio_status": "已回收",
                        "procurement_price_rmb": 20.0,
                        "gross_margin_rate": 0.36,
                        "distribution_margin_rate": 0.31,
                    }
                ]
            )
            batch_client.records[0].fields["Accio状态"] = "超时"
            batch_client.records[0].fields["Hermes状态"] = "未开始"
            batch_client.records[0].fields["整体状态"] = "规则完成待Accio"

            result = pipeline.run_hermes_for_batches(batch_id="batch_timeout_manual")
            self.assertEqual(result["completed"], 1)
            self.assertEqual(batch_client.records[0].fields["Hermes状态"], "已完成")
            self.assertEqual(workspace_client.records[0].fields["Hermes推荐动作"], "优先跟进")

    def test_run_hermes_chunks_large_candidate_set(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            db_path = temp_root / "fastmoss.sqlite3"
            settings = Settings(
                database_url="sqlite:///{path}".format(path=db_path),
                archive_root=temp_root / "runs",
                download_root=temp_root / "downloads",
                hermes_chunk_size=1,
            )
            database = Database(settings.database_url)
            database.init_schema()

            sample_frame = pd.DataFrame(
                [
                    {
                        "商品名称": "Cloud Tee",
                        "TikTok商品落地页地址": "https://shop.tiktok.com/view/product/1729384756?item_id=1729384756",
                        "店铺名称": "TH Apparel",
                        "商品图片": "https://img.example.com/1.jpg",
                        "预估商品上架时间": "2026-03-20",
                        "售价": "THB 120 - 180",
                        "7天销量": 240,
                        "7天销售额": 36000,
                        "总销量": 1200,
                        "总销售额": 156000,
                        "带货达人总数": 18,
                        "达人出单率": "12.5%",
                        "带货视频总数": 4,
                        "带货直播总数": 1,
                        "佣金比例": "15%",
                    },
                    {
                        "商品名称": "Velvet Tee",
                        "TikTok商品落地页地址": "https://shop.tiktok.com/view/product/1729384757?item_id=1729384757",
                        "店铺名称": "TH Apparel",
                        "商品图片": "https://img.example.com/2.jpg",
                        "预估商品上架时间": "2026-03-16",
                        "售价": "THB 130 - 190",
                        "7天销量": 260,
                        "7天销售额": 39000,
                        "总销量": 1400,
                        "总销售额": 168000,
                        "带货达人总数": 20,
                        "达人出单率": "11.5%",
                        "带货视频总数": 4,
                        "带货直播总数": 1,
                        "佣金比例": "15%",
                    },
                ]
            )
            attachment_bytes = dataframe_to_xlsx_bytes(sample_frame)
            hermes_batches = []

            def tracking_hermes_runner(batch_id, selection_rows, archive_dir, command_template, timeout_seconds):
                hermes_batches.append([row["work_id"] for row in selection_rows])
                return fake_hermes_runner(batch_id, selection_rows, archive_dir, command_template, timeout_seconds)

            config_client = FakeTableClient(
                records=[
                    TableRecord(
                        record_id="cfg_rec",
                        fields={
                            "config_id": "cfg_1",
                            "国家": "TH",
                            "类目": "Apparel",
                            "是否启用": True,
                            "新品天数阈值": 90,
                            "总销量下限": 500,
                            "总销量上限": 5000,
                            "新品7天销量下限": 120,
                            "老品7天销量下限": 200,
                            "老品7天销量占比下限": 0.10,
                            "视频竞争密度上限": 5,
                            "达人竞争密度上限": 20,
                            "汇率到人民币": 4.8,
                            "Accio目标群ID": "chat_accio",
                            "是否启用Hermes": True,
                            "规则版本号": "v1",
                            "备注": "",
                        },
                    )
                ]
            )
            batch_client = FakeTableClient(
                records=[
                    TableRecord(
                        record_id="batch_rec",
                        fields={
                            "batch_id": "batch_chunked",
                            "国家": "TH",
                            "类目": "Apparel",
                            "快照时间": "2026-04-14 12:00:00",
                            "原始文件附件": [{"file_token": "file_tok_1", "name": "fastmoss.xlsx"}],
                            "原始文件名": "fastmoss.xlsx",
                            "原始记录数": 2,
                            "A导入状态": "已完成",
                            "整体状态": "待B下载",
                            "重试次数": 0,
                        },
                    )
                ],
                attachment_payloads={"file_tok_1": attachment_bytes},
            )
            workspace_client = FakeTableClient()
            messenger = FakeMessenger()

            pipeline = FastMossPipeline(
                settings=settings,
                db=database,
                config_client=config_client,
                batch_client=batch_client,
                workspace_client=workspace_client,
                followup_client=FakeTableClient(),
                messenger=messenger,
                hermes_runner=tracking_hermes_runner,
                image_downloader=fake_image_downloader,
            )

            pipeline.process_pending_batches(send_accio=False)
            work_ids = [record.fields["work_id"] for record in workspace_client.records]
            pipeline.db.upsert_selection_records(
                [
                    {
                        "work_id": work_ids[0],
                        "accio_status": "已回收",
                        "procurement_price_rmb": 20.0,
                        "gross_margin_rate": 0.36,
                        "distribution_margin_rate": 0.31,
                    },
                    {
                        "work_id": work_ids[1],
                        "accio_status": "已回收",
                        "procurement_price_rmb": 21.0,
                        "gross_margin_rate": 0.37,
                        "distribution_margin_rate": 0.32,
                    },
                ]
            )
            batch_client.records[0].fields["Accio状态"] = "已回收"
            batch_client.records[0].fields["Hermes状态"] = "未开始"
            batch_client.records[0].fields["整体状态"] = "Accio完成待Hermes"

            result = pipeline.run_hermes_for_batches(batch_id="batch_chunked")
            self.assertEqual(result["completed"], 1)
            self.assertEqual(len(hermes_batches), 2)
            self.assertEqual({tuple(chunk) for chunk in hermes_batches}, {(work_ids[0],), (work_ids[1],)})
            self.assertEqual(batch_client.records[0].fields["Hermes状态"], "已完成")
            self.assertEqual(batch_client.records[0].fields["整体状态"], "Hermes完成待人审")

    def test_run_hermes_reprocesses_skipped_placeholder_rows_after_enable(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            db_path = temp_root / "fastmoss.sqlite3"
            settings = Settings(
                database_url="sqlite:///{path}".format(path=db_path),
                archive_root=temp_root / "runs",
                download_root=temp_root / "downloads",
            )
            database = Database(settings.database_url)
            database.init_schema()

            sample_frame = pd.DataFrame(
                [
                    {
                        "商品名称": "Cloud Tee",
                        "TikTok商品落地页地址": "https://shop.tiktok.com/view/product/1729384756?item_id=1729384756",
                        "店铺名称": "TH Apparel",
                        "商品图片": "https://img.example.com/1.jpg",
                        "预估商品上架时间": "2026-03-20",
                        "售价": "THB 120 - 180",
                        "7天销量": 240,
                        "7天销售额": 36000,
                        "总销量": 1200,
                        "总销售额": 156000,
                        "带货达人总数": 18,
                        "达人出单率": "12.5%",
                        "带货视频总数": 4,
                        "带货直播总数": 1,
                        "佣金比例": "15%",
                    }
                ]
            )
            attachment_bytes = dataframe_to_xlsx_bytes(sample_frame)

            config_client = FakeTableClient(
                records=[
                    TableRecord(
                        record_id="cfg_rec",
                        fields={
                            "config_id": "cfg_1",
                            "国家": "TH",
                            "类目": "Apparel",
                            "是否启用": True,
                            "新品天数阈值": 90,
                            "总销量下限": 500,
                            "总销量上限": 5000,
                            "新品7天销量下限": 120,
                            "老品7天销量下限": 200,
                            "老品7天销量占比下限": 0.10,
                            "视频竞争密度上限": 5,
                            "达人竞争密度上限": 20,
                            "汇率到人民币": 4.8,
                            "Accio目标群ID": "chat_accio",
                            "是否启用Hermes": True,
                            "规则版本号": "v1",
                            "备注": "",
                        },
                    )
                ]
            )
            batch_client = FakeTableClient(
                records=[
                    TableRecord(
                        record_id="batch_rec",
                        fields={
                            "batch_id": "batch_skip_reprocess",
                            "国家": "TH",
                            "类目": "Apparel",
                            "快照时间": "2026-04-14 12:00:00",
                            "原始文件附件": [{"file_token": "file_tok_1", "name": "fastmoss.xlsx"}],
                            "原始文件名": "fastmoss.xlsx",
                            "原始记录数": 1,
                            "A导入状态": "已完成",
                            "整体状态": "待B下载",
                            "重试次数": 0,
                        },
                    )
                ],
                attachment_payloads={"file_tok_1": attachment_bytes},
            )
            workspace_client = FakeTableClient()

            pipeline = FastMossPipeline(
                settings=settings,
                db=database,
                config_client=config_client,
                batch_client=batch_client,
                workspace_client=workspace_client,
                followup_client=FakeTableClient(),
                messenger=FakeMessenger(),
                hermes_runner=fake_hermes_runner,
                image_downloader=fake_image_downloader,
            )

            pipeline.process_pending_batches(send_accio=False)
            work_id = workspace_client.records[0].fields["work_id"]
            pipeline.db.upsert_selection_records(
                [
                    {
                        "work_id": work_id,
                        "accio_status": "已回收",
                        "procurement_price_rmb": 20.0,
                        "gross_margin_rate": 0.36,
                        "distribution_margin_rate": 0.31,
                        "hermes_status": "已完成",
                        "recommended_action": "待人工判断",
                        "recommendation_reason": "参数配置关闭 Hermes，已跳过自动判断",
                    }
                ]
            )

            result = pipeline.run_hermes_for_batches(batch_id="batch_skip_reprocess")
            self.assertEqual(result["completed"], 1)
            row = pipeline.db.list_selection_records("batch_skip_reprocess")[0]
            self.assertEqual(row["recommended_action"], "优先跟进")
            self.assertEqual(row["hermes_status"], "已完成")


if __name__ == "__main__":
    unittest.main()
