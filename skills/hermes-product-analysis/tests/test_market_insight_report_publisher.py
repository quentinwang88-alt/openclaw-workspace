import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.feishu import FeishuAPIError  # noqa: E402
from src.market_insight_report_publisher import MarketInsightReportPublisher  # noqa: E402


class FakeDocClient(object):
    def __init__(self):
        self.created = []
        self.appended = []
        self.messages = []
        self.webhooks = []

    def create_document(self, title, folder_token=""):
        self.created.append({"title": title, "folder_token": folder_token})
        return {"document_id": "doc_123", "doc_token": "doc_123", "title": title}

    def append_markdown(self, document_id, content, batch_size=40):
        self.appended.append({"document_id": document_id, "content": content, "batch_size": batch_size})
        return {"line_count": 3, "batch_count": 1}

    def send_text_message(self, receive_id_type, receive_id, text):
        self.messages.append({"receive_id_type": receive_id_type, "receive_id": receive_id, "text": text})
        return {"message_id": "om_123"}

    def send_webhook_text(self, webhook_url, text, secret=""):
        self.webhooks.append({"webhook_url": webhook_url, "text": text, "secret": secret})


class FailingDocCreateClient(FakeDocClient):
    def create_document(self, title, folder_token=""):
        raise FeishuAPIError("docx create forbidden")


class MarketInsightReportPublisherTest(unittest.TestCase):
    def test_publish_skips_when_not_enabled(self):
        publisher = MarketInsightReportPublisher(doc_client=FakeDocClient())
        result = publisher.publish(
            report_markdown="# test\n",
            report_payload={"decision_summary": {}},
            country="VN",
            category="hair_accessory",
            batch_date="2026-04-21",
            report_output={},
        )

        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["notification_status"], "skipped")

    def test_publish_creates_doc_and_sends_message(self):
        client = FakeDocClient()
        publisher = MarketInsightReportPublisher(doc_client=client)
        result = publisher.publish(
            report_markdown="# test\n",
            report_payload={
                "decision_summary": {
                    "enter": {"display_names": ["韩系轻通勤型"], "total_count": 1, "overflow_count": 0},
                    "watch": {"display_names": ["基础通勤型"], "total_count": 1, "overflow_count": 0},
                    "avoid": {"display_names": ["甜感装饰型"], "total_count": 1, "overflow_count": 0},
                }
            },
            country="VN",
            category="hair_accessory",
            batch_date="2026-04-21",
            report_output={
                "enabled": True,
                "parent_folder_token": "fld_123",
                "notify_receive_id_type": "open_id",
                "notify_receive_id": "ou_123",
                "feishu_domain": "gcngopvfvo0q.feishu.cn",
            },
        )

        self.assertEqual(result["status"], "published")
        self.assertEqual(result["notification_status"], "sent")
        self.assertTrue(result["feishu_doc_url"].endswith("/docx/doc_123"))
        self.assertEqual(client.created[0]["folder_token"], "fld_123")
        self.assertEqual(client.messages[0]["receive_id_type"], "open_id")

    def test_publish_sends_signed_webhook_when_secret_configured(self):
        client = FakeDocClient()
        publisher = MarketInsightReportPublisher(doc_client=client)
        result = publisher.publish(
            report_markdown="# test\n",
            report_payload={"decision_summary": {}},
            country="VN",
            category="hair_accessory",
            batch_date="2026-04-21",
            report_output={
                "enabled": True,
                "parent_folder_token": "fld_123",
                "notify_webhook_url": "https://open.feishu.cn/open-apis/bot/v2/hook/test",
                "notify_webhook_secret": "secret_123",
                "feishu_domain": "gcngopvfvo0q.feishu.cn",
            },
        )

        self.assertEqual(result["status"], "published")
        self.assertEqual(result["notification_status"], "sent")
        self.assertEqual(client.webhooks[0]["webhook_url"], "https://open.feishu.cn/open-apis/bot/v2/hook/test")
        self.assertEqual(client.webhooks[0]["secret"], "secret_123")

    def test_publish_falls_back_to_webhook_summary_when_doc_creation_fails(self):
        client = FailingDocCreateClient()
        publisher = MarketInsightReportPublisher(doc_client=client)
        result = publisher.publish(
            report_markdown="# test\n",
            report_payload={
                "decision_summary": {
                    "enter": {"display_names": ["韩系轻通勤型"], "total_count": 1, "overflow_count": 0},
                    "watch": {"display_names": [], "total_count": 0, "overflow_count": 0},
                    "avoid": {"display_names": ["甜感装饰型"], "total_count": 1, "overflow_count": 0},
                }
            },
            country="VN",
            category="hair_accessory",
            batch_date="2026-04-21",
            report_output={
                "enabled": True,
                "notify_webhook_url": "https://open.feishu.cn/open-apis/bot/v2/hook/test",
                "notify_webhook_secret": "secret_123",
            },
        )

        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["notification_status"], "sent")
        self.assertEqual(client.webhooks[0]["secret"], "secret_123")
        self.assertIn("文档创建失败", client.webhooks[0]["text"])


if __name__ == "__main__":
    unittest.main()
