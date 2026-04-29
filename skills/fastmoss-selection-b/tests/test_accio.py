import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.accio import parse_accio_response_from_messages  # noqa: E402


class AccioParsingTest(unittest.TestCase):
    def test_ignores_example_block_without_matching_work_id(self):
        messages = [
            {
                "message_id": "msg_example",
                "body": {
                    "content": json.dumps(
                        {
                            "text": "```json\n"
                            + json.dumps(
                                {
                                    "batch_id": "batch_001",
                                    "items": [
                                        {
                                            "work_id": "xxx",
                                            "source_url": "https://detail.1688.com/offer/xxx.html",
                                            "procurement_price_rmb": 12.8,
                                        }
                                    ],
                                },
                                ensure_ascii=False,
                            )
                            + "\n```"
                        },
                        ensure_ascii=False,
                    )
                },
            }
        ]
        response = parse_accio_response_from_messages(
            messages,
            "batch_001",
            valid_work_ids={"batch_001_123"},
        )
        self.assertIsNone(response)

    def test_parses_split_post_json_without_markdown_fence(self):
        messages = [
            {
                "message_id": "msg_tail",
                "create_time": "200",
                "body": {
                    "content": json.dumps(
                        {
                            "title": "",
                            "content": [
                                [
                                    {
                                        "tag": "text",
                                        "text": '"note": "ok"\n    }\n  ]\n}',
                                    }
                                ]
                            ],
                        },
                        ensure_ascii=False,
                    )
                },
            },
            {
                "message_id": "msg_head",
                "create_time": "100",
                "body": {
                    "content": json.dumps(
                        {
                            "title": "",
                            "content": [
                                [
                                    {
                                        "tag": "code_block",
                                        "language": "JSON",
                                        "text": '{\n  "batch_id": "batch_001",\n  "items": [\n    {\n      "work_id": "batch_001_123",\n      "source_url": "https://detail.1688.com/offer/1.html",\n      "procurement_price_rmb": 2.5,\n      ',
                                    }
                                ]
                            ],
                        },
                        ensure_ascii=False,
                    )
                },
            },
        ]
        response = parse_accio_response_from_messages(
            messages,
            "batch_001",
            valid_work_ids={"batch_001_123"},
        )
        self.assertIsNotNone(response)
        self.assertEqual(response.items["batch_001_123"]["procurement_price_rmb"], 2.5)

    def test_parses_split_item_objects_without_complete_outer_payload(self):
        messages = [
            {
                "message_id": "msg_items",
                "body": {
                    "content": json.dumps(
                        {
                            "title": "",
                            "content": [
                                [
                                    {
                                        "tag": "text",
                                        "text": '{\n  "work_id": "batch_001_123",\n  "source_url": "https://detail.1688.com/offer/1.html",\n  "procurement_price_rmb": 3.2,\n  "note": "ok"\n}\n{\n  "work_id": "batch_001_456",\n  "source_url": "https://detail.1688.com/offer/2.html",\n  "procurement_price_rmb": 4.5,\n  "note": "ok2"\n}',
                                    }
                                ]
                            ],
                        },
                        ensure_ascii=False,
                    )
                },
            }
        ]
        response = parse_accio_response_from_messages(
            messages,
            "batch_001",
            valid_work_ids={"batch_001_123", "batch_001_456"},
        )
        self.assertIsNotNone(response)
        self.assertEqual(len(response.items), 2)
        self.assertEqual(response.items["batch_001_456"]["procurement_price_rmb"], 4.5)

    def test_merges_items_across_multiple_messages_for_same_batch(self):
        messages = [
            {
                "message_id": "msg_2",
                "create_time": "200",
                "body": {
                    "content": json.dumps(
                        {
                            "text": json.dumps(
                                {
                                    "batch_id": "batch_001",
                                    "items": [
                                        {
                                            "work_id": "batch_001_456",
                                            "source_url": "https://detail.1688.com/offer/2.html",
                                            "procurement_price_rmb": 4.5,
                                        }
                                    ],
                                },
                                ensure_ascii=False,
                            )
                        },
                        ensure_ascii=False,
                    )
                },
            },
            {
                "message_id": "msg_1",
                "create_time": "100",
                "body": {
                    "content": json.dumps(
                        {
                            "text": json.dumps(
                                {
                                    "batch_id": "batch_001",
                                    "items": [
                                        {
                                            "work_id": "batch_001_123",
                                            "source_url": "https://detail.1688.com/offer/1.html",
                                            "procurement_price_rmb": 2.5,
                                        }
                                    ],
                                },
                                ensure_ascii=False,
                            )
                        },
                        ensure_ascii=False,
                    )
                },
            },
        ]
        response = parse_accio_response_from_messages(
            messages,
            "batch_001",
            valid_work_ids={"batch_001_123", "batch_001_456"},
        )
        self.assertIsNotNone(response)
        self.assertEqual(len(response.items), 2)
        self.assertEqual(response.items["batch_001_123"]["procurement_price_rmb"], 2.5)
        self.assertEqual(response.items["batch_001_456"]["procurement_price_rmb"], 4.5)


if __name__ == "__main__":
    unittest.main()
