#!/usr/bin/env python3
"""飞书 bitable 客户端兼容性测试。"""

from __future__ import annotations

import unittest
from pathlib import Path
import sys


TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from core.bitable import FeishuBitableClient, TableField


class DummyResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class FakeClient(FeishuBitableClient):
    def __init__(self):
        super().__init__(app_token="app_token", table_id="tbl_token")
        self.calls = []

    def _headers(self):
        return {"Authorization": "Bearer test"}

    def _request(self, method: str, url: str, **kwargs):
        self.calls.append((method, url, kwargs))
        if method == "GET":
            return DummyResponse(
                {
                    "code": 0,
                    "data": {
                        "items": [
                            {
                                "field_id": "fld_1",
                                "field_name": "脚本ID",
                                "type": 1,
                                "ui_type": "Text",
                                "property": None,
                            }
                        ]
                    },
                }
            )
        if method == "POST":
            return DummyResponse(
                {
                    "code": 0,
                    "data": {
                        "field": {
                            "field_id": "fld_2",
                            "field_name": kwargs["json"]["field_name"],
                        }
                    },
                }
            )
        raise AssertionError(f"Unexpected method: {method}")


class BitableClientCompatibilityTest(unittest.TestCase):
    def test_list_fields_returns_table_field_objects(self) -> None:
        client = FakeClient()

        fields = client.list_fields()

        self.assertEqual(len(fields), 1)
        self.assertIsInstance(fields[0], TableField)
        self.assertEqual(fields[0].field_name, "脚本ID")
        self.assertEqual(client.list_field_names(), ["脚本ID"])

    def test_create_field_posts_expected_payload(self) -> None:
        client = FakeClient()

        result = client.create_field(field_name="发布状态", field_type=1, ui_type="Text")

        self.assertEqual(result["field"]["field_name"], "发布状态")
        method, url, kwargs = client.calls[-1]
        self.assertEqual(method, "POST")
        self.assertIn("/fields", url)
        self.assertEqual(
            kwargs["json"],
            {"field_name": "发布状态", "type": 1, "ui_type": "Text"},
        )


if __name__ == "__main__":
    unittest.main()
