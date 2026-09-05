#!/usr/bin/env python3
"""飞书 bitable 客户端兼容性测试。"""

from __future__ import annotations

import unittest
from pathlib import Path
import sys
from unittest.mock import patch


TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from core.bitable import FeishuBitableClient, TableField


class DummyResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

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
    @patch("core.bitable.time.sleep")
    @patch("core.bitable.requests.request")
    def test_request_retries_transient_business_error(self, request, sleep) -> None:
        request.side_effect = [
            DummyResponse({"code": 1254607, "msg": "Data not ready, please try again later"}),
            DummyResponse({"code": 0, "data": {"items": []}}),
        ]
        client = FeishuBitableClient(app_token="app_token", table_id="tbl_token")
        client.access_token = "test"
        client.token_expires_at = 99999999999

        records = client.list_records()

        self.assertEqual(records, [])
        self.assertEqual(request.call_count, 2)
        self.assertEqual(sleep.call_count, 1)

    @patch("core.bitable.get_tenant_access_token", return_value="fresh-token")
    @patch("core.bitable.requests.request")
    def test_request_refreshes_invalid_access_token(self, request, get_token) -> None:
        request.side_effect = [
            DummyResponse({"code": 99991663, "msg": "Invalid access token for authorization"}),
            DummyResponse({"code": 0, "data": {"items": []}}),
        ]
        client = FeishuBitableClient(app_token="app_token", table_id="tbl_token")
        client.access_token = "stale-token"
        client.token_expires_at = 99999999999

        records = client.list_records()

        self.assertEqual(records, [])
        self.assertEqual(request.call_count, 2)
        self.assertEqual(get_token.call_count, 1)
        self.assertEqual(
            request.call_args_list[1].kwargs["headers"]["Authorization"],
            "Bearer fresh-token",
        )
        self.assertEqual(client.retry_count, 1)

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

    @patch("core.bitable.requests.request")
    def test_get_record_reads_one_record_endpoint(self, request) -> None:
        request.return_value = DummyResponse(
            {
                "code": 0,
                "data": {
                    "record": {
                        "record_id": "rec_123",
                        "fields": {"脚本ID": "S1"},
                    }
                },
            }
        )
        client = FeishuBitableClient(app_token="app_token", table_id="tbl_token")
        client.access_token = "test"
        client.token_expires_at = 99999999999

        record = client.get_record("rec_123")

        self.assertEqual(record.record_id, "rec_123")
        self.assertEqual(record.fields, {"脚本ID": "S1"})
        self.assertIn("/records/rec_123", request.call_args.args[1])


if __name__ == "__main__":
    unittest.main()
