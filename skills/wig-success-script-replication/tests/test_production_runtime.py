from __future__ import annotations

import sys
import unittest
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from production_runtime import (  # noqa: E402
    FeishuPromptWriter,
    FeishuTaskRunner,
    _notes_parts,
    _optional_count,
    _relation_ids,
)


class FakeFeishu:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.created = []

    def list_records(self, app_token, table_id):
        return list(self.rows)

    def create_record(self, app_token, table_id, fields):
        self.created.append(fields)
        return {"record": {"record_id": "recNew"}}

    def update_record(self, app_token, table_id, record_id, fields):
        for row in self.rows:
            if row.get("record_id") == record_id:
                row.setdefault("fields", {}).update(fields)
                return

    def get_record(self, app_token, table_id, record_id):
        return next(row for row in self.rows if row.get("record_id") == record_id)


class ProductionRuntimeTests(unittest.TestCase):
    def test_one_click_is_the_primary_feishu_action(self):
        self.assertEqual(FeishuTaskRunner.ACTIONS["开始生成"], "one-click")

    def test_preview_routes_start_action_to_one_click(self):
        client = FakeFeishu([{
            "record_id": "recA",
            "fields": {"母版名称": "测试", "动作请求": "开始生成", "待复刻产品": []},
        }])
        runner = FeishuTaskRunner(
            client=client, app_token="app", mother_table="mother", product_table="product",
            repository=object(), mother_service=object(), replication_service=object(), outbox_service=object(),
        )
        preview = runner.preview_pending(limit=5)
        self.assertEqual(preview["tasks"][0]["planned_action"], "one-click")
    def test_relation_ids_accepts_feishu_shapes(self):
        value = [
            {"record_id": "recA"}, "recB", {"id": "ignored"}, {"recordId": "recC"},
            {"record_ids": ["recD", "recE"], "type": "text"},
        ]
        self.assertEqual(_relation_ids(value), ["recA", "recB", "recC", "recD", "recE"])

    def test_only_explicit_note_labels_become_claims(self):
        points, actions, forbidden = _notes_parts(
            "这是一条普通备注\n卖点：已确认长度 26 英寸\n证明动作: 正面转身, 侧面拨发\n禁用说法：耐热"
        )
        self.assertEqual(points, ["已确认长度 26 英寸"])
        self.assertEqual(actions, ["正面转身", "侧面拨发"])
        self.assertEqual(forbidden, ["耐热"])

    def test_prompt_outbox_retry_is_idempotent_without_hidden_fields(self):
        payload = {
            "product_id": "WIG-1",
            "variant_type": "hook_variant",
            "change_summary": "只改开头",
            "full_prompt": "完整内容",
            "status": "待审核",
        }
        existing = {"fields": {
            "复刻产品": "WIG-1", "版本类型": "钩子变体", "本条改动": "只改开头",
            "完整提示词": "完整内容", "状态": "待审核",
        }}
        client = FakeFeishu([existing])
        FeishuPromptWriter(client, "app", "table").apply("create_prompt_row", payload)
        self.assertEqual(client.created, [])

    def test_optional_count_uses_default_or_validated_override(self):
        self.assertIsNone(_optional_count(None))
        self.assertIsNone(_optional_count(""))
        self.assertEqual(_optional_count(10.0), 10)
        for bad in (0, 21, 1.5, "abc", True):
            with self.assertRaises(ValueError):
                _optional_count(bad)


if __name__ == "__main__":
    unittest.main()
