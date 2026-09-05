from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch


SCRIPT_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from production_runtime import (  # noqa: E402
    FeishuPromptWriter,
    FeishuTaskRunner,
    _notes_parts,
    _split_special_requirements,
    _optional_count,
    _relation_ids,
    MotherStatus,
    DeferredModelClient,
    build_application,
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
    def validation_runner(self, **overrides):
        fields = {
            "母版名称": "养号母版", "成功脚本": "完整养号原稿", "验证说明": "",
            "来源产品": ["recSource"], "待复刻产品": ["recTarget"],
            "人物脸部参考图": [{"file_token": "face"}], "发布用途": "养号", "每产品生成数": 3,
            **overrides,
        }
        client = FakeFeishu([{"record_id": "recA", "fields": fields}])
        mother = SimpleNamespace(status=MotherStatus.CONFIRMED, mother_id="mother", version=1,
                                 contract=SimpleNamespace(human_summary="原稿摘要"))
        mother_service = SimpleNamespace(process=Mock(return_value=SimpleNamespace(record=mother, created=True)))
        runner = FeishuTaskRunner(
            client=client, app_token="app", mother_table="mother", product_table="product",
            repository=object(), mother_service=mother_service, replication_service=object(), outbox_service=object(),
        )
        runner._product_row = Mock(return_value={"record_id": "recSource"})
        runner._prepare_product = Mock(return_value=(SimpleNamespace(product_id="source", fact=object()), ["data:image/png;base64,fake"]))
        runner._generate = Mock(return_value={"status": "test-generated"})
        return runner, fields, mother_service

    def test_one_click_accepts_empty_validation_without_inventing_evidence(self):
        runner, fields, mother_service = self.validation_runner()
        self.assertEqual(runner._one_click("recA"), {"status": "test-generated"})
        self.assertEqual(mother_service.process.call_args.args[0].validation_notes, "")
        self.assertEqual(fields["验证说明"], "")
        runner._generate.assert_called_once_with("recA")

    def test_process_mother_accepts_empty_validation_without_inventing_evidence(self):
        runner, fields, mother_service = self.validation_runner()
        self.assertEqual(runner._process_mother("recA")["status"], "待确认")
        self.assertEqual(mother_service.process.call_args.args[0].validation_notes, "")
        self.assertEqual(fields["验证说明"], "")

    def test_optional_validation_preserves_supplied_notes_in_both_entrypoints(self):
        for action in ("_one_click", "_process_mother"):
            with self.subTest(action=action):
                runner, fields, mother_service = self.validation_runner(**{"验证说明": "运营手动记录的原始说明"})
                getattr(runner, action)("recA")
                self.assertEqual(mother_service.process.call_args.args[0].validation_notes, "运营手动记录的原始说明")
                self.assertEqual(fields["验证说明"], "运营手动记录的原始说明")

    def test_other_required_mother_fields_still_block_both_entrypoints(self):
        for action in ("_one_click", "_process_mother"):
            for key, value in (("母版名称", ""), ("成功脚本", ""), ("来源产品", []),
                               ("来源产品", ["recSource1", "recSource2"])):
                with self.subTest(action=action, field=key, value=value):
                    runner, _, mother_service = self.validation_runner(**{key: value})
                    with self.assertRaisesRegex(ValueError, "母版名称、成功脚本.*唯一来源产品"):
                        getattr(runner, action)("recA")
                    mother_service.process.assert_not_called()
                    runner._prepare_product.assert_not_called()

    def test_ordinary_special_requirements_do_not_reparse_the_mother(self):
        for action in ("_one_click", "_process_mother"):
            runner, fields, service = self.validation_runner(**{"特殊要求": "本条改成单掌遮镜\n保留原口播"})
            getattr(runner, action)("recA")
            self.assertEqual(service.process.call_args.args[0].human_notes, "")
            self.assertEqual(_split_special_requirements(fields["特殊要求"])[1], fields["特殊要求"])

    def test_only_explicit_mother_notes_enter_the_mother_input(self):
        runner, _, service = self.validation_runner(**{"特殊要求": "母版要求：完整遮挡后再换发\n本条改成单掌"})
        runner._one_click("recA")
        self.assertEqual(service.process.call_args.args[0].human_notes, "完整遮挡后再换发")
        self.assertEqual(_split_special_requirements("[母版要求]\n保留因果\n[本次要求]\n单掌\n不换妆"),
                         ("保留因果", "单掌\n不换妆"))
        self.assertEqual(_split_special_requirements("母版要求:保留因果\n本次要求:单掌"), ("保留因果", "单掌"))

    def test_one_click_other_generation_requirements_are_unchanged(self):
        for key, value, expected in (
            ("待复刻产品", [], "至少选择一个待复刻产品"),
            ("人物脸部参考图", [], "上传人物脸部参考图"),
            ("发布用途", "", "发布用途必须"),
            ("每产品生成数", 0, "1.*20"),
        ):
            with self.subTest(field=key):
                runner, _, mother_service = self.validation_runner(**{key: value})
                with self.assertRaisesRegex(ValueError, expected):
                    runner._one_click("recA")
                mother_service.process.assert_not_called()

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

    def test_writeback_application_does_not_require_model_credentials(self):
        with patch("production_runtime._feishu_credentials", return_value=("fake", "fake")), \
             patch("production_runtime._database_url", return_value="mysql://fake:fake@offline/test"), \
             patch("production_runtime._openai_client", side_effect=AssertionError("must not read model auth")):
            app = build_application()
            self.assertIsNotNone(app.outbox_service)
            self.assertIsInstance(app._runner.replication_service.llm, DeferredModelClient)

    def test_model_client_is_initialized_once_only_on_first_generation(self):
        model = Mock()
        factory = Mock(return_value=model)
        deferred = DeferredModelClient(factory)
        factory.assert_not_called()
        deferred.call(task_type="MOTHER_ANALYZE")
        deferred.call(task_type="REPLICATION_COMPILE")
        factory.assert_called_once_with()
        self.assertEqual(model.call.call_count, 2)


if __name__ == "__main__":
    unittest.main()
