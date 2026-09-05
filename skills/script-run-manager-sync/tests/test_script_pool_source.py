import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from core.bitable import TableRecord
from core.original_batch_source import build_original_batch_sync_tasks, resolve_original_batch_field_mapping
from core.sync import TARGET_FIELD_ALIASES, build_target_fields, resolve_field_mapping, task_script_type
from run_pipeline import (
    _main_with_lock, build_existing_target_updates, can_update_existing_target,
    frozen_pool_target_conflict, validate_pool_target_mapping,
    register_pool_task_metadata,
)


class ScriptPoolSourceTest(unittest.TestCase):
    def row(self, **overrides):
        fields = {
            "脚本ID": "wsr_123", "脚本来源": "成功脚本复刻", "产品编码": "S260724029604",
            "发布用途": "带货", "是否挂车": "是", "进入生产": True,
            "短视频提示词": "原始最终提示词", "产品图片": [{"file_token": "product"}],
            "人物参考图（系统）": [{"file_token": "face"}], "口播_目标语言": "Mira este cambio.",
            "口播_中文": "看看这个变化。", "目标语言": "西语", "目标国家": "墨西哥",
        }
        fields.update(overrides)
        return TableRecord("recPool", fields)

    def task(self, **overrides):
        row = self.row(**overrides)
        return build_original_batch_sync_tasks([row], resolve_original_batch_field_mapping(row.fields))[0]

    def target_mapping(self):
        return resolve_field_mapping([aliases[0] for aliases in TARGET_FIELD_ALIASES.values()], TARGET_FIELD_ALIASES)

    def test_four_cart_purpose_combinations_are_independent(self):
        for purpose in ("带货", "养号"):
            for cart in ("是", "否"):
                with self.subTest(purpose=purpose, cart=cart):
                    task = self.task(**{"发布用途": purpose, "是否挂车": cart})
                    self.assertEqual(task.publish_purpose, purpose)
                    self.assertEqual(task.cart_enabled, cart)
                    self.assertEqual(task.script_source, "成功脚本复刻")
                    fields = build_target_fields(task, self.target_mapping())
                    self.assertEqual(fields["是否挂车"], cart)
                    self.assertEqual(fields["全球产品ID"], "S260724029604")
                    self.assertIsNone(fields["产品ID"])
        self.assertEqual(self.task(**{"发布用途": "养号", "是否挂车": ""}).cart_enabled, "否")

    def test_no_product_and_no_images_nurture_is_valid(self):
        task = self.task(**{"发布用途": "养号", "是否挂车": "否", "产品编码": "", "产品图片": [], "人物参考图（系统）": []})
        self.assertEqual(task.task_name, task.script_id)
        self.assertEqual(task_script_type(task), "养号脚本")
        self.assertEqual(build_target_fields(task, self.target_mapping())["免参考图"], "是")

    def test_source_copy_and_reference_roles_survive(self):
        task = self.task()
        self.assertEqual(task_script_type(task), "短视频复刻脚本")
        self.assertEqual([image["file_token"] for image in task.reference_images], ["face", "product"])
        self.assertEqual(json.loads(task.persona_contract)["reference_assets"], [
            {"index": 1, "role": "person_identity"}, {"index": 2, "role": "product"},
        ])
        plan = json.loads(task.voiceover_execution_plan)
        self.assertEqual(plan["mode"], "PRESERVE_SOURCE_COPY")
        self.assertEqual(plan["target_text"], "Mira este cambio.")
        self.assertTrue(task.voiceover_requested)
        fields = build_target_fields(task, self.target_mapping())
        self.assertEqual(fields["提示词"], "【脚本ID】\n- wsr_123\n\n原始最终提示词")
        self.assertNotIn("翻译", fields["提示词"])

    def test_music_only_explicitly_disables_voiceover(self):
        task = self.task(**{"口播_目标语言": "纯音乐"})
        fields = build_target_fields(task, self.target_mapping())
        self.assertFalse(fields["是否配口播"])
        self.assertIsNone(fields["口播执行计划"])
        self.assertIsNone(fields["口播状态"])

    def test_unknown_or_missing_source_is_not_original(self):
        for changes in ({"脚本来源": "未来未知来源"}, {"脚本来源": ""}, {"脚本ID": "original_1"}):
            with self.subTest(changes=changes), self.assertRaisesRegex(ValueError, "SCRIPT_POOL_SOURCE"):
                self.task(**changes)

    def test_missing_policy_schema_is_reported_per_row(self):
        row = self.row()
        row.fields.pop("是否挂车")
        errors = {}
        tasks = build_original_batch_sync_tasks([row], resolve_original_batch_field_mapping(row.fields), errors=errors)
        self.assertEqual(tasks, [])
        self.assertIn("SCRIPT_POOL_FIELD_MISSING", errors[row.record_id])

    def test_known_sources_have_explicit_routing(self):
        for source in ("原创生成", "视频复刻", "人工编写"):
            task = self.task(**{"脚本来源": source, "脚本ID": "custom_123"})
            self.assertTrue(task.script_pool_entry)
            self.assertEqual(task.script_source, source)
        with self.assertRaisesRegex(ValueError, "SEEDING_CART_GUARD"):
            self.task(**{"发布用途": "种草", "是否挂车": "是"})

    def test_pool_canonical_identity_never_uses_batch_item_id(self):
        task = self.task(**{"批次ItemID": "wsr:123"})
        self.assertEqual(task.internal_script_key, task.script_id)
        self.assertEqual(build_target_fields(task, self.target_mapping())["内部脚本键"], "wsr_123")

    def test_management_title_is_not_a_publication_title(self):
        task = self.task(**{"脚本标题": "墨西哥｜养号｜一般复刻G2"})
        self.assertEqual(task.direction_label, "墨西哥｜养号｜一般复刻G2")
        self.assertEqual(task.short_video_title, "")
        task = self.task(**{"脚本标题": "中文管理名", "发布标题": "Un cambio de look"})
        self.assertEqual(task.short_video_title, "Un cambio de look")

    def test_cancellation_clears_existing_platform_product_and_voiceover(self):
        task = self.task(**{"是否挂车": "否", "口播_目标语言": ""})
        mapping = self.target_mapping()
        existing = TableRecord("recTarget", {"任务状态": "待处理", "产品ID": "1723456789012", "是否挂车": "是", "是否配口播": True})
        updates = build_existing_target_updates(existing, build_target_fields(task, mapping), mapping, allow_full_patch=True)
        self.assertIsNone(updates["产品ID"])
        self.assertEqual(updates["是否挂车"], "否")
        self.assertFalse(updates["是否配口播"])
        self.assertIsNone(updates["口播执行计划"])

    def test_submitted_or_scheduled_record_is_never_patched(self):
        task = self.task(**{"是否挂车": "否"})
        mapping = self.target_mapping()
        fields = build_target_fields(task, mapping)
        for status in ("已排期", "已提交", "处理中", "已完成"):
            existing = TableRecord("recTarget", {"任务状态": status, "是否挂车": "是"})
            self.assertFalse(can_update_existing_target(existing, mapping))
            self.assertEqual(build_existing_target_updates(existing, fields, mapping, allow_full_patch=True), {})
            self.assertIn("FROZEN", frozen_pool_target_conflict(task, existing, fields, mapping))

    def test_target_missing_audio_contract_is_not_silently_ignored(self):
        mapping = self.target_mapping()
        mapping["voiceover_execution_plan"] = None
        with self.assertRaisesRegex(ValueError, "TARGET_FIELDS_MISSING"):
            validate_pool_target_mapping(self.task(), mapping)

    def test_dry_run_never_changes_schema_or_registers_metadata(self):
        source, target = Mock(), Mock()
        source.request_count = target.request_count = 0
        source.list_field_names.return_value = list(self.row().fields)
        source.list_records.return_value = [self.row()]
        target.list_field_names.return_value = [aliases[0] for aliases in TARGET_FIELD_ALIASES.values()]
        target.list_records.return_value = []
        args = SimpleNamespace(mode="manual", source_kind="original-batch", include_publish_metadata=False,
            source_feishu_url=None, target_feishu_url="target", metadata_db_path="/nonexistent/test.sqlite",
            record_id=None, product_code=None, limit=20, dry_run=True, batch_size=100)
        with patch("run_pipeline.resolve_feishu_config", return_value=("app", "table")), \
             patch("run_pipeline.FeishuBitableClient", side_effect=[source, target]), \
             patch("run_pipeline.register_pool_task_metadata") as register, \
             patch("run_pipeline.ensure_target_default_fields") as ensure:
            _main_with_lock(args)
        ensure.assert_not_called()
        register.assert_not_called()
        target.batch_create_records.assert_not_called()
        target.update_record_fields.assert_not_called()
        source.update_record_fields.assert_not_called()

    def test_frozen_publish_metadata_blocks_target_write_and_keeps_selection(self):
        source, target = Mock(), Mock()
        source.request_count = target.request_count = 0
        row = self.row(**{"同步结果": "", "处理状态": "待审核"})
        source.list_field_names.return_value = list(row.fields)
        source.list_records.return_value = [row]
        names = [aliases[0] for aliases in TARGET_FIELD_ALIASES.values()]
        target.list_field_names.return_value = names
        target.list_records.return_value = []
        args = SimpleNamespace(mode="manual", source_kind="original-batch", include_publish_metadata=False,
            source_feishu_url=None, target_feishu_url="target", metadata_db_path="/nonexistent/test.sqlite",
            record_id=None, product_code=None, limit=20, dry_run=False, batch_size=100)
        with patch("run_pipeline.resolve_feishu_config", return_value=("app", "table")), \
             patch("run_pipeline.FeishuBitableClient", side_effect=[source, target]), \
             patch("run_pipeline.register_pool_task_metadata", return_value={"status": "frozen"}), \
             patch("run_pipeline.ensure_target_default_fields", return_value=names):
            _main_with_lock(args)
        target.batch_create_records.assert_not_called()
        target.update_record_fields.assert_not_called()
        result = source.update_record_fields.call_args.args[1]
        self.assertIn("FROZEN", result["同步结果"])
        self.assertEqual(result["处理状态"], "同步失败")
        self.assertNotIn("进入生产", result)

    def test_publish_db_registration_keeps_internal_sku_and_preserves_mapping(self):
        with tempfile.TemporaryDirectory(prefix="script-pool-test-") as directory:
            db_path = str(Path(directory) / "publisher.sqlite3")
            task = self.task()
            first = register_pool_task_metadata(task, db_path)
            self.assertEqual(first["status"], "created")
            self.assertEqual(first["platform_product_id"], "")
            with sqlite3.connect(db_path) as conn:
                conn.execute("UPDATE script_pool_bindings SET platform_product_id = ?", ("17234567890123",))
            second = register_pool_task_metadata(task, db_path)
            self.assertEqual(second["platform_product_id"], "17234567890123")
            task.cart_enabled = "否"
            cleared = register_pool_task_metadata(task, db_path)
            self.assertEqual(cleared["platform_product_id"], "")
            with sqlite3.connect(db_path) as conn:
                metadata = conn.execute("SELECT product_id, cart_enabled FROM script_metadata").fetchone()
            self.assertEqual(metadata, ("S260724029604", "否"))


if __name__ == "__main__":
    unittest.main()
