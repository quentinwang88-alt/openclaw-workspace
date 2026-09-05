from contextlib import redirect_stdout
import io
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from core.bitable import TableRecord
from core.first_frame_handoff import (
    PROTOCOL, generate_first_frame_from_snapshot,
    complete_wsr_reference_handoff, apply_composite_reference_handoff,
)
from core.original_batch_source import build_original_batch_sync_tasks, resolve_original_batch_field_mapping
from core.sync import TARGET_FIELD_ALIASES, build_target_fields, resolve_field_mapping
from run_pipeline import _main_with_lock, process_lock


def row(record_id="recFirst", **updates):
    fields = {
        "脚本ID": "wsr_123", "脚本来源": "成功脚本复刻", "产品编码": "S260",
        "发布用途": "养号", "是否挂车": "否", "进入生产": True,
        "生成首帧（需勾选）": True, "统一首帧（系统）": [], "首帧准备状态（系统）": "",
        "视觉参考模式（系统）": "", "视频形态（系统）": "15秒原创",
        "短视频提示词": "前两张是人物，后两张是产品。保留原镜头、口播。",
        "人物参考图（系统）": [{"file_token": f"person{i}"} for i in range(4)],
        "产品图片": [{"file_token": f"product{i}"} for i in range(2)],
        "同步结果": "", "同步时间": None, "处理状态": "待审核", "运行任务ID": "",
    }
    fields.update(updates)
    return TableRecord(record_id, fields)


def ready():
    return {"统一首帧（系统）": [{"file_token": "opening"}],
            "首帧准备状态（系统）": "已就绪", "视觉参考模式（系统）": "USER_SELECTED_FIRST_FRAME"}


class FirstFrameSnapshotProtocolTest(unittest.TestCase):
    def invoke(self, result=None, returncode=0):
        record = row()
        output = result or {"protocol": PROTOCOL, "record_id": record.record_id,
                            "script_id": "wsr_123", "status": "ready", "fields": ready()}

        def worker(command, **kwargs):
            self.assertNotIn("--record-id", command)
            request = json.loads(Path(command[command.index("--record-snapshot") + 1]).read_text())
            self.assertEqual(request["record"]["fields"], record.fields)
            self.assertEqual(request["source_url"], "source-url")
            self.assertEqual(request["protocol"], PROTOCOL)
            Path(command[command.index("--result-json") + 1]).write_text(json.dumps(output))
            return SimpleNamespace(returncode=returncode)

        with patch("core.first_frame_handoff.subprocess.run", side_effect=worker) as command:
            actual = generate_first_frame_from_snapshot(record, "source-url")
            command.assert_called_once()
            return actual

    def test_exact_snapshot_and_ready_patch(self):
        self.assertEqual(self.invoke(), ready())

    def test_result_cannot_clear_checkbox_or_rewrite_source_script(self):
        self.assertEqual(self.invoke({"protocol": PROTOCOL, "record_id": "recFirst",
            "script_id": "wsr_123", "status": "ready", "fields": {
                **ready(), "进入生产": False, "短视频提示词": "bad"}}), ready())

    def test_wrong_script_identity_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "IDENTITY_MISMATCH"):
            self.invoke({"protocol": PROTOCOL, "record_id": "recFirst", "script_id": "other",
                         "status": "ready", "fields": ready()})

    def test_failure_json_keeps_clear_reason_without_auto_retry(self):
        with self.assertRaisesRegex(RuntimeError, "FIRST_FRAME_FAILED:图片生成失败"):
            self.invoke({"protocol": PROTOCOL, "record_id": "recFirst", "script_id": "wsr_123",
                         "status": "failed", "error": "图片生成失败"}, returncode=1)

    def test_ready_without_attachment_is_not_success(self):
        with self.assertRaisesRegex(RuntimeError, "RESULT_NOT_READY"):
            self.invoke({"protocol": PROTOCOL, "record_id": "recFirst", "script_id": "wsr_123",
                         "status": "ready", "fields": {"首帧准备状态（系统）": "已就绪"}})


class FirstFrameSyncTest(unittest.TestCase):
    def run_sync(self, records, *, dry_run=False, failure=None, target_records=None, record_id=None):
        source, target = Mock(), Mock()
        source.request_count = target.request_count = 0
        source.list_field_names.return_value = sorted({key for record in records for key in record.fields})
        source.list_records.return_value = records
        source.get_record.return_value = records[0]
        names = [aliases[0] for aliases in TARGET_FIELD_ALIASES.values()]
        target.list_field_names.return_value = names
        target.list_records.return_value = target_records or []
        target.batch_create_records.return_value = ["recRun"]
        args = SimpleNamespace(mode="scheduled", source_kind="original-batch", include_publish_metadata=False,
            source_feishu_url="source-url", target_feishu_url="target-url", metadata_db_path="/nonexistent/test.sqlite",
            record_id=record_id, product_code=None, limit=20, dry_run=dry_run, batch_size=100)
        stdout = io.StringIO()
        def transfer(_source, _target, images, cache):
            for image in images:
                cache[image["file_token"]] = {**image, "_transfer_sha256": hashlib.sha256(image["file_token"].encode()).hexdigest()}
            return images
        with patch("run_pipeline.resolve_feishu_config", return_value=("app", "table")), \
             patch("run_pipeline.FeishuBitableClient", side_effect=[source, target]), \
             patch("run_pipeline.ensure_target_default_fields", return_value=names) as schema, \
             patch("run_pipeline.register_pool_task_metadata", return_value={"status": "created", "canonical_script_key": "wsr_123"}) as register, \
             patch("run_pipeline.transfer_reference_images", side_effect=transfer), \
             patch("run_pipeline.load_frozen_wsr_context", return_value={"provenance": "final_prompt_only"}), \
             patch("run_pipeline.generate_first_frame_from_snapshot", return_value=ready(), side_effect=failure) as generate, \
             redirect_stdout(stdout):
            _main_with_lock(args)
        return SimpleNamespace(source=source, target=target, generate=generate, register=register, schema=schema, stdout=stdout.getvalue())

    def test_selected_frame_is_generated_and_synced_in_same_scan(self):
        record = row()
        original_prompt = record.fields["短视频提示词"]
        result = self.run_sync([record])
        result.generate.assert_called_once_with(record, "source-url")
        result.source.list_records.assert_called_once_with(page_size=500)
        result.source.get_record.assert_not_called()
        result.target.list_records.assert_called_once_with(page_size=500)
        fields = result.target.batch_create_records.call_args.args[0][0]["fields"]
        self.assertEqual([asset["file_token"] for asset in fields["参考图"]],
                         ["opening", "person0", "person1", "person2", "person3", "product0", "product1"])
        roles = json.loads(fields["人物模板合同"])["reference_assets"]
        self.assertEqual([asset["role"] for asset in roles], ["composite_first_frame"] + ["person_identity"] * 4 + ["product"] * 2)
        self.assertTrue(fields["提示词"].startswith("【当前实际附件角色说明"))
        self.assertIn("附件 7：商品外观", fields["提示词"])
        self.assertIn(original_prompt, fields["提示词"])
        self.assertEqual(record.fields["短视频提示词"], original_prompt)
        self.assertFalse(result.source.update_record_fields.call_args.args[1]["进入生产"])

    def test_check_only_reports_wait_and_does_not_generate_or_write(self):
        result = self.run_sync([row()], dry_run=True)
        self.assertIn("action=wait(first_frame", result.stdout)
        self.assertNotIn("action=create", result.stdout)
        for call in (result.generate, result.register, result.schema,
                     result.source.update_record_fields, result.target.batch_create_records):
            call.assert_not_called()

    def test_check_unsupported_new_source_is_explicit(self):
        result = self.run_sync([row(**{"脚本来源": "人工编写", "脚本ID": "manual_1"})], dry_run=True)
        self.assertIn("unsupported(first_frame_source=人工编写)", result.stdout)
        result.generate.assert_not_called()

    def test_failed_image_keeps_selection_and_never_writes_target(self):
        result = self.run_sync([row()], failure=RuntimeError("FIRST_FRAME_FAILED:原图无法下载"))
        result.target.batch_create_records.assert_not_called()
        result.register.assert_not_called()
        failure_fields = result.source.update_record_fields.call_args.args[1]
        self.assertIn("原图无法下载", failure_fields["同步结果"])
        self.assertNotIn("进入生产", failure_fields)

    def test_ready_rows_are_verified_and_unchecked_rows_are_ignored(self):
        records = [row(**ready()), row("recOther", **{"进入生产": False})]
        result = self.run_sync(records)
        result.generate.assert_called_once()
        result.target.batch_create_records.assert_called_once()

    def test_longform_rows_do_not_reach_shortform_first_frame(self):
        result = self.run_sync([row(**{"视频形态（系统）": "长视频"})])
        result.generate.assert_not_called()
        result.target.list_records.assert_not_called()
        result.target.batch_create_records.assert_not_called()

    def test_record_selector_stays_record_scoped(self):
        result = self.run_sync([row()], record_id="recFirst")
        result.source.get_record.assert_called_once_with("recFirst")
        result.source.list_records.assert_not_called()
        result.generate.assert_called_once()

    def test_submitted_task_does_not_spend_on_new_frame(self):
        existing = TableRecord("recSubmitted", {"脚本ID": "wsr_123", "任务状态": "已提交"})
        result = self.run_sync([row()], target_records=[existing])
        result.generate.assert_not_called()
        result.target.batch_create_records.assert_not_called()
        result.target.update_record_fields.assert_not_called()
        self.assertIn("EXECUTION_FROZEN", result.source.update_record_fields.call_args.args[1]["同步结果"])

    def test_retry_after_target_creation_neither_regenerates_nor_duplicates(self):
        record = row(**ready())
        existing = TableRecord("recExisting", {"脚本ID": "wsr_123", "任务状态": "待处理"})
        result = self.run_sync([record], target_records=[existing])
        result.generate.assert_called_once()
        result.target.batch_create_records.assert_not_called()
        result.target.update_record_fields.assert_called_once()
        self.assertFalse(result.source.update_record_fields.call_args.args[1]["进入生产"])

    def test_failed_first_row_does_not_block_next_ready_row(self):
        records = [row(), row("recReady", **{**ready(), "脚本ID": "wsr_next"})]
        result = self.run_sync(records, failure=[RuntimeError("FIRST_FRAME_FAILED:首帧失败"), ready()])
        self.assertEqual(result.generate.call_count, 2)
        created = result.target.batch_create_records.call_args.args[0][0]["fields"]
        self.assertEqual(created["脚本ID"], "wsr_next")
        writes = {call.args[0]: call.args[1] for call in result.source.update_record_fields.call_args_list}
        self.assertNotIn("进入生产", writes["recFirst"])
        self.assertFalse(writes["recReady"]["进入生产"])

    def test_unsupported_source_does_not_call_paid_worker(self):
        result = self.run_sync([row(**{"脚本来源": "视频复刻", "脚本ID": "video_1"})])
        result.generate.assert_not_called()
        result.target.batch_create_records.assert_not_called()
        self.assertIn("SOURCE_UNSUPPORTED", result.source.update_record_fields.call_args.args[1]["同步结果"])

    def test_nurture_without_product_keeps_person_and_opening(self):
        result = self.run_sync([row(**{"产品编码": "", "产品图片": []})])
        created = result.target.batch_create_records.call_args.args[0][0]["fields"]
        self.assertEqual(len(created["参考图"]), 5)
        self.assertNotIn("product", [asset["role"] for asset in json.loads(created["人物模板合同"])["reference_assets"]])

    def test_lock_protects_entire_sync_process(self):
        with tempfile.TemporaryDirectory() as directory:
            lock = str(Path(directory) / "sync.lock")
            with process_lock(lock):
                with self.assertRaisesRegex(RuntimeError, "已在运行"):
                    with process_lock(lock):
                        self.fail("concurrent generation must not start")
            with process_lock(lock):
                pass

    def test_legacy_original_still_uses_only_ready_first_frame(self):
        record = row(**{**ready(), "脚本ID": "SCSCRIPT_ABC", "脚本来源": "原创脚本"})
        mapping = resolve_original_batch_field_mapping(record.fields)
        task = build_original_batch_sync_tasks([record], mapping)[0]
        task = complete_wsr_reference_handoff(task, record, mapping)
        self.assertEqual(task.reference_images, [{"file_token": "opening"}])
        target_mapping = resolve_field_mapping([v[0] for v in TARGET_FIELD_ALIASES.values()], TARGET_FIELD_ALIASES)
        fields = build_target_fields(task, target_mapping)
        self.assertEqual(apply_composite_reference_handoff(task, fields, target_mapping), fields)


if __name__ == "__main__":
    unittest.main()
