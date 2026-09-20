import unittest
from unittest.mock import patch

import core.production_script_feishu as production_feishu
from core.bitable import TaskRecord
from core.production_script_feishu import (
    OPERATION_TASK_STATUS_OPTIONS,
    OUTFIT_SCENE_MATCH_OPTIONS,
    FIRST_FRAME_STATUS_OPTIONS,
    PRODUCT_TYPE_OPTIONS,
    TEST_PHASE_OPTIONS,
    TOP_CATEGORY_OPTIONS,
    VIDEO_SPEC_OPTIONS,
    LONGFORM_SCENE_MODE_OPTIONS,
    _records_by_batch_item_id,
    export_ready_batch,
    operation_record_values,
    projection_to_feishu_fields,
)


class ProductionScriptFeishuTest(unittest.TestCase):
    def test_workbench_enum_scope(self):
        self.assertEqual(("女装", "配饰"), TOP_CATEGORY_OPTIONS)
        self.assertEqual(("初测", "复测", "终测", "放大观察"), TEST_PHASE_OPTIONS)
        self.assertEqual("待执行", OPERATION_TASK_STATUS_OPTIONS[0])
        self.assertIn("已完成", OPERATION_TASK_STATUS_OPTIONS)
        self.assertEqual("15秒原创", VIDEO_SPEC_OPTIONS[0])
        self.assertIn("45秒", VIDEO_SPEC_OPTIONS)
        self.assertEqual(("自动", "单场景", "双场景"), LONGFORM_SCENE_MODE_OPTIONS)
        self.assertEqual("用户未选择", FIRST_FRAME_STATUS_OPTIONS[0])
        self.assertIn("已就绪", FIRST_FRAME_STATUS_OPTIONS)
        self.assertEqual(
            ("已匹配", "未配置偏好", "已回退", "不适用"),
            OUTFIT_SCENE_MATCH_OPTIONS,
        )
        for value in (
            "外套", "上衣", "连衣裙", "耳饰", "发饰", "围巾",
            "秋冬围巾", "丝巾", "头巾", "帽子",
        ):
            self.assertIn(value, PRODUCT_TYPE_OPTIONS)

    def test_legacy_aliases_normalize_to_enum_values(self):
        record = TaskRecord(
            record_id="rec1",
            fields={
                "一级类目（需填写）": "服饰",
                "产品类型（需填写）": "T恤",
            },
        )
        task = operation_record_values(record)
        self.assertEqual("女装", task["top_category"])
        self.assertEqual("上衣", task["product_type"])

    def test_chinese_test_phase_maps_to_internal_code(self):
        record = TaskRecord(
            record_id="rec-phase",
            fields={"测试阶段（可选，默认初测）": "复测"},
        )
        self.assertEqual("RETEST", operation_record_values(record)["test_phase"])

    def test_blank_test_phase_defaults_to_initial(self):
        record = TaskRecord(record_id="rec-default", fields={})
        self.assertEqual("INITIAL", operation_record_values(record)["test_phase"])

    def test_video_spec_routes_longform_without_using_legacy_number(self):
        record = TaskRecord(
            record_id="rec-longform",
            fields={
                "视频规格（需填写）": "35秒",
                "视频时长": 15,
                "长视频场景模式（可选）": "双场景",
            },
        )
        task = operation_record_values(record)
        self.assertEqual("35秒", task["video_spec"])
        self.assertEqual(35.0, task["duration_seconds"])
        self.assertEqual("LONGFORM", task["video_format"])
        self.assertEqual("multi", task["longform_scene_mode"])

    def test_blank_video_spec_keeps_legacy_15_second_task_compatible(self):
        record = TaskRecord(record_id="rec-short", fields={"视频时长": 15})
        task = operation_record_values(record)
        self.assertEqual("15秒原创", task["video_spec"])
        self.assertEqual("SHORT_15S", task["video_format"])

    def test_accessory_alias_normalizes_to_registered_display_name(self):
        record = TaskRecord(
            record_id="rec2",
            fields={
                "一级类目（需填写）": "饰品",
                "产品类型（需填写）": "耳线",
            },
        )
        task = operation_record_values(record)
        self.assertEqual("配饰", task["top_category"])
        self.assertEqual("耳饰", task["product_type"])

    def test_batch_item_identity_uses_first_row_as_canonical(self):
        first = TaskRecord(
            record_id="rec-first",
            fields={"批次ID": "BATCH_1", "批次ItemID": "ITEM_1"},
        )
        duplicate = TaskRecord(
            record_id="rec-duplicate",
            fields={"批次ID": "BATCH_1", "批次ItemID": "ITEM_1"},
        )
        indexed = _records_by_batch_item_id([first, duplicate])
        self.assertEqual("rec-first", indexed[("BATCH_1", "ITEM_1")].record_id)

    def test_projection_exports_human_readable_outfit_metadata(self):
        fields = projection_to_feishu_fields(
            {
                "script_id": "S1",
                "outfit_template_id": "STYLE_001",
                "outfit_template_name": "城市轻通勤穿搭",
                "outfit_accessories": "小号肩包；细金属耳环",
                "outfit_scene_match": "已匹配",
                "outfit_scene_contract_json": '{"match_status":"MATCHED"}',
            },
            include_workflow_defaults=False,
        )
        self.assertEqual(fields["穿搭模板名称（系统）"], "城市轻通勤穿搭")
        self.assertEqual(fields["实际配饰（系统）"], "小号肩包；细金属耳环")
        self.assertEqual(fields["穿搭场景匹配（系统）"], "已匹配")
        self.assertIn("MATCHED", fields["穿搭场景关联合同_JSON（系统）"])

    def test_export_updates_frozen_batch_item_when_script_id_changes(self):
        class FakeClient:
            def __init__(self):
                self.updated = []
                self.created = []

            def list_records(self, *, page_size):
                self.page_size = page_size
                return [
                    TaskRecord(
                        record_id="rec-existing",
                        fields={
                            "脚本ID": "OLD_SCRIPT_ID",
                            "批次ID": "BATCH_1",
                            "批次ItemID": "ITEM_1",
                            "处理状态": "已审核",
                            "进入生产": True,
                        },
                    )
                ]

            def update_record_fields(self, record_id, fields):
                self.updated.append((record_id, fields))

            def batch_create_records(self, records):
                self.created.extend(records)
                return ["new-record"] * len(records)

        projection = {
            "script_id": "NEW_SCRIPT_ID",
            "product_code": "1730000000000000000",
            "batch_id": "BATCH_1",
            "batch_item_id": "ITEM_1",
            "item_index": 1,
            "script_title": "test",
            "duration_seconds": 15,
            "processing_status": "待审核",
        }
        item = type("Item", (), {"status": "SCRIPT_READY"})()
        client = FakeClient()
        with patch.object(
            production_feishu,
            "build_production_projection",
            return_value=projection,
        ):
            result = export_ready_batch(
                batch=object(),
                items=[item],
                target_client=client,
            )

        # The counters added by the execution-validation gate *and* by the
        # delivery-snapshot gate are part of the returned summary now.  Both are
        # zero here for the same reason: this projection carries no audit and no
        # necklace contract, so there is nothing to block and nothing to record.
        # The workflow columns must still be untouched, because a projection
        # without an audit is not a conflict.
        self.assertEqual(
            {
                "created": 0,
                "updated": 1,
                "skipped": 0,
                "execution_blocked": 0,
                "snapshot_written": 0,
                "snapshot_blocked": 0,
                "snapshot_errors": {},
                "render_validation_schema": "mixed-render-validation-v1",
            },
            result,
        )
        self.assertEqual([], client.created)
        self.assertEqual("rec-existing", client.updated[0][0])
        fields = client.updated[0][1]
        self.assertEqual("NEW_SCRIPT_ID", fields["脚本ID"])
        self.assertNotIn("处理状态", fields)
        self.assertNotIn("进入生产", fields)

    def _export_with_validation(self, validation, *, existing_record):
        """Run the hand-off with one ready item and a given audit result."""

        class FakeClient:
            def __init__(self):
                self.updated = []
                self.created = []

            def list_records(self, *, page_size):
                return [existing_record] if existing_record else []

            def update_record_fields(self, record_id, fields):
                self.updated.append((record_id, fields))

            def batch_create_records(self, records):
                self.created.extend(records)
                return ["new-record"] * len(records)

        projection = {
            "script_id": "SCRIPT_CONFLICT",
            "product_code": "1730000000000000000",
            "batch_id": "BATCH_1",
            "batch_item_id": "ITEM_1",
            "item_index": 1,
            "script_title": "test",
            "duration_seconds": 15,
            "processing_status": "待审核",
            "render_validation": validation,
        }
        item = type("Item", (), {"status": "SCRIPT_READY"})()
        client = FakeClient()
        with patch.object(
            production_feishu,
            "build_production_projection",
            return_value=projection,
        ):
            result = export_ready_batch(
                batch=object(), items=[item], target_client=client
            )
        return result, client

    def test_execution_conflict_forces_production_off_on_existing_row(self):
        """A hard conflict must not stay enabled for production.

        The row already has 进入生产=True (a human turned it on).  A delivered
        prompt that contradicts its own frozen contract cannot be repaired
        downstream, so the hand-off has to switch it back off and say why.
        """

        existing = TaskRecord(
            record_id="rec-enabled",
            fields={
                "脚本ID": "SCRIPT_CONFLICT",
                "批次ID": "BATCH_1",
                "批次ItemID": "ITEM_1",
                "处理状态": "已审核",
                "进入生产": True,
            },
        )
        result, client = self._export_with_validation(
            {
                "status": "FAIL",
                "renderer_version": "production-script-renderer-v4",
                "prompt_hash": "abc123",
                "issues": [
                    {
                        "shot_id": 2,
                        "module": "HANDHELD_PRODUCT",
                        "code": "MIXED_EXECUTION_ACTION_BOUNDARY",
                        "source": "RENDERER",
                    }
                ],
            },
            existing_record=existing,
        )

        self.assertEqual(1, result["execution_blocked"])
        self.assertEqual(0, result["created"])
        self.assertEqual(1, result["updated"])
        fields = client.updated[0][1]
        self.assertIs(False, fields["进入生产"])
        self.assertIs(False, fields["生成首帧（需勾选）"])
        self.assertEqual("执行校验未通过", fields["处理状态"])
        self.assertEqual("执行校验未通过", fields["首帧准备状态（系统）"])
        self.assertIn("镜2", fields["审核意见"])
        self.assertIn("MIXED_EXECUTION_ACTION_BOUNDARY", fields["审核意见"])

    def test_execution_conflict_creates_row_with_production_disabled(self):
        """No existing row: the row is still created so the reason is visible,
        but production and the first-frame task are off from the start."""

        result, client = self._export_with_validation(
            {"status": "FAIL", "issues": []}, existing_record=None
        )

        self.assertEqual(1, result["execution_blocked"])
        self.assertEqual(1, result["created"])
        self.assertEqual(1, len(client.created))
        fields = client.created[0]["fields"]
        self.assertIs(False, fields["进入生产"])
        self.assertIs(False, fields["生成首帧（需勾选）"])
        self.assertEqual("执行校验未通过", fields["处理状态"])

    def test_passing_audit_leaves_workflow_columns_to_the_operator(self):
        """PASS must not touch a human's choices — the gate is conflict-only."""

        existing = TaskRecord(
            record_id="rec-enabled",
            fields={
                "脚本ID": "SCRIPT_CONFLICT",
                "批次ID": "BATCH_1",
                "批次ItemID": "ITEM_1",
                "处理状态": "已审核",
                "进入生产": True,
            },
        )
        result, client = self._export_with_validation(
            {"status": "PASS", "issues": []}, existing_record=existing
        )

        self.assertEqual(0, result["execution_blocked"])
        fields = client.updated[0][1]
        self.assertNotIn("进入生产", fields)
        self.assertNotIn("处理状态", fields)
        self.assertNotIn("审核意见", fields)

    def test_audit_error_does_not_block_delivery(self):
        """Our own bookkeeping failing must never stop real content."""

        existing = TaskRecord(
            record_id="rec-enabled",
            fields={
                "脚本ID": "SCRIPT_CONFLICT",
                "批次ID": "BATCH_1",
                "批次ItemID": "ITEM_1",
                "处理状态": "已审核",
                "进入生产": True,
            },
        )
        result, client = self._export_with_validation(
            {"status": "AUDIT_ERROR", "reason": "RuntimeError: boom"},
            existing_record=existing,
        )

        self.assertEqual(0, result["execution_blocked"])
        self.assertNotIn("进入生产", client.updated[0][1])


if __name__ == "__main__":
    unittest.main()
