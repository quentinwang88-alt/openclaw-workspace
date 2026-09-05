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

        self.assertEqual({"created": 0, "updated": 1, "skipped": 0}, result)
        self.assertEqual([], client.created)
        self.assertEqual("rec-existing", client.updated[0][0])
        fields = client.updated[0][1]
        self.assertEqual("NEW_SCRIPT_ID", fields["脚本ID"])
        self.assertNotIn("处理状态", fields)
        self.assertNotIn("进入生产", fields)


if __name__ == "__main__":
    unittest.main()
