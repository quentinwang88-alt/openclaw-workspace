import unittest

from core.bitable import TaskRecord
from core.production_script_feishu import (
    OPERATION_TASK_STATUS_OPTIONS,
    PRODUCT_TYPE_OPTIONS,
    TEST_PHASE_OPTIONS,
    TOP_CATEGORY_OPTIONS,
    operation_record_values,
)


class ProductionScriptFeishuTest(unittest.TestCase):
    def test_workbench_enum_scope(self):
        self.assertEqual(("女装", "配饰"), TOP_CATEGORY_OPTIONS)
        self.assertEqual(("初测", "复测", "终测", "放大观察"), TEST_PHASE_OPTIONS)
        self.assertEqual("待执行", OPERATION_TASK_STATUS_OPTIONS[0])
        self.assertIn("已完成", OPERATION_TASK_STATUS_OPTIONS)
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


if __name__ == "__main__":
    unittest.main()
