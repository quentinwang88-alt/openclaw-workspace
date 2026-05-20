#!/usr/bin/env python3
"""短视频复刻结果同步到原始脚本管理表的单元测试。"""

from pathlib import Path
import sys
import unittest


TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from sync_to_script_table import (  # noqa: E402
    SCRIPT_TYPE_SHORT_VIDEO_REMAKE,
    SCRIPT_TYPE_NURTURE_REMAKE,
    build_source_synced_fields,
    build_target_fields,
    resolve_field_mapping,
    should_process_source_record,
    SOURCE_FIELD_ALIASES,
    TARGET_FIELD_ALIASES,
    STATUS_DONE,
)


class FakeRecord:
    def __init__(self, record_id, fields):
        self.record_id = record_id
        self.fields = fields


class VideoRemakeScriptTableSyncTest(unittest.TestCase):
    def setUp(self) -> None:
        self.source_mapping = resolve_field_mapping(
            [
                "状态",
                "同步状态",
                "脚本ID",
                "同步时间",
                "店铺ID",
                "产品ID",
                "产品图片",
                "目标国家",
                "目标语言",
                "商品类型",
                "最终视频提示词",
                "视频时长",
            ],
            SOURCE_FIELD_ALIASES,
        )
        self.target_mapping = resolve_field_mapping(
            [
                "产品编码",
                "产品ID",
                "产品图片",
                "店铺ID",
                "脚本方向一",
                "所属母版1",
                "目标国家",
                "目标语言",
                "产品类型",
                "脚本来源",
                "脚本类型",
                "发布用途",
                "最终视频提示词",
                "视频时长",
                "脚本状态",
                "任务状态",
                "源复刻任务ID",
                "是否可同步母版",
                "是否可同步",
            ],
            TARGET_FIELD_ALIASES,
        )

    def test_build_target_fields_uses_generated_script_id_and_disables_downstream_sync(self) -> None:
        record = FakeRecord(
            "rec_001",
            {
                "状态": STATUS_DONE,
                "脚本ID": "source_should_not_be_reused",
                "店铺ID": "shop_9",
                "产品ID": "P10086",
                "产品图片": [{"file_token": "file_1"}],
                "目标国家": "US",
                "目标语言": "English",
                "商品类型": "bracelet",
                "最终视频提示词": "final prompt body",
                "视频时长": "23秒",
            },
        )

        script_id, fields = build_target_fields(record, self.source_mapping, self.target_mapping)

        self.assertEqual(script_id, "P10086_VRrec001_M")
        self.assertEqual(fields["产品编码"], "P10086")
        self.assertEqual(fields["产品ID"], "P10086")
        self.assertEqual(fields["产品图片"], [{"file_token": "file_1"}])
        self.assertEqual(fields["视频时长"], 23)
        self.assertEqual(fields["店铺ID"], "shop_9")
        self.assertEqual(fields["脚本来源"], SCRIPT_TYPE_SHORT_VIDEO_REMAKE)
        self.assertEqual(fields["脚本类型"], SCRIPT_TYPE_SHORT_VIDEO_REMAKE)
        self.assertEqual(fields["所属母版1"], "VRrec001")
        self.assertEqual(fields["任务状态"], "已完成")
        self.assertEqual(fields["是否可同步母版"], False)
        self.assertEqual(fields["是否可同步"], False)
        self.assertTrue(fields["脚本方向一"].startswith("【脚本ID】\n- P10086_VRrec001_M\n\n"))

    def test_nurture_profile_marks_done_and_keeps_downstream_sync_enabled(self) -> None:
        record = FakeRecord(
            "rec_nurture",
            {
                "状态": STATUS_DONE,
                "店铺ID": "shop_9",
                "产品ID": "YR100",
                "最终视频提示词": "nurture final prompt",
            },
        )

        script_id, fields = build_target_fields(
            record,
            self.source_mapping,
            self.target_mapping,
            profile="nurture",
        )

        self.assertEqual(script_id, "YR100_YR1_M")
        self.assertEqual(fields["脚本来源"], SCRIPT_TYPE_NURTURE_REMAKE)
        self.assertEqual(fields["发布用途"], "养号")
        self.assertEqual(fields["任务状态"], "已完成")
        self.assertEqual(fields["是否可同步母版"], True)
        self.assertEqual(fields["是否可同步"], True)
        self.assertEqual(fields["脚本方向一"], "nurture final prompt")
        self.assertEqual(fields["视频时长"], 15)

    def test_should_process_new_completed_record_even_when_source_has_script_id(self) -> None:
        fields = {
            "状态": STATUS_DONE,
            "同步状态": "",
            "脚本ID": "legacy_source_id",
            "最终视频提示词": "body",
        }

        self.assertTrue(should_process_source_record(fields, self.source_mapping))

    def test_source_backwrite_refreshes_script_id_and_prompt_header(self) -> None:
        fields = build_source_synced_fields(self.source_mapping, "P10086_VR1_M", "body")

        self.assertEqual(fields["同步状态"], "已同步")
        self.assertEqual(fields["脚本ID"], "P10086_VR1_M")
        self.assertTrue(fields["最终视频提示词"].startswith("【脚本ID】\n- P10086_VR1_M\n\n"))


if __name__ == "__main__":
    unittest.main()
