import unittest

from core.bitable import TableRecord
from core.original_batch_source import (
    build_original_batch_sync_tasks,
    resolve_original_batch_field_mapping,
)


class OriginalBatchSourceTest(unittest.TestCase):
    def test_one_row_becomes_one_sync_task(self):
        fields = [
            "脚本ID", "产品编码", "产品图片", "店铺ID", "批次ItemID", "批次序号",
            "脚本标题", "产品类型", "目标语言", "一级类目", "视频时长",
            "完整生产脚本", "视频生成提示词", "进入生产", "同步结果", "同步时间",
            "处理状态", "运行任务ID",
        ]
        mapping = resolve_original_batch_field_mapping(fields)
        records = [
            TableRecord(
                "rec1",
                {
                    "脚本ID": "SCSCRIPT_1",
                    "产品编码": "P1",
                    "产品图片": [{"file_token": "img1"}],
                    "批次ItemID": "OCI_1",
                    "批次序号": 3,
                    "脚本标题": "看展",
                    "产品类型": "外套",
                    "目标语言": "泰语",
                    "一级类目": "女装",
                    "视频时长": 15,
                    "视频生成提示词": "完整生产提示词",
                    "进入生产": True,
                },
            )
        ]
        tasks = build_original_batch_sync_tasks(records, mapping)
        self.assertEqual(len(tasks), 1)
        task = tasks[0]
        self.assertEqual(task.script_id, "SCSCRIPT_1")
        self.assertEqual(task.script_slot, "D03")
        self.assertEqual(task.prompt_text, "完整生产提示词")
        self.assertEqual(task.internal_script_key, "OCI_1")

    def test_unchecked_row_is_ignored(self):
        mapping = resolve_original_batch_field_mapping(
            ["脚本ID", "产品编码", "视频生成提示词", "进入生产"]
        )
        tasks = build_original_batch_sync_tasks(
            [TableRecord("rec", {"脚本ID": "S1", "产品编码": "P", "视频生成提示词": "x"})],
            mapping,
        )
        self.assertEqual(tasks, [])


if __name__ == "__main__":
    unittest.main()
