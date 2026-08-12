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
            "人物模板ID（系统）", "人物模板合同_JSON（系统）", "视觉参考模式（系统）",
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
                    "人物模板ID（系统）": "TH_PERSONA_001",
                    "人物模板合同_JSON（系统）": '{"availability":"AVAILABLE"}',
                    "视觉参考模式（系统）": "PERSONA_PRODUCT_COMPOSITE_PREFERRED",
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
        self.assertEqual(task.persona_id, "TH_PERSONA_001")
        self.assertEqual(task.persona_contract, '{"availability":"AVAILABLE"}')
        self.assertEqual(
            task.first_frame_strategy, "PERSONA_PRODUCT_COMPOSITE_PREFERRED"
        )

    def test_unchecked_row_is_ignored(self):
        mapping = resolve_original_batch_field_mapping(
            ["脚本ID", "产品编码", "视频生成提示词", "进入生产"]
        )
        tasks = build_original_batch_sync_tasks(
            [TableRecord("rec", {"脚本ID": "S1", "产品编码": "P", "视频生成提示词": "x"})],
            mapping,
        )
        self.assertEqual(tasks, [])

    def test_required_strategy_does_not_block_without_user_request(self):
        fields = [
            "脚本ID", "产品编码", "产品图片", "视频生成提示词", "进入生产",
            "人物模板合同_JSON（系统）", "视觉参考模式（系统）",
            "统一首帧（系统）", "首帧准备状态（系统）",
        ]
        mapping = resolve_original_batch_field_mapping(fields)
        task = build_original_batch_sync_tasks(
            [TableRecord("rec", {
                "脚本ID": "S1",
                "产品编码": "P1",
                "产品图片": [{"file_token": "raw_product"}],
                "视频生成提示词": "x",
                "进入生产": True,
                "人物模板合同_JSON（系统）": '{"availability":"AVAILABLE"}',
                "视觉参考模式（系统）": "PERSONA_PRODUCT_COMPOSITE_REQUIRED",
            })],
            mapping,
        )[0]
        self.assertEqual("", task.reference_preparation_error)
        self.assertEqual(task.reference_images, [{"file_token": "raw_product"}])

    def test_user_requested_first_frame_blocks_until_ready(self):
        fields = [
            "脚本ID", "产品编码", "产品图片", "视频生成提示词", "进入生产",
            "生成首帧（需勾选）", "统一首帧（系统）", "首帧准备状态（系统）",
        ]
        mapping = resolve_original_batch_field_mapping(fields)
        task = build_original_batch_sync_tasks(
            [TableRecord("rec", {
                "脚本ID": "S1", "产品编码": "P1",
                "产品图片": [{"file_token": "raw_product"}],
                "视频生成提示词": "x", "进入生产": True,
                "生成首帧（需勾选）": True,
                "首帧准备状态（系统）": "待生成",
            })], mapping,
        )[0]
        self.assertIn(
            "WAITING_USER_SELECTED_FIRST_FRAME",
            task.reference_preparation_error,
        )

    def test_confirmed_composite_replaces_raw_product_reference(self):
        fields = [
            "脚本ID", "产品编码", "产品图片", "视频生成提示词", "进入生产",
            "人物模板合同_JSON（系统）", "视觉参考模式（系统）",
            "生成首帧（需勾选）",
            "统一首帧（系统）", "首帧准备状态（系统）",
        ]
        mapping = resolve_original_batch_field_mapping(fields)
        task = build_original_batch_sync_tasks(
            [TableRecord("rec", {
                "脚本ID": "S1",
                "产品编码": "P1",
                "产品图片": [{"file_token": "raw_product"}],
                "视频生成提示词": "x",
                "进入生产": True,
                "人物模板合同_JSON（系统）": '{"availability":"AVAILABLE"}',
                "视觉参考模式（系统）": "PERSONA_PRODUCT_COMPOSITE_REQUIRED",
                "生成首帧（需勾选）": True,
                "统一首帧（系统）": [{"file_token": "composite"}],
                "首帧准备状态（系统）": "已就绪",
            })],
            mapping,
        )[0]
        self.assertEqual(task.reference_preparation_error, "")
        self.assertEqual(task.reference_images, [{"file_token": "composite"}])


if __name__ == "__main__":
    unittest.main()
