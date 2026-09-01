import json
import unittest

from core.bitable import TableRecord
from core.seeding_batch_source import build_seeding_sync_tasks, resolve_seeding_batch_field_mapping
from core.sync import build_target_fields, resolve_field_mapping, TARGET_FIELD_ALIASES


class SeedingBatchSourceTest(unittest.TestCase):
    def _mapping(self):
        return resolve_seeding_batch_field_mapping(
            [
                "种草脚本ID", "产品编码", "产品图片", "首帧图", "批次序号", "脚本标题",
                "产品类型", "目标语言", "一级类目", "视频时长", "视频提示词",
                "口播", "中文口播", "进入生产", "发布策略",
            ]
        )

    def test_valid_seed_task_never_transports_shoppable_product_id(self):
        tasks = build_seeding_sync_tasks(
            [TableRecord("rec1", {
                "种草脚本ID": "SEED_1", "产品编码": "P1", "批次序号": 1,
                "视频提示词": "seed prompt", "进入生产": True,
                "口播": "ข้อความภาษาไทย", "中文口播": "中文对照",
                "脚本标题": "แชร์ลุคง่าย ๆ ในวันสบาย ๆ",
                "发布策略": "种草不挂车",
            })], self._mapping(),
        )
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].product_id, "")
        target_mapping = resolve_field_mapping(
            [
                "产品ID", "全球产品ID", "脚本类型", "发布用途", "是否挂车", "内容分支",
                "口播表达合同", "口播执行计划", "是否配口播", "口播状态", "首帧策略",
            ],
            TARGET_FIELD_ALIASES,
        )
        fields = build_target_fields(tasks[0], target_mapping)
        self.assertNotIn("产品ID", fields)
        self.assertEqual(fields["全球产品ID"], "P1")
        self.assertEqual(fields["脚本类型"], "种草脚本")
        plan = json.loads(tasks[0].voiceover_execution_plan)
        self.assertEqual(plan["mode"], "PRESERVE_SOURCE_COPY")
        self.assertEqual(plan["target_text"], "ข้อความภาษาไทย")
        self.assertTrue(plan["reuse_source_copy"])
        self.assertEqual(plan["audio_policy"]["bgm"]["mode"], "REQUIRED_BED")
        self.assertEqual(plan["audio_policy"]["bgm"]["energy_envelope"][0]["energy"], "medium")
        self.assertEqual(plan["audio_policy"]["bgm"]["energy_envelope"][1]["energy"], "low")
        self.assertIn("commercial_whoosh", plan["audio_policy"]["bgm"]["forbidden_styles"])
        self.assertEqual(json.loads(fields["口播执行计划"])["mode"], "PRESERVE_SOURCE_COPY")
        self.assertIs(fields["是否配口播"], True)
        self.assertEqual(fields["口播状态"], "待处理")

        publish_mapping = resolve_field_mapping(
            ["短视频标题", "所属母版", "母版方向", "变体强度"],
            TARGET_FIELD_ALIASES,
        )
        publish_fields = build_target_fields(
            tasks[0], publish_mapping, include_publish_metadata=True,
        )
        self.assertEqual(publish_fields["短视频标题"], "แชร์ลุคง่าย ๆ ในวันสบาย ๆ")
        self.assertEqual(publish_fields["母版方向"], "种草内容")

    def test_first_frame_takes_precedence_over_product_images(self):
        tasks = build_seeding_sync_tasks(
            [TableRecord("rec1", {
                "种草脚本ID": "SEED_1", "产品编码": "P1", "批次序号": 1,
                "视频提示词": "seed prompt", "进入生产": True,
                "口播": "ข้อความภาษาไทย", "中文口播": "中文对照",
                "发布策略": "种草不挂车",
                "产品图片": [{"file_token": "product-token", "name": "product.jpg"}],
                "首帧图": [{"file_token": "first-token", "name": "first.png"}],
            })], self._mapping(),
        )
        self.assertEqual(tasks[0].reference_images[0]["file_token"], "first-token")
        self.assertEqual(tasks[0].first_frame_strategy, "GENERATED_FIRST_FRAME")

    def test_missing_seed_voiceover_blocks_handoff(self):
        with self.assertRaisesRegex(ValueError, "SEEDING_SCRIPT_HANDOFF_INCOMPLETE"):
            build_seeding_sync_tasks(
                [TableRecord("rec1", {
                    "种草脚本ID": "SEED_1", "产品编码": "P1",
                    "视频提示词": "seed prompt", "进入生产": True,
                    "发布策略": "种草不挂车",
                })], self._mapping(),
            )

    def test_invalid_publish_policy_blocks_sync(self):
        with self.assertRaisesRegex(ValueError, "SEEDING_PUBLISH_POLICY_INVALID"):
            build_seeding_sync_tasks(
                [TableRecord("rec1", {
                    "种草脚本ID": "SEED_1", "产品编码": "P1",
                    "视频提示词": "seed prompt", "进入生产": True,
                    "发布策略": "种草挂车",
                })], self._mapping(),
            )


if __name__ == "__main__":
    unittest.main()
